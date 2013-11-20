#!/usr/bin/env python
# -*- coding: utf-8 -*-S
#
# Copyright (C) 2013 Mark Richardson.
#
# pylint: disable=E1103,F0401
"""Usage: filefind.py
          filefind.py [--build dir -c computer_name]
          filefind.py [--list option -c computer_name]

-b <dir> --build <dir>    Walk the directory of the file system storing the
                          results in our redis datastore.
-c <name> --computer <name>    Set the name of the computer
-l <opt> --list <opt>    List the following option:
        option        Description of listing
        -----------   --------------------------------------------------------
        keys          list all keys in redis data store
        paths         list the FileFind paths run against this machine
        machines      list all machines & dirs with FileFind Paths on it

    A new option requires that you update process_arguments and write a
    function to check the validity of the options
"""

__version__ = 'Version 0.1'

import os
import sys
import json
import docopt
import platform
import sha1sum
import unidecode
import python3_timer


#do some adiministrative overhead
from redis import StrictRedis
CONNECTION = StrictRedis(host='192.168.1.96', port=6379)

from redis_collections import Dict
#from time import strftime
from collections import namedtuple  # , Counter, OrderedDict, defaultdict

#define the namedtuples we will use throughout the program.
FILE_INFO = namedtuple('FILE_INFO', 'size, sha1, dateepoch')


def die_with(error_message):
    """If you find an unrecoverable error. send the final dying gasp here
       and we'll broadcast it and then kill the program.
    """
    print("Fatal Error")
    print("    %s" % error_message)
    sys.exit(2)


def review_build_arg(args, key):
    """ Is the directory handed in by the user a valid file directory?
        Echo build options
    """
    file_directory = args[key]
    try:
        if os.path.isdir(file_directory):
            print("file_directory to be built is -> %s" % file_directory)
        else:
            die_with("File directory does not exist -> %s" % file_directory)
    except TypeError:
        pass


def review_name_arg(args, key):
    """We take this at face value. Really no need to review it the user can
       name this machine anything they want and we will accept it.
       Echo name options
    """
    machine_name = args[key]
    if machine_name is None:
        #use the computer to get the machines name.
        args[key] = (platform.uname()[1])
        machine_name = args[key]
    else:
        pass
    print("Machine name for this run is -> %s" % machine_name)


def review_list_arg(args, key):
    """These are the listings that the user can request
       Echo list options
    """
    #if option is None, then skip processing
    if args[key] is not None:
        #define the valid listing options (in lower case)
        valid_listing_opts = ['keys', 'paths', 'machines']
        user_wants = args[key].lower()
        if user_wants in valid_listing_opts:
            pass
        else:
            die_with('Invalid --listing option -> %s' % user_wants)
        print('listing option is -> %s' % user_wants)

    else:
        pass


def can_stat_file(filename):
    """I don't know why the file system returns a file that is not capable
       of having the os.stat call run against it. For some reason, some of
       these files fail that smell test.  This is that smell test. Let the
       caller know that the file is capable of being 'stat'ed.
    """
    try:
        _ = os.stat(filename)
        statable_file = True
    except (IOError, OSError):
        statable_file = False
    return statable_file


def get_os_size_date(filename):
    """Given a filename, return the size and mod time  of it"""
    try:
        (_, _, _, _, _, _, size, _, mtime, _) = os.stat(filename)
    except (IOError, OSError):
        die_with('unstatable file %s' % filename)
    return size, mtime


def recur_dir(road_to_nowhere, stop=None, stat_file=True):
    """ Yield the list of all files underneath the directory given by the
        variable road_to_nowhere. Apologies to Talking Heads. I have a problem
        here and I'm not sure what to do about it.  Non-ascii filenames are
        sure to make the rest of my program barf.  It's a shame, but if the
        utf-8 name does not equal the filesystem filename, I'm not going to
        process that file. Also add some extra variables to control how
        many records are processed (for testing purposes) and why not add a
        variable that stats the file first.  That way, I know I'm only passing
        back files that bona-fide.
    """
    count = 0
    for root, _, files in os.walk(road_to_nowhere):
        for curfile in files:
            count += 1
            #get the filename from the system.
            filename = os.path.join(root, curfile)
            #sanitize the filename
            pretty = unidecode.unidecode(filename)
            if pretty == filename:
                if stop is not None:
                    if count >= stop:
                        raise StopIteration
                if stat_file:
                    if can_stat_file(filename):
                        yield filename
                    else:
                        sys.stderr.write('unstatable file -> %s' % pretty)
                        sys.stderr.write('    file ignored')
                else:
                    yield filename
            else:
                sys.stderr.write('filename %s has non-ascii chars\n' % pretty)
                sys.stderr.write('    file ignored ->', [filename])


def remove_apple_nonsense(filename, apple_signature):
    """On my samba server, my apple mac pro litters the filesystem with files
       that have the moniker ./AppleDouble in them.  These add nothing to my
       results and only serve to piss me off.  This is where I eliminate them
       from processing so they don't clog up my results with somewhat
       duplicate information.
    """
    #if you don't find the apple signature, then it's a keeper
    return (filename.find(apple_signature) < 0)


def get_datastore_dict(key=None, connection=CONNECTION):
    """This is where we are going to store our information. The housekeeping
       chores are all done. We just need to go and grab the key that contains
       the dictionary that we are interested in.
    """
    redis_obj = Dict(redis=connection, pickler=json, key=key)
    return redis_obj


def get_values_for_datastore(filename, compute_sha1_sum=True):
    """ Given a filename, go out and get the size, date and the sha1 sum
        for the file.  Return these values in a list which will be stored
        in our redis datastore.
    """
    size, dateepoch = get_os_size_date(filename)
    if compute_sha1_sum:
        sha1 = sha1sum.sha1sum(filename)
    else:
        sha1 = 'sha1 not computed'

    file_data = FILE_INFO(size, sha1, dateepoch)
    store_this = [file_data.size,  # zeroth element
                  file_data.sha1,  # 1st element
                  file_data.dateepoch]  # 2nd element
    return store_this


def update_datastore(filename, datastore):
    """Check to see if the filename is in our database.  If it is, then check
       the size of the file.  If the filesize in the datastore is equal to the
       size reported by the file system, we assume the files are equal. This
       saves us time having to compute the sha1 sum of a file.  This
       fingerprint allows us to identify files that are identical in our query
       operations that are sure to follow.
    """

    #does the incoming filename already have a sha1 sum computed?
    if filename in datastore:
        #yes, it does, now you have to decide

        #Is the file size the same as it was when we computed the sha1 sum
        #before? if yes, we do not compute it again.
        if get_os_size_date(filename)[0] == datastore[filename][0]:
            #yes, it is so no work to do
            pass
        else:
            #No, file size does not compare so re-read and compute sha1
            datastore[filename] = get_values_for_datastore(filename)
            #print('recomputing sha1 sum for changed file -> %s' % filename)
    else:
        #no the file does not have an entry in the sha1 database, so we know
        #we need to compute it
        datastore[filename] = get_values_for_datastore(filename)
        #print('computing sha1 sum for filename -> %s' % filename)


def consume_pipeline(pipename):
    """ Turn on the juice to fire up the generators and push the
        data through them. Return the count of the number
        of items falling out the end of the pipe.  It may be along
        the way that initial items in the pipe are diverted to the
        rubbish bin.  This only counts what comes out.
    """
    count = 0
    print 'Processed'
    for _ in pipename:
        count += 1
        if count % 10000 == 0:
            print ' %d files' % count
    return count


def build_mode(*args, **kwargs):
    """This is the command to walk a file system and store the results
       of the walk in our redis datastore."""
    print(args, kwargs)
    dir_path = args[0]['--build']
    machine_name = args[0]['--computer']
    print("\n\n***********************************")
    print("Beginning build process")
    print("***********************************")
    print("For Machine named -> %s" % machine_name)
    print("Now beginning walk of this directory path -> %s" % dir_path)

    nosql_key = 'FileFind:%s:%s:' % (machine_name, dir_path)
    datastore = get_datastore_dict(key=nosql_key)
    print('nosql_key -> %s' % nosql_key)

    #Cascade generators together (like pipes) to build out our redis store
    # database. Up first,

    #make generator to pull file names found along our directory path
    filenames = (filename for filename in recur_dir(dir_path, stop=None))
    #now, combine with getting rid of those aggravating ./AppleDouble files
    good_files = (good_file for good_file in filenames
                  if remove_apple_nonsense(good_file, '/.AppleDouble/'))
    #a generator that updates our datastore as file information is updated.
    process_names = (update_datastore(filename, datastore)
                     for filename in good_files)

    count = consume_pipeline(process_names)
    print ('Total items processed is -> %d' % count)


def list_all_redis_keys():
    """ All of the keys in our redis datastore.
    """
    key_list = CONNECTION.keys('*')
    print ('list of all keys in redis data store')
    for index, key in enumerate(sorted(key_list)):
        print('%d    %s' % (index, key))


def list_keys_for_this_machine(computer):
    """List all matching directory paths that have been run against this
       computer.
    """
    nosql_key = 'FileFind:%s' % computer
    key_list = CONNECTION.keys('*')
    print('List of directories searched for machine -> %s' % computer)
    index = 0
    for key in sorted(key_list):
        if key[0:len(nosql_key)] == nosql_key:
            index += 1
            print('%d    %s' % (index, key))
    if index == 0:
        print('no directories found')


def list_machines_in_redis():
    """If a FileFind run has been performed against a machine, the results
       of that scan will be stored in the database.  This listing option
       shows all of the machines scanned and the subdirectories found in
       our datastore.
    """
    filefind_sig = 'FileFind:'
    key_list = CONNECTION.keys('*')
    print('List of all machines and directories searched for in datastore')
    print('%42s %s' % ('Machine', ': Directory'))
    index = 0
    for key in sorted(key_list):
        if key[0:9] == filefind_sig:
            index += 1
            pieces = key.split(':')
            print('%d %40s : %s' % (index, pieces[1], pieces[2]))
    if index == 0:
        print('no directories found')


def list_redis_keys(*args, **kwargs):
    """List all or some of the keys in our redis datastore.
    """
    print kwargs
    list_option = args[0][0]['--list']
    if list_option == 'keys':
        list_all_redis_keys()
    elif list_option == 'paths':
        list_keys_for_this_machine(args[0][0]['--computer'])
    elif list_option == 'machines':
        list_machines_in_redis()
    else:
        die_with('unknown option -> %s' % args[0][0]['--list'])


def query_mode(*args, **kwargs):
    """This is the mode where we are reviewing the results of our file walk
       and are displaying the results to the user."""
    print(args, kwargs)
    print("Querying our file system")
    #build a dispatcher dictionary with our reports in it
    dispatcher = {'paths': list_redis_keys,
                  'keys': list_redis_keys,
                  'machines': list_redis_keys,
                 }
    #now execute the report
    #is it a list with an option
    if args[0]['--list'] is not None:
        dispatcher[args[0]['--list']](args)


def process_arguments(args):
    """ Take the user's input specified on the command line and make sure it
        makes sense. If it doesn't then abort the program from going further.
        The values in the process_arg dictionary must match the args talked
        about in the module doc string (which is also processed by docopt).
    """
    process_arg = {"--build": review_build_arg,
                   "--computer": review_name_arg,
                   "--list": review_list_arg}
    #iterate over the command line arguments, checking them for errors
    for key in args:
        process_arg[key](args, key)

    process_function = query_mode if args['--build'] is None else build_mode
    return process_function


if __name__ == '__main__':
    # for giggles, how long did it take?
    with python3_timer.Timer():
        #build argument and options list with docopt
        ARGUMENTS = docopt.docopt(__doc__, version=__version__)
        print (ARGUMENTS)

        #process arguments determines which form of the command we are going
        #to run as specified above after checking that all of the ARGUMENTS
        #are up to snuff.
        RUN_PROCESS = process_arguments(ARGUMENTS)

        #now run the program
        RUN_PROCESS(ARGUMENTS)
