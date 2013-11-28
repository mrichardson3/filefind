#!/usr/bin/env python
# -*- coding: utf-8 -*-S
#
# Copyright (C) 2013 Mark Richardson.
#
# pylint: disable=E1103,F0401
"""Usage: filefind.py
    filefind.py [--build dir -c computer_name]
    filefind.py [--list option -c computer_name]
    filefind.py -u id [--name name --path name --any name --ext name --size <range> --date <range> --work <filter>]

-b <dir> --build <dir>    Walk the directory of the file system storing the
                          results in our redis datastore.
-c <name> --computer <name>    Override the name of the computer for this run.
                               This allows you to see other machine results
-l <opt> --list <opt>    List with the following options:
        option        Description of listing
        -----------   --------------------------------------------------------
        keys          list of ids for all keys in redis data store
        paths         list of ids for the FileFind paths run on this computer
        machines      list of ids for all machines & dirs with FileFind Paths

Query mode options
-u <id> --use <id>         First run <-l paths> | <-l machines> to get the id
                               of the machine and path that you would like to
                               query. Then use -u option to select that path. 
-n <name> --name <name>    Locates files with the string <name> in the
                               basename of the filename
-p <name> --path <name>    Locates files with the string <name> in the path to
                               the filename
-a <name> --any <name>     Locates files with the string <name> anywhere
                               in the filename
-e <name> --ext <name>     Locates files with the string <name> found in
                               the filename's extension.
-s <range> --size <range>    Select files based on the size of the file.

-d <range> --date <range>    Select files based on the date of the file.
-w <filter> --work <filter>    Second filter to optionally review a file or 
                               perform operations on it like copying.

    A new option requires that you update process_arguments and write a
    function to check the validity of the options
"""

__version__ = 'Version 0.1'

import os
import sys
import json
import arrow
import docopt
import platform
import sha1sum
import unidecode
import datetime
import python3_timer

from operator import attrgetter


#do some adiministrative overhead
from redis import StrictRedis
CONNECTION = StrictRedis(host='192.168.1.96', port=6379)
BEGIN_TIME = arrow.get(-62135596800)  # 0001-01-01T00:00:00+00:00 
END_TIME = arrow.get(253402300799) # 9999-12-31T23:59:59+00:00


from redis_collections import Dict, List
#from time import strftime
from collections import namedtuple  # , Counter, OrderedDict, defaultdict

#define the namedtuples we will use throughout the program.

#1. this defines the namedtuple of what we store in our datastore
DATASTORE_VALUE = namedtuple('DATASTORE_VALUE', 'size, sha1, dateepoch')
#2. this is the format of the results returned by a file query. 
FILE_INFO = namedtuple('FILE_INFO', 'path, name, ext, size, sha1, date')
#3. this is the format for date ranges
DATE_RANGE = namedtuple('DATE_RANGE','begin_date end_date')


def die_with(error_message):
    """If you find an unrecoverable error. send the final dying gasp here
       and we'll broadcast it and then kill the program.
    """
    print("Fatal Error")
    print("    %s" % error_message)
    sys.exit(2)


def comma(number):
    """Given an integer, return a string of that number with commas inserted.
    """
    if type(number) is not int and type(number) is not long:
        raise TypeError("Not an integer!")
    seq = '%d' % number
    groups = []
    while seq and seq[-1].isdigit():
        groups.append(seq[-3:])
        seq = seq[:-3]
    return seq + ','.join(reversed(groups))


def review_build_arg(args, key):
    """ Is the directory handed in by the user a valid file directory?
        Echo build options
    """
    if args[key] is not None:
        user_path = args[key]
        user_path = os.path.normpath(user_path)
        args[key] = user_path  # push normalized path into argument database
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

def dummy_review(*args, **kwargs):
    """A dummy routine to pass the items through while I await to figure
       out what I'm going to do with them.
    """
    print('using', args, kwargs)


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
                print('    file ignored -> ', [filename])


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


def get_datastore_list(key=None, connection=CONNECTION):
    """The redis datastore List abstraction object.  Use this when you want
       to store lists in redis.
    """
    redis_obj = List(redis=connection, pickler=json, key=key)
    return redis_obj


def get_values_for_datastore(filename, compute_sha1_sum=False):
    """ Given a filename, go out and get the size, date and the sha1 sum
        for the file.  Return these values in a list which will be stored
        in our redis datastore. Here we load the database payload.
    """
    size, dateepoch = get_os_size_date(filename)
    if compute_sha1_sum:
        sha1 = sha1sum.sha1sum(filename)
    else:
        sha1 = 'sha1 not computed'

    file_data = DATASTORE_VALUE(size, sha1, dateepoch)
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


def list_keys(signature=None, title=None):
    """Iterate over all of the keys in our redis store.  When you find keys
       that match the signature, store them in our memory list and output
       them for the user to see.  That way we can use the id's as shorthand
       to access that particular path search.
    """
    #housekeeping to remember what keys are listed by this routine.
    memory = get_datastore_list(key='memory_bank')
    memory.clear()

    key_list = CONNECTION.keys('*')
    print('%s' % title)
    if signature:
        print('%s %39s %62s %16s' % ('id', 'Machine', 'Directory', 'Files'))
    index = 0
    for key in sorted(key_list):
        if signature is not None:
            if key[0:len(signature)] == signature:
                index += 1
                #get number of recs on file.
                datastore = get_datastore_dict(key=key)
                num_recs = len(datastore)
                pieces = key.split(':')
                print('%d %40s   %60s %16d' % (index, 
                                               pieces[1],
                                               pieces[2],
                                               num_recs))
                memory.append((index, key))
        else:
            index += 1
            print('%d    %s' % (index, key))
            memory.append((index,key))
    if index == 0:
        print('no directories found')


def list_redis_keys(*args, **kwargs):
    """List all or some of the keys in our redis datastore.
    """
    print kwargs
    list_option = args[0][0]['--list']
    if list_option == 'keys':
        list_keys(title='List of all keys in redis database')
    elif list_option == 'paths':
        list_keys(signature = 'FileFind:%s' % args[0][0]['--computer'],
                  title = ('List of all paths searched for this machine' +
                           '-> %s' % args[0][0]['--computer']))
    elif list_option == 'machines':
        list_keys(signature='FileFind:',
                  title = 'List of all machine and paths in redis datastore')
    else:
        die_with('unknown option -> %s' % args[0][0]['--list'])


def extension_expansion_dict():
    """ Return a dictionary with the keys being the text entered by users
        and the values being the replacement strings to be substituted.
        This is designed so that we don't have to remember all of the
        extensions that might be associated with geeks. For example,
        By specifying --extension geek
            we get all of the file extensions found in the values of our
            dictionary (i.e., log, sql etc. etc.) It's shorthand notation
    """
    expansions = {
        'txt_files' : 'txt,rtf,doc,xls,org,htm,html,odp,odt,pps,ppt,nfo,tex',
        'ebook_files' : 'pdf,epub,chm,ps,djvu',
        'images' : 'png,gif,jpg,bmp,jpeg,tiff,ico,psd,xcf,svg,tga,ai',
        'exe_files' : 'exe,msi,lnk,swf,jar,jnlp,dll,com,bat,app,gadget',
        'iso_files' : 'iso,nrg,bin,cue,mds,ccd,udf,daa,uif,vcd',
        'zip_files' : ('zip,gz,tar,bz2,rar,ace,tgz,z,7z,deb,pls,m3u,' +
                       'sfv,pkg,dmg,rpm'),
        'audio_files' : 'wav,mp3,midi,mid,wma,aac,ac3,faac,ape,m4a',
        'music' : 'mp3,ogg',
        'photoshop' : 'psd',
        'video' : ('mp4,mkv,ogg,mpg,mpeg,wmv,avi,m4v,flv,' +
                   'divx,ogv,mov,vob,rm,3gp'),
        'src_files' : ('php,c,py,js,css,fla,lsp,erl,sh,hs,scm,d,go' +
                       ',pl,avs,ahk,as,fla,cpp,bash,hrl,h,java,m,ml'),
        'geek' : 'log,sql,cnf,conf,patch,diff,ini,xml,cvs,cfg',
    }
    #convert the text string to a list real quick like
    keys = expansions.keys()
    for key in keys:
        new_list = expansions[key].split(',')
        expansions[key] = new_list  #replace the string with a list
    return expansions


def default_filter_function(file_info):
    """dummy default function to return all elements unless other filter
       functions elements specify otherwise.
    """
    return True

def get_eos_30d(file_info):
    """  This function expects to get a filename of a jpg. Hopefully, the
         jpg was taken with a camera.  If it was taken with a camera, then
         it looks to see if it was taken with a Canon EOS 30D.  If it was
         the function returns True, otherwise False.  It was written to see
         what was the first picture, I ever took with my eos 30d.
    """
    if file_info.filename[-3:].lower() == 'jpg':
        foto = open(file_info.filename, 'rb')
        # Return Exif tags
        tags = exifread.process_file(foto)
        #keynames = tags.keys()
        #print keynames
        #print type(tags)
        if 'Image Model' in tags:
            #print 'tags of image model', [tags['Image Model']], str(tags['Image Model'])
            if str(tags['Image Model']).strip() == 'Canon EOS 30D':
                return True
            else:
                return False
        else:
            return False
    else:
        return False

def copy_music(file_info):
    """With some caveats, copy all music files to the /tmp/Music directory.
    """
    #does the path have "iTunes Music" in it?
    if file_info.path.find('iTunes Music') >= 0:
        """
        start = file_info.path.find('iTunes Music')
        dest = ('/tmp/Music/' +
                file_info.path[start+len('iTunes Music')+1:] +
                '/' + file_info.name + file_info.ext)
        src = file_info.path + '/' + file_info.name + file_info.ext

        dest_path, dest_filename = os.path.split(dest)
        command = 'mkdir -p "' + dest_path + '"'
        returned_output = os.popen(command).readlines()
        command = 'cp "' + src + '" "' + dest + '"'
        returned_output = os.popen(command).readlines()
        print('copy source --> %s' % src)
        print('     dest. ---> %s' % dest)
        print('     results -> %s\n' % returned_output)
        """
        return False
    else:
        return True #show me music not in itunes folder

def copy_pics(file_info):
    """With some caveats, copy all photos to the /tmp/Photos directory.
    """
    #does the path have "/Pictures/" in it?
    if file_info.path.find('/Pictures/') >= 0:
        start = file_info.path.find('/Pictures/')
        dest = ('/tmp/Photos/' +
                file_info.path[start+len('/Pictures/'):] +
                '/' + file_info.name + file_info.ext)
        src = file_info.path + '/' + file_info.name + file_info.ext

        dest_path, dest_filename = os.path.split(dest)
        command = 'mkdir -p "' + dest_path + '"'
        returned_output = os.popen(command).readlines()
        command = 'cp "' + src + '" "' + dest + '"'
        returned_output = os.popen(command).readlines()
        print('copy source --> %s' % src)
        print('     dest. ---> %s' % dest)
        print('     results -> %s\n' % returned_output)
        return True
    else:
        return False



def add_work_filter(func, function_name=None):
    """ wrap another function around our worker_function defined above.
        Here's how you do it.
        First you write your specialized function similiar to the
        get_eos_30d.  You will be passed a meta_info namedtuple.
        This will allow you to operate on said namedtuple to make
        the decision of whether you want to include (or exclude) the
        file in the final report.  Then you wrap the worker_function
        with a True or False boolean appropriately and return the new
        worker_function.
    """
    #this is the name you will call your worker_function from the
    # command line line with the -w option. Print out a description
    #of what the function does so that it informs the user that 
    #the file has been tested against a particular worker function.
    if function_name == '30d':
        print ('image file must have been taken with eos 30d')
    if function_name == 'copy_music':
        print('copying all music files to /tmp/Music')
    if function_name == 'copy_pics':
        print('copying all photos to /tmp/Photos/')

    #add keys to the dispatcher dictionary with the key being the
    #-w command and the value being the name of the function that
    #returns true or false 
    dispatcher={'30d':get_eos_30d,
                'copy_music':copy_music,
                'copy_pics':copy_pics,}

    #use the dispatcher to make sure only approved functions are called.
    try:
        function=dispatcher[function_name]
    except KeyError:
        raise ValueError('invalid input to the -w parameter --> %s' % function_name)
    #print function_name
    #print type(function_name)

    #define the new function
    def new_worker_function(*args, **kwargs):
        #see, its the new worker function combo-ed up with the old
        #worker function and sent back to the calling program as the
        #newest worker function.
        return function(*args, **kwargs) and func(*args, **kwargs)
    return new_worker_function


def add_name_filter(func, name_values=None, file_part=None):
    """This function wraps a selection filter around our default filter
       function so that it now discerning on what the user wants to 
       search on.  This takes care of the the name, extension, path, and
       anywhere options passed in through the command line. The parameter
       name_values are the options passed in by the user from the command
       line.  the file_part parameter, tells the program what portion of the
       filename is to be searched.  We then generate a custom function and
       wrap it around the other search functions which determine whether or
       not a certain file makes the cut.
    """
    #do some administrative work so that we can return our custom filter
    #function that returns only the results we want.

    #extension searches cannot be anded together as it makes no sense as the
    #extension is only one word. Or's yes! And's No!
    if file_part == 'ext':
        find_all = False
    else: 
        find_all = name_values.find('&') >= 0

    if find_all:
        #don't mix | and & in the same search parameter. If you do the |
        #will become an &.  So, I don't have to parse very much boolean 
        #logic.

        #convert user's input into and array of searchable items.
        name_values = name_values.replace('|', '&')  #  no |'s if & found
        search_for = name_values.split('&')
        search_for = [x.strip().lower() for x in search_for]
        print file_part, " must match all strings below"
        print search_for
    else:
        if file_part == 'ext':
            # in case, somebody put a & in the extension list, convert it to |
            name_values = name_values.replace('&', '|')
            search_list = name_values.split('|')
            search_list = [x.strip().lower() for x in search_list]
            #build a shorthand extension substitution dictionary
            sub_dict = extension_expansion_dict()
            keys = sub_dict.keys()

            #final list to search across assuming substitution has been done
            search_for = set()
            for item in search_list:
                if item in keys:
                    #perform substitution
                    for sub_ext in sub_dict[item]:
                        search_for.add(sub_ext)
                else:
                    search_for.add(item)
            search_for = list(search_for)
            print 'extension must contain one of the following strings below'
            print search_for
        else:
            #use the or bar notation to split out the user defined list
            search_for = name_values.split('|')
            search_for = [x.strip().lower() for x in search_for]
            print file_part, " must contain at least 1 of the strings below"
            print search_for

    #now build the function that will return the results of our original
    #function and add to it the results of what this function found
    #This sounds like a stilted description, but I do not know of another
    #way to say what is happening.

    #ultimately, this function will be called with the filename as arg[0]
    #note this filename looks like a namedtuple called FILE_INFO
    def wrapped_function(*args, **kwargs):
        #print args, 'args in add_name_test'
        #print kwargs, 'keyword args in add_name_test'
        #sys.exit(0)
        compare_filename = (args[0].path+'/'+args[0].name+args[0].ext).lower()
        #print('compare_filename %s' % compare_filename)
        path, leaf = os.path.split(compare_filename)
        base, ext = os.path.splitext(leaf)
        if file_part is None:
            #default is search everything
            search_this = compare_filename
        elif file_part == 'name':
            search_this = base
        elif file_part == 'path':
            search_this = path
        elif file_part == 'any':
            search_this = compare_filename
        elif file_part == 'ext':
            search_this = ext
            #extension is normally passed in with a "." e.g. ".jpg". we want
            #to remove that dot so that we can compare the text
            if len(search_this) >= 1:
                if search_this[0] == '.':
                    search_this = search_this[1:]
                else:
                    pass
            #print 'search this is --->', search_this
        else:
            #this should never happen but it if does, we're not going to blow
            #up the search because of it. We just compare the full file name
            #and carry on.
            search_this = compare_filename

        if find_all:
            all_found = 0
            for item in search_for:
                #print args[0].filename.lower()
                if search_this.find(item) >= 0:
                    all_found += 1
                    continue
            return ((all_found == len(search_for)) and func(*args, **kwargs))
        else:
            #find any one of the strings
            any_one_found = False
            for item in search_for:
                if file_part == 'ext':
                    #must match the entire string not substring.
                    #print ('item, searchthis %s %s' % (item, search_this))
                    if item == search_this:
                        any_one_found = True
                        break
                else:
                    if search_this.find(item) >= 0:
                        any_one_found = True
                        break
            return ((any_one_found) and func(*args, **kwargs))
    return wrapped_function


def convert_to_int(string_integer):
    """ Convert a supposed "string integer" into a bona-fide integer.
        With substitution for these abbreviations.
            K = Kilo  = 1,024
            M = Mega  = 1,048,576
            G = Giga  = 1,073,741,824
            T = Tera  = 1,099,511,627,776
            P = Peta  = 1,125,899,906,842,624 #beyond here is future proofing.
            E = Exa   = 1,152,921,504,606,846,976
            Z = Zetta = 1,180,591,620,717,411,303,424
            Y = Yotta = 1,208,925,819,614,629,174,706,176
        e.g to find all files less than 1 gigabyte use --size "-1g"
            between 1K and 2M use --size 1k-2m
    """
    string_integer = string_integer.lower()
    sub_vals = {
    'k' : '*1024**1',
    'm' : '*1024**2',
    'g' : '*1024**3',
    't' : '*1024**4',
    'p' : '*1024**5',
    'e' : '*1024**6',
    'z' : '*1024**7',
    'y' : '*1024**8'}
    invalid_letters = 'a,b,c,d,f,h,i,j,l,n,o,q,r,s,u,v,w,x'
    invalids = invalid_letters.split(',')
    for letter in string_integer:
        if letter in invalids:
            die_with(('Invalid letter in size abbreviation -> %s' %
                      string_integer))
    #now replace the letters with the values
    for key in sub_vals:
        if string_integer.find(key) >= 0:
            string_integer = string_integer.replace(key, sub_vals[key])
  
    try:
        num_int = eval('int(' + string_integer + ')')
    except ValueError:
        die_with(('unable to convert this string to an integer %s' %
                  string_integer))
    return num_int


def add_size_filter(func, size_range=None):
    """ Tack on a filter function that measure the size of the file
        for measure.  I named it "mass" to suggest size but the "s"
        was already taken for sorting options. What's a developer to
        do?  It uses the protocol <smallest size> - <greatest_size> and 
        we use our best sense to make sense of the user's desires.  See
        the documentation above if you need specifics.
        As I see it, there are 4 types of comparisons when we consider the
        endpoints inclusive. They are:
        1: -<number>    --which means file size comparison is <= <number>
        2: <number>     --which means file size comparison is == <number>
        3: <number>-    --which means file size comparison is >= <number>
        4: <lonum>-<hinum> -- which means lonum <= file size comparison <= <hinum> # range test
    """
    if size_range.find('-') >= 0:
        #that means we have at least case 1, 3 or 4
        size_range = size_range.strip() #remove whitespace fore and aft.
        #print size_range
        if size_range[0] == '-':
            #this is case 1
            #print '1'
            case = 1
            file_size = convert_to_int(size_range[1:])
            print 'file size is <= to %s' % comma(file_size)
        elif size_range[-1] == '-':
            #this is case 3
            #print '3'
            case = 3
            file_size = convert_to_int(size_range[0:-1])
            print 'file size is >= to %s' % comma(file_size)
        else:
             #it must be in the middle somewhere so this is case 4
             #print '4'
             lo, hi = size_range.split('-')
             #print lo, hi
             lo = convert_to_int(lo)
             hi = convert_to_int(hi)
             file_size_lo = min(lo, hi) #put in the right order anyways
             file_size_hi = max(lo, hi)
             case = 4
             print 'file size is between %s and %s' % (comma(file_size_lo), comma(file_size_hi))
    else:
        #print '2'
        case = 2
        file_size = convert_to_int(size_range)
        print 'file size is equal to %s' % comma(file_size)

    def add_size_test(*args, **kwargs):
        #print args, 'args in add_name_test'
        tested_positive = False
        if case == 1:
            if args[0].size <= file_size:
                tested_positive = True
        elif case == 2:
            if args[0].size == file_size:
                tested_positive = True
        elif case == 3:
            if args[0].size >= file_size:
                tested_positive = True
        elif case == 4:
            if file_size_lo <= args[0].size <= file_size_hi:
                tested_positive = True
        else:
            print 'illegal value for case', case
            sys.exit(2)
        
        return tested_positive and func(*args, **kwargs)
    return add_size_test


def last_x_days(num_days=7):
    """return an arrow date range for the previous X number of days.
       0 days means files created today
       1 day means files created yesterday
       7 days means last week. you get the idea
    """
    end_day = arrow.now('US/Eastern').ceil('day')
    start_day = end_day.replace(hours=-(num_days+1)*24).replace(seconds=+1)
    return DATE_RANGE(start_day, end_day)


def days_since_beginning_of_year():
    """return an arrow date range starting from the beginning of the
       year and running up until today.
    """
    end_day = arrow.now('US/Eastern').ceil('day')
    start_day = arrow.get(datetime.datetime(end_day.year, 1, 1), 'US/Eastern')
    return DATE_RANGE(start_day, end_day)


def days_since_beginning_of_month():
    """return an arrow date range starting from the beginning of the
       month and running up until today.
    """
    end_day = arrow.now('US/Eastern').ceil('day')
    start_day = arrow.get(datetime.datetime(end_day.year, end_day.month, 1),
                         'US/Eastern')
    return DATE_RANGE(start_day, end_day)

def mod_date(date_string, end=None):
    """ Take a date of the format 12/31/2000 (in string format) and return
        a quasi "datetime" value of integers in the format 2000, 12, 31.
        So 'mm/dd/yyyy' maps to an int tuple of yyyy, mm, dd. Simple.
    """
    mm, dd, yyyy = return_mmddyy(date_string)
    return (datetime.datetime(yyyy, mm, dd), 'US/Eastern')


def return_mmddyy(user_input):
    """Given a date string input by a user in the format mm/dd/yyyy
       return 3 integers in a tuple with the format mm, dd, yy
    """
    mm, dd, yyyy = user_input.split('/')
    try:
        mm, dd, yyyy = map(int,(mm, dd, yyyy))
    except ValueError:
        die_with('Unable to convert date string ->%s' % user_input)
    return mm, dd, yyyy

def single_day(users_input):
    """For a single date, return the datespan """
    mm, dd, yy = return_mmddyy(users_input)
    end_day = arrow.get(datetime.datetime(yy, mm, dd), 'US/Eastern').ceil('day')
    start_day = arrow.get(datetime.datetime(yy, mm, dd), 'US/Eastern').floor('day')
    return DATE_RANGE(start_day, end_day)

def convert_stars(num_or_star):
    """ convert a date string number with wild card stars 
        into a three valued tuple.  Numbers go as themselves while
        stars get mapped to a -1. -1 indicates to the comparison
        function that you do not need to compare this value. The
        three tuple's format is (mm, dd, yy).
    """
    if num_or_star == '*':
         ret_val = -1
    else:
        try:
            ret_val = int(num_or_star)
        except ValueError:
            die_with('template date has bad component -> %s' % num_or_star)
    return ret_val


def extract_3_tuple(date_range):
    """ the format should be something like
        [ <month> | *] / [ <day> | *] / [<year> | *]
        so 1/*/*  means all files created in january over the years
           */2/*  means all files created on the second day of the month
           12/31/* means all new years eve files
           */*/2000 means all files made during the millenium
    """
    #print 'input date range', date_range
    vals = date_range.strip().split('/')
    dates = map(convert_stars,vals)
    return (dates)
 

def extract_epoch_mmddyy(date_template):
    """given an epoch datetime stamp, return 3 tuple format for 
       comparison against our wildcard user template.
    """
    file_datetime = arrow.get(datetime.datetime.fromtimestamp(date_template), 'US/Eastern')
    file_date = str(file_datetime.month) + '/' + str(file_datetime.day) + '/' + str(file_datetime.year)
    file_3_tuple = extract_3_tuple(file_date)
    return file_3_tuple

def compare3tup(mmddyy,file_tuple):
    """ compare the first to the second and use the -1 as keys to not do
        a comparison. if all match, then return true else return false.
    """
    #iterate over first tuple and compare to the file's tuple.  If they match
    #return true otherwise return false
    pattern_match = True
    for index, item in enumerate(mmddyy):
        # -1 is the code to ignore this field
        if item == -1:
            continue
        else:
            if mmddyy[index] == file_tuple[index]:
                continue
            else:
                pattern_match = False
                break
    return pattern_match
        

def decode_date_range(users_input):
    """Should look like the following formats for this to decode 
       the user's input.
       1/10/2013-      # from january 10, 2013 until end of epoch time
       -1/10/2013      # from beginning of epoch time until Jan 10, 2013
       1/10/2013-1/1/2014 #from Jan 10, 2013 to Jan 1, 2014 inclusive
       All date ranges are inclusive of the end points
    """
    users_input = users_input.strip() #whack off leading and trailing whitespace
    print 'users_input', users_input
    #define constants representing the beginning and end of Arrow Time

    if users_input[0] == '-':
        #format is -<date>
        try:
            ending_date = arrow.get(*mod_date(users_input[1:])).ceil('day')
        except ValueError:
            #date didn't decode correctly, use today's instead.
            die_with('bad date entered -> %s, program aborting' %
                      users_input[1:])
        return DATE_RANGE(BEGIN_TIME, ending_date) 
    elif users_input[-1] == '-':
        #format is <date>-
        try:
            starting_date = arrow.get(*mod_date(users_input[0:-1])).floor('day')
        except ValueError:
            die_with('bad date entered -> %s, program aborting' %
                     users_input[0:-1])
        return DATE_RANGE(starting_date, END_TIME)
    else:
        #format better be <date> - <date>
        user_start, user_end = users_input.split('-')
        try:
            starting_date = arrow.get(*mod_date(user_start.strip())).floor('day')
            ending_date =   arrow.get(*mod_date(user_end.strip())).ceil('day')
        except ValueError:
            die_with('bad starting or ending dates: start->%s end->%s' %
                     (user_start, user_end))
        return DATE_RANGE(starting_date, ending_date)


def add_date_filter(func, date_range=None):
    """Wrap some date range checking stuff around the filter function
       so that we can test based on the date of the file as well.
    """
    date_range = date_range.lower()
    #boolean to indicate we are going to use a range check for
    #determining if we will keep a file.  Otherwise, we'll use a
    #tuple check when we do not care about the month, day or year.
    use_date_range = True
    if date_range == 'today':
        date_span = last_x_days(num_days=0)
    elif date_range == 'last_week':
        date_span = last_x_days(num_days=7)
    elif date_range == 'last_month':
        date_span = last_x_days(num_days=30)
    elif date_range == 'last_year':
        date_span = last_x_days(num_days=365)
    elif date_range == 'this_year':
        date_span = days_since_beginning_of_year()
    elif date_range == 'this_month':
        date_span = days_since_beginning_of_month()
    #handle the -14d format
    elif date_range.strip()[0] == '-' and date_range.strip()[-1] == 'd':
        try:
            date_span = last_x_days(num_days=int(date_range.strip()[1:-1]))
        except ValueError:
            print('Invalid number of days -- now using the last 7 days')
            date_span = last_x_days(num_days=7)
    #handle date ranges with a leading or trailing "-" sign.
    elif date_range.find('-') >= 0:
        date_span = decode_date_range(date_range)
    #find all files on this month/day/year wildcard expression
    elif date_range.find('*') >= 0:
        use_date_range = False
        mmddyy = extract_3_tuple(date_range) #returns a tuple (mm, dd, yy)
    #handle a single date
    else:
        date_span = single_day(date_range)

    if use_date_range:
        print('Files must be')
        if date_span.begin_date == BEGIN_TIME:
            print('    before this date -> %s' % date_span.end_date)
        elif date_span.end_date == END_TIME:
            print('    after this date -> %s' % date_span.begin_date)
        else:
            print('between %s ... %s' % (date_span.begin_date, date_span.end_date))
    else:
        print 'file''s date must match this date template', date_range


    def add_date_test(*args, **kwargs):
        #
        if use_date_range:
            file_datestamp = arrow.get(args[0].date)
            if date_span.begin_date <= file_datestamp <= date_span.end_date:
                return True and func(*args, **kwargs)
            else:
                return False and func(*args, **kwargs)
        else:
            file_tuple = extract_epoch_mmddyy(args[0].date)
            return compare3tup(mmddyy,file_tuple) and func(*args, **kwargs)
    return add_date_test


def get_file_id(db_id):
    """Use the memory database to select which file we want to process
       by getting the input database id number that is returned when
       you list by "paths" or "machines".  Also do any error correction
       that you might need to do to make things go better.
    """
    print('looking for id key -> %s' % db_id)
    #load our memory of ids mapping to dictionaries
    memory = get_datastore_list(key='memory_bank')
    if len(memory) == 0:
        die_with('Must run command with -l paths or -l machines first')

    #convert the string db_id to numeric
    try:
        num_id = int(db_id)
    except ValueError:
        die_with('--use option not convertable to int -> %s' % db_id)
       

    #now match the id to the dictionary with the results we are looking for.
    foundit = False
    for key in memory:
        if key[0] == num_id:
            foundit = True
            redis_key = key[1]
            break

    if foundit:
        if CONNECTION.exists(redis_key):
            return redis_key
        else:
            die_with("key ->%s, does not exist in redis database" % redis_key)
    else:
        die_with('Unable to find ID=%d. Run -l paths and try again' % num_id)

def breakout_key(datastore_key_value):
    """Given a key and value from our FileFind datastore, convert it into
       a named tuple so that we can access the components easily.
    """
    #format of NT is filename, path, name, extension, size, sha1, date
    #the format of the datastore_key_value is:
    #it will be a 2-tuple formatted like
    #(filename,[size, sha1, & date])
    datastore_key, datastore_value = datastore_key_value
    size, sha1, date = datastore_value
    path, leaf = os.path.split(datastore_key)
    name, ext = os.path.splitext(leaf)

    file_info = FILE_INFO(path, name, ext, size, sha1, date)
    return file_info


def run_query(database_key = None,
             filter_by = None,
             worker_filter = None):
    """ Iterate over the entire database
        filter_by = function used to test for inclusion in the report.
        worker_filter = secondary function used to peek inside of
                        file that passes the filter_by test.  This is
                        a second bite at the apple so to speak.
    """
    #namedtuple meta_info defined at top of program.

    #get our persistant file directory database path.
    datastore = get_datastore_dict(key=database_key)

    #tally the results of the search in query_results
    query_results = []
    print('Begin retrieving data from database ->%s' % database_key)
    count_all = 0
    count_results = 0
    #iterate over every key in the database. I.e. all files in os system.
    for key in datastore.iteritems():
        #key will be a 2-tuple of
        #(filename,[size, sha1, & date])
        count_all += 1
        file_info = breakout_key(key)  # break the key into a named_tuple 
        if filter_by(file_info):  #big sword
            if worker_filter(file_info):  #peek inside, if desired.
                count_results += 1
                query_results.append(file_info)  #5, file_info object

        #show a pulse to the outside world
        if count_all % 100000 == 0:
            print('Processed %d files' % count_all)

    #some diagnostic data on what we found.
    print('Read through a database of %d filenames' % count_all)
    print('Selected %d records for inclusion in final report' % count_results)
    
    return query_results


def sort_table(table, cols):
    """ sort a namedtuple by fields within the namedtuple
        table: a list of lists (or tuple of tuples) where each inner list 
               represents a row
        cols:  a list (or tuple) specifying the column names to sort by
               Allowed column names are:
                   any
                   name
                   path
                   ext
                   sha1
                   size
                   date
               e.g. to sort the FILE_INFO namedtuple by name ascending and 
                    filesize descending, deliver this tuple to cols
               ('name+', 'size-')
    """
    for col in reversed(cols):
        direction = col[-1:]
        column_name = col[:-1]
        get_col = {'name': lambda : attrgetter('name'),
                   'any': lambda: attrgetter('path', 'name', 'ext'),
                   'path': lambda: attrgetter('path'),
                   'ext': lambda : attrgetter('ext'),
                   'sha1': lambda : attrgetter('sha1'),
                   'size': lambda : attrgetter('size'),
                   'date': lambda : attrgetter('date'),
                  }
        print('sort by direction & column', direction, column_name)
        sort_order = (direction != '+')
        table = sorted(table, reverse=sort_order, key=get_col[column_name]())
    return table


def has_arg(args, name):
    """Makes the code look cleaner, but all it does is check to see if the
       argument with the given "name" exists in our docopt dictionary.
    """
    return args[0][name] is not None

def query_mode(*args, **kwargs):
    """This is the mode where we are reviewing the results of our file walk
       and are displaying the results to the user."""
    print(args, kwargs)
    print("Querying our file system")

    #First, handle reports that are created with the --list options, namely:
    # keys, paths, machines. terminate after listing the user's requests

    if args[0]['--list'] is not None:
        #dispatcher[args[0]['--list']](args)
        list_redis_keys(args)
        #now call it quits
        sys.exit(0)

    #next, look to see if the request is of the type where you are specifying
    #arguments like name, size, date, etc. etc.  If so build up the functions
    #that will be processing the filename and finally run those functions 
    #against the filenames returned by our generator.

    #wrap our default_filter_function with "filename" tests where they exist
    filter_function = default_filter_function
    work_filter = default_filter_function
    if has_arg(args, '--name'):  # the basename portion
        #wrap the default filter_function with the add_name_filter
        filter_function = add_name_filter(filter_function,
                                          name_values = args[0]['--name'],
                                          file_part = 'name')
    if has_arg(args, '--path'):
        filter_function = add_name_filter(filter_function,
                                          name_values = args[0]['--path'],
                                          file_part = 'path')

    if has_arg(args, '--any'):
        filter_function = add_name_filter(filter_function,
                                          name_values = args[0]['--any'],
                                          file_part = 'any')
      
    if has_arg(args, '--ext'):
        filter_function = add_name_filter(filter_function,
                                          name_values = args[0]['--ext'],
                                          file_part = 'ext')

    if has_arg(args, '--size'):
        filter_function = add_size_filter(filter_function,
                                          size_range = args[0]['--size'])

    if has_arg(args, '--date'):
        filter_function = add_date_filter(filter_function,
                                          date_range = args[0]['--date'])

    if has_arg(args, '--work'):
        work_filter = add_work_filter(work_filter,
                                      function_name = args[0]['--work'])


    #pick the redis datastore "path" to search against
    if has_arg(args, '--use'):
        query_this_path = get_file_id(args[0]['--use'])

    #run query against our database and return successful candidates
    query_results =  run_query(database_key = query_this_path,
                               filter_by = filter_function,
                               worker_filter = work_filter)

    print('Sorting the final report')
    sorted_results = sort_table(query_results, ('path+','name+', 'date+',))
    #print 'sorted_results', sorted_results
    #list_these = get_datastore_dict(key=query_against)
    for index, key in enumerate(sorted_results):
        abc = arrow.get(key.date)
        print(abc.datetime)
        print('key', key)
        print('%d -> %s' % (index, arrow.get(key.date)))


    #run the final program


def process_arguments(args):
    """ Take the user's input specified on the command line and make sure it
        makes sense. If it doesn't then abort the program from going further.
        The values in the process_arg dictionary must match the args talked
        about in the module doc string (which is also processed by docopt).
        It the --build option is present then the program is assume to be
        doing a build request otherwise it will do a query request.
    """
    #print(arrow.utcnow().ceil('day'))
    process_arg = {"--build": review_build_arg,
                   "--computer": review_name_arg,
                   "--list": review_list_arg,
                   "--name": dummy_review,
                   "--any": dummy_review,
                   "--path": dummy_review,
                   "--ext": dummy_review,
                   "--use": dummy_review,
                   "--size": dummy_review,
                   "--date": dummy_review,
                   "--work": dummy_review,
                  }
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
