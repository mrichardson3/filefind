#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 Mark Richardson.
#
# pylint: disable=E1103,F0401
"""Usage: filefind.py
          filefind.py [--build dir -c computer_name ]

-b <dir> --build <dir>    Walk the directory of the file system storing the
                          results in our redis datastore.
-c <name> --computer <name>    Set the name of the computer
"""

__version__ = 'Version 0.1'

import os
import sys
import docopt
import platform
import python3_timer


def die_with(error_message):
    """If you find an unrecoverable error. send the final dying gasp here
       and we'll broadcast it and then kill the program.
    """
    print("Fatal Error")
    print("    %s" % error_message)
    sys.exit(2)


def review_build_arg(args, key):
    """ Is the directory handed in by the user a valid file directory?"""
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
    """
    machine_name = args[key]
    if machine_name is None:
        #use the computer to get the machines name.
        args[key] = (platform.uname()[1])
        machine_name = args[key]
    else:
        pass
    print("Machine name for this run is -> %s" % machine_name)


def build_mode(*args, **kwargs):
    """This is the command to walk a file system and store the results
       of the walk in our redis datastore."""
    print(args, kwargs)
    file_system = args[0]['--build']
    machine_name = args[0]['--computer']
    print("\n\n***********************************")
    print("Beginning build process")
    print("***********************************")
    print("For Machine named -> %s" % machine_name)
    print("Now beginning walk of this directory path -> %s" % file_system)


def query_mode(*args, **kwargs):
    """This is the mode where we are reviewing the results of our file walk
       and are displaying the results to the user."""
    print(args, kwargs)
    print("Querying our file system")


def process_arguments(args):
    """ Take the user's input specified on the command line and make sure it
        makes sense. If it doesn't then abort the program from going further.
        The values in the process_arg dictionary must match the args talked
        about in the module doc string (which is also processed by docopt).
    """
    process_arg = {"--build": review_build_arg,
                   "--computer": review_name_arg}
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
