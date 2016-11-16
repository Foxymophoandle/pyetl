'''
Created on Jul 29, 2014

@author: davide
'''
import os
import sys
import argparse
import logging.handlers

sys.path.append(os.path.join(os.path.dirname(__file__), '../')) # Needed for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '../../')) # Needed for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../')) # Needed for imports

from pyetl.working_stations_manager import WorkersManager


def parse_arguments(args):
    """Handles input arguments
    """
    parser = argparse.ArgumentParser(description='''Activates the synchronization specified in the config file''')
    parser.add_argument('config_file', help='Path to the configuration ini file')
    parser.add_argument('-f', dest='logfile', help='Path to the file where to log (default is stream)')
    parser.set_defaults(logfile=None)
    parser.add_argument('-m', dest='max_proc', help='Maximum number of processes that the synchronization can spawn (default = 30)')
    parser.set_defaults(max_proc=30)
    parser.add_argument('-l', dest='max_proc_life', help='Maximum seconds a process can live (default=3600 (1h))')
    parser.set_defaults(max_proc_life=3600)
    parser.add_argument('-a', dest='mail_to', nargs='*', help='Address to send mail to in case of exceptions')
    parser.set_defaults(mail_to=['davide.favero@netstorming.net'])
    parser.add_argument('-v', '--verbose', help='Activates more logging (may spam)', action='store_true')
    parser.set_defaults(verbose=False)
    return parser.parse_args(args)

def init_log_file(log_path):
    logfile = logging.getLogger("logfun")
    logfile.setLevel(logging.DEBUG)
    h = logging.handlers.RotatingFileHandler(log_path, 'a+', 0, 10)
    f = logging.Formatter("[%(asctime)s] - %(levelname)s - %(message)s")
    h.setFormatter(f)
    logfile.addHandler(h)
    logfile.handlers[0].doRollover()
    return logfile

def start_manager(args):
    """Activates the workers specified in the .ini file
    """
    args = parse_arguments(args)
    if args.logfile:
        args.logfile = init_log_file(args.logfile)
    manager = WorkersManager(config_file=args.config_file, 
                             logfile=args.logfile,
                             max_active_processes=args.max_proc,
                             verbose=args.verbose,
                             max_process_life=args.max_proc_life,
                             mail_to=args.mail_to)
    manager.start()

if __name__ == '__main__':
    start_manager(sys.argv[1:])