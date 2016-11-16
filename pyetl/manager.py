'''
Created on Feb 5, 2014

@author: davide
'''
import sys
import time
from multiprocessing import Process, Queue
from Queue import Empty

from pyetl._utils import init_workers_from_file, init_stream_log, \
    init_workers_from_dict
from pyetl.handlers import QueuedWorker, QueuedWorkerException

class ManagerException(Exception):
    """Used to wrap worker exceptions and forward them
    """
    pass

class WorkersManager(object):
    '''
    Handles the starting of each Worker in a different process
    with a shared Queue to store every worker result
    '''
    def __init__(self, config_file=None, logfile=None, max_active_processes=30, verbose=False):
        self.workers = set()
        self.registered_workers = []
        self.started_workers = set()
        self.workers_done = set()
        self.data_collected = dict()
        self.active_workers = 0
        self.task_queue = Queue()
        self.logfile = logfile if logfile else init_stream_log()
        if config_file: 
            self.init_workers_from_file(config_file)
        self.max_active_processes = max_active_processes
        self.verbose = verbose
    
    def start(self, global_params=None):
        '''
        Initialize every ready worker in a dedicated process.
        Exits when all stored workers are running
        '''
        start = time.time()
        self.registered_workers = set([worker.name for worker in self.workers])
        
        self.logfile.info("manager - APP STARTING WITH: %s" % global_params)
        if self.verbose:
            self.logfile.debug("manager - WORKERS REGISTERED: %s" % self.registered_workers)
        self.data_collected['global_params'] = global_params
        try:
            while len(self.started_workers) < len(self.workers):
                for worker in self.workers:
                    assert isinstance(worker, QueuedWorker)
                    if worker._can_start(self.workers_done) and \
                     worker.name not in self.started_workers and \
                     self.active_workers < self.max_active_processes:
                        self.__start_worker(worker)
                try:
                    signals = self.task_queue.get_nowait()
                except Empty:
                    continue
                else:
                    for worker, signal in signals.iteritems():
                        self.active_workers -= 1
                        self.workers_done.add(worker)
                        if isinstance(signal, Exception):
                            raise QueuedWorkerException(signal)
                        self._report_workers_status()
                    self.data_collected.update(signals)
            self.logfile.info("manager - ALL WORKERS LAUNCHED")
            self._report_workers_status()
        except QueuedWorkerException, exc:
            self.logfile.critical("manager - STARTING CLEAN EXIT")
            self.__clean_exit()
            raise ManagerException(exc)
        finally:
            self.logfile.info("manager - ELAPSED: %.4f s" % (time.time() - start))
            self.logfile.info("manager - CLOSING APP")
            sys.exit()
    
    def __start_worker(self, worker):
        """Given a QueuedWorker object, starts it in a separate
        process with its needed data
        """
        self.active_workers += 1
        self.started_workers.add(worker.name)
        self._report_workers_status()
        data = worker.get_needed_data(self.data_collected)
        work_process = Process(target=worker._run, args=(data,))
        work_process.start()
        
    def init_workers_from_file(self, fname):
        '''
        Given an .ini file, initializes every worker specified
        assigning the queue instance where to put results
        '''
        worker_objs = init_workers_from_file(fname, self.logfile)
        for worker in worker_objs:
            worker.output_queue = self.task_queue
            self.workers.add(worker)
    
    def init_workers_from_dict(self, dict_config):
        '''Given a configuration dict, initializes every worker specified
        '''
        worker_objs = init_workers_from_dict(dict_config, self.logfile)
        for worker in worker_objs:
            worker.output_queue = self.task_queue
            self.workers.add(worker)
            
    def _report_workers_status(self):
        '''Gives some debug informations about active workers
        '''
        if self.verbose:
            still_working = self.started_workers - self.workers_done
            registered_workers = set([worker.name for worker in self.workers])
            missing_workers = registered_workers.difference(self.started_workers)
            self.logfile.debug("manager - WORKING: %s" % [worker for worker in still_working])
            self.logfile.debug("manager - WORKERS MISSING: %s" % [worker for worker in missing_workers])
    
    def __clean_exit(self):
        '''
        Needed to wait and close every process spawned with the main one.
        '''
        remaining_workers = self.started_workers - self.workers_done
        while len(remaining_workers) > 0:
            try:
                signals = self.task_queue.get_nowait()
            except Empty:
                self.logfile.info("manager - WAITING SOME WORKERS TO FINISH")
                time.sleep(2)
                self._report_workers_status()
                continue
            else:
                for worker in signals.keys():
                    self.workers_done.add(worker)
                remaining_workers = self.started_workers - self.workers_done
                continue
        