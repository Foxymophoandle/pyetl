'''
Created on Sep 15, 2014

@author: davide
'''
from Queue import Empty
from exceptions import OSError
from multiprocessing import Process, Queue
import os
import signal
import sys
import time
import traceback

from _utils import init_workers_from_file, init_stream_log, \
    REPORT_WAIT_INTERVAL, sendmail, TO_ADDRESS, _WORKER_SPLIT_FLAG
from handlers import QueuedWorker, QueuedWorkerException, _DatabaseHandler
from pytree.tree import Tree


class WorkingStation(object):
    """Represent a working station that can be occupied by an
    object to do its job
    """
    def __init__(self):
        self.free = True
        self.guest = None
        self.occupy_time = None
    
    def allocate(self, guest):
        """Method to call when a station is taken
        """
        self.free = False
        self.guest = guest
        self.occupy_time = time.time()
    
    def release(self):
        """Method to call when a station is released
        """
        self.free = True
        self.guest = None
        self.occupy_time = None
    
    def __str__(self):
        if self.free:
            return "IDLE"
        else:
            elapsed = (time.time() - self.occupy_time)
            return "%s - working for %.4f s" % (str(self.guest), elapsed)


class CannotAllocateStationsException(Exception):
    """Raised when we expect to have a free station
    but we failed
    """


class NonExistentGuestException(Exception):
    """Raised when trying to release a station for a non 
    registered guest
    """
    
    
class WorkingStationPool(object):
    """Represent a set of WorkingStation objects
    providing methods to analyze status
    """
    def __init__(self, pool_size=10):
        self.pool = []
        self.__init_pool(pool_size)
        self.__registry = dict()

    def __init_pool(self, pool_size):
        """Initializes the stations pool
        """
        for _ in xrange(int(pool_size)):
            self.pool.append(WorkingStation())

    def are_there_free_stations(self):
        """True if there's at least one free working station
        """
        return any([station.free for station in self.pool])

    def is_someone_working(self):
        """True if there's at least one working station active
        """
        for station in self.pool:
            if not station.free:
                return True
        return False
    
    def allocate_first_free_station(self, guest):
        """Returns the first free WorkingStation in the pool 
        """
        for station_id, station in enumerate(self.pool):
            if station.free:
                station.allocate(guest)
                self.__registry[str(guest)] = station_id
                return
        raise CannotAllocateStationsException()
    
    def release_station_by_guest(self, guest):
        """Given a guest it gets the index of
        the station where he was working and releases it
        """
        try:
            station_id = self.__registry[str(guest)]
        except KeyError:
            raise NonExistentGuestException()
        else:            
            station = self.pool[station_id]
            station.release()
    
    def report_status(self, logfile):
        """Reports the status of all stations connected
        """
        for station_id, station in enumerate(self.pool):
            if not station.free:
                logfile.info("working_pool - STATION %s - %s" % (str(station_id), str(station)))
    
    def get_working_guests(self):
        """
        Returns a list of guests that are still working and their
        occupy time for each one of them
        """
        occupied_stations = self.get_occupied_stations()
        return [(station.guest, station.occupy_time) for station in occupied_stations]
    
    def get_occupied_stations(self):
        """
        Returns the stations that are occupied by an active guest
        """
        occupied_stations = []
        for station in self.pool:
            if not station.free:
                occupied_stations.append(station)
        return occupied_stations
                

class ManagerException(Exception):
    """Used to wrap worker exceptions and forward them
    """
    pass


class WorkerTimeoutException(Exception):
    """
    Raised when a Worker is killed by the Manager
    """
    pass


class WorkerHasNoPIDException(Exception):
    """
    Raised when a worker could not communicate its pid.
    """
    pass


class WorkersManager(object):
    """Represent the manager which is responsible of making
    all workers execute in the configured order using all
    working stations available and storing their data
    in a common queue
    """
    def __init__(self, config_file=None, logfile=None, max_active_processes=30, verbose=None, max_process_life=3600,
                 mail_to=TO_ADDRESS):
        self.working_stations_pool = WorkingStationPool(max_active_processes)
        self.verbose = verbose
        self.workers = set()
        self.workers_tree = None
        self.task_queue = Queue()
        self.worker_db_pid_control_queue = Queue()
        self.completed_jobs = dict()
        self.workers_registry = dict()
        self.pid_by_worker_name = dict()
        self.connection_pid_by_worker_name = dict()
        self.logfile = logfile if logfile else init_stream_log()
        if config_file: 
            self.init_workers_from_file(config_file)
        self.max_process_life = int(max_process_life)
        self.mail_to = mail_to
        self.exceptions = dict()
        self.workers_queue = None

    def start(self, global_params=None):
        """Prepares the workers queue, handles global parameters and exceptions
        """
        start = time.time()
        self.workers_queue = self.build_workers_execution_order()
        if self.verbose:
            self.logfile.debug('manager - EXECUTION ORDER: %s' % self.workers_queue)
        self.completed_jobs['global_params'] = global_params
        try:
            self.__run_queue()
        except Exception, exc:
            self.logfile.exception(exc)
            self.logfile.critical("manager - STARTING CLEAN EXIT")
            self.__clean_exit()
            raise exc
        finally:
            self.__clean_exit()
            self.logfile.info("manager - ELAPSED: %.4f s" % (time.time() - start))
            self.logfile.info("manager - CLOSING")
            if len(self.exceptions) > 0:
                self.__send_mail()
            sys.exit()
    
    def __run_queue(self):
        """Manages all workers
        """
        cycles_start = time.time()
        while len(self.workers_queue) > 0:
            workers_started = False
            if self.working_stations_pool.are_there_free_stations():
                next_worker_position = self.get_next_in_line(self.workers_queue)
                if next_worker_position >= 0:
                    ready_worker = self.workers_queue.pop(next_worker_position)
                    self.__start_worker(ready_worker)
                    workers_started = True
            try:
                self.__may_collect_db_pid()
                self.__may_collect_completed_jobs()
                self.__may_terminate_locked_processes()
            except (QueuedWorkerException, WorkerTimeoutException):
                self.__remove_dead_branches()
            
            now = time.time()
            if now - cycles_start >= REPORT_WAIT_INTERVAL and not workers_started:
                self.logfile.info('manager - STILL WORKING...')
                self.working_stations_pool.report_status(self.logfile)
                cycles_start = time.time()
                if not self.working_stations_pool.is_someone_working():
                    break
                
        self.logfile.info("manager - ALL WORKERS LAUNCHED")
        self.working_stations_pool.report_status(self.logfile)
    
    def get_next_in_line(self, workers_queue):
        """Returns the first worker ready to start in the queue line
        """
        for worker_position, worker in enumerate(workers_queue):
            if worker._can_start(set(self.completed_jobs)):
                return worker_position
        return -1

    def __start_worker(self, worker):
        """Given a QueuedWorker object, starts it in a separate
        process with its needed data
        """
        self.logfile.debug("manager - STARTING %s" % worker.name)
        assert isinstance(worker, QueuedWorker)
        self.logfile.debug("manager - STARTING %s" % worker.name)
        self.working_stations_pool.allocate_first_free_station(worker)
        data = worker.get_needed_data(self.completed_jobs)
        work_process = Process(target=worker._run, args=(data,))
        work_process.start()
        self.pid_by_worker_name.setdefault(worker.name, work_process.pid)

    def __may_collect_db_pid(self):
        """
        DB workers will communicate their connection pid once they have a connection made.
        We need to store those by worker name in order to destroy connections in case of kill
        """
        try:
            signals = self.worker_db_pid_control_queue.get_nowait()
        except Empty:
            pass
        else:
            for worker_name, db_pid in signals.iteritems():
                self.connection_pid_by_worker_name.setdefault(worker_name, db_pid)

    def __may_collect_completed_jobs(self):
        """Tries to get results from the queue. If there's nothing
        to get, just returns else it stores data coming from the worker
        in the completed_jobs with the worker name as a label
        """
        try:
            signals = self.task_queue.get_nowait()
        except Empty:
            pass
        else:
            for worker, response in signals.iteritems():
                try:
                    self.working_stations_pool.release_station_by_guest(worker)
                except NonExistentGuestException:
                    pass
                if isinstance(response, Exception):
                    self.exceptions.setdefault(worker, dict())
                    self.exceptions[worker]['exception'] = response
                    if self.verbose:
                        self.logfile.debug('manager - EXCEPTIONS: [%s]' % self.exceptions)
                    raise QueuedWorkerException(response)
                if response and type(response) is dict and _WORKER_SPLIT_FLAG in response:
                    # If I have this, it means that a worker has split its results
                    # and all its partial sub workers have been collected
                    # so I need to join results into the main worker once more
                    self.logfile.info('manager - JOINING RESULTS FOR WORKER [%s] TAKING WORKERS [%s]' % (worker,
                                                                                                         response))
                    joined_data = self.__rejoin_split_workers_results(response[_WORKER_SPLIT_FLAG])
                    self.completed_jobs.update({worker: joined_data})
                else:
                    self.completed_jobs.update({worker: response})

    def __rejoin_split_workers_results(self, sub_workers):
        """
        If a worker is split, we need to rejoin all data into the main worker name
        we use the result stored in the _WORKER_SPLIT_FLAG to identify all the sub workers.
        Data may come in form of a dictionary or a list so we handle both cases and return the one populated.
        We assume here that all sub workers are returning the same type of result
        """
        dict_joined_data = dict()
        list_joined_data = []
        for sub_worker in sub_workers:
            if type(self.completed_jobs[sub_worker]) is dict:
                dict_joined_data.update(self.completed_jobs[sub_worker])
            elif type(self.completed_jobs[sub_worker]) is list:
                list_joined_data.extend(self.completed_jobs[sub_worker])
            self.completed_jobs.pop(sub_worker)
        if len(dict_joined_data) > 0:
            return dict_joined_data
        else:
            return list_joined_data

    def __remove_dead_branches(self):
        """
        When a worker fails, we need to remove all workers who were waiting for him.
        In fact removing the worker's sons from the execution queue
        """
        for worker_name in self.exceptions.keys():
            self.exceptions[worker_name].setdefault('removed_workers', [])
            self.logfile.info('manager - WORKER [%s] FAILED. REMOVING HIS SONS' % worker_name)
            dead_worker = self.workers_registry.get(worker_name, None)
            if dead_worker is not None:
                dead_worker_sons = self.workers_tree.get_ancestors(dead_worker)
                self.exceptions[worker_name]['removed_workers'].append(dead_worker.name)
                for worker_to_remove in dead_worker_sons:
                    try:
                        self.workers_queue.remove(worker_to_remove)
                        self.exceptions[worker_name]['removed_workers'].append(worker_to_remove.name)
                        self.logfile.warning('manager - REMOVING WORKER [%s] BECAUSE IS WAITING ON [%s] THAT FAILED' % (worker_to_remove.name, worker_name))
                    except ValueError:
                        continue

    def __may_terminate_locked_processes(self, report=False):
        """
        I will check who is working for too much time and kill 
        the process if I see it. We may need to kill the connection backend
        in case there is an open db connection
        """
        guests_working = self.working_stations_pool.get_working_guests()
        now = time.time()
        for worker, worker_start_istant in guests_working:
            worker_pid = self.pid_by_worker_name.get(worker.name, None)
            if now - worker_start_istant >= self.max_process_life:
                self.logfile.warning('manager - Worker %s is active for %s seconds. KILLING IT (procpid: %s)' % (worker.name, now - worker_start_istant, worker_pid))
                try:
                    self._terminate_process(worker)
                    self.working_stations_pool.release_station_by_guest(worker)
                    self.exceptions.setdefault(worker, dict())
                    self.exceptions[worker]['exception'] = WorkerTimeoutException()
                    raise WorkerTimeoutException()
                except (TypeError, OSError):
                    pass
            elif report:
                self.logfile.warning('manager - %s: [pid: %s] working for %s seconds' % (worker.name, worker_pid, now - worker_start_istant))

    def _terminate_process(self, worker):
        """
        before killing the process we check if there is still an open connection in a db
        and kill that first
        """
        assert isinstance(worker, QueuedWorker)
        worker_name = worker.name
        worker_pid = self.pid_by_worker_name.get(worker_name, None)
        if worker_pid is not None:
            if isinstance(worker, _DatabaseHandler):
                db_connection_pid = self.connection_pid_by_worker_name.get(worker_name, None)
                self.logfile.warning('manager - Cancelling db backend pid[%s] for worker [%s]' % (db_connection_pid, worker_name))
                worker.cancel_backend(db_connection_pid)
                self.logfile.warning('manager - Done')
            os.kill(worker_pid, signal.SIGQUIT)
        else:
            raise WorkerHasNoPIDException(worker.name)

    def init_workers_from_file(self, fname):
        '''
        Given an .ini file, initializes every worker specified
        assigning the queue instance where to put results
        '''
        self.workers = init_workers_from_file(fname, self.logfile)
        self.workers_registry = dict()
        for worker in self.workers:
            worker.output_queue = self.task_queue
            if hasattr(worker, 'db_pid_control_queue'):
                worker.db_pid_control_queue = self.worker_db_pid_control_queue
            self.workers_registry[worker.name] = worker

    def build_workers_execution_order(self):
        """Performs a check so that the workers chain configured 
        will come to an end, eventually removing dead branches.
        Sets the workers tree attribute and returns a list of workers
        that is the complete ordered walk of the tree
        """
        workers_tree = Tree()
        workers_dict_tree = dict()
        workers_to_be_removed = []
        for worker in self.workers:
            workers_dict_tree.setdefault(worker, [])
            for worker_name in worker.waiting_for:
                try:
                    workers_dict_tree[worker].append(self.workers_registry[worker_name])
                except KeyError:
                    workers_to_be_removed.append(worker_name)
                    workers_dict_tree[worker].append(worker_name)
        workers_tree.parse_dict(workers_dict_tree)
        workers_tree = self.prune_workers(workers_tree, workers_to_be_removed)
        waiting_chains = workers_tree.reverse()
        self.workers_tree = workers_tree
        if self.verbose:
            self.logfile.debug('manager - WORKERS TREE: %s' % self.workers_tree)
        return waiting_chains.walk()
    
    def prune_workers(self, workers_tree, workers_to_be_removed):
        """I will remove all workers who are not waited by anyone (unwanted)
        or all workers that are waiting on someone who does not exists (deluded)
        """
        workers_tree = self.__remove_unwanted_workers(workers_tree)
        workers_tree = self.__remove_deluded_workers(workers_tree, workers_to_be_removed)
        return workers_tree
    
    def __remove_unwanted_workers(self, workers_tree):
        """Unwanted workers have no family, they have no ancestors and no progeny
        """
        assert isinstance(workers_tree, Tree)
        for worker in self.workers:
            ancestors = workers_tree.get_ancestors(worker)
            progeny = workers_tree.get_progeny(worker)
            if len(ancestors) == 0 and len(progeny) == 0:
                self.logfile.warning("REMOVING WORKER %s. NOONE WANTS HIM" % worker)
                workers_tree.remove(worker)
        return workers_tree
    
    def __remove_deluded_workers(self, workers_tree, workers_to_be_removed):
        """Deluded workers are those that are waiting for someone
        who does not exists
        """
        assert isinstance(workers_tree, Tree)
        for worker in self.workers:
            bad_son = False
            progeny = workers_tree.get_progeny(worker)
            for son in progeny:
                if son in workers_to_be_removed:
                    bad_son = True
            if bad_son:
                self.logfile.warning("REMOVING WORKER %s. HE'S DELUDED" % worker)
                workers_tree.remove(worker)
        return workers_tree

    def __clean_exit(self):
        '''
        Needed to wait and close every process spawned with the main one.
        '''
        while self.working_stations_pool.is_someone_working():
            try:
                self.__may_collect_completed_jobs()
                self.logfile.warning('manager - processes still running...')
                self.__may_terminate_locked_processes(report=True)
            except (QueuedWorkerException, WorkerTimeoutException):
                pass
            time.sleep(REPORT_WAIT_INTERVAL)
    
    def __send_mail(self):
        """
        If we are here, it means that one or more workers exploded.
        We need to gather all exceptions and build a report to send by mail
        """
        msg = 'PYETL FAILED WITH ARGUMENTS:\n'
        msg += ' '.join(sys.argv[1:]) + '\n'
        msg += '\nEXCEPTIONS RAISED: \n'
        for worker, error_info in self.exceptions.items():
            msg += '\nWORKER:\t %s\n' % worker
            exc_lines = traceback.format_exception(error_info['exception'].__class__, error_info['exception'], tb=None)
            msg += exc_lines[0]
            if len(error_info['removed_workers']) > 0:
                msg += 'REMOVED WORKERS:\n'
                msg += ', '.join(error_info.get('removed_workers', []))
                msg += ' \n'
        sendmail(msg, mail_to=self.mail_to)
