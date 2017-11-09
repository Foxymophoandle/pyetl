# -*- coding: utf-8 -*-
"""
Created on Feb 10, 2014

@author: davide
"""
import cPickle
import copy
import datetime
import os
import sys
import time
from abc import ABCMeta, abstractmethod

from psycopg2 import connect, extras, extensions

from _utils import merge_data_by_keys, SELECT_SQL, invert_dict, get_insert_query_and_values, \
    get_update_query_and_values, init_object_from_dict, DBINFO, str_to_bool, chunks, _WORKER_SPLIT_FLAG

try:
    import xlsxwriter
except ImportError:
    pass


class _ConfigurableObject(object):
    """
    Abstract class to represent an object that can init 
    some or all of its attributes starting from a configuration dictionary
    """
    __meta__ = ABCMeta

    @abstractmethod
    def config_from_dict(self, config_dict, name):
        """
        Given a dictionary, inits its attributes. Must be overridden
        """
        pass


class _Semaphore(object):
    """
    Abstract class that can be used as a synchronization between
    workers competing for the same resource
    """
    __meta__ = ABCMeta

    @abstractmethod
    def lock(self):
        """
        Locks the resource
        """
        pass

    @abstractmethod
    def unlock(self):
        """
        Removes the lock
        """
        pass

    @abstractmethod
    def is_locked(self):
        """
        Returns if the resource is locked or not
        """
        pass


class QueuedWorkerException(Exception):
    """Used to forward Exceptions raised by a QueuedWorker running
    """
    pass


class QueuedWorker(_ConfigurableObject):
    """
    Represents a working class which can wait on other workers to finish.
    Uses a Queue object (global) to store results. its name is the
    dictionary key and the output is the value
    """

    def __init__(self, name, log):
        self.name = name
        self.waiting_for = set()
        self.output_queue = None
        self.db_pid_control_queue = None
        self.log = log
        super(QueuedWorker, self).__init__()

    def __str__(self, *args, **kwargs):
        return self.name

    def __repr__(self):
        return self.__str__()

    def __hash__(self):
        return hash(self.__repr__())

    def __eq__(self, other):
        return isinstance(other, QueuedWorker) and self.name == other.name

    def _run(self, input_data):
        """
        Runs and puts the result in the shared Queue as a dict
        with its name as a key
        """
        start = time.time()
        pid = os.getpid()
        self.log.info("%s - START - Process %i" % (self.name, pid))
        data = []
        try:
            data = self.do_work(input_data)
        except Exception, exc:
            data = exc
            self.log.error('%s - RAISED EXCEPTION' % self.name)
            self.log.exception(exc)
        finally:
            self.send_data(data)
            end = time.time()
            self.log.info("%s - END - Elapsed %.4f s" % (self.name, (end - start)))
            sys.exit()

    def send_data(self, data):
        """
        By default we add the results in a dictionary with the worker name as key
        in the output queue
        """
        self.output_queue.put({self.name: data})

    def do_work(self, input_data):
        """
        Method to implement to give behavior to a worker
        """
        raise NotImplementedError()

    def configure(self, worker_section):
        """
        May be overridden to set peculiar attributes defined in the .ini file
        """
        pass

    def _can_start(self, workers_done):
        """
        Checks that all workers which I'm waiting for are done
        """
        return len(self.waiting_for & workers_done) == len(self.waiting_for)

    def get_needed_data(self, data_collected):
        """
        Extracts all data coming from the workers I'm waiting for
        """
        needed = dict()
        for worker in self.waiting_for:
            if worker in data_collected:
                needed.update({worker: data_collected[worker]})
        return needed

    def config_from_dict(self, config_dict, name):
        """
        Sets the eventual execution order if specified in config
        """
        my_section = config_dict.get(self.name, dict())
        waiting_for = my_section.get('waiting_for', None)
        if waiting_for:
            if not hasattr(waiting_for, '__iter__'):
                waiting_for = [waiting_for]
            self.waiting_for = set(waiting_for)
        self.configure(my_section)
        super(QueuedWorker, self).config_from_dict(config_dict, self.name)


class _DatabaseHandler(_ConfigurableObject):
    """
    Inherited by all workers that should interact with a database.
    Offers methods to execute a particular read query for extracting part
    and to insert or update tables for the loading part
    """

    def __init__(self):
        self.connection = None
        self.cursor = None
        self.host = None
        self.user = None
        self.dbname = None
        self.password = None
        self.port = 5432
        self.use_split = True
        self.connection_pid = None
        super(_DatabaseHandler, self).__init__()

    def read(self, query, params=None, keys=None, is_primary=True):
        """
        Wrapper to init a connection, execute a query and return its result.
        The result can be saved as a dict if keys parameters is specified
        """
        if self.connection is None or self.cursor is None:
            self.init_connection_and_cursor()
        if params and len(params) > 0:
            self.cursor.execute(query, params)
        else:
            self.cursor.execute(query)
        data = self._get_results()
        if keys:
            data = merge_data_by_keys(data, keys, is_primary)
        self.connection.commit()
        return data

    def _get_results(self):
        if self.use_split:
            all_res = []
            for results in self.__fetch_results():
                all_res.extend(results)
            return all_res
        else:
            return self.cursor.fetchall()

    def __fetch_results(self):
        self.cursor.itersize = 1000
        while True:
            records = self.cursor.fetchmany(self.cursor.itersize)
            if records:
                yield records
            else:
                return

    def write_or_update(self, target_table, target_keys, data_to_insert, actual_data, log):
        """
        Wrapper to write a value in a database table. If the value already
        exists, performs an update, else inserts it.
        """
        updates = 0
        inserts = 0
        if self.connection is None or self.cursor is None:
            self.init_connection_and_cursor()
        for key, record in data_to_insert.iteritems():
            if key in actual_data:
                updates += 1
                query, values = get_update_query_and_values(target_table, target_keys, record)
            else:
                inserts += 1
                query, values = get_insert_query_and_values(target_table, record)
            try:
                self.cursor.execute(query, values)
                self.connection.commit()
            except Exception, exc:
                log.warning('QUERY FAILED: %s WITH PARAMS %s' % (query, values))
                log.exception(exc)
                self.connection.rollback()
            if (inserts + updates) % 2000 == 0:
                log.info("%s - %i/%i" % (target_table, (inserts + updates), len(data_to_insert)))
                self.connection.commit()
        log.info("%s  -  INSERTS: %i  -  UPDATES: %i" % (target_table, inserts, updates))
        self.connection.commit()

    def execute(self, query, params=None):
        if self.connection is None or self.cursor is None:
            self.init_connection_and_cursor()
        if params:
            self.cursor.execute(query, params)
        else:
            self.cursor.execute(query)

    def config_from_dict(self, config_dict, name):
        """
        Sets the needed database specifications as class attributes
        """
        my_section = config_dict.get(name, dict())
        self.use_split = str_to_bool(my_section.get('use_split', True))
        if 'dbinfo' in my_section:
            db_section = config_dict.get(my_section['dbinfo'], None)
            if db_section:
                self.host = db_section['host']
                self.user = db_section['user']
                self.dbname = db_section['dbname']
                self.password = db_section.get('password', None)
                self.port = db_section.get('port', 5432)
            else:
                print "ERROR CONFIGURING WORKER ", name
                print "UNEXISTENT dbinfo SPECIFIED [%s] FOR %s CONFIGURATION" % (my_section['dbinfo'], name)
        else:
            print "ERROR CONFIGURING WORKER ", name
            print "MANDATORY dbinfo OPTION MISSING FROM %s CONFIGURATION" % name
        super(_DatabaseHandler, self).config_from_dict(config_dict, name)

    def init_connection_and_cursor(self, name=None):
        """
        We init a named cursor so it will be executed at server side
        """
        connection_info = DBINFO % (self.host, self.user, self.dbname, str(self.port))
        if self.password:
            connection_info += ' password=%s' % self.password
        self.connection = connect(connection_info)
        if name:
            self.cursor = self.connection.cursor(name=name, cursor_factory=extras.DictCursor)
        else:
            self.cursor = self.connection.cursor(cursor_factory=extras.DictCursor)
        self.connection_pid = self.connection.get_backend_pid()
        self.connection.set_isolation_level(extensions.ISOLATION_LEVEL_READ_UNCOMMITTED)

    def cancel_backend(self, connection_pid):
        self.init_connection_and_cursor()
        self.execute('SELECT pg_cancel_backend(%s)' % connection_pid)


class DBSemaphore(_DatabaseHandler, _Semaphore):
    """
    Adapter class that represent a semaphore handled
    via a database table
    """

    def __init__(self):
        super(DBSemaphore, self).__init__()


class QueuedDatabaseHandler(QueuedWorker, _DatabaseHandler):
    """
    Adapter class that represents a queue-able database handler
    """
    pass


class DBExtractor(QueuedWorker, _DatabaseHandler):
    """
    Worker that extracts data from a database given
    a query and parametric filters coming from the
    manager
    """

    def __init__(self, name, log):
        self.query = None
        self.keys = None
        self.filters = None
        self.keys_are_primary = None
        super(DBExtractor, self).__init__(name, log)

    def get_needed_data(self, data_collected):
        """
        This only needs filters to execute the right query
        """
        return data_collected.get('global_params', dict())

    def do_work(self, input_data):
        """
        Builds the query and gets optional parameters values and gets
        the result from the database
        """
        if self.filters and not input_data:
            raise Exception('Worker %s requires parametric filters %s' % (self.name, str(self.filters.keys())))
        self.init_connection_and_cursor(name=self.name)
        self.db_pid_control_queue.put({self.name: self.connection_pid})
        query = self._build_query()
        params = self._get_params(input_data)
        self.log.debug('%s - %s , %s' % (self.name, query, params))
        data = self.read(query, params, self.keys, self.keys_are_primary)
        self.connection.commit()
        self.log.info("%s - DONE - %i Results" % (self.name, len(data)))
        return data

    def config_from_dict(self, config_dict, name):
        """
        Gets the query, the table primary keys and optional
        filters that are parametric
        """
        worker_section = config_dict.get(self.name, dict())
        self.query = worker_section.get('query', None)
        self.keys = worker_section.get('keys', None)
        self.keys_are_primary = str_to_bool(worker_section.get('keys_are_primary', True))
        self.filters = worker_section.get('filters', None)
        super(DBExtractor, self).config_from_dict(config_dict, self.name)

    def _build_query(self):
        """
        Given filters and the query from ini creates the right query
        """
        if self.filters:
            all_filters = copy.deepcopy(self.filters)
            all_filters = all_filters.keys()
            if 'WHERE' not in self.query:
                first_filter = all_filters.pop(0)
                self.query += ' WHERE ' + first_filter + ' ' + self.filters[first_filter]
            if len(all_filters) > 0:
                for filter_field, filter_condition in self.filters.iteritems():
                    self.query += ' AND ' + filter_field + ' ' + filter_condition
        return self.query

    def _get_params(self, data_params):
        """
        Gets the needed parameters ordered by filter
        """
        param_values = []
        if self.filters and len(self.filters) > 0:
            for filter_field in self.filters:
                if '.' in filter_field:
                    filter_key = filter_field.split('.')[1]
                else:
                    filter_key = filter_field
                if filter_key in data_params:
                    if hasattr(data_params[filter_key], '__iter__'):
                        param_values.extend(data_params[filter_key])
                    else:
                        param_values.append(data_params[filter_key])
        return param_values

    def send_data(self, data):
        """
        If the worker is going to return too much data, we split the results in different
        sub-workers. The main worker will still return a list with all sub-workers names
        so that everyone who were waiting for him, will need to recalculate
        """
        if not isinstance(data, Exception) and len(data) >= 10000 and self.use_split:
            idx = 0
            sub_workers = []
            for split_value in chunks(data, size=10000):
                sub_worker_name = self.name + '_' + str(idx)
                sub_workers.append(sub_worker_name)
                self.log.debug('%s - SPLIT RESULT - SUB-WORKER NAME %s' % (self.name, sub_worker_name))
                self.output_queue.put({sub_worker_name: split_value})
                idx += 1
            self.output_queue.put({self.name: {_WORKER_SPLIT_FLAG: sub_workers}})
        else:
            self.output_queue.put({self.name: data})


class CacheExtractor(QueuedWorker):
    """
    Worker that extracts pickled data from a file
    """

    def __init__(self, name, log):
        self.cache_path = None
        super(CacheExtractor, self).__init__(name, log)

    def do_work(self, input_data):
        cache_file = open(self.cache_path, 'r')
        data = cPickle.load(cache_file)
        return data

    def config_from_dict(self, config_dict, name):
        """
        Gets the path where to read the cached data
        """
        worker_section = config_dict.get(self.name, dict())
        self.cache_path = worker_section.get('cache_path', None)
        super(CacheExtractor, self).config_from_dict(config_dict, self.name)


class CacheLoader(QueuedWorker):
    """
    Worker that writes pickled every worker data
    to its file if specified
    """

    def __init__(self, name, log):
        self.workers_cache_paths = None
        super(CacheLoader, self).__init__(name, log)

    def do_work(self, input_data):
        for worker, data in input_data.iteritems():
            cache_file = open(self.workers_cache_paths[worker], 'w')
            cPickle.dump(data, cache_file, -1)
        return

    def config_from_dict(self, config_dict, name):
        """
        Gets the path where to read the cached data
        """
        worker_section = config_dict.get(self.name, dict())
        self.workers_cache_paths = worker_section.get('workers_cache_paths', None)
        super(CacheLoader, self).config_from_dict(config_dict, self.name)


class Transformer(QueuedWorker):
    """
    Transforms input data to match the target schema definition.
    The output is a list of maps between each input record and
    the target record
    """

    def __init__(self, name, log):
        super(Transformer, self).__init__(name, log)
        self.target_definition = dict()

    def do_work(self, input_data):
        """
        Hook to the real working method
        """
        return self.transform_data(input_data)

    def transform_data(self, input_data):
        """
        This should be overriden if we want to add specific
        behavior to the transform like conditions or additional loops
        """
        output = []
        for worker_data in input_data.values():
            mapped_data = [self.map_record(record) for record in worker_data if record]
            output.extend(mapped_data)
        return output

    def map_record(self, record):
        """
        Maps input record into the target definition
        """
        map_dict = copy.deepcopy(self.target_definition)
        return map_dict.update(record)


class DBLoader(QueuedDatabaseHandler):
    """
    Generic loading class that writes or updates input
    data into the respective database tables
    """

    def __init__(self, name, log):
        self.tables_workers = None
        self.tables_keys = None
        self.semaphore = None
        self.can_unlock = False
        self.tables_sequences = dict()
        super(DBLoader, self).__init__(name, log)

    def do_work(self, input_data):
        """
        For each table to load, gets existent data from the database
        and inserts or updates each record
        """
        self.init_connection_and_cursor()
        self.db_pid_control_queue.put({self.name: self.connection_pid})
        data_by_target = self._group_workers_data_by_target_table(input_data)
        for target_table, workers_data in data_by_target.iteritems():
            target_data = self.get_target_data(target_table)
            table_keys = self.tables_keys[target_table]
            # STRONG assumption here. data coming from workers
            # MUST have populated all fields corresponding to the target table keys
            data_to_insert = merge_data_by_keys(workers_data, table_keys)
            if self.semaphore:
                if self.semaphore.is_locked():
                    self.log.critical("SEMAPHORE LOCKED!")
                    raise Exception("SEMAPHORE LOCKED!")
                else:
                    self.semaphore.lock()
                    self.write_or_update(target_table, table_keys, data_to_insert, target_data, self.log)
                    self.semaphore.unlock(datetime.datetime.now())
            elif self.can_unlock:
                try:
                    self.unlock_table(target_table)
                    self.write_or_update(target_table, table_keys, data_to_insert, target_data, self.log)
                except Exception, exc:
                    raise exc
                finally:
                    self.lock_table(target_table)
                    self.may_fix_table_sequence(target_table)
            else:
                self.write_or_update(target_table, table_keys, data_to_insert, target_data, self.log)
        self.connection.close()
        return

    def _group_workers_data_by_target_table(self, workers_data):
        data_by_target = dict()
        tables_by_workers = invert_dict(self.tables_workers)
        for worker, data in workers_data.iteritems():
            targets = tables_by_workers.get(worker, [])
            for target in targets:
                data_by_target.setdefault(target, []).extend(data)
        return data_by_target

    def unlock_table(self, table_name):
        """Disables triggers of the specified table
        """
        self.log.info('%s - DISABLING TRIGGERS FOR %s' % (self.name, table_name))
        self.cursor.execute('ALTER TABLE %s DISABLE TRIGGER ALL' % table_name)
        self.connection.commit()

    def lock_table(self, table_name):
        """Enables triggers of the specified table
        """
        self.log.info('%s - ENABLING TRIGGERS FOR %s' % (self.name, table_name))
        self.cursor.execute('ALTER TABLE %s ENABLE TRIGGER ALL' % table_name)
        self.connection.commit()

    def get_target_data(self, target_table):
        """
        Returns all data contained in the target database table
        into a dictionary with primary keys as key
        """
        try:
            table_keys = self.tables_keys[target_table]
            keys_fields = ', '.join(table_keys)
        except KeyError:
            table_keys = None
            keys_fields = '*'
        sql = SELECT_SQL % (keys_fields, target_table)
        return self.read(sql, params=None, keys=table_keys)

    def config_from_dict(self, config_dict, name):
        """
        This worker needs two maps: the primary keys for each target table
        and the table that each transformer has mapped.
        Sequences may also be added to let the worker resets the values to the max id
        """
        worker_section = config_dict.get(self.name, dict())
        self.tables_workers = worker_section.get('tables_workers', dict())
        self.tables_keys = dict()
        tables_keys = worker_section.get('tables_keys', dict())
        for table, keys in tables_keys.iteritems():
            if not hasattr(keys, '__iter__'):
                keys = [keys]
            self.tables_keys[table] = keys
        semaphore_name = worker_section.get('semaphore', None)
        if semaphore_name:
            semaphore_section = config_dict.get(semaphore_name, dict())
            self.semaphore = init_object_from_dict(semaphore_section)
            self.semaphore.config_from_dict(config_dict, semaphore_name)
        self.can_unlock = str_to_bool(worker_section.get('can_unlock', False))
        self.tables_sequences = dict()
        for table, seq_info in worker_section.get('tables_sequences', dict()).items():
            self.tables_sequences.setdefault(table, seq_info)
        super(DBLoader, self).config_from_dict(config_dict, self.name)

    def may_fix_table_sequence(self, table_name):
        """
        When inserting or updating without triggers the sequences may de-sync.
        we need to set the nextval to the right value
        """
        self.log.info('%s - FIXING SEQUENCE FOR %s' % (self.name, table_name))
        sequence_info = self.tables_sequences.get(table_name, None)
        sequence_name = None
        sequence_field = None
        if sequence_info is not None:
            sequence_name, sequence_field = sequence_info
        else:
            # We check if there is a sequence owned by this table and get the details
            self.log.info('%s - GETTING SEQUENCE FROM SQL' % self.name)
            self.cursor.execute("""SELECT a.attname AS referred_column, pg_get_serial_sequence(c.relname, a.attname) AS name
                          FROM pg_class c
                          JOIN pg_attribute a ON (c.oid=a.attrelid)
                          JOIN pg_attrdef d ON (a.attrelid=d.adrelid AND a.attnum=d.adnum)
                          JOIN pg_namespace n ON (c.relnamespace=n.oid)
                          WHERE n.nspname NOT LIKE 'pg!_%%' escape '!' AND (NOT a.attisdropped) AND d.adsrc ILIKE '%%nextval%%' and c.relname = '%s'""" % table_name)
            res = self.cursor.fetchone()
            if res is not None:
                sequence_field, sequence_name = res
                self.log.info('%s - GOT SEQUENCE %s' % (self.name, sequence_name))
            else:
                self.log.info('%s - THERE IS NO SEQUENCE CONNECTED' % self.name)
        if sequence_name is not None and sequence_field is not None:
            self.cursor.execute(
                """SELECT setval('%s', (SELECT MAX(%s) FROM %s))""" % (sequence_name, sequence_field, table_name))
            self.connection.commit()


class XLSXLoader(QueuedWorker):
    """Worker that loads input data in
    a .xlsx file given header and path.
    *WARNING* does not check column order so
    fields MUST be coherent with headers order
    """

    def __init__(self, name, log):
        self.headers = None
        self.output_file = None
        self.formats = dict()
        super(XLSXLoader, self).__init__(name, log)

    def do_work(self, input_data):
        """Creates an .xlsx file with a worksheet named
        after the worker generating data, with headers and
        applying specific formats
        """
        for worker_name, data in input_data.iteritems():
            wb = xlsxwriter.Workbook(self.output_file)
            ws = wb.add_worksheet(worker_name)
            self.set_formats(wb)
            for i, label in enumerate(self.headers):
                cell_format = self.get_format_to_apply(0, i, label)
                ws.write(0, i, label, cell_format)
            for row_count, row in enumerate(data):
                row_count += 1
                for idx, _ in enumerate(self.headers):
                    value = row[idx]
                    if type(value) is str:
                        value = value.decode('utf-8')
                    cell_format = self.get_format_to_apply(row_count, idx, value)
                    ws.write(row_count, idx, value, cell_format)
            wb.close()

    def set_formats(self, wb):
        """Override this to apply custom formats to the workbook
        """
        pass

    def get_format_to_apply(self, row, col, value):
        """Every cell may have a specific format related. This
        method has to be implemented to apply them
        """
        return None

    def config_from_dict(self, config_dict, name):
        """
        Needs file headers and output path
        """
        worker_section = config_dict.get(self.name, dict())
        self.headers = worker_section.get('headers', None)
        if not hasattr(self.headers, '__iter__'):
            self.headers = [self.headers]
        self.output_file = worker_section.get('output_file', None)
        super(XLSXLoader, self).config_from_dict(config_dict, self.name)


class CSVExtractor(QueuedWorker):
    """This worker extracts csv data
    as a list of dictionaries. Each row is represented by
    a dictionary with the header labels as keys
    """

    def __init__(self, name, log):
        self.filepath = None
        self.separator = '\t'
        self.has_headers = None
        self.headers = []
        super(CSVExtractor, self).__init__(name, log)

    def do_work(self, input_data):
        in_file = open(self.filepath, 'r')
        dict_lines = []
        try:
            for line in self.get_all_lines(in_file):
                records = line.split(self.separator)
                line_dict = dict()
                for idx, label in enumerate(self.headers):
                    line_dict[label] = records[idx]
                dict_lines.append(line_dict)
        except:
            print line, records, label
            raise
        in_file.close()
        self.log.info('READ %i LINES' % len(dict_lines))
        return dict_lines

    def get_all_lines(self, file_d):
        if self.has_headers:
            return file_d.readlines()[1:]
        else:
            return file_d.readlines()

    def config_from_dict(self, config_dict, name):
        worker_section = config_dict.get(self.name, dict())
        self.separator = worker_section.get('separator', '\t')
        self.has_headers = str_to_bool(worker_section.get('has_headers', True))
        self.filepath = worker_section.get('filepath', None)
        if not self.filepath or os.path.basename(self.filepath).split('.')[1] != 'csv':
            raise Exception('Missing or wrong file configuration')
        self.headers = worker_section.get('headers', None)
        if not hasattr(self.headers, '__iter__'):
            self.headers = [self.headers]
        if not self.headers:
            raise Exception('csv headers configuration missing')
        super(CSVExtractor, self).config_from_dict(config_dict, name)
