'''
Created on Feb 5, 2014

@author: davide
'''
import ConfigParser
import importlib
import logging
import re
import smtplib
import sys
import traceback

from psycopg2 import extras, connect


REPORT_WAIT_INTERVAL = 5

DBINFO = 'host=%s user=%s dbname=%s port=%s'
SELECT_SQL = 'SELECT %s FROM %s'
UPDATE_SQL = 'UPDATE %s SET %s'
INSERT_SQL = 'INSERT INTO %s (%s) VALUES (%s)'
DELETE_SQL = 'DELETE FROM %s'

_KEY_VALUE_INI_SEP = ':'
_TUPLE_INI_SEP = ','
_DICT_KEYS_INI_SEP = '|'
_STR_INIT_KEY = '"'
_RE_PATTERN = '"([\S\D]*)"'# % (_STR_INIT_KEY, _STR_INIT_KEY)

_STATIC_DATA_FLAG = 'static.'
_SEMAPHORE_FLAG = 'semaphore.'
_WORKER_SPLIT_FLAG = 'WORKER_SPLITTED_IN'

_DEFAULT_WORKER_MODULE = 'pyetl.handlers'
_DEFAULT_WORKER_CLASS = 'DBExtractor'

def init_stream_log():
    logfile = logging.getLogger("logfun")
    logfile.setLevel(logging.DEBUG)
    h = logging.StreamHandler()
    f = logging.Formatter("[%(asctime)s] - %(levelname)s - %(message)s")
    h.setFormatter(f)
    logfile.addHandler(h)
    return logfile

def init_workers_from_file(fname, log_file):
    config = convert_ini_to_dict_config(fname)
    return init_workers_from_dict(config, log_file)

def init_workers_from_dict(config_dict, log_file):
    workers = set()
    for section, conf in config_dict.iteritems():
        if '.' not in section:
            modu = importlib.import_module(conf.get('module', _DEFAULT_WORKER_MODULE))
            klass = getattr(modu, conf.get('class', _DEFAULT_WORKER_CLASS))
            worker = klass(section, log_file)
            worker.config_from_dict(config_dict, section)
            workers.add(worker)
    return workers

def init_worker_from_dict(config_dict, worker_name, log=None):
    if '.' not in worker_name:
        worker_config = config_dict.get(worker_name, dict())
        modu = importlib.import_module(worker_config.get('module', _DEFAULT_WORKER_MODULE))
        klass = getattr(modu, worker_config.get('class', _DEFAULT_WORKER_CLASS))
        worker = klass(worker_name, log)
        worker.config_from_dict(config_dict, worker_name)
        return worker
    return None
    

def init_object_from_dict(config_dict):
        modu = importlib.import_module(config_dict.get('module', None))
        if modu:
            klass = getattr(modu, config_dict.get('class', None))
            if klass:
                obj = klass()
                return obj
        return None

def init_connection_and_cursor(host, user, dbname, password=None, port=5432):
    connection_info = DBINFO % (host, user, dbname, str(port))
    if password:
        connection_info += ' password=%s' % password
    conn = connect(connection_info)
    return conn, conn.cursor(cursor_factory=extras.DictCursor)

def merge_data_by_keys(data, keys, is_primary=True):
    merged_data = dict()
    if not hasattr(keys, '__iter__'):
        keys = [keys]
    for record in data:
        values = [record[k] for k in keys]
        key = combine_fields_for_keys(values)
        if is_primary:
            merged_data[key] = record
        else:
            merged_data.setdefault(key,[]).append(record)
    return merged_data

def combine_fields_for_keys(records):
    return '-'.join(map(str, records))

def convert_ini_to_dict_config(fname):
    config = dict()
    cp = ConfigParser.ConfigParser()
    cp.read(fname)
    for section in cp.sections():
        sect_dict = dict()
        for option in cp.options(section):
            opt_value = cp.get(section, option)
            opt_value = _convert_init_to_dict_value(opt_value)
            sect_dict[option] = opt_value
        config[section] = sect_dict
    return config

def _convert_init_to_dict_value(ini_value):
    try:
        final_value = ini_value
        if ini_value.startswith(_STR_INIT_KEY):
            final_values = re.findall(_RE_PATTERN, ini_value)
            if len(final_values) == 1:        #if I got only one
                final_value = final_values[0] # I return the str value
            else:
                final_value = final_values
        elif _DICT_KEYS_INI_SEP in ini_value or _KEY_VALUE_INI_SEP in ini_value:
            final_value = dict()
            kv_couples = ini_value.split(_DICT_KEYS_INI_SEP)
            for couple in kv_couples:
                a_key, a_value = couple.split(_KEY_VALUE_INI_SEP)
                final_value[a_key] = _convert_init_to_dict_value(a_value)
        elif _TUPLE_INI_SEP in ini_value:
            final_value = tuple(ini_value.split(_TUPLE_INI_SEP))
        return final_value
    except:
        print ini_value
        raise

def invert_dict(dict_to_invert):
    inverted = dict()
    for k, vs in dict_to_invert.iteritems():
        if hasattr(vs, '__iter__'):
            for v in vs:
                inverted.setdefault(v, []).append(k)
        else:
            inverted.setdefault(vs, []).append(k)
    return inverted

def get_insert_query_and_values(table_name, data_map):
    fields = []
    values = []
    names_for_fields = ','.join(['%s']*len(data_map))
    values_for_fields = ','.join(['%%s']*len(data_map))
    write_query = INSERT_SQL % (table_name, names_for_fields, values_for_fields)
    for field, value in data_map.iteritems():
        fields.append(field)
        values.append(value)
    write_query =  write_query % tuple(fields)
    return write_query, values
    
def get_update_query_and_values(table_name, table_keys, data_map):
    keys = []
    values = []
    for (key, value) in data_map.iteritems():
        keys.append(key)
        values.append(value)
    const = ["%s = %%s"]*len(data_map.keys())
    const = ', '.join(const)
    all_values = values
    all_fields = keys
    keys = []
    values = []
    null_values = 0
    if not hasattr(table_keys, '__iter__'):
        table_keys = [table_keys]
    for key in table_keys:
        keys.append(key)
        value_to_append = str(data_map[key]).replace("'", "''")
        if value_to_append == 'None':
            null_values += 1
            continue
        values.append(value_to_append)
    conditions = ["%s = %%s"] * (len(table_keys) - null_values)
    conditions.extend(["%s IS NULL"] * (null_values))
    conditions = ' AND '.join(conditions)
    all_fields.extend(keys)
    all_values.extend(values)
    query = UPDATE_SQL + ' WHERE %s'
    query = query % (table_name, const, conditions)
    query = query % tuple(all_fields)
    return query, all_values

def get_delete_query_and_values(table_name, table_keys, data_map):
    keys = []
    values = []
    for key in table_keys:
        keys.append(key)
        values.append(data_map[key])
    
    conditions = ["%s = %%s"] * len(keys)
    conditions = ' AND '.join(conditions)
    
    query = DELETE_SQL + ' WHERE %s'
    query = query % (table_name, conditions)
    query = query % tuple(keys)
    
    return query, values
    

TXT_TO_BOOL = dict()
TXT_TO_BOOL['True'] = True
TXT_TO_BOOL['true'] = True
TXT_TO_BOOL['1'] = True
TXT_TO_BOOL['t'] = True
TXT_TO_BOOL['False'] = False
TXT_TO_BOOL['false'] = False
TXT_TO_BOOL['f'] = False
TXT_TO_BOOL['0'] = False

def str_to_bool(text):
    if type(text) is bool:
        return text
    else:
        value = TXT_TO_BOOL.get(text, None)
        if value is None:
            raise Exception('%s is not a valid boolean indicator' % text)
        else:
            return value

FROM_ADDRESS = "PyETL@netstorming.net"
TO_ADDRESS = ['davide.favero@netstorming.net']

BODY_TEMPLATE = """Subject: %s
From: %s

%s
"""


def sendmail(msg, mail_to=TO_ADDRESS, subject='[PyETL] Exceptions report'):
    server = smtplib.SMTP()
    server.connect()
    body = BODY_TEMPLATE % (subject, FROM_ADDRESS, msg)
    if mail_to:
        server.sendmail(FROM_ADDRESS, mail_to, body)
    else:
        server.sendmail(FROM_ADDRESS, TO_ADDRESS, body)
    server.quit()
    
def current_exception_to_string():
    """Return a traceback as string. Can only be called from within an exception handler."""
    return "".join(traceback.format_exception(*sys.exc_info()))

from itertools import islice


def chunks(data, size=1000):
    if type(data) is dict:
        it = iter(data)
        for i in xrange(0, len(data), size):
            yield {k: data[k] for k in islice(it, size)}
    elif type(data) is list:
        for i in xrange(0, len(data), size):
            yield data[i:i + size]
