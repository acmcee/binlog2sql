#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import argparse
import datetime
from contextlib import contextmanager
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent,
)
from pymysqlreplication.event import QueryEvent
import struct
import time

if sys.version > '3':
    PY3PLUS = True
else:
    PY3PLUS = False


def is_valid_datetime(string):
    try:
        datetime.datetime.strptime(string, "%Y-%m-%d %H:%M:%S")
        return True
    except:
        return False


def create_unique_file(filename):
    version = 0
    result_file = filename
    # if we have to try more than 1000 times, something is seriously wrong
    while os.path.exists(result_file) and version < 1000:
        result_file = filename + '.' + str(version)
        version += 1
    if version >= 1000:
        raise OSError('cannot create unique file %s.[0-1000]' % filename)
    return result_file


@contextmanager
def temp_open(filename, mode):
    f = open(filename, mode)
    try:
        yield f
    finally:
        f.close()
        os.remove(filename)


def parse_args():
    """parse args for binlog2sql"""

    parser = argparse.ArgumentParser(description='Parse MySQL binlog to SQL you want', add_help=False)
    connect_setting = parser.add_argument_group('connect setting')
    connect_setting.add_argument('-h', '--host', dest='host', type=str,
                                 help='Host the MySQL database server located', default='127.0.0.1')
    connect_setting.add_argument('-u', '--user', dest='user', type=str,
                                 help='MySQL Username to log in as', default='root')
    connect_setting.add_argument('-p', '--password', dest='password', type=str,
                                 help='MySQL Password to use', default='')
    connect_setting.add_argument('-P', '--port', dest='port', type=int,
                                 help='MySQL port to use', default=3306)
    interval = parser.add_argument_group('interval filter')
    interval.add_argument('--start-file', dest='start_file', type=str, help='Start binlog file to be parsed')
    interval.add_argument('--start-position', '--start-pos', dest='start_pos', type=int,
                          help='Start position of the --start-file', default=4)
    interval.add_argument('--stop-file', '--end-file', dest='end_file', type=str,
                          help="Stop binlog file to be parsed. default: '--start-file'", default='')
    interval.add_argument('--stop-position', '--end-pos', dest='end_pos', type=int,
                          help="Stop position. default: latest position of '--stop-file'", default=0)
    interval.add_argument('--start-datetime', dest='start_time', type=str,
                          help="Start time. format %%Y-%%m-%%d %%H:%%M:%%S", default='')
    interval.add_argument('--stop-datetime', dest='stop_time', type=str,
                          help="Stop Time. format %%Y-%%m-%%d %%H:%%M:%%S;", default='')
    parser.add_argument('--stop-never', dest='stop_never', action='store_true', default=False,
                        help="Continuously parse binlog. default: stop at the latest event when you start.")
    parser.add_argument('--help', dest='help', action='store_true', help='help information', default=False)

    schema = parser.add_argument_group('schema filter')
    schema.add_argument('-d', '--databases', dest='databases', type=str, nargs='*',
                        help='dbs you want to process', default='')
    schema.add_argument('-t', '--tables', dest='tables', type=str, nargs='*',
                        help='tables you want to process', default='')

    event = parser.add_argument_group('type filter')
    event.add_argument('--only-dml', dest='only_dml', action='store_true', default=False,
                       help='only print dml, ignore ddl')
    event.add_argument('--sql-type', dest='sql_type', type=str, nargs='*', default=['INSERT', 'UPDATE', 'DELETE'],
                       help='Sql type you want to process, support INSERT, UPDATE, DELETE.')

    # exclusive = parser.add_mutually_exclusive_group()
    parser.add_argument('-K', '--no-primary-key', dest='no_pk', action='store_true',
                        help='Generate insert sql without primary key if exists', default=False)
    parser.add_argument('-B', '--flashback', dest='flashback', action='store_true',
                        help='Flashback data to start_position of start_file', default=False)
    parser.add_argument('--back-interval', dest='back_interval', type=float, default=1.0,
                        help="Sleep time between chunks of 1000 rollback sql. set it to 0 if do not need sleep")
    return parser


def command_line_args(args):
    need_print_help = False if args else True
    parser = parse_args()
    args = parser.parse_args(args)
    if args.help or need_print_help:
        parser.print_help()
        sys.exit(1)
    # if not args.start_file:
    #     raise ValueError('Lack of parameter: start_file')
    if args.flashback and args.stop_never:
        raise ValueError('Only one of flashback or stop-never can be True')
    if args.flashback and args.no_pk:
        raise ValueError('Only one of flashback or no_pk can be True')
    if (args.start_time and not is_valid_datetime(args.start_time)) or \
            (args.stop_time and not is_valid_datetime(args.stop_time)):
        raise ValueError('Incorrect datetime argument')
    return args


def compare_items(items):
    # caution: if v is NULL, may need to process
    (k, v) = items
    if v is None:
        return '`%s` IS %%s' % k
    else:
        return '`%s`=%%s' % k


def fix_object(value):
    """Fixes python objects so that they can be properly inserted into SQL queries"""
    if isinstance(value, set):
        value = ','.join(value)
    if PY3PLUS and isinstance(value, bytes):
        return value.decode('utf-8')
    elif not PY3PLUS and isinstance(value, unicode):
        return value.encode('utf-8')
    else:
        return value


def is_dml_event(event):
    if isinstance(event, WriteRowsEvent) or isinstance(event, UpdateRowsEvent) or isinstance(event, DeleteRowsEvent):
        return True
    else:
        return False


def event_type(event):
    t = None
    if isinstance(event, WriteRowsEvent):
        t = 'INSERT'
    elif isinstance(event, UpdateRowsEvent):
        t = 'UPDATE'
    elif isinstance(event, DeleteRowsEvent):
        t = 'DELETE'
    return t


def concat_sql_from_binlog_event(cursor, binlog_event, row=None, e_start_pos=None, flashback=False, no_pk=False):
    if flashback and no_pk:
        raise ValueError('only one of flashback or no_pk can be True')
    if not (isinstance(binlog_event, WriteRowsEvent) or isinstance(binlog_event, UpdateRowsEvent)
            or isinstance(binlog_event, DeleteRowsEvent) or isinstance(binlog_event, QueryEvent)):
        raise ValueError('binlog_event must be WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent or QueryEvent')

    sql = ''
    if isinstance(binlog_event, WriteRowsEvent) or isinstance(binlog_event, UpdateRowsEvent) \
            or isinstance(binlog_event, DeleteRowsEvent):
        pattern = generate_sql_pattern(binlog_event, row=row, flashback=flashback, no_pk=no_pk)
        sql = cursor.mogrify(pattern['template'], pattern['values'])
        time = datetime.datetime.fromtimestamp(binlog_event.timestamp)
        sql += ' #start %s end %s time %s' % (e_start_pos, binlog_event.packet.log_pos, time)
    elif flashback is False and isinstance(binlog_event, QueryEvent) and binlog_event.query != 'BEGIN' \
            and binlog_event.query != 'COMMIT':
        if binlog_event.schema:
            sql = 'USE {0};\n'.format(binlog_event.schema)
        sql += '{0};'.format(fix_object(binlog_event.query))

    return sql


def generate_sql_pattern(binlog_event, row=None, flashback=False, no_pk=False):
    template = ''
    values = []
    if flashback is True:
        if isinstance(binlog_event, WriteRowsEvent):
            template = 'DELETE FROM `{0}`.`{1}` WHERE {2} LIMIT 1;'.format(
                binlog_event.schema, binlog_event.table,
                ' AND '.join(map(compare_items, row['values'].items()))
            )
            values = map(fix_object, row['values'].values())
        elif isinstance(binlog_event, DeleteRowsEvent):
            template = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
                binlog_event.schema, binlog_event.table,
                ', '.join(map(lambda key: '`%s`' % key, row['values'].keys())),
                ', '.join(['%s'] * len(row['values']))
            )
            values = map(fix_object, row['values'].values())
        elif isinstance(binlog_event, UpdateRowsEvent):
            template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} LIMIT 1;'.format(
                binlog_event.schema, binlog_event.table,
                ', '.join(['`%s`=%%s' % x for x in row['before_values'].keys()]),
                ' AND '.join(map(compare_items, row['after_values'].items())))
            values = map(fix_object, list(row['before_values'].values())+list(row['after_values'].values()))
    else:
        if isinstance(binlog_event, WriteRowsEvent):
            if no_pk:
                # print binlog_event.__dict__
                # tableInfo = (binlog_event.table_map)[binlog_event.table_id]
                # if tableInfo.primary_key:
                #     row['values'].pop(tableInfo.primary_key)
                if binlog_event.primary_key:
                    row['values'].pop(binlog_event.primary_key)

            template = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
                binlog_event.schema, binlog_event.table,
                ', '.join(map(lambda key: '`%s`' % key, row['values'].keys())),
                ', '.join(['%s'] * len(row['values']))
            )
            values = map(fix_object, row['values'].values())
        elif isinstance(binlog_event, DeleteRowsEvent):
            template = 'DELETE FROM `{0}`.`{1}` WHERE {2} LIMIT 1;'.format(
                binlog_event.schema, binlog_event.table, ' AND '.join(map(compare_items, row['values'].items())))
            values = map(fix_object, row['values'].values())
        elif isinstance(binlog_event, UpdateRowsEvent):
            template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} LIMIT 1;'.format(
                binlog_event.schema, binlog_event.table,
                ', '.join(['`%s`=%%s' % k for k in row['after_values'].keys()]),
                ' AND '.join(map(compare_items, row['before_values'].items()))
            )
            values = map(fix_object, list(row['after_values'].values())+list(row['before_values'].values()))

    return {'template': template, 'values': list(values)}


def reversed_lines(fin):
    """Generate the lines of file in reverse order."""
    part = ''
    for block in reversed_blocks(fin):
        if PY3PLUS:
            block = block.decode("utf-8")
        for c in reversed(block):
            if c == '\n' and part:
                yield part[::-1]
                part = ''
            part += c
    if part:
        yield part[::-1]


def reversed_blocks(fin, block_size=4096):
    """Generate blocks of file's contents in reverse order."""
    fin.seek(0, os.SEEK_END)
    here = fin.tell()
    while 0 < here:
        delta = min(block_size, here)
        here -= delta
        fin.seek(here, os.SEEK_SET)
        yield fin.read(delta)


def new_strip(s):
    return s.strip()


def read_event_header(f):
    tr = f.read(1)
    if tr:
        f.seek(-1, 1)
        cpos = f.tell()
        timestamp, event_type, server_id, event_length, next_position, flag = struct.unpack('<IBIIIH', f.read(19))
        return cpos, timestamp, next_position
    else:
        return None, None, None


def analyse_binlog(binlog):
    """
    https://my.oschina.net/alchemystar/blog/850467
    https://dev.mysql.com/doc/internals/en/event-meanings.html
    :param binlog:
    :return:
    """
    f = open(binlog, 'rb', 1024)
    binlog_header = b'\xfe\x62\x69\x6e'
    file_header = f.read(4)
    if file_header != binlog_header:
        raise Exception('%s is not mysqlbinlog' % binlog)
    # binlog  event 包
    # event header timestamp
    f_pos, f_timestamp, f_next_position = read_event_header(f)
    if f_pos is None:
        return None
    begin_time = f_timestamp
    f.seek(0, 2)
    file_size = f.tell()

    f.seek(-47, 2)   # 到最后一个ROTATE_EVENT
    event_header = read_event_header(f)
    if event_header:
        c_pos, c_timestamp, c_next_position = event_header
        # 读取event header timestamp
        if c_next_position == file_size and c_next_position - c_pos == 47:
            end_time = c_timestamp
        else:
            f.seek(-23, 2)  # 到最后一个STOP_EVENT
            event_header = read_event_header(f)
            c_pos, c_timestamp, c_next_position = event_header
            if c_next_position == file_size and c_next_position - c_pos == 23:
                end_time = c_timestamp
            else:
                end_time = 0
        f.close()
        return {'create_at': begin_time, 'update_at': end_time}
    else:
        f.close()
        return


def get_binlog_pos_from_time(binlog, start_datetime, stop_datetime=''):
    """
    根据binlog名字 和  时间戳获取相应的binlog 位点，因为binlog2sql 本身根据时间点去执行太慢了
    :param binlog:
    :param start_datetime:
    :param stop_datetime:
    :return:start_position, stop_position
    """
    start_timestamp = int(time.mktime(start_datetime.timetuple())) if start_datetime else 0
    stop_timestamp = int(time.mktime(stop_datetime.timetuple())) if stop_datetime else 0
    if start_timestamp == 0 and stop_timestamp == 0:
        # 如果两个位点都是空值，直接返回None
        return
    f = open(binlog, 'rb', 1024)
    binlog_header = b'\xfe\x62\x69\x6e'
    file_header = f.read(4)
    if file_header != binlog_header:
        raise Exception('%s is not mysqlbinlog' % binlog)
    start_position = 0
    stop_position = 0
    last_position = 0
    start_position_fin_flag = 1 if start_timestamp == 0 else 0  # 表示开始位点已经检查过了
    stop_position_fin_flag = 1 if stop_timestamp == 0 else 0  # 表示结束位点一键检查过了
    while True:
        event_header = read_event_header(f)
        if event_header:
            f_pos, f_timestamp, f_next_position = event_header
            if start_position_fin_flag == 0:
                if f_timestamp >= start_timestamp:  # 如果现在的时间戳大于等于需要的时间戳，把上一次的位点设置为起始位点
                    # agent_logger.info(event_header)
                    # agent_logger.info('analyse start pos finished')
                    start_position = last_position
                    start_position_fin_flag = 1
            elif stop_position_fin_flag == 0:
                if f_timestamp > stop_timestamp:  # 如果现在的时间戳大于需要的时间戳，那么就设置这个位点为结束位点
                    # agent_logger.info(event_header)
                    # agent_logger.info('analyse finish pos finished')
                    stop_position = f_pos
                    break
            else:
                break
            last_position = f_pos
            f.seek(f_next_position)
        else:
            break
    f.close()
    return start_position, stop_position


def get_start_end_file_pos(conn, start_datetime, end_datetime):
    """
    获取binlog 的位点，假设操作系统的文件系统时间是可靠的
    :param conn:  连接
    :param start_datetime: 开始时间
    :param end_datetime: 结束时间
    :return:
    """
    start_file = ''
    end_file = ''
    start_pos = 0
    end_pos = 0
    start_datetime = datetime.datetime.strptime(start_datetime, "%Y-%m-%d %H:%M:%S")
    if end_datetime:
        end_datetime = datetime.datetime.strptime(end_datetime, "%Y-%m-%d %H:%M:%S")
    with conn as cursor:
        cursor.execute('show variables like "log_bin_index"')
        log_bin_index_file = cursor.fetchone()[1]
        with open(log_bin_index_file, 'r') as f:
            binlog_file_paths = map(new_strip, f.readlines())
            # binlog_files = map(lambda x: x.split('/')[-1], binlog_file_paths)

        start_pos_flag = False  # 表示开始位点已经找到
        stop_pos_flag = False  # 表示开始位点已经找到
        if not end_datetime:
            stop_pos_flag = True

        for binlog_file_path in binlog_file_paths:
            binlog_create_time = datetime.datetime.fromtimestamp(os.stat(binlog_file_path).st_atime)
            binlog_modify_time = datetime.datetime.fromtimestamp(os.stat(binlog_file_path).st_mtime)
            # sys.stderr.write('%s %s %s \n' % (binlog_file_path, binlog_create_time, binlog_modify_time))
            if not start_pos_flag and binlog_create_time <= start_datetime <= binlog_modify_time:
                if binlog_create_time <= end_datetime <= binlog_modify_time:
                    start_pos, end_pos = get_binlog_pos_from_time(binlog_file_path, start_datetime, end_datetime)
                    start_pos_flag = True
                    stop_pos_flag = True
                    start_file = end_file = binlog_file_path.split('/')[-1]
                else:
                    start_pos, _ = get_binlog_pos_from_time(binlog_file_path, start_datetime)
                    start_file = binlog_file_path.split('/')[-1]

                    start_pos_flag = True
            elif not stop_pos_flag and binlog_create_time <= end_datetime <= binlog_modify_time:
                _, end_pos = get_binlog_pos_from_time(binlog_file_path, start_datetime)
                stop_pos_flag = True
                end_file = binlog_file_path.split('/')[-1]

            else:
                continue

            if start_pos_flag and stop_pos_flag:
                break
    binlog_info = {'start_file': start_file, 'end_file': end_file, 'start_pos': start_pos, 'end_pos': end_pos}
    sys.stderr.write('%s \n' % str(binlog_info))
    return binlog_info
