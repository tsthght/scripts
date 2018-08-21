#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import getopt
import json
import os
import re
import time

"""
功能：
     根据过滤条件，过滤Cetus全量日志
输入：
     Cetus全量日志的路径
     查询的开始时间
     查询的结束时间
     全量日志的过滤条件,JSON格式
     输出文件的文件名,默认为sqllog.sql
输出：
     将过滤后的日志输出到指定文件中，默认为sqllog.sql
"""

def usage():
    print('Usage:\n'
          '    -h or --help:   显示帮助信息\n'
          '    -p or --path:   日志文件路径\n'
          '    -s or --start:  开始时间\n'
          '    -e or --end:    结束时间\n'
          '    -c or --cond:   过滤条件，JSON格式\n'
          '    -o or --output: 输出文件名, 默认为sqllog.sql\n'
         )

def filter_file(path, name, start_t):
    pathname = r'%s/%s'%(path, name)
    file_mt = os.stat(pathname).st_mtime
    if start_t == 0:
        return True
    if file_mt < start_t:
        return False
    return True

def filter_metadata(s):
    try:
        flag = re.search(r"(#.*#)", s).group(0)
    except AttributeError:
        flag = ""
    if flag.strip() == "":
        return False
    return True

def filter_str(s, d):
    for (k, v) in d.items():
        cond = r"%s:%s"%(str(k), str(v))
        if s.find(cond) < 0:
            return False
    return True;

def filter_time(s, start_t, end_t):
    try:
        cur = re.search(r"(\d{4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2}:\d{1,2})", s).group(0)
    except AttributeError:
        cur = ""
    if cur.strip() == "":
        cur_t = 0
    else:
        cur_t = time.mktime(time.strptime(cur, '%Y-%m-%d %H:%M:%S'))
    if start_t == 0 and end_t != 0:
        if cur_t <= end_t:
            return True
    elif start_t != 0 and end_t == 0:
        if cur_t >= start_t:
            return True
    elif start_t !=0 and end_t != 0:
        if cur_t >= start_t and cur_t <= end_t:
            return True
    else:
        return True
    return False

log_path=""
log_cond_json=""
log_output="sqllog.sql"
log_time_start=""
log_time_end=""

try:
    options, args = getopt.getopt(sys.argv[1:], "hp:c:o:s:e:", ["help", "path", "cond", "output", "start", "end"])
except getopt.GetoptError:
    sys.exit()
for name, value in options:
    if name in ("-h", "--help"):
        usage()
    if name in ("-p", "--path"):
        log_path = value
    if name in ("-c", "--cond"):
        log_cond_json = value
    if name in ("-o", "--output"):
        log_output = value
    if name in ("-s", "--start"):
        log_time_start = value
    if name in ("-e", "--end"):
        log_time_end = value

# 处理输入参数
if log_path.strip() == "":
    print("Error: path is NULL")
    sys.exit()

if log_time_start.strip() == "":
    start_t = 0
else:
    start_t = time.mktime(time.strptime(log_time_start, '%Y-%m-%d %H:%M:%S'))
if log_time_end.strip() == "":
    end_t = 0
else:
    end_t = time.mktime(time.strptime(log_time_end, '%Y-%m-%d %H:%M:%S'))

# 在路径下搜索待分析的日志文件
path_file_list = os.listdir(log_path)
path_file_list = sorted(path_file_list, key=lambda k: os.path.getmtime(os.path.join(log_path, k)))
log_file_list=[]
for f in path_file_list:
    # 按特定的后缀名过滤
    if f.endswith(".clg"):
        # 按文件的修改时间过滤
        if filter_file(log_path, f, start_t):
            log_file_list.append(f)
        else:
            # 由于已经按修改时间排序，因此一旦某一个文件不满足，后续文件定然不满足
            break

# 获取过滤条件进行过滤
log_cond_json = log_cond_json.replace("'", '"')
log_cond_dict = json.loads(log_cond_json)
log_output_file = r"%s/%s"%(log_path, log_output)
wfp = open(log_output_file, "w+")
for f in log_file_list:
    st_md_start = 0
    rfp = open(f, "r")
    for line in rfp:
        line = line.strip()
        # 考虑有可能SQL有换行的情况,只过滤metadata
        if st_md_start == 1 and not filter_metadata(line):
            wfp.write(line + "\n")
        else:
            st_md_start = 0
        # 按条件过滤
        if filter_str(line, log_cond_dict) and filter_time(line, start_t, end_t):
            st_md_start = 1
            wfp.write(line + "\n")
    rfp.close()
wfp.close()

