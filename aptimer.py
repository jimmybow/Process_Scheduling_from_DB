# -*- coding: utf-8 -*-
"""
Created on Fri Oct  5 15:03:49 2018

@author: jimmybow
"""

import sys
import logging
import subprocess
import time
from apscheduler.schedulers.background import BackgroundScheduler 
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import create_engine, MetaData
import pandas as pd

path_log = r'C:\Users\jimmybow\Desktop\log.txt'
db_path = r'C:\Users\jimmybow\Desktop\mydb.sqlite'
table_name = 'jobs'

logging.basicConfig(handlers=[logging.StreamHandler(sys.stdout),
                              logging.FileHandler(path_log)     ], 
                    level=logging.INFO,
                    format='>>>[%(asctime)s] [%(filename)s, line %(lineno)d] [%(levelname)s] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logging.getLogger('apscheduler').setLevel(logging.WARNING)

engine = create_engine( 'sqlite:///{}'.format(db_path) )
conn = engine.connect()
q = conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' and name = '{}'".format(table_name)).fetchall()
jobs = pd.DataFrame(columns = ['id', 'name', 'cmd', 'cron', 'cron_second', 'max_instances', 'timeout', 'log_start_end'])
if len(q) == 0:  jobs.to_sql(table_name, conn, if_exists = 'replace', index = False)

def get_CronTrigger_value(field_name):
    index = crontab.FIELD_NAMES.index(field_name)
    return str(crontab.fields[index])

def subprocess_job(cmd, timeout = 60, log_start_end = False):
    if log_start_end: logging.info('"{}" start'.format(cmd))
    try:   subprocess.run(cmd, timeout = timeout, check = True, stderr = subprocess.PIPE, shell = True)
    except subprocess.CalledProcessError as e: logging.error('"{}" error for\n{}'.format(cmd, e.stderr.decode()))
    except :logging.exception('')
    if log_start_end: logging.info('"{}" end'.format(cmd))

sched = BackgroundScheduler({'apscheduler.executors.default': {
                                 'class': 'apscheduler.executors.pool:ThreadPoolExecutor',
                                 'max_workers': '10000'
                             },
                             'apscheduler.job_defaults.max_instances': '10',
                             'apscheduler.job_defaults.misfire_grace_time':'30'}) 
sched.start()

while True:
    try:
        jobs_db = pd.read_sql(table_name, conn)
        if not jobs.equals(jobs_db):
            for job_id in jobs.id[~jobs.id.isin(jobs_db.id)]:  
                sched.remove_job(job_id) 
            for row in jobs_db.itertuples():
                crontab = CronTrigger.from_crontab(row.cron)
                sched.add_job(subprocess_job, 'cron', replace_existing = True,
                              id = row.id,
                              name = row.name,
                              second = row.cron_second, 
                              minute = get_CronTrigger_value('minute'),
                              hour = get_CronTrigger_value('hour'),
                              day = get_CronTrigger_value('day'),
                              month = get_CronTrigger_value('month'),
                              day_of_week = get_CronTrigger_value('day_of_week'),
                              max_instances = int(row.max_instances),
                              kwargs={'cmd': row.cmd,
                                      'timeout': int(row.timeout),
                                      'log_start_end': eval(row.log_start_end)}  )  
            jobs = jobs_db
    except:
        logging.exception('')
    ############################
    time.sleep(1)

####################################################################################
##conn.execute(
'''
insert into jobs ('id', 'name', 'cmd', 'cron', 'cron_second', 'max_instances', 'timeout', 'log_start_end')
values ('test', 'test', 'python C:/Users/jimmybow/Desktop/test.py', '* * * * *', '0/5', '3', '60', 'False')
'''
##)

##conn.execute('''delete from jobs''')
