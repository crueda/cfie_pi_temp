#!/usr/bin/env python
#-*- coding: UTF-8 -*-

from configobj import ConfigObj
#config = ConfigObj('/opt/kyrosx_sync/sync.properties')
config = ConfigObj('/Users/Carlos/Workspace/KyrosX/kyrosx_sync/sync.properties')

PID = config['pid']

MONGODB1_HOST = config['mongodb1_host']
MONGODB1_PORT = config['mongodb1_port']
MONGODB2_HOST = config['mongodb2_host']
MONGODB2_PORT = config['mongodb2_port']
MONGODB_TIMEOUT = int(config['mongodb_timeoutMS'])
MONGODB_DATABASE = config['mongodb_database']

MYSQL_HOST = config['mysql_host']
MYSQL_USER = config['mysql_user']
MYSQL_PASSWORD = config['mysql_password']
MYSQL_NAME = config['mysql_name']

LOG_FOLDER = config['log_folder']
LOG_FILE = config['log_file']
PERFORMANCE_LOG_FILE = config['performance_log_file']
MONGO_LOG_FILE = config['mongo_log_file']
MYSQL_LOG_FILE = config['mysql_log_file']
DAYS_FOR_ROTATE = config['days_for_rotate']
