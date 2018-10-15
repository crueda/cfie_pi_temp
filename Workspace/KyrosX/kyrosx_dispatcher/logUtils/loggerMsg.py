#!/usr/bin/env python
#-*- coding: UTF-8 -*-

import logging, logging.handlers
import os
from KyrosUtils import config

LOG_FOLDER = config.LOG_FOLDER
LOG_FILE = config.MSG_LOG_FILE
DAYS_FOR_ROTATE = int(config.DAYS_FOR_ROTATE)
LOG = LOG_FOLDER + LOG_FILE

try:
    os.stat(LOG_FOLDER)
except:
    os.mkdir(LOG_FOLDER)

try:
    logger = logging.getLogger('msg')
    loggerHandler = logging.handlers.TimedRotatingFileHandler(filename=LOG, when='midnight', interval=1, backupCount=DAYS_FOR_ROTATE)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    loggerHandler.setFormatter(formatter)
    logger.addHandler(loggerHandler)
    logger.setLevel(logging.DEBUG)
except:
    print('------------------------------------------------------------------')
    print('[ERROR] Error writing log at %s' % LOG)
    print('[ERROR] Please verify path folder exits and write permissions')
    print('------------------------------------------------------------------')
    exit()

def get_logger():
    """ Return an instance of logUtils to write log at msg.log file """
    return logger
