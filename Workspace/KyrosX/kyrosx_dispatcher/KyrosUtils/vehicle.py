#!/usr/bin/env python
#-*- coding: UTF-8 -*-

from logUtils import loggerDispatcher
from database import mongoUtils
from KyrosUtils import common

logger = loggerDispatcher.get_logger()



@common.catch_exceptions
def get_vehicle_from_id(device_id):
    """ Get vehicle entry at VEHICLE collection from device_id """
    return mongoUtils.read_single_document(collection='VEHICLE', filter={'device_id': device_id})

