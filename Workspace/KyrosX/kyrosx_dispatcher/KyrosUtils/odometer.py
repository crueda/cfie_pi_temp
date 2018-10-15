#!/usr/bin/env python
#-*- coding: UTF-8 -*-

import datetime

import pytz

from logUtils import loggerDispatcher
from database import mongoUtils
from KyrosUtils import utils
from KyrosUtils import vehicle
from KyrosUtils import common

logger = loggerDispatcher.get_logger()

days = {0: 'sunday', 1: 'monday', 2: 'tuesday', 3: 'wednesday', 4: 'thursday', 5: 'friday', 6: 'saturday'}


@common.catch_exceptions
def execute_mongo_find_one(collection, condition_query):
    return mongoUtils.read_single_document(filter=condition_query, collection=collection)


@common.catch_exceptions
def update_odometer(odometer_entry, device_id):
    """ Update odometer data for a device """
    collection_name = 'ODOMETER'
    condition_query = {"device_id": device_id}
    mongoUtils.update_single_document(document=odometer_entry, collection=collection_name, filter=condition_query)
    logger.debug("ODOMETER update for device_id %d", device_id)


@common.catch_exceptions
def save_odometer(odometer_entry):
    """ Get last odometer entry for one device """
    collection_name = 'ODOMETER'
    mongoUtils.write_single_document(document=odometer_entry, collection=collection_name)


@common.catch_exceptions
def get_last_odometer(device_id):
    """ Get last odometer entry for one device """
    collection_name = 'ODOMETER'
    condition = {'device_id': device_id}
    return mongoUtils.read_single_document(filter=condition, collection=collection_name)


@common.catch_exceptions
def init_odometer(device_id, speed, event_type):
    """ Init odometer for device: it returns odometer dict with initial values """
    odometer_entry = {}
    odometer_entry["device_id"] = device_id
    odometer_entry["dayCounter"] = 1
    odometer_entry["weekCounter"] = 1
    odometer_entry["monthCounter"] = 1
    odometer_entry["dayDistance"] = 0
    odometer_entry["weekDistance"] = 0
    odometer_entry["monthDistance"] = 0
    odometer_entry["daySpeed"] = speed
    odometer_entry["dayMaxSpeed"] = speed
    odometer_entry["weekSpeed"] = speed
    odometer_entry["monthSpeed"] = speed
    odometer_entry["dayConsume"] = 0
    odometer_entry["weekConsume"] = 0
    odometer_entry["monthConsume"] = 0
    odometer_entry["weekTrackingCounter"] = {"monday": 0, "tuesday": 0, "wednesday": 0, "thursday": 0, "friday": 0, "saturday": 0, "sunday": 0}
    odometer_entry["weekEventCounter"] = {"monday":0, "tuesday":0, "wednesday": 0, "thursday":0, "friday": 0, "saturday": 0, "sunday": 0}
    odometer_entry["hourTrackingCounter"] = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    odometer_entry["hourEventCounter"] = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    odometer_entry["eventTypeCounter"] = {}
    odometer_entry["dayEventTypeCounter"] = {}
    odometer_entry["totalCounter"] = 0
    odometer_entry["totalDistance"] = 0
    return odometer_entry


@common.catch_exceptions
def gen_odometer_tracking_day_position(last_odometer, msg_data, distance, consumption, pos_date_gmt):
    """ Gen odometer data from msg_data and last Odometer data stored at MONGO for device for now day position"""
    odometer_entry = {}
    odometer_entry["weekTrackingCounter"] = last_odometer["weekTrackingCounter"]
    odometer_entry["hourTrackingCounter"] = last_odometer["hourTrackingCounter"]
    odometer_entry["weekEventCounter"] = last_odometer["weekEventCounter"]
    odometer_entry["hourEventCounter"] = last_odometer["hourEventCounter"]
    odometer_entry["dayCounter"] = last_odometer["dayCounter"] + 1
    odometer_entry["weekCounter"] = last_odometer["weekCounter"] + 1
    odometer_entry["monthCounter"] = last_odometer["monthCounter"] + 1
    odometer_entry["dayDistance"] = utils.format_long_float(last_odometer["dayDistance"] + distance)
    odometer_entry["weekDistance"] = utils.format_long_float(last_odometer["weekDistance"] + distance)
    odometer_entry["monthDistance"] = utils.format_long_float(last_odometer["monthDistance"] + distance)
    if 'dayMaxSpeed' in last_odometer:
        if msg_data['speed'] > last_odometer["dayMaxSpeed"]:
            odometer_entry["dayMaxSpeed"] = utils.format_long_float( msg_data['speed'])
        else:
            odometer_entry["dayMaxSpeed"] = utils.format_long_float( last_odometer["dayMaxSpeed"])
    else:
        odometer_entry["dayMaxSpeed"] = utils.format_float( msg_data['speed'])
    odometer_entry["daySpeed"] = utils.format_float( ( (last_odometer["dayCounter"] * last_odometer["daySpeed"]) + (msg_data['speed']) ) / (last_odometer["dayCounter"]+1))
    odometer_entry["weekSpeed"] = utils.format_float( ( (last_odometer["weekCounter"] * last_odometer["weekSpeed"]) + (msg_data['speed']) ) / (last_odometer["weekCounter"]+1))
    odometer_entry["monthSpeed"] = utils.format_float( ( (last_odometer["monthCounter"] * last_odometer["monthSpeed"]) + (msg_data['speed']) ) / (last_odometer["monthCounter"]+1))
    odometer_entry["dayConsume"] = utils.format_long_float( last_odometer["dayConsume"] + consumption)
    odometer_entry["weekConsume"] = utils.format_long_float( last_odometer["weekConsume"] + consumption )
    odometer_entry["monthConsume"] = utils.format_long_float( last_odometer["monthConsume"] + consumption )
    actual_day = days[pos_date_gmt.weekday()]
    actual_hour = pos_date_gmt.hour
    odometer_entry["weekTrackingCounter"][actual_day] = last_odometer["weekTrackingCounter"][actual_day] + 1
    odometer_entry["hourTrackingCounter"][actual_hour] = last_odometer["hourTrackingCounter"][actual_hour] + 1
    odometer_entry["weekEventCounter"][actual_day] = last_odometer["weekEventCounter"][actual_day] + 1
    odometer_entry["hourEventCounter"][actual_hour] = last_odometer["hourEventCounter"][actual_hour] + 1
    odometer_entry["totalCounter"] = last_odometer["totalCounter"] + 1
    odometer_entry["totalDistance"] = utils.format_long_float(last_odometer["totalDistance"] + distance)
    return odometer_entry


@common.catch_exceptions
def gen_odometer_tracking_week_position(last_odometer, msg_data, distance, consumption, pos_date_gmt):
    """ Gen odometer data from msg_data and last Odometer data stored at MONGO for device for now week position"""
    odometer_entry = {}
    odometer_entry["weekTrackingCounter"] = last_odometer["weekTrackingCounter"]
    odometer_entry["hourTrackingCounter"] = last_odometer["hourTrackingCounter"]
    odometer_entry["weekEventCounter"] = last_odometer["weekEventCounter"]
    odometer_entry["hourEventCounter"] = last_odometer["hourEventCounter"]
    odometer_entry["weekCounter"] = last_odometer["weekCounter"] + 1
    odometer_entry["monthCounter"] = last_odometer["monthCounter"] + 1
    odometer_entry["weekDistance"] = utils.format_long_float(last_odometer["weekDistance"] + distance)
    odometer_entry["monthDistance"] = utils.format_long_float(last_odometer["monthDistance"] + distance)
    odometer_entry["weekSpeed"] = utils.format_float( ( (last_odometer["weekCounter"] * last_odometer["weekSpeed"]) + (msg_data['speed']) ) / (last_odometer["weekCounter"] +1) )
    odometer_entry["monthSpeed"] = utils.format_float( ( (last_odometer["monthCounter"] * last_odometer["monthSpeed"]) + (msg_data['speed']) ) / (last_odometer["monthCounter"] +1) )
    odometer_entry["weekConsume"] = utils.format_long_float( last_odometer["weekConsume"] + consumption )
    odometer_entry["monthConsume"] = utils.format_long_float( last_odometer["monthConsume"] + consumption )
    actual_day = days[pos_date_gmt.weekday()]
    actual_hour = pos_date_gmt.hour
    odometer_entry["weekTrackingCounter"][actual_day] = last_odometer["weekTrackingCounter"][actual_day] + 1
    odometer_entry["hourTrackingCounter"][actual_hour] = last_odometer["hourTrackingCounter"][actual_hour] + 1
    odometer_entry["weekEventCounter"][actual_day] = last_odometer["weekEventCounter"][actual_day] + 1
    odometer_entry["hourEventCounter"][actual_hour] = last_odometer["hourEventCounter"][actual_hour] + 1
    return odometer_entry


@common.catch_exceptions
def gen_odometer_tracking_month_position(last_odometer, msg_data, distance, consumption, pos_date_gmt):
    """ Gen odometer data from msg_data and last Odometer data stored at MONGO for device for now week position"""
    odometer_entry = {}
    odometer_entry["weekTrackingCounter"] = last_odometer["weekTrackingCounter"]
    odometer_entry["hourTrackingCounter"] = last_odometer["hourTrackingCounter"]
    odometer_entry["weekEventCounter"] = last_odometer["weekEventCounter"]
    odometer_entry["hourEventCounter"] = last_odometer["hourEventCounter"]
    odometer_entry["monthCounter"] = last_odometer["monthCounter"] + 1
    odometer_entry["monthDistance"] = utils.format_long_float(last_odometer["monthDistance"] + distance)
    odometer_entry["monthSpeed"] = utils.format_float(((last_odometer["monthCounter"] * last_odometer["monthSpeed"]) + (msg_data['speed'])) / (last_odometer["monthCounter"]+1))
    odometer_entry["monthConsume"] = utils.format_long_float(last_odometer["monthConsume"] + consumption)
    actual_day = days[pos_date_gmt.weekday()]
    actual_hour = pos_date_gmt.hour
    odometer_entry["weekTrackingCounter"][actual_day] = last_odometer["weekTrackingCounter"][actual_day] + 1
    odometer_entry["hourTrackingCounter"][actual_hour] = last_odometer["hourTrackingCounter"][actual_hour] + 1
    odometer_entry["weekEventCounter"][actual_day] = last_odometer["weekEventCounter"][actual_day] + 1
    odometer_entry["hourEventCounter"][actual_hour] = last_odometer["hourEventCounter"][actual_hour] + 1
    return odometer_entry


@common.catch_exceptions
def gen_odometer_tracking(last_odometer, msg_data, features):
    """ Gen odometer data from msg_data and last Odometer data stored at MONGO for device """
    device_id = int(msg_data['device_id'])
    vehicle_device = vehicle.get_vehicle_from_id(device_id)
    vehicle_timezone = vehicle_device['time_zone'] if 'time_zone' in vehicle_device else 'Europe/Madrid'
    try:
        distance = features['distance']
    except:
        distance = float(0)
    try:
        consumption = features['consumption']
    except:
        consumption = float(0)
    # check if position is in now day/week/month according vehicle time_zone
    vehicle_gmt = pytz.timezone(vehicle_timezone)
    now_gmt = vehicle_gmt.normalize(pytz.utc.localize(datetime.datetime.utcnow()))
    pos_date = msg_data['pos_date']/1000
    pos_date_gmt = vehicle_gmt.normalize(pytz.utc.localize(datetime.datetime.utcfromtimestamp(pos_date)))
    # first check if position in now day
    if (now_gmt.month == pos_date_gmt.month) and (now_gmt.day == pos_date_gmt.day):
        odometer_entry = gen_odometer_tracking_day_position(last_odometer, msg_data, distance, consumption,
                                                            pos_date_gmt)
        return odometer_entry
    # if not in now day check if in now week
    elif (now_gmt - pos_date_gmt).days < 7:
        odometer_entry = gen_odometer_tracking_week_position(last_odometer, msg_data, distance, consumption,
                                                             pos_date_gmt)
        return odometer_entry
    # if not in now week check if in now month
    elif now_gmt.month == pos_date_gmt.month:
        odometer_entry = gen_odometer_tracking_month_position(last_odometer, msg_data, distance, consumption,
                                                              pos_date_gmt)
        return odometer_entry
    else:
        return None


@common.catch_exceptions_and_performance
def manage_odometer_tracking(msg_data, features):
    """ Save or update odometer entry at ODOMETER collection from msg_data and features """
    device_id = int(msg_data['device_id'])
    last_odometer = get_last_odometer(device_id)
    if last_odometer is None:
        speed = msg_data["speed"]
        odometer_entry = init_odometer(device_id, speed, 0)
        save_odometer(odometer_entry)
    else:
        odometer_entry = gen_odometer_tracking(last_odometer, msg_data, features)
        if odometer_entry is not None:
            update_odometer(odometer_entry, device_id)


@common.catch_exceptions
def gen_odometer_event(last_odometer, msg_data):
    """ Gen odometer data from msg_data and last_odometer data stored at MONGO for device """
    device_id = int(msg_data['device_id'])
    condition = {'device_id':device_id}
    vehicle_device = mongoUtils.read_single_document(filter=condition, collection='VEHICLE')
    vehicle_timezone = vehicle_device['time_zone'] if 'time_zone' in vehicle_device else 'Europe/Madrid'
    # check if position is in now day/week/month according vehicle time_zone
    vehicle_gmt = pytz.timezone(vehicle_timezone)
    # nowGMT = vehicle_gmt.normalize(pytz.utc.localize(datetime.datetime.utcnow()))
    pos_date = msg_data['event_date']/1000
    pos_date_gmt = vehicle_gmt.normalize(pytz.utc.localize(datetime.datetime.utcfromtimestamp(pos_date)))
    odometer = last_odometer
    actual_day = days[pos_date_gmt.weekday()]
    actual_hour = pos_date_gmt.hour
    odometer["weekEventCounter"][actual_day] = last_odometer["weekEventCounter"][actual_day] + 1
    odometer["hourEventCounter"][actual_hour] = last_odometer["hourEventCounter"][actual_hour] + 1
    try:
        counter = last_odometer["eventTypeCounter"][str(msg_data['event_type'])]
        odometer["eventTypeCounter"][str(msg_data['event_type'])] = counter + 1
    except:
        odometer["eventTypeCounter"][str(msg_data['event_type'])] = 1
    try:
        if odometer["dayEventTypeCounter"] is None:
            odometer["dayEventTypeCounter"] = {}
        if last_odometer["dayEventTypeCounter"] is None:
            counter = last_odometer["dayEventTypeCounter"][str(msg_data['event_type'])]
            odometer["dayEventTypeCounter"][str(msg_data['event_type'])] = counter + 1
        else:
            odometer["dayEventTypeCounter"][str(msg_data['event_type'])] = 1
    except:
        odometer["dayEventTypeCounter"] = {}
        odometer["dayEventTypeCounter"][str(msg_data['event_type'])] = 1
    return odometer


@common.catch_exceptions_and_performance
def save_odometer_event(msg_data):
    """ Save odometer data """
    device_id = int(msg_data['device_id'])
    collection_name = 'ODOMETER'
    condition = {'device_id': device_id}
    last_odometer = mongoUtils.read_single_document(filter=condition, collection=collection_name)
    if last_odometer is None:
        odometer = init_odometer(device_id, 0, msg_data['event_type'])
        mongoUtils.write_single_document(document=odometer, collection=collection_name)
    else:
        odometer = gen_odometer_event(last_odometer, msg_data)
        condition = {"device_id": device_id}
        mongoUtils.update_single_document(document=odometer, collection=collection_name, filter=condition)
