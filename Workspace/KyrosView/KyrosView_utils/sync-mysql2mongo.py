#!/usr/bin/env python
#-*- coding: UTF-8 -*-

# autor: Carlos Rueda
# date: 2016-11-25
# version: 1.1

##################################################################################
# version 1.0 release notes: extract data from MySQL and generate json
# Initial version
# Requisites: library python-mysqldb. To install: "apt-get install python-mysqldb"
##################################################################################

import logging, logging.handlers
import os
import json
import sys
import datetime
import calendar
import time
import MySQLdb
import json
from pymongo import MongoClient


#### VARIABLES #########################################################
LOG_FILE = "./sync-mysql2mongo.py"
LOG_FOR_ROTATE = 10

DB_IP = "192.168.28.251"
DB_PORT = 3306
DB_NAME = "kyros4f1"
DB_USER = "root"
DB_PASSWORD = "dat1234"

#DB_MONGO_IP = "127.0.0.1"
DB_MONGO_IP = "192.168.28.248"
DB_MONGO_PORT = 27017
DB_MONGO_NAME = "kyros_test"

########################################################################

########################################################################
monitors = {}
monitorsFleet = {}
users = {}
usersMonitor = {}
devices = {}
vehicles = {}
iconsRealTime = {}
iconsCover = {}
iconsAlarm = {}
userTracking = {}
monitorTree = None
monitorTreeJson = None
fleetNameDict = {}
fleetParentDict = {}

monitorTree = None
monitorJson = None
new_monitorJson = []
fleetNameDict = {}
fleetParentDict = {}
fleetChildsDict = {}
fleetDevicesIdDict = {}
fleetDevicesLicenseDict = {}
fleetDevicesAliasDict = {}

########################################################################
# definimos los logs internos que usaremos para comprobar errores
try:
	logger = logging.getLogger('kyrosView-backend')
	loggerHandler = logging.handlers.TimedRotatingFileHandler(LOG_FILE , 'midnight', 1, backupCount=10)
	formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
	loggerHandler.setFormatter(formatter)
	logger.addHandler(loggerHandler)
	#logger.setLevel(logging.DEBUG)
	logger.setLevel(logging.INFO)
except:
	print '------------------------------------------------------------------'
	print '[ERROR] Error writing log at %s' % LOG_FILE
	print '[ERROR] Please verify path folder exits and write permissions'
	print '------------------------------------------------------------------'
	exit()
########################################################################


def addMonitor(deviceId, username):
	#logger.debug('  -> addMonitor: %s - %s', deviceId, username) 
	global monitors
	if (deviceId in monitors):
		monitors[deviceId].append(username) 
	else:
		monitors[deviceId] = [username]

def addMonitorFleet(fleetId, username):
	#logger.debug('  -> addMonitor: %s - %s', deviceId, username) 
	global monitorsFleet
	if (fleetId

	 in monitors):
		monitorsFleet[fleetId].append(username) 
	else:
		monitorsFleet[fleetId] = [username]

def getIcons():
	global icons
	icons = {}
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
		try:
			queryIcons = """SELECT v.DEVICE_ID, d.ICON_REAL_TIME , d.ICON_COVER, d.ICON_ALARM from VEHICLE v, DEVICE d where v.ICON_DEVICE=d.ID"""
			logger.debug("QUERY:" + queryIcons)
			cursor = dbConnection.cursor()
			cursor.execute(queryIcons)
			row = cursor.fetchone()
			while row is not None:
				deviceId = row[0]
				iconRealTime = row[1]
				iconCover = row[2]
				iconAlarm = row[3]
				iconsRealTime[deviceId] = iconRealTime
				iconsCover[deviceId] = iconCover
				iconsAlarm[deviceId] = iconAlarm
				row = cursor.fetchone()
			cursor.close
			dbConnection.close
		except Exception, error:
			logger.error('Error executing query: %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

def getUsers():
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
		try:
			queryUsers = """SELECT USERNAME, DATE_END, LANGUAGE_USER, EMAIL, FIRSTNAME, LASTNAME, PASSWORD, BLOCKED, DEFAULT_VEHICLE_LICENSE from USER_GUI"""
			logger.debug("QUERY:" + queryUsers)
			cursor = dbConnection.cursor()
			cursor.execute(queryUsers)
			rows = cursor.fetchall()
			return rows
			cursor.close
			dbConnection.close
		except Exception, error:
			logger.error('Error executing query: %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

def loadUsers():
	global users, usersMonitor
	users = {}
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
		try:
			queryUsers = """SELECT USERNAME, DATE_END from USER_GUI"""
			logger.debug("QUERY:" + queryUsers)
			cursor = dbConnection.cursor()
			cursor.execute(queryUsers)
			row = cursor.fetchone()
			while row is not None:
				username = str(row[0])
				dateEnd = row[1]
				users[username] = dateEnd
				row = cursor.fetchone()
				usersMonitor[username] = {"_id": username, "username": username, "monitor": [] }
			cursor.close
			dbConnection.close
		except Exception, error:
			logger.error('Error executing query: %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

def getDevices():
	global devices
	devices = {}
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
		try:
			queryDevices = """SELECT DEVICE_ID, VEHICLE_LICENSE from VEHICLE"""
			logger.debug("QUERY:" + queryDevices)
			cursor = dbConnection.cursor()
			cursor.execute(queryDevices)
			row = cursor.fetchone()
			while row is not None:
				deviceId = str(row[0])
				vehicleLicense = str(row[1])
				devices[deviceId] = vehicleLicense
				row = cursor.fetchone()
			cursor.close
			dbConnection.close
		except Exception, error:
			logger.error('Error executing query: %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

def getVehicles():
	global vehicles
	vehicles = {}
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
		try:
			queryVehicles = """SELECT VEHICLE.VEHICLE_LICENSE, VEHICLE.DEVICE_ID, VEHICLE.ALIAS, 
			DEVICE.ICON_REAL_TIME, DEVICE.ICON_COVER, DEVICE.ICON_ALARM, VEHICLE.CONSUMPTION	
			FROM VEHICLE inner join (DEVICE) 
			WHERE VEHICLE.ICON_DEVICE = DEVICE.ID"""
			logger.debug("QUERY:" + queryVehicles)
			cursor = dbConnection.cursor()
			cursor.execute(queryVehicles)
			rows = cursor.fetchall()
			cursor.close
			dbConnection.close
			return rows
		except Exception, error:
			logger.error('Error executing query: %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

def getMonitorSystem(username):
	logger.debug('getMonitorSystem with username: %s', username)
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
		try:
			queryDevices = """SELECT v.DEVICE_ID, d.ICON_REAL_TIME from VEHICLE v, DEVICE d where v.ICON_DEVICE=d.ID"""
			logger.debug("QUERY:" + queryDevices)
			cursor = dbConnection.cursor()
			cursor.execute(queryDevices)
			row = cursor.fetchone()
			while row is not None:
				deviceId = int(row[0])
				deviceIcon = str(row[1])
				addMonitor(deviceId, username)
				row = cursor.fetchone()
			cursor.close
			dbConnection.close
		except Exception, error:
			logger.error('Error executing query: %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

def getMonitorCompany(username):
	logger.debug('getMonitorCompany with username: %s', username)
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
		try:
			query = """SELECT ID from MONITORS where USERNAME='xxx'"""
			queryMonitor = query.replace('xxx', str(username))
			logger.debug("QUERY:" + queryMonitor)
			cursor = dbConnection.cursor()
			cursor.execute(queryMonitor)
			row = cursor.fetchone()
			while row is not None:
				consignorId = str(row[0])
				queryDevices = "SELECT h.DEVICE_ID from HAS h, FLEET f where h.FLEET_ID=f.FLEET_ID and f.CONSIGNOR_ID=" + consignorId
				logger.debug("QUERY:" + queryDevices)
				cursor2 = dbConnection.cursor()
				cursor2.execute(queryDevices)
				row2 = cursor2.fetchone()
				while row2 is not None:
					deviceId = row2[0]
					deviceIcon = ''
					logger.debug('  -> addMonitor: %s - %s', deviceId, deviceIcon) 
					addMonitor(deviceId, username)
					row2 = cursor2.fetchone()

				row = cursor.fetchone()
				cursor2.close
			cursor.close
			dbConnection.close
		except Exception, error:
			logger.error('Error executing query: %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

def getMonitorFleet(username):
	logger.debug('getMonitorFleet with username: %s', username)
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
		try:
			query = """SELECT ID from MONITORS where USERNAME='xxx'"""
			queryMonitor = query.replace('xxx', str(username))
			logger.debug("QUERY:" + queryMonitor)
			cursor = dbConnection.cursor()
			cursor.execute(queryMonitor)
			row = cursor.fetchone()
			while row is not None:
				fleetId = str(row[0])
				queryDevices = "SELECT DEVICE_ID from HAS where FLEET_ID=" + fleetId
				cursor2 = dbConnection.cursor()
				cursor2.execute(queryDevices)
				row2 = cursor2.fetchone()
				while row2 is not None:
					deviceId = row2[0]
					deviceIcon = ''
					addMonitor(deviceId, username)
					addMonitorFleet(fleetId, username)
					row2 = cursor2.fetchone()

				row = cursor.fetchone()
				cursor2.close
			cursor.close
			dbConnection.close
		except Exception, error:
			logger.error('Error executing query: %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

def getMonitorDevice(username):
	logger.debug('getMonitorDevice with username: %s', username)
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
		try:
			t = calendar.timegm(datetime.datetime.utcnow().utctimetuple())
			query = """SELECT ID from MONITORS where USERNAME=='xxx'"""
			queryMonitor = query.replace('xxx', str(username))
			cursor = dbConnection.cursor()
			cursor.execute(queryMonitor)
			row = cursor.fetchall()
			while row is not None:
				deviceId = row[0]
				addMonitor(deviceId, username)
				row = cursor.fetchone()
			cursor.close
			dbConnection.close
		except Exception, error:
			logger.error('Error executing query: %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)
	
def getMonitor():
	global monitors, monitorsFleet
	monitors, monitorsFleet = {}, {}
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
		try:
			t = calendar.timegm(datetime.datetime.utcnow().utctimetuple())*1000
			queryUsers = """SELECT USERNAME, KIND_MONITOR from USER_GUI"""
			logger.debug("QUERY:" + queryUsers)
			cursor = dbConnection.cursor()
			cursor.execute(queryUsers)
			row = cursor.fetchone()
			while row is not None:
				username = row[0]
				kindMonitor = int(row[1])
				if (kindMonitor==0):
					getMonitorCompany(username)
				elif (kindMonitor==1):
					getMonitorFleet(username)
				elif (kindMonitor==2):
					getMonitorSystem(username)
				elif (kindMonitor==3):
					getMonitorDevice(username)
				row = cursor.fetchone()
			cursor.close
			dbConnection.close
		except Exception, error:
			logger.error('Error executing query: %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)



########################################################################

########################################################################

def getConsignors():
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)

		cursor = dbConnection.cursor()
		queryConsignor = """SELECT CONSIGNOR_ID,
			NAME_CONSIGNOR
			FROM CONSIGNOR
			WHERE CONSIGNOR_ID>0"""
		cursor.execute(queryConsignor)
		result = cursor.fetchall()
		cursor.close
		dbConnection.close
		
		return result

	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

def getFleets(consignor):
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)

		cursor = dbConnection.cursor()
		query = """SELECT FLEET_ID,
			DESCRIPTION_FLEET,
			PARENT_ID
			FROM FLEET
			WHERE CONSIGNOR_ID=xxx ORDER BY LEVEL"""
		queryFleets = query.replace('xxx', str(consignor))
		cursor.execute(queryFleets)
		result = cursor.fetchall()
		cursor.close
		dbConnection.close
		
		return result

	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

def getDevicesByFleet(fleetId):
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)

		cursor = dbConnection.cursor()
		query = """SELECT VEHICLE.DEVICE_ID,
			VEHICLE.VEHICLE_LICENSE,
			VEHICLE.ALIAS
			FROM VEHICLE inner join (HAS) 
			WHERE VEHICLE.DEVICE_ID = HAS.DEVICE_ID
			AND HAS.FLEET_ID=xxx"""

		queryDevices = query.replace('xxx', str(fleetId))
		cursor.execute(queryDevices)
		result = cursor.fetchall()
		cursor.close
		dbConnection.close
		
		return result

	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)


########################################################################

########################################################################

class Arbol:
    def __init__(self, elemento):
        self.hijos = []
        self.elemento = elemento

def agregarElemento(arbol, elemento, elementoPadre):
    subarbol = buscarSubarbol(arbol, elementoPadre);
    subarbol.hijos.append(Arbol(elemento))

def buscarSubarbol(arbol, elemento):
    if arbol.elemento == elemento:
        return arbol
    for subarbol in arbol.hijos:
        arbolBuscado = buscarSubarbol(subarbol, elemento)
        if (arbolBuscado != None):
            return arbolBuscado
    return None 

def profundidad(arbol):
    if len(arbol.hijos) == 0: 
        return 1
    return 1 + max(map(profundidad,arbol.hijos)) 

def grado(arbol):
    return max(map(grado, arbol.hijos) + [len(arbol.hijos)])

def ejecutarProfundidadPrimero(arbol, funcion):
    funcion(arbol.elemento)
    for hijo in arbol.hijos:
        ejecutarProfundidadPrimero(hijo, funcion)

def processTreeElement(element):
	global monitorTreeJson
	if (element!=0):
		elementParent = fleetParentDict[element]
		elementName = fleetNameDict[element]
		if (fleetChildsDict.has_key(elementParent)):
			fleetChildsDict[elementParent].append (element)
		else:
			fleetChildsDict[elementParent] = [element]
		

def generateMonitorTree():
	global monitorTree, fleetDevicesIdDict, fleetDevicesLicenseDict, fleetDevicesAliasDict
	consignorInfo = getConsignors()
	for consignor in consignorInfo:
		consignorId = consignor[0]
		consignorName = consignor[1]
		#agregarElemento(consignorName, '0', 0)
		consignorFleetInfo = getFleets(consignorId)
		for consignorFleet in consignorFleetInfo:
			fleetId = consignorFleet[0]
			fleetName = consignorFleet[1]
			fleetParent = consignorFleet[2]
			agregarElemento(monitorTree, fleetId, fleetParent)
			fleetNameDict[fleetId] = fleetName
			fleetParentDict[fleetId] = fleetParent

			#leer dispositivos y rellenar diccionarios
			devicesInfo = getDevicesByFleet(fleetId)
			for device in devicesInfo:
				deviceId = device[0]
				deviceLicense = device[1]
				deviceAlias = device[2]
				#print deviceId
				if (fleetDevicesIdDict.has_key(fleetId)):
					fleetDevicesIdDict[fleetId].append(deviceId)
					fleetDevicesLicenseDict[fleetId].append(deviceLicense)
					fleetDevicesAliasDict[fleetId].append(deviceAlias)
				else:
					fleetDevicesIdDict[fleetId] = [deviceId]
					fleetDevicesLicenseDict[fleetId] = [deviceLicense]
					fleetDevicesAliasDict[fleetId] = [deviceAlias]

def generateMonitorJson():
	global fleetChildsDict, fleetIdDict, fleetNameDict, monitorJson, usersMonitor
	#nivel 1	
	for fleetId1 in fleetChildsDict[0]:
		fleetJson1 = {"type": 0, "level": 1, "id": fleetId1, "name": fleetNameDict[fleetId1], "childs": []}
		ndevicesFleet1 = 0
		if (fleetDevicesIdDict.has_key(fleetId1)):
			for i in range(len(fleetDevicesIdDict[fleetId1])):
				device = {"type": 1, "level": 2, "id": fleetDevicesIdDict[fleetId1][i], "name": fleetDevicesLicenseDict[fleetId1][i]}
				fleetJson1['childs'].append(device)
		if (fleetChildsDict.has_key(fleetId1)):
			#nivel 2
			for fleetId2 in fleetChildsDict[fleetId1]:
				fleetJson2 = {"type": 0, "level": 2, "id": fleetId2, "name": fleetNameDict[fleetId2], "childs": []}
				ndevicesFleet2 = 0
				if (fleetDevicesIdDict.has_key(fleetId2)):
					for i in range(len(fleetDevicesIdDict[fleetId2])):
						device = {"type": 1, "level": 2, "id": fleetDevicesIdDict[fleetId2][i], "name": fleetDevicesLicenseDict[fleetId2][i]}
						fleetJson2['childs'].append(device)
				if (fleetChildsDict.has_key(fleetId2)):
					#nivel 3
					for fleetId3 in fleetChildsDict[fleetId2]:
						fleetJson3 = {"type": 0, "level": 3, "id": fleetId3, "name": fleetNameDict[fleetId3], "state": {"checked": "false"}, "childs": []}
						if (fleetDevicesIdDict.has_key(fleetId3)):
							for i in range(len(fleetDevicesIdDict[fleetId3])):
								device = {"type": 1, "level": 3, "id": fleetDevicesIdDict[fleetId3][i], "name": fleetDevicesLicenseDict[fleetId3][i]}
								fleetJson3['childs'].append(device)
						fleetJson2['childs'].append(fleetJson3)
				fleetJson1['childs'].append(fleetJson2)
		monitorJson.append(fleetJson1)
		for username in users.keys():
			usersMonitor[username]['monitor'].append(fleetJson1)


########################################################################

########################################################################

def save2Mongo(deviceData, collectionName):
	con = MongoClient(DB_MONGO_IP, int(DB_MONGO_PORT))
	db = con[DB_MONGO_NAME]
	device_collection = db[collectionName]
	device_collection.save(deviceData)

def getUserType(username):
	dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

	cursor = dbConnection.cursor()
	queryUser = """SELECT KIND_MONITOR
		FROM USER_GUI 
		WHERE USERNAME = 'xxx'"""
	query = queryUser.replace('xxx',username)
	cursor.execute(query)
	result = cursor.fetchone()			
	cursor.close
	dbConnection.close
	return result[0]

def getUserCompany(username):
	dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

	cursor = dbConnection.cursor()
	queryUser = """SELECT GROUP_CONCAT( distinct ID) 
		FROM MONITORS  
		WHERE USERNAME = 'xxx'"""
	query = queryUser.replace('xxx',username)
	cursor.execute(query)
	result = cursor.fetchone()			
	cursor.close
	dbConnection.close
	return result[0]

def getUserCompanyList(username):
	dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

	cursor = dbConnection.cursor()
	cursor2 = dbConnection.cursor()
	query = """SELECT GROUP_CONCAT( distinct ID )
		FROM MONITORS  
		WHERE USERNAME = 'xxx'"""
	query = query.replace('xxx',username)
	cursor.execute(query)
	result = cursor.fetchone()			
	fleetList = result[0]

	query = """SELECT GROUP_CONCAT( distinct CONSIGNOR_ID )
		FROM FLEET  
		WHERE FLEET_ID in (xxx)"""
	query = query.replace('xxx',fleetList)
	cursor2.execute(query)
	result2 = cursor2.fetchone()			
	consignorList = result2[0]

	cursor.close
	cursor2.close
	dbConnection.close

	return consignorList

def getUserFleetList(username):
	dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

	cursor = dbConnection.cursor()
	cursor2 = dbConnection.cursor()
	query = """SELECT GROUP_CONCAT( distinct ID )
		FROM MONITORS  
		WHERE USERNAME = 'xxx'"""
	query = query.replace('xxx',username)
	cursor.execute(query)
	result = cursor.fetchone()			
	fleetList = result[0]

	return fleetList

def savePoisMongo():
	global users
	con = MongoClient(DB_MONGO_IP, int(DB_MONGO_PORT))
	db = con[DB_MONGO_NAME]
	pois_collection = db['POIS']

	for username in users.keys():
		#userPois = {"username": username, "pois": []}

		userType = getUserType(username)

		#procesar pois de sistema		
		try:
			poisInfo = getPoisSystem()
			for poi in poisInfo:
				poiId = poi[0]
				poiName = make_unicode(poi[1])
				poiLat = poi[2]
				poiLon = poi[3]
				poiIcon = poi[4]

				poiData = {"username": username, "id": poiId, "type": 0, "name": poiName, "location": {"type": "Point", "coordinates": [poiLon, poiLat]},
				"icon": poiIcon
				}
				pois_collection.save(poiData)
		except:
			pass			
			#userPois['pois'].append(poiData)
		

		#procesar los pois publicos
		try:
			poisInfo = getPoisPublic(username)
			for poi in poisInfo:
				poiId = poi[0]
				poiName = make_unicode(poi[1])
				poiLat = poi[2]
				poiLon = poi[3]
				poiIcon = poi[4]

				poiData = {"username": username, "id": poiId, "type": 0, "name": poiName, "location": {"type": "Point", "coordinates": [poiLon, poiLat]},
				"icon": poiIcon
				}
				pois_collection.save(poiData)
					
				#userPois['pois'].append(poiData)
		except:
			pass

		#procesar los pois privados
		try:
			poisInfo = getPoisPrivate(username)
			for poi in poisInfo:
				poiId = poi[0]
				poiName = make_unicode(poi[1])
				poiLat = poi[2]
				poiLon = poi[3]
				poiIcon = poi[4]

				poiData = {"username": username, "id": poiId, "type": 0, "name": poiName, "location": {"type": "Point", "coordinates": [poiLon, poiLat]},
				"icon": poiIcon
				}
				pois_collection.save(poiData)
		except:
			pass
				

		#guardar los pois del usuario
		#pois_collection.save(userPois)


def getPoisSystem():
	dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

	cursor = dbConnection.cursor()
	queryPois = """SELECT POI.POI_ID, 
		POI.NAME, 
		POI.LATITUDE, 
		POI.LONGITUDE, 
		POI_CATEGORY.ICON
		FROM POI inner join (POI_CATEGORY) 
		WHERE POI.CATEGORY_ID = POI_CATEGORY.CATEGORY_ID and POI_CATEGORY.TYPE=0"""
	cursor.execute(queryPois)
	result = cursor.fetchall()			
	cursor.close
	dbConnection.close

	return result

def getPoisPrivate(username):
	dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

	cursor = dbConnection.cursor()
	queryPois = """SELECT POI.POI_ID, 
		POI.NAME, 
		POI.LATITUDE, 
		POI.LONGITUDE, 
		POI_CATEGORY.ICON
		FROM POI inner join (POI_CATEGORY) 
		WHERE POI.CATEGORY_ID = POI_CATEGORY.CATEGORY_ID and POI_CATEGORY.TYPE=1 and POI_CATEGORY.CREATOR_USER='xxx'"""
	queryPois = queryPois.replace('xxx', username)
	cursor.execute(queryPois)
	result = cursor.fetchall()			
	cursor.close
	dbConnection.close

	return result

def getPoisPublic(username):
	dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)

	cursor = dbConnection.cursor()
	#mirar si el usuario es de compañia o de flota
	typeUser = getUserType(username)
	if (typeUser==2):
		#el usuario es de sistema -> todas las categorias public		
		queryCategoryList = """SELECT GROUP_CONCAT(distinct POI_CATEGORY_ID)
			FROM POI_CATEGORY_MONITOR"""
		cursor.execute(queryCategoryList)
		result = cursor.fetchone()	
		categoryList = result[0]
		cursor2 = dbConnection.cursor()
		queryPois = """SELECT POI.POI_ID, 
			POI.NAME, 
			POI.LATITUDE, 
			POI.LONGITUDE, 
			POI_CATEGORY.ICON
		FROM POI inner join (POI_CATEGORY) 
		WHERE POI.CATEGORY_ID in (xxx)"""
		queryPois = queryPois.replace('xxx', categoryList)
		cursor2.execute(queryPois)
		result = cursor.fetchall()
		return result
	elif (typeUser==0):
		#el usuario es de compañia
		try:
			companyId = getUserCompany(username)
			queryCategoryList = """SELECT GROUP_CONCAT(distinct POI_CATEGORY_ID)
				FROM POI_CATEGORY_MONITOR
				where KIND_ENTITY=0 and ENTITY_ID in (xxx)"""
			queryCategoryList = queryCategoryList.replace('xxx', str(companyId))
			cursor.execute(queryCategoryList)
			result = cursor.fetchone()	
			categoryList = result[0]
			cursor2 = dbConnection.cursor()
			queryPois = """SELECT POI.POI_ID, 
				POI.NAME, 
				POI.LATITUDE, 
				POI.LONGITUDE, 
				POI_CATEGORY.ICON
			FROM POI inner join (POI_CATEGORY) 
			WHERE POI.CATEGORY_ID in (xxx)"""
			queryPois = queryPois.replace('xxx', categoryList)
			cursor2.execute(queryPois)
			result = cursor2.fetchall()
			return result
		except:
			pass
	elif (typeUser==1):
		#el usuario es de flota
		try:
			companyList = getUserCompanyList(username)
			queryCategoryList = """SELECT GROUP_CONCAT(distinct POI_CATEGORY_ID)
				FROM POI_CATEGORY_MONITOR
				where KIND_ENTITY=0 and ENTITY_ID in (xxx)"""
			queryCategoryList = queryCategoryList.replace('xxx', companyList)
			#print queryCategoryList
			cursor.execute(queryCategoryList)
			result = cursor.fetchone()	
			categoryList = result[0]
			#print categoryList
			cursor2 = dbConnection.cursor()
			queryPois = """SELECT POI.POI_ID, 
				POI.NAME, 
				POI.LATITUDE, 
				POI.LONGITUDE, 
				POI_CATEGORY.ICON
			FROM POI inner join (POI_CATEGORY) 
			WHERE POI.CATEGORY_ID in (xxx)"""
			queryPois = queryPois.replace('xxx', categoryList)
			cursor2.execute(queryPois)
			result1 = cursor2.fetchall()
			#print result1
			#return result
			fleetList = getUserFleetList(username)
			queryCategoryList = """SELECT GROUP_CONCAT(distinct POI_CATEGORY_ID)
				FROM POI_CATEGORY_MONITOR
				where KIND_ENTITY=1 and ENTITY_ID in (xxx)"""
			queryCategoryList = queryCategoryList.replace('xxx', fleetList)
			#print username
			#print queryCategoryList
			cursor2.execute(queryCategoryList)
			result2 = cursor2.fetchone()	
			result3 = None
			categoryList = result[0]
			if (categoryList!=None):
				#print str(categoryList)
				cursor3 = dbConnection.cursor()
				queryPois = """SELECT POI.POI_ID, 
					POI.NAME, 
					POI.LATITUDE, 
					POI.LONGITUDE, 
					POI_CATEGORY.ICON
				FROM POI inner join (POI_CATEGORY) 
				WHERE POI.CATEGORY_ID in (xxx)"""
				queryPois = queryPois.replace('xxx', str(categoryList))
				#print queryPois
				cursor3.execute(queryPois)
				result3 = cursor3.fetchall()

			return result1+result3
		except:
			pass


def saveVehiclesMongo():
	global vehicles
	con = MongoClient(DB_MONGO_IP, int(DB_MONGO_PORT))
	db = con[DB_MONGO_NAME]
	vehicle_collection = db['VEHICLE']

	vehiclesInfo = getVehicles()
	for vehicle in vehiclesInfo:
		vehicleLicense = vehicle[0]
		deviceId = vehicle[1]
		alias = vehicle[2]
		iconRealTime = vehicle[3]
		iconCover = vehicle[4]
		iconAlarm = vehicle[5]
		consumption = vehicle[6]

		mongoData = {"device_id": deviceId, "_id": vehicleLicense, "vehicle_license": vehicleLicense, "alias": make_unicode(alias), "icon_real_time": iconRealTime, "icon_cover": iconCover, "icon_alarm": iconAlarm, "consumption": consumption
		}
		
		vehicle_collection.save(mongoData)


def saveUsersMongo():
	global users
	con = MongoClient(DB_MONGO_IP, int(DB_MONGO_PORT))
	db = con[DB_MONGO_NAME]
	user_collection = db['USER']

	usersInfo = getUsers()
	for user in usersInfo:
		username = user[0]
		dateEnd = user[1]
		language = user[2]		
		email = user[3]
		firstname = make_unicode(user[4])
		lastname = make_unicode(user[5])
		password = user[6]
		blocked = user[7]
		defaultVehicle = user[8]

		mongoData = {"username": username, "_id": username, "date_end": dateEnd, "language": language, "email": email, "firstname": firstname, 
		"lastname": lastname, "password": password, "blocked": blocked, "vehicle_license": defaultVehicle, "token": "", 
		"notifications_active" : 1, "push_limit": 50, "push_enabled": 1, "push_count": 0
		}
		
		user_collection.save(mongoData)

def saveMonitorMongo():
	global monitorJson
	con = MongoClient(DB_MONGO_IP, int(DB_MONGO_PORT))
	db = con[DB_MONGO_NAME]
	monitor_collection = db['monitorDevice']
	monitor_collection.insert(monitorJson)

def saveMonitorUserMongo():
	global monitorJson, usersMonitor
	con = MongoClient(DB_MONGO_IP, int(DB_MONGO_PORT))
	db = con[DB_MONGO_NAME]
	#monitor_collection = db['MONITOR']
	for username in users.keys():
		monitor_collection = db['MONITOR_'+username]	
		userMonitorData = {"username": username, "monitor": usersMonitor[username]}
		for element in (usersMonitor[username]['monitor']):
			monitor_collection.save(element)
		#monitor_collection.save(usersMonitor[username])

def new_saveMonitorUserMongo():
	global new_monitorJson
	con = MongoClient(DB_MONGO_IP, int(DB_MONGO_PORT))
	db = con[DB_MONGO_NAME]
	monitor_collection = db['MONITOR_crueda']
	monitor_collection.save(new_monitorJson)

########################################################################

########################################################################
	
def getActualTime():
	now_time = datetime.datetime.now()
	format = "%H:%M:%S.%f"
	return now_time.strftime(format)

def make_unicode(input):
    if type(input) != unicode:
    	try:
        	input =  input.decode('utf-8', 'ignore')
        	return input
        except:
        	return ""
    else:
        return input

########################################################################

########################################################################

print getActualTime()

print "Procesando datos...."
loadUsers()
getDevices()
getIcons()
getMonitor()

print "Guardando coleccion USER"
#saveUsersMongo()

print "Guardando coleccion POI"
#savePoisMongo()

print "Guardando coleccion VEHICLE"
#saveVehiclesMongo()

print "Guardando coleccion MONITOR"
monitorTree = Arbol(0)
fleetParentDict[0] = 0
fleetNameDict[0] = "root"
generateMonitorTree()
ejecutarProfundidadPrimero(monitorTree, processTreeElement)
monitorJson = []
generateMonitorJson()
saveMonitorUserMongo()


print getActualTime()
