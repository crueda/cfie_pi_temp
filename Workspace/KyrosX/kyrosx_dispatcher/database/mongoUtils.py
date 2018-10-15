#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

import pymongo
from KyrosUtils import config
from logUtils import loggerDispatcher
from logUtils import loggerMongo
from KyrosUtils import common

MONGODB1_HOST = config.MONGODB1_HOST
MONGODB1_PORT = config.MONGODB1_PORT
MONGODB2_HOST = config.MONGODB2_HOST
MONGODB2_PORT = config.MONGODB2_PORT
MONGODB_TIMEOUT = config.MONGODB_TIMEOUT
MONGODB_DATABASE = config.MONGODB_DATABASE

logger = loggerDispatcher.get_logger()
URI_CONNECTION = "mongodb://" + MONGODB1_HOST + ":" + MONGODB1_PORT + "," + MONGODB2_HOST + ":" + MONGODB2_PORT + "/"

logger_database = loggerMongo.get_logger()

TYPES_NOT_DICT=(int, str, float, list, bool, long, unicode)

try:
    client = pymongo.MongoClient(URI_CONNECTION, serverSelectionTimeoutMS=MONGODB_TIMEOUT)
    #client = pymongo.MongoClient(URI_CONNECTION)
    client.server_info()
except pymongo.errors.ServerSelectionTimeoutError as error:
    logger_database.error('Error with mongoDB connection: %s', error)
except pymongo.errors.ConnectionFailure as error:
    logger_database.error('Could not connect to MongoDB: %s', error)

def get_mongo_db_connection():
    """ Return a MongoDB connection from connection pool """
    return client[MONGODB_DATABASE]

@common.database_catch_exceptions_and_performance
def write_single_document(document, collection):
    """ Write a single json document at collection """
    mongo_connection = get_mongo_db_connection()
    mongo_client = mongo_connection[collection]
    mongo_client.insert_one(document)

def write_multi_document(document_array, collection):
    """ Write an array of documents at collection """
    mongo_connection = get_mongo_db_connection()
    mongo_client = mongo_connection[collection]
    try:
        mongo_client.insert_many(document_array)
    except Exception as error:
        logger_database.error("Error writing array of documents: %s", str(error))
        for document in document_array:
            write_single_document(document, collection)

@common.database_catch_exceptions_and_performance
def read_single_document(filter, collection, projection=None):
    """ Read a single json document from collection in function of filter """
    mongo_connection = get_mongo_db_connection()
    mongo_client = mongo_connection[collection]
    result = mongo_client.find_one(filter, projection)
    return result

@common.database_catch_exceptions_and_performance
def read_multi_document(filter, collection, projection=None):
    """ Read a several json documents from collection in function of filter """
    mongo_connection = get_mongo_db_connection()
    mongo_client = mongo_connection[collection]
    result = mongo_client.find(filter, projection)
    return result

@common.database_catch_exceptions_and_performance
def update_single_document(document, filter, collection, upsert=False):
    """ Update a single json document at collection in function of filter """
    document = modify_document_to_update(document)
    mongo_connection = get_mongo_db_connection()
    mongo_client = mongo_connection[collection]
    result = mongo_client.update_one(filter, {'$set': document}, upsert=upsert)
    return result

@common.database_catch_exceptions_and_performance
def update_single_document_full_document(document, filter, collection, upsert=False):
    """ Update a single json document at collection in function of filter """
    mongo_connection = get_mongo_db_connection()
    mongo_client = mongo_connection[collection]
    result = mongo_client.update_one(filter, {'$set': document}, upsert=upsert)
    return result

@common.database_catch_exceptions_and_performance
def update_single_document_debug(document, filter, collection):
    """ Update a single json document at collection in function of filter """
    logger.info("Executing update_single_document_debug -- document %s, filter %s, collection %s", str(document), str(filter), collection)
    document = modify_document_to_update(document)
    mongo_connection = get_mongo_db_connection()
    mongo_client = mongo_connection[collection]
    result = mongo_client.update_one(filter, {'$set': document})
    logger.info("Update_single_document_debug result -- ack: %s, matchedCount: %d, modifiedCount: %d", str(result.acknowledged), result.matched_count, result.modified_count)

@common.database_catch_exceptions_and_performance
def delete_single_document(filter, collection):
    """ Delete a single json document from collection in function of filter """
    mongo_connection = get_mongo_db_connection()
    mongo_client = mongo_connection[collection]
    mongo_client.delete_one(filter)

@common.database_catch_exceptions_and_performance
def delete_multi_document(filter, collection):
    """ Delete a single json document from collection in function of filter """
    mongo_connection = get_mongo_db_connection()
    mongo_client = mongo_connection[collection]
    mongo_client.delete_many(filter)

@common.database_catch_exceptions_and_performance
def remove_element_from_array(array, value, collection, filter={}):
    """ Remove one element from array for some elements in function of filter """
    mongo_connection = get_mongo_db_connection()
    mongo_client = mongo_connection[collection]
    mongo_client.update_many(filter,{'$pull': { array:value}})

@common.database_catch_exceptions_and_performance
def remove_last_element_from_array(array, collection, filter={}):
    """ Remove one element from array for some elements in function of filter """
    mongo_connection = get_mongo_db_connection()
    mongo_client = mongo_connection[collection]
    mongo_client.update_many(filter,{'$pop': {array: 1}})

@common.database_catch_exceptions_and_performance
def add_element_at_array(array, value, collection, filter={}):
    """ Add one element to array for some elements in collection in function of filter  """
    mongo_connection = get_mongo_db_connection()
    mongo_client = mongo_connection[collection]
    mongo_client.update_many(filter,{'$push': { array:value}})

@common.database_catch_exceptions_and_performance
def count_records(filter, collection):
    """ count json documents at collection in function of filter """
    mongo_connection = get_mongo_db_connection()
    mongo_client = mongo_connection[collection]
    result = mongo_client.find(filter).count()
    return int(result)

@common.database_catch_exceptions_and_performance
def increase_counter_at_single_document(counter, value, collection, filter):
    """ increase counter at collection for one document in function of filter """
    mongo_connection = get_mongo_db_connection()
    mongo_client = mongo_connection[collection]
    mongo_client.update_one(filter,{'$inc': { counter: value}})

@common.database_catch_exceptions_and_performance
def increase_counter_at_multi_document(counter, value, collection, filter):
    """ increase counter at collection for one document in function of filter """
    mongo_connection = get_mongo_db_connection()
    mongo_client = mongo_connection[collection]
    mongo_client.update_many(filter,{'$inc': { counter: value}})

@common.database_catch_exceptions_and_performance
def read_document_with_order_and_limit(filter, collection, key, direction, limit):
    """ Read a several json documents from collection in function of filter """
    mongo_connection = get_mongo_db_connection()
    mongo_client = mongo_connection[collection]
    result = mongo_client.find(filter).sort(key, direction).limit(limit)
    return result

@common.database_catch_exceptions_and_performance
def remove_field_at_single_document(filter, collection, field):
    mongo_connection = get_mongo_db_connection()
    mongo_client = mongo_connection[collection]
    mongo_client.update_one(filter, {'$unset': { field: 1}})

@common.database_catch_exceptions_and_performance
def loop_recursively_into_dict(document_to_make_update, name, document):
    for entry in document:
        if isinstance(document[entry], TYPES_NOT_DICT):
            name_to_add = name + '.' + str(entry)
            document_to_make_update[name_to_add] = document[entry]
        elif isinstance(document[entry], dict):
            loop_recursively_into_dict(document_to_make_update, name + '.' + entry, document[entry])
    return document_to_make_update

@common.database_catch_exceptions_and_performance
def modify_document_to_update(document):
    document_to_make_update ={}
    for entry in document:
        if isinstance(document[entry], TYPES_NOT_DICT):
            document_to_make_update[str(entry)] = document[entry]
        elif isinstance(document[entry], dict):
            document_to_make_update = loop_recursively_into_dict(document_to_make_update, str(entry), document[entry])
    return document_to_make_update

@common.database_catch_exceptions_and_performance
def update_partial_single_document(document, filter, collection):
    """ Update a single json document at collection in function of filter """
    mongo_connection = get_mongo_db_connection()
    mongo_client = mongo_connection[collection]
    mongo_client.update_one(filter, {'$set': modify_document_to_update(document)})
