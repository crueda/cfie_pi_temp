
import asyncio
import requests
from concurrent.futures import ProcessPoolExecutor
from xml.etree import cElementTree as ET
from utils import decorator
from config import config
from logs import loggerMain
from databases import mongo
from bson import ObjectId

WORKER = config.WORKER
geocoding_executor = ProcessPoolExecutor(max_workers=int(WORKER['geocoding_worker_count']))
logger = loggerMain.get_logger()

GEOCODING_INFO = config.GEOCODING
GOOGLE_INFO = GEOCODING_INFO['google']
NOMINATIM_INFO = GEOCODING_INFO['nominatim']

google_url = 'http://' + GOOGLE_INFO['host'] + GOOGLE_INFO['path']
nominatim_url = 'http://' + NOMINATIM_INFO['host'] + NOMINATIM_INFO['path']


@decorator.catch_exceptions
def build_google_payload(latitude, longitude):
    """ Compose payload for Google geocoding request from latitude and longitude """
    coordinates = latitude + ',' + longitude
    payload = 'latlng=' + coordinates + "&language=es&client=" + GOOGLE_INFO['client'] + "&signature=" + GOOGLE_INFO['signature'] + "=&result_type=route"
    return payload


@decorator.catch_exceptions
def build_osm_payload(latitude, longitude):
    """ Compose payload for OSM geocoding request from latitude and longitude """
    payload = 'format=json&lat=' + latitude + '&lon=' + longitude + '&accept-language=es'
    return payload


@decorator.catch_exceptions
def extract_data_from_google_response(geocoding_response):
    """ Extract util information (formatted_adddress) from Google geocoding response """
    root = ET.fromstring(geocoding_response)
    for result in root.findall('result'):
        data = result.find('formatted_address').text
        if data != '':
            return data
    return 'Dirección desconocida'


@decorator.catch_exceptions
def extract_data_from_nominatim_response(geocoding_response):
    """ Extract util information (formatted_adddress) from local Nominatim geocoding response """
    root = ET.fromstring(geocoding_response)
    for result in root.findall('result'):
        data = result.find('formatted_address').text
        if data != '':
            return data
    return 'Dirección desconocida'

@decorator.catch_exceptions_with_return_none
def get_coordinates_from_id(tracking_id=None, event_id=None):
    """ Get coordinates for tracking_id or event_id previously saved at MongoDB """
    if tracking_id:
        json_document = mongo.read_single_document(collection='TRACKING', filter={'_id':ObjectId(tracking_id)}, projection={'coordinates':True})
        if not json_document:
            json_document = mongo.read_single_document(collection='TRACKING', filter={'_id': ObjectId(tracking_id)}, projection={'coordinates':True})
        return {'latitude': str(json_document['coordinates'][1]), 'longitude': str(json_document['coordinates'][0])}
    elif event_id:
        json_document = mongo.read_single_document(collection='EVENT', filter={'_id':ObjectId(event_id)}, projection={'coordinates':True})
        if json_document:
            return {'latitude': str(json_document['coordinates'][1]), 'longitude': str(json_document['coordinates'][0])}
    else:
        return None

@decorator.catch_exceptions_with_return_none
def get_google_geocoding(coordinates):
    """ Get geocoding from google provider """
    latitude = coordinates['latitude']
    longitude = coordinates['longitude']
    payload = build_google_payload(latitude=latitude, longitude=longitude)
    response = requests.get(google_url, params=payload, timeout=int(GEOCODING_INFO['timeout']))
    if response.status_code == 200:
        formated_data = extract_data_from_google_response(response.text.encode('utf-8'))
        return formated_data
    else:
        return None

@decorator.catch_exceptions_with_return_none
def get_osm_geocoding(coordinates):
    """ Get geocoding from OSM provider """
    latitude = coordinates['latitude']
    longitude = coordinates['longitude']
    payload = build_osm_payload(latitude=latitude, longitude=longitude)
    response = requests.get(nominatim_url, params=payload, timeout=int(GEOCODING_INFO['timeout'])).json()
    formated_data = response['display_name']
    return formated_data


@decorator.catch_exceptions
def sync_set_geocoding(provider, tracking_id, event_id):
    """ Set geocoding for one tracking_id or/and event_id already saved at mongo """
    coordinates = get_coordinates_from_id(tracking_id=tracking_id, event_id=event_id)
    geocoding = None
    if coordinates:
        if not provider or provider == 'osm':
            geocoding = get_osm_geocoding(coordinates)
            if geocoding == None:
                geocoding = get_google_geocoding(coordinates)
        elif provider == 'google':
            geocoding = get_google_geocoding(coordinates)
            if geocoding == None:
                geocoding = get_osm_geocoding(coordinates)
        if geocoding:
            if tracking_id:
                json_entry = dict()
                json_entry['geocoding'] = str(geocoding)
                mongo.update_single_document(collection='TRACKING', document=json_entry, filter={'_id':ObjectId(tracking_id)})
            if event_id:
                json_entry = dict()
                json_entry['geocoding'] = str(geocoding)
                mongo.update_single_document(collection='EVENT', document=json_entry, filter={'_id': ObjectId(event_id)})


@decorator.catch_exceptions
def async_set_geocoding(provider, tracking_id=None, event_id=None):
    """ Async set geocoding for one tracking_id or/and event_id already saved at mongo"""
    loop = asyncio.get_event_loop()
    loop.run_in