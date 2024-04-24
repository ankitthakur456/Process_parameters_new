import datetime
import schedule
import requests
import json
import sys
import time
import base64
import logging
import logging.handlers
from database import DBHelper
from logging.handlers import TimedRotatingFileHandler
import os
import struct
log_level = logging.INFO
from dotenv import load_dotenv
load_dotenv()
FORMAT = ('%(asctime)-15s %(levelname)-8s %(name)s %(module)-15s:%(lineno)-8s %(message)s')

logFormatter = logging.Formatter(FORMAT)
log = logging.getLogger("HIS_LOGS")
ob_db = DBHelper("EM_data")
# # checking and creating logs directory here

if getattr(sys, 'frozen', False):
    dirname = os.path.dirname(sys.executable)
else:
    dirname = os.path.dirname(os.path.abspath(__file__))
logdir = f"{dirname}/logs"
log.info(f"log directory name is {logdir}")
if not os.path.isdir(logdir):
    log.info("[-] logs directory doesn't exists")
    try:
        os.mkdir(logdir)
        log.info("[+] Created logs dir successfully")
    except Exception as e:
        log.info(f"[-] Can't create dir logs Error: {e}")

fileHandler = TimedRotatingFileHandler(f'{logdir}/app_log',
                                       when='midnight', interval=1)
fileHandler.setFormatter(logFormatter)
fileHandler.suffix = "%Y-%m-%d.log"
log.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
log.addHandler(consoleHandler)
log.setLevel(log_level)

#endregion
SENSOR_DATA_INTERVAL = 5
SAMPLE_RATE = 1

#ENV FILE VARIABLES
HOST=os.getenv('HOST')
ACCESS_TOKEN=os.getenv('ACCESS_TOKEN')
USERNAME = os.getenv('user')
print(USERNAME)
PASSWORD = os.getenv('PASSWORD')
HOST_SENSOR=os.getenv("HOST_SENSOR")
HOST_SENSOR1=os.getenv("HOST_SENSOR1")
ACCESS_TOKEN_SENSOR = os.getenv('ACCESS_TOKEN_SENSOR')
#END REGION

payload = {}
HEADERS_1 = {
              'Content-Type': 'application/json',
              'Authorization': 'Basic Og==',
              'Cookie': 'JSESSIONID=5BB7E1FA21C8A0A94FAA017C90CA32B1'
            }

HEADERS = {
  'Authorization': f'Bearer {ACCESS_TOKEN_SENSOR}'
        }


def get_plant_data():
    try:
        payload = {}
        req = requests.request("GET", HOST_SENSOR, headers=HEADERS, data=payload)
       # log.info(req.text)
        log.info(req.reason)
        if req.status_code in [401, 403]:
            pass
            refresh_jwt_token()
        raw_data = req.json()
        payload = {}
        #log.info(f'raw data is {raw_data}')
        id_list = []
        if raw_data is not None:
            max_id = max([m_data['id'] for m_data in raw_data])
            latest_data = {}
            for m_data in raw_data:
                if m_data['id'] == max_id:
                    latest_data = m_data
                    log.info(f'lstest data is {latest_data}')
        return payload
    except Exception as e:
        log.error(f"Error while reading plant data {e}")
    return []


def convert_hex_to_ieee754(hex_str):
    try:
        # currently converting to big endian
        decimal_value_big_endian = struct.unpack('>f', bytes.fromhex(hex_str))[0]
    except Exception as e:
        log.error(f"Error while converting {hex_str} to float: {e}")
        decimal_value_big_endian = 0.0

    return round(decimal_value_big_endian, 5)

def get_equipment_area_asset():
    try:
        payload = {}
        req = requests.request("POST", HOST_SENSOR1, headers=HEADERS, data=payload)
        log.info(req.status_code)
        #log.info(req.text)
        if req.status_code in [401, 403]:
          pass
          refresh_jwt_token()
        raw_data = req.json()
        payload = {}
        #log.info(f'raw data is {raw_data}')

        id_list = []
        if raw_data is not None:
            latest_data = raw_data['data']['externalTableResponse']

            latest_data1 = raw_data['data']['area']['machineGropus']['externalTableResponse'][0]['externalTableData']['00:30:11:78:A1:A6:11'][0]['process_parameter_001_k']                             #['externalTableResponse'][0]['externalTableData']['00:30:11:78:A1:A6:11']['process_parameter_001_k']  #['machineGropus']['externalTableResponse'][0]

            if latest_data:
                steam_temp = latest_data[0]['externalTableData']['00:30:11:78:A1:A6:01'][0]['process_parameter_001_a']
                #log.info(f'raw data is {steam_temp}')
                # if steam_temp is not None:
                #     max_id = max(steam_temp, key=lambda x: x['id'])
                    # print(max_id)
                    # steam = max_id['process_parameter_001_a']
                    # print(f'steam data is {steam}')
                paper_gsm = latest_data[1]['externalTableData']['00:30:11:78:A1:A6:02'][0]['process_parameter_001_b']
                machine_speed = latest_data[2]['externalTableData']['00:30:11:78:A1:A6:03'][0]['process_parameter_001_c']
                payload = {
                'Steam Temperature': convert_hex_to_ieee754(steam_temp),
                'Paper GSM': convert_hex_to_ieee754(paper_gsm),
                'Machine Speed': convert_hex_to_ieee754(machine_speed),
                'Steam Presure GP 8': convert_hex_to_ieee754(latest_data1)
                }
                log.info(payload)
                return(payload)
    except Exception as e:
        log.error(f"Error while getting the API data {e}")
    return []


def post_sensors_data(payload):
    try:
        if payload:
            host_tb = f'{HOST}/api/v1/{ACCESS_TOKEN}/telemetry/'
            log.info(f"Sending {payload} to {host_tb}")
            try:
                req_post = requests.post(host_tb, json=payload, headers=HEADERS, timeout=2)
                log.info(req_post.status_code)
                log.info(req_post.text)
                req_post.raise_for_status()
            except Exception as e:
                log.error(f"[-] Error While sending data to server {e}")
        else:
            log.info("got empty payload")
    except Exception as e:
        log.error(f"Error while sending data {e}")

def refresh_jwt_token():
    try:
        global ACCESS_TOKEN_SENSOR, HEADERS_1, USERNAME, PASSWORD, HEADERS
        ob_db.add_access_data(ACCESS_TOKEN_SENSOR)
        refresh_url = 'https://api.infinite-uptime.com/api/3.0/idap-api/login'
        a_payload =json.dumps({'username': base64.b64decode(USERNAME).decode('ascii'),
                     'password': base64.b64decode(PASSWORD).decode('ascii')})
        req = requests.request("POST", refresh_url, headers=HEADERS_1, data=a_payload)
        log.info(req.status_code)
        raw_data = req.json()
        if raw_data:
            access_token = raw_data['data']['accessToken']
            if access_token:
                ob_db.add_access_data(access_token)
        at = ob_db.get_access_data()
        if at:
            ACCESS_TOKEN_SENSOR = at
            HEADERS = {'Authorization': f"Bearer {ACCESS_TOKEN_SENSOR}"}
    except Exception as e:
        log.error(f"Error while Refreshing JWT TOKEN: {e}")

if __name__ == "__main__":
    try:
        while True:
            access_token = ob_db.get_access_data()
            if access_token is None:
                log.info("[+] Error No Access Token is Found Refreshing...")
                refresh_jwt_token()
            data = get_equipment_area_asset()
            log.info(data)
            # data2 = get_plant_data()
            # log.info(f'plant data is {data2}')
            at = ob_db.get_access_data()
            ACCESS_TOKEN_SENSOR = at
            HEADERS['Authorization'] = f'Bearer {ACCESS_TOKEN_SENSOR}'
            post_sensors_data(data)
            #schedule.run_pending()
            time.sleep(5)
    except Exception as e:
        log.info(f"Error Running Program {e}")