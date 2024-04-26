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
from datetime import datetime
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
USERNAME = os.getenv('USER')
PASSWORD = os.getenv('PASSWORD')
HOST_SENSOR=os.getenv("HOST_SENSOR")
HOST_SENSOR1=os.getenv("HOST_SENSOR1")
ACCESS_TOKEN_SENSOR = ob_db.get_access_data()
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
        all_payload = []
        # Get the current date in the format YYYY-MM-DD
        current_date = datetime.now().strftime('%Y-%m-%d')
        date = "2024-04-01"
        print(date)
        plant_id = [3047, 3048, 3049, 3058, 3059, 3060, 3061, 3062, 3063]
        for pid in plant_id:
            url = f"{HOST_SENSOR1}equipmentId={pid}&start={date}T12%3A13%3A14Z&end={current_date}T12%3A13%3A14Z&page=1&pageSize=10"
            req1 = requests.request("POST", url, headers=HEADERS, data=payload)
            #print(req1.json())  # Print the response
            if req1.status_code in [401, 403]:
                pass
                refresh_jwt_token()
            raw_data = req1.json()

            id_list = []
            if raw_data is not None:
                payload = {}
                if pid ==3047:
                    latest_data_a = raw_data['data']['externalTableResponse'][0]['externalTableData']['00:30:11:78:A1:A6:01'][0]['process_parameter_001_a']
                    payload['Steam Temperature']= convert_hex_to_ieee754(latest_data_a)
                    latest_data_b= raw_data['data']['externalTableResponse'][1]['externalTableData']['00:30:11:78:A1:A6:02'][0]['process_parameter_001_b']
                    payload['Paper GSM']= convert_hex_to_ieee754(latest_data_b)
                    latest_data_c = raw_data['data']['externalTableResponse'][2]['externalTableData']['00:30:11:78:A1:A6:03'][0]['process_parameter_001_c']
                    payload['Machine Speed']= convert_hex_to_ieee754(latest_data_c)
                    latest_data_d = raw_data['data']['area']['machineGropus']['externalTableResponse'][0]['externalTableData']['00:30:11:78:A1:A6:04'][0]['process_parameter_001_d']
                    payload['Steam Presure GP1']= convert_hex_to_ieee754(latest_data_d)
                    post_sensors_data(payload)
                elif pid == 3048:
                    latest_data_e = raw_data['data']['area']['machineGropus']['externalTableResponse'][0]['externalTableData']['00:30:11:78:A1:A6:05'][0]['process_parameter_001_e']
                    payload['Steam Presure GP2'] = convert_hex_to_ieee754(latest_data_e)
                    post_sensors_data(payload)
                elif pid == 3049:
                    latest_data_f =  raw_data['data']['area']['machineGropus']['externalTableResponse'][0]['externalTableData']['00:30:11:78:A1:A6:06'][0]['process_parameter_001_f']
                    payload['Steam Presure GP3'] = convert_hex_to_ieee754(latest_data_f)
                    post_sensors_data(payload)
                elif pid ==3058:
                    latest_data_g = raw_data['data']['area']['machineGropus']['externalTableResponse'][0]['externalTableData']['00:30:11:78:A1:A6:07'][0]['process_parameter_001_g']
                    payload["Steam Presure GP4"]=convert_hex_to_ieee754(latest_data_g)
                    post_sensors_data(payload)
                elif pid == 3059:
                    latest_data_h = raw_data['data']['area']['machineGropus']['externalTableResponse'][0]['externalTableData']['00:30:11:78:A1:A6:08'][0]['process_parameter_001_h']
                    payload['Steam Presure GP5']= convert_hex_to_ieee754(latest_data_h)
                    post_sensors_data(payload)
                elif pid == 3060:
                    latest_data_i = raw_data['data']['area']['machineGropus']['externalTableResponse'][0]['externalTableData']['00:30:11:78:A1:A6:09'][0]['process_parameter_001_i']
                    payload['Steam Presure GP6']= convert_hex_to_ieee754(latest_data_i)
                    post_sensors_data(payload)
                elif pid == 3061:
                    latest_data_j = raw_data['data']['area']['machineGropus']['externalTableResponse'][0]['externalTableData']['00:30:11:78:A1:A6:10'][0]['process_parameter_001_j']
                    payload['Steam Presure GP7'] =  convert_hex_to_ieee754(latest_data_j)
                    post_sensors_data(payload)
                elif pid == 3062:
                    latest_data_k = raw_data['data']['area']['machineGropus']['externalTableResponse'][0]['externalTableData']['00:30:11:78:A1:A6:11'][0]['process_parameter_001_k']
                    payload['Steam Presure GP8']= convert_hex_to_ieee754(latest_data_k)
                    post_sensors_data(payload)
                elif pid == 3063:
                    latest_data_k = raw_data['data']['area']['machineGropus']['externalTableResponse'][0]['externalTableData']['00:30:11:78:A1:A6:12'][0]['process_parameter_001_l']
                    payload['Steam Presure GP9']= convert_hex_to_ieee754(latest_data_k)
                    post_sensors_data(payload)

                time.sleep(5)
        #         all_payload.append(payload)
        #
        # log.info(payload)
        # return(all_payload)
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
            at = ob_db.get_access_data()
            ACCESS_TOKEN_SENSOR = at
            HEADERS['Authorization'] = f'Bearer {ACCESS_TOKEN_SENSOR}'
            #post_sensors_data(data)
            #schedule.run_pending()
            time.sleep(5)
    except Exception as e:
        log.info(f"Error Running Program {e}")