import datetime
import schedule
import requests
import os
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

# checking and creating logs directory here

if getattr(sys, 'frozen', False):
    dirname = os.path.dirname(sys.executable)
else:
    dirname = os.path.dirname(os.path.abspath(__file__))

logdir = f"{dirname}/logs"
print(f"log directory name is {logdir}")
if not os.path.isdir(logdir):
    log.info("[-] logs directory doesn't exists")
    try:
        os.mkdir(logdir)
        log.info("[+] Created logs dir successfully")
    except Exception as e:
        log.error(f"[-] Can't create dir logs Error: {e}")

fileHandler = TimedRotatingFileHandler(f'{logdir}/app_log',
                                       when='midnight', interval=1)
fileHandler.setFormatter(logFormatter)
fileHandler.suffix = "%Y-%m-%d.log"
log.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
log.addHandler(consoleHandler)

log.setLevel(log_level)

# endregion
SENSOR_DATA_INTERVAL = 5
SAMPLE_RATE = 1
HOST = 'https://ithingspro.cloud'

SEND_DATA = True

machine_obj = {}

ob_db = DBHelper("EM_data")

prev_sensor_data_sent = time.time()

ACCESS_TOKEN = 'Nmz6IGFsFEjhmFhDjThD'

HEADERS = {'content-type': 'application/json'}

HOST_SENSOR = 'https://api.infinite-uptime.com/api/3.0/idap-api'
print(HOST_SENSOR)

ACCESS_TOKEN_SENSOR = 'eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICIyY0NxRnpxRXJrNU5GS1RjTi1YSk1IdE9NS2tWVTZXS1hIdHZFMF8xZE5ZIn0.eyJleHAiOjE3MTAyNjI1NTIsImlhdCI6MTcxMDIxOTM1MiwianRpIjoiZTcxZWMyNzItNDNkZC00MzI3LTgwNzYtZWVhYmU1Zjc5NjFkIiwiaXNzIjoiaHR0cHM6Ly9pZGVudGl0eS5pbmZpbml0ZS11cHRpbWUuY29tL3JlYWxtcy9pZGFwIiwic3ViIjoiZjo2MGNhNzY4Yy1iMTA0LTQ0OTktYjU4Yy05MzliOTdlNzAzM2Q6OTg3NCIsInR5cCI6IkJlYXJlciIsImF6cCI6ImlkYXAiLCJzZXNzaW9uX3N0YXRlIjoiNGE2MzM3MzItYzQxZC00ZjEyLWJlMTYtZDAwMTEzZTgzZWNlIiwiYWxsb3dlZC1vcmlnaW5zIjpbIioiXSwic2NvcGUiOiJwcm9maWxlIGVtYWlsIiwic2lkIjoiNGE2MzM3MzItYzQxZC00ZjEyLWJlMTYtZDAwMTEzZTgzZWNlIiwiaXNfYWRtaW4iOiJmYWxzZSIsImVtYWlsX3ZlcmlmaWVkIjpmYWxzZSwibmFtZSI6IlNoaXZhbSBNYXVyeWEiLCJpZGFwX3JvbGUiOiJST0xFX1VTRVIiLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJzaGl2YW1tYXVyeWEwMjQ4QGdtYWlsLmNvbSIsImdpdmVuX25hbWUiOiJTaGl2YW0iLCJsb2NhbGUiOiJlbiIsImZhbWlseV9uYW1lIjoiTWF1cnlhIiwiZW1haWwiOiJzaGl2YW1tYXVyeWEwMjQ4QGdtYWlsLmNvbSJ9.mUBIzeh5Vny593evB7MHU0oe4SGCPv8wNcup-pDrBG9ug_vLoa72nq9Z2sw87e_4QHlPUxhTL-LcnLYBjTfgHxNlyTbkCeL6YfZA3bQH2e-jmkSBatBOu1d4u27xTSOR-QCmAbAAniz61wLzvfIvz8kyzHzXdSn_9frrIQsoMCVDo0anWaiasaP7HHy5Tj1KLVIRfPMsfsvQX92XkkIPx_HMD4X2ge7nQWEiutp499kpoEuOz2beriHNQrr2eYvcmUJk1rQuw3bNNq9g2c1F7Rbl6R5KZgKSDpN3QyS3p6masP5SAwBisDbBDJ9VrX4_-f5pCJk2NtoFao7Eklk1Gg'

PASSWORD = "'RTRzeVA0JCR3MHJk'"
USERNAME = "'c2hpdmFtbWF1cnlhMDI0OEBnbWFpbC5jb20='"

HEADERS_SENSOR = {'content-type': 'application/json',
                  'Authorization': f"Bearer {ACCESS_TOKEN_SENSOR}"}

sensors = {
    "STEAM_TEMP": {
        "tableName": "external_device_data_ald_steam_001_a_763"
    },
    "SPEED": {
        "tableName": "external_device_data_ald_speed_001_b_763"
    },
    "PAPER_GSM": {
        "tableName": "external_device_data_ald_gsm_001_c_763"
    },
    "STEAM_PRGP_1": {
        "tableName": "external_device_data_ald_gp1_001_d_763"
    },
    "STEAM_PRGP_2": {
        "tableName": "external_device_data_ald_gp2_001_e_763"
    },
    "STEAM_PRGP_3": {
        "tableName": "external_device_data_ald_gp3_001_f_763"
    },
    "STEAM_PRGP_4": {
        "tableName": "external_device_data_ald_gp4_001_g_763"
    },
    "STEAM_PRGP_5": {
        "tableName": "external_device_data_ald_gp5_001_h_763"
    },
    "STEAM_PRGP_6": {
        "tableName": "external_device_data_ald_gp6_001_i_763"
    },
    "STEAM_PRGP_7": {
        "tableName": "external_device_data_ald_gp7_001_j_763"
    },
    "STEAM_PRGP_8": {
        "tableName": "external_device_data_ald_gp8_001_k_763"
    },
    "STEAM_PRGP_9": {
        "tableName": "external_device_data_ald_gp9_001_l_763"
    }
}


def generate_sensor_config(sensor_name):
    config = {
        "filters": [
            {
                "fields": [],
                "nextCondition": "string",
                "fieldsCondition": "string"
            }
        ],
        "tableName": sensors[sensor_name]["tableName"],  # Access tableName dynamically
        "sortField": [{"field": "id", "dir": "desc"}],
        "groupBy": [],
        "functions": [],
        "page": 1,
        "pageSize": 200,
        "selectedFields": []
    }
    return config


STEAM_TEMP = generate_sensor_config("STEAM_TEMP")
SPEED = generate_sensor_config("SPEED")
PAPER_GSM = generate_sensor_config("PAPER_GSM")
STEAM_PRGP_1 = generate_sensor_config("STEAM_PRGP_1")
STEAM_PRGP_2 = generate_sensor_config("STEAM_PRGP_2")
STEAM_PRGP_3 = generate_sensor_config("STEAM_PRGP_3")
STEAM_PRGP_4 = generate_sensor_config("STEAM_PRGP_4")
STEAM_PRGP_5 = generate_sensor_config("STEAM_PRGP_5")
STEAM_PRGP_6 = generate_sensor_config("STEAM_PRGP_6")
STEAM_PRGP_7 = generate_sensor_config("STEAM_PRGP_7")
STEAM_PRGP_8 = generate_sensor_config("STEAM_PRGP_8")
STEAM_PRGP_9 = generate_sensor_config("STEAM_PRGP_9")


def convert_hex_to_ieee754(hex_str):
    try:
        # currently converting to big endian
        decimal_value_big_endian = struct.unpack('>f', bytes.fromhex(hex_str))[0]
    except Exception as e:
        log.error(f"Error while converting {hex_str} to float: {e}")
        decimal_value_big_endian = 0.0

    return round(decimal_value_big_endian, 5)


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


def get_steam_data():
    try:
        req = requests.post(f'{HOST_SENSOR}/advance-query?organizationId=763', json=STEAM_TEMP, headers=HEADERS_SENSOR,
                            timeout=2)
        log.info(req.status_code)
        if req.status_code in [401, 403]:
            refresh_jwt_token()
            req = requests.post(f'{HOST_SENSOR}/advance-query?organizationId=763', json=STEAM_TEMP,
                                headers=HEADERS_SENSOR,timeout=2)
        raw_data = req.json()
        payload = {}
        id_list = []
        if raw_data is not None:
            max_id = max([m_data['id'] for m_data in raw_data])
            latest_data = {}
            for m_data in raw_data:
                if m_data['id'] == max_id:
                    latest_data = m_data
                    break
            if latest_data:
                payload = {
                    'INF_UPTM_PR_MAIN_TEMP_DEG_CELCIUS': convert_hex_to_ieee754(latest_data['process_parameter_001_a']),
                }
                post_sensors_data(payload)
        return payload
    except Exception as e:
        log.error(f"Error while sending the Steam data {e}")
        return []


def get_speed_data():
    try:
        req = requests.post(f'{HOST_SENSOR}/advance-query?organizationId=763', json=SPEED, headers=HEADERS_SENSOR,
                            timeout=2)
        log.info(req.status_code)
        if req.status_code in [401, 403]:
            refresh_jwt_token()
            req = requests.post(f'{HOST_SENSOR}/advance-query?organizationId=763', json=SPEED, headers=HEADERS_SENSOR,
                                timeout=2)
        raw_data = req.json()
        payload = {}
        id_list = []
        if raw_data is not None:
            max_id = max([m_data['id'] for m_data in raw_data])
            latest_data = {}
            for m_data in raw_data:
                if m_data['id'] == max_id:
                    latest_data = m_data
                    break
            if latest_data:
                hex_value = latest_data['process_parameter_001_b']
                float_value = struct.unpack('!f', bytes.fromhex(hex_value))[0]
                payload = {

                    'INF_UPTM_MAL_SPEED_MPM': round(float_value, 2),
                }
                post_sensors_data(payload)
        return payload
    except Exception as e:
        log.error(f"Error while sending the speed data {e}")
        return []


def get_GSM_data():
    try:
        req = requests.post(f'{HOST_SENSOR}/advance-query?organizationId=763', json=PAPER_GSM, headers=HEADERS_SENSOR,
                            timeout=2)
        log.info(req.status_code)
        if req.status_code in [401, 403]:
            refresh_jwt_token()
            req = requests.post(f'{HOST_SENSOR}/advance-query?organizationId=763', json=PAPER_GSM,
                                headers=HEADERS_SENSOR, timeout=2)
        raw_data = req.json()
        payload = {}
        id_list = []
        if raw_data is not None:
            max_id = max([m_data['id'] for m_data in raw_data])
            latest_data = {}
            for m_data in raw_data:
                if m_data['id'] == max_id:
                    latest_data = m_data
                    break
            if latest_data:
                hex_value = latest_data['process_parameter_001_c']
                float_value = struct.unpack('!f', bytes.fromhex(hex_value))[0]
                payload = {
                    'INF_UPTM_GSM_GSM': float_value,
                }
                post_sensors_data(payload)
        return payload
    except Exception as e:
        log.error(f"Error while sending the gsm data {e}")
        return []


def get_steam1_data():
    try:
        req = requests.post(f'{HOST_SENSOR}/advance-query?organizationId=763', json=STEAM_PRGP_1,
                            headers=HEADERS_SENSOR, timeout=2)
        log.info(req.status_code)
        if req.status_code in [401, 403]:
            refresh_jwt_token()
            req = requests.post(f'{HOST_SENSOR}/advance-query?organizationId=763', json=STEAM_PRGP_1,
                                headers=HEADERS_SENSOR, timeout=2)
        raw_data = req.json()
        payload = {}
        id_list = []
        if raw_data is not None:
            max_id = max([m_data['id'] for m_data in raw_data])
            latest_data = {}
            for m_data in raw_data:
                if m_data['id'] == max_id:
                    latest_data = m_data
                    break
            if latest_data:
                hex_value = latest_data['process_parameter_001_d']
                float_value = struct.unpack('!f', bytes.fromhex(hex_value))[0]
                payload = {
                    'INF_UPTM_STEAM_PR_GP1_BAR': round(float_value, 2),
                }
                post_sensors_data(payload)
        return payload
    except Exception as e:
        log.error(f"Error while sending the steam1 data {e}")
        return []


def get_steam2_data():
    try:
        req = requests.post(f'{HOST_SENSOR}/advance-query?organizationId=763', json=STEAM_PRGP_2,
                            headers=HEADERS_SENSOR, timeout=2)
        log.info(req.status_code)
        if req.status_code in [401, 403]:
            refresh_jwt_token()
            req = requests.post(f'{HOST_SENSOR}/advance-query?organizationId=763', json=STEAM_PRGP_2,
                                headers=HEADERS_SENSOR, timeout=2)
        raw_data = req.json()
        payload = {}
        id_list = []
        if raw_data is not None:
            max_id = max([m_data['id'] for m_data in raw_data])
            latest_data = {}
            for m_data in raw_data:
                if m_data['id'] == max_id:
                    latest_data = m_data
                    break
            if latest_data:
                hex_value = latest_data['process_parameter_001_e']
                float_value = struct.unpack('!f', bytes.fromhex(hex_value))[0]
                payload = {
                    'INF_UPTM_STEAM_PR_GP2_BAR': float_value,
                }
                post_sensors_data(payload)
        return payload
    except Exception as e:
        log.error(f"Error while sending the steam2 data {e}")
        return []


def get_steam3_data():
    try:
        req = requests.post(f'{HOST_SENSOR}/advance-query?organizationId=763', json=STEAM_PRGP_3,
                            headers=HEADERS_SENSOR, timeout=2)
        log.info(req.status_code)
        if req.status_code in [401, 403]:
            refresh_jwt_token()
            req = requests.post(f'{HOST_SENSOR}/advance-query?organizationId=763', json=STEAM_PRGP_3,
                                headers=HEADERS_SENSOR, timeout=2)
        raw_data = req.json()
        payload = {}
        id_list = []
        if raw_data is not None:
            max_id = max([m_data['id'] for m_data in raw_data])
            latest_data = {}
            for m_data in raw_data:
                if m_data['id'] == max_id:
                    latest_data = m_data
                    break
            if latest_data:
                hex_value = latest_data['process_parameter_001_f']
                float_value = struct.unpack('!f', bytes.fromhex(hex_value))[0]
                payload = {
                    'INF_UPTM_STEAM_PR_GP3_BAR': float_value,
                }
                post_sensors_data(payload)
        return payload
    except Exception as e:
        log.error(f"Error while sending the steam3 data {e}")
        return []


def get_steam4_data():
    try:
        req = requests.post(f'{HOST_SENSOR}/advance-query?organizationId=763', json=STEAM_PRGP_4,
                            headers=HEADERS_SENSOR, timeout=2)
        log.info(req.status_code)
        if req.status_code in [401, 403]:
            refresh_jwt_token()
            req = requests.post(f'{HOST_SENSOR}/advance-query?organizationId=763', json=STEAM_PRGP_4,
                                headers=HEADERS_SENSOR, timeout=2)
        raw_data = req.json()
        payload = {}
        id_list = []
        if raw_data is not None:
            max_id = max([m_data['id'] for m_data in raw_data])
            latest_data = {}
            for m_data in raw_data:
                if m_data['id'] == max_id:
                    latest_data = m_data
                    break
            if latest_data:
                hex_value = latest_data['process_parameter_001_g']
                float_value = struct.unpack('!f', bytes.fromhex(hex_value))[0]
                payload = {
                    'INF_UPTM_STEAM_PR_GP4_BAR': float_value,
                }
                post_sensors_data(payload)
        return payload
    except Exception as e:
        log.error(f"Error while sending the steam4 data {e}")
        return []


def get_steam5_data():
    try:
        req = requests.post(f'{HOST_SENSOR}/advance-query?organizationId=763', json=STEAM_PRGP_5,
                            headers=HEADERS_SENSOR, timeout=2)
        log.info(req.status_code)
        if req.status_code in [401, 403]:
            refresh_jwt_token()
            req = requests.post(f'{HOST_SENSOR}/advance-query?organizationId=763', json=STEAM_PRGP_5,
                                headers=HEADERS_SENSOR, timeout=2)
        raw_data = req.json()
        payload = {}
        id_list = []
        if raw_data is not None:
            max_id = max([m_data['id'] for m_data in raw_data])
            latest_data = {}
            for m_data in raw_data:
                if m_data['id'] == max_id:
                    latest_data = m_data
                    break
            if latest_data:
                hex_value = latest_data['process_parameter_001_h']
                float_value = struct.unpack('!f', bytes.fromhex(hex_value))[0]
                payload = {
                    'INF_UPTM_STEAM_PR_GP5_BAR': float_value,
                }
                post_sensors_data(payload)
        return payload
    except Exception as e:
        log.error(f"Error while sending the steam5 data {e}")
        return []


def get_steam6_data():
    try:
        req = requests.post(f'{HOST_SENSOR}/advance-query?organizationId=763', json=STEAM_PRGP_6,
                            headers=HEADERS_SENSOR, timeout=2)
        log.info(req.status_code)
        if req.status_code in [401, 403]:
            refresh_jwt_token()
            req = requests.post(f'{HOST_SENSOR}/advance-query?organizationId=763', json=STEAM_PRGP_6,
                                headers=HEADERS_SENSOR, timeout=2)
        raw_data = req.json()
        payload = {}
        id_list = []
        if raw_data is not None:
            max_id = max([m_data['id'] for m_data in raw_data])
            latest_data = {}
            for m_data in raw_data:
                if m_data['id'] == max_id:
                    latest_data = m_data
                    break
            if latest_data:
                hex_value = latest_data['process_parameter_001_i']
                float_value = struct.unpack('!f', bytes.fromhex(hex_value))[0]
                payload = {
                    'INF_UPTM_STEAM_PR_GP6_BAR': float_value,
                }
                post_sensors_data(payload)
        return payload
    except Exception as e:
        log.error(f"Error while sending the steam6 data {e}")
        return []


def get_steam7_data():
    try:
        req = requests.post(f'{HOST_SENSOR}/advance-query?organizationId=763', json=STEAM_PRGP_7,
                            headers=HEADERS_SENSOR, timeout=2)
        log.info(req.status_code)
        if req.status_code in [401, 403]:
            refresh_jwt_token()
            req = requests.post(f'{HOST_SENSOR}/advance-query?organizationId=763', json=STEAM_PRGP_7,
                                headers=HEADERS_SENSOR, timeout=2)
        raw_data = req.json()
        payload = {}
        id_list = []
        if raw_data is not None:
            max_id = max([m_data['id'] for m_data in raw_data])
            latest_data = {}
            for m_data in raw_data:
                if m_data['id'] == max_id:
                    latest_data = m_data
                    break
            if latest_data:
                hex_value = latest_data['process_parameter_001_j']
                float_value = struct.unpack('!f', bytes.fromhex(hex_value))[0]
                payload = {
                    'INF_UPTM_STEAM_PR_GP7_BAR': float_value,
                }
                post_sensors_data(payload)
        return payload
    except Exception as e:
        log.error(f"Error while sending the steam7 data {e}")
        return []


schedule.every(SENSOR_DATA_INTERVAL).seconds.do(get_steam7_data)


def get_steam8_data():
    try:
        req = requests.post(f'{HOST_SENSOR}/advance-query?organizationId=763', json=STEAM_PRGP_8,
                            headers=HEADERS_SENSOR, timeout=2)
        log.info(req.status_code)
        if req.status_code in [401, 403]:
            refresh_jwt_token()
            req = requests.post(f'{HOST_SENSOR}/advance-query?organizationId=763', json=STEAM_PRGP_8,
                                headers=HEADERS_SENSOR, timeout=2)
        raw_data = req.json()
        payload = {}
        if raw_data is not None:
            max_id = max([m_data['id'] for m_data in raw_data])
            latest_data = {}
            for m_data in raw_data:
                if m_data['id'] == max_id:
                    latest_data = m_data
                    break
            if latest_data:
                hex_value = latest_data['process_parameter_001_k']
                float_value = struct.unpack('!f', bytes.fromhex(hex_value))[0]
                payload = {
                    'INF_UPTM_STEAM_PR_GP8_BAR': float_value,
                }
                post_sensors_data(payload)
        return payload
    except Exception as e:
        log.error(f"Error while sending the steam8 data {e}")
        return []


def get_steam9_data():
    try:
        req = requests.post(f'{HOST_SENSOR}/advance-query?organizationId=763', json=STEAM_PRGP_9,
                            headers=HEADERS_SENSOR, timeout=2)
        log.info(req.status_code)
        if req.status_code in [401, 403]:
            refresh_jwt_token()
            req = requests.post(f'{HOST_SENSOR}/advance-query?organizationId=763', json=STEAM_PRGP_9,
                                headers=HEADERS_SENSOR, timeout=2)
        raw_data = req.json()
        payload = {}
        id_list = []
        if raw_data is not None:
            max_id = max([m_data['id'] for m_data in raw_data])
            latest_data = {}
            for m_data in raw_data:
                if m_data['id'] == max_id:
                    latest_data = m_data
                    break
            if latest_data:
                hex_value = latest_data['process_parameter_001_l']
                float_value = struct.unpack('!f', bytes.fromhex(hex_value))[0]
                payload = {
                    'INF_UPTM_STEAM_PR_GP9_BAR': float_value,
                }
                post_sensors_data(payload)
        return payload
    except Exception as e:
        log.error(f"Error while sending the steam9 data {e}")
        return []


functions_to_schedule = [
    get_steam_data,
    get_speed_data,
    get_GSM_data,
    get_steam1_data,
    get_steam2_data,
    get_steam3_data,
    get_steam4_data,
    get_steam5_data,
    get_steam6_data,
    get_steam8_data,
    get_steam9_data
]

for function in functions_to_schedule:
    schedule.every(SENSOR_DATA_INTERVAL).seconds.do(function)


def refresh_jwt_token():
    try:
        global ACCESS_TOKEN_SENSOR, HEADERS_SENSOR, USERNAME, PASSWORD
        ob_db.add_access_data(ACCESS_TOKEN_SENSOR)
        refresh_url = f"{HOST_SENSOR}/login"
        a_payload = {'username': base64.b64decode(USERNAME).decode('ascii'),
                     'password': base64.b64decode(PASSWORD).decode('ascii')}

        req = requests.post(refresh_url, json=a_payload, headers=HEADERS, timeout=2)
        log.info(req.status_code)

        raw_data = req.json()
        if raw_data:
            access_token = raw_data['data']['accessToken']
            if access_token:
                ob_db.add_access_data(access_token)

        at = ob_db.get_access_data()
        if at:
            ACCESS_TOKEN_SENSOR = at
            HEADERS_SENSOR = {'content-type': 'application/json',
                              'Authorization': f"Bearer {ACCESS_TOKEN_SENSOR}"}
    except Exception as e:
        log.error(f"Error while Refreshing JWT TOKEN: {e}")


if __name__ == "__main__":
    try:
        access_token = ob_db.get_access_data()
        if access_token is None:
            log.info("[+] Error No Access Token is Found Refreshing...")
        refresh_jwt_token()

        at = ob_db.get_access_data()
        ACCESS_TOKEN_SENSOR = at
        HEADERS_SENSOR['Authorization'] = f'Bearer {ACCESS_TOKEN_SENSOR}'
        while True:
            schedule.run_pending()
            time.sleep(2)
    except Exception as e:
        log.error(f"Error Running Program {e}")
