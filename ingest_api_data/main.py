import time
import json
import requests
import datetime
import asyncio
import aiohttp
from google.cloud import pubsub_v1
from ingest_api_data.constants import TOKEN, PUBSUB_TOPIC

# -----------------------------------------------------------------------------#
# GLOBAL VARIABLES
# -----------------------------------------------------------------------------#
ALL_SITES_URL = f'https://te-data-test.herokuapp.com/api/sites?token={TOKEN}'
ALL_SIGNALS_URL_PREFIX = f'https://te-data-test.herokuapp.com/api/signals?token={TOKEN}&site='

MAX_RETRY = 5
RETRY_INTERAL_SECONDS = 2

# -----------------------------------------------------------------------------#
# UTIL FUNCTIONS
# -----------------------------------------------------------------------------#


def processTime(raw_timestamp):
    # input: 'Wed, 25 Nov 2020 18:19:24 GMT'
    time_str_arr = raw_timestamp.split(' ')
    time_temp = ' '.join(time_str_arr[1:-1])
    year_month_day = datetime.datetime.strptime(
        time_temp, '%d %b %Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
    output = year_month_day + ' UTC'
    # output: '2020-11-25 18:19:24 GMT'
    return output


def pushData(publisher, event):
    event = str.encode(json.dumps(event))
    # Publishing our data into the topic
    publisher.publish(PUBSUB_TOPIC, event)


def getValidateValue(hashMap, key): return hashMap[key] if (
    hashMap and key in hashMap and 'nan' not in str(hashMap[key]).lower()) else None

# -----------------------------------------------------------------------------#
# API CALLING FUNCTIONS
# -----------------------------------------------------------------------------#


async def fetch(session, url, publisher, current_event_timestamp):

    for i in range(MAX_RETRY):
        async with session.get(url) as response:
            site_payload = await response.json()
            signals = site_payload['signals'] if 'signals' in site_payload else None

            # if we couldn't find signals in the data, retry
            if not signals and i < MAX_RETRY-1:
                await asyncio.sleep(RETRY_INTERAL_SECONDS)
                continue

            # extract the values from API
            timestamp = processTime(
                getValidateValue(site_payload, 'timestamp'))
            site = getValidateValue(site_payload, 'site')
            SITE_SM_batteryInstPower = getValidateValue(
                signals, 'SITE_SM_batteryInstPower')
            SITE_SM_siteInstPower = getValidateValue(
                signals, 'SITE_SM_siteInstPower')
            SITE_SM_solarInstPower = getValidateValue(
                signals, 'SITE_SM_solarInstPower')

            # check if any value is NULL, if yes retry
            if (not timestamp or not site or not SITE_SM_batteryInstPower or not SITE_SM_siteInstPower or not SITE_SM_solarInstPower) and i < MAX_RETRY-1:
                await asyncio.sleep(RETRY_INTERAL_SECONDS)
                continue

            # construct the final payload
            event_payload = {
                'event_timestamp': current_event_timestamp,
                'timestamp': timestamp,
                'site': site,
                'SITE_SM_batteryInstPower': SITE_SM_batteryInstPower,
                'SITE_SM_siteInstPower': SITE_SM_siteInstPower,
                'SITE_SM_solarInstPower': SITE_SM_solarInstPower,
            }

            pushData(publisher, event_payload)
            break

    print(event_payload)
    return event_payload


async def fetch_signals(sites, publisher, current_event_timestamp):
    urls = [
        ALL_SIGNALS_URL_PREFIX + site for site in sites
    ]
    tasks = []
    async with aiohttp.ClientSession() as session:
        for url in urls:
            tasks.append(fetch(session, url, publisher,
                               current_event_timestamp))
        response_list = await asyncio.gather(*tasks)

    return response_list


def fetch_sites():
    sites = None
    for _ in range(MAX_RETRY):
        getAllSites = requests.get(ALL_SITES_URL)
        if getAllSites.status_code == 200:
            sites = (getAllSites.json())['sites']
            break
        # sleep RETRY_INTERAL_SECONDS seconds before retry
        time.sleep(RETRY_INTERAL_SECONDS)
    return (sites, getAllSites.raise_for_status())

# -----------------------------------------------------------------------------#
# MAIN FUNCTION
# -----------------------------------------------------------------------------#


def execute(request):

    # Step I: initialize publisher
    publisher = pubsub_v1.PublisherClient()

    # Step II: get current event_timestamp as the partition of table
    current_event_timestamp = datetime.datetime.utcnow().strftime(
        '%Y-%m-%d %H:%M:%S') + ' UTC'

    # Step III: call API to get a list of all sites. If failed, retries 2 times
    sites, sites_fetch_status_code = fetch_sites()

    if not sites:  # if all retries failed. return failed status
        return {
            "statusCode": int(sites_fetch_status_code),
            "message": f"fail to get Tesla API after trying {MAX_RETRY} times"
        }

    # Step IV: call API to get telemetry of all sites using asyncio
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    events = loop.run_until_complete(fetch_signals(
        sites, publisher, current_event_timestamp))

    return {
        "statusCode": 200,
        "body": json.dumps(events)
    }
