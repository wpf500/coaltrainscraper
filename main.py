import argparse, logging, os.path, pickle, sys, time
from collections import Counter

import requests
from apiclient import discovery
from bs4 import BeautifulSoup
from google.oauth2 import service_account

parser = argparse.ArgumentParser(description='Fetch train movements')
parser.add_argument('date', help='YYYY-MM-DD')
parser.add_argument('--no-sheets', action='store_true')

args = parser.parse_args()

base_dir = os.path.dirname(os.path.realpath(__file__))
secret_file = os.path.join(base_dir, 'auth.json')
stations_file = os.path.join(base_dir, 'stations.pkl')
rtt_dir = os.path.join(base_dir, 'rtt', args.date)
if not os.path.exists(rtt_dir):
    os.mkdir(rtt_dir)

scopes = ['https://www.googleapis.com/auth/spreadsheets']
sheet_id = '1U22HX4tFcHx5xe9potpsItoVeOq7sVpfwferiqbkpHs'

credentials = service_account.Credentials.from_service_account_file(secret_file, scopes=scopes)
service = discovery.build('sheets', 'v4', credentials=credentials)

rtt_date = args.date.replace('-', '/')
base_rtt_url = 'http://www.realtimetrains.co.uk/search/detailed/{}/' + rtt_date + '/0000-2359'

# Get list of stations

stations = None

if not args.no_sheets:
    try:
        logging.warn('Fetching station list')
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range='Station whitelist'
        ).execute()
        rows = result.get('values', [])

        stations = [dict(zip(rows[0], row)) for row in rows[1:]]
        logging.warn('Got %d stations', len(stations))

        logging.warn('Saving station list')
        with open(stations_file, 'wb') as f:
            pickle.dump(stations, f)
    except Exception as err:
        logging.error('Failed to fetch station list')
        logging.error(err)

if not stations:
    logging.warn('Using saved station list')
    with open(stations_file, 'rb') as f:
        stations = pickle.load(f)

# Fetch RTT pages

session = requests.Session() # Reuse connection

stations_by_name = {station['Name']: station for station in stations}

services = []

def rtt_v1_service_list(service_list):
    station_services = []

    # recursive=False stops us from parsing trs in thead (there is no tbody)
    for row in service_list.find_all('tr', recursive=False):
        _, plan_arr, act_arr, origin, _, id, toc, dest, plan_dep, act_dep = row.find_all('td')

        is_actual = 'actual' in (act_arr.get('class', []) + act_dep.get('class', []))
        is_origin = origin.text == 'Starts here'

        # We only want trains that actually ran
        if not is_actual:
            continue

        # Ignoring passing trains for now
        if not is_origin and dest.text != 'Terminates here':
            continue

        other_station = stations_by_name.get(dest.text if is_origin else origin.text)
        if not other_station:
            continue

        origin_station = station if is_origin else other_station
        dest_station = other_station if is_origin else station
        # ' stops Sheets from parsing the value
        station_services.append([
            args.date,
            '\'' + station['Name'] if is_origin else other_station['Name'],
            '\'' + other_station['Name'] if is_origin else station['Name'],
            '\'' + act_arr.text,
            '\'' + act_dep.text,
            '\'' + id.text,
            '\'' + args.date + id.text
        ])
    return station_services

def rtt_v2_service_list(service_list):
    station_services = []

    for row in service_list.find_all('a', class_='service', recursive=False):
        plan_arr = row.select('div.plan.a')[0]
        act_arr = row.select('div.real.a')[0]
        origin = row.select('div.location.o')[0]
        id = row.select('div.tid')[0]
        toc = row.select('div.toc')[0]
        dest = row.select('div.location.d')[0]
        plan_dep = row.select('div.plan.d')[0]
        act_dep = row.select('div.real.d')[0]

        is_actual = 'act' in (act_arr.get('class', []) + act_dep.get('class', []))
        is_origin = origin.text == 'Starts here'

        # We only want trains that actually ran
        if not is_actual:
            continue

        # Ignoring passing trains for now
        if not is_origin and dest.text != 'Terminates here':
            continue

        other_station = stations_by_name.get(dest.text if is_origin else origin.text)
        if not other_station:
            continue

        origin_station = station if is_origin else other_station
        dest_station = other_station if is_origin else station
        # ' stops Sheets from parsing the value
        station_services.append([
            args.date,
            '\'' + station['Name'] if is_origin else other_station['Name'],
            '\'' + other_station['Name'] if is_origin else station['Name'],
            '\'' + act_arr.text,
            '\'' + act_dep.text,
            '\'' + id.text,
            '\'' + args.date + id.text
        ])

    return station_services

for i, station in enumerate(stations):
    if not station['Code']:
        continue

    logging.warn('Processing %s (%d/%d)', station['Name'], i + 1, len(stations))

    station_file = os.path.join(rtt_dir, '{}.html'.format(station['Code']))

    if os.path.exists(station_file):
        logging.warn('..Using cached station page')
        with open(station_file) as f:
            html = f.read()
    else:
        logging.warn('..Fetching station page')
        r = session.get(base_rtt_url.format(station['Code']))
        html = r.text
        with open(station_file, 'w') as f:
            f.write(html)
        time.sleep(1) # Rate limit

    soup = BeautifulSoup(html, features='html.parser')

    service_list = soup.find('div', class_='servicelist')
    if service_list:
        station_services = rtt_v2_service_list(service_list)
    else:
        service_list = soup.find('table', class_='servicelist')
        if service_list:
            station_services = rtt_v1_service_list(service_list)

    if not service_list:
        logging.warn('..No services today')
        continue

    logging.warn('..Got %d services', len(station_services))

    services.extend(station_services)

logging.warn('Finished processing stations')
logging.warn('Got %d services in total', len(services))

if args.no_sheets:
    print('Date\tOrigin\tDestination\tArrival\tDeparture\tID\tUnique ID')
    for service in services:
        print('\t'.join(service))
else:
    logging.warn('Saving services')
    result = service.spreadsheets().values().append(
        spreadsheetId=sheet_id,
        range='Services',
        valueInputOption='USER_ENTERED',
        body={'values': services}
    ).execute()
