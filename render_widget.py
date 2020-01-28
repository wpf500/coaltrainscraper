import os
from datetime import datetime

import pystache
from apiclient import discovery
from google.oauth2 import service_account

COAL_PER_TRAIN = 1600

base_dir = os.path.dirname(os.path.realpath(__file__))
secret_file = os.path.join(base_dir, 'auth.json')

scopes = ['https://www.googleapis.com/auth/spreadsheets']
sheet_id = '1U22HX4tFcHx5xe9potpsItoVeOq7sVpfwferiqbkpHs'

credentials = service_account.Credentials.from_service_account_file(secret_file, scopes=scopes)
service = discovery.build('sheets', 'v4', credentials=credentials)

result = service.spreadsheets().values().get(
    spreadsheetId=sheet_id,
    range='Services'
).execute()
rows = result.get('values', [])

services = [dict(zip(rows[0], row)) for row in rows[1:]]

service_ids = set()
relevant_services = []
for service in services:
    if service['Origin'] != 'North Blyth Gbrf' or service['Destination'] != 'West Burton Ps (Gbrf)':
        continue

    if service['Arrival'] == '':
        continue

    if service['Date'] < '2019-09-01':
        continue

    service_ids.add(service['Unique ID'])
    relevant_services.append(service)

assert(len(service_ids) == len(relevant_services))

last_train = list(reversed(sorted(relevant_services, key=lambda s: s['Date'])))[0]

last_train_date = '/'.join(reversed(last_train['Date'].split('-')))

widget_context = {
    'total_trains': list(str(len(relevant_services))),
    'total_coal': list(str(len(relevant_services) * COAL_PER_TRAIN)),
    'last_train_date': last_train_date,
    'last_updated': datetime.utcnow().strftime('%d/%m/%Y')
}

with open('templates/widget.html') as f:
    widget_template = f.read()
    print(pystache.render(widget_template, widget_context))
