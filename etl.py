# Import labraries

import requests # to connect to API
import pandas as pd #for data transformation
import configparser # to create my confirgurations
from sqlalchemy import create_engine # this helps communicate with postgres
import os
import time
from datetime import datetime

def etl_process():
    #create configparser instance
    config = configparser.ConfigParser()
    config.read('config.ini')

    # create openweather credential
    api_key = config['openweather']['api_key']
    lat = config['openweather']['lat']
    lon = config['openweather']['lon']
    city_id = config['openweather']['city_id']
    city_name = 'kaduna'

    #start postgres engine
    postgres_config = config['postgres']
    engine = create_engine(
        f"postgresql://{postgres_config['user']}:{postgres_config['password']}"
        f"@{postgres_config['host']}/{postgres_config['database']}"
    )
    # create API request
    
    # create API request
    url = f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}'
    response = requests.get(url)

    data = response.json()

    df = pd.json_normalize(data)
    df['dt'] = pd.to_datetime(df['dt'], unit='s')
    df['lastupdated'] = pd.to_datetime('now')
    df['weather'] = df['weather'].astype(str)
    #load data into postgres
    df.to_sql('testdata', engine, if_exists='append', index=False)
if __name__=="__main__":
    etl_process()
        
        # set up automation loop
    while True:
        time.sleep(3600)
        etl_process()