from sqlalchemy import create_engine, Table, Float, Column, Integer, MetaData, String, DateTime
import traceback
import json
import requests
import time
import datetime
import mysql.connector

#coordinates of dublin city centre
city = "Dublin,ie"
api_key = "bd7d1521214da9913bfd5624b8c3d6d0"
units = "metric"
station = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units={units}"

def main_weather():
    while True:
        try:
            r = requests.get(station)
            #call the store_dynamic function
            store_weather(r)
            #make it go sleep
            time.sleep(60*60)
        except:
            print(traceback.format_exc())

def get_weather(object):
    return {'last_updated': datetime.datetime.fromtimestamp(int(object['dt'])),
            'windspeed': object['wind']['speed'],
            'description': object['weather'][0]['description'],
            'main_desc': object['weather'][0]['main'],
            'temperature': object['main']['temp']}

#connection database set up info
user = "admin"
password = "dublinbikesapp"
uri = "dublinbikesapp.czojulseblvc.us-east-1.rds.amazonaws.com"
port = "3306"
db = "dublinbikesapp"

#creates table for weather data IF there is no rain
meta2 = MetaData()
weather_data = Table('weather', meta2,
Column('last_updated', DateTime),
Column('windspeed', Integer),
Column('description', String(128)),
Column('main_desc', String(128)),
Column('temperature', Integer))

def store_weather(files):
    try:
        engine2 = create_engine(f"mysql+mysqlconnector://{user}:{password}@{uri}:{port}/{db}", echo=True)

        # creates database if it doesn't already exist
        dbname2 = "dublinbikesapp"
        tableName2 = "availability"
        sql2 = "CREATE DATABASE IF NOT EXISTS %s ;" % (dbname2)
        engine2.execute(sql2)

        # execute and inserts values into tables
        values = get_weather(files.json())
        insert2 = weather_data.insert().values(values)

        # checks if table already exists
        if not engine2.dialect.has_table(engine2, tableName2):
            meta2.create_all(engine2)
            engine2.execute(insert2)
        else:
            engine2.execute(insert2)
    except:
        print(traceback.format_exc())

main_weather()