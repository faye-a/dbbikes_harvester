"""
Made by Faye Arejola.
"""

from sqlalchemy import create_engine, Table, Column, Integer, MetaData, String, DateTime
import traceback
import json
import requests
import time
import datetime
import mysql.connector

key = "56bd4c6e05f01e155b1e57859a5148dc4da93b23"
NAME = "Dublin"
STATIONS = "https://api.jcdecaux.com/vls/v1/stations/"


def main_dynamic():
    while True:
        try:
            r = requests.get(STATIONS, params={"apiKey": key, "contract": NAME})
            # write repy into a json file
            store_dynamic(r)
            # make it go to sleep for 10 minutes before parsing from the API again
            time.sleep(10 * 60)
        except:
            print(traceback.format_exc())


def get_stations_dynamic(object):
    return {'station_num': object['number'],
            'avail_stands': object['available_bike_stands'],
            'avail_bikes': object['available_bikes'],
            'last_update': datetime.datetime.fromtimestamp(int(object['last_update'] / 1e3))}


#connection database set up information
user = "admin"
password = "dublinbikesapp"
uri = "dublinbikesapp.czojulseblvc.us-east-1.rds.amazonaws.com"
port = "3306"
db = "dublinbikesapp"

#creates table for dynamic data
meta2 = MetaData()
stations_dynamic = Table('availability', meta2,
Column('station_num', Integer, primary_key = True),
Column('avail_stands', Integer),
Column('avail_bikes', Integer),
Column('status', String(40)),
Column('last_update', DateTime))

#checking if table already exists
table_exists_str = "SELECT name FROM availability WHERE type='table' AND name='availability'"


def store_dynamic(files):
    try:
        # sets up the connection for the database and requests
        engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{uri}:{port}/{db}", echo=True)

        # creates database if it doesn't already exist
        dbname = "dublinbikesapp"
        tableName = "availability"
        sql = "CREATE DATABASE IF NOT EXISTS %s ;" % (dbname)
        engine.execute(sql)

        # executes and inserts values into table
        value = list(map(get_stations_dynamic, files.json()))
        insert = stations_dynamic.insert().values(value)

        # checks if table already exists
        if not engine.dialect.has_table(engine, tableName):
            meta2.create_all(engine)
            engine.execute(insert)
        # else
        else:
            engine.execute(insert)
    except:
        print(traceback.format_exc())


main_dynamic()