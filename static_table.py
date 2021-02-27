"""
Made by Faye Arejola.
"""

from sqlalchemy import create_engine, Table, Column, Float, Integer, MetaData, String
import traceback
import json
import requests
import time
import mysql.connector

key = "56bd4c6e05f01e155b1e57859a5148dc4da93b23"
NAME = "Dublin"
STATIONS = "https://api.jcdecaux.com/vls/v1/stations/"

def main_static():
    #running forever
    while True:
        try:
            reply = requests.get(STATIONS, params={"apiKey": key, "contract": NAME})
            #write reply into a json file
            store_static(reply)
            #make it go to sleep for 5 minutes before parsing from the API again
            time.sleep(10080*60)
        except:
            print(traceback.format_exc())

def get_stations_static(object):
    return {'number' : object['number'],
    'name' : object['name'],
    'address': object['address'],
    'post_lat': object['position']['lat'],
    'post_long': object['position']['lng'],}

#connection database set up information
user = "admin"
password = "dublinbikesapp"
uri = "dublinbikesapp.czojulseblvc.us-east-1.rds.amazonaws.com"
port = "3306"
db = "dublinbikesapp"

#drops and creates new table every time it parses information
drop_stations = """
DROP TABLE IF EXISTS stations
"""
#creates table for static data
meta = MetaData()
stations_static = Table('stations', meta,
Column('number', Integer, primary_key = True),
Column('name', String(128)),
Column('address', String(128)),
Column('post_lat', Float),
Column('post_long', Float))


def store_static(files):
    try:
        # sets up the connection for the database and requests
        engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{uri}:{port}/{db}", echo=True)

        # creates database if it doesn't already exist
        dbname = "dublinbikesapp"
        sql = "CREATE DATABASE IF NOT EXISTS %s ;" % (dbname)
        engine.execute(sql)

        # executing following statements
        engine.execute(drop_stations)
        meta.create_all(engine)

        # executes and inserts values into table
        value = list(map(get_stations_static, files.json()))
        insert = stations_static.insert().values(value)
        engine.execute(insert)
    except:
        print(traceback.format_exc())

main_static()