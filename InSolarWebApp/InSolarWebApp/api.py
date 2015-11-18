"""
Routes and views for the flask application.
"""

# TODO: correct orientation-180

from InSolarWebApp import app
from flask import Flask, request
from azure.storage.table import TableService, Entity

import numpy as np 
import pandas as pd
import datetime as dt # used for rounding time
import pvlib #the library of sandia, the us national laboratories
import os
import sys
import json

from helper_functions import *
from radiation_on_azure import *
from model import *

@app.route('/hw', methods = ['GET'])
def api_hello_world():
    return "hello" 

@app.route('/testAzureConnection', methods = ['GET'])
def test_connection():
    # read config file
    config_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), "config.py")
    config = dict()
    execfile(config_file, config)

    # tests connection with Azure, if succesfull returns one row from table
    table_service = TableService(account_name=config['account_name'], 
                                 account_key=config['account_key'])
    storage_name = config['storage_name']
    partition_key = "000000_20130601"
    row_key = "20130601_074500"
    meteo_data = table_service.get_entity(storage_name, partition_key, row_key)
    test_json = {"storage_name" : storage_name,
                 "partition_key": partition_key,
                 "row_key" : row_key,
                 "diffuse" : meteo_data.diffuse,
                 "downwelling" : meteo_data.downwelling,
                 "extrapolated" : meteo_data.extrapolated}
    return json.dumps(test_json)

@app.route('/GetPVProduction', methods = ['POST'])
def api_json_extract():
    if request.headers['Content-Type'] == 'application/json':
        # parse request data
        input_data = request.json
        time_begin = round_time(pd.to_datetime(input_data['dateTimeBegin'], utc=True), 15)
        time_end = round_time(pd.to_datetime(input_data['dateTimeEnd'], utc=True), 15)
        geo_long =  float(input_data['longitude']) #362.4001514126 
        geo_lat = float(input_data['latitude'])
        orientation = float(input_data['orientation'])
        tilt = float(input_data['tilt'])
        installation_capacity = float(input_data['installationCapacity'])

        # read config file
        config_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), "config.py")
        config = dict()
        execfile(config_file, config)
        pars_grid = config['pars_grid']

        # Calculate gridpoint based on long/lat and grid parameters
        table_service = TableService(account_name=config['account_name'], 
                                     account_key=config['account_key'])
        gridpoint = fnLonLat2GridPoint(pars_grid, geo_long, geo_lat)

        # get radiation from Azure for gridpoint for time interval
        df = GetRadiationFromAzure(gridpoint, time_begin, time_end, 
                                   config['storage_name'], table_service)
            
        # Calcualte the expected production
        df = fnGetExpectedProduction(df, geo_long, geo_lat, orientation, tilt, 
                                     installation_capacity, pars_grid)

        # Convert the resulting dataframe to JSON
        output =  ConvertToJSON(df)
        
        return output
    else:
        return "415 Unsupported Input Media Type: has to be JSON, i.e. Content-Type = application/json"

