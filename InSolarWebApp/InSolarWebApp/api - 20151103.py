"""
Routes and views for the flask application.
"""

from InSolarWebApp import app
from flask import Flask, request, json

#from scipy.interpolate import interp1d # cannot only import scipy # numpy.interp
import numpy as np 
import pandas as pd
import datetime as dt # used for rounding time
import pvlib #the library of sandia, the us national laboratories
import os

@app.route('/hw', methods = ['GET'])
def api_hello_world():
    return "hello" 

def fnLonLat2GridPoint(pars_grid, long_installation, lat_installation):
    """ returns nearest gridpoint for given longitude and latitude
    - distance is calculated using Manhattan city block distance (d1)
    """
    glong_min = pars_grid["glong_min"]
    glong_max = pars_grid["glong_max"]
    glat_min = pars_grid["glat_min"]
    glat_max = pars_grid["glat_max"]
    N_width = pars_grid["N_width"]
    N_height = pars_grid["N_height"]  
    
    # set up grid
    dlong = (glong_max - glong_min)/ N_width
    dlat = (glat_max - glat_min)/ N_height
          
    long_grid = np.linspace(glong_min + 0.5*dlong,
                             glong_max - 0.5*dlong,
                             N_width)
    
    lat_grid = np.linspace(glat_min + 0.5*dlat,
                             glat_max - 0.5*dlat,
                             N_height)
    
    # calc distance               
    long_distance = np.tile(abs(long_grid - long_installation), N_height)
    lat_distance = np.repeat(abs(lat_grid - lat_installation) ,N_width)
    dist_cityblock = long_distance + lat_distance
    
    # return index
    gridpoint = dist_cityblock.argmin() 
    return gridpoint

def fnGetExpectedProduction(time_begin, 
                            time_end, 
                            time_resolution, 
                            geo_long, 
                            geo_lat, 
                            orientation,
                            tilt,
                            installation_capacity,
                            Toon_ID):
    
    
    ## Get MeteoSat
    file_MeteoSat = os.path.join(os.path.dirname(__file__),"MeteoSat_29696.csv")
    df = pd.read_csv(file_MeteoSat)
    df = df.applymap(lambda x: np.nan if x < 0  else x)
    df.index = pd.DatetimeIndex(df.datetime,tz='UTC')
    # select time period    
    df = df.ix[time_begin:time_end]

    ## MeteoSat radiation to GTI        
    def Meteosat2GTI_sandia(geo_lat, geo_long, df):
        '''
        Inputs:
                datalocation: is the dataframe from the file with the static data of toon houses
                df_ID: the dataframe of the meteosat data for the spesific toon id.
        output: GTI the pandas series with the GTI of the spesific toon id
        '''
        tz=pvlib.location.pytz.timezone('Europe/Amsterdam')
        df.index=pd.DatetimeIndex(df.datetime,tz='UTC')
        location=pvlib.location.Location(geo_lat,geo_long,'Europe/Amsterdam')
        
        sun_position = pvlib.solarposition.ephemeris(df.index,location)
        sun_position.index=sun_position.index.tz_convert('utc')
        beam=pvlib.irradiance.beam_component(tilt,
                                             orientation-180,
                                             sun_position.zenith,
                                             sun_position.azimuth,
                                             df.downwelling)
        DIA=pvlib.irradiance.isotropic(tilt,df.diffuse)
        GTI=beam+DIA
        return GTI
    
    df['GTI'] = Meteosat2GTI_sandia(geo_lat, geo_long, df).tolist()

    ## GTI to DC
    performance_ratio = 0.78
    
    def fnGTI2DC(GTI, installation_capacity, performance_ratio = 0.78):
        '''
        inputs:
            GTI the tilted irradiance in W/m2 (in series or value)
            Toon_ID: the id of the house in order to get its capacity (int)        
        output: the dc power of the system in W (series or value)
        '''
        
        DC = installation_capacity * GTI * performance_ratio/1000
    
        return DC
    
    inverter_capacity = installation_capacity
    df['DC'] = fnGTI2DC(df.GTI, installation_capacity, performance_ratio).tolist()
    
    ## DC to AC, i.e. expected production
        
    def fnDC2AC(DC,inverter_capacity):
        '''
        input: PDC: the produced DC power from the pannels (pandas series) 
               cap: the inverter capacity. If we dont have it we can make assumptions based on system capacity 
                    However without it we cannot include correctly the losses on the inverter. Espesially in low radiation there are big
        output: Pac: the AC power, output of the inverter (pandas series)
        '''
        #the % load of the inverter= power dc/inverter rated power (inverter capacity)
        load = [0,0.05,0.1,0.15,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1]        
        #the inverter efficiency: Pac=eff*Pdc
        efficiency = [0,0.9,0.94,0.95,0.953,0.96,0.965,0.9675,0.965,0.965,0.9625,0.96,0.9575]
        #creates the serries of the power AC    
        AC = np.interp(DC/inverter_capacity,load,efficiency) *  DC
        return AC
        
    inverter_capacity = installation_capacity    
    df['AC'] = fnDC2AC(df.DC,inverter_capacity).tolist()
    
    if time_resolution == "all":
        output = "total production: "+ "%.2f" %df.AC.sum() + ' Wh'
    elif time_resolution == "15":
        output = df.AC.to_json(orient='index',date_unit='s')
    else:
        output = 'wrong time_resolution fool: has to be "all" or "15"'
    
    return output

@app.route('/GetPVProduction', methods = ['POST'])
def api_json_extract():
    if request.headers['Content-Type'] == 'application/json':
        input_data = request.json
        #aa = float(input_data['a'])
        #bb = float(input_data['b'])
        time_begin = pd.to_datetime(input_data['time_begin'])
        time_end = pd.to_datetime(input_data['time_end'])
        time_resolution = str(input_data['time_resolution']) #all or 15
        geo_long =  float(input_data['geo_long']) #362.4001514126 
        geo_lat = float(input_data['geo_lat'])
        orientation = float(input_data['orientation'])
        tilt = float(input_data['tilt'])
        installation_capacity = float(input_data['installation_capacity'])
        Toon_ID = int(input_data['Toon_ID'])
        
        df = fnGetExpectedProduction(time_begin, 
                            time_end, 
                            time_resolution, 
                            geo_long, 
                            geo_lat, 
                            orientation,
                            tilt,
                            installation_capacity,
                            Toon_ID)
        
        return df
    else:
        return "415 Unsupported Input Media Type: has to be JSON, i.e. Content-Type = application/json"

