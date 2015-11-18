import numpy as np 
import pandas as pd
import datetime as dt # used for rounding time
import pvlib #the library of sandia, the us national laboratories
import os

from helper_functions import *

def fnGetExpectedProduction(df, geo_long, geo_lat, orientation, tilt,
                            installation_capacity, pars_grid):
    """ Calculates the expected PV production 
    """
    
    ## MeteoSat radiation to GTI        
    def Meteosat2GTI_sandia(geo_lat, geo_long, df):
        '''
        Inputs:
                datalocation: is the dataframe from the file with the static data of toon houses
                df_ID: the dataframe of the meteosat data for the spesific toon id.
        output: GTI the pandas series with the GTI of the spesific toon id
        '''
        tz=pvlib.location.pytz.timezone('Europe/Amsterdam')
        location=pvlib.location.Location(geo_lat,geo_long,'Europe/Amsterdam')
        
        sun_position = pvlib.solarposition.ephemeris(df.index,location)
        sun_position.index=sun_position.index.tz_convert('utc')
        beam=pvlib.irradiance.beam_component(tilt,
                                             orientation,
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
        power = np.interp(DC/inverter_capacity,load,efficiency) *  DC
        return power
        
    inverter_capacity = installation_capacity    
    df['power'] = fnDC2AC(df.DC,inverter_capacity).tolist()

    return df