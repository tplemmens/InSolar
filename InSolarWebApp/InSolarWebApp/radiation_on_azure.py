import numpy as np 
import pandas as pd
import datetime as dt # used for rounding time
import pvlib #the library of sandia, the us national laboratories

from helper_functions import *

def perdelta(start, end, delta, include):
    """ returns generator for list of datetimes between start and end, 
    in steps defined by delta
    """
    curr = start
    if include:
        while curr <= end:
            yield curr
            curr += delta
    else:
        while curr < end:
            yield curr
            curr += delta

def fnLonLat2GridPoint(pars_grid, geo_long, geo_lat):
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
    long_distance = np.tile(abs(long_grid - geo_long), N_height)
    lat_distance = np.repeat(abs(lat_grid - geo_lat) ,N_width)
    dist_cityblock = long_distance + lat_distance
    
    # return index
    gridpoint = dist_cityblock.argmin() 
    return gridpoint

def DoAzureQuery(table_service, storage_name, query_azure):
    """ performs the azure query and returns the timestamp and radiations
    @ param table_service:  This is the main class managing Table resources.
    @ param storage_name: str: name of the table storage
    @ param query_azure: str: the query to be performed
    
    @ returns: lists with timestamp, diffuse and downwelling
    
    Azure returns a continuation token if a query returns more than 1000 
    entities. This functions will query while a continuation token is returned.
    """
    # initialize    
    i=0
    next_pk = None
    next_rk = None
    diffuse = []
    downwelling = []
    timestamp = []

    # make query, while checking for contiunation token
    while True:
        entities = table_service.query_entities(storage_name, 
                                                query_azure, 
                                                next_partition_key = next_pk, 
                                                next_row_key = next_rk, 
                                                top=1000)
        i+=1
        print i
        for entity in entities:
            timestamp.append(str(entity.RowKey))
            diffuse.extend([float(entity.diffuse)])
            downwelling.extend([float(entity.downwelling)])
            
        if hasattr(entities, 'x_ms_continuation'):
            x_ms_continuation = getattr(entities, 'x_ms_continuation')
            next_pk = x_ms_continuation['nextpartitionkey']
            next_rk = x_ms_continuation['nextrowkey']
            
        else:
            break;
    
    return timestamp, diffuse, downwelling

def GetRadiationFromAzure(gridpoint, time_begin, time_end, storage_name, table_service):
    """ queries Azure table and writes to dataframe
    @param gridpoint: gridpoint [0:7999]
    @param time_begin: pd datetime: begin of time interval 
    @param time_end: pd datetime: end of interval
    @param table_name: str: name of azure table 

    @returns: pd dataframe: with direct and diffuse radiation for gridpoint/time range
    """

    date_begin = time_begin.date()
    date_end = time_end.date()
    partition_begin = "{0:s}_{1:s}".format('%06d' %gridpoint, str(date_begin).replace("-",""))
    partition_end   = "{0:s}_{1:s}".format('%06d' %gridpoint, str(date_end + dt.timedelta(days=1)).replace("-",""))
    datetimes = [x for x in perdelta(time_begin, time_end, dt.timedelta(minutes=15), include = True)]
    
    # pre-allocate dataframe with all datetimes
    df = pd.DataFrame(index  = datetimes)
    
    # get data from Azure for partitions range, e.g. 000001_20130101-000001_20140629
    query_azure = "PartitionKey ge '{0:s}' and PartitionKey lt '{1:s}'"\
                                        .format(partition_begin, partition_end)
    
    timestamp, diffuse, downwelling = DoAzureQuery(table_service, storage_name, query_azure)   
    
    # insert in data frame
    df_azure = pd.DataFrame({"downwelling": downwelling, "diffuse": diffuse})
    timestamp = [pd.to_datetime(x.replace("_", ""), format =  '%Y%m%d%H%M%S') for x in timestamp]
    df_azure.index = pd.DatetimeIndex(timestamp , tz = "UTC")
    
    # merge and select data within request interval
    df = pd.merge(df, df_azure, how='left', left_index=True, right_index = True)
    df = df.ix[time_begin:time_end]

    return df