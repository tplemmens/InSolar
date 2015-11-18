import pandas as pd
import datetime as dt # used for rounding time
import sys
import json

def round_time(time, interval_size):
    """ rounds off time to closest interval
    @param t: time to be rounded
    @type t: datetime
    @param interval_size: interval size in minutes
    @type interval_size: integer

    @returns: time rounded to closest interval
    """

    m = time.minute
    m_lower = interval_size * (m // interval_size)

    time_lower = time.replace(minute = m_lower, second = 0)
    time_upper = time_lower + dt.timedelta(minutes=interval_size)

    time_rounded = time_lower if (time - time_lower) <= (time_upper - time) else time_upper

    return time_rounded

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

def ConvertToJSON(df):
    """ converts a dataframe to JSON
    @param df: pd dataframe: with column 'power' and datetimeindex
    
    @returns: output: JSON
    """
    
    df["timestamp"] = [str(ts).replace(" ", "T") for ts in df.index]

    output = {"successful" : True,
            "timestamp": {
                "startTime": str(df.timestamp[0]),
                "endTime": str(df.timestamp[-1]),
                "format" : "ISO-8061",
                "interval" : "15 minutes"
                },
            "power": {
                "measure": "mean power in interval",
                "unit": "Watt"
                },
            "recordCount": len(df),
            "sum" : {
                "value" : round(df["power"].sum()/4, 2), 
                "unit" : "Watt hour",
                "measure" : "total production in period"
                },
            "data": df[["timestamp","power"]].where((pd.notnull(df)), None).to_dict(orient = "records")}
    
    return json.dumps(output, sys.stdout)