import os

from pathlib import Path


ROOT_DIR = Path(__file__).parents[0].parents[0]
DATA_PATH = os.path.join(ROOT_DIR, 'data')

if not os.path.isdir(DATA_PATH):
    os.makedirs(DATA_PATH)

DATA_URL = 'https://s3.amazonaws.com/nyc-tlc/trip+data/'
TLC_URL = 'http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml'

MONTH_MAP = {
    1: '01', 2: '02', 3: '03', 4: '04', 5: '05', 6: '06', 7: '07', 8: '08', 9: '09', 10: '10', 11: '11', 12: '12'
}

DTYPES = [
    'str', 'str', 'float', 'float', 'float', 'float', 'float', 'float', 'float'
]

GREEN_COLUMNS = [
    'lpep_pickup_datetime', 'Lpep_dropoff_datetime', 'Pickup_longitude', 'Pickup_latitude', 'Dropoff_longitude',
    'Dropoff_latitude', 'Passenger_count', 'Trip_distance', 'Fare_amount'
]

GREEN_COLUMN_MAPPING = {c: c.lower() for c in GREEN_COLUMNS}

YELLOW_COLUMNS = [
    'pickup_datetime', 'dropoff_datetime', 'pickup_longitude', 'pickup_latitude', 'dropoff_longitude',
    'dropoff_latitude', 'passenger_count', 'trip_distance', 'fare_amount'
]

YELLOW_2009 = [
    'Trip_Pickup_DateTime', 'Trip_Dropoff_DateTime', 'Start_Lon', 'Start_Lat',
    'End_Lon', 'End_Lat', 'Passenger_Count', 'Trip_Distance', 'Fare_Amt'
]

YELLOW_2009_MAPPING = {k: v for (k, v) in zip(YELLOW_2009, YELLOW_COLUMNS)}

YELLOW_2014 = [
    ' pickup_datetime', ' dropoff_datetime', ' pickup_longitude', ' pickup_latitude', ' dropoff_longitude',
    ' dropoff_latitude', ' passenger_count', ' trip_distance', ' fare_amount'
]

YELLOW_2014_MAPPING = {k: v for (k, v) in zip(YELLOW_2014, YELLOW_COLUMNS)}

YELLOW_2015_2016 = [
    'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'pickup_longitude', 'pickup_latitude',
    'dropoff_longitude', 'dropoff_latitude', 'passenger_count', 'trip_distance', 'fare_amount'
]

YELLOW_2015_2016_MAPPING = {k: v for (k, v) in zip(YELLOW_2015_2016, YELLOW_COLUMNS)}
