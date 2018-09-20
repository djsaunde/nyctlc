import os
import dask
import requests
import warnings
import dask.dataframe as dd

from bs4 import BeautifulSoup
from requests.exceptions import HTTPError
from typing import Optional, Union, Sequence

from .definitions import MONTH_MAP, DATA_URL, TLC_URL, GREEN_COLUMNS, DTYPES, GREEN_COLUMN_MAPPING, YELLOW_2009, \
    YELLOW_2009_MAPPING, YELLOW_2014, YELLOW_2014_MAPPING, YELLOW_2015_2016, YELLOW_2015_2016_MAPPING, YELLOW_COLUMNS, \
    DATA_PATH


def load_file(color: str, month: int, year: int, save: bool = True) -> dask.dataframe.DataFrame:
    # language=rst
    """
    Downloads a single taxi or for-hire vehicle data file from a certain month and year.

    :param color: Yellow or green taxi or for-hire vehicle data.
    :param month: Month to get taxi data file for.
    :param year: Year to get taxi data file for.
    :param save: Whether to save the file to disk.
    :return: :code:`dask.dataframe.DataFrame` with taxi or for-hire vehicle data.
    """
    if year > 2016 or year == 2016 and month > 6:
        warnings.warn('No coordinate data for this month')

    if color == 'yellow':
        assert 2009 <= year <= 2017
    elif color == 'green':
        assert 2013 <= year <= 2017
        if year == 2013:
            assert month >= 8
    elif color == 'fhv':
        assert 2015 <= year <= 2017

    folder = '_'.join([color, 'tripdata', str(year) + '-' + MONTH_MAP[month]])
    path = os.path.join(DATA_PATH, folder)
    if os.path.isdir(path):
        df = dd.read_parquet(path)
    else:
        f = '_'.join([color, 'tripdata', str(year) + '-' + MONTH_MAP[month] + '.csv'])
        href = os.path.join(DATA_URL, f)

        try:
            if color == 'green':
                if year < 2017 or (year == 2016 and month < 7):
                    dtypes = {k: v for (k, v) in zip(GREEN_COLUMNS, DTYPES)}
                    df = dd.read_csv(
                        href, usecols=GREEN_COLUMNS, dtype=dtypes
                    )

                    df = df.rename(columns=GREEN_COLUMN_MAPPING)
                    for column in ['lpep_pickup_datetime', 'lpep_dropoff_datetime']:
                        df[column] = df[column].astype('M8[us]')
                else:
                    df = dd.read_csv(href)

            elif color == 'yellow':
                if year == 2009:
                    dtypes = {k: v for (k, v) in zip(YELLOW_2009, DTYPES)}
                    df = dd.read_csv(
                        href, usecols=YELLOW_2009, dtype=dtypes, parse_dates=YELLOW_2009[:2]
                    )

                    df = df.rename(columns=YELLOW_2009_MAPPING)
                elif year == 2014:
                    dtypes = {k: v for (k, v) in zip(YELLOW_2014, DTYPES)}
                    df = dd.read_csv(
                        href, usecols=YELLOW_2014, dtype=dtypes, parse_dates=YELLOW_2014[:2]
                    )

                    df = df.rename(columns=YELLOW_2014_MAPPING)
                elif year == 2015 or (year == 2016 and month <= 6):
                    dtypes = {k: v for (k, v) in zip(YELLOW_2015_2016, DTYPES)}
                    df = dd.read_csv(
                        href, usecols=YELLOW_2015_2016, dtype=dtypes, parse_dates=YELLOW_2015_2016[:2]
                    )

                    df = df.rename(columns=YELLOW_2015_2016_MAPPING)
                elif year in [2010, 2011, 2012, 2013]:
                    dtypes = {k: v for (k, v) in zip(YELLOW_COLUMNS, DTYPES)}
                    df = dd.read_csv(
                        href, usecols=YELLOW_COLUMNS, dtype=dtypes, parse_dates=YELLOW_COLUMNS[:2]
                    )
                else:
                    df = dd.read_csv(href)
            else:
                raise NotImplementedError(
                    'Currently, only loading of yellow and green taxi data files is supported.'
                )
        except HTTPError:
            raise FileNotFoundError(f'No {color} taxi data for month {month} and year {year}.')

    if save:
        df.to_parquet(folder)

    return df


def load_files(coordinates: bool = False, colors: Optional[Union[str, Sequence[str]]] = None,
               months: Optional[Union[int, Sequence[int]]] = None,
               years: Optional[Union[int, Sequence[int]]] = None) -> dask.dataframe.DataFrame:
    # language=rst
    """
    Downloads all taxi and for-hire vehicle data files into a single DataFrame.

    :param coordinates: Whether to download files with geospatial coordinates included.
    :param colors: If provided, restricts files to those of a certain taxi color ("yellow", "green", or "fhv").
    :param months: If provided, restricts files to those from a certain month.
    :param years: If provided, restricts files to those from a certain year.
    :return: List of :code:`dask.dataframe.DataFrame` with taxi data.
    """
    if isinstance(colors, str):
        colors = [colors]

    if isinstance(months, int):
        months = [months]
        months = [MONTH_MAP[m] for m in months]

    if isinstance(years, int):
        years = [years]

    result = requests.get(TLC_URL).text
    soup = BeautifulSoup(result, 'html.parser')

    ddfs = []
    for node in soup.find_all('a'):
        href = node.get('href')
        if href.endswith('.csv') and 'lookup' not in href:
            f = href.split('/')[-1].split('.')[0]

            c = href.split('_')[0].split('/')[-1]
            m = int(href.split('_')[2].split('-')[1].split('.')[0])
            y = int(href.split('_')[2].split('-')[0])

            if not coordinates or (y < 2016 and m < 7):  # Coordinates are not recorded past June 2016.
                if colors is None or c in colors:
                    if months is None or m in months:
                        if years is None or y in years:
                            ddf = dask.delayed(load_file)(color=c, month=m, year=y)
                            ddfs.append(ddf)

    ddf = dask.delayed(dd.concat)(ddfs).compute()
    return ddf
