from numpy.random import choice
from dask.dataframe import DataFrame
from requests.exceptions import HTTPError

from nyctlc.query import load_file, load_files


def test_load_file():
    for color in ['yellow', 'green']:
        for month in [1, 12]:
            for year in [2015]:
                print(color, month, year)

                try:
                    ddf = load_file(color, month, year)
                except HTTPError:
                    continue

                assert isinstance(ddf, DataFrame)


def test_load_all_files():
    for color in ['green', 'yellow']:
        for i in range(3):
            if color == 'yellow':
                years = choice(range(2009, 2018), 3, replace=False)
            else:
                years = choice(range(2014, 2018), 3, replace=False)

            for j in range(3):
                if 2016 in years:
                    months = choice(range(7), 3)
                else:
                    months = choice(range(13), 3)

                print(color, years, months)

                ddf = load_files(colors=color, years=years, months=months)
                assert isinstance(ddf, DataFrame)


def test_load_all_files_of_type():
    pass