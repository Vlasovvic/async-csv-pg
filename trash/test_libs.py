import datetime
import pandas as pd
import json


def date():
    date = datetime.datetime.now()
    print(date.strftime("%Y.%m"))


def pandas_lib():
    cvs = pd.read_csv('../data/business-financial-data-june-2022-quarter-csv.csv')
    print(cvs.dtypes)
    #    cvs["Period"] = pd.to_datetime(cvs["Period"])
    #    print("Cvs=", cvs)
"""
    sort = cvs.sort_values(by=['Period'], ascending=False)
    #    print(sort)
    #    print(sort['Period'].max())
    df_filter = sort['Period'].isin([sort['Period'].max()])
    a = 2022.01
    b = 2022.06
    actual1 = sort.query(f'{a}<=Period<={b}')
    actual = sort[df_filter]
    #print(actual)
    print(actual1)
"""
def pandas_lib1():

    cvs = pd.read_csv('../data/business-financial-data-june-2022-quarter-csv.csv', na_filter=False)
    print(cvs)
    for elem in cvs.itertuples(index=False):
        for elem1 in elem:
            print(elem1)
        #print(elem.Series_reference)
def JSon():
    with open('../config.json') as file:
        a = json.load(file)
    print(a)
if __name__ == '__main__':
    #    date()
    JSon()
