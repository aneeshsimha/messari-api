import os
import pandas as pd
from pandas import json_normalize
import numpy as np
import requests
import time
import json

messari_TS_url = "hello"
messari_MP_url =  "hi"
messari_test_url = "https://data.messari.io/api/v1/assets/yfi/metrics/price/time-series?start=2021-01-01&end=2021-02-01&interval=1d"

def jprint(obj):
    # create a formatted string of the Python JSON object
    text = json.dumps(obj, sort_keys=True, indent=4)
    print(text)

def api_request(assets):
    messari_url = "https://data.messari.io/api/v1/assets/yfi/metrics/price/time-series?start=2021-01-01&end=2021-02-01&interval=1d"
    response = requests.get(messari_test_url)
    print(response.status_code)
    data = response.json()
    #jprint(data)
    json_to_df(data)

def json_to_df(json_obj):
    #df = pd.read_json(json_obj)

    dumps = json.dumps(json_obj, sort_keys=True, indent=4)
    info = json.loads(dumps)
    df2 =json_normalize(info['data'], record_path = ['values'])
    df2.columns = ["timestamp","open","high", "low", "close", "volume"]
    df2['timestamp']=df2.apply(lambda x: time.strftime('%Y-%m-%d', time.gmtime(x['timestamp']/1000)),axis=1)

    df3 = df2[['timestamp','close']]
    print(df3.head())

def messari_test():
    response = requests.get(messari_test_url)
    print(response.status_code)
    data = response.json()
    #jprint(data)
    json_to_df(data)

def get_list_of_assets():
    print("Enter list of assets: ")
    asset_list = input()
    print(asset_list)

if __name__ == "__main__":
    #print(messari_TS_url)
    #print(messari_MP_url)
    messari_test()
    get_list_of_assets()
