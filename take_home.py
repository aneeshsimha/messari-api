import os
import pandas as pd
from pandas import json_normalize
import numpy as np
import requests
import time
import json
import datetime
from functools import reduce

def validate(date_text):
    #Validates date inputted
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d')
        return True
    except ValueError:
        #raise ValueError("Incorrect data format, should be YYYY-MM-DD")
        return False


def ts_api_request(asset,start_date,end_date, failed_assets):
    
    messari_url = "https://data.messari.io/api/v1/assets/"+ asset +"/metrics/price/time-series?start="+ start_date +"&end="+ end_date + "&interval=1d"
    try:
        response = requests.get(messari_url)
        if response.status_code == 200:
            data = response.json()
            output_df = ts_json_to_df(data)
            return(output_df)
        elif response.status_code == 404:
            failed_assets.append(asset)
            return None
            
    except requests.exceptions.RequestException as e:  # This is the correct syntax
        raise SystemExit(e)


def ts_json_to_df(json_obj):
    dumps = json.dumps(json_obj, sort_keys=True, indent=4)
    info = json.loads(dumps)
    data_df =json_normalize(info['data'], record_path = ['values'])
    data_df.columns = ["timestamp","open","high", "low", "close", "volume"]
    data_df['timestamp']=data_df.apply(lambda x: time.strftime('%Y-%m-%d', time.gmtime(x['timestamp']/1000)),axis=1)
    asset_symbol= info['data']['symbol']
    #timestamp formatting
    ts_output_df = data_df[['timestamp','close']]
    ts_output_df.columns = ['timestamp',asset_symbol]
    ts_output_df = ts_output_df.set_index('timestamp')

    return ts_output_df

def merge_dfs(df_list):
    #merge all df based on timestamp index into final output df
    merge_df = reduce(lambda df1,df2: pd.merge(df1,df2, left_index = True, right_index = True), df_list)
    return merge_df

def get_list_of_assets():
    print("Enter list of assets seperating by a comma: ")
    asset_list = input().split(",")#.strip(" ")
    asset_list = [asset.strip(" ") for asset in asset_list]
    #print(asset_list)
    return asset_list

def get_start_date():
    valid_start_date = False
    ]

    while(not valid_start_date):
        print("Please enter Start Date in this format 'YYYY-MM-DD'")
        start_date_input = input()
        valid_start_date = validate(start_date_input)
        
    return start_date_input

def get_end_date():
    valid_end_date = False
    while(not valid_end_date):
        print("Please enter End Date in this format 'YYYY-MM-DD'")
        end_date_input = input()
        valid_end_date = validate(end_date_input)
        
    return end_date_input
    
def run_main():
    failed_assets = []
    df_list = []
    asset_list = []

    asset_list = get_list_of_assets()
    start_date = get_start_date()
    end_date = get_end_date()

    for asset in asset_list:
        asset_df = ts_api_request(asset, start_date, end_date, failed_assets)
        if asset_df is not None:
            df_list.append(asset_df)
    print("All asset prices shown in USD and are closing prices")
    final_df = merge_dfs(df_list)
    
    if failed_assets:
        print("Here is a list of assets that did not load: ", failed_assets)
    return final_df

if __name__ == "__main__":

    final_output_df = run_main()
    
    print(final_output_df)
