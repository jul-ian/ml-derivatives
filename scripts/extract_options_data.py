"""

The data/options.zip file takes up over 8GB and will take more space if all the files are unzipped. Since it is so much data, and I will be focusing on one stock at a time for now, this script will extract a dataframe from the files for a given stock ticker. 

"""
import os
from os.path import join
import json
import numpy as np
import pandas as pd
from zipfile import ZipFile
from collections import defaultdict
from tempfile import mkdtemp

def json_to_dataframe(json_path: str) -> tuple:
    with open(json_path, 'r') as f:
        opt_dict = json.load(f)

    df_dict = defaultdict(list)   
    quote = opt_dict.pop('quote')
    stock_bid = quote['bid']
    stock_ask = quote['ask']
    for date_key, data_list in opt_dict.items():
        for data_dict in data_list:
            if 'OptionGreeks' in data_dict.keys():
                del data_dict['OptionGreeks']
                source = 'ameritrade'
            else:
                source = 'yahoo'
            df_dict['date_expired'] = df_dict['date_expired'] + [date_key]
            df_dict['stock_bid'] = df_dict['stock_bid'] + [stock_bid]
            df_dict['stock_ask'] = df_dict['stock_ask'] + [stock_ask]
            for key, item in data_dict.items():
                df_dict[key] = df_dict[key] + [item]
        
    return source, pd.DataFrame(df_dict)


def clean_opt_df(df: pd.DataFrame, source: str) -> pd.DataFrame:
    if source == 'ameritrade':
        df = df.rename(columns={'optionType': 'option_type', 
                   'strikePrice': 'strike_price',
                   'bid': 'option_bid',
                   'ask': 'option_ask'
                   })
        df['option_type'] = df['option_type'].str.lower()
    elif source == 'yahoo':
        df = df.rename(columns={'strike': 'strike_price',
                   'bid': 'option_bid',
                   'ask': 'option_ask'
                   })
        df['is_call'] = df['contractSymbol'].str.contains('^[A-Z\d]+C\d+$')
        df['option_type'] = pd.Series(np.where(df.is_call, 'call', 'put'))
        
    
    df['date_priced'] = pd.to_datetime(df['date_priced'])
    df['date_expired']= pd.to_datetime(df['date_expired'])
    
    df['days_to_maturity'] = (df['date_expired'] - df['date_priced']).dt.days
    
    return df[
        ['date_priced', 'date_expired', 'days_to_maturity', 
         'stock_bid', 'stock_ask', 'option_bid', 'option_ask',
         'strike_price', 'option_type']
        ]

if __name__ == "__main__":
    ticksym = 'AAPL'
    tmpdir = mkdtemp()
    
    ameri_dfs = list()
    yahoo_dfs = list()
    
    optzip_path = '/home/jul-ian/Github/ml-options/data/raw/options.zip'
    
    with ZipFile(optzip_path, 'r') as optzip:
        ziplist = [x for x in optzip.namelist() 
                   if x.startswith('options/') and x.endswith('zip')]  
        for zipf in ziplist:
            optzip.extract(zipf, path=tmpdir)
            
            with ZipFile(join(tmpdir, zipf), 'r') as datezip:
                datezip.extract(f'{ticksym}.json', path=join(tmpdir, 'options'))
            
            source, dframe = json_to_dataframe(
                join(tmpdir, 'options', f'{ticksym}.json')
                )
            dframe['date_priced'] = zipf.replace('options/', '').replace('.zip', '')
            if source == 'yahoo':
                yahoo_dfs.append(dframe)
            if source == 'ameritrade':
                ameri_dfs.append(dframe)
            os.remove(join(tmpdir, zipf))
            os.remove(join(tmpdir, 'options', f'{ticksym}.json'))
    
    df_yahoo = clean_opt_df(pd.concat(yahoo_dfs, ignore_index=True), 'yahoo')
    df_ameri = clean_opt_df(pd.concat(ameri_dfs, ignore_index=True), 'ameritrade')
    
    options_df = pd.concat([df_yahoo, df_ameri], ignore_index=True)
    options_df = options_df[options_df['days_to_maturity'] <= 365]
    
    risk_free_df = pd.read_pickle('/home/jul-ian/Github/ml-options/data/processed/risk_free_df.pkl')
    
    options_df = pd.merge(
        options_df, risk_free_df,
        left_on='date_priced', right_on='date'
        ).drop(columns='date')
    options_df['rf_rate'] = np.select(
        condlist=[
            options_df['days_to_maturity'] <= 4*7,
            options_df['days_to_maturity'] <= 8*7,
            options_df['days_to_maturity'] <= 13*7,
            options_df['days_to_maturity'] <= 26*7,
            options_df['days_to_maturity'] <= 365
            ],
        choicelist=[
            options_df['rate_4wk'],
            options_df['rate_8wk'],
            options_df['rate_13wk'],
            options_df['rate_26wk'],
            options_df['rate_52wk']
            ],
        default=np.nan
        )
    options_df = options_df[
        [name for name in options_df.columns if not name.startswith('rate_')]
        ]
    
    outdir = '/home/jul-ian/Github/ml-options/data/processed/stocks_df'
    
    pd.to_pickle(options_df, join(outdir, f'{ticksym.lower()}_df.pkl'))
    
    