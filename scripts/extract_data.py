"""

The data/options.zip file takes up over 8GB and will take more space if all the files are unzipped. Since it is so much data, and I will be focusing on one stock at a time for now, this script will extract a dataframe from the files for a given stock ticker. 

"""
import os
from os.path import join
import json
import pandas as pd
from zipfile import ZipFile
from collections import defaultdict
from tempfile import mkdtemp

ticksym = 'AAPL'
tmpdir = mkdtemp()

ameri_dfs = list()
yahoo_dfs = list()

optzip_path = '/home/jul-ian/Github/ml-options/data/raw/options2.zip'

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

df_yahoo = pd.concat(yahoo_dfs, ignore_index=True)
df_ameri = pd.concat(ameri_dfs, ignore_index=True)

def json_to_dataframe(json_path: str) -> tuple:
    with open(json_path, 'r') as f:
        opt_dict = json.load(f)

    df_dict = defaultdict(list)   
    quote = opt_dict.pop('quote')
    for date_key, data_list in opt_dict.items():
        for data_dict in data_list:
            if 'OptionGreeks' in data_dict.keys():
                del data_dict['OptionGreeks']
                source = 'ameritrade'
            else:
                source = 'yahoo'
            df_dict['date_expired'] = df_dict['date_expired'] + [date_key]
            for key, item in data_dict.items():
                df_dict[key] = df_dict[key] + [item]
        
    return source, pd.DataFrame(df_dict)

def clean_ameri(df: pd.DataFrame) -> pd.DataFrame:
    """
    ['date',
     'optionCategory',
     'optionRootSymbol',
     'timeStamp',
     'adjustedFlag',
     'displaySymbol',
     'optionType',
     'strikePrice',
     'symbol',
     'bid',
     'ask',
     'bidSize',
     'askSize',
     'inTheMoney',
     'volume',
     'openInterest',
     'netChange',
     'lastPrice',
     'quoteDetail',
     'osiKey']
    """


