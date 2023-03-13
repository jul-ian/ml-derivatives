"""

The data/options.zip file takes up over 8GB and will take more space if all the files are unzipped. Since it is so much data, and I will be focusing on one stock at a time for now, this script will extract a dataframe from the files for a given stock ticker. 

"""
import json
import pandas as pd
from zipfile import ZipFile
from collections import defaultdict
from tempfile import mkdtemp

optzip_path = '/home/jul-ian/Github/ml-options/data/raw/options_2.zip'

with ZipFile(optzip_path, 'r') as optzip:
    nmlist = optzip.namelist()
    
    
    

ameri_dfs = list()
yahoo_dfs = list()

json_file = '/home/jul-ian/Github/ml-options/data/raw/files/AAPL_AT.json'

def json_to_dataframe(json_path: str) -> pd.DataFrame:
    with open(json_path, 'r') as f:
        opt_dict = json.load(f)

    df_dict = defaultdict(list)   
    quote = opt_dict.pop('quote')
    for date_key, data_list in opt_dict.items():
        for data_dict in data_list:
            if 'OptionGreeks' in data_dict.keys():
                del data_dict['OptionGreeks']
            df_dict['date'] = df_dict['date'] + [date_key]
            for key, item in data_dict.items():
                df_dict[key] = df_dict[key] + [item]
    return pd.DataFrame(df_dict)

# if Yahoo Finance
if len(quote) == 68:
    yahoo_dfs.append(pd.DataFrame(df_dict))
elif len(quote) == 58:
    ameri_dfs.append(pd.DataFrame(df_dict))
else:
    raise Exception('Neither Ameritrade nor Yahoo Finance')

