"""

The data/options.zip file takes up over 8GB and will take more space if all the files are unzipped. Since it is so much data, and I will be focusing on one stock at a time for now, this script will extract a dataframe from the files for a given stock ticker. 

"""

import json
from collections import defaultdict

import pandas as pd

json_file = '/home/jul-ian/Github/ml-options/data/raw/AAPL.json'
with open(json_file, 'r') as f:
    opt_dict = json.load(f)

## Ameritrade
if 'quote' in opt_dict.keys():
    del opt_dict['quote']
    df_dict = defaultdict(list)   
    for date_key, data_list in opt_dict.items():
        for data_dict in data_list:
            del data_dict['OptionGreeks']
            df_dict['date'] = df_dict['date'] + [date_key]
            for key, item in data_dict.items():
                df_dict[key] = df_dict[key] + [item]   
        
        
