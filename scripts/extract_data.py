"""

The data/options.zip file takes up over 8GB and will take more space if all the files are unzipped. Since it is so much data, and I will be focusing on one stock at a time for now, this script will extract a dataframe from the files for a given stock ticker. 

"""

import json
fl = '/home/jul-ian/Github/ml-options/data/raw/AAPL.json'
with open(fl, 'r') as f:
    dat = json.load(f)