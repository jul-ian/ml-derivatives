"""
Script to clean Treasury bill data.
"""
import os
import pandas as pd

datadir = '/home/jul-ian/Github/ml-options/data'

df_list = list()
for year in range(2018, 2021+1):
    df = pd.read_csv(os.path.join(datadir, f'raw/daily-treasury-rates-{year}.csv'))
    df_list.append(df)

rf_df = pd.concat(df_list, ignore_index=True)

dates = pd.to_datetime(rf_df['Date']).rename('date')
rates_df = rf_df[[c for c in rf_df.columns if 'DISCOUNT' in c]]
rates_df = rates_df.applymap(lambda d: (1 - d/100)**(-1) - 1)
rename_map = {
 '4 WEEKS BANK DISCOUNT': 'rate_4wk',
 '8 WEEKS BANK DISCOUNT': 'rate_8wk',
 '13 WEEKS BANK DISCOUNT': 'rate_13wk',
 '26 WEEKS BANK DISCOUNT': 'rate_26wk',
 '52 WEEKS BANK DISCOUNT': 'rate_52wk'
 }
risk_free_df = (
    pd.merge(dates, rates_df, left_index=True, right_index=True)
    .rename(columns=rename_map)
    )
risk_free_df.to_pickle(os.path.join(datadir, 'processed', 'risk_free_df.pkl'))
