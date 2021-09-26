import requests
import csv
import io
import pandas as pd
import numpy as np
import os
from datetime import datetime

urls = ['https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_INNOVATION_ETF_ARKK_HOLDINGS.csv',
        'https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_SPACE_EXPLORATION_&_INNOVATION_ETF_ARKX_HOLDINGS.csv']

#Get the csvs and store it in a dataframe
def url_to_df(target_url):
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    url_data = requests.get(target_url, headers=headers).content
    df = pd.read_csv(io.StringIO(url_data.decode('utf-8')))
    #ark files have two blank lines followed by a line with string. remove these
    df.drop(df.tail(3).index, inplace=True)

    return df



#create a new date folder and store the csv's in that location
def saveAsCSVs():
    todaysDate = datetime.today().strftime('%Y-%m-%d')
    path = f"csv Data/{todaysDate}"
    if not os.path.exists(path):
        os.mkdir(path)

    #Convert to csv and store in the path + use index=false to exclude the unnecessary index column
    url_to_df(urls[0]).to_csv(f'{path}/ARKK.csv', index=False)
    url_to_df(urls[1]).to_csv(f'{path}/ARKX.csv', index=False)

    print(f'{path}/ARKK.csv')

saveAsCSVs()











