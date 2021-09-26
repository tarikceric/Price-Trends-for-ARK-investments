#Use numpy and pandas to perform analysis with the data

import config
import alpaca_trade_api as tradeapi
import psycopg2
import psycopg2.extras # the extras module holds different helper functions - use this is return dicts instead of tuples
import pandas as pd
import numpy as np
import os
from datetime import datetime
from datetime import timedelta
from sqlalchemy import create_engine

#Read the postgres tables into pandas dataframes to perform computations

#construct an engine connection string
engineString = f"postgresql+psycopg2://{config.DB_USER}:{config.DB_PASS}@{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}"

#create sqlalchemy engine
engine = create_engine(engineString)

#connect to my postgres database and delete any existing values in the pricetrendsdf
connection = psycopg2.connect(host=config.DB_HOST, database=config.DB_NAME, user=config.DB_USER, password=config.DB_PASS)

cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

cursor.execute("DELETE FROM pricetrends")

#create dfs from my postgres tables
etfholdingDF = pd.read_sql_table('etfholding',engine)
stockPriceDF = pd.read_sql_table('stockprice', engine)
priceTrendsDF = pd.read_sql_table('pricetrends', engine)

#convert date columns in the df's from datetime object to string for comparison
etfholdingDF['date'] = etfholdingDF['date'].dt.strftime('%Y-%m-%d').astype(str)
stockPriceDF['date'] = stockPriceDF['date'].dt.strftime('%Y-%m-%d').astype(str)


#Get todays date and check if it is a market open day by checking if date is in stockPrice, and if not then subtract days until it is
today= datetime.today()
todaysDate = datetime.today().strftime('%Y-%m-%d')

while todaysDate not in stockPriceDF.date.values:
    print(f'Current date {todaysDate} does not exist.')
    today = today - timedelta(days=1)
    todaysDate = today.strftime('%Y-%m-%d')
    print('Moved todaysDate one day backward')

print(f'current date {todaysDate} is a day when the market was open.')

#Get yesterday/week/month date and from updated todayDate
yesterday = today - timedelta(days=1)

week = today - timedelta(days=7)

month = today - timedelta(days=30)

#check if y/w/m are days when the market is open, then add to 'updatedList'

dateList = [yesterday, week, month]

updatedDateList = [todaysDate]

for date in dateList:
    dateString = date.strftime('%Y-%m-%d')
    while dateString not in stockPriceDF.date.values:
        print(f'{dateString} is a day when the market was open')
        date = date - timedelta(days=1)
        dateString = date.strftime('%Y-%m-%d')
    print(f'{dateString} Exists')
    updatedDateList.append(dateString)

print(updatedDateList)


#Rename fields in etfholding so they append and map to the correct field in pricetrends
etfholdingDF.rename(columns={'etfid': 'trendetfid',
                    'holdingid': 'trendholdingid',
                    'weight': 'currentweight',
                    'shares': 'numberofshares'}, inplace = True)


#append the current etfholding rows to pricetrends
#***** this will add all current holdings, and Im reading the priceTrendsDF from the Postgres table, so need to figure
#how to replace the existing values since deleting+overwriting will remove the ones that were removed by ark and I still want to see those
priceTrendsDF = priceTrendsDF.append(etfholdingDF[etfholdingDF['date'] == todaysDate])



#----------------Populate current/yesterday/week/month price + holding  from stockprice + etfholding table

#Functions to populate the holding and price % changes
def populatePercentChange(priceChangeColumn):
    priceTrendsDF[priceChangeColumn] = ((priceTrendsDF['currentprice'] - priceTrendsDF['price']) /
                                         priceTrendsDF['currentprice']) * 100
    priceTrendsDF.drop(['price', 'stockid'], 1, inplace=True)
    return priceTrendsDF

def populateHoldingChange(holdingChangeColumn):
    priceTrendsDF[holdingChangeColumn] = ((priceTrendsDF['numberofshares'] - priceTrendsDF['holding']) /
                                            priceTrendsDF['numberofshares']) * 100
    priceTrendsDF.drop(['holding'], 1, inplace=True)
    return priceTrendsDF


# Loop through updatedDateList and populate price + holding fields in stockTrends table
for date in updatedDateList:
    index = updatedDateList.index(date)
    print(index)
    print(f'this is index {index} and date {date}')

    # Create tempDF that holds the price at the time of date variable
    tempPriceDF = stockPriceDF[stockPriceDF['date'] == date]
    tempPriceDF = tempPriceDF[['stockid', 'close']]
    tempPriceDF.rename(columns={'close': 'price'}, inplace=True)

    # Merge tempPriceDF to the priceTrendsDF to get the price
    priceTrendsDF = priceTrendsDF.merge(tempPriceDF, how='left', left_on='trendholdingid', right_on='stockid')

    # If on the current date, Use fillna to update the values of existing 'currentprice' column
    if index == 0:
        priceTrendsDF['currentprice'] = priceTrendsDF['currentprice'].fillna(priceTrendsDF['price'])
        priceTrendsDF.drop(['price', 'stockid'], 1, inplace=True)
        print ('Insert the current price data.')

    else:
        # For y/w/m get %change of price and holding

        # Create tempHoldingDF for y/w/m (Skip for current date since current holding is already populated)
        tempHoldingDF = etfholdingDF[etfholdingDF['date'] == date]
        tempHoldingDF = tempHoldingDF[['trendetfid', 'trendholdingid', 'numberofshares']]
        tempHoldingDF.rename(columns={'numberofshares': 'holding'}, inplace=True)
        print(f'Created temp df for ETF Holding data for {date}.')

        # for y/m/w first find the percentage change using 'populatePercentChange' function
        # then merge tempHoldingDF to into the priceTrendsDF & use 'populateHoldingChange' function to get the holding change

        #Updating Yesterday fields
        if index == 1:
            print(f'Populating holding and price change for yesterday: {date}')
            populatePercentChange('pricedailychange')

            #merge HoldingDF - using two fields since there can be duplicate holdingids since ETFS might hold the same stock
            priceTrendsDF = priceTrendsDF.merge(tempHoldingDF, how='left',
                                                left_on=['trendetfid', 'trendholdingid'],
                                                right_on=['trendetfid', 'trendholdingid'])

            populateHoldingChange('holdingdailychange')

        # Updating Weekly fields
        elif index == 2:
            print(f'Populating holding and price change for last week: {date}')
            populatePercentChange('priceweeklychange')

            #merge HoldingDF - using two fields since there can be duplicate holdingids since ETFS might hold the same stock
            priceTrendsDF = priceTrendsDF.merge(tempHoldingDF, how='left',
                                                left_on=['trendetfid', 'trendholdingid'],
                                                right_on=['trendetfid', 'trendholdingid'])

            populateHoldingChange('holdingweeklychange')

        # Updating Monthly fields
        elif index == 3:
            print(f'Populating holding and price change for last month: {date}')
            populatePercentChange('pricemonthlychange')

            #merge HoldingDF - using two fields since there can be duplicate holdingids since ETFS might hold the same stock
            priceTrendsDF = priceTrendsDF.merge(tempHoldingDF, how='left',
                                                left_on=['trendetfid', 'trendholdingid'],
                                                right_on=['trendetfid', 'trendholdingid'])

            populateHoldingChange('holdingmonthlychange')


#********** Insert priceTrends Dataframe into pricetrends Postgresql Table



#Insert dataframe into Postgres table
print ("Inserting dataframe into Postgresql Talbe")
priceTrendsDF.drop('date', axis = 1, inplace = True)# delete 'date' since it does not exist in the postgres table

priceTrendsDF.to_sql('pricetrends', engine, if_exists = 'append', index = False)
print ("Complete inserting of dataframe into Postgres Table")





# #***********  Get current price
# currentPriceDF = stockPriceDF[stockPriceDF['date'] == todaysDate]
# currentPriceDF = currentPriceDF[['stockid', 'close']]
# currentPriceDF.rename(columns={'close': 'currentprice'}, inplace = True)
#
# priceTrendsDF = priceTrendsDF.merge(currentPriceDF, how='left', left_on = 'trendholdingid', right_on = 'stockid')
# #this merge renames the currentprice as currentprice_x and adds a currentprice_y column from currentpriceDF
# #Use fillna to update the values and drop the unnecessary columns
# priceTrendsDF['currentprice'] = priceTrendsDF['currentprice_x'].fillna(priceTrendsDF['currentprice_y'])
# priceTrendsDF.drop(['currentprice_x', 'currentprice_y', 'stockid'],1, inplace=True)
#
#
# #***********  Get yesterdays price and subtract from current price to get price change
#
# yesterdayPriceDF = stockPriceDF[stockPriceDF['date'] == yesterdaysDate]
# yesterdayPriceDF = yesterdayPriceDF[['stockid', 'close']]
# yesterdayPriceDF.rename(columns={'close': 'yesterdayprice'}, inplace = True)
#
# priceTrendsDF = priceTrendsDF.merge(yesterdayPriceDF, how='left', left_on = 'trendholdingid', right_on = 'stockid')
# #Use fillna to update the values and drop the unnecessary columns
# priceTrendsDF['pricedailychange'] = ((priceTrendsDF['currentprice'] - priceTrendsDF['yesterdayprice'])/priceTrendsDF['currentprice']) * 100
# priceTrendsDF.drop(['yesterdayprice', 'stockid'],1, inplace=True)
#
#
# #***********  Get last week price and subtract from current price to get weekly price change
#
# lastWeekPriceDF = stockPriceDF[stockPriceDF['date'] == weekDate]
# lastWeekPriceDF = lastWeekPriceDF[['stockid', 'close']]
# lastWeekPriceDF.rename(columns={'close': 'lastweekprice'}, inplace = True)
#
# priceTrendsDF = priceTrendsDF.merge(lastWeekPriceDF, how='left', left_on = 'trendholdingid', right_on = 'stockid')
# #Use fillna to update the values and drop the unnecessary columns
# priceTrendsDF['priceweeklychange'] = ((priceTrendsDF['currentprice'] - priceTrendsDF['lastweekprice'])/priceTrendsDF['currentprice']) * 100
# priceTrendsDF.drop(['lastweekprice', 'stockid'],1, inplace=True)
#
#
# #***********  Get last month price and subtract from current price to get monthly price change
#
# lastMonthPriceDF = stockPriceDF[stockPriceDF['date'] == monthDate]
# lastMonthPriceDF = lastMonthPriceDF[['stockid', 'close']]
# lastMonthPriceDF.rename(columns={'close': 'lastmonthprice'}, inplace = True)
#
# priceTrendsDF = priceTrendsDF.merge(lastMonthPriceDF, how='left', left_on = 'trendholdingid', right_on = 'stockid')
# #Use fillna to update the values and drop the unnecessary columns
# priceTrendsDF['pricemonthlychange'] = ((priceTrendsDF['currentprice'] - priceTrendsDF['lastmonthprice'])/priceTrendsDF['currentprice']) * 100
# priceTrendsDF.drop(['lastmonthprice', 'stockid'],1, inplace=True)
#



# #----------------Populate current/yesterday/week/month HOLDING from ETFholding table
#
# #***********  Get yesterdays holding and subtract from current holding to get holding change
#
# yesterdayHoldingDF = etfholdingDF[etfholdingDF['date'] == yesterdaysDate]
# yesterdayHoldingDF = yesterdayHoldingDF[['trendetfid', 'trendholdingid', 'numberofshares']]
# yesterdayHoldingDF.rename(columns={'numberofshares': 'yesterdayholding'}, inplace = True)
#
# #need to merge using two fields since there can be duplicate holdingids since both etfs might hold the same stock
# priceTrendsDF = priceTrendsDF.merge(yesterdayHoldingDF, how='left', left_on = ['trendetfid', 'trendholdingid'], right_on = ['trendetfid', 'trendholdingid'])
#
# priceTrendsDF['holdingdailychange'] = ((priceTrendsDF['numberofshares'] - priceTrendsDF['yesterdayholding'])/priceTrendsDF['numberofshares']) * 100
# priceTrendsDF.drop(['yesterdayholding'],1, inplace=True)
#
#
# #***********  Get last weeks holding and subtract from current holding to get holding change
#
# lastWeekHoldingDF = etfholdingDF[etfholdingDF['date'] == weekDate]
# lastWeekHoldingDF = lastWeekHoldingDF[['trendetfid', 'trendholdingid', 'numberofshares']]
# lastWeekHoldingDF.rename(columns={'numberofshares': 'lastweekholding'}, inplace = True)
#
# priceTrendsDF = priceTrendsDF.merge(lastWeekHoldingDF, how='left', left_on = ['trendetfid', 'trendholdingid'], right_on = ['trendetfid', 'trendholdingid'])
#
# priceTrendsDF['holdingweeklychange'] = ((priceTrendsDF['numberofshares'] - priceTrendsDF['lastweekholding'])/priceTrendsDF['numberofshares']) * 100
# priceTrendsDF.drop(['lastweekholding'],1, inplace=True)
#
#
# #***********  Get last months holding and subtract from current holding to get holding change
#
# lastMonthHoldingDF = etfholdingDF[etfholdingDF['date'] == monthDate]
# lastMonthHoldingDF = lastMonthHoldingDF[['trendetfid', 'trendholdingid', 'numberofshares']]
# lastMonthHoldingDF.rename(columns={'numberofshares': 'lastmonthholding'}, inplace = True)
#
# priceTrendsDF = priceTrendsDF.merge(lastMonthHoldingDF, how='left', left_on = ['trendetfid', 'trendholdingid'], right_on = ['trendetfid', 'trendholdingid'])
#
# priceTrendsDF['holdingmonthlychange'] = ((priceTrendsDF['numberofshares'] - priceTrendsDF['lastmonthholding'])/priceTrendsDF['numberofshares']) * 100
# priceTrendsDF.drop(['lastmonthholding'],1, inplace=True)
#
# #EOD update - can get all of my fields populated now just need to make some function to clean it up