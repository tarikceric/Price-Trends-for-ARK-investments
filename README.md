# Price-Trends-for-ARK-investments
Tracking ARKK and ARKX ETF holdings with analysis in Pandas dataframes and exporting to Postgresql databases

The goal of this project is to track the change of stock prices in relation to whether ARK invest ETFs are increasing or decreasing the number of shares they hold. It creates the framework to allow a user to determine any trends in how stock prices are affected by the actions of ARK investments. 

The thesis is that ARK Invest is significantly influential and their buys/sells will affect the price of stocks, and tracking these ETF's can showcase what companies they are interested in.
## Contents
This repository has a series of .py files which perform separate tasks:
1. createTables.sql : create the PostgreSQL tables to store stocks, stock prices, ETF holdings, and price/holding trends.
2. downloadArkCSVS :  Download ETF holdings from the web
3. populateStocks.py : Populate a master stock table via Alpaca Trade API 
4. populateETFHoldings.py : Populate the etfholding table with daily ETF data from ARK
5. populatePrices : Populate the stockprice table with daily price of the stocks within ARK holdings
6. populateTrends : Populate the pricetrends table with daily/weekly/monthly percentage changes in holding and price.
