import config
import alpaca_trade_api as tradeapi
import psycopg2
import psycopg2.extras # the extras module holds different helper functions - use this is return dicts instead of tuples
import csv #pythons built in csv reader
import os

#connect to my postgres database by grabbing the values from my config
#psycopg2.connect creates a new database session and returns a 'connection' object
connection = psycopg2.connect(host=config.DB_HOST, database=config.DB_NAME, user=config.DB_USER, password=config.DB_PASS)

cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

#clean out the existing data in the etfHolding database
cursor.execute("DELETE FROM etfholding")


#Remember, we populated the etf=true with a sql query implemented in populateStocks.py
cursor.execute("SELECT * FROM stock WHERE is_etf = TRUE")

# To actually get the results of a sql query, you have to use 'fetch' and print the results
etfRows = cursor.fetchall()
print(etfRows)


#get all of the date values in my 'csv data' folder and create a list of these date strings so I'm only getting data from these csv's
your_path = 'csv data'
dates = os.listdir(your_path)

#now we parse through the csvs
for currentDate in dates:
    for etf in etfRows:
        print(etf['symbol'])
        # open csvs: use 'open' and a 'f' string to find our folder containing the downloaded csvs, and read it via 'reader'
        with open(f"csv data/{currentDate}/{etf['symbol']}.csv") as csvFile:
            reader = csv.reader(csvFile)
            next(reader) #use this to skip the first line which are the headers
            for row in reader:
                ticker = row[3]
                #clean up the data we are retrieving from the CSV, by only grabbing the values that have a ticker symbol
                # 'if ticker:' will only return the ones that are true for having a ticker symbol value
                if ticker:
                    weight = row[7]
                    shares = row[5]
                    stockName = row[2]
                    stockSymbol = row[3]
                    #insert these into our etf database, by finding the corresponding ticker in 'stock' to grab from
                    #(ticker,) is a tuple that we need to use since that is what the cursor returns and it is used to populate the %s
                    cursor.execute("SELECT * FROM stock WHERE symbol = %s", (ticker,) ) 

                    #fetch the record that matches from the database
                    stock = cursor.fetchone()
                    #if the stock is found in our 'stock' database, we insert it into 'etfHolding' database
                    if stock:
                        cursor.execute("""
                            INSERT INTO etfHolding (etfid, holdingid, date, shares, weight, stockname, stocksymbol )
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """, (etf['id'], stock['id'], currentDate, shares, weight, stockName, stockSymbol)) #these are the variables we are inserting

connection.commit()

#make it a habit to close the cursor and connection
cursor.close()

connection.close()









