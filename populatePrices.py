import config
import alpaca_trade_api as tradeapi
import psycopg2
import psycopg2.extras # the extras module holds different helper functions - use this is return dicts instead of tuples
import datetime

#connect to my postgres database by grabbing the values from my config
#psycopg2.connect creates a new database session and returns a 'connection' object
connection = psycopg2.connect(host=config.DB_HOST, database=config.DB_NAME, user=config.DB_USER, password=config.DB_PASS)

cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)


#clean out the existing data in the stockprice database
cursor.execute("DELETE FROM stockprice")

#instantiate the Alpaca trade api
api = tradeapi.REST(config.API_KEY, config.API_SECRET, base_url=config.API_URL)


#fetch all of the stocks from 'stock' table that have an id in the 'etfholding' table
#We use the 'stock' table and its 'id' field so it is the common key between all the tables
cursor.execute("SELECT * FROM stock WHERE id IN (SELECT holdingid FROM etfholding)" )

rows = cursor.fetchall()
#print(rows)

#currentDate = datetime.date.today()
# Loop through the stocks (with a matching id in the etfholding) and populate the stockPrice database

for row in rows:
    print(row)

    #Grab the daily bar data (from the last 23 days via 'limit=23') and populate stockPrice
    barsets = api.get_barset([row['symbol']], 'day', limit = 23)
    # loop over the keys in the barsets dictionary
    for symbol in barsets:
        print(f"Retrieving bar info for {symbol}")

        for bar in barsets[symbol]:
            barClose = bar.c
            barOpen = bar.o
            barHigh = bar.h
            barLow = bar.l
            barVolume = bar.v
            print(barClose)
            cursor.execute("""
                INSERT INTO stockprice (stockid, date, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (row['id'], bar.t.date(), barOpen, barHigh, barLow, barClose, barVolume))
            #print(barClose)

connection.commit()

#make it a habit to close the cursor and connection
cursor.close()
connection.close()







