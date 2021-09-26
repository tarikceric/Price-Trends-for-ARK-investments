import config
import alpaca_trade_api as tradeapi
import psycopg2
import psycopg2.extras # the extras module holds different helper functions - use this is return dicts instead of tuples

#connect to my postgres database by grabbing the values from my config
#psycopg2.connect creates a new database session and returns a 'connection' object
connection = psycopg2.connect(host=config.DB_HOST, database=config.DB_NAME, user=config.DB_USER, password=config.DB_PASS)

cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

#clean out the existing data in ALL of the database
cursor.execute("DELETE FROM etfholding")
cursor.execute("DELETE FROM stockprice")
cursor.execute("DELETE FROM stock")
cursor.execute("DELETE FROM pricetrends")


#instantiate the Alpaca trade api
api = tradeapi.REST(config.API_KEY, config.API_SECRET, base_url=config.API_URL)

#Testing to see if it works by calling a tradeapi function
assets = api.list_assets() #this function grabs every stock available in the alpaca api


# Loop through all the 'assets' and insert into our postgres stock database table
for asset in assets:
    if asset.status == 'active' and asset.tradable == True:
        print (f"Inserting stock: {asset.name} {asset.symbol}")

        # insert all records from Alpaca into our stock database table
        cursor.execute("""
        INSERT INTO stock (name, symbol, exchange, is_etf) VALUES (%s, %s, %s, false)
        """, (asset.name, asset.symbol, asset.exchange))

#set the is_etf to true for all of the etfs I am using
cursor.execute("UPDATE stock SET is_etf = TRUE WHERE symbol IN ('ARKX','ARKK')")


#Need to commit our 'inserts' performed in the loop above - these actions arent saved until we 'commit'
connection.commit()

#make it a habit to close the cursor and connection
cursor.close()
connection.close()

