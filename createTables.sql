/* creating a stock table, etf holding table, stock price table, and price trend */

CREATE TABLE stock(
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    name TEXT NOT NULL,
    exchange TEXT NOT NULL,
    is_etf BOOLEAN NOT NULL /* a True/false value that indicates if the stock is one of the etfs */


);
/* This table stores the relationship between the etfs and the stocks in the etf */
CREATE TABLE etfHolding(
    etfID  INTEGER NOT NULL, /* The id of the ETF in the stock table (which would be one of the ARK funds) */
    holdingID INTEGER NOT NULL,/* The id of the stock in the stock table */
    date DATE NOT NULL,
    shares NUMERIC,
    weight NUMERIC,
    stockName  TEXT NOT NULL,
    stockSymbol  TEXT NOT NULL,
    PRIMARY KEY (etfID, holdingID, date), /* creating a unique 'compound' primary key by combining these fields */
    CONSTRAINT fkETF FOREIGN KEY (etfID) REFERENCES stock (id), /*This states that the etfID must reference an id in the stock table*/
    CONSTRAINT fkHolding FOREIGN KEY (holdingID) REFERENCES stock (id)


);

CREATE TABLE stockPrice(
    stockID INTEGER NOT NULL, /* Each of these price records will reference a specific stock in the stock table*/
    date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    open NUMERIC NOT NULL,
    high NUMERIC NOT NULL,
    low NUMERIC NOT NULL,
    close NUMERIC NOT NULL,
    volume NUMERIC NOT NULL,
    PRIMARY KEY (stockID, date),
    /*Below is a foreign key constraint = This states that the stockID references the id in the stock table*/
    CONSTRAINT fkStock FOREIGN KEY (stockID) REFERENCES stock (id)
);

CREATE TABLE priceTrends(

    trendetfID  INTEGER NOT NULL, /* The id of the ETF in the stock table (which would be one of the ARK funds) */
    trendholdingID INTEGER NOT NULL,/* The id of the stock in the stock table */
    holdingDailyChange NUMERIC,
    holdingWeeklyChange NUMERIC,
    holdingMonthlyChange NUMERIC,
    priceDailyChange NUMERIC,
    priceWeeklyChange NUMERIC,
    priceMonthlyChange NUMERIC,
    currentPrice NUMERIC NOT NULL,
    currentWeight NUMERIC NOT NULL,
    numberofshares NUMERIC NOT NULL,
    stockname TEXT NOT NULL,
    stocksymbol TEXT NOT NULL,
    PRIMARY KEY (trendetfID, trendholdingID), /* creating a unique 'compound' primary key by combining these fields */

    /*the foreign key constraint = The foreign keys must reference values in these columns in other tables*/
    CONSTRAINT fkTrendStock FOREIGN KEY (trendholdingID) REFERENCES stock (id)
);




/*
Creating an index to search through the table quicker - creating an index on the stockPrice table since this one will be large.
Tells the database to optimize searching.
*/
CREATE INDEX ON stockPrice(stockID, date DESC);
