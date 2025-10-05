# Amazon RDS Connector Java Implementation

This project implements a comprehensive Amazon RDS connector in Java with MySQL database operations.

## Features

### Database Operations
1. **connect()** - Establishes connection to Amazon RDS MySQL database
2. **drop()** - Drops company and stockprice tables from the database
3. **create()** - Creates company and stockprice tables with proper schema
4. **insert()** - Inserts test data for both tables
5. **delete()** - Deletes stock price records based on specified criteria

### Query Operations
6. **queryOne()** - Returns companies with >10,000 employees or revenue <$1M
7. **queryTwo()** - Returns stock analysis for August 22-26 week
8. **queryThree()** - Returns companies with closing price analysis

### Utility Methods
9. **resultSetToString()** - Converts ResultSet to formatted string
10. **resultSetMetaDataToString()** - Converts ResultSetMetaData to formatted string

## Database Schema

### Company Table
- `id` (INT, PRIMARY KEY)
- `name` (VARCHAR(50))
- `ticker` (CHAR(10))
- `annualRevenue` (DECIMAL(14,2)) - up to 999,999,999,999.99
- `numEmployees` (INT)

### Stock Price Table
- `companyId` (INT)
- `priceDate` (DATE)
- `openPrice` (DECIMAL(10,2)) - up to 99999999.99
- `highPrice` (DECIMAL(10,2)) - up to 99999999.99
- `lowPrice` (DECIMAL(10,2)) - up to 99999999.99
- `closePrice` (DECIMAL(10,2)) - up to 99999999.99
- `volume` (INT)
- PRIMARY KEY: (companyId, priceDate)
- FOREIGN KEY: companyId REFERENCES company(id)

## Test Data

### Company Data
- Apple (AAPL) - $387.54B revenue, 154,000 employees
- GameStop (GME) - $611M revenue, 12,000 employees
- Handy Repair (no ticker) - $2M revenue, 50 employees
- Microsoft (MSFT) - $198.27B revenue, 221,000 employees
- StartUp (no ticker) - $50K revenue, 3 employees

### Stock Price Data
- Apple: August 15-30, 2022 stock prices
- GameStop: August 15-30, 2022 stock prices
- Microsoft: August 15-30, 2022 stock prices

## Usage

### Compile and Run
```bash
javac -cp "mysql-connector-java-8.0.33.jar" SQLonRDS.java
java -cp ".:mysql-connector-java-8.0.33.jar" SQLonRDS
```

## Query Descriptions

### Query One
Returns company information (name, revenue, employees) for companies with:
- More than 10,000 employees OR
- Annual revenue less than $1 million
Ordered by company name ascending.

### Query Two
Returns company name, ticker, and calculates:
- Lowest price
- Highest price
- Average closing price
- Average volume
For the week of August 22-26, 2022 (inclusive).
Ordered by average volume descending.

### Query Three
Returns companies with:
- Company name, ticker, and closing stock price on August 30, 2022
- Only shows companies where closing price is no more than 10% below the average closing price for August 15-19, 2022
- Companies without stock tickers are always shown
Ordered by company name ascending.

## Dependencies

- MySQL Connector/J 8.0.33 or later
- Java 8 or later

## Database Connection

The connector uses the following connection parameters:
- Host: [YOUR_RDS_ENDPOINT]
- Database: [YOUR_DATABASE_NAME]
- Username: [YOUR_USERNAME]
- Password: [YOUR_PASSWORD]

**Note:** Replace the placeholder values with your actual database credentials before running the application.