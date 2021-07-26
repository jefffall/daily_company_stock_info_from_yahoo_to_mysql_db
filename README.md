
as of July 26, 2021 - 
This program will load some stocks into the db.

daily_company_stock_info_from_yahoo_to_mysql_db
To use this software you need a few things configured first.

install python3 from https://python.org

installing python will install pip3

for each import statement at the top of the program you need to run pip3 install

you might need gcc in first.
apt-get install gcc. Or yum unstall gcc. Or on Macos install Xcode

pip3 install  pymysql
pip3 install  yfinance

Now you need to install or configure mysql database.
I will assume you have installed mysql db and it is running.

mysql -u root (from your root account at a shell on linux. Wiht MacOS it can vary wildly.

Once you get the mysql> prompt then:
create the database:
mysql> create database stockmarket;

CREATE USER 'user1'@'localhost' IDENTIFIED BY 'user1'; and change the username and password in the python prgram

and add permissions:
GRANT ALL PRIVILEGES ON stockmarket.* TO 'user1'@'localhost';

then run the program:
python3 load_company_info_yahoo_to_sql.py
It should create the table in the stocksdb and read the csv file for stock symbols and start to query yahoo.

to query the table:
mysql> use stocks;
mysql> show tables
mysql> describe (put the table name here).
Then for queries:
mysql> select column1, column2, ... from stocks;
where you got the colums from the describe table command
and use the where command to limit the searches.
look up how to write sql queries. It is easy and fun
