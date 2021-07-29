#!/usr/bin/python3
import pymysql
import yfinance as yf
import time

#import json
# -*- coding: utf-8 -*-

import unicodedata as unicodedata


def build_yahoo_company_info_table(name):
    #mysql_table_description = "CREATE TABLE company_info (\
    mysql_table_description = "create table "+name+" ( row_id BIGINT AUTO_INCREMENT PRIMARY KEY,\
    sector VARCHAR(40),\
    quoteDate DATETIME,\
    err FLOAT,\
    ebitdaMargins FLOAT,\
    grossMargins FLOAT,\
    operatingCashflow FLOAT,\
    operatingMargins FLOAT,\
    revenueGrowth FLOAT,\
    ebitda FLOAT,\
    targetLowPrice FLOAT,\
    currentPrice FLOAT,\
    earningsGrowth FLOAT,\
    recommendationKey VARCHAR(50),\
    lastDividendDate DATETIME,\
    grossProfits FLOAT,\
    freeCashflow FLOAT,\
    impliedSharesOutstanding BIGINT,\
    targetMedianPrice FLOAT,\
    currentRatio FLOAT,\
    returnOnAssets FLOAT,\
    quoteSourceName VARCHAR(20),\
    numberOfAnalystOpinions INT,\
    targetMeanPrice FLOAT,\
    debtToEquity FLOAT,\
    returnOnEquity FLOAT,\
    targetHighPrice FLOAT,\
    financialCurrency VARCHAR(20),\
    revenuePerShare FLOAT,\
    quickRatio FLOAT,\
    recommendationMean FLOAT,\
    longName VARCHAR(150),\
    exchangeTimezoneName VARCHAR(20),\
    quoteType VARCHAR(20),\
    symbol VARCHAR(10),\
    regularMarketOpen FLOAT,\
    twoHundredDayAverage FLOAT,\
    payoutRatio FLOAT,\
    volume24Hr BIGINT,\
    trailingAnnualDividendRate FLOAT,\
    algorithm VARCHAR(20),\
    beta FLOAT,\
    circulatingSupply BIGINT,\
    lastMarket FLOAT,\
    maxSupply BIGINT,\
    volumeAllCurrencies BIGINT,\
    ytdReturn FLOAT,\
    askSize BIGINT,\
    fromCurrency VARCHAR(20),\
    totalCash BIGINT,\
    totalDebt BIGINT,\
    totalRevenue BIGINT,\
    totalCashPerShare BIGINT,\
    averageDailyVolume3Month BIGINT,\
    regularMarketTime VARCHAR(20),\
    dayHigh FLOAT,\
    trailingPE FLOAT,\
    fundFamily VARCHAR(60),\
    revenueQuarterlyGrowth FLOAT,\
    shortPercentOfFloat FLOAT DEFAULT '0.000',\
    sharesPercentSharesOut FLOAT DEFAULT '0.000',\
    gmtOffSetMilliseconds INT DEFAULT '0',\
    regularMarketVolume INT DEFAULT '0',\
    averageVolume10days  INT DEFAULT '0',\
    nextFiscalYearEnd  DATETIME,\
    isEsgPopulated  VARCHAR(10) DEFAULT 'False',\
    enterpriseToEbitda  FLOAT DEFAULT '0.000',\
    earningsQuarterlyGrowth FLOAT DEFAULT '0.000',\
    morningStarRiskRating  VARCHAR(10) DEFAULT 'None',\
    fiftyDayAverage  FLOAT DEFAULT '0.0',\
    regularMarketDayLow FLOAT,\
    address1 VARCHAR(80) DEFAULT 'None',\
    address2 VARCHAR(80) DEFAULT 'None',\
    lastSplitFactor  VARCHAR(30) DEFAULT 'None',\
    uuid VARCHAR(50),\
    mostRecentQuarter BIGINT DEFAULT '0',\
    longBusinessSummary TEXT,\
    logo_url VARCHAR(80) DEFAULT 'None',\
    profitMargins  FLOAT,\
    yield FLOAT DEFAULT '0.0',\
    priceToSalesTrailing12Months FLOAT DEFAULT '0.0',\
    companyOfficers VARCHAR(50),\
    lastSplitDate DATETIME,\
    floatShares BIGINT DEFAULT '0',\
    heldPercentInstitutions  FLOAT,\
    category VARCHAR(40),\
    state   VARCHAR(10),\
    exchangeTimezoneShortName VARCHAR(15) DEFAULT 'None',\
    dateShortInterest  DATETIME,\
    openInterest   BIGINT DEFAULT '0',\
    fiveYearAverageReturn  FLOAT,\
    lastFiscalYearEnd DATETIME,\
    annualHoldingsTurnover BIGINT DEFAULT '0',\
    lastDividendValue FLOAT DEFAULT '0.00',\
    regularMarketDayHigh FLOAT,\
    exchange VARCHAR(10) DEFAULT 'None',\
    legalType    VARCHAR(60) DEFAULT 'None',\
    enterpriseValue  BIGINT DEFAULT '0',\
    dividendYield FLOAT DEFAULT '0.0',\
    ask FLOAT,\
    shortName   VARCHAR(80) DEFAULT 'None',\
    regularMarketPreviousClose FLOAT,\
    heldPercentInsiders  FLOAT DEFAULT '0.0',\
    country  VARCHAR(20) DEFAULT 'None',\
    industry  VARCHAR(80) DEFAULT 'None',\
    lastCapGain  VARCHAR(20) DEFAULT 'None',\
    dayLow FLOAT,\
    marketCap BIGINT DEFAULT '0',\
    startDate   DATETIME,\
    messageBoardId   VARCHAR(15) DEFAULT 'None',\
    averageVolume  BIGINT,\
    bookValue FLOAT,\
    toCurrency VARCHAR(10),\
    maxAge BIGINT,\
    exDividendDate DATETIME,\
    totalAssets BIGINT DEFAULT '0',\
    bidSize INT,\
    bid FLOAT,\
    open FLOAT,\
    market  VARCHAR(20),\
    city VARCHAR(60),\
    zip  VARCHAR(20),\
    morningStarOverallRating  VARCHAR(10) DEFAULT 'NONE',\
    sharesShortPriorMonth  BIGINT,\
    tradeable VARCHAR(10) DEFAULT 'False',\
    website VARCHAR(120),\
    pegRatio FLOAT,\
    forwardPE  FLOAT,\
    volume  BIGINT,\
    phone  VARCHAR(25),\
    fax VARCHAR(25),\
    sharesOutstanding BIGINT,\
    strikePrice FLOAT,\
    expireDate DATETIME,\
    averageDailyVolume10Day BIGINT,\
    annualReportExpenseRatio   FLOAT,\
    enterpriseToRevenue FLOAT,\
    fiftyTwoWeekHigh  FLOAT,\
    shortRatio  FLOAT,\
    the52WeekChange  FLOAT,\
    regularMarketPrice FLOAT,\
    priceHint  INT,\
    beta3Year   VARCHAR(15),\
    previousClose  FLOAT,\
    currency VARCHAR(5),\
    navPrice  FLOAT,\
    threeYearAverageReturn  FLOAT,\
    trailingEps FLOAT,\
    netIncomeToCommon  BIGINT,\
    fiftyTwoWeekLow  FLOAT,\
    fundInceptionDate   DATETIME,\
    dividendRate  FLOAT,\
    priceToBook FLOAT,\
    fiveYearAvgDividendYield FLOAT,\
    headSymbol VARCHAR(10),\
    underlyingSymbol VARCHAR(10),\
    underlyingExchangeSymbol VARCHAR(10),\
    fullTimeEmployees  INT,\
    sharesShortPreviousMonthDate DATETIME,\
    SandP52WeekChange  FLOAT,\
    forwardEps  FLOAT,\
    trailingAnnualDividendYield  FLOAT,\
    sharesShort  BIGINT)"
    return (mysql_table_description)
    
    
def build_stocks_daily_table():
    #mysql_table_description = "CREATE TABLE nasdaq_daily (\
    mysql_table_description = "create table stocks_daily ( row_id BIGINT AUTO_INCREMENT PRIMARY KEY,\
    quoteTime DATETIME,\
    dayHigh FLOAT,\
    revenueQuarterlyGrowth FLOAT,\
    lastMarket FLOAT,\
    regularMarketVolume INT DEFAULT '0',\
    averageVolume10days  INT DEFAULT '0',\
    fiftyDayAverage  FLOAT DEFAULT '0.0',\
    regularMarketDayLow FLOAT,\
    bid FLOAT,\
    volume24Hr BIGINT,\
    askSize INT,\
    regularMarketDayHigh FLOAT,\
    dividendYield FLOAT DEFAULT '0.0',\
    ask FLOAT,\
    regularMarketPreviousClose FLOAT,\
    dayLow FLOAT,\
    startDate   DATETIME,\
    averageVolume  BIGINT,\
    bidSize INT,\
    open FLOAT,\
    volume  BIGINT,\
    averageDailyVolume10Day BIGINT,\
    twoHundredDayAverage  FLOAT,\
    fiftyTwoWeekHigh  FLOAT,\
    regularMarketOpen FLOAT,\
    regularMarketPrice FLOAT,\
    previousClose  FLOAT,\
    symbol  VARCHAR(10),\
    sharesShort  BIGINT,\
    err FLOAT)"
    return (mysql_table_description)


def initialize_company_info_table():

    print ("Using pymysql...")

    con = pymysql.connect( host="localhost", user="user1", passwd="user1", db="stockmarket" )
    with con.cursor() as cur: 
        cur.execute("SELECT VERSION()")
        version = cur.fetchone()
        print("Database version: {}".format(version[0]))
        
        try:
            cur.execute("DROP TABLE company_info")
        except:
            pass
       
        table_build_command =  build_yahoo_company_info_table("company_info")
        
        cur.execute(table_build_command)
        
        tables = sql_query("SHOW TABLES")
        print ("Tables:\n")
        for table in tables:
            print (table)
        
    con.commit()
    con.close()
    

    
def sanitize_data(s):
    print ("***********************************Sanitize data*******************************************")
    print ("Data: ", s)
    
    if (isinstance(s, basestring)):
        s = s.strip()
        s = s.rstrip()
        return(s)
         
    if (isinstance(s, unicode)):
        unicodedata.normalize('NFKD', s).encode('ascii','ignore')
    #clean_data = re.sub(r'[^\x00-\x2050]',r'', info[data])
    elif (isinstance(s, float)):
        s = str(s)
    elif s is None:
        s = '0.00'
    elif (isinstance(s, int)):
        s = str(s)
    elif (isinstance(s, list)):
        s = 'None'
       
    if (isinstance(s, unicode)):
        unicodedata.normalize('NFKD', s).encode('ascii','ignore')
        s = s.replace(u'\u2014',u'')
             
    s = s.replace(',','')
    if (isinstance(s, unicode)):
        unicodedata.normalize('NFKD', s).encode('ascii','ignore')
        s = s.replace(u'\x91',u'')
        s = s.replace(u'\xc3',u'')
        s = str(s.encode("utf-8"))
        s = s.replace("'","")
                
        s = strip_non_ascii(s)
    return(s)      
   

def write_to_table(these_columns):

    con = pymysql.connect( host="localhost", user="user1", passwd="user1", db="stockmarket" )
    with con.cursor() as cur: 
        print ("Inserting into table")  
        cur.execute(these_columns)
    con.commit()
    con.close()
    
def sql_query(query):
    query_results = []
    con = pymysql.connect( host="localhost", user="user1", passwd="user1", db="stockmarket" )
    with con:
        cur = con.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        for row in rows:
            row_data = ""

            #print ("{0}\t {1}\t {2}".format(row[0], row[1], row[2]))
            for item in row:
                row_data = row_data + str(item) + ","
            query_results.append(row_data)

    #con.close()
    return(query_results)

def strip_non_ascii(string):
    ''' Returns the string without non ASCII characters'''
    stripped = (c for c in string if 0 < ord(c) < 127)
    return ''.join(stripped)


def make_insert_into_company_info_table_string(info):
    
    table_insert_command = "INSERT INTO company_info (quoteDate, "
    for column_name in info:
        
        if (column_name == "ebitdaMargins"):
            print (info)
        
        
        
        
        
        if (column_name == "52WeekChange"):
            my_column = "the52WeekChange"
            table_insert_command = table_insert_command + my_column + ", "
        elif (column_name == "err"):
            #my_column = "payoutRatio"
            pass

        else:
            my_column = column_name
            table_insert_command = table_insert_command + my_column + ", "

    table_insert_command = table_insert_command[:-2] # remove the last ,
    table_insert_command = table_insert_command + ") "
        
    table_insert_command = table_insert_command + "VALUES (" + "FROM_UNIXTIME(" + str(int(time.time())) + ")" + ","
    
    count = 0
    for data in info:
        s = info[data]
        
        if (isinstance(s,dict)):
            s = "88888.888"
            
        
        #if (isinstance(s, unicode)):
        #    unicodedata.normalize('NFKD', s).encode('ascii','ignore')
        #clean_data = re.sub(r'[^\x00-\x2050]',r'', info[data])
        elif (isinstance(s, float)):
            s = str(s)
        elif s is None:
            s = '0.00'
        elif (isinstance(s, int)):
            s = str(s)
        elif (isinstance(s, list)):
            s = 'None'
       
        #if (isinstance(s, unicode)):
        #    unicodedata.normalize('NFKD', s).encode('ascii','ignore')
        #    s = s.replace(u'\u2014',u'')
            
        if (data == "longBusinessSummary"):
            s = s.replace(";",".")
            s = s.replace("'","")
   
            
        if ( data == "nextFiscalYearEnd" or data == "lastSplitDate" or data == "dateShortInterest" or data == "lastFiscalYearEnd"\
             or data == "startDate"  or data == "exDividendDate" or data == "expireDate" or data == "fundInceptionDate"
             or data == "sharesShortPreviousMonthDate"
             or data == "lastDividendDate"):
            
            s = "FROM_UNIXTIME(" + s + ")"
            table_insert_command = table_insert_command + str(s) + ", "
            print (str(count)+"   "+data+"  "+str(s))
            count = count + 1
           
            
        else:  
            s = s.replace(',','')
            #if (isinstance(s, unicode)):
            #    unicodedata.normalize('NFKD', s).encode('ascii','ignore')
            s = s.replace(u'\x91',u'')
            s = s.replace(u'\xc3',u'')
            s = str(s.encode("utf-8"))
            s = s.replace("'","")
            if s[0] == "b":
                s = s[1:]
            if s == "null":
                s = "0.00"
            #print ("UNICODE TEST: "+str(type(s)))
            #if (isinstance(s, unicode)):
            #    for x in range(0,len(s)):
            #        print (ord(x))
                
                #unicodedata.normalize('NFKD', s).encode('ascii','ignore')
            #exit(0)
            #print (type(s))
            s = strip_non_ascii(s)
            if ( s == "Infinity" or s == "infinity"):
                table_insert_command = table_insert_command + "'" + str(999999999.9) +"'" + ", "
            else:
                table_insert_command = table_insert_command + "'" + str(s) +"'" + ", "
            print (str(count)+"   "+data+"  "+str(s))
            count = count + 1
        
        
    table_insert_command = table_insert_command[:-2] # remove the last ,
    
    table_insert_command = table_insert_command + ")"
    
    print (table_insert_command)
    
    print ("number of columns from internet = "+str(count))
    
    return(table_insert_command)

def make_insert_into_stocks_daily_table_string(info):
    
    needed_columns = build_stocks_daily_table()
    columns_list = needed_columns.split(",")
    
    table_insert_command = "INSERT INTO stocks_daily ("
    line_pointer = 0
    
    for column_name in columns_list:
        if (line_pointer > 0):
            column_name = column_name.strip()
            col = column_name.split(" ")
            print (col[0])
            table_insert_command = table_insert_command + col[0] + ", "
        line_pointer = line_pointer + 1
   
    table_insert_command = table_insert_command[:-2] # remove the last ,
    table_insert_command = table_insert_command + ") "
        
    table_insert_command = table_insert_command + "VALUES ("
    
    count = 0
    for data in columns_list:
        if (count > 0):
            data = data.strip()
            cols = data.split(" ")
            col = cols[0]
            col = col.strip()
           
            if ( col == "quoteTime" or col == "startDate"):
                s = "FROM_UNIXTIME(" + "'" + str(int(time.time())) + "'" + ")"
                table_insert_command = table_insert_command  + str(s) + ", "
                
            else: 
                key = sanitize_data(col)
                try:
                    s = sanitize_data(info[key])
                except:
                    s = "0.00"
                table_insert_command = table_insert_command + "'" + str(s) +"'" + ", "
        
        count = count + 1
        
        
    table_insert_command = table_insert_command[:-2] # remove the last ,
    
    table_insert_command = table_insert_command + ")"
    
    
    print ("number of columns from internet = "+str(count))
    print (table_insert_command)

    return(table_insert_command)
    
   

def get_company_info_nasdaq_nyse_amex_stocks_unfiltered_csv():
    
    time.sleep(5)
    #File from rankandfiled.com

    mycsv = open("./nyse_nasd_symbols.csv","r")
    
    counter = 0
    stock_counter = 0
    sql_insert_error = 0
    bad_symbol_list = []
    yfinance_choked = 0
    yfinance_no_data = 0
    
  
    for item in mycsv:
        counter = counter + 1
        if counter > 300000:
            break
        
        if (1 == 1):
        # get white space out of items in list. csv read imports a lot of whitespace
            symbol = item.strip() 
            if counter > 1:
                print ("Stock to process below:")
                #row_list[1] = "GCP"
                print (symbol)

                stock = (yf.Ticker(symbol))
                print (stock)
                do_not_write = 0
                time.sleep(.1) # wait for slow I/O
                try:
                    info = stock.info
                except:
                    print ("yfinance choked on ???? for "+str(symbol))
                    do_not_write = 1
                    yfinance_choked = yfinance_choked + 1
                
                if (not info):
                    print ("Yahoo returned an empty dictionary for "+str(symbol))
                    do_not_write = 1
                    yfinance_no_data = yfinance_no_data + 1
                    #for line in info:
                    #print (str(line)+"   "+str(info[line]))
                #except: 
                    #print ("Exception thrown")
                    #do_not_write = 1
                
                
                if (do_not_write == 0): 
                 
                    insert_string = make_insert_into_company_info_table_string(info)  
                    #print (insert_string)
                if (do_not_write == 0):
                
                    try:
                        print ("Writing to Table............")
                        print (insert_string)
                        write_to_table(insert_string)
                        stock_counter = stock_counter + 1
                        if sql_insert_error > 0:
                            print ("SQL insert errors = "+str(sql_insert_error))
                        print ("Processed "+str(stock_counter)+" stocks...")
                        if bad_symbol_list != []:
                            print (bad_symbol_list)
                    except:
                        print ("Write to table blew up")
                        sql_insert_error = sql_insert_error + 1
                        print ("SQL insert errors = "+str(sql_insert_error))
                        bad_symbol_list.append(str(symbol))
                   
                #time.sleep(.3)   
                   
    mycsv.close()
    print ("Total stocks processed into db: ", stock_counter)
    print ("Total stocks which choked yFinance: ", yfinance_choked)
    print ("Total stocks no data from yfinance: ",yfinance_no_data)
    print ("Total failed inserts: ",sql_insert_error)
    print ("stocks which failed insert:")
    for a_stock in bad_symbol_list:
        print (a_stock)
    


    

#------------------------- main -----------------------------------------------
initialize_company_info_table()
get_company_info_nasdaq_nyse_amex_stocks_unfiltered_csv()
