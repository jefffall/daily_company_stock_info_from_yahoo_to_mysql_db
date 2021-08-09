#!/usr/bin/python3
import yfinance as yf
import time
import datetime
import pymysql
import RestartStocksRising as restart

started_at = str(datetime.datetime.now())

def get_start_end_dates():
    started_at = str(datetime.datetime.now())
    parts = started_at.split(" ")
    start_date = parts[0]
    parts = start_date.split("-")
    next_day_str = str(int(parts[2])+1)
    if len(next_day_str) < 2:
        next_day_str = "0" + next_day_str
    next_day = parts[0]+"-"+parts[1]+"-"+next_day_str
    return start_date.strip(), next_day.strip()

print ("Run started at "+str(started_at))

def write_to_eoddata_table(these_columns):
    con = pymysql.connect( host="localhost", user="user1", passwd="user1", db="eoddata" )
    with con.cursor() as cur:
        for row in these_columns:
            cur.execute(row)
    con.commit()
    con.close()
    return

def read_stocks_from_csv_download_via_yfinance_to_mysql():
    lines_processed = 0
    bad_symbol_or_bad_date = 0
    mycsv = open ("eoddata_nyse_nasdaq_stock_list_cleaned.csv","r")
    start, end = get_start_end_dates()
    yfinance_list = []
    mydata = None
    for symbol in mycsv:
        try:
            data = yf.download(symbol, start=start, end=end)
        except:
            mydata = str(data)
            #print (mydata)
            print ("exiting....")
            exit(0)
        print ("mydata = ",mydata)
        if mydata != None:
            print (mydata)
            parts = mydata.split("\n")
            clean_string = ' '.join(parts[2].split())
            rows = clean_string.split(" ")
            date = rows[0]
            myopen = str(round(float(rows[1]),2))
            high =  str(round(float(rows[2]),2))
            low =  str(round(float(rows[3]),2))
            close =  str(round(float(rows[4]),2))
            adjclose =  str(round(float(rows[5]),2))
            volume =  str(round(float(rows[1]),6))
            print (date, myopen,high, low, close, adjclose, volume)
            time.sleep (1)
            columns = "INSERT INTO stocks (symbol, date, exchange, open, low, high, close, volume) "+\
                           "VALUES ('"+symbol+"' ,"+date+" ,'"+"NYSEorNASD"+"', "+myopen+", "+high+" ,"+low+", "+close+", "+volume+")"
            lines_processed = lines_processed + 1
            print ("Processed: "+symbol+" "+str(lines_processed)+" stocks processed...")
            yfinance_list.append(columns)
            time.sleep(1)
        else:
            bad_symbol_or_bad_date = bad_symbol_or_bad_date + 1
            mydata = None
            print ("bad symbol: "+symbol+" or date: start="+str(start)+" end="+str(end))
            time.sleep(1)
    print (str(len(yfinance_list))+" symbols downloaded. Writing to mysql database now...")
    print (" ")
    write_to_eoddata_table(yfinance_list)
    mycsv.close()
    print (" ")
    print ("Run started at "+str(started_at))
    print ("Run finished at: ",datetime.datetime.now()) 
    print ("Bad symbols or bad date count = "+str(bad_symbol_or_bad_date)) 
    print (str(lines_processed)+" Equities processed...")
    print (" ")
    
read_stocks_from_csv_download_via_yfinance_to_mysql()

time.sleep(20)
restart.restart_stocks_rising()
