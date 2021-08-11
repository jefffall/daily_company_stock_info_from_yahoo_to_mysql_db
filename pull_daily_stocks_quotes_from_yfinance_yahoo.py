#!/usr/bin/python3
import yfinance as yf
import time
import datetime
import pymysql
import RestartStocksRising as restart

started_at = str(datetime.datetime.now())
parts = started_at.split(" ")
today_date = parts[0]

print ("Run started at "+str(started_at),flush=True)

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
    #start, end = get_start_end_dates()
    yfinance_list = []
    for symbol in mycsv:
        tickerData = yf.Ticker(symbol.strip())
        todayData = tickerData.history(period='1d')
        try:
            myopen = str(round(float(todayData['Open'][0]),2))
            low =  str(round(float(todayData['Low'][0]),2))
            high =  str(round(float(todayData['High'][0]),2))
            close =  str(round(float(todayData['Close'][0]),2))
            volume =  str(todayData['Volume'][0])
            skip = False
        except:
            bad_symbol_or_bad_date = bad_symbol_or_bad_date + 1
            print ("bad symbol: "+symbol.strip()+" or date: start="+str(today_date),flush=True)
            time.sleep(.5)
            skip = True
        if skip == False:
            time.sleep (.5)
            columns = "INSERT INTO stocks (symbol, date, exchange, open, low, high, close, volume) "+\
                            "VALUES ('"+symbol.strip()+"' ,'"+today_date+" 15:00:00'"+" ,'"+"NYSEorNASD"+"', "+myopen+", "+high+" ,"+low+", "+close+", "+volume+")"
            lines_processed = lines_processed + 1
            print ("Processed: "+symbol.strip()+" --> "+str(lines_processed)+" stocks processed...",flush=True)
            print ("Date:"+str(today_date)+" Open:"+str(myopen)+" Low:"+str(low)+" High:"+str(high)+" Close:"+str(close)+" Volume:"+str(volume),flush=True)
            yfinance_list.append(columns)
        
    print (str(len(yfinance_list))+" symbols downloaded. Writing to mysql database now...",flush=True)
    print (" ",flush=True)
    write_to_eoddata_table(yfinance_list)
    mycsv.close()
    print (" ",flush=True)
    print ("Run started at "+str(started_at),flush=True)
    print ("Run finished at: ",datetime.datetime.now(),flush=True)
    print ("Bad symbols or bad date count = "+str(bad_symbol_or_bad_date),flush=True)
    print (str(lines_processed)+" Equities processed...",flush=True)
    print (" ",flush=True)

read_stocks_from_csv_download_via_yfinance_to_mysql()

time.sleep(60)
restart.restart_stocks_rising()
