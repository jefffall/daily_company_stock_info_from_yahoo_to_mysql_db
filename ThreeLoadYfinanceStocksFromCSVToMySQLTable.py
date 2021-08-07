import pymysql
from datetime import date

today_date = date.today()

def convert_month(month):
    if month == "Jan":
        return ("01")
    elif month == "Feb":
        return("02")
    elif month == "Mar":
        return("03")
    elif month == "Apr":
        return ("04")
    elif month == "May":
        return("05")
    elif month == "Jun":
        return("06")
    elif month == "Jul":
        return("07")
    elif month == "Aug":
        return("08")
    elif month == "Sep":
        return("09")
    elif month == "Oct":
        return("10")
    elif month == "Nov":
        return("11")
    elif month == "Dec":
        return("12")
    else:
        print ("Month conversion. Bad month from eoddata spread sheet")
    
def write_to_eoddata_table(these_columns):

    con = pymysql.connect( host="localhost", user="user1", passwd="user1", db="eoddata" )
    with con.cursor() as cur:
        for row in these_columns:
            cur.execute(row)
    con.commit()
    con.close()
    return

def process_yfinance_csv():
    print ("Beginning Process yFinance...")
    lines_processed = 0
   
    mycsv = open("/home/jfall/daily_stock_data.csv","r")
    yfinance_list = [] 
    for line in mycsv:
        lines_processed = lines_processed + 1
        column = line.split(",")
        if len(column) > 5:
            symbol = column[0]
            date = column[1]
            exchange = column[2]
            openprice = column[3]
            high = column[4]
            low = column[5]
            close = column[6]
            volume = column[7]
        
            #datetime = "'2021-08-06 15:00:00'"
            datetime = "'"+str(today_date)+" 15:00:00'"           
            columns = "INSERT INTO stocks (symbol, date, exchange, open, low, high, close, volume) "+\
                       "VALUES ('"+symbol+"' ,"+datetime+" ,'"+exchange+"', "+openprice+", "+high+" ,"+low+", "+close+", "+volume+")"
            yfinance_list.append(columns)
    print (yfinance_list)
    write_to_eoddata_table(yfinance_list)
    mycsv.close()
    #out.close()   
    print (lines_processed)
   

#------------------------- main -----------------------------------------------

#process_yfinance_csv()


