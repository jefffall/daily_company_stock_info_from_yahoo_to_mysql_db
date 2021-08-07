import pymysql

def read_data_from_company_info_db():
    con = pymysql.connect( host="localhost", user="user1", passwd="user1", db="stockmarket" )
    with con.cursor() as cur:
        # symbol, date, exchange, open, low, high, close, volume)
        #cur.execute("select symbol, exchange, open, Daylow, dayHigh, volume from company_info where symbol != 'None'")
        cur.execute("select symbol, quoteDate, exchange, open, dayHigh, Daylow, bid, volume from company_info where symbol != 'None'")
        mydata = cur.fetchall()
    return(mydata)

def convertTuple(tup):
        # initialize an empty string
    mystr = ''
    for item in tup:
        mystr = mystr + str(item) + ","
    return mystr[:-1]

def create_daily_stock_data_csv():
    print ("Beginning create daily stock data csv from company_info table")
    fdout = open("/home/jfall/daily_stock_data.csv","w")        
    rows = 0
    mydata = read_data_from_company_info_db()
    for line in mydata:
        rows = rows + 1
        myrow = convertTuple(line)
        print (myrow)
        fdout.write(myrow+"\n")
