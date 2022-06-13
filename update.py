import datetime
import pandas as pd
from sqlalchemy import create_engine, types
import mysql.connector

def importCsvIntoMySQL(url, table_name):
    df = pd.read_csv(url, index_col=False)
    print(df.head())
    engine = create_engine(
        'mysql://admin:caoanhvan123@doantrucquan.cfhk4lqvpnds.us-east-1.rds.amazonaws.com/DATA')  # enter your password and database names here

    df.to_sql(table_name, con=engine, index=False, if_exists='append')  # Replace Table_name with your sql table name

mydb = mysql.connector.connect(
    host="doantrucquan.cfhk4lqvpnds.us-east-1.rds.amazonaws.com",
    user="admin",
    password="caoanhvan123",
    database="DATA"
)

def testData(mydb, Previous_Date):
    mycursor = mydb.cursor()
    # Tim ngay duoc truyen vao co trong tap du lieu chua
    mycursor.execute("SELECT DISTINCT  date(Last_Update)  from COVID_19 "
                     + "where date(Last_Update) = " + "'" + Previous_Date + "'")
    myresult = mycursor.fetchall()
    try:
        date = myresult[0][0].strftime("%m-%d-%Y")
        print("Ngay lay trong data: "+date)
        return 0
    except:
        print("Chua co du lieu nay trong data")
        return 1

def update():
    #update tu 10 ngay truoc
    i = 10
    while i > 1:

        # Lay ngay
        Previous_Date = datetime.datetime.today() - datetime.timedelta(days=i)
        # Lay ten file tren github
        StrPrevious_Date = Previous_Date.strftime("%m-%d-%Y")
        name_File = StrPrevious_Date + ".csv"

        # Lay ngay cua du lieu tren git = ten file + 1
        Previous_Date = Previous_Date + datetime.timedelta(days=1)
        Previous_Date = Previous_Date.strftime("%Y-%m-%d")
        print("Ten file duoc lay xuong: " + name_File)

        # Kiem tra du lieu da co hay chua
        result = testData(mydb, Previous_Date)
        print(result)

        # Thuc hien cap nhat
        if(result == 1):
            url_world='https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/'+name_File
            importCsvIntoMySQL(url_world,'COVID_19')
            print("Them vao COVID_19 thanh cong !")

            url_us = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports_us/' + name_File
            importCsvIntoMySQL(url_us, 'data_covid_us')
            print("Them vao data_covid_us thanh cong !")
        i -= 1
        print("=========================================")

def upDateUsName():
    # Cap nhat lai ten US -> United States
    mycursor = mydb.cursor()
    sql = "UPDATE COVID_19 SET  Country_Region= 'United States' WHERE Country_Region = 'US'"
    mycursor.execute(sql)
    mydb.commit()
    print("So dong nuoc My duoc cap nhat: ")
    print(mycursor.rowcount, "record(s) affected")

if __name__ == '__main__':
    print(mydb)
    update()
    upDateUsName()
