def get_nameFile_current():
    import datetime

    Previous_Date = datetime.datetime.today() - datetime.timedelta(days=1)
    Previous_Date = Previous_Date.strftime("%m-%d-%Y")
    print(Previous_Date)
    name_File = Previous_Date + ".csv"
    return (name_File)
def importCsvIntoMySQL(url,table_name):

    import pandas as pd
    from sqlalchemy import create_engine, types

    df = pd.read_csv(url, index_col=False)
    print(df.head())
    engine = create_engine(
        'mysql://admin:tranduy2906@doan.c3ftrgdgjym4.us-east-1.rds.amazonaws.com/DATA')  # enter your password and database names here

    df.to_sql(table_name, con=engine, index=False, if_exists='append')  # Replace Table_name with your sql table name

if __name__ == '__main__':
    file_name=get_nameFile_current()

    url_world='https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/'+file_name
    importCsvIntoMySQL(url_world,'COVID_19')

    url_us = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports_us/' + file_name
    importCsvIntoMySQL(url_us, 'data_covid_us')

