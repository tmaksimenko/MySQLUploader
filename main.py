import mysql.connector as msql
from mysql.connector import Error
import pandas as pd
import config

path = "csvupload/" + config.filename

dataframe = pd.read_csv(path, index_col=False, delimiter=',')
data = dataframe.values.tolist()
for datum in data:
    datum = tuple(datum)

columns = dataframe.columns.values.tolist()
types = "(%s" + ", %s"*(len(columns) - 1) + ")"

try:
    connection = msql.connect(
        host=config.host, user=config.user, password=config.password, database=config.database)

    if connection.is_connected():
        cursor = connection.cursor()

    sql = "INSERT INTO " + config.tablename + " (" + ", ".join(columns) + ") VALUES " + types

    cursor.executemany(sql, data)
    connection.commit()
    print("Records were inserted")


except Error as e:
    print("Error while connecting to MySQL:", e)
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("Connection closed")
