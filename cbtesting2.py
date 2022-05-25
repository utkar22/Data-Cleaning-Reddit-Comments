import sqlite3
import json
from datetime import datetime

timeframe = '2018-08-part-358'
sql_transaction = []

connection = sqlite3.connect('{}.db'.format(timeframe))
c = connection.cursor()

def deleterows():
    sql = "DELETE FROM parent_reply WHERE parent IS NULL"
    c.execute(sql)
    connection.commit()

def count_all_rows():
    sql = "SELECT COUNT(*) FROM parent_reply"
    c.execute(sql)
    result=c.fetchone()
    return result[0]

def count_null_rows():
    sql = "SELECT COUNT(*) FROM parent_reply WHERE parent IS NULL"
    c.execute(sql)
    result=c.fetchone()
    return result[0]

def count_non_null_rows():
    return (count_all_rows()-count_null_rows())

def delete_same():
    sql = "DELETE FROM parent_reply WHERE parent=comment"
    c.execute(sql)
    connection.commit()

def number_of_rows():
    row_counter=0
    with open(r'C:\Users\Utkarsh\Documents\chatbot\RC_2018-08', buffering=100000) as f:
        for row in f:
            row_counter+=1

    return (row_counter)

def number_of_rows_in_file():
    f=open("train.from","r")
    x=f.readline()
    f.close()
    return (len(x))


