import sqlite3
import time

timeframe_1 = '2019-09'
connection_1 = sqlite3.connect('{}.db'.format(timeframe_1))
c_1 = connection_1.cursor()

def count_all_rows(c):
    sql = "SELECT COUNT(*) FROM parent_reply"
    c.execute(sql)
    result=c.fetchone()
    return result[0]


for a in range(2,4):
    timeframe_2 = '2019-09-part-{}'.format(a)
    connection_2 = sqlite3.connect('{}.db'.format(timeframe_2))
    c_2 = connection_2.cursor()
    
    sql="SELECT * FROM parent_reply"
    c_2.execute(sql)
    r_2=c_2.fetchall()

    sql_transaction=[]

    for b in r_2:
        parentid, commentid, parent, comment, subreddit, time, score=b

        try:
            sql_2="""INSERT INTO parent_reply (parent_id, comment_id, parent, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}","{}",{},{});""".format(parentid, commentid, parent, comment, subreddit, int(time), score)
            sql_transaction.append(sql_2)
            #print (sql_2)
            #time.sleep(60)
        except Exception as e:
            print (e)

    c_1.execute('BEGIN TRANSACTION')
    
    for s in sql_transaction:
        try:
            c_1.execute(s)
        except Exception as e:
            print('s0 insertion',str(e))
            time.sleep(60)
    connection_1.commit()

    print ("DB {} Done!".format(a))
