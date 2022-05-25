import sqlite3
import json
from datetime import datetime

sql_transaction = []

paired_rows = 0

def create_table(c):
    c.execute("CREATE TABLE IF NOT EXISTS parent_reply(parent_id TEXT, comment_id TEXT, parent TEXT, comment TEXT, subreddit TEXT, unix INT, score INT)")

def format_data(data):
    data = data.replace('\n',' newlinechar ').replace('\r',' newlinechar ').replace('"',"'")
    return (data)

def transaction_bldr(c,connection,sql):
    global sql_transaction
    sql_transaction.append(sql)
    if len(sql_transaction) > 1000:
        c.execute('BEGIN TRANSACTION')
        for s in sql_transaction:
            try:
                c.execute(s)
            except Exception as e:
                #print('s0 insertion',str(e))
                pass
        connection.commit()
        sql_transaction = []

def sql_insert_replace_comment(c,connection,commentid,parentid,parent,comment,subreddit,time,score):
    try:
        sql = """UPDATE parent_reply SET parent_id = "{}", comment_id = "{}", parent = "{}", comment = "{}", subreddit = "{}", unix = "{}", score = "{}" WHERE parent_id ="{}";""".format(parentid, commentid, parent, comment, subreddit, int(time), score, parentid)
        transaction_bldr(c,connection,sql)
    except Exception as e:
        print('s1 insertion',str(e))

def sql_insert_has_parent(c,connection,commentid,parentid,parent,comment,subreddit,time,score):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, parent, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}","{}",{},{});""".format(parentid, commentid, parent, comment, subreddit, int(time), score)
        transaction_bldr(c,connection,sql)
        global paired_rows
        paired_rows+=1
    except Exception as e:
        print('s0 insertion',str(e))

def sql_insert_no_parent(c,connection,commentid,parentid,comment,subreddit,time,score):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}",{},{});""".format(parentid, commentid, comment, subreddit, int(time), score)
        transaction_bldr(c,connection,sql)
    except Exception as e:
        print('s0 insertion',str(e))

def find_parent(c,pid):
    try:
        sql = "SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(pid)
        c.execute(sql)
        result=c.fetchone()
        if result!=None:
            return (result[0])
        else:
            return (False)

    except exception as e:
        return (False)

def find_existing_score(c,pid):
    try:
        sql = "SELECT score FROM parent_reply WHERE parent_id = '{}' LIMIT 1".format(pid)
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else: return False
    except Exception as e:
        #print(str(e))
        return False

def acceptable(data):
    if len(data.split(' ')) > 50 or len(data) < 2:
        return False
    elif len(data) > 1000:
        return False
    elif data.lower() == 'nice':
        return False
    elif data == '[deleted]':
        return False
    elif data == '[removed]':
        return False
    else:
        return True


def count_all_rows(c):
    sql = "SELECT COUNT(*) FROM parent_reply"
    c.execute(sql)
    result=c.fetchone()
    return result[0]

def count_null_rows(c):
    sql = "SELECT COUNT(*) FROM parent_reply WHERE parent IS NULL"
    c.execute(sql)
    result=c.fetchone()
    return result[0]

def count_non_null_rows(c):
    return (count_all_rows(c)-count_null_rows(c))

x=0

science_subreddits=["science","technology","askscience","history","math","learnpython"]
conversational_subs=["casualconversation"]
large_subreddits=["askreddit","technology","pics","aww","gaming","movies","videos","showerthoughts","todayilearned","iama","food","lifeprotips",
                  "midlyinteresting","books","blog","sports","nottheonion","starwars","news","tifu","minecraft","pokemon",
                  "pokemongo","atheism","science"]

def mainloop():
    timeframe = '2019-09-part-5'
    connection = sqlite3.connect('{}.db'.format(timeframe))
    c = connection.cursor()
    
    create_table(c)
    row_counter = 0
    earlier_real_rows = 0
    file_number=5
    whole_total_rows=0
    just_change=False

    with open(r'C:\Users\Utkarsh\Documents\RC_2019-09', buffering=1000) as f:        
        for row in f:
            row_counter+=1

            if row_counter<0:
                if row_counter % 10000 == 0:
                    print (row_counter)
                continue
            
            row = json.loads(row)
                       
            score = row['score']
            
            if score in (2,3,4):
                subreddit = row['subreddit'].lower()
                if subreddit not in blacklisted_subreddits:
                    parent_id = row['parent_id']
                    body = format_data(row['body'])
                    created_utc = row['created_utc']
                    parent_data = find_parent(c,parent_id)
                    comment_id = row['link_id']

                    if acceptable(body):
                        existing_comment_score = find_existing_score(c,parent_id)
                        if existing_comment_score:
                            if score > existing_comment_score:
                                sql_insert_replace_comment(c,connection,comment_id,parent_id,parent_data,body,subreddit,created_utc,score)
                                    
                        else:
                            if parent_data:
                                sql_insert_has_parent(c,connection,comment_id,parent_id,parent_data,body,subreddit,created_utc,score)
                                #paired_rows += 1
                            else:
                                sql_insert_no_parent(c,connection,comment_id,parent_id,body,subreddit,created_utc,score)
                                     
                                
            if row_counter % 100000 == 0:
                now_real_rows=count_non_null_rows(c)
                new_real_rows=now_real_rows-earlier_real_rows
                whole_total_rows+=new_real_rows
                
                print('Total Rows Read: {}, Added Rows: {}, Total Rows in Current DB: {}, Total Rows: {} Time: {}'.format(row_counter, new_real_rows, now_real_rows, whole_total_rows, str(datetime.now())))


                earlier_real_rows=now_real_rows

            if row_counter % 300000 == 0:

                #if row_counter==120000000:
                #    break
                
                print("Cleanin up!")
                sql = "DELETE FROM parent_reply WHERE parent IS NULL"
                c.execute(sql)
                connection.commit()

                file_number+=1
                
                timeframe="2019-09-part-{}".format(file_number)

                
                connection = sqlite3.connect('{}.db'.format(timeframe))
                c = connection.cursor()
                create_table(c)

                print ("Now using database number '{}'".format(file_number))

                earlier_real_rows = 0
