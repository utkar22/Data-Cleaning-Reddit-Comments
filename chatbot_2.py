# -*- coding: utf-8 -*-
"""
Created on Wed Jul 29 20:22:28 2020

@author: Utkarsh
"""

import sqlite3
import pandas as pd

timeframe="2018-08"


connection=sqlite3.connect('{}.db'.format(timeframe))
c=connection.cursor()
limit=50000
last_unix=0
cur_length=limit
counter = 0

while cur_length==limit:
    df=pd.read_sql("SELECT * FROM parent_reply WHERE unix > {} and parent NOT NULL and score > 0 ORDER BY unix ASC LIMIT {}".format(last_unix,limit),connection)
    last_unix=df.tail(1)['unix'].values[0]
    cur_length=len(df)
    
    with open('train.from','a', encoding='utf8') as f:
        for content in df['parent'].values:
            f.write(content+'\n')

    with open('train.to','a', encoding='utf8') as f:
        for content in df['comment'].values:
            f.write(str(content)+'\n')
            
    counter += 1
    if counter % 2 == 0:
        print(counter*limit,'rows completed so far')
            
    
    
    
