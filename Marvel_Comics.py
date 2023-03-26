#import all necessary libraries into the code.
import pandas as pd 
import re
from re import sub 
from decimal import Decimal
from datetime import datetime
import mysql.connector as mysql
from mysql.connector import Error

#Step 1: reading the csv file into the code and storing it in a database
Data = pd.read_csv("Marvel_Comics.csv")
print(Data.head())

#step 2: connecting to the mysql and creating the database
try:
    conn = mysql.connect(host='localhost', user='root', password='a1h2m3e4d5')
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS Marvel_Comics;")
        print("Marvel_Comics database is created")
except Error as e:
    print("Error while connecting to MySQL", e)
    
#step 3: connected to the mysql server to import the data
try:
    conn = mysql.connect(host='localhost', database = 'Marvel_comics', user='root', password='a1h2m3e4d5')
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)

#comic table: created the comic table and created all the appropriate attributes
        cursor.execute('DROP TABLE IF EXISTS comic;') #dropping the existing table incase of running the code multiple times
        print('Creating table......')
        cursor.execute("CREATE TABLE comic (comic_id INT NOT NULL,"
                       " comic_name VARCHAR(200) NOT NULL,"
                       " active_year_start INTEGER, "
                       " active_year_end INTEGER, "
                       " issue_title VARCHAR(200), "
                       " publish_date DATE,"
                       " issue_description LONGTEXT,"
                       " imprint VARCHAR(100),"
                       " format VARCHAR(100),"
                       " Rating VARCHAR(100),"
                       " Price FLOAT,"
                       " PRIMARY KEY (comic_id) )")
        print("comic table is created......")
        
#cover_artist table: created the cover artist table and inserted all appropriate attributes 
        cursor.execute("DROP TABLE IF EXISTS cover_artist;") #dropping the existing table incase of running the code multiple times
        print("Creating table....")
        cursor.execute("CREATE TABLE cover_artist (cover_artist_id INT NOT NULL,"
                       " cover_artist_name VARCHAR(40) NOT NULL,"
                       " PRIMARY KEY (cover_artist_id) )")
        print("cover_artist table is created....")
        
#comic_artist table: created the comic cover artist table and inserted all appropriate attributes
        cursor.execute("DROP TABLE IF EXISTS comic_cover_artist;") #dropping the existing table incase of running the code multiple times
        print("Creating table....")
        cursor.execute("CREATE TABLE comic_cover_artist (comic_id INT NOT NULL,"
                       "cover_artist_id INT NOT NULL)")
        print("comic_cover_artist table is created....")
        
#penciler table: created the penciler table and inserted all appropriate attributes
        cursor.execute("DROP TABLE IF EXISTS penciler;") #dropping the existing table incase of running the code multiple times
        print("Creating table....")
        cursor.execute("CREATE TABLE penciler (penciler_id INT NOT NULL,"
                       "penciler_name VARCHAR(40) NOT NULL,"
                       "PRIMARY KEY (penciler_id) )")
        print("penciler table is created....")
        
#comic_penciler table: created the comic penciler table and inserted all appropriate attributes
        cursor.execute("DROP TABLE IF EXISTS comic_penciler;") #dropping the existing table incase of running the code multiple times
        print("Creating table....")
        cursor.execute("CREATE TABLE comic_penciler (comic_id INT NOT NULL,"
                       " penciler_id INT NOT NULL )")
        print("comic_penciler table is created....")
        
#writer table: created the writer table and inserted all appropriate attributes
        cursor.execute("DROP TABLE IF EXISTS writer;") #dropping the existing table incase of running the code multiple times
        print("Creating table....")
        cursor.execute("CREATE TABLE writer (writer_id INT NOT NULL,"
                       " writer_name VARCHAR(40) NOT NULL,"
                       "PRIMARY KEY (writer_id) )")
        print("writer table is created....")
        
#comic_writer table: created the comic writer table and inserted all appropriate attributes
        cursor.execute("DROP TABLE IF EXISTS comic_writer;") #dropping the existing table incase of running the code multiple times
        print("Creating table....")
        cursor.execute("CREATE TABLE comic_writer (comic_id INT NOT NULL,"
                       " writer_id INT NOT NULL )")
        print("comic_writer table is created....")
        
        
        comic_id = 0 #attribute clarification 
        cover_artist_id = 0 #attribute clarification 
        penciler_id = 0 #attribute clarification 
        writer_id = 0 #attribute clarification 
        for i,row in Data.iterrows(): #primary key creation and auto numbering
            print (i)
            comic_id = 1
            
            comic_name = row[0].encode("utf-8") #attribute clarification 
            
            active_year = re.split('[-)()]',row[1]) # splitted the text to only keep the year values
            #active_year = row[1].split('-')
            size = len(active_year)
            
            active_year_start = 0 #attribute clarification 
            active_year_end = 0 #attribute clarification 
            
            if len(active_year) == 2: #checks if there is a start date and changes it to 2023
                if active_year[1].strip() == "Present":
                    active_year[1] = "2023"
                active_year_start = active_year[1]
                active_year_end = active_year[1]
            
            issue_title = row[2] #attribute clarification 
            publish_date = None #attribute clarification 
            row[3] = row[3].replace('-','')
            if row[3].strip() == "None": #changes none value in text as null values in python
                publish_date = None #attribute clarification
            else: 
                publish_date = datetime.strptime(row[3], '%B %d, %Y')
                publish_date = publish_date.strftime('%Y-%m-%d')# changes the date format to the appropriate form
            issue_description = row[4]
#penciler: changed the data in the penciler table to make adapt to normalization 
        row[5] = row[5].replace('\"','') # replaces all / to empty space
        penciler=re.split(',|\/',row[6])# splits the data to text segments
            
        for p in penciler: #adds an id for each penciler and puts the penciler id and comic id in the comic penciler table
            cursor.execute("SELECT penciler_id FROM penciler where penciler_name=\""+p+"\";")
            results = cursor.fetchall()
            if len(results) == 0:
                sql = "INSERT INTO Marvel_Comics.penciler VALUES (%s, %s)"
                val = (penciler_id, p)
                cursor.execute(sql,val)
                conn.commit()
                    
                sql = "INSERT INTO Marvel_Comics.comic_penciler VALUES (%s, %s)"
                val = (comic_id, penciler_id)
                cursor.execute(sql, val)
                conn.commit()
                penciler_id = penciler_id +1
            else:
                sql = "INSERT INTO Marvel_Comics.comic_penciler VALUES (%s, %s)"
                val = (penciler_id, results[0][0])
                cursor.execute(sql, val)
                conn.commit()
#writer: changed the data in the writer table to make adapt to normalization      
        row[6] = row[6].replace('\"','')
        writer=re.split(',|\/',row[6]) #attribute clarification 
        
        for w in writer:#adds an id for each writer and puts the writer id and comic id in the comic writer table
            cursor.execute("SELECT writer_id FROM writer where writer_name=\""+w+"\";")
            results = cursor.fetchall()
            if len(results) == 0:
                sql = "INSERT INTO Marvel_Comics.writer VALUES (%s, %s)"
                val = (writer_id, w)
                cursor.execute(sql,val)
                conn.commit()
                
                sql = "INSERT INTO Marvel_Comics.comic_writer VALUES (%s, %s)"
                val = (comic_id, writer_id)
                cursor.execute(sql, val)
                conn.commit()
                writer_id = writer_id +1 #attribute clarification 
            else:
                sql = "INSERT INTO Marvel_Comics.comic_writer VALUES (%s, %s)"
                val = (comic_id, results[0][0])
                cursor.execute(sql, val)
                conn.commit()
        Imprint = row[8] #attribute clarification 
        format = row[9] #attribute clarification 
        Rating = row[10] #attribute clarification 
        if row[11].strip() == 'Free': #attribute clarification and changes Free values to 0
            price = 0.0
        elif row[11].strip() == 'None':
            price = 0.0
        else:
            number = row[11].strip().strip('$')#removes the $ sign from the data
            Price = number
        #insert the attributes to the comic table
        sql = "INSERT INTO Marvel_Comics.comic VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (comic_id, comic_name, active_year_start, active_year_end, issue_title, publish_date, issue_description, Imprint, format, Rating, Price)
        
        conn.commit()
except Error as e:
    print("Error while connecting to MySQL", e)       
               