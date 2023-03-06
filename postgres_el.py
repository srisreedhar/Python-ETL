# This is using Insert Query

import pandas as pd
import psycopg2
import sqlalchemy
import boto3
import os

# create connection
# generally these are set as os.environ variables should not be hardcoded
connection = psycopg2.connect(
    host='hostname',
    port='port',
    user='username',
    password='password',
    database='database'
)

# create cursor/executor
cursor = connection.cursor()

# Extract
data = pd.read_csv('bankloans.csv')

# columns
"""
age	
job	
marital	
education	
default
balance	
housing	
loan	
contact	
day	
month	
duration	
campaign	
pdays	
previous	
poutcome	
y

"""


# Load

for index, row in data.iterrows():
    cursor.execute("""INSERT INTO bankloan (column1, column2, column3,column4,column5,column6,column7,column8,column9,column10,column11,column12,column13,column14,column15,column16,column17) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", 
                   (row['column1'], row['column2'], row['column3'],row['column4'],row['column5'],row['column6'],row['column7'],row['column8'],row['column9'],row['column10'],row['column11'],row['column12'],row['column13'],row['column14'],row['column15'],row['column16'],row['column17'])
                   )


# commit to db
connection.commit()
