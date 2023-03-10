import pandas as pd
from sqlalchemy import create_engine
import requests
import boto3
import urllib3
import json


data = pd.read_csv('data.csv')


# data = data.rename(columns={'old_name': 'new_name'})
# data = data.groupby('group_by_column').agg({'aggregate_column': 'sum'})


# Load

engine = create_engine('redshift+psycopg2://username:password@host:port/database')

data.to_sql('table_name', 
             engine, 
             index=False, 
             if_exists='replace')


engine = create_engine('redshift+psycopg2://username:password@host:port/database')

data.to_sql('table_name', engine, index=False, if_exists='replace')

print("successful")

# send notification to a chime group
# Lambda setup required

http = urllib3.PoolManager()


def lambda_handler(event, context):
    url = "https://hooks.chime.aws/incomingwebhooks/xxxxxxx"
    msg = {"Content": event["Records"][0]["Sns"]["Message"]}
    encoded_msg = json.dumps(msg).encode("utf-8")
    resp = http.request("POST", url, body=encoded_msg)
    print(
        {
            "message": event["Records"][0]["Sns"]["Message"],
            "status_code": resp.status,
            "response": resp.data,
        }
    )

# send notification to a Slack