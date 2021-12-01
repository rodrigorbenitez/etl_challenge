import db, utils
import pandas as pd
import numpy as np
from numpy import nan as Nan
import sqlite3
import os , datetime
import hashlib



# Database Setup
# --------------
# Firstly, check if the database is created, ff not create tables and load it
have_to_initialize_db = not os.path.isfile(db.DATABASE_NAME) # Return True of False if the file exists
# Execute SQL code to create model if have_to_initialize_db is False
if have_to_initialize_db: 
    db.create()
    db.initialize()
else: utils.log('Database is already created')


#EXTRACT AND TRANSFORM

def users():
    url_users='https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/users'
    df_users = pd.read_json(url_users)
    profile_dict=df_users['profile']
    
    # Extracting profile objects from profile column and then concate with the main table
   
    df_profile = pd.DataFrame([x for x in profile_dict])
    df_users=pd.concat([df_users, df_profile], axis=1)
    df_users=df_users[['id','createdAt','updatedAt','firstName','lastName','address','city','country','zipCode','email','birthDate','gender','isSmoking','profession','income']]
    df_users=df_users.rename(columns={"id": "user_id"})
    
    # Apply hashing function to the column in order to hide PII
    
    df_users[['firstName','lastName','address','birthDate']] = df_users[['firstName','lastName','address','birthDate']].astype(str)
    columns=['firstName','lastName','address','birthDate']
    for column in columns:
        df_users[column] = df_users[column].apply(lambda x: hashlib.sha256(x.encode()).hexdigest())
    
    #extract domain from emails
    df_users['email']=df_users['email'].str.extract('((?<=@).*)')
    
    return df_users

def subscriptions():
    url_users='https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/users'    
    df_users = pd.read_json(url_users)

    # Extracting subscription objects from subscription column and then create a separate dataframe

    subscription_df=df_users[['id','subscription']]
    subscription_df = subscription_df.explode('subscription')
    df = pd.DataFrame(columns = ['createdAt', 'startDate','endDate','status','amount','id'])
    for index, row in subscription_df.iterrows():
        if row['subscription'] is not Nan:
            dicts=row['subscription']
            dicts['id']=row['id']
            df = df.append(dicts, ignore_index=True, sort=False)
        else:
            df2 = {'createdAt': Nan, 'startDate': Nan, 'endDate':Nan, 'status': Nan, 'amount': Nan, 'id': row['id'], }
            df = df.append(df2, ignore_index = True)
    df=df.rename(columns={"id": "user_id"})
    return df

def messages():
    url_messages='https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/messages'    
    df_messages=pd.read_json(url_messages)
    
    # Apply hashing function to the column in order to hide messages

    df_messages['message']=df_messages['message'].astype(str)
    df_messages['message'] = df_messages['message'].apply(lambda x: hashlib.sha256(x.encode()).hexdigest())
    
    return df_messages


#Saving each dataframe in a separate variable executing functions

users_df=users()
subscriptions_df=subscriptions()
mmessages_df=messages()

#Connecting to sqlite
conn = sqlite3.connect('spark.db')

#Creating a cursor object using the cursor() method
cursor = conn.cursor()



#LOAD

start_execution = datetime.datetime.now()

users_df.to_sql('users', conn, if_exists='append', index=False)
subscriptions_df.to_sql('subscriptions', conn, if_exists='append', index=False)
mmessages_df.to_sql('messages', conn, if_exists='append', index=False)

conn.close()

utils.execution_time(start_execution)
utils.log('Finish  ETL')