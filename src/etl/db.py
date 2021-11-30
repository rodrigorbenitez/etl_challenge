import sqlite3
import utils
import pandas as pd

DATABASE_NAME = 'spark.db'

# Get database connection
def get():
    return sqlite3.connect(DATABASE_NAME)


# Create tables
def create():

    utils.log('Creating database schema')
    
    # Table definition
    model = [
         """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            createdAt TEXT NOT NULL,
            updatedAt TEXT NOT NULL,
            firstName TEXT NOT NULL,
            lastName TEXT NOT NULL,
            address TEXT NOT NULL,
            city TEXT NOT NULL,
            country TEXT NOT NULL,
            zipCode TEXT NOT NULL,
            email TEXT NOT NULL,
            birthDate TEXT NOT NULL,
            gender TEXT NOT NULL,
            isSmoking TEXT NOT NULL,
            profession TEXT NOT NULL,
            income TEXT NOT NULL
            

        )
        """,
        """
        CREATE TABLE IF NOT EXISTS subscriptions (
            createdAt TEXT NOT NULL,
            startDate TEXT NOT NULL,
            endDate TEXT NOT NULL,
            status TEXT NOT NULL,
            amount TEXT NOT NULL,
            FOREIGN KEY (id)
               REFERENCES users (id)
                    ON DELETE CASCADE 
                    ON UPDATE NO ACTION         
        )
        """,
                """
        CREATE TABLE IF NOT EXISTS messages (
            createdAt TIMESTAMP,
            message TEXT NOT NULL,
            FOREIGN KEY (receiverId),
            FOREIGN KEY (receiverId)
               REFERENCES users (id)
                    ON DELETE CASCADE 
                    ON UPDATE NO ACTION,
             FOREIGN KEY (senderId)
               REFERENCES users (id)
                    ON DELETE CASCADE 
                    ON UPDATE NO ACTION         

        )
        """
        
    ]
     # Database Connection
    db = get()
    cursor = db.cursor()

    # Execute model SQL
    for table in model: cursor.execute(table)

    utils.log('Database Schema created!')
     

# Method to Load tables

def load(table, schema, values):
    # Database Connection
    db = get()
    cursor = db.cursor()

    # From schema auto generated INSERT statement
    question_tags = str(['?' for n in range(0,len(schema))])[1:-1].replace("'", '')
    schema_names = str(schema)[1:-1].replace("'", '')
    statement = f'INSERT INTO {table} ({schema_names}) VALUES({question_tags});'

    utils.log(f'Loading {table}')
    print(f'\t{statement}')

    # Insert rows
    cursor.executemany(statement, values)
    print('\t* Inserted', cursor.rowcount, 'sources.')
    db.commit()

    # Queries to log inserts.
    query = f'SELECT * FROM {table} LIMIT 1;'
    cursor.execute(query)
    print(f'\t* Example of row inserted: {cursor.fetchone()}')

    # # Close the connection
    db.close()

    utils.log(f'Load {table}')

# Dataframes generation from url

def users():
    url_users='https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/users'
    df_users = pd.read_json(url_users)
    profile_dict=df_users['profile']
    df_profile = pd.DataFrame([x for x in profile_dict])
    df_users=pd.concat([df_users, df_profile], axis=1)
    df_users[['id','createdAt','updatedAt','firstName','lastName','address','city','country','zipCode','email','birthDate','gender','isSmoking','profession','income']]
    return df_users

def subscriptions():
    url_users='https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/users'    
    df_users = pd.read_json(url_users)
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
    return df

def messages():
    url_messages='https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/messages'    
    df_messages=pd.read_json(url_messages)     
    return df_messages

# Method to load database after creation with dimensions
def initialize():
    utils.log('Initializing Database')
    utils.log('Generating users dimensions')
    users=users()
    subscriptions=subscriptions()
    messages=messages()
    
    load('users', ['id','createdAt','updatedAt','firstName','lastName','address','city','country','zipCode','email','birthDate','gender','isSmoking','profession','income'], users)
    load('subscriptions', ['createdAt','startDate','endDate','status','amount','id'], subscriptions)
    load('messages', ['createdAt','message','receiverId','id','senderId'], messages)    
    
    utils.log('Initialized Database!')