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
            createdAt TIMESTAMP,
            updatedAt TIMESTAMP,
            firstName TEXT NOT NULL,
            lastName TEXT NOT NULL,
            address TEXT NOT NULL,
            city TEXT NOT NULL,
            country TEXT NOT NULL,
            zipCode TEXT NOT NULL,
            email TEXT NOT NULL,
            birthDate TEXT NOT NULL,
            profile TEXT NOT NULL,
            subscription TEXT NOT NULL,
            id INTEGER PRIMARY KEY

        )
        """,
        """
        CREATE TABLE IF NOT EXISTS subscriptions_and_messages (
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

def users():
    df_users = pd.read_json('https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/users')
    return df_users

def df_subscription_and_messages():
    df_users = pd.read_json('https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/messages')
    return df_subscription_and_messages


# Method to load database after creation with dimensions
def initialize():
    utils.log('Initializing Database')
    utils.log('Generating users dimensions')
    users=users()
    subscription_and_messages=df_subscription_and_messages()
    
    load('users', ['createdAt','updatedAt','firstName','lastName','address','city','country','zipCode','email','birthDate','profile','subscription','id'], users)
    load('subscription_and_messages', ['createdAt','message','receiverId','id','senderId'], subscription_and_messages)

    
    
    utils.log('Initialized Database!')