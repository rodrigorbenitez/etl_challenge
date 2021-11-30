import sqlite3
import utils
import pandas as pd
from numpy import nan as Nan

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
            createdAt TEXT ,
            updatedAt TEXT,
            firstName TEXT,
            lastName TEXT,
            address TEXT,
            city TEXT,
            country TEXT,
            zipCode TEXT,
            email TEXT,
            birthDate TEXT,
            user_id INTEGER PRIMARY KEY,
            gender TEXT,
            isSmoking TEXT,
            profession TEXT,
            income TEXT
            

        )
        """,
        """
        CREATE TABLE IF NOT EXISTS subscriptions (
            createdAt TEXT,
            startDate TEXT,
            endDate TEXT,
            status TEXT,
            amount TEXT,
            user_id INTEGER,
            FOREIGN KEY (user_id) REFERENCES users(user_id)
                    ON DELETE CASCADE
                    ON UPDATE NO ACTION
        )
        """,
                """
        CREATE TABLE IF NOT EXISTS messages (
            createdAt TIMESTAMP,
            message TEXT,
            receiverId INTEGER,
            id INTEGER PRIMARY KEY,
            senderId INTEGER,
            FOREIGN KEY (receiverId) REFERENCES users(user_id)
                    ON DELETE CASCADE 
                    ON UPDATE NO ACTION,
            FOREIGN KEY (senderId)
               REFERENCES users (user_id)
                    ON DELETE CASCADE 
                    ON UPDATE NO ACTION         

        )
        """]
     # Database Connection
    db = get()
    cursor = db.cursor()

    # Execute model SQL
    for table in model: cursor.execute(table)

    utils.log('Database Schema created!')
     


# Method to load database after creation with dimensions
def initialize():
    utils.log('Initializing Database')
    utils.log('Generating users dimensions')
      
  
    utils.log('Initialized Database!')