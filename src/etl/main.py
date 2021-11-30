from datetime import datetime
import os, datetime
import db, utils
import pandas as pd
import numpy as np



# Database Setup
# --------------
# Firstly, check if the database is created, ff not create tables and load it
have_to_initialize_db = not os.path.isfile(db.DATABASE_NAME) # Return True of False if the file exists
# Execute SQL code to create model if have_to_initialize_db is False
if have_to_initialize_db: 
    db.create()
    db.initialize()
else: utils.log('Database is already created')
