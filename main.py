# import necessary libraries
import requests  #to handle HTTP requests
import sqlalchemy   # for database operations
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String  # import specific conponents from SQLAlchemy
from sqlalchemy.orm import sessionmaker  ## import sessionmaker for managing database session
import configparser  # Import configparser to read configuration files


# Read the database configuration from the config.ini file
config = configparser.ConfigParser()  # Create a ConfigParser object
config.read('config.ini')  # Read the configuration file

# Database configuration
db_config = config['postgres']  # Access the 'postgres' section of the configuration file
# Construct the database URL using the configuration parameters
db_url = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"

# Create the SQLAlchemy engine
engine = create_engine(db_url)  # Create a database engine using the constructed URL

# Define the categories table schem
metadata = MetaData() #create a metaData object to hold the table schema
categories_table = Table('categories', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),  # Primary key column
    Column('category_id', Integer),  # Column to store the category ID from the API
    Column('category_name', String),  # Column to store the category name from the API
    Column('description', String)  # Column to store the description from the API
)

# Create the table in the database (if it doesn't already exist)
metadata.create_all(engine)   # Use metadata to create the table in the database 
 


# Create a session for database transactions
Session = sessionmaker(bind=engine)  # Bind the engine to the sessionmaker
session = Session()  # Instantiate a session object.create_all(engine) 

# Fetch data from the API
response = requests.get('https://demodata.grapecity.com/northwind/api/v1/Categories')  # Send a GET request to the API
data = response.json()  # Parse the JSON response into a Python list

# Insert data into the database
for item in data:  # Iterate over each item in the JSON data
    category = {
        'category_id': item['categoryId'],  # Map API categoryId to category_id
        'category_name': item['categoryName'],  # Map API categoryName to category_name
        'description': item['description']  # Map API description to description
    }
    insert_stmt = categories_table.insert().values(category)  # Create an insert statement for the category
    session.execute(insert_stmt)  # Execute the insert statement
    

# Commit the transaction
session.commit()  # Commit the transaction to persist the changes in the database

# Close the session
session.close()  # Close the session to release the connection

print("Data has been successfully inserted into the database.")  # Print a success message