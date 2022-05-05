
from mysql.connector import connect, Error
from getpass import getpass

import logging
import json


with open("starting.json") as file:
  starting = json.load(file)


logging.basicConfig(level=logging.INFO)

try:
  with connect(
    host="localhost",
    user=starting["username"],
    password=starting["password"]
  ) as connection:
    logging.info("logged to database")

    try:
      create_db_query = "CREATE DATABASE online_movie_rating"
      with connection.cursor() as cursor:
          cursor.execute(create_db_query)
          logging.info("Database online_movie_rating created")

    except:
      show_db_query = "SHOW DATABASES"
      with connection.cursor() as cursor:
        cursor.execute(show_db_query)
        logging.info("The database that you try to create already exist below you can find "
                     "list of databases avaliable")
        for db in cursor:
          print(db)

except Error as e:
  logging.error(f"{e}")