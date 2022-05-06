from getpass import getpass
from mysql.connector import connect, Error
import json
import logging

def delete_record():
    """Sample code for deleting record"""
    select_movie_query = """
    SELECT reviewer_id, movie_id FROM ratings
    WHERE reviewer_id = 2
    """
    with connection.cursor() as cursor:
        cursor.execute(select_movie_query)
        for movie in cursor.fetchall():
            print(movie)
        print('\n')

    delete_query = """
    DELETE FROM ratings
    WHERE reviewer_id = 2
    """
    with connection.cursor() as cursor:
        cursor.execute(delete_query)
        connection.commit()


def update_multiple_elements():
    """Sample code updating multiple records in table"""
    movie_id = input("Enter movie id: ")
    reviewer_id = input("Enter reviewer id: ")
    new_rating = input("Enter new rating: ")
    update_query = """
    UPDATE
        ratings
    SET
        rating = %s
    WHERE
        movie_id = %s AND reviewer_id = %s;
    
    SELECT *
    FROM ratings
    WHERE 
        movie_id =%s AND reviewer_id = %s
    """
    val_tuple = (
        new_rating,
        movie_id,
        reviewer_id,
        movie_id,
        reviewer_id
    )
    with connection.cursor() as cursor:
        for result in cursor.execute(update_query, val_tuple, multi=True):
            if result.with_rows:
                print(result.fetchall())
        connection.commit()


def update_one_element():
    """Sample code that updates one element in one record of one table"""
    update_movie_query = """
    UPDATE
        reviewers
    SET
        last_name = "Cooper"
    Where
        first_name = "Amy"
    """
    with connection.cursor() as cursor:
        cursor.execute(update_movie_query)
        connection.commit()
    logging.info("Succesfully updatet one element")


def find_reviewer():
    """Sample code that winds in ouer database the reviewer who made the most reviews"""
    select_movie_query = """
    SELECT CONCAT(first_name, " ", last_name), COUNT(*) as num 
    FROM reviewers
    INNER JOIN ratings
        ON reviewers.id =  ratings.reviewer_id
    GROUP BY reviewer_id
    ORDER BY num DESC
    LIMIT 2   
    """
    with connection.cursor() as cursor:
        cursor.execute(select_movie_query)
        for reviewer in cursor.fetchall():
            print(reviewer)
        print('\n')


def joining_usage():
    """Sample code for using Join to search 5 best rated movies"""
    select_movie_query = """
    SELECT title, AVG(rating) as average_rating
    FROM ratings
    INNER JOIN movies
        ON movies.id = ratings.movie_id
    GROUP BY movie_id
    ORDER BY average_rating DESC
    LIMIT 5
    """
    with connection.cursor() as cursor:
        cursor.execute(select_movie_query)
        for movies in cursor.fetchall():
            print(movies)
        print("\n")


def fetchmany_usage():
    """Sample code for using fetchmany """
    select_movies_query = """
    SELECT CONCAT(title, " (", release_year, ")"),
          collection_in_mil
    FROM movies
    ORDER BY collection_in_mil DESC
    """
    with connection.cursor() as cursor:
        cursor.execute(select_movies_query)
        for movie in cursor.fetchmany(size=5):
            print(movie)
        cursor.fetchall()
        print("\n")


def filtering_records_with_formating():
    """Sample code for filtering record with string formating """
    select_movies_query = """
    SELECT CONCAT(title, "(", release_year, ")"), collection_in_mil    
    FROM movies
    ORDER BY collection_in_mil DESC
    LIMIT 5
    """
    with connection.cursor() as cursor:
        cursor.execute(select_movies_query)
        for movie in cursor.fetchall():
            print(movie)
        print("\n")


def filtering_records():
    """Sample code for filtering records in MySQL using WHERE"""
    select_movies_query = """
    SELECT title, collection_in_mil
    FROM movies
    WHERE collection_in_mil > 300
    ORDER BY collection_in_mil DESC
    """
    with connection.cursor() as cursor:
        cursor.execute(select_movies_query)
        for movie in cursor.fetchall():
            print(movie)
        print("\n")


def reading_records_by_columns():
    """Sample code for displaying exact columns from database table"""
    select_table_query = """SELECT title, release_year FROM movies LIMIT 5"""
    with connection.cursor() as cursor:
        cursor.execute(select_table_query)
        for row in cursor.fetchall():
            print(row)
        print("\n")


def reading_records(table_name, amount):
    """Reads the chosen table with chosen amount of records
    if we want to read certain rows we should replace * element and pass column names"""
    select_table_query = f"""SELECT * FROM {table_name} LIMIT {amount}"""
    with connection.cursor() as cursor:
        cursor.execute(select_table_query)
        for row in cursor.fetchall():
            print(row)
        print("\n")


def insert_single_execute():
    """inserting data to movies table using .execute method, This method is used to insert small amount of data"""
    insert_movies_query = """
    INSERT INTO movies (title, release_year, genre, collection_in_mil)
    VALUES
        ("Forrest Gump", 1994, "Drama", 330.2),
        ("3 Idiots", 2009, "Drama", 2.4),
        ("Eternal Sunshine of the Spotless Mind", 2004, "Drama", 34.5),
        ("Good Will Hunting", 1997, "Drama", 138.1),
        ("Skyfall", 2012, "Action", 304.6),
        ("Gladiator", 2000, "Action", 188.7),
        ("Black", 2005, "Drama", 3.0),
        ("Titanic", 1997, "Romance", 659.2),
        ("The Shawshank Redemption", 1994, "Drama",28.4),
        ("Udaan", 2010, "Drama", 1.5),
        ("Home Alone", 1990, "Comedy", 286.9),
        ("Casablanca", 1942, "Romance", 1.0),
        ("Avengers: Endgame", 2019, "Action", 858.8),
        ("Night of the Living Dead", 1968, "Horror", 2.5),
        ("The Godfather", 1972, "Crime", 135.6),
        ("Haider", 2014, "Action", 4.2),
        ("Inception", 2010, "Adventure", 293.7),
        ("Evil", 2003, "Horror", 1.3),
        ("Toy Story 4", 2019, "Animation", 434.9),
        ("Air Force One", 1997, "Drama", 138.1),
        ("The Dark Knight", 2008, "Action",535.4),
        ("Bhaag Milkha Bhaag", 2013, "Sport", 4.1),
        ("The Lion King", 1994, "Animation", 423.6),
        ("Pulp Fiction", 1994, "Crime", 108.8),
        ("Kai Po Che", 2013, "Sport", 6.0),
        ("Beasts of No Nation", 2015, "War", 1.4),
        ("Andadhun", 2018, "Thriller", 2.9),
        ("The Silence of the Lambs", 1991, "Crime", 68.2),
        ("Deadpool", 2016, "Action", 363.6),
        ("Drishyam", 2015, "Mystery", 3.0)
    """
    with connection.cursor() as cursor:
        cursor.execute(insert_movies_query)
        connection.commit()


def insert_many_executemany():
    """inserting data to reviewers table at once using .executemany method it is good for inserting data form
    file or another script"""
    insert_reviewers_query = """
    INSERT INTO reviewers
    (first_name, last_name)
    VALUES ( %s, %s )
    """
    reviewers_records = [
        ("Chaitanya", "Baweja"),
        ("Mary", "Cooper"),
        ("John", "Wayne"),
        ("Thomas", "Stoneman"),
        ("Penny", "Hofstadter"),
        ("Mitchell", "Marsh"),
        ("Wyatt", "Skaggs"),
        ("Andre", "Veiga"),
        ("Sheldon", "Cooper"),
        ("Kimbra", "Masters"),
        ("Kat", "Dennings"),
        ("Bruce", "Wayne"),
        ("Domingo", "Cortes"),
        ("Rajesh", "Koothrappali"),
        ("Ben", "Glocker"),
        ("Mahinder", "Dhoni"),
        ("Akbar", "Khan"),
        ("Howard", "Wolowitz"),
        ("Pinkie", "Petit"),
        ("Gurkaran", "Singh"),
        ("Amy", "Farah Fowler"),
        ("Marlon", "Crafford"),
    ]
    with connection.cursor() as cursor:
        cursor.executemany(insert_reviewers_query, reviewers_records)
        connection.commit()

    insert_ratings_query = """
    INSERT INTO ratings
    (rating, movie_id, reviewer_id)
    VALUES ( %s, %s, %s)
    """
    ratings_records = [
        (6.4, 17, 5), (5.6, 19, 1), (6.3, 22, 14), (5.1, 21, 17),
        (5.0, 5, 5), (6.5, 21, 5), (8.5, 30, 13), (9.7, 6, 4),
        (8.5, 24, 12), (9.9, 14, 9), (8.7, 26, 14), (9.9, 6, 10),
        (5.1, 30, 6), (5.4, 18, 16), (6.2, 6, 20), (7.3, 21, 19),
        (8.1, 17, 18), (5.0, 7, 2), (9.8, 23, 3), (8.0, 22, 9),
        (8.5, 11, 13), (5.0, 5, 11), (5.7, 8, 2), (7.6, 25, 19),
        (5.2, 18, 15), (9.7, 13, 3), (5.8, 18, 8), (5.8, 30, 15),
        (8.4, 21, 18), (6.2, 23, 16), (7.0, 10, 18), (9.5, 30, 20),
        (8.9, 3, 19), (6.4, 12, 2), (7.8, 12, 22), (9.9, 15, 13),
        (7.5, 20, 17), (9.0, 25, 6), (8.5, 23, 2), (5.3, 30, 17),
        (6.4, 5, 10), (8.1, 5, 21), (5.7, 22, 1), (6.3, 28, 4),
        (9.8, 13, 1)
    ]
    with connection.cursor() as cursor:
        cursor.executemany(insert_ratings_query, ratings_records)
        connection.commit()


def delete_table(name):
    """Deletes table of passed name"""
    drop_table_query = f"DROP TABLE {name}"
    try:
        with connection.cursor() as cursor:
            cursor.execute(drop_table_query)
    except Error as e:
        logging.error(f"During operation error occured: \n {e}")


def modify_table():
    """Modifying one column """
    alter_table_query = """
    ALTER TABLE movies
    MODIFY COLUMN collection_in_mil DECIMAL(4,1)
    """
    table_query = "DESCRIBE movies"
    with connection.cursor() as cursor:
        cursor.execute(alter_table_query)
        cursor.execute(table_query)
        result = cursor.fetchall()

        for row in result:
            print(row)
        print("\n")


def describe_tables():
    """Describe all created tables"""
    show_movie_table_query = "DESCRIBE movies"
    show_reviewers_table_query = "DESCRIBE reviewers"
    show_ratings_table_query = "DESCRIBE ratings"
    with connection.cursor() as cursor:
        cursor.execute(show_movie_table_query)
        movie_result = cursor.fetchall()
        for row in movie_result:
            print(row)
        print("\n next one \n")

        cursor.execute(show_reviewers_table_query)
        reviewers_result = cursor.fetchall()
        for row in reviewers_result:
            print(row)

        print("\n next one \n")
        cursor.execute(show_ratings_table_query)
        ratings_result = cursor.fetchall()
        for row in ratings_result:
            print(row)
        print("\n end of the checking \n")


def set_data():
    """Creating all tables, if tables are created gives logs"""
    try:
        create_movies_table_query = """
        CREATE TABLE movies(
            id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(100),
            release_year YEAR(4),
            genre VARCHAR(100),
            collection_in_mil INT
        )
        """
        with connection.cursor() as cursor:
            cursor.execute(create_movies_table_query)
            connection.commit()

            logging.info("Successfully created a movies table in database")
    except Error as e:
        logging.info(f"Following error appeared \n {e}")

    try:
        create_reviewers_table_query = """
        CREATE TABLE reviewers (
        id INT AUTO_INCREMENT PRIMARY KEY,
        first_name VARCHAR(100),
        last_name VARCHAR(100)
        )
        """
        with connection.cursor() as cursor:
            cursor.execute(create_reviewers_table_query)
            connection.commit()
            logging.info("Successfully created reviewers table in database")
    except Error as e:
        logging.info(f"Following error appeared \n {e}")

    try:
        create_ratings_table_query = """
        CREATE TABLE ratings (
            movie_id INT,
            reviewer_id INT,
            rating DECIMAL(2,1),
            FOREIGN KEY(movie_id) REFERENCES movies(id),
            FOREIGN KEY(reviewer_id) REFERENCES reviewers(id),
            PRIMARY KEY(movie_id, reviewer_id)
        )
        """
        with connection.cursor() as cursor:
            cursor.execute(create_ratings_table_query)
            connection.commit()
            logging.info("Successfully created ratings table in database")

    except Error as e:
        logging.info(f"Following error appeared \n {e}")


logging.basicConfig(level=logging.INFO)

with open("starting.json") as file:
  starting = json.load(file)

try:
    with connect(
        host="localhost",
        user=starting["username"],
        password=starting["password"],
        database="online_movie_rating",
    ) as connection:
        logging.info(f"Succesfully connected to {connection}")
        selected = input("if you want to set data type 'y': ")

        if selected.lower() == 'y':
            set_data()
        describe_selection = input("If you wanna to show description of tables type 'y': ")

        if describe_selection.lower() == 'y':
            describe_tables()
        modify_selection = input("If you wanna to modify movies table type 'y': ")

        if modify_selection.lower() == 'y':
            modify_table()

        remove_selection = input("If you wanna to remove any table from database type 'y': ")
        if remove_selection.lower() == 'y':
            remove_choice = input("Plis type name of table you want to remove: ")
            delete_table(name=remove_choice.lower())

        insertion_selection = input("If you want to insert base data into the tables type 'y': ")
        if insertion_selection.lower() == 'y':
            try:
                insert_single_execute()
            except Error as e:
                logging.error(e)
            try:
                insert_many_executemany()
            except Error as e:
                logging.error(e)
        reading_selection = input("If you want to read data from database tables type 'y': ")
        if reading_selection.lower() == 'y':
            tablename = input(" What table would you like to read ratings/movies/reviews: ").lower()
            ammount = input("what amount of records you want to see: ").lower()
            try:
                reading_records(table_name=tablename, amount=ammount)
                reading_records_by_columns()
            except Error as e:
                logging.error(e)
        filtering_selection = input("If you want to filter data from datase type 'y': ")
        if filtering_selection.lower() == 'y':
            try:
                filtering_records()
                filtering_records_with_formating()
                fetchmany_usage()
                joining_usage()
                find_reviewer()
            except Error as e:
                logging.error(e)
        updating_selection = input("If you want to update data from database type 'y': ")
        if updating_selection.lower() == 'y':
            try:
                update_one_element()
                update_multiple_elements()
            except Error as e:
                print(e)
        deleting_selection = input("If you want to delete data from database type 'y': ")
        if deleting_selection.lower() == 'y':
            try:
                delete_record()
            except Error as e:
                logging.error(e)
except Error as e:
    logging.error(f"error during connecting to database occured \n {e}")


