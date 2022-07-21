# data base helper script 

# imports
import sqlite3
from sqlite3 import Error

def create_connection(path: str) -> sqlite3.Connection:
    '''
    A function that creates a connection

    PARAMETERS
    ----------
    path: string
        The location to the data base

    RETURNS
    -------
    connection: connection object
        A connection to the data base
    '''

    connection = None
    try:
        connection = sqlite3.connect(path)
        print("Connection to SQLite DB successful")
    except Error as e:
        print("An error occurred")
        print(f"error: {e}")

    return connection


def get_cursor(connection):
    try:
        cursor = connection.cursor()
        print("cursor creation successful")
    except Error as e:
        print("An error occurred")
        print(f"error: {e}")
    
    return cursor


def create_table(cursor, script_path):
    try:
        sql_file = open(script_path)
        sql_as_string = sql_file.read()
        cursor.executescript(sql_as_string)
        sql_file.close()
        print("table successfully created")
    except Error as e:
        print("An error occurred")
        print(f"error: {e}")

        
def df_to_sql(df, connection, table, if_exists):
    try:
        df.to_sql(name=table, con=connection, if_exists=if_exists, index = False)
    except Error as e:
        print("An error occurred")
        print(f"error: {e}")


def delete_table(cursor, table):
    try:
        cursor.execute(f"DROP TABLE {table}")
        print("Table dropped... ")
    except Error as e:
        print("An error occurred")
        print(f"error: {e}")


def close_connection(connection):
    try:
        connection.commit()
        connection.close()
        print("successfully closed")
    except Error as e:
        print("An error occurred")
        print(f'error: {e}')


def load_content(cursor, table, limit):
    try:
        cursor.execute(f'''  
        SELECT * FROM {table} LIMIT {limit}
                ''')
        for row in cursor.fetchall():
            print (row)

    except Error as e:
        print("An error occurred")
        print(f'error: {e}')
