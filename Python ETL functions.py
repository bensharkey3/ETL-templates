import datetime
import os
import logging
import psycopg2
import cx_Oracle
import pandas as pd
import requests
import json
import __main__
from awssecrets.awssecrets import get_secret


def logger_func(loadtype='full'):
    '''Create and configure logger
    Args:
        loadtype (str): specify the loadtype
    Returns:
        logger (obj): logging object 
    '''
    dt = datetime.datetime.now().strftime('%Y%m%d')
    logFile = os.path.join(os.path.abspath("./Logs"), "{} - {}".format(os.path.basename(__main__.__file__), loadtype+"load") + "_" + dt + ".log")
    logging.basicConfig(
        filename=logFile,
        filemode='a',
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(relativeCreated)d %(message)s')
    logger = logging.getLogger()
    return logger


def read_sql_text(filepath):
    '''Opens and reads SQL from a text file
    Args:
        filename (str): the location and filename of the SQL query. eg. r'C:\DataWarehouse\tables\SQL scripts\signed_date.sql'
    Returns:
        the SQL query as a string (str) 
    '''
    with open(filepath, 'r') as f:
        return f.read()


def connect_db():
    '''Connect to a PostgreSQL db
    Args: 
        none
    Returns:
        dbConn (obj): connection object
        dbCur (obj): cursor object 
    '''
    dbConn = psycopg2.connect("dbname='database' user='username' password={} host={}".format(get_secret('data_postgres', 'password'), get_secret('data_postgres', 'host')))
    dbCur = dbConn.cursor()
    dbConn.set_session(autocommit=True)
    return dbConn, dbCur


def close_connect_db():
    '''Close connection and cursor objects to the PostgreSQL db, after connect_db() is run'''
    connect_db()[0].close()
    connect_db()[1].close()


def connect_replica():
    '''Connect to the Oracle Replica database
    Args: 
        none
    Returns:
        replConn (obj): connection object 
        replCur (obj): cursor object 
    '''
    dsn_tns = cx_Oracle.makedsn(get_secret('oracle_clone','host'), 1521, service_name='pxa')
    replConn = cx_Oracle.connect(get_secret('oracle_clone','username'), get_secret('oracle_clone','password'), dsn_tns)
    replCur = replConn.cursor()
    return replConn, replCur


def close_connect_replica():
    '''Close connection and cursor objects to the Oracle Replica database, after connect_replica() is run'''
    connect_replica()[0].close()
    connect_replica()[1].close()


def drop_table(cur, table):
    '''Drops an existing table if exists
    Args:
        cur: cursor object (obj)
        table: table name (str)
    Returns:
        none
    '''
    cur.execute("""
        DROP TABLE IF EXISTS {};
        """.format(table))


def delete_table(cur, table):
    '''Deletes contents of table
    Args:
        cur (obj): cursor object
        table (str): table name
    Returns:
        none
    '''
    cur.execute("""
        DELETE FROM {};
        """.format(table))


def send_slack_message(url, message):
    '''Sends a slack webhook message for a given url
    Args:
        url (str): the webhook url
        message (str): the message to be sent
    Returns:
        none
    '''
    slack_data = {'text': message}
    requests.post(url, data=json.dumps(slack_data), headers={'Content-Type': 'application/json'})


def get_daterange_db_col(cur, table, column):
    '''Returns 3 days before most recent date in column of db table, and the current date.
    Args:
        cur (obj): the cursor object of the database 
        table (str): the table in the database 
        column (str): a date column in the table 
    Returns:
        datefrom (str): 3 days before the most recent date in column, in format '25/DEC/19'
        datetime (str): current date in format '25/DEC/19'
    '''
    cur.execute("""
        select to_char((max({column}) - interval '3 day')::DATE, 'DD/MON/YY') from {table}
        """.format(
            column=column,
            table=table))
    datefrom = cur.fetchall()
    datefrom = datefrom[0][0]
    print('datefrom: '+ datefrom)
    dateto = datetime.datetime.now().strftime('%d/%b/%y')
    print('dateto: '+ dateto)
    return datefrom, dateto


def execute_query_full(cur, conn, filepath, outputtype):  
    '''executes query from a filepath
    Args:
        cur (obj): the database cursor object
        conn (obj): the database connection object
        filepath (str): the filepath of the SQL file
        outputtype (str): either 'pandas' for a df or 'tuples' for a list of tuples
    Returns:
        output (tup/df): either a tuple of tuples, or a pandas df, depending on the outputtype entered
    '''
    if outputtype == 'tuples':
        cur.execute(read_sql_text(filepath))
        output = cur.fetchall()
    elif outputtype == 'pandas':
        output = pd.read_sql_query(read_sql_text(filepath), conn)
    else:
        print('invalid output type entered')
    return output


def execute_query_wdates(cur, conn, filepath, outputtype, datefrom, dateto):  
    '''executes a query from a filepath that includes dynamic dates as inputs to the SQL string
    Args:
        cur (obj): database cursor object
        conn (obj): database connection object
        filepath (str): filepath of the SQL file
        outputtype (str): either 'pandas' for a df or 'tuples' for a list of tuples
        datefrom (str): start of the daterange in format '25/DEC/19'
        dateto (str): end of the daterange in format '25/DEC/19'
    Returns:
        out (tup/df): either a tuple of tuples, or a pandas df, depending on the outputtype entered
    '''
    if outputtype == 'tuples':
        cur.execute(read_sql_text(filepath).format(datefrom=datefrom, dateto=dateto))
        out = cur.fetchall()
    elif outputtype == 'pandas':
        out = pd.read_sql_query(read_sql_text(filepath).format(datefrom=datefrom, dateto=dateto), conn)
    else:
        print('invalid output type entered')
    return out


def write_pd_to_pg(df, cur, table):
    '''writing to a database from pandas df
    Args:
        df (dataframe): the pandas dataframe to be written
        cur (obj): database cursor object
        table (str): table name of existing table in database
    Returns:
        none
        '''
    tuples = list(df.itertuples(index=False, name=None))
    for i in range(0, len(tuples), 10000):
        args_str = ','.join(cur.mogrify("("+("%s, "*len(tuples[0]))[:-2]+")", x).decode("utf-8") for x in tuples[i:i+10000])
        cur.execute("INSERT INTO {table} VALUES {args_str}".format(table=table, args_str=args_str))
