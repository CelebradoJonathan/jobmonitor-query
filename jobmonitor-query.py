import requests
import concurrent.futures
import threading
import time
import sqlite3
import configparser
import os
from sqlite3 import Error
import logging.config
import yaml
import datetime

thread_local = threading.local()
config = configparser.ConfigParser()
conf_dir = os.path.join(os.path.dirname(__file__), 'conf.ini')
config.read(conf_dir)
URL = config['args']['URL']
thread_count = config['args']['threadcount']


def setup_logging(
        default_path='logging.yaml',
        default_level=logging.INFO,
        env_key='LOG_CFG'
):
    """Setup logging configuration from a yaml file.
    """
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)

    else:
        logging.basicConfig(level=default_level)


def sql_connection():
    """Create database"""
    try:
        con = sqlite3.connect('jobdatabase.db')
        return con
    except Error:
        logger.error(Error, exc_info=True)


def sql_table():
    """Create table"""
    con = sql_connection()
    cursor_obj = con.cursor()
    # cursor_obj.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='jobs';")

    cursor_obj.execute(
            "CREATE TABLE IF NOT EXISTS jobs(PKid integer PRIMARY KEY,"
            " id VARCHAR,"
            " JobID VARCHAR,"
            " AppName VARCHAR,"
            " created_at VARCHAR,"
            " state VARCHAR);")
    con.commit()


def sql_insert(con, entities):
    """
    :param con: database connection
    :param entities: list of dictionaries containing the metadata
    """
    try:
        cursorObj = con.cursor()
        insert_query = "INSERT INTO jobs(id, JobID, AppName, created_at, state)" \
                       " VALUES(:id, :job_id, :app_name, :created_at, :state)"

        cursorObj.executemany(insert_query, entities)
        con.commit()
    except Error:
        logger.error(Error, exc_info=True)


def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session


def get_metadata(jobid):
    """
    produces the list of dictionaries needed for inserting metadata record to the database.
    :param jobid: the list of jobid from a file
    """
    session = get_session()
    with session.get(URL + jobid) as response:
        outer = response.json()
        for item in outer:
            for inner in item:
                job_list = inner['jobs']
                con = sql_connection()
                sql_insert(con, job_list)


def get_all_jobs(jobids):
    with concurrent.futures.ThreadPoolExecutor(max_workers=int(thread_count)) as executor:
        executor.map(get_metadata, jobids)


def get_jobid_fromfile(some_file):
    with open(some_file) as f:
        content = f.read()
        return content.splitlines()


def main():
    jobids = get_jobid_fromfile("jobid.txt")
    sql_table()
    start_time = time.time()
    logger.info(f"Started at {datetime.datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')}")
    get_all_jobs(jobids)
    duration = time.time() - start_time
    logger.info(f"Ended at {datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Inserted {len(jobids)} Job ID/s in {duration} seconds")


if __name__ == "__main__":
    setup_logging()
    logger = logging.getLogger(__name__)
    main()
