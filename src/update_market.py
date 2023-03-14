import os
import logging
import datetime
import subprocess

import mysql.connector
import pandas as pd
import pandas_gbq
from google.cloud import bigquery


# Set bq environ
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/vogler/keys/impvoltracker-b6e7e48ae142.json'
ROOT = '/home/vogler/projects/market_data'
DB_DIR = f'{ROOT}/db'
OPT_DB_DIR = f'{DB_DIR}/options'
STO_DB_DIR = f'{DB_DIR}/stocks'
PROJECT_ID = 'impvoltracker'



logging.basicConfig(filename=f'{ROOT}/logs/update_market.log',
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M',
                    level=logging.INFO)
logger = logging.getLogger(name=__name__)


logger.info('========================== Start of Execution ===========================')

def get_new_data(cnx, bq_table_id, local_table_name, local_database, dflast_mod):
    # USE OPTION DATABASE
    sel_base_q = f'use {local_database};'
    cursor.execute(sel_base_q)
    # fetch last written date from option chain on BQ
    last_mod = dflast_mod[dflast_mod['table_id']==bq_table_id].iloc[0, 1]
    lower_date = (last_mod - datetime.timedelta(days=15)).strftime('%Y-%m-%d')
    upper_date = (datetime.datetime.today() + datetime.timedelta(days=7)).strftime('%Y-%m-%d')

    date_query = f'SELECT distinct date FROM `impvoltracker.market_data.{bq_table_id}` WHERE DATE(date) > "{lower_date}" and DATE(date) < "{upper_date}" order by date;'

    try:
        dfdate_mod = pandas_gbq.read_gbq(date_query, project_id=PROJECT_ID)
    except Exception as err:
        logger.critical(f'Could not fetch unique dates from table {bq_table_id} from BQ due to exception \n {err}')
        exit()

    last_date = dfdate_mod.max()[0]
    last_date_str = last_date.strftime('%Y-%m-%d')

    # read updates from table 
    query = f'select * from {local_table_name} where date > "{last_date_str}"'
    return pd.read_sql(query, cnx)


# Set Up Connections
# BQ
try:
    client = bigquery.Client()
except Exception as err:
    logger.critical(f'Could not connect to BQ with exception \n {err}')
    exit()
# Local DB
try:
    cnx = mysql.connector.connect(host='localhost', user='root')
except Exception as err:
    logger.critical(f'Could not connect to DOLT database with exception \n {err}')
    exit()
cursor = cnx.cursor()

# get latest BQ Updates
latest_updates_query = 'SELECT table_id, timestamp_millis(last_modified_time) as last_modified from impvoltracker.market_data.__TABLES__'
try:
    dflast_mod = pandas_gbq.read_gbq(latest_updates_query, project_id=PROJECT_ID)
except Exception as err:
    logger.critical(f'Could not fetch last_modified from BQ due to exception \n {err}')
    exit()
dflast_mod['last_modified'] = dflast_mod['last_modified'].dt.tz_localize(None)


# fetch last written date from option chain on BQ
bq_table_id = 'option_chain'
local_db_name = 'options'
local_table_name = 'option_chain'

dfoptions = get_new_data(cnx=cnx, bq_table_id=bq_table_id, local_table_name=local_table_name, local_database=local_db_name, dflast_mod=dflast_mod) 
added_rows = dfoptions.shape[0]

dfoptions['date'] = pd.to_datetime(dfoptions['date'], format='%Y-%m-%d')
dfoptions['expiration'] = pd.to_datetime(dfoptions['expiration'], format='%Y-%m-%d')
# If there are more than zero rows added since last update, push to gbq
if added_rows > 0:
    try:
            pandas_gbq.to_gbq(dfoptions, f'market_data.{bq_table_id}', project_id='impvoltracker', if_exists='append')
            logger.info(f'Sucessfully pushed {added_rows} rows to TABLE {bq_table_id} on BQ')
    except Exception as err:
        logger.critical(f'There was an error in pushing {added_rows} rows to TABLE {bq_table_id} with error: \n {err}')
else:
    logger.info(f'No data has been written to TABLE {bq_table_id} since no new data is available')

# fetch last written date from option chain on BQ
bq_table_id = 'volatility_history'
local_db_name = 'options'
local_table_name = 'volatility_history'
dfvol = get_new_data(cnx=cnx, bq_table_id=bq_table_id, local_table_name=local_table_name, local_database=local_db_name, dflast_mod=dflast_mod)
added_rows = dfvol.shape[0]

if added_rows > 0:
    dfvol['date'] = pd.to_datetime(dfvol['date'], format='%Y-%m-%d')
    dfvol['hv_year_high_date'] = pd.to_datetime(dfvol['hv_year_high_date'], format='%Y-%m-%d')
    dfvol['hv_year_low_date'] = pd.to_datetime(dfvol['hv_year_low_date'], format='%Y-%m-%d')
    dfvol['iv_year_high_date'] = pd.to_datetime(dfvol['iv_year_high_date'], format='%Y-%m-%d')
    dfvol['iv_year_low_date'] = pd.to_datetime(dfvol['iv_year_low_date'], format='%Y-%m-%d')

    try:
        pandas_gbq.to_gbq(dfvol, f'market_data.{bq_table_id}', project_id='impvoltracker', if_exists='append')
        logger.info(f'Sucessfully pushed {added_rows} rows to TABLE {bq_table_id} on BQ')
    except Exception as err:
        logger.info(f'There was an error in pushing {added_rows} rows to TABLE {bq_table_id} with error: \n {err}')
else:
    logger.info(f'No data has been written to TABLE {bq_table_id} since no new data is available')


bq_table_id = 'stock_ohlcv'
local_db_name = 'stocks'
local_table_name = 'ohlcv'
dfohlcv = get_new_data(cnx=cnx, bq_table_id=bq_table_id, local_table_name=local_table_name, local_database=local_db_name, dflast_mod=dflast_mod)
added_rows = dfohlcv.shape[0]

if added_rows > 0:
    dfohlcv['date'] = pd.to_datetime(dfvol['date'], format='%Y-%m-%d')
    try:
        pandas_gbq.to_gbq(dfohlcv, f'market_data.{bq_table_id}', project_id='impvoltracker', if_exists='append')
        logger.info(f'Sucessfully pushed {added_rows} rows to TABLE {bq_table_id} on BQ')
    except Exception as err:
        logger.info(f'There was an error in pushing {added_rows} rows to TABLE {bq_table_id} with error: \n {err}')
else:
    logger.info(f'No data has been written to TABLE {bq_table_id} since no new data is available')

cursor.close()


# TODO
# Fast forward DB


#logger.info(f'Fast forwarded database OPTIONS ff status {ff} and {conflicts} conflicts!')
cursor.close()
logger.info('========================== End of execution ===========================')

