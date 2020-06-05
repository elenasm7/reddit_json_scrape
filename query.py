import os
from datetime import datetime
import pymysql
import pandas as pd
from random import randint
from time import sleep
from sqlalchemy import create_engine, exc
from sqlalchemy.types import VARCHAR, INTEGER, BIGINT, DateTime

import database


def write_stream_data(engine, db_columns, db_name, create_column, create_primary_key, create_index, stream_data):
    """ Write Twitch stream data to a final table. """
    with database.dbconn(engine) as connection:       
        query_a = (f"CREATE TABLE IF NOT EXISTS `{db_name}` ({create_column}, "
        f"PRIMARY KEY ({create_primary_key}), "
        f"INDEX id_1_multicol ({create_index})) "
        f"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;")
        connection.execute(query_a)
        
        for i,row in stream_data.iterrows(): 
            query_b = f"INSERT IGNORE INTO {db_name} ({db_columns}) VALUES(" + "%s,"*(len(row)-1) + "%s);"
            connection.execute(query_b, tuple(row))
    return