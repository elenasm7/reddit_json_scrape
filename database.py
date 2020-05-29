'''
Usage instructions for the following errors:
    ModuleNotFoundError: No module named 'MySQLdb'
    OSError: mysql_config not found

Solution:
    MacOS:
        Before installing mysqlclient:
            xcode-select --install
        Python 3:
            pip3 install mysqlclient
        Python 2:
            pip install mysql-python

    EC2/LINUX(REDHAT/CENTOS):
    sudo yum install MySQL-python python3-devel mysql-devel gcc python-devel -y
    Python 3:
        pip3 install mysqlclient
    Python 2:
        pip install mysql-python

    WINDOWS:
            pip install mysqlclient
            OR
            pip install mysqlclient==1.3.4

'''


from contextlib import contextmanager
import logging
import os

from sqlalchemy import create_engine


logger = logging.getLogger(__name__)


def setup_database(username, password, dbhost, dbport, dbname):
    database_uri = 'mysql://{}:{}@{}:{}/{}?charset=utf8mb4'.format(
        username, password, dbhost, dbport, dbname)
    options = {
        'pool_size': 10,
        'pool_recycle': 300,
        'pool_pre_ping': True
    }
    engine = create_engine(database_uri, **options)
    logger.debug("Created engine in process %s, parent %s",
                 os.getpid(), os.getppid())
    return engine


@contextmanager
def dbconn(engine):
    connection = engine.connect()
    try:
        yield connection
    finally:
        connection.close()