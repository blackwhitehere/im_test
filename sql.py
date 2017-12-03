"""
Requires to connect to mysql server through proxy:

cloud_sql_proxy -instances=<instance name>=tcp:3306 -credential_file=<path to credential file>

TODO: automate connection

E.g.
instance name = imtestsg:europe-west1:im-mysql
path = mysql-client.json
cloud_sql_proxy -instances=imtestsg:europe-west1:im-mysql=tcp:3306 -credential_file=mysql-client.json
"""

from sqlalchemy import create_engine, text
import config
__all__ = ['text', 'conn']

engine = create_engine('mysql+pymysql://{}:{}@127.0.0.1/'.format(config.mysql_user, config.mysql_password))
conn = engine.connect()

r = conn.execute("SHOW DATABASES;").fetchall()
print(r)

conn.execute("""CREATE DATABASE IF NOT EXISTS solutions
                DEFAULT CHARACTER SET utf8
                DEFAULT COLLATE utf8_general_ci;""")
conn.execute("""USE solutions;""")


if __name__ == "__main__":
    conn.execute("DROP DATABASE `solutions`;")