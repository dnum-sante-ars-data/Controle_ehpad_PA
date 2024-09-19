import sqlite3
import os
import json

def checkIfDBExists(dbname):
    if os.path.exists(dbname + '.sqlite'):
        os.remove(dbname + '.sqlite')
        print('Ancienne base de donnée écrasée')

def init_db(dbname):
    checkIfDBExists(dbname)
    conn = sqlite3.connect(dbname + '.sqlite')
    print('Création de la base de donnée {}.sqlite '.format(dbname))
    return conn

def conn_db(dbname):
    conn = sqlite3.connect(dbname + '.sqlite')
    return conn

def importSrcData(df, table_name, conn):
    df.to_sql(name=table_name, con=conn, if_exists='append', index=False)
    print('La table {} a été ajoutée à la base de donnée'.format(table_name))


def tableExists(conn, table_name):
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
    return cursor.fetchone() is not None
