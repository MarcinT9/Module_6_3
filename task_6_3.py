import sqlite3
from sqlite3 import Error
import csv


def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn

def execute_sql(conn, sql):
    try:
        c = conn.cursor()
        c.execute(sql)
    except Error as e:
        print(e)

def get_clean_stations():
    clean_stations = open("clean_stations.csv")
    rows = csv.reader(clean_stations)
    cur = conn.cursor()
    cur.executemany("INSERT OR IGNORE INTO stations VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
    conn.commit()
    
def get_clean_measure():
    clean_measure = open("clean_measure.csv")
    rows = csv.reader(clean_measure)
    cur = conn.cursor()
    cur.executemany("INSERT OR IGNORE INTO measure VALUES (?, ?, ?, ?)", rows)
    conn.commit()

def select_all(conn, table):
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table}")
    rows = cur.fetchall()

    return rows

def select_where(conn, table, **query):
    cur = conn.cursor()
    qs = []
    values = ()
    for k, v in query.items():
        qs.append(f"{k}=?")
        values += (v,)
    q = " AND ".join(qs)
    cur.execute(f"SELECT * FROM {table} WHERE {q}", values)
    rows = cur.fetchall()
    return rows

def update(conn, table, id, **kwargs):
    parameters = [f'{k} = ?' for k in kwargs]
    parameters = ', '.join(parameters)
    values = tuple(v for v in kwargs.values())
    values += (id, )

    sql = f''' UPDATE {table}
            SET {parameters}
            WHERE id = ?'''

    try:
        cur = conn.cursor()
        cur.execute(sql, values)
        conn.commit()
        print('OK')
    except sqlite3.OperationalError as e:
        print(e)

def delete_all(conn, table):
    sql = f'DELETE FROM {table}'
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    print('Delete')


if __name__ == "__main__":

    db_file = "stationbase.db"   
    conn = create_connection(db_file)

    create_stations_sql = """
    -- stations table
    CREATE TABLE IF NOT EXISTS stations (
        station text PRIMARY KEY,
        latitude float,
        longitude float,
        elevation float,
        name text,
        country text,
        state text
    );
    """

    create_measure_sql = """
    -- measure table
    CREATE TABLE IF NOT EXISTS measure (
        station_name text,
        date text,
        precip float,
        tobs integer,
        FOREIGN KEY (station_name) REFERENCES stations (station)
    );
    """

    if conn is not None:
        execute_sql(conn, create_stations_sql)
        execute_sql(conn, create_measure_sql)
    
        get_clean_stations()
        get_clean_measure()

        print(conn.execute('SELECT station FROM stations LIMIT 5').fetchall())
        print(conn.execute('SELECT * FROM stations').fetchall())


        delete_all(conn, 'stations')
        delete_all(conn, 'measure')