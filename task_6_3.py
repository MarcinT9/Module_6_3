import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, MetaData, create_engine
import csv


engine = create_engine('sqlite:///stationbase.db', echo=True)

meta = MetaData()

measures = Table(
    'measures', meta,
    Column('station', String),
    Column('date', String),
    Column('precip', String),
    Column('tobs', Integer)
)

stations = Table('stations', meta,
    Column('station', String),
    Column('latitude', String),
    Column('longitude', String),
    Column('elevation', String),
    Column('name', String),
    Column('country', String),
    Column('state', String)
)
  

def delete_all(conn, obj):
    s = obj.delete()
    result = conn.execute(s)
    print('Delete')

def delete_where(conn, obj, column, value):
    s = obj.delete().where(column == value)
    result = conn.execute(s)
    print('Delete')

def select_where(conn, obj, column, value):
    s = obj.select().where(column == value)
    result = conn.execute(s)
    for row in result:
        print(row)

def select_all(conn, obj):
    s = obj.select()
    result = conn.execute(s)
    for row in result:
        print(row)

def update(conn, obj, column, value):
    s = obj.update().where(column == value)
    result = conn.execute(s)
    print('Update')


if __name__ == '__main__':
    meta.create_all(engine)
    conn = engine.connect()
    ins = measures.insert()
    conn.execute(ins, [
        {'station': 'USC00519397', 'date': '2010-01-01', 'precip': '0.08', 'tobs': '65'},
        {'station': 'USC00519397', 'date': '2010-01-02', 'precip': '0.0', 'tobs': '63'},
        {'station': 'USC00519397', 'date': '2010-01-02', 'precip': '0.0', 'tobs': '64'},
        {'station': 'USC00519397', 'date': '2010-01-02', 'precip': '0.0', 'tobs': '69'}
    ])

    ins2 = stations.insert()
    conn.execute(ins2, [
        {'station': 'USC00519397', 'latitude': '21.2716', 'longitude': '-157.8168', 'elevation': '3.0', 'name': 'WAIKIKI 717.2', 'country': 'US','state': 'HI'},
        {'station': 'USC00513117', 'latitude': '21.4234', 'longitude': '-157.8015', 'elevation': '14.6', 'name': 'KANEOHE 838.1', 'country': 'US','state': 'HI'},
        {'station': 'USC00514830', 'latitude': '21.5213', 'longitude': '-157.8374', 'elevation': '7.0', 'name': 'KUALOA RANCH HEADQUARTERS 886.9', 'country': 'US','state': 'HI'},
        {'station': 'USC00517948', 'latitude': '21.3934', 'longitude': '-157.9751', 'elevation': '11.9', 'name': 'PEARL CITY', 'country': 'US','state': 'HI'},
        {'station': 'USC00518838', 'latitude': '21.4992', 'longitude': '-158.0111', 'elevation': '306.6', 'name': 'UPPER WAHIAWA 874.3', 'country': 'US','state': 'HI'}
    ])


    print('1---------------------------')
    select_all(conn, stations)
    print('2---------------------------')
    print(conn.execute("SELECT * FROM stations LIMIT 5").fetchall())


    delete_all(conn, measures)
    delete_all(conn, stations)

    conn.close()