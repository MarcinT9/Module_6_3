import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, MetaData
from sqlalchemy import create_engine


engine = create_engine('sqlite:///stationbase.db', echo=True)

meta = MetaData()

measures = Table(
    'measures', meta,
    Column('station', String),
    Column('date', String),
    Column('precip', String),
    Column('tobs', Integer)
)

stations = Table(
    'stations', meta,
    Column('station', String),
    Column('latitude', String),
    Column('longitude', String),
    Column('elevation', String),
    Column('name', String),
    Column('country', String),
    Column('state', String)
)

meta.create_all(engine)

ins = measures.insert()
conn = engine.connect()
result = conn.execute(ins)
ins.compile().params
conn.execute(ins, [
    {'station': 'USC00519397', 'date': '2010-01-01', 'precip': '0.08', 'tobs': '65'},
    {'station': 'USC00519397', 'date': '2010-01-02', 'precip': '0.0', 'tobs': '63'},
    {'station': 'USC00519397', 'date': '2010-01-03', 'precip': '0.0', 'tobs': '74'},
    {'station': 'USC00519397', 'date': '2010-01-04', 'precip': '0.0', 'tobs': '76'},
    {'station': 'USC00519397', 'date': '2010-01-06', 'precip': '0.0', 'tobs': '73'}
])

ins2 = stations.insert()
result2 = conn.execute(ins2)
ins2.compile().params
conn.execute(ins2, [
    {'station': 'USC00519397', 'latitude': '21.2716', 'longitude': '-157.8168', 'elevation': '3.0', 'name': 'WAIKIKI 717.2', 'country': 'US','state': 'HI'},
    {'station': 'USC00513117', 'latitude': '21.4234', 'longitude': '-157.8015', 'elevation': '14.6', 'name': 'KANEOHE 838.1', 'country': 'US','state': 'HI'},
    {'station': 'USC00514830', 'latitude': '21.5213', 'longitude': '-157.8374', 'elevation': '7.0', 'name': 'KUALOA RANCH HEADQUARTERS 886.9', 'country': 'US','state': 'HI'},
    {'station': 'USC00517948', 'latitude': '21.3934', 'longitude': '-157.9751', 'elevation': '11.9', 'name': 'PEARL CITY', 'country': 'US','state': 'HI'},
    {'station': 'USC00518838', 'latitude': '21.4992', 'longitude': '-158.0111', 'elevation': '306.6', 'name': 'UPPER WAHIAWA 874.3', 'country': 'US','state': 'HI'}
])

result = conn.execute(measures.select().where(measures.c.tobs > 70))
for row in result:
    print(row)


