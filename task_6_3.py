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
    Column('station', Integer),
    Column('latitude', String),
    Column('longitude', String),
    Column('elevation', String),
    Column('name', String),
    Column('country', String),
    Column('state', String)
)

ins = measures.insert()
#ins = measures.insert().values(station='USC00519397', date='2010-01-01', precip='0.08', tobs=65)
print(ins.compile().params)

conn = engine.connect()
result = conn.execute(ins)
conn.execute(ins, [
    {'station': 'USC00519397', 'date': '2010-01-02', 'precip': '0.0', 'tobs': '63'}
])


meta.create_all(engine)
print(engine.table_names())
print(conn.execute("SELECT * FROM stations LIMIT 5").fetchall())
