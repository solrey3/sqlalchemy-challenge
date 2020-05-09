import numpy as np
import pandas as pd
import datetime as dt

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from dateutil.parser import parse

# import Flask
from flask import Flask, jsonify

# Setup Database
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(engine, reflect=True)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station


# Create an app, being sure to pass __name__
app = Flask(__name__)


# Define what to do when a user hits the index route
@app.route("/")
def home():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Design a query to retrieve the last 12 months of precipitation data and plot the results
    first_date = session.query(Measurement.date).order_by(Measurement.date).first()
    last_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()


    return (
        f"Welcome to the Hawaii Weather API!<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/&lt;start&gt; and /api/v1.0/&lt;start&gt;/&lt;end&gt;<br/>"
        f"Data from {first_date[0]} to {last_date[0]}"
    )


# 4. Define what to do when a user hits the /about route
@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Design a query to retrieve the last 12 months of precipitation data and plot the results
    last_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()

    # Calculate the date 1 year ago from the last data point in the database
    query_date = parse(last_date[0]) - dt.timedelta(days=365)

    # Perform a query to retrieve the data and precipitation scores
    sel = [Measurement.date,func.max(Measurement.prcp)]

    precipitations = session.query(*sel).filter(Measurement.date >= query_date).group_by(Measurement.date).all()

    # Save the query results as a Pandas DataFrame and set the index to the date column
    df = pd.DataFrame(precipitations, columns=['date', 'precipitation']).set_index("date")

    # Sort the dataframe by date
    df = df.sort_values(by=['date'])

    prcp_dict = df.to_dict('dict')

    # jsonify the dict  
    return jsonify(prcp_dict)

@app.route("/api/v1.0/stations")
def stations():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    sel = [Station.station, Station.name, Station.latitude, Station.longitude, Station.elevation]

    stations = session.query(*sel).all()

    df = pd.DataFrame(stations,\
        columns=['station','name','latitude','longitude','elevation'])\
        .set_index('station')

    df = df.sort_values(by=['station'])

    stations_dict = df.to_dict('index')
    return jsonify(stations_dict)

# Query the dates and temperature observations of the most active station for the last year of data.
# Return a JSON list of temperature observations (TOBS) for the previous year.
@app.route("/api/v1.0/tobs")
def tobs():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    sel = [Station.station, func.count(Measurement.date)]
    station_counts = session.query(*sel).\
        filter(Station.station == Measurement.station).\
        group_by(Station.station).\
        order_by(func.count(Measurement.date).\
        desc()).\
        all()
    most_active_station = station_counts[0][0]
    last_date = session.query(Measurement.date).\
        filter(Measurement.station == most_active_station).\
        order_by(Measurement.date.desc()).first()
    query_date = parse(last_date[0]) - dt.timedelta(days=365)
    sel = [Measurement.date, Measurement.tobs]
    station_temp_stats = session.query(*sel).\
        filter(Measurement.station == most_active_station).\
        filter(Measurement.date >= query_date).\
        all()
    df = pd.DataFrame(station_temp_stats,\
        columns=['date','tobs'])\
        .set_index('date')
    # Sort the dataframe by date
    df = df.sort_values(by=['date'])

    tobs_dict = df.to_dict('dict')

    # jsonify the dict  
    return jsonify(tobs_dict)

@app.route("/api/v1.0/<start>")
def start_date(start):
    # Create our session (link) from Python to the DB
    session = Session(engine)

    sel = [Station.station, func.count(Measurement.date)]
    station_counts = session.query(*sel).\
        filter(Station.station == Measurement.station).\
        group_by(Station.station).\
        order_by(func.count(Measurement.date).\
        desc()).\
        all()
    most_active_station = station_counts[0][0]
    sel = [Measurement.station, \
        func.min(Measurement.tobs), \
        func.avg(Measurement.tobs), \
        func.max(Measurement.tobs)]
    station_temp_stats = session.query(*sel)\
        .filter(Measurement.station == most_active_station)\
        .filter(Measurement.date >= start)\
        .group_by(Measurement.station).all()
    df = pd.DataFrame(station_temp_stats,\
        columns=['date','TMIN','TAVG','TMAX'])\
        .set_index('date')
    # Sort the dataframe by date
    df = df.sort_values(by=['date'])

    tobs_dict = df.to_dict('list')

    # jsonify the dict  
    return jsonify(tobs_dict)

@app.route("/api/v1.0/<start>/<end>")
def start_end_date(start,end):
    # Create our session (link) from Python to the DB
    session = Session(engine)

    sel = [Station.station, func.count(Measurement.date)]
    station_counts = session.query(*sel).\
        filter(Station.station == Measurement.station).\
        group_by(Station.station).\
        order_by(func.count(Measurement.date).\
        desc()).\
        all()
    most_active_station = station_counts[0][0]
    sel = [Measurement.station, \
        func.min(Measurement.tobs), \
        func.avg(Measurement.tobs), \
        func.max(Measurement.tobs)]
    station_temp_stats = session.query(*sel)\
        .filter(Measurement.station == most_active_station)\
        .filter(Measurement.date >= start)\
        .filter(Measurement.date <= end)\
        .group_by(Measurement.station).all()
    df = pd.DataFrame(station_temp_stats,\
        columns=['Station','TMIN','TAVG','TMAX'])\
        .set_index('Station')
    # Sort the dataframe by date
    df = df.sort_values(by=['Station'])

    tobs_dict = df.to_dict('list')

    # jsonify the dict  
    return jsonify(tobs_dict)


if __name__ == "__main__":
    app.run(debug=True)