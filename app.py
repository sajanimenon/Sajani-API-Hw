import datetime as dt
import pandas as pd
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify


#-----------------------------------------------
# Flask App Setup                              |
#-----------------------------------------------

engine = create_engine("sqlite:///Resources/hawaii.sqlite")

Base = automap_base()

Base.prepare(engine, reflect=True)

Measurement = Base.classes.measurement
Station = Base.classes.station

session = Session(bind=engine)


app = Flask(__name__)

@app.route("/")
def welcome():
    prcp_df = pd.read_sql("SELECT date, prcp FROM measurement", con=engine, columns=[["date"],["prcp"]])
    prcp_df["date"] = pd.to_datetime(prcp_df["date"],format="%Y-%m-%d", errors="coerce")
    pa_max_date = str(prcp_df["date"].max().date()-dt.timedelta(days=1))
    pa_py_date = str(prcp_df["date"].max().date()-dt.timedelta(days=366))
    return (
        f"<H3>Welcome to the Hawaii Climate Analysis API!<br/><br />"
        f"<b>Avalable Routes:<br/></b>"
        f"<a href='/api/v1.0/precipitation'>/api/v1.0/precipitation</a><br/>"
        f"<a href='/api/v1.0/stations'>/api/v1.0/stations</a><br/>"
        f"<a href='/api/v1.0/tobs'>/api/v1.0/tobs</a><br/>"
        f"<a href='/api/v1.0/temp/"+pa_py_date+"'>/api/v1.0/temp-range/</a>start<br />"
        f"<a href='/api/v1.0/temp-range/"+pa_py_date+"/"+pa_max_date+"'>api/v1.0/temp-range/</a>start/end"
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    PY = dt.date.today() - dt.timedelta(days=365)
    CY = dt.date.today()
    prcp_str = "SELECT date, prcp FROM measurement WHERE date >'"+str(PY)+"' AND date <='"+str(CY)+"'  ORDER BY date ASC"
    precipitation = pd.read_sql(prcp_str, con=engine, columns=[["prcp"],["date"]])
    #precipitation.set_index("date",inplace = true)

    return precipitation.to_json(orient="records")

@app.route("/api/v1.0/stations")
def stations():
    stat_str = "SELECT station FROM station"
    active_df = pd.read_sql(stat_str, con=engine, columns=[["Station"]])
    active_df.set_index("station")
    return active_df.to_json(orient="values")


@app.route("/api/v1.0/tobs")
def temp_monthly():
    CY = dt.date.today()
    PY = dt.date.today() - dt.timedelta(days=365)
    tobs_str= "SELECT date, tobs FROM measurement WHERE date >'"+str(PY)+"' AND date <='"+str(CY)+"'  ORDER BY date ASC"
    tobs_df = pd.read_sql(tobs_str, con=engine, columns=[["date"],["tobs"]])

    return tobs_df.to_json(orient="records")


@app.route("/api/v1.0/temp/<start>")
def stats(start=None):
    no_e_str = "SELECT MIN(tobs) Min, AVG(tobs) Avg, MAX(tobs) Max FROM measurement WHERE date >'"+start+"' ORDER BY date ASC"
    st_df = pd.read_sql(no_e_str, con=engine, columns=[["date"],["tobs"]])
    return st_df.to_json(orient="records")


@app.route("/api/v1.0/temp-range/<start>/<end>")
def stat_range(start=None, end=None):
    se_str = "SELECT MIN(tobs) Min, AVG(tobs) Avg, MAX(tobs) Max FROM measurement WHERE date >'" + start + "' AND date <='" + end + "' ORDER BY date ASC"
    ed_df = pd.read_sql(se_str, con=engine, columns=[["date"],["tobs"]])
    return ed_df.to_json(orient="records")


if __name__ == '__main__':
    app.run(debug=True)