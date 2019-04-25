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
    prcp_df = pd.read_sql("SELECT date, prcp FROM measurement", con=engine, columns=[["date"],["prcp"]])
    prcp_df["date"] = pd.to_datetime(prcp_df["date"],format="%Y-%m-%d", errors="coerce")
    pa_max_date = prcp_df["date"].max().date()
    pa_today = pd.Timestamp(dt.date.today())
    pa_min_date = (pa_max_date - dt.timedelta(days=365))
    prcp_df = prcp_df.loc[prcp_df["date"]>=pa_min_date]
    
    PRCP_DICT = prcp_df.to_dict()    
    return (jsonify(PRCP_DICT))



@app.route("/api/v1.0/stations")
def stations():
    stat_str = "SELECT station FROM station"
    active_df = pd.read_sql(stat_str, con=engine, columns=[["Station"]])
    active_df.set_index("station")
    return active_df.to_json(orient="values")


@app.route("/api/v1.0/tobs")
def temp_monthly():
    tobs_df = pd.read_sql("SELECT date, tobs FROM measurement", con=engine, columns=[["date"],["tobs"]])
    tobs_df["date"] = pd.to_datetime(tobs_df["date"],format="%Y-%m-%d", errors="coerce")
    pa_max_date = tobs_df["date"].max().date()
    pa_today = pd.Timestamp(dt.date.today())
    pa_min_date = (pa_max_date - dt.timedelta(days=365))
    tobs_df = tobs_df.loc[tobs_df["date"]>=pa_min_date]
    
    TOBS_DICT = tobs_df.to_dict()    
    return (jsonify(TOBS_DICT))
    


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