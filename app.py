#required packages
from pickle import FALSE, TRUE
from flask import Flask, render_template, url_for, request, redirect, send_file, session, render_template_string
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, create_engine
from datetime import datetime, timedelta
import pandas as pd
import requests
from datetime import timedelta
import numpy as np
import datetime as dt
from math import radians, cos, sin, asin, sqrt
import pdfkit
from flask_session import Session  # https://pythonhosted.org/Flask-Session
import msal
import app_config
import uuid
import io
from sqlalchemy import or_


config = pdfkit.configuration(wkhtmltopdf=r"C:/Program Files/wkhtmltopdf/bin/wkhtmltopdf.exe")
css = "static/css/main.css"
#input API access key
access_key = "9c8669b7ff0b37ea6e988dac926c7339"

#initialise some things for distance calc
airports = pd.read_csv(r'C:\Users\James Morgan\OneDrive - Oracle Solicitors\Desktop\Flightbot JM/airports.csv')
airports = airports[airports['scheduled_service']=="yes"]

def greatcirclecalc(departure, arrival):
    deplat = airports[airports['iata_code']==departure].iloc[:,4]
    deplong = airports[airports['iata_code']==departure].iloc[:,5]
    arrlat = airports[airports['iata_code']==arrival].iloc[:,4]
    arrlong = airports[airports['iata_code']==arrival].iloc[:,5]

    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [deplong, deplat, arrlong, arrlat])
    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    km = 6371 * c
    
    if km < 1500:
        eu261 = 250
    elif km < 3500:
        eu261 = 400
    else:
        eu261 = 600
    
    return km, eu261

#Define a load of things required for auth

def _load_cache():
    cache = msal.SerializableTokenCache()
    if session.get("token_cache"):
        cache.deserialize(session["token_cache"])
    return cache

def _save_cache(cache):
    if cache.has_state_changed:
        session["token_cache"] = cache.serialize()

def _build_msal_app(cache=None, authority=None):
    return msal.ConfidentialClientApplication(
        app_config.CLIENT_ID, authority=authority or app_config.AUTHORITY,
        client_credential=app_config.CLIENT_SECRET, token_cache=cache)

def _build_auth_code_flow(authority=None, scopes=None):
    return _build_msal_app(authority=authority).initiate_auth_code_flow(
        scopes or [],
        redirect_uri=url_for("authorized", _external=True))

def _get_token_from_cache(scope=None):
    cache = _load_cache()  # This web app maintains one cache per session
    cca = _build_msal_app(cache=cache)
    accounts = cca.get_accounts()
    if accounts:  # So all account(s) belong to the current signed-in user
        result = cca.acquire_token_silent(scope, account=accounts[0])
        _save_cache(cache)
        return result

#set up database
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///test.db' # 3 slashes is relative path - 4 for absolute

db = SQLAlchemy(app)

class flight(db.Model):
    flightid = db.Column(db.String(200), primary_key  = True)
    flight_date = db.Column(db.DateTime)
    flight_status = db.Column(db.String(200), nullable = False)
    departure_timezone = db.Column(db.String(200))
    departure_airport = db.Column(db.String(200), nullable = False)
    departure_iata = db.Column(db.String(200), nullable = False)
    departure_icao = db.Column(db.String(200), nullable = True)
    departure_terminal = db.Column(db.String(200), nullable = True)
    departure_gate = db.Column(db.String(200), nullable = True)
    departure_delay = db.Column(db.Integer, nullable = True)
    departure_scheduled = db.Column(db.DateTime)
    departure_estimated = db.Column(db.DateTime)
    departure_actual = db.Column(db.DateTime, nullable = True)
    departure_estimated_runway = db.Column(db.DateTime, nullable = True)
    departure_actual_runway = db.Column(db.DateTime, nullable = True)
    arrival_airport = db.Column(db.String(200), nullable = False)
    arrival_timezone = db.Column(db.String(200), nullable = False)
    arrival_iata = db.Column(db.String(200), nullable = False)
    arrival_icao = db.Column(db.String(200), nullable = True)
    arrival_terminal = db.Column(db.String(200), nullable = True)
    arrival_gate = db.Column(db.String(200), nullable = True)
    arrival_baggage = db.Column(db.String(200), nullable = True)
    arrival_delay = db.Column(db.Integer, nullable = True)
    arrival_scheduled = db.Column(db.DateTime)
    arrival_estimated = db.Column(db.DateTime)
    arrival_actual = db.Column(db.DateTime, nullable = True)
    arrival_estimated_runway = db.Column(db.DateTime, nullable = True)
    arrival_actual_runway = db.Column(db.DateTime, nullable = True)
    airline_name = db.Column(db.String(200), nullable = True)
    airline_iata = db.Column(db.String(200), nullable = True)
    airline_icao = db.Column(db.String(200), nullable = True)
    flight_number = db.Column(db.Integer, nullable = True)
    flight_iata = db.Column(db.String(200), nullable = True)
    flight_icao = db.Column(db.String(200))
    queried_by = db.Column(db.String(200), nullable = False)
    date_created = db.Column(db.DateTime, default = datetime.utcnow())

    def __repr__(self):
        return '<Task %r>' % self.queryid

class directalternative(db.Model):
    db_id = db.Column(db.Integer, primary_key = True)
    routeid = db.Column(db.String(200))
    flight_date = db.Column(db.DateTime)
    flight_status = db.Column(db.String(200), nullable = False)
    departure_timezone = db.Column(db.String(200))
    departure_airport = db.Column(db.String(200), nullable = False)
    departure_iata = db.Column(db.String(200), nullable = False)
    departure_icao = db.Column(db.String(200), nullable = True)
    departure_terminal = db.Column(db.String(200), nullable = True)
    departure_gate = db.Column(db.String(200), nullable = True)
    departure_delay = db.Column(db.Integer, nullable = True)
    departure_scheduled = db.Column(db.DateTime)
    departure_estimated = db.Column(db.DateTime)
    departure_actual = db.Column(db.DateTime, nullable = True)
    departure_estimated_runway = db.Column(db.DateTime, nullable = True)
    departure_actual_runway = db.Column(db.DateTime, nullable = True)
    arrival_airport = db.Column(db.String(200), nullable = False)
    arrival_timezone = db.Column(db.String(200), nullable = False)
    arrival_iata = db.Column(db.String(200), nullable = False)
    arrival_icao = db.Column(db.String(200), nullable = True)
    arrival_terminal = db.Column(db.String(200), nullable = True)
    arrival_gate = db.Column(db.String(200), nullable = True)
    arrival_baggage = db.Column(db.String(200), nullable = True)
    arrival_delay = db.Column(db.Integer, nullable = True)
    arrival_scheduled = db.Column(db.DateTime)
    arrival_estimated = db.Column(db.DateTime)
    arrival_actual = db.Column(db.DateTime, nullable = True)
    arrival_estimated_runway = db.Column(db.DateTime, nullable = True)
    arrival_actual_runway = db.Column(db.DateTime, nullable = True)
    airline_name = db.Column(db.String(200), nullable = True)
    airline_iata = db.Column(db.String(200), nullable = True)
    airline_icao = db.Column(db.String(200), nullable = True)
    flight_number = db.Column(db.Integer, nullable = True)
    flight_iata = db.Column(db.String(200), nullable = True)
    flight_icao = db.Column(db.String(200))
    queried_by = db.Column(db.String(200), nullable = False)
    separate_query = db.Column(db.String(200))
    date_created = db.Column(db.DateTime, default = datetime.utcnow())


    def __repr__(self):
        return '<Task %r>' % self.queryid

class indirectalternative(db.Model):
    db_id = db.Column(db.Integer, primary_key = True)
    routeid = db.Column(db.String(200))
    f1_flight_date = db.Column(db.DateTime)
    f1_flight_status = db.Column(db.String(200), nullable = False)
    f1_departure_timezone = db.Column(db.String(200))
    f1_departure_airport = db.Column(db.String(200), nullable = False)
    f1_departure_iata = db.Column(db.String(200), nullable = False)
    f1_departure_icao = db.Column(db.String(200), nullable = True)
    f1_departure_terminal = db.Column(db.String(200), nullable = True)
    f1_departure_gate = db.Column(db.String(200), nullable = True)
    f1_departure_delay = db.Column(db.Integer, nullable = True)
    f1_departure_scheduled = db.Column(db.DateTime)
    f1_departure_estimated = db.Column(db.DateTime)
    f1_departure_actual = db.Column(db.DateTime, nullable = True)
    f1_departure_estimated_runway = db.Column(db.DateTime, nullable = True)
    f1_departure_actual_runway = db.Column(db.DateTime, nullable = True)
    f1_arrival_airport = db.Column(db.String(200), nullable = False)
    f1_arrival_timezone = db.Column(db.String(200), nullable = False)
    f1_arrival_iata = db.Column(db.String(200), nullable = False)
    f1_arrival_icao = db.Column(db.String(200), nullable = True)
    f1_arrival_terminal = db.Column(db.String(200), nullable = True)
    f1_arrival_gate = db.Column(db.String(200), nullable = True)
    f1_arrival_baggage = db.Column(db.String(200), nullable = True)
    f1_arrival_delay = db.Column(db.Integer, nullable = True)
    f1_arrival_scheduled = db.Column(db.DateTime)
    f1_arrival_estimated = db.Column(db.DateTime)
    f1_arrival_actual = db.Column(db.DateTime, nullable = True)
    f1_arrival_estimated_runway = db.Column(db.DateTime, nullable = True)
    f1_arrival_actual_runway = db.Column(db.DateTime, nullable = True)
    f1_airline_name = db.Column(db.String(200), nullable = True)
    f1_airline_iata = db.Column(db.String(200), nullable = True)
    f1_airline_icao = db.Column(db.String(200), nullable = True)
    f1_flight_number = db.Column(db.Integer, nullable = True)
    f1_flight_iata = db.Column(db.String(200), nullable = True)
    f1_flight_icao = db.Column(db.String(200))
    f2_flight_date = db.Column(db.DateTime)
    f2_flight_status = db.Column(db.String(200), nullable = False)
    f2_departure_timezone = db.Column(db.String(200))
    f2_departure_airport = db.Column(db.String(200), nullable = False)
    f2_departure_iata = db.Column(db.String(200), nullable = False)
    f2_departure_icao = db.Column(db.String(200), nullable = True)
    f2_departure_terminal = db.Column(db.String(200), nullable = True)
    f2_departure_gate = db.Column(db.String(200), nullable = True)
    f2_departure_delay = db.Column(db.Integer, nullable = True)
    f2_departure_scheduled = db.Column(db.DateTime)
    f2_departure_estimated = db.Column(db.DateTime)
    f2_departure_actual = db.Column(db.DateTime, nullable = True)
    f2_departure_estimated_runway = db.Column(db.DateTime, nullable = True)
    f2_departure_actual_runway = db.Column(db.DateTime, nullable = True)
    f2_arrival_airport = db.Column(db.String(200), nullable = False)
    f2_arrival_timezone = db.Column(db.String(200), nullable = False)
    f2_arrival_iata = db.Column(db.String(200), nullable = False)
    f2_arrival_icao = db.Column(db.String(200), nullable = True)
    f2_arrival_terminal = db.Column(db.String(200), nullable = True)
    f2_arrival_gate = db.Column(db.String(200), nullable = True)
    f2_arrival_baggage = db.Column(db.String(200), nullable = True)
    f2_arrival_delay = db.Column(db.Integer, nullable = True)
    f2_arrival_scheduled = db.Column(db.DateTime)
    f2_arrival_estimated = db.Column(db.DateTime)
    f2_arrival_actual = db.Column(db.DateTime, nullable = True)
    f2_arrival_estimated_runway = db.Column(db.DateTime, nullable = True)
    f2_arrival_actual_runway = db.Column(db.DateTime, nullable = True)
    f2_airline_name = db.Column(db.String(200), nullable = True)
    f2_airline_iata = db.Column(db.String(200), nullable = True)
    f2_airline_icao = db.Column(db.String(200), nullable = True)
    f2_flight_number = db.Column(db.Integer, nullable = True)
    f2_flight_iata = db.Column(db.String(200), nullable = True)
    f2_flight_icao = db.Column(db.String(200))
    queried_by = db.Column(db.String(200), nullable = False)
    date_created = db.Column(db.DateTime, default = datetime.utcnow())

    def __repr__(self):
        return '<Task %r>' % self.queryid

app.config.from_object(app_config)
Session(app)
from werkzeug.middleware.proxy_fix import ProxyFix
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

Flask.permanent_session_lifetime = timedelta(minutes=1)
#Session.permanent = False

@app.route('/', methods = ['POST', 'GET']) #url string goes in here


def index():
    if not session.get("user"):
        return redirect(url_for("login"))
    
    session['username'] = session['user'].get('preferred_username')
    session['flightnotfound'] = False
    
    if request.method == 'POST':
        #get all info from inputs


        
        
        if request.form['btn'] == "Find":
            session['altairports'] = []
            session['requested_flight_iata'] = request.form['flight number']
            session['requested_flight_date'] = datetime.strptime(request.form['flight date'], '%Y-%m-%d')
            session['daf_requested'] = "error"
            session['iaf_requested'] = "error"
            session['altroutesrequested'] = []
            session['alts_added'] = False
            

            try:
                daf_checkbox = request.form['DA']
                session['daf_requested'] = True
            except:
                session['daf_requested'] = False
            try:
                iaf_checkbox = request.form['IA']
                session['iaf_requested'] = True
            except:
                session['iaf_requested'] = False

            #create the flightid 
            session['flightid'] = str(session['requested_flight_iata']) + str(session['requested_flight_date'])


            #if the flightid is not already in the database
            if len(flight.query.filter(flight.flightid == session['flightid']).all()) == 0:
                
                #query the API for the requested flight details
                flight_params = {'access_key' : access_key, 'flight_iata' : session['requested_flight_iata']}#, 'flight_date' : requested_flight_date}
                

                flight_api_result = requests.get('http://api.aviationstack.com/v1/flights', flight_params)
                flight_api_response = flight_api_result.json()
                flight_api_result.close()

                #format the response ready to save to Dataframe
                temp_df = pd.DataFrame()

                for col in pd.DataFrame(list(flight_api_response['data'])).iloc[:,:-2]:
                    temp_df = pd.concat([temp_df, pd.DataFrame(list(pd.DataFrame(list(flight_api_response['data']))[col])).add_prefix(col)], axis = 1)
                flight_df = temp_df

                if len(flight_df) == 0:
                    session['flightnotfound'] = True
                    flightid = session['flightid']
                    flight_date = pd.to_datetime(0)
                    flight_status = "NOTFOUND"
                    departure_timezone = "NOTFOUND"
                    departure_airport = "NOTFOUND"
                    departure_iata = "NOTFOUND"
                    departure_icao = "NOTFOUND"
                    departure_terminal = "NOTFOUND"
                    departure_gate = 0
                    departure_delay = 0
                    departure_scheduled = pd.to_datetime(0)
                    departure_estimated = pd.to_datetime(0)
                    departure_actual = pd.to_datetime(0)
                    departure_estimated_runway = pd.to_datetime(0)
                    departure_actual_runway = pd.to_datetime(0)
                    arrival_airport = "NOTFOUND"
                    arrival_timezone = "NOTFOUND"
                    arrival_iata = "NOTFOUND"
                    arrival_icao = "NOTFOUND"
                    arrival_terminal = "NOTFOUND"
                    arrival_gate = 0
                    arrival_baggage = "NOTFOUND"
                    arrival_delay = 0
                    arrival_scheduled = pd.to_datetime(0)
                    arrival_estimated = pd.to_datetime(0)
                    arrival_actual = pd.to_datetime(0)
                    arrival_estimated_runway = pd.to_datetime(0)
                    arrival_actual_runway = pd.to_datetime(0)
                    airline_name = "NOTFOUND"
                    airline_iata = "NOTFOUND"
                    airline_icao = "NOTFOUND"
                    flight_number = 0
                    flight_iata = "NOTFOUND"
                    flight_icao = "NOTFOUND"
                    queried_by = session['username']
                    
                    new_flight_row = flight(flightid=session['flightid'], flight_date = flight_date, flight_status = flight_status, departure_timezone = departure_timezone, departure_airport = departure_airport, departure_iata = departure_iata, departure_icao = departure_icao, departure_terminal = departure_terminal, departure_gate = departure_gate, departure_delay = departure_delay, departure_scheduled = departure_scheduled, departure_estimated = departure_estimated, departure_actual = departure_actual, departure_estimated_runway = departure_estimated_runway, departure_actual_runway = departure_actual_runway, arrival_airport = arrival_airport, arrival_timezone = arrival_timezone, arrival_iata = arrival_iata, arrival_icao = arrival_icao, arrival_terminal = arrival_terminal, arrival_gate = arrival_gate, arrival_baggage = arrival_baggage, arrival_delay = arrival_delay, arrival_scheduled = arrival_scheduled, arrival_estimated = arrival_estimated, arrival_actual = arrival_actual, arrival_estimated_runway = arrival_estimated_runway, arrival_actual_runway = arrival_actual_runway, airline_name = airline_name, airline_iata = airline_iata, airline_icao = airline_icao, flight_number = flight_number, flight_iata = flight_iata, flight_icao = flight_icao, queried_by = queried_by)
                    db.session.add(new_flight_row)
                    db.session.commit()
                    query_departure_iata =  departure_iata
                    query_arrival_iata = arrival_iata
                
                else:
                    
                    flightid = session['flightid']
                    flight_date = pd.to_datetime(flight_df.iloc[0, 0])
                    flight_status = flight_df.iloc[0, 1]
                    departure_timezone = flight_df.iloc[0, 3]
                    departure_airport = flight_df.iloc[0, 2]
                    departure_iata = flight_df.iloc[0, 4]
                    departure_icao = flight_df.iloc[0, 5]
                    departure_terminal = flight_df.iloc[0, 6]
                    departure_gate = flight_df.iloc[0, 7]
                    if pd.isna(flight_df.iloc[0, 8]):
                        departure_delay = flight_df.iloc[0, 8]
                    else:
                        departure_delay = int(flight_df.iloc[0, 8])
                    departure_scheduled = pd.to_datetime(flight_df.iloc[0, 9])
                    departure_estimated = pd.to_datetime(flight_df.iloc[0, 10])
                    departure_actual = pd.to_datetime(flight_df.iloc[0, 11])
                    departure_estimated_runway = pd.to_datetime(flight_df.iloc[0, 12])
                    departure_actual_runway = pd.to_datetime(flight_df.iloc[0, 13])
                    arrival_airport = flight_df.iloc[0, 14]
                    arrival_timezone = flight_df.iloc[0, 15]
                    arrival_iata = flight_df.iloc[0, 16]
                    arrival_icao = flight_df.iloc[0, 17]
                    arrival_terminal = flight_df.iloc[0, 18]
                    arrival_gate = flight_df.iloc[0, 19]
                    arrival_baggage = flight_df.iloc[0, 20]
                    if pd.isna(flight_df.iloc[0, 21]):
                        arrival_delay = flight_df.iloc[0, 21]
                    else:
                        arrival_delay = int(flight_df.iloc[0, 21])
                    arrival_scheduled = pd.to_datetime(flight_df.iloc[0, 22])
                    arrival_estimated = pd.to_datetime(flight_df.iloc[0, 23])
                    arrival_actual = pd.to_datetime(flight_df.iloc[0, 24])
                    arrival_estimated_runway = pd.to_datetime(flight_df.iloc[0, 25])
                    arrival_actual_runway = pd.to_datetime(flight_df.iloc[0, 26])
                    airline_name = flight_df.iloc[0, 27]
                    airline_iata = flight_df.iloc[0, 28]
                    airline_icao = flight_df.iloc[0, 29]
                    flight_number = flight_df.iloc[0, 30]
                    flight_iata = flight_df.iloc[0, 31]
                    flight_icao = flight_df.iloc[0, 32]
                    queried_by = session['username']
                    

                    new_flight_row = flight(flightid=session['flightid'], flight_date = flight_date, flight_status = flight_status, departure_timezone = departure_timezone, departure_airport = departure_airport, departure_iata = departure_iata, departure_icao = departure_icao, departure_terminal = departure_terminal, departure_gate = departure_gate, departure_delay = departure_delay, departure_scheduled = departure_scheduled, departure_estimated = departure_estimated, departure_actual = departure_actual, departure_estimated_runway = departure_estimated_runway, departure_actual_runway = departure_actual_runway, arrival_airport = arrival_airport, arrival_timezone = arrival_timezone, arrival_iata = arrival_iata, arrival_icao = arrival_icao, arrival_terminal = arrival_terminal, arrival_gate = arrival_gate, arrival_baggage = arrival_baggage, arrival_delay = arrival_delay, arrival_scheduled = arrival_scheduled, arrival_estimated = arrival_estimated, arrival_actual = arrival_actual, arrival_estimated_runway = arrival_estimated_runway, arrival_actual_runway = arrival_actual_runway, airline_name = airline_name, airline_iata = airline_iata, airline_icao = airline_icao, flight_number = flight_number, flight_iata = flight_iata, flight_icao = flight_icao, queried_by = queried_by)
                    db.session.add(new_flight_row)
                    db.session.commit()
                    query_departure_iata =  departure_iata
                    query_arrival_iata = arrival_iata
                    
            
            else:
                flights = flight.query.filter(flight.flightid == session['flightid']).order_by(flight.flight_date).all()
                query_departure_iata = flights[0].departure_iata
                query_arrival_iata = flights[0].arrival_iata


            session['routeid'] = query_departure_iata + query_arrival_iata + str(session['requested_flight_date'])
            session['altdepsrequested'] = [query_departure_iata]
            session['altarrsrequested'] = [query_arrival_iata]
            if session['altarrsrequested'][0] == "NOTFOUND":
                session['flightnotfound']  =  True
                

            if session['flightnotfound'] == False:

                #if just direct alternatives that must be acquired:
                if ((session['daf_requested'] == True) and (session['iaf_requested'] == False)) or ((session['daf_requested'] == True) and (session['iaf_requested'] == True) and (len(indirectalternative.query.filter(indirectalternative.routeid == session['routeid']).all()) != 0)):
                    #if the routeid does not already exist in the db (if it does, no action as it will just be queried at the end)
                    if len(directalternative.query.filter(directalternative.routeid == session['routeid']).all()) == 0:
                        #initialise some variables and a dataframe to collect all the query results
                        offset = 0
                        complete = False
                        daf_df = pd.DataFrame(columns = ['flight_date0', 'flight_status0', 'departureairport', 'departuretimezone', 'departureiata', 'departureicao', 'departureterminal', 'departuregate', 'departuredelay', 'departurescheduled', 'departureestimated', 'departureactual', 'departureestimated_runway', 'departureactual_runway', 'arrivalairport', 'arrivaltimezone', 'arrivaliata', 'arrivalicao', 'arrivalterminal', 'arrivalgate', 'arrivalbaggage', 'arrivaldelay', 'arrivalscheduled', 'arrivalestimated', 'arrivalactual', 'arrivalestimated_runway', 'arrivalactual_runway', 'airlinename', 'airlineiata', 'airlineicao', 'flightnumber', 'flightiata', 'flighticao', 'flightcodeshared'])
                        #query the api and collect all results
                        while complete == False:
                            daf_params = {'access_key' : access_key, 'dep_iata' : query_departure_iata, 'arr_iata' : query_arrival_iata, 'offset' : offset}
                            daf_api_result = requests.get('http://api.aviationstack.com/v1/flights', daf_params)
                            daf_api_response = daf_api_result.json()
                            daf_api_result.close()

                            #sort the data and add it to a dataframe
                            temp_df = pd.DataFrame()
                            for col in pd.DataFrame(list(daf_api_response['data'])).iloc[:,:-2]:
                                temp_df = pd.concat([temp_df, pd.DataFrame(list(pd.DataFrame(list(daf_api_response['data']))[col])).add_prefix(col)], axis = 1)

                            daf_df = daf_df.append(temp_df)
                            offset += 100

                            if len(temp_df)<100:
                                complete = True
                        #drop any duplicates
                        daf_df = daf_df.drop_duplicates(subset="flightiata")
                        
                        #if the flight was delayed, remove all flights that arrived after the original - THIS WILL ALL NOW BE DONE AT THE END
                        #if arrival_actual is not None:
                            #daf_df = daf_df[daf_df['arrivalscheduled'] < arrival_actual]

                        for row in range(len(daf_df)):

                            routeid = session['routeid']
                            flight_date = pd.to_datetime(daf_df.iloc[row, 0])
                            flight_status = daf_df.iloc[row, 1]
                            departure_timezone = daf_df.iloc[row, 3]
                            departure_airport = daf_df.iloc[row, 2]
                            departure_iata = daf_df.iloc[row, 4]
                            departure_icao = daf_df.iloc[row, 5]
                            departure_terminal = daf_df.iloc[row, 6]
                            departure_gate = daf_df.iloc[row, 7]
                            departure_delay = daf_df.iloc[row, 8]
                            departure_scheduled = pd.to_datetime(daf_df.iloc[row, 9])
                            departure_estimated = pd.to_datetime(daf_df.iloc[row, 10])
                            departure_actual = pd.to_datetime(daf_df.iloc[row, 11])
                            departure_estimated_runway = pd.to_datetime(daf_df.iloc[row, 12])
                            departure_actual_runway = pd.to_datetime(daf_df.iloc[row, 13])
                            arrival_airport = daf_df.iloc[row, 14]
                            arrival_timezone = daf_df.iloc[row, 15]
                            arrival_iata = daf_df.iloc[row, 16]
                            arrival_icao = daf_df.iloc[row, 17]
                            arrival_terminal = daf_df.iloc[row, 18]
                            arrival_gate = daf_df.iloc[row, 19]
                            arrival_baggage = daf_df.iloc[row, 20]
                            arrival_delay = daf_df.iloc[row, 21]
                            arrival_scheduled = pd.to_datetime(daf_df.iloc[row, 22])
                            arrival_estimated = pd.to_datetime(daf_df.iloc[row, 23])
                            arrival_actual = pd.to_datetime(daf_df.iloc[row, 24])
                            arrival_estimated_runway = pd.to_datetime(daf_df.iloc[row, 25])
                            arrival_actual_runway = pd.to_datetime(daf_df.iloc[row, 26])
                            airline_name = daf_df.iloc[row, 27]
                            airline_iata = daf_df.iloc[row, 28]
                            airline_icao = daf_df.iloc[row, 29]
                            flight_number = daf_df.iloc[row, 30]
                            flight_iata = daf_df.iloc[row, 31]
                            flight_icao = daf_df.iloc[row, 32]
                            separate_query = "True"
                            queried_by = session['username']
                            new_directalternative_row = directalternative(routeid=routeid, flight_date = flight_date, flight_status = flight_status, departure_timezone = departure_timezone, departure_airport = departure_airport, departure_iata = departure_iata, departure_icao = departure_icao, departure_terminal = departure_terminal, departure_gate = departure_gate, departure_delay = departure_delay, departure_scheduled = departure_scheduled, departure_estimated = departure_estimated, departure_actual = departure_actual, departure_estimated_runway = departure_estimated_runway, departure_actual_runway = departure_actual_runway, arrival_airport = arrival_airport, arrival_timezone = arrival_timezone, arrival_iata = arrival_iata, arrival_icao = arrival_icao, arrival_terminal = arrival_terminal, arrival_gate = arrival_gate, arrival_baggage = arrival_baggage, arrival_delay = arrival_delay, arrival_scheduled = arrival_scheduled, arrival_estimated = arrival_estimated, arrival_actual = arrival_actual, arrival_estimated_runway = arrival_estimated_runway, arrival_actual_runway = arrival_actual_runway, airline_name = airline_name, airline_iata = airline_iata, airline_icao = airline_icao, flight_number = flight_number, flight_iata = flight_iata, flight_icao = flight_icao, separate_query = separate_query, queried_by = queried_by)
                            db.session.add(new_directalternative_row)
                            db.session.commit()
                        
                        if len(daf_df) == 0:
                            routeid = session['routeid']
                            flight_date = pd.to_datetime(0)
                            flight_status = "NOTFOUND"
                            departure_timezone = "NOTFOUND"
                            departure_airport = "NOTFOUND"
                            departure_iata = "NOTFOUND"
                            departure_icao = "NOTFOUND"
                            departure_terminal = "NOTFOUND"
                            departure_gate = 0
                            departure_delay = 0
                            departure_scheduled = pd.to_datetime(0)
                            departure_estimated = pd.to_datetime(0)
                            departure_actual = pd.to_datetime(0)
                            departure_estimated_runway = pd.to_datetime(0)
                            departure_actual_runway = pd.to_datetime(0)
                            arrival_airport = "NOTFOUND"
                            arrival_timezone = "NOTFOUND"
                            arrival_iata = "NOTFOUND"
                            arrival_icao = "NOTFOUND"
                            arrival_terminal = "NOTFOUND"
                            arrival_gate = 0
                            arrival_baggage = "NOTFOUND"
                            arrival_delay = 0
                            arrival_scheduled = pd.to_datetime(0)
                            arrival_estimated = pd.to_datetime(0)
                            arrival_actual = pd.to_datetime(0)
                            arrival_estimated_runway = pd.to_datetime(0)
                            arrival_actual_runway = pd.to_datetime(0)
                            airline_name = "NOTFOUND"
                            airline_iata = "NOTFOUND"
                            airline_icao = "NOTFOUND"
                            flight_number = 0
                            flight_iata = "NOTFOUND"
                            flight_icao = "NOTFOUND"
                            separate_query = "True"
                            queried_by = session['username']
                            
                            new_directalternative_row = directalternative(routeid=routeid, flight_date = flight_date, flight_status = flight_status, departure_timezone = departure_timezone, departure_airport = departure_airport, departure_iata = departure_iata, departure_icao = departure_icao, departure_terminal = departure_terminal, departure_gate = departure_gate, departure_delay = departure_delay, departure_scheduled = departure_scheduled, departure_estimated = departure_estimated, departure_actual = departure_actual, departure_estimated_runway = departure_estimated_runway, departure_actual_runway = departure_actual_runway, arrival_airport = arrival_airport, arrival_timezone = arrival_timezone, arrival_iata = arrival_iata, arrival_icao = arrival_icao, arrival_terminal = arrival_terminal, arrival_gate = arrival_gate, arrival_baggage = arrival_baggage, arrival_delay = arrival_delay, arrival_scheduled = arrival_scheduled, arrival_estimated = arrival_estimated, arrival_actual = arrival_actual, arrival_estimated_runway = arrival_estimated_runway, arrival_actual_runway = arrival_actual_runway, airline_name = airline_name, airline_iata = airline_iata, airline_icao = airline_icao, flight_number = flight_number, flight_iata = flight_iata, flight_icao = flight_icao, separate_query = separate_query, queried_by = queried_by)
                            db.session.add(new_directalternative_row)
                            db.session.commit()

                #if indirect alternatives have been requested
                if session['iaf_requested'] == True:

                    #if the routeid does not already exist in the db
                    if len(indirectalternative.query.filter(indirectalternative.routeid == session['routeid']).all()) == 0:                
                        #initialise variables and dataframe that allow to iterate through
                        offset = 0
                        complete = False

                        dep_df = pd.DataFrame(columns = ['flight_date0', 'flight_status0', 'departureairport',
                    'departuretimezone', 'departureiata', 'departureicao',
                    'departureterminal', 'departuregate', 'departuredelay',
                    'departurescheduled', 'departureestimated', 'departureactual',
                    'departureestimated_runway', 'departureactual_runway', 'arrivalairport',
                    'arrivaltimezone', 'arrivaliata', 'arrivalicao', 'arrivalterminal',
                    'arrivalgate', 'arrivalbaggage', 'arrivaldelay', 'arrivalscheduled',
                    'arrivalestimated', 'arrivalactual', 'arrivalestimated_runway',
                    'arrivalactual_runway', 'airlinename', 'airlineiata', 'airlineicao',
                    'flightnumber', 'flightiata', 'flighticao', 'flightcodeshared'], dtype = object)

                        #request all flights from departure airport until done
                        while complete == False:


                            dep_params = {'access_key' : access_key, 'dep_iata' : query_departure_iata, 'offset' : offset}

                            dep_api_result = requests.get('http://api.aviationstack.com/v1/flights', dep_params)

                            dep_api_response = dep_api_result.json()

                            dep_api_result.close()

                            temp_df = pd.DataFrame()
                            for col in pd.DataFrame(list(dep_api_response['data'])).iloc[:,:-2]:
                                temp_df = pd.concat([temp_df, pd.DataFrame(list(pd.DataFrame(list(dep_api_response['data']))[col])).add_prefix(col)], axis = 1)

                            dep_df = dep_df.append(temp_df)

                            offset += 100

                            if len(temp_df)<100:

                                complete = True



                        #query all arriving flights at destination airport

                        #initialise variables and a dataframe that allow to iterate through
                        offset = 0
                        complete = False
                        arr_df = pd.DataFrame(columns = ['flight_date0', 'flight_status0', 'departureairport',
                            'departuretimezone', 'departureiata', 'departureicao',
                            'departureterminal', 'departuregate', 'departuredelay',
                            'departurescheduled', 'departureestimated', 'departureactual',
                            'departureestimated_runway', 'departureactual_runway', 'arrivalairport',
                            'arrivaltimezone', 'arrivaliata', 'arrivalicao', 'arrivalterminal',
                            'arrivalgate', 'arrivalbaggage', 'arrivaldelay', 'arrivalscheduled',
                            'arrivalestimated', 'arrivalactual', 'arrivalestimated_runway',
                            'arrivalactual_runway', 'airlinename', 'airlineiata', 'airlineicao',
                            'flightnumber', 'flightiata', 'flighticao', 'flightcodeshared'], dtype = object)
                        
                        #request all flights from arrival airport until done
                        while complete == False:


                            arr_params = {'access_key' : access_key, 'arr_iata' : query_arrival_iata, 'offset' : offset}

                            arr_api_result = requests.get('http://api.aviationstack.com/v1/flights', arr_params)

                            arr_api_response = arr_api_result.json()

                            arr_api_result.close()

                            temp_df = pd.DataFrame()
                            for col in pd.DataFrame(list(arr_api_response['data'])).iloc[:, :-2]:
                                temp_df = pd.concat([temp_df, pd.DataFrame(list(pd.DataFrame(list(arr_api_response['data']))[col])).add_prefix(col)], axis = 1)

                            arr_df = arr_df.append(temp_df)

                            offset += 100

                            if len(temp_df)<100:

                                complete = True
                        
                        #pull direct alternative flights from this query if required
                        if session['daf_requested'] == True:
                            if len(directalternative.query.filter(directalternative.routeid == session['routeid']).all()) == 0: 
                                daf_df = dep_df[dep_df['arrivaliata']==query_arrival_iata].drop_duplicates(subset="flightiata")
                                #if the flight was delayed, remove all flights that arrived after the original
                                #if arrival_actual is not None:
                                    #daf_df = daf_df[daf_df['arrivalscheduled'] < arrival_actual]
                                for row in range(len(daf_df)):
                                    routeid = session['routeid']
                                    flight_date = pd.to_datetime(daf_df.iloc[row, 0])
                                    flight_status = daf_df.iloc[row, 1]
                                    departure_timezone = daf_df.iloc[row, 3]
                                    departure_airport = daf_df.iloc[row, 2]
                                    departure_iata = daf_df.iloc[row, 4]
                                    departure_icao = daf_df.iloc[row, 5]
                                    departure_terminal = daf_df.iloc[row, 6]
                                    departure_gate = daf_df.iloc[row, 7]
                                    departure_delay = daf_df.iloc[row, 8]
                                    departure_scheduled = pd.to_datetime(daf_df.iloc[row, 9])
                                    departure_estimated = pd.to_datetime(daf_df.iloc[row, 10])
                                    departure_actual = pd.to_datetime(daf_df.iloc[row, 11])
                                    departure_estimated_runway = pd.to_datetime(daf_df.iloc[row, 12])
                                    departure_actual_runway = pd.to_datetime(daf_df.iloc[row, 13])
                                    arrival_airport = daf_df.iloc[row, 14]
                                    arrival_timezone = daf_df.iloc[row, 15]
                                    arrival_iata = daf_df.iloc[row, 16]
                                    arrival_icao = daf_df.iloc[row, 17]
                                    arrival_terminal = daf_df.iloc[row, 18]
                                    arrival_gate = daf_df.iloc[row, 19]
                                    arrival_baggage = daf_df.iloc[row, 20]
                                    arrival_delay = daf_df.iloc[row, 21]
                                    arrival_scheduled = pd.to_datetime(daf_df.iloc[row, 22])
                                    arrival_estimated = pd.to_datetime(daf_df.iloc[row, 23])
                                    arrival_actual = pd.to_datetime(daf_df.iloc[row, 24])
                                    arrival_estimated_runway = pd.to_datetime(daf_df.iloc[row, 25])
                                    arrival_actual_runway = pd.to_datetime(daf_df.iloc[row, 26])
                                    airline_name = daf_df.iloc[row, 27]
                                    airline_iata = daf_df.iloc[row, 28]
                                    airline_icao = daf_df.iloc[row, 29]
                                    flight_number = daf_df.iloc[row, 30]
                                    flight_iata = daf_df.iloc[row, 31]
                                    flight_icao = daf_df.iloc[row, 32]
                                    separate_query = "False"
                                    queried_by = session['username']
                                    new_directalternative_row = directalternative(routeid=routeid, flight_date = flight_date, flight_status = flight_status, departure_timezone = departure_timezone, departure_airport = departure_airport, departure_iata = departure_iata, departure_icao = departure_icao, departure_terminal = departure_terminal, departure_gate = departure_gate, departure_delay = departure_delay, departure_scheduled = departure_scheduled, departure_estimated = departure_estimated, departure_actual = departure_actual, departure_estimated_runway = departure_estimated_runway, departure_actual_runway = departure_actual_runway, arrival_airport = arrival_airport, arrival_timezone = arrival_timezone, arrival_iata = arrival_iata, arrival_icao = arrival_icao, arrival_terminal = arrival_terminal, arrival_gate = arrival_gate, arrival_baggage = arrival_baggage, arrival_delay = arrival_delay, arrival_scheduled = arrival_scheduled, arrival_estimated = arrival_estimated, arrival_actual = arrival_actual, arrival_estimated_runway = arrival_estimated_runway, arrival_actual_runway = arrival_actual_runway, airline_name = airline_name, airline_iata = airline_iata, airline_icao = airline_icao, flight_number = flight_number, flight_iata = flight_iata, flight_icao = flight_icao, separate_query = separate_query, queried_by = queried_by)
                                    db.session.add(new_directalternative_row)
                                    db.session.commit()
                                if len(daf_df) ==0:                    
                                    routeid = session['routeid']
                                    flight_date = pd.to_datetime(0)
                                    flight_status = "NOTFOUND"
                                    departure_timezone = "NOTFOUND"
                                    departure_airport = "NOTFOUND"
                                    departure_iata = "NOTFOUND"
                                    departure_icao = "NOTFOUND"
                                    departure_terminal = "NOTFOUND"
                                    departure_gate = 0
                                    departure_delay = 0
                                    departure_scheduled = pd.to_datetime(0)
                                    departure_estimated = pd.to_datetime(0)
                                    departure_actual = pd.to_datetime(0)
                                    departure_estimated_runway = pd.to_datetime(0)
                                    departure_actual_runway = pd.to_datetime(0)
                                    arrival_airport = "NOTFOUND"
                                    arrival_timezone = "NOTFOUND"
                                    arrival_iata = "NOTFOUND"
                                    arrival_icao = "NOTFOUND"
                                    arrival_terminal = "NOTFOUND"
                                    arrival_gate = 0
                                    arrival_baggage = "NOTFOUND"
                                    arrival_delay = 0
                                    arrival_scheduled = pd.to_datetime(0)
                                    arrival_estimated = pd.to_datetime(0)
                                    arrival_actual = pd.to_datetime(0)
                                    arrival_estimated_runway = pd.to_datetime(0)
                                    arrival_actual_runway = pd.to_datetime(0)
                                    airline_name = "NOTFOUND"
                                    airline_iata = "NOTFOUND"
                                    airline_icao = "NOTFOUND"
                                    flight_number = 0
                                    flight_iata = "NOTFOUND"
                                    flight_icao = "NOTFOUND"
                                    separate_query = "False"
                                    queried_by = session['username']
                                    
                                    new_directalternative_row = directalternative(routeid=routeid, flight_date = flight_date, flight_status = flight_status, departure_timezone = departure_timezone, departure_airport = departure_airport, departure_iata = departure_iata, departure_icao = departure_icao, departure_terminal = departure_terminal, departure_gate = departure_gate, departure_delay = departure_delay, departure_scheduled = departure_scheduled, departure_estimated = departure_estimated, departure_actual = departure_actual, departure_estimated_runway = departure_estimated_runway, departure_actual_runway = departure_actual_runway, arrival_airport = arrival_airport, arrival_timezone = arrival_timezone, arrival_iata = arrival_iata, arrival_icao = arrival_icao, arrival_terminal = arrival_terminal, arrival_gate = arrival_gate, arrival_baggage = arrival_baggage, arrival_delay = arrival_delay, arrival_scheduled = arrival_scheduled, arrival_estimated = arrival_estimated, arrival_actual = arrival_actual, arrival_estimated_runway = arrival_estimated_runway, arrival_actual_runway = arrival_actual_runway, airline_name = airline_name, airline_iata = airline_iata, airline_icao = airline_icao, flight_number = flight_number, flight_iata = flight_iata, flight_icao = flight_icao, separate_query = separate_query, queried_by = queried_by)
                                    db.session.add(new_directalternative_row)
                                    db.session.commit()

                        
                        #join the departures and arrivals dataframes and convert dates to datetime format
                        potential_alternative_routes = pd.merge(dep_df, arr_df, left_on = 'arrivaliata', right_on = 'departureiata', suffixes = ('_flight_1', '_flight_2'))
                        potential_alternative_routes['arrivalscheduled_flight_1'] = pd.to_datetime(potential_alternative_routes['arrivalscheduled_flight_1'])
                        potential_alternative_routes['departurescheduled_flight_2'] = pd.to_datetime(potential_alternative_routes['departurescheduled_flight_2'])
                        potential_alternative_routes['arrivalscheduled_flight_2'] =  pd.to_datetime(potential_alternative_routes['arrivalscheduled_flight_2'])

                        # filter where: - HAS BEEN REMOVED TO BE DONE AT THE FILTER STAGE
                        #     -both flights operated
                        #     -flight 2 lands after flight one with a buffer (30 mins)?
                        #     -any more?
                        #buffer_between_flights = 30
                        indirect_alternatives = indirect_alternatives.dropna(subset = ["flightiata_flight_1", "flightiata_flight_2"])

                        #if the initial flight was delayed, alternative route must land before the delayed flight
                        #if flight_info_df['arrivalactual'][0] is not None:
                            #indirect_alternatives = indirect_alternatives[indirect_alternatives['arrivalscheduled_flight_2'] < flight_info_df['arrivalactual'][0]]

                        for row in range(len(indirect_alternatives)):
                            routeid = session['routeid']
                            f1_flight_date = pd.to_datetime(indirect_alternatives.iloc[row, 0])
                            f1_flight_status = indirect_alternatives.iloc[row, 1]
                            f1_departure_timezone = indirect_alternatives.iloc[row, 3]
                            f1_departure_airport = indirect_alternatives.iloc[row, 2]
                            f1_departure_iata = indirect_alternatives.iloc[row, 4]
                            f1_departure_icao = indirect_alternatives.iloc[row, 5]
                            f1_departure_terminal = indirect_alternatives.iloc[row, 6]
                            f1_departure_gate = indirect_alternatives.iloc[row, 7]
                            f1_departure_delay = indirect_alternatives.iloc[row, 8]
                            f1_departure_scheduled = pd.to_datetime(indirect_alternatives.iloc[row, 9])
                            f1_departure_estimated = pd.to_datetime(indirect_alternatives.iloc[row, 10])
                            f1_departure_actual = pd.to_datetime(indirect_alternatives.iloc[row, 11])
                            f1_departure_estimated_runway = pd.to_datetime(indirect_alternatives.iloc[row, 12])
                            f1_departure_actual_runway = pd.to_datetime(indirect_alternatives.iloc[row, 13])
                            f1_arrival_airport = indirect_alternatives.iloc[row, 14]
                            f1_arrival_timezone = indirect_alternatives.iloc[row, 15]
                            f1_arrival_iata = indirect_alternatives.iloc[row, 16]
                            f1_arrival_icao = indirect_alternatives.iloc[row, 17]
                            f1_arrival_terminal = indirect_alternatives.iloc[row, 18]
                            f1_arrival_gate = indirect_alternatives.iloc[row, 19]
                            f1_arrival_baggage = indirect_alternatives.iloc[row, 20]
                            f1_arrival_delay = indirect_alternatives.iloc[row, 21]
                            f1_arrival_scheduled = pd.to_datetime(indirect_alternatives.iloc[row, 22])
                            f1_arrival_estimated = pd.to_datetime(indirect_alternatives.iloc[row, 23])
                            f1_arrival_actual = pd.to_datetime(indirect_alternatives.iloc[row, 24])
                            f1_arrival_estimated_runway = pd.to_datetime(indirect_alternatives.iloc[row, 25])
                            f1_arrival_actual_runway = pd.to_datetime(indirect_alternatives.iloc[row, 26])
                            f1_airline_name = indirect_alternatives.iloc[row, 27]
                            f1_airline_iata = indirect_alternatives.iloc[row, 28]
                            f1_airline_icao = indirect_alternatives.iloc[row, 29]
                            f1_flight_number = indirect_alternatives.iloc[row, 30]
                            f1_flight_iata = indirect_alternatives.iloc[row, 31]
                            f1_flight_icao = indirect_alternatives.iloc[row, 32]
                            f2_flight_date = pd.to_datetime(indirect_alternatives.iloc[row, 33])
                            f2_flight_status = indirect_alternatives.iloc[row, 34]
                            f2_departure_timezone = indirect_alternatives.iloc[row, 35]
                            f2_departure_airport = indirect_alternatives.iloc[row, 36]
                            f2_departure_iata = indirect_alternatives.iloc[row, 37]
                            f2_departure_icao = indirect_alternatives.iloc[row, 38]
                            f2_departure_terminal = indirect_alternatives.iloc[row, 39]
                            f2_departure_gate = indirect_alternatives.iloc[row, 40]
                            f2_departure_delay = indirect_alternatives.iloc[row, 41]
                            f2_departure_scheduled = pd.to_datetime(indirect_alternatives.iloc[row, 42])
                            f2_departure_estimated = pd.to_datetime(indirect_alternatives.iloc[row, 43])
                            f2_departure_actual = pd.to_datetime(indirect_alternatives.iloc[row, 44])
                            f2_departure_estimated_runway = pd.to_datetime(indirect_alternatives.iloc[row, 45])
                            f2_departure_actual_runway = pd.to_datetime(indirect_alternatives.iloc[row, 46])
                            f2_arrival_airport = indirect_alternatives.iloc[row, 47]
                            f2_arrival_timezone = indirect_alternatives.iloc[row, 48]
                            f2_arrival_iata = indirect_alternatives.iloc[row, 49]
                            f2_arrival_icao = indirect_alternatives.iloc[row, 50]
                            f2_arrival_terminal = indirect_alternatives.iloc[row, 51]
                            f2_arrival_gate = indirect_alternatives.iloc[row, 52]
                            f2_arrival_baggage = indirect_alternatives.iloc[row, 53]
                            f2_arrival_delay = indirect_alternatives.iloc[row, 54]
                            f2_arrival_scheduled = pd.to_datetime(indirect_alternatives.iloc[row, 55])
                            f2_arrival_estimated = pd.to_datetime(indirect_alternatives.iloc[row, 56])
                            f2_arrival_actual = pd.to_datetime(indirect_alternatives.iloc[row, 57])
                            f2_arrival_estimated_runway = pd.to_datetime(indirect_alternatives.iloc[row, 58])
                            f2_arrival_actual_runway = pd.to_datetime(indirect_alternatives.iloc[row, 59])
                            f2_airline_name = indirect_alternatives.iloc[row, 60]
                            f2_airline_iata = indirect_alternatives.iloc[row, 61]
                            f2_airline_icao = indirect_alternatives.iloc[row, 62]
                            f2_flight_number = indirect_alternatives.iloc[row, 63]
                            f2_flight_iata = indirect_alternatives.iloc[row, 64]
                            f2_flight_icao = indirect_alternatives.iloc[row, 65]
                            queried_by = session['username']
                            date_created = datetime.utcnow()
                            new_indirectalternative_row = indirectalternative(routeid=routeid, f1_flight_date = f1_flight_date, f1_flight_status = f1_flight_status, f1_departure_timezone = f1_departure_timezone, f1_departure_airport = f1_departure_airport, f1_departure_iata = f1_departure_iata, f1_departure_icao = f1_departure_icao, f1_departure_terminal = f1_departure_terminal, f1_departure_gate = f1_departure_gate, f1_departure_delay = f1_departure_delay, f1_departure_scheduled = f1_departure_scheduled, f1_departure_estimated = f1_departure_estimated, f1_departure_actual = f1_departure_actual, f1_departure_estimated_runway = f1_departure_estimated_runway, f1_departure_actual_runway = f1_departure_actual_runway, f1_arrival_airport = f1_arrival_airport, f1_arrival_timezone = f1_arrival_timezone, f1_arrival_iata = f1_arrival_iata, f1_arrival_icao = f1_arrival_icao, f1_arrival_terminal = f1_arrival_terminal, f1_arrival_gate = f1_arrival_gate, f1_arrival_baggage = f1_arrival_baggage, f1_arrival_delay = f1_arrival_delay, f1_arrival_scheduled = f1_arrival_scheduled, f1_arrival_estimated = f1_arrival_estimated, f1_arrival_actual = f1_arrival_actual, f1_arrival_estimated_runway = f1_arrival_estimated_runway, f1_arrival_actual_runway = f1_arrival_actual_runway, f1_airline_name = f1_airline_name, f1_airline_iata = f1_airline_iata, f1_airline_icao = f1_airline_icao, f1_flight_number = f1_flight_number, f1_flight_iata = f1_flight_iata, f1_flight_icao = f1_flight_icao, f2_flight_date = f2_flight_date, f2_flight_status = f2_flight_status, f2_departure_timezone = f2_departure_timezone, f2_departure_airport = f2_departure_airport, f2_departure_iata = f2_departure_iata, f2_departure_icao = f2_departure_icao, f2_departure_terminal = f2_departure_terminal, f2_departure_gate = f2_departure_gate, f2_departure_delay = f2_departure_delay, f2_departure_scheduled = f2_departure_scheduled, f2_departure_estimated = f2_departure_estimated, f2_departure_actual = f2_departure_actual, f2_departure_estimated_runway = f2_departure_estimated_runway, f2_departure_actual_runway = f2_departure_actual_runway, f2_arrival_airport = f2_arrival_airport, f2_arrival_timezone = f2_arrival_timezone, f2_arrival_iata = f2_arrival_iata, f2_arrival_icao = f2_arrival_icao, f2_arrival_terminal = f2_arrival_terminal, f2_arrival_gate = f2_arrival_gate, f2_arrival_baggage = f2_arrival_baggage, f2_arrival_delay = f2_arrival_delay, f2_arrival_scheduled = f2_arrival_scheduled, f2_arrival_estimated = f2_arrival_estimated, f2_arrival_actual = f2_arrival_actual, f2_arrival_estimated_runway = f2_arrival_estimated_runway, f2_arrival_actual_runway = f2_arrival_actual_runway, f2_airline_name = f2_airline_name, f2_airline_iata = f2_airline_iata, f2_airline_icao = f2_airline_icao, f2_flight_number = f2_flight_number, f2_flight_iata = f2_flight_iata, f2_flight_icao = f2_flight_icao, queried_by = queried_by)
                            db.session.add(new_indirectalternative_row)
                            db.session.commit()

                        if len(indirect_alternatives) == 0:
                            routeid = session['routeid']
                            f1_flight_date = pd.to_datetime(0)
                            f1_flight_status = "NOT FOUND"
                            f1_departure_timezone = "NOT FOUND"
                            f1_departure_airport = "NOT FOUND"
                            f1_departure_iata = "NOT FOUND"
                            f1_departure_icao = "NOT FOUND"
                            f1_departure_terminal = "NOT FOUND"
                            f1_departure_gate = 0
                            f1_departure_delay = 0
                            f1_departure_scheduled = pd.to_datetime(0)
                            f1_departure_estimated = pd.to_datetime(0)
                            f1_departure_actual = pd.to_datetime(0)
                            f1_departure_estimated_runway = pd.to_datetime(0)
                            f1_departure_actual_runway = pd.to_datetime(0)
                            f1_arrival_airport = "NOT FOUND"
                            f1_arrival_timezone = "NOT FOUND"
                            f1_arrival_iata = "NOT FOUND"
                            f1_arrival_icao = "NOT FOUND"
                            f1_arrival_terminal = "NOT FOUND"
                            f1_arrival_gate = 0
                            f1_arrival_baggage = "NOT FOUND"
                            f1_arrival_delay = 0
                            f1_arrival_scheduled = pd.to_datetime(0)
                            f1_arrival_estimated = pd.to_datetime(0)
                            f1_arrival_actual = pd.to_datetime(0)
                            f1_arrival_estimated_runway = pd.to_datetime(0)
                            f1_arrival_actual_runway = pd.to_datetime(0)
                            f1_airline_name = "NOT FOUND"
                            f1_airline_iata = "NOT FOUND"
                            f1_airline_icao = "NOT FOUND"
                            f1_flight_number = 0
                            f1_flight_iata = "NOT FOUND"
                            f1_flight_icao = "NOT FOUND"
                            f2_flight_date = pd.to_datetime(0)
                            f2_flight_status = "NOT FOUND"
                            f2_departure_timezone = "NOT FOUND"
                            f2_departure_airport = "NOT FOUND"
                            f2_departure_iata = "NOT FOUND"
                            f2_departure_icao = "NOT FOUND"
                            f2_departure_terminal = "NOT FOUND"
                            f2_departure_gate = 0
                            f2_departure_delay = 0
                            f2_departure_scheduled = pd.to_datetime(0)
                            f2_departure_estimated = pd.to_datetime(0)
                            f2_departure_actual = pd.to_datetime(0)
                            f2_departure_estimated_runway = pd.to_datetime(0)
                            f2_departure_actual_runway = pd.to_datetime(0)
                            f2_arrival_airport = "NOT FOUND"
                            f2_arrival_timezone = "NOT FOUND"
                            f2_arrival_iata = "NOT FOUND"
                            f2_arrival_icao = "NOT FOUND"
                            f2_arrival_terminal = "NOT FOUND"
                            f2_arrival_gate = 0
                            f2_arrival_baggage = "NOT FOUND"
                            f2_arrival_delay = 0
                            f2_arrival_scheduled = pd.to_datetime(0)
                            f2_arrival_estimated = pd.to_datetime(0)
                            f2_arrival_actual = pd.to_datetime(0)
                            f2_arrival_estimated_runway = pd.to_datetime(0)
                            f2_arrival_actual_runway = pd.to_datetime(0)
                            f2_airline_name = "NOT FOUND"
                            f2_airline_iata = "NOT FOUND"
                            f2_airline_icao = "NOT FOUND"
                            f2_flight_number = 0
                            f2_flight_iata = "NOT FOUND"
                            f2_flight_icao = "NOT FOUND"
                            queried_by = session['username']
                            date_created = datetime.utcnow()
                            new_indirectalternative_row = indirectalternative(routeid=routeid, f1_flight_date = f1_flight_date, f1_flight_status = f1_flight_status, f1_departure_timezone = f1_departure_timezone, f1_departure_airport = f1_departure_airport, f1_departure_iata = f1_departure_iata, f1_departure_icao = f1_departure_icao, f1_departure_terminal = f1_departure_terminal, f1_departure_gate = f1_departure_gate, f1_departure_delay = f1_departure_delay, f1_departure_scheduled = f1_departure_scheduled, f1_departure_estimated = f1_departure_estimated, f1_departure_actual = f1_departure_actual, f1_departure_estimated_runway = f1_departure_estimated_runway, f1_departure_actual_runway = f1_departure_actual_runway, f1_arrival_airport = f1_arrival_airport, f1_arrival_timezone = f1_arrival_timezone, f1_arrival_iata = f1_arrival_iata, f1_arrival_icao = f1_arrival_icao, f1_arrival_terminal = f1_arrival_terminal, f1_arrival_gate = f1_arrival_gate, f1_arrival_baggage = f1_arrival_baggage, f1_arrival_delay = f1_arrival_delay, f1_arrival_scheduled = f1_arrival_scheduled, f1_arrival_estimated = f1_arrival_estimated, f1_arrival_actual = f1_arrival_actual, f1_arrival_estimated_runway = f1_arrival_estimated_runway, f1_arrival_actual_runway = f1_arrival_actual_runway, f1_airline_name = f1_airline_name, f1_airline_iata = f1_airline_iata, f1_airline_icao = f1_airline_icao, f1_flight_number = f1_flight_number, f1_flight_iata = f1_flight_iata, f1_flight_icao = f1_flight_icao, f2_flight_date = f2_flight_date, f2_flight_status = f2_flight_status, f2_departure_timezone = f2_departure_timezone, f2_departure_airport = f2_departure_airport, f2_departure_iata = f2_departure_iata, f2_departure_icao = f2_departure_icao, f2_departure_terminal = f2_departure_terminal, f2_departure_gate = f2_departure_gate, f2_departure_delay = f2_departure_delay, f2_departure_scheduled = f2_departure_scheduled, f2_departure_estimated = f2_departure_estimated, f2_departure_actual = f2_departure_actual, f2_departure_estimated_runway = f2_departure_estimated_runway, f2_departure_actual_runway = f2_departure_actual_runway, f2_arrival_airport = f2_arrival_airport, f2_arrival_timezone = f2_arrival_timezone, f2_arrival_iata = f2_arrival_iata, f2_arrival_icao = f2_arrival_icao, f2_arrival_terminal = f2_arrival_terminal, f2_arrival_gate = f2_arrival_gate, f2_arrival_baggage = f2_arrival_baggage, f2_arrival_delay = f2_arrival_delay, f2_arrival_scheduled = f2_arrival_scheduled, f2_arrival_estimated = f2_arrival_estimated, f2_arrival_actual = f2_arrival_actual, f2_arrival_estimated_runway = f2_arrival_estimated_runway, f2_arrival_actual_runway = f2_arrival_actual_runway, f2_airline_name = f2_airline_name, f2_airline_iata = f2_airline_iata, f2_airline_icao = f2_airline_icao, f2_flight_number = f2_flight_number, f2_flight_iata = f2_flight_iata, f2_flight_icao = f2_flight_icao, queried_by = queried_by)
                            db.session.add(new_indirectalternative_row)
                            db.session.commit()

                        #Get alternative departure and arrival airport options#
                        #get the closest 5 airports to departure (euclidean but hopefully accurate enough)
                        array = np.array(airports.iloc[:, 4:6])
                        coords = np.array([(airports[airports['iata_code']==departure_iata].iloc[:,4], airports[airports['iata_code']==departure_iata].iloc[:,5])])[0]
                        coords.shape = (1, 2)
                        distances = np.linalg.norm(array-coords, axis=1)
                        idx = np.argpartition(distances.ravel(), 6)
                        nearest_5 = np.array(np.unravel_index(idx, distances.shape))[:, range(6)].transpose().tolist()

                        nearest_5_dep_df = pd.DataFrame(columns = ["Iata_code", "Distance", "Drive_time"])

                        for i in nearest_5[1:5]:
                            row = []
                            row.append(airports.iloc[i, 13].values[0])
                            
                            dist, eu261 = greatcirclecalc(departure_iata,airports.iloc[i, 13].values[0])
                            
                            row.append(dist)
                            
                            deplat = airports[airports['iata_code']==departure_iata].iloc[:,4]
                            deplong = airports[airports['iata_code']==departure_iata].iloc[:,5]
                            arrlat = airports[airports['iata_code']==arrival_iata].iloc[:,4]
                            arrlong = airports[airports['iata_code']==arrival_iata].iloc[:,5]
                            altdeplat = airports.iloc[i, 4]
                            altdeplong = airports.iloc[i, 5]
                            travel_mode = "Driving"
                            query_url = r"http://dev.virtualearth.net/REST/v1/Routes/" + travel_mode + "?o=json&wp.0=" + str(deplat.values[0]) + "," + str(deplong.values[0]) +"&wp.1=" + str(altdeplat.values[0]) + "," + str(altdeplong.values[0]) + "&key=AghfZWXQ0nEiSbp6zjT2N3OUppVxXwv1dmUb3gfcvimiYhAqzhNvqXVAIyy_7Rk7"
                            response = requests.get(query_url)
                            response.close()
                            df = pd.DataFrame(response.json()['resourceSets'])
                            df = pd.DataFrame(df['resources'][0])['travelDuration'][0]
                            row.append(df)
                            nearest_5_dep_df.loc[len(nearest_5_dep_df)] = row



                        #get the closest 5 airports to arrival (euclidean but hopefully accurate enough)
                        array = np.array(airports.iloc[:, 4:6])
                        coords = np.array([(airports[airports['iata_code']==arrival_iata].iloc[:,4], airports[airports['iata_code']==arrival_iata].iloc[:,5])])[0]
                        coords.shape = (1, 2)
                        distances = np.linalg.norm(array-coords, axis=1)
                        idx = np.argpartition(distances.ravel(), 6)
                        nearest_5 = np.array(np.unravel_index(idx, distances.shape))[:, range(6)].transpose().tolist()

                        nearest_5_arr_df = pd.DataFrame(columns = ["Iata_code", "Distance", "Drive_time"])

                        for i in nearest_5[1:5]:
                            row = []
                            row.append(airports.iloc[i, 13].values[0])
                            
                            dist, eu261 = greatcirclecalc(arrival_iata,airports.iloc[i, 13].values[0])
                            
                            row.append(dist)
                            
                            deplat = airports[airports['iata_code']==departure_iata].iloc[:,4]
                            deplong = airports[airports['iata_code']==departure_iata].iloc[:,5]
                            arrlat = airports[airports['iata_code']==arrival_iata].iloc[:,4]
                            arrlong = airports[airports['iata_code']==arrival_iata].iloc[:,5]
                            altarrlat = airports.iloc[i, 4]
                            altarrlong = airports.iloc[i, 5]
                            travel_mode = "Driving"
                            query_url = r"http://dev.virtualearth.net/REST/v1/Routes/" + travel_mode + "?o=json&wp.0=" + str(arrlat.values[0]) + "," + str(arrlong.values[0]) +"&wp.1=" + str(altarrlat.values[0]) + "," + str(altarrlong.values[0]) + "&key=AghfZWXQ0nEiSbp6zjT2N3OUppVxXwv1dmUb3gfcvimiYhAqzhNvqXVAIyy_7Rk7"
                            response = requests.get(query_url)
                            response.close()
                            df = pd.DataFrame(response.json()['resourceSets'])
                            df = pd.DataFrame(df['resources'][0])['travelDuration'][0]
                            row.append(df)
                            nearest_5_arr_df.loc[len(nearest_5_arr_df)] = row


                        
                    db.session.add(new_flight_row)
                    db.session.commit()

        #if a pdf has been requested
        elif request.form['btn'] == "Generate pdf of this page":
            string_template = session['print_template']
            pdf = pdfkit.from_string(string_template, False, configuration= config, css = css)
            file = io.BytesIO(pdf)
            return send_file(file, attachment_filename="test.pdf", mimetype='application/pdf', as_attachment=True, cache_timeout=-1)

        #if alternative flights to add
        elif request.form['btn'] == "Add Alternative airports":
            query_departure_iata = session['routeid'][:3]
            query_arrival_iata = session['routeid'][3:6]

            session['alts_added'] = True
            #check which forms have been ticked + create list
            try:
                daf_checkbox = request.form['altdep1']
                session['altdepsrequested'].append(session['altairports'][0])
            except:
                pass
            try:
                daf_checkbox = request.form['altdep2']
                session['altdepsrequested'].append(session['altairports'][1])
            except:
                pass
            try:
                daf_checkbox = request.form['altdep3']
                session['altdepsrequested'].append(session['altairports'][2])
            except:
                pass
            try:
                daf_checkbox = request.form['altdep4']
                session['altdepsrequested'].append(session['altairports'][3])
            except:
                pass
            try:
                daf_checkbox = request.form['altarr1']
                session['altarrsrequested'].append(session['altairports'][4])
            except:
                pass
            try:
                daf_checkbox = request.form['altarr2']
                session['altarrsrequested'].append(session['altairports'][5])
            except:
                pass
            try:
                daf_checkbox = request.form['altarr3']
                session['altarrsrequested'].append(session['altairports'][6])
            except:
                pass
            try:
                daf_checkbox = request.form['altarr4']
                session['altarrsrequested'].append(session['altairports'][7])
            except:
                pass

            #iterate through altdeps and altarrs to get routeids + find those routes (copied  from 'find' with session['routeid'] replaced with temprouteid for each - these are appended to a list to be queried later)
            for dep in session['altdepsrequested']:
                for arr in session['altarrsrequested']:
                    temprouteid = dep + arr + str(session['requested_flight_date'])
                    session['altroutesrequested'].append(temprouteid)
                    #copied from find query

                #if just direct alternatives that must be acquired:
                    if ((session['daf_requested'] == True) and (session['iaf_requested'] == False)) or ((session['daf_requested'] == True) and (session['iaf_requested'] == True) and (len(indirectalternative.query.filter(indirectalternative.routeid == temprouteid).all()) != 0)):
                        #if the routeid does not already exist in the db (if it does, no action as it will just be queried at the end)
                        if len(directalternative.query.filter(directalternative.routeid == temprouteid).all()) == 0:
                            #initialise some variables and a dataframe to collect all the query results
                            offset = 0
                            complete = False
                            daf_df = pd.DataFrame(columns = ['flight_date0', 'flight_status0', 'departureairport', 'departuretimezone', 'departureiata', 'departureicao', 'departureterminal', 'departuregate', 'departuredelay', 'departurescheduled', 'departureestimated', 'departureactual', 'departureestimated_runway', 'departureactual_runway', 'arrivalairport', 'arrivaltimezone', 'arrivaliata', 'arrivalicao', 'arrivalterminal', 'arrivalgate', 'arrivalbaggage', 'arrivaldelay', 'arrivalscheduled', 'arrivalestimated', 'arrivalactual', 'arrivalestimated_runway', 'arrivalactual_runway', 'airlinename', 'airlineiata', 'airlineicao', 'flightnumber', 'flightiata', 'flighticao', 'flightcodeshared'])
                            #query the api and collect all results
                            while complete == False:
                                daf_params = {'access_key' : access_key, 'dep_iata' : dep, 'arr_iata' : arr, 'offset' : offset}
                                daf_api_result = requests.get('http://api.aviationstack.com/v1/flights', daf_params)
                                daf_api_response = daf_api_result.json()
                                daf_api_result.close()

                                #sort the data and add it to a dataframe
                                temp_df = pd.DataFrame()
                                for col in pd.DataFrame(list(daf_api_response['data'])).iloc[:,:-2]:
                                    temp_df = pd.concat([temp_df, pd.DataFrame(list(pd.DataFrame(list(daf_api_response['data']))[col])).add_prefix(col)], axis = 1)

                                daf_df = daf_df.append(temp_df)
                                offset += 100

                                if len(temp_df)<100:
                                    complete = True
                            #drop any duplicates
                            daf_df = daf_df.drop_duplicates(subset="flightiata")
                            


                            for row in range(len(daf_df)):

                                routeid = temprouteid
                                flight_date = pd.to_datetime(daf_df.iloc[row, 0])
                                flight_status = daf_df.iloc[row, 1]
                                departure_timezone = daf_df.iloc[row, 3]
                                departure_airport = daf_df.iloc[row, 2]
                                departure_iata = daf_df.iloc[row, 4]
                                departure_icao = daf_df.iloc[row, 5]
                                departure_terminal = daf_df.iloc[row, 6]
                                departure_gate = daf_df.iloc[row, 7]
                                departure_delay = daf_df.iloc[row, 8]
                                departure_scheduled = pd.to_datetime(daf_df.iloc[row, 9])
                                departure_estimated = pd.to_datetime(daf_df.iloc[row, 10])
                                departure_actual = pd.to_datetime(daf_df.iloc[row, 11])
                                departure_estimated_runway = pd.to_datetime(daf_df.iloc[row, 12])
                                departure_actual_runway = pd.to_datetime(daf_df.iloc[row, 13])
                                arrival_airport = daf_df.iloc[row, 14]
                                arrival_timezone = daf_df.iloc[row, 15]
                                arrival_iata = daf_df.iloc[row, 16]
                                arrival_icao = daf_df.iloc[row, 17]
                                arrival_terminal = daf_df.iloc[row, 18]
                                arrival_gate = daf_df.iloc[row, 19]
                                arrival_baggage = daf_df.iloc[row, 20]
                                arrival_delay = daf_df.iloc[row, 21]
                                arrival_scheduled = pd.to_datetime(daf_df.iloc[row, 22])
                                arrival_estimated = pd.to_datetime(daf_df.iloc[row, 23])
                                arrival_actual = pd.to_datetime(daf_df.iloc[row, 24])
                                arrival_estimated_runway = pd.to_datetime(daf_df.iloc[row, 25])
                                arrival_actual_runway = pd.to_datetime(daf_df.iloc[row, 26])
                                airline_name = daf_df.iloc[row, 27]
                                airline_iata = daf_df.iloc[row, 28]
                                airline_icao = daf_df.iloc[row, 29]
                                flight_number = daf_df.iloc[row, 30]
                                flight_iata = daf_df.iloc[row, 31]
                                flight_icao = daf_df.iloc[row, 32]
                                separate_query = "True"
                                queried_by = session['username']
                                new_directalternative_row = directalternative(routeid=routeid, flight_date = flight_date, flight_status = flight_status, departure_timezone = departure_timezone, departure_airport = departure_airport, departure_iata = departure_iata, departure_icao = departure_icao, departure_terminal = departure_terminal, departure_gate = departure_gate, departure_delay = departure_delay, departure_scheduled = departure_scheduled, departure_estimated = departure_estimated, departure_actual = departure_actual, departure_estimated_runway = departure_estimated_runway, departure_actual_runway = departure_actual_runway, arrival_airport = arrival_airport, arrival_timezone = arrival_timezone, arrival_iata = arrival_iata, arrival_icao = arrival_icao, arrival_terminal = arrival_terminal, arrival_gate = arrival_gate, arrival_baggage = arrival_baggage, arrival_delay = arrival_delay, arrival_scheduled = arrival_scheduled, arrival_estimated = arrival_estimated, arrival_actual = arrival_actual, arrival_estimated_runway = arrival_estimated_runway, arrival_actual_runway = arrival_actual_runway, airline_name = airline_name, airline_iata = airline_iata, airline_icao = airline_icao, flight_number = flight_number, flight_iata = flight_iata, flight_icao = flight_icao, separate_query = separate_query, queried_by = queried_by)
                                db.session.add(new_directalternative_row)
                                db.session.commit()

                            if len(daf_df) == 0:
                                routeid = temprouteid
                                flight_date = pd.to_datetime(0)
                                flight_status = "NOTFOUND"
                                departure_timezone = "NOTFOUND"
                                departure_airport = "NOTFOUND"
                                departure_iata = "NOTFOUND"
                                departure_icao = "NOTFOUND"
                                departure_terminal = "NOTFOUND"
                                departure_gate = 0
                                departure_delay = 0
                                departure_scheduled = pd.to_datetime(0)
                                departure_estimated = pd.to_datetime(0)
                                departure_actual = pd.to_datetime(0)
                                departure_estimated_runway = pd.to_datetime(0)
                                departure_actual_runway = pd.to_datetime(0)
                                arrival_airport = "NOTFOUND"
                                arrival_timezone = "NOTFOUND"
                                arrival_iata = "NOTFOUND"
                                arrival_icao = "NOTFOUND"
                                arrival_terminal = "NOTFOUND"
                                arrival_gate = 0
                                arrival_baggage = "NOTFOUND"
                                arrival_delay = 0
                                arrival_scheduled = pd.to_datetime(0)
                                arrival_estimated = pd.to_datetime(0)
                                arrival_actual = pd.to_datetime(0)
                                arrival_estimated_runway = pd.to_datetime(0)
                                arrival_actual_runway = pd.to_datetime(0)
                                airline_name = "NOTFOUND"
                                airline_iata = "NOTFOUND"
                                airline_icao = "NOTFOUND"
                                flight_number = 0
                                flight_iata = "NOTFOUND"
                                flight_icao = "NOTFOUND"
                                separate_query = "True"
                                queried_by = session['username']
                                
                                new_directalternative_row = directalternative(routeid=routeid, flight_date = flight_date, flight_status = flight_status, departure_timezone = departure_timezone, departure_airport = departure_airport, departure_iata = departure_iata, departure_icao = departure_icao, departure_terminal = departure_terminal, departure_gate = departure_gate, departure_delay = departure_delay, departure_scheduled = departure_scheduled, departure_estimated = departure_estimated, departure_actual = departure_actual, departure_estimated_runway = departure_estimated_runway, departure_actual_runway = departure_actual_runway, arrival_airport = arrival_airport, arrival_timezone = arrival_timezone, arrival_iata = arrival_iata, arrival_icao = arrival_icao, arrival_terminal = arrival_terminal, arrival_gate = arrival_gate, arrival_baggage = arrival_baggage, arrival_delay = arrival_delay, arrival_scheduled = arrival_scheduled, arrival_estimated = arrival_estimated, arrival_actual = arrival_actual, arrival_estimated_runway = arrival_estimated_runway, arrival_actual_runway = arrival_actual_runway, airline_name = airline_name, airline_iata = airline_iata, airline_icao = airline_icao, flight_number = flight_number, flight_iata = flight_iata, flight_icao = flight_icao, separate_query = separate_query, queried_by = queried_by)
                                db.session.add(new_directalternative_row)
                                db.session.commit()

                    #if indirect alternatives have been requested
                    if session['iaf_requested'] == True:

                        #if the routeid does not already exist in the db
                        if len(indirectalternative.query.filter(indirectalternative.routeid == temprouteid).all()) == 0:                
                            #initialise variables and dataframe that allow to iterate through
                            offset = 0
                            complete = False

                            dep_df = pd.DataFrame(columns = ['flight_date0', 'flight_status0', 'departureairport',
                        'departuretimezone', 'departureiata', 'departureicao',
                        'departureterminal', 'departuregate', 'departuredelay',
                        'departurescheduled', 'departureestimated', 'departureactual',
                        'departureestimated_runway', 'departureactual_runway', 'arrivalairport',
                        'arrivaltimezone', 'arrivaliata', 'arrivalicao', 'arrivalterminal',
                        'arrivalgate', 'arrivalbaggage', 'arrivaldelay', 'arrivalscheduled',
                        'arrivalestimated', 'arrivalactual', 'arrivalestimated_runway',
                        'arrivalactual_runway', 'airlinename', 'airlineiata', 'airlineicao',
                        'flightnumber', 'flightiata', 'flighticao', 'flightcodeshared'], dtype = object)

                            #request all flights from departure airport until done
                            while complete == False:


                                dep_params = {'access_key' : access_key, 'dep_iata' : dep, 'offset' : offset}

                                dep_api_result = requests.get('http://api.aviationstack.com/v1/flights', dep_params)

                                dep_api_response = dep_api_result.json()

                                dep_api_result.close()

                                temp_df = pd.DataFrame()
                                for col in pd.DataFrame(list(dep_api_response['data'])).iloc[:,:-2]:
                                    temp_df = pd.concat([temp_df, pd.DataFrame(list(pd.DataFrame(list(dep_api_response['data']))[col])).add_prefix(col)], axis = 1)

                                dep_df = dep_df.append(temp_df)

                                offset += 100

                                if len(temp_df)<100:

                                    complete = True



                            #query all arriving flights at destination airport

                            #initialise variables and a dataframe that allow to iterate through
                            offset = 0
                            complete = False
                            arr_df = pd.DataFrame(columns = ['flight_date0', 'flight_status0', 'departureairport',
                                'departuretimezone', 'departureiata', 'departureicao',
                                'departureterminal', 'departuregate', 'departuredelay',
                                'departurescheduled', 'departureestimated', 'departureactual',
                                'departureestimated_runway', 'departureactual_runway', 'arrivalairport',
                                'arrivaltimezone', 'arrivaliata', 'arrivalicao', 'arrivalterminal',
                                'arrivalgate', 'arrivalbaggage', 'arrivaldelay', 'arrivalscheduled',
                                'arrivalestimated', 'arrivalactual', 'arrivalestimated_runway',
                                'arrivalactual_runway', 'airlinename', 'airlineiata', 'airlineicao',
                                'flightnumber', 'flightiata', 'flighticao', 'flightcodeshared'], dtype = object)
                            
                            #request all flights from arrival airport until done
                            while complete == False:


                                arr_params = {'access_key' : access_key, 'arr_iata' : arr, 'offset' : offset}

                                arr_api_result = requests.get('http://api.aviationstack.com/v1/flights', arr_params)

                                arr_api_response = arr_api_result.json()

                                arr_api_result.close()

                                temp_df = pd.DataFrame()
                                for col in pd.DataFrame(list(arr_api_response['data'])).iloc[:, :-2]:
                                    temp_df = pd.concat([temp_df, pd.DataFrame(list(pd.DataFrame(list(arr_api_response['data']))[col])).add_prefix(col)], axis = 1)

                                arr_df = arr_df.append(temp_df)

                                offset += 100

                                if len(temp_df)<100:

                                    complete = True
                            
                            #pull direct alternative flights from this query if required
                            if session['daf_requested'] == True:
                                if len(directalternative.query.filter(directalternative.routeid == temprouteid).all()) == 0: 
                                    daf_df = dep_df[dep_df['arrivaliata']==query_arrival_iata].drop_duplicates(subset="flightiata")
                                    #if the flight was delayed, remove all flights that arrived after the original
                                    #if arrival_actual is not None:
                                        #daf_df = daf_df[daf_df['arrivalscheduled'] < arrival_actual]
                                    for row in range(len(daf_df)):
                                        routeid = temprouteid
                                        flight_date = pd.to_datetime(daf_df.iloc[row, 0])
                                        flight_status = daf_df.iloc[row, 1]
                                        departure_timezone = daf_df.iloc[row, 3]
                                        departure_airport = daf_df.iloc[row, 2]
                                        departure_iata = daf_df.iloc[row, 4]
                                        departure_icao = daf_df.iloc[row, 5]
                                        departure_terminal = daf_df.iloc[row, 6]
                                        departure_gate = daf_df.iloc[row, 7]
                                        departure_delay = daf_df.iloc[row, 8]
                                        departure_scheduled = pd.to_datetime(daf_df.iloc[row, 9])
                                        departure_estimated = pd.to_datetime(daf_df.iloc[row, 10])
                                        departure_actual = pd.to_datetime(daf_df.iloc[row, 11])
                                        departure_estimated_runway = pd.to_datetime(daf_df.iloc[row, 12])
                                        departure_actual_runway = pd.to_datetime(daf_df.iloc[row, 13])
                                        arrival_airport = daf_df.iloc[row, 14]
                                        arrival_timezone = daf_df.iloc[row, 15]
                                        arrival_iata = daf_df.iloc[row, 16]
                                        arrival_icao = daf_df.iloc[row, 17]
                                        arrival_terminal = daf_df.iloc[row, 18]
                                        arrival_gate = daf_df.iloc[row, 19]
                                        arrival_baggage = daf_df.iloc[row, 20]
                                        arrival_delay = daf_df.iloc[row, 21]
                                        arrival_scheduled = pd.to_datetime(daf_df.iloc[row, 22])
                                        arrival_estimated = pd.to_datetime(daf_df.iloc[row, 23])
                                        arrival_actual = pd.to_datetime(daf_df.iloc[row, 24])
                                        arrival_estimated_runway = pd.to_datetime(daf_df.iloc[row, 25])
                                        arrival_actual_runway = pd.to_datetime(daf_df.iloc[row, 26])
                                        airline_name = daf_df.iloc[row, 27]
                                        airline_iata = daf_df.iloc[row, 28]
                                        airline_icao = daf_df.iloc[row, 29]
                                        flight_number = daf_df.iloc[row, 30]
                                        flight_iata = daf_df.iloc[row, 31]
                                        flight_icao = daf_df.iloc[row, 32]
                                        separate_query = "False"
                                        queried_by = session['username']
                                        new_directalternative_row = directalternative(routeid=routeid, flight_date = flight_date, flight_status = flight_status, departure_timezone = departure_timezone, departure_airport = departure_airport, departure_iata = departure_iata, departure_icao = departure_icao, departure_terminal = departure_terminal, departure_gate = departure_gate, departure_delay = departure_delay, departure_scheduled = departure_scheduled, departure_estimated = departure_estimated, departure_actual = departure_actual, departure_estimated_runway = departure_estimated_runway, departure_actual_runway = departure_actual_runway, arrival_airport = arrival_airport, arrival_timezone = arrival_timezone, arrival_iata = arrival_iata, arrival_icao = arrival_icao, arrival_terminal = arrival_terminal, arrival_gate = arrival_gate, arrival_baggage = arrival_baggage, arrival_delay = arrival_delay, arrival_scheduled = arrival_scheduled, arrival_estimated = arrival_estimated, arrival_actual = arrival_actual, arrival_estimated_runway = arrival_estimated_runway, arrival_actual_runway = arrival_actual_runway, airline_name = airline_name, airline_iata = airline_iata, airline_icao = airline_icao, flight_number = flight_number, flight_iata = flight_iata, flight_icao = flight_icao, separate_query = separate_query, queried_by = queried_by)
                                        db.session.add(new_directalternative_row)
                                        db.session.commit()

                                    if len(daf_df)==0:
                                        routeid = temprouteid
                                        flight_date = pd.to_datetime(0)
                                        flight_status = "NOTFOUND"
                                        departure_timezone = "NOTFOUND"
                                        departure_airport = "NOTFOUND"
                                        departure_iata = "NOTFOUND"
                                        departure_icao = "NOTFOUND"
                                        departure_terminal = "NOTFOUND"
                                        departure_gate = 0
                                        departure_delay = 0
                                        departure_scheduled = pd.to_datetime(0)
                                        departure_estimated = pd.to_datetime(0)
                                        departure_actual = pd.to_datetime(0)
                                        departure_estimated_runway = pd.to_datetime(0)
                                        departure_actual_runway = pd.to_datetime(0)
                                        arrival_airport = "NOTFOUND"
                                        arrival_timezone = "NOTFOUND"
                                        arrival_iata = "NOTFOUND"
                                        arrival_icao = "NOTFOUND"
                                        arrival_terminal = "NOTFOUND"
                                        arrival_gate = 0
                                        arrival_baggage = "NOTFOUND"
                                        arrival_delay = 0
                                        arrival_scheduled = pd.to_datetime(0)
                                        arrival_estimated = pd.to_datetime(0)
                                        arrival_actual = pd.to_datetime(0)
                                        arrival_estimated_runway = pd.to_datetime(0)
                                        arrival_actual_runway = pd.to_datetime(0)
                                        airline_name = "NOTFOUND"
                                        airline_iata = "NOTFOUND"
                                        airline_icao = "NOTFOUND"
                                        flight_number = 0
                                        flight_iata = "NOTFOUND"
                                        flight_icao = "NOTFOUND"
                                        separate_query = "False"
                                        queried_by = session['username']
                                        
                                        new_directalternative_row = directalternative(routeid=routeid, flight_date = flight_date, flight_status = flight_status, departure_timezone = departure_timezone, departure_airport = departure_airport, departure_iata = departure_iata, departure_icao = departure_icao, departure_terminal = departure_terminal, departure_gate = departure_gate, departure_delay = departure_delay, departure_scheduled = departure_scheduled, departure_estimated = departure_estimated, departure_actual = departure_actual, departure_estimated_runway = departure_estimated_runway, departure_actual_runway = departure_actual_runway, arrival_airport = arrival_airport, arrival_timezone = arrival_timezone, arrival_iata = arrival_iata, arrival_icao = arrival_icao, arrival_terminal = arrival_terminal, arrival_gate = arrival_gate, arrival_baggage = arrival_baggage, arrival_delay = arrival_delay, arrival_scheduled = arrival_scheduled, arrival_estimated = arrival_estimated, arrival_actual = arrival_actual, arrival_estimated_runway = arrival_estimated_runway, arrival_actual_runway = arrival_actual_runway, airline_name = airline_name, airline_iata = airline_iata, airline_icao = airline_icao, flight_number = flight_number, flight_iata = flight_iata, flight_icao = flight_icao, separate_query = separate_query, queried_by = queried_by)
                                        db.session.add(new_directalternative_row)
                                        db.session.commit()

                            
                            #join the departures and arrivals dataframes and convert dates to datetime format
                            potential_alternative_routes = pd.merge(dep_df, arr_df, left_on = 'arrivaliata', right_on = 'departureiata', suffixes = ('_flight_1', '_flight_2'))
                            potential_alternative_routes['arrivalscheduled_flight_1'] = pd.to_datetime(potential_alternative_routes['arrivalscheduled_flight_1'])
                            potential_alternative_routes['departurescheduled_flight_2'] = pd.to_datetime(potential_alternative_routes['departurescheduled_flight_2'])
                            potential_alternative_routes['arrivalscheduled_flight_2'] =  pd.to_datetime(potential_alternative_routes['arrivalscheduled_flight_2'])

                            # filter where: - HAS BEEN REMOVED TO BE DONE AT THE FILTER STAGE
                            #     -both flights operated
                            #     -flight 2 lands after flight one with a buffer (30 mins)?
                            #     -any more?
                            #buffer_between_flights = 30
                            indirect_alternatives = indirect_alternatives.dropna(subset = ["flightiata_flight_1", "flightiata_flight_2"])

                            #if the initial flight was delayed, alternative route must land before the delayed flight
                            #if flight_info_df['arrivalactual'][0] is not None:
                                #indirect_alternatives = indirect_alternatives[indirect_alternatives['arrivalscheduled_flight_2'] < flight_info_df['arrivalactual'][0]]

                            for row in range(len(indirect_alternatives)):
                                routeid = temprouteid
                                f1_flight_date = pd.to_datetime(indirect_alternatives.iloc[row, 0])
                                f1_flight_status = indirect_alternatives.iloc[row, 1]
                                f1_departure_timezone = indirect_alternatives.iloc[row, 3]
                                f1_departure_airport = indirect_alternatives.iloc[row, 2]
                                f1_departure_iata = indirect_alternatives.iloc[row, 4]
                                f1_departure_icao = indirect_alternatives.iloc[row, 5]
                                f1_departure_terminal = indirect_alternatives.iloc[row, 6]
                                f1_departure_gate = indirect_alternatives.iloc[row, 7]
                                f1_departure_delay = indirect_alternatives.iloc[row, 8]
                                f1_departure_scheduled = pd.to_datetime(indirect_alternatives.iloc[row, 9])
                                f1_departure_estimated = pd.to_datetime(indirect_alternatives.iloc[row, 10])
                                f1_departure_actual = pd.to_datetime(indirect_alternatives.iloc[row, 11])
                                f1_departure_estimated_runway = pd.to_datetime(indirect_alternatives.iloc[row, 12])
                                f1_departure_actual_runway = pd.to_datetime(indirect_alternatives.iloc[row, 13])
                                f1_arrival_airport = indirect_alternatives.iloc[row, 14]
                                f1_arrival_timezone = indirect_alternatives.iloc[row, 15]
                                f1_arrival_iata = indirect_alternatives.iloc[row, 16]
                                f1_arrival_icao = indirect_alternatives.iloc[row, 17]
                                f1_arrival_terminal = indirect_alternatives.iloc[row, 18]
                                f1_arrival_gate = indirect_alternatives.iloc[row, 19]
                                f1_arrival_baggage = indirect_alternatives.iloc[row, 20]
                                f1_arrival_delay = indirect_alternatives.iloc[row, 21]
                                f1_arrival_scheduled = pd.to_datetime(indirect_alternatives.iloc[row, 22])
                                f1_arrival_estimated = pd.to_datetime(indirect_alternatives.iloc[row, 23])
                                f1_arrival_actual = pd.to_datetime(indirect_alternatives.iloc[row, 24])
                                f1_arrival_estimated_runway = pd.to_datetime(indirect_alternatives.iloc[row, 25])
                                f1_arrival_actual_runway = pd.to_datetime(indirect_alternatives.iloc[row, 26])
                                f1_airline_name = indirect_alternatives.iloc[row, 27]
                                f1_airline_iata = indirect_alternatives.iloc[row, 28]
                                f1_airline_icao = indirect_alternatives.iloc[row, 29]
                                f1_flight_number = indirect_alternatives.iloc[row, 30]
                                f1_flight_iata = indirect_alternatives.iloc[row, 31]
                                f1_flight_icao = indirect_alternatives.iloc[row, 32]
                                f2_flight_date = pd.to_datetime(indirect_alternatives.iloc[row, 33])
                                f2_flight_status = indirect_alternatives.iloc[row, 34]
                                f2_departure_timezone = indirect_alternatives.iloc[row, 35]
                                f2_departure_airport = indirect_alternatives.iloc[row, 36]
                                f2_departure_iata = indirect_alternatives.iloc[row, 37]
                                f2_departure_icao = indirect_alternatives.iloc[row, 38]
                                f2_departure_terminal = indirect_alternatives.iloc[row, 39]
                                f2_departure_gate = indirect_alternatives.iloc[row, 40]
                                f2_departure_delay = indirect_alternatives.iloc[row, 41]
                                f2_departure_scheduled = pd.to_datetime(indirect_alternatives.iloc[row, 42])
                                f2_departure_estimated = pd.to_datetime(indirect_alternatives.iloc[row, 43])
                                f2_departure_actual = pd.to_datetime(indirect_alternatives.iloc[row, 44])
                                f2_departure_estimated_runway = pd.to_datetime(indirect_alternatives.iloc[row, 45])
                                f2_departure_actual_runway = pd.to_datetime(indirect_alternatives.iloc[row, 46])
                                f2_arrival_airport = indirect_alternatives.iloc[row, 47]
                                f2_arrival_timezone = indirect_alternatives.iloc[row, 48]
                                f2_arrival_iata = indirect_alternatives.iloc[row, 49]
                                f2_arrival_icao = indirect_alternatives.iloc[row, 50]
                                f2_arrival_terminal = indirect_alternatives.iloc[row, 51]
                                f2_arrival_gate = indirect_alternatives.iloc[row, 52]
                                f2_arrival_baggage = indirect_alternatives.iloc[row, 53]
                                f2_arrival_delay = indirect_alternatives.iloc[row, 54]
                                f2_arrival_scheduled = pd.to_datetime(indirect_alternatives.iloc[row, 55])
                                f2_arrival_estimated = pd.to_datetime(indirect_alternatives.iloc[row, 56])
                                f2_arrival_actual = pd.to_datetime(indirect_alternatives.iloc[row, 57])
                                f2_arrival_estimated_runway = pd.to_datetime(indirect_alternatives.iloc[row, 58])
                                f2_arrival_actual_runway = pd.to_datetime(indirect_alternatives.iloc[row, 59])
                                f2_airline_name = indirect_alternatives.iloc[row, 60]
                                f2_airline_iata = indirect_alternatives.iloc[row, 61]
                                f2_airline_icao = indirect_alternatives.iloc[row, 62]
                                f2_flight_number = indirect_alternatives.iloc[row, 63]
                                f2_flight_iata = indirect_alternatives.iloc[row, 64]
                                f2_flight_icao = indirect_alternatives.iloc[row, 65]
                                queried_by = session['username']
                                date_created = datetime.utcnow()
                                new_indirectalternative_row = indirectalternative(routeid=routeid, f1_flight_date = f1_flight_date, f1_flight_status = f1_flight_status, f1_departure_timezone = f1_departure_timezone, f1_departure_airport = f1_departure_airport, f1_departure_iata = f1_departure_iata, f1_departure_icao = f1_departure_icao, f1_departure_terminal = f1_departure_terminal, f1_departure_gate = f1_departure_gate, f1_departure_delay = f1_departure_delay, f1_departure_scheduled = f1_departure_scheduled, f1_departure_estimated = f1_departure_estimated, f1_departure_actual = f1_departure_actual, f1_departure_estimated_runway = f1_departure_estimated_runway, f1_departure_actual_runway = f1_departure_actual_runway, f1_arrival_airport = f1_arrival_airport, f1_arrival_timezone = f1_arrival_timezone, f1_arrival_iata = f1_arrival_iata, f1_arrival_icao = f1_arrival_icao, f1_arrival_terminal = f1_arrival_terminal, f1_arrival_gate = f1_arrival_gate, f1_arrival_baggage = f1_arrival_baggage, f1_arrival_delay = f1_arrival_delay, f1_arrival_scheduled = f1_arrival_scheduled, f1_arrival_estimated = f1_arrival_estimated, f1_arrival_actual = f1_arrival_actual, f1_arrival_estimated_runway = f1_arrival_estimated_runway, f1_arrival_actual_runway = f1_arrival_actual_runway, f1_airline_name = f1_airline_name, f1_airline_iata = f1_airline_iata, f1_airline_icao = f1_airline_icao, f1_flight_number = f1_flight_number, f1_flight_iata = f1_flight_iata, f1_flight_icao = f1_flight_icao, f2_flight_date = f2_flight_date, f2_flight_status = f2_flight_status, f2_departure_timezone = f2_departure_timezone, f2_departure_airport = f2_departure_airport, f2_departure_iata = f2_departure_iata, f2_departure_icao = f2_departure_icao, f2_departure_terminal = f2_departure_terminal, f2_departure_gate = f2_departure_gate, f2_departure_delay = f2_departure_delay, f2_departure_scheduled = f2_departure_scheduled, f2_departure_estimated = f2_departure_estimated, f2_departure_actual = f2_departure_actual, f2_departure_estimated_runway = f2_departure_estimated_runway, f2_departure_actual_runway = f2_departure_actual_runway, f2_arrival_airport = f2_arrival_airport, f2_arrival_timezone = f2_arrival_timezone, f2_arrival_iata = f2_arrival_iata, f2_arrival_icao = f2_arrival_icao, f2_arrival_terminal = f2_arrival_terminal, f2_arrival_gate = f2_arrival_gate, f2_arrival_baggage = f2_arrival_baggage, f2_arrival_delay = f2_arrival_delay, f2_arrival_scheduled = f2_arrival_scheduled, f2_arrival_estimated = f2_arrival_estimated, f2_arrival_actual = f2_arrival_actual, f2_arrival_estimated_runway = f2_arrival_estimated_runway, f2_arrival_actual_runway = f2_arrival_actual_runway, f2_airline_name = f2_airline_name, f2_airline_iata = f2_airline_iata, f2_airline_icao = f2_airline_icao, f2_flight_number = f2_flight_number, f2_flight_iata = f2_flight_iata, f2_flight_icao = f2_flight_icao, queried_by = queried_by)
                                db.session.add(new_indirectalternative_row)
                                db.session.commit()

                            if len(indirect_alternatives) == 0:
                                routeid = temprouteid
                                f1_flight_date = pd.to_datetime(0)
                                f1_flight_status = "NOT FOUND"
                                f1_departure_timezone = "NOT FOUND"
                                f1_departure_airport = "NOT FOUND"
                                f1_departure_iata = "NOT FOUND"
                                f1_departure_icao = "NOT FOUND"
                                f1_departure_terminal = "NOT FOUND"
                                f1_departure_gate = 0
                                f1_departure_delay = 0
                                f1_departure_scheduled = pd.to_datetime(0)
                                f1_departure_estimated = pd.to_datetime(0)
                                f1_departure_actual = pd.to_datetime(0)
                                f1_departure_estimated_runway = pd.to_datetime(0)
                                f1_departure_actual_runway = pd.to_datetime(0)
                                f1_arrival_airport = "NOT FOUND"
                                f1_arrival_timezone = "NOT FOUND"
                                f1_arrival_iata = "NOT FOUND"
                                f1_arrival_icao = "NOT FOUND"
                                f1_arrival_terminal = "NOT FOUND"
                                f1_arrival_gate = 0
                                f1_arrival_baggage = "NOT FOUND"
                                f1_arrival_delay = 0
                                f1_arrival_scheduled = pd.to_datetime(0)
                                f1_arrival_estimated = pd.to_datetime(0)
                                f1_arrival_actual = pd.to_datetime(0)
                                f1_arrival_estimated_runway = pd.to_datetime(0)
                                f1_arrival_actual_runway = pd.to_datetime(0)
                                f1_airline_name = "NOT FOUND"
                                f1_airline_iata = "NOT FOUND"
                                f1_airline_icao = "NOT FOUND"
                                f1_flight_number = 0
                                f1_flight_iata = "NOT FOUND"
                                f1_flight_icao = "NOT FOUND"
                                f2_flight_date = pd.to_datetime(0)
                                f2_flight_status = "NOT FOUND"
                                f2_departure_timezone = "NOT FOUND"
                                f2_departure_airport = "NOT FOUND"
                                f2_departure_iata = "NOT FOUND"
                                f2_departure_icao = "NOT FOUND"
                                f2_departure_terminal = "NOT FOUND"
                                f2_departure_gate = 0
                                f2_departure_delay = 0
                                f2_departure_scheduled = pd.to_datetime(0)
                                f2_departure_estimated = pd.to_datetime(0)
                                f2_departure_actual = pd.to_datetime(0)
                                f2_departure_estimated_runway = pd.to_datetime(0)
                                f2_departure_actual_runway = pd.to_datetime(0)
                                f2_arrival_airport = "NOT FOUND"
                                f2_arrival_timezone = "NOT FOUND"
                                f2_arrival_iata = "NOT FOUND"
                                f2_arrival_icao = "NOT FOUND"
                                f2_arrival_terminal = "NOT FOUND"
                                f2_arrival_gate = 0
                                f2_arrival_baggage = "NOT FOUND"
                                f2_arrival_delay = 0
                                f2_arrival_scheduled = pd.to_datetime(0)
                                f2_arrival_estimated = pd.to_datetime(0)
                                f2_arrival_actual = pd.to_datetime(0)
                                f2_arrival_estimated_runway = pd.to_datetime(0)
                                f2_arrival_actual_runway = pd.to_datetime(0)
                                f2_airline_name = "NOT FOUND"
                                f2_airline_iata = "NOT FOUND"
                                f2_airline_icao = "NOT FOUND"
                                f2_flight_number = 0
                                f2_flight_iata = "NOT FOUND"
                                f2_flight_icao = "NOT FOUND"
                                queried_by = session['username']
                                date_created = datetime.utcnow()
                                new_indirectalternative_row = indirectalternative(routeid=routeid, f1_flight_date = f1_flight_date, f1_flight_status = f1_flight_status, f1_departure_timezone = f1_departure_timezone, f1_departure_airport = f1_departure_airport, f1_departure_iata = f1_departure_iata, f1_departure_icao = f1_departure_icao, f1_departure_terminal = f1_departure_terminal, f1_departure_gate = f1_departure_gate, f1_departure_delay = f1_departure_delay, f1_departure_scheduled = f1_departure_scheduled, f1_departure_estimated = f1_departure_estimated, f1_departure_actual = f1_departure_actual, f1_departure_estimated_runway = f1_departure_estimated_runway, f1_departure_actual_runway = f1_departure_actual_runway, f1_arrival_airport = f1_arrival_airport, f1_arrival_timezone = f1_arrival_timezone, f1_arrival_iata = f1_arrival_iata, f1_arrival_icao = f1_arrival_icao, f1_arrival_terminal = f1_arrival_terminal, f1_arrival_gate = f1_arrival_gate, f1_arrival_baggage = f1_arrival_baggage, f1_arrival_delay = f1_arrival_delay, f1_arrival_scheduled = f1_arrival_scheduled, f1_arrival_estimated = f1_arrival_estimated, f1_arrival_actual = f1_arrival_actual, f1_arrival_estimated_runway = f1_arrival_estimated_runway, f1_arrival_actual_runway = f1_arrival_actual_runway, f1_airline_name = f1_airline_name, f1_airline_iata = f1_airline_iata, f1_airline_icao = f1_airline_icao, f1_flight_number = f1_flight_number, f1_flight_iata = f1_flight_iata, f1_flight_icao = f1_flight_icao, f2_flight_date = f2_flight_date, f2_flight_status = f2_flight_status, f2_departure_timezone = f2_departure_timezone, f2_departure_airport = f2_departure_airport, f2_departure_iata = f2_departure_iata, f2_departure_icao = f2_departure_icao, f2_departure_terminal = f2_departure_terminal, f2_departure_gate = f2_departure_gate, f2_departure_delay = f2_departure_delay, f2_departure_scheduled = f2_departure_scheduled, f2_departure_estimated = f2_departure_estimated, f2_departure_actual = f2_departure_actual, f2_departure_estimated_runway = f2_departure_estimated_runway, f2_departure_actual_runway = f2_departure_actual_runway, f2_arrival_airport = f2_arrival_airport, f2_arrival_timezone = f2_arrival_timezone, f2_arrival_iata = f2_arrival_iata, f2_arrival_icao = f2_arrival_icao, f2_arrival_terminal = f2_arrival_terminal, f2_arrival_gate = f2_arrival_gate, f2_arrival_baggage = f2_arrival_baggage, f2_arrival_delay = f2_arrival_delay, f2_arrival_scheduled = f2_arrival_scheduled, f2_arrival_estimated = f2_arrival_estimated, f2_arrival_actual = f2_arrival_actual, f2_arrival_estimated_runway = f2_arrival_estimated_runway, f2_arrival_actual_runway = f2_arrival_actual_runway, f2_airline_name = f2_airline_name, f2_airline_iata = f2_airline_iata, f2_airline_icao = f2_airline_icao, f2_flight_number = f2_flight_number, f2_flight_iata = f2_flight_iata, f2_flight_icao = f2_flight_icao, queried_by = queried_by)
                                db.session.add(new_indirectalternative_row)
                                db.session.commit()



    try:        
        flights = flight.query.filter(flight.flightid == session['flightid']).order_by(flight.flight_date).all()
        directalternatives = directalternative.query.filter(or_(directalternative.routeid.in_(session['altroutesrequested']), directalternative.routeid == session['routeid'])).order_by(directalternative.flight_date).all()
        indirectalternatives = indirectalternative.query.filter(or_(indirectalternative.routeid == session['routeid'], indirectalternative.routeid.in_(session['altroutesrequested']))).order_by(indirectalternative.f1_flight_date).all()
        
    except:
        return render_template('index.html')

    else:
        if session['flightnotfound']!= True:
            
            if len(flights) > 0:
                query_departure_iata = flights[0].departure_iata
                query_arrival_iata = flights[0].arrival_iata

            

            if (session['daf_requested'] == True or session['iaf_requested'] == True):
                departure_iata = query_departure_iata
                arrival_iata = query_arrival_iata

                #get the 5 closest departure airports for display
                array = np.array(airports.iloc[:, 4:6])
                coords = np.array([(airports[airports['iata_code']==departure_iata].iloc[:,4], airports[airports['iata_code']==departure_iata].iloc[:,5])])[0]
                coords.shape = (1, 2)
                distances = np.linalg.norm(array-coords, axis=1)
                idx = np.argpartition(distances.ravel(), 6)
                nearest_5 = np.array(np.unravel_index(idx, distances.shape))[:, range(6)].transpose().tolist()

                nearest_5_dep_df = pd.DataFrame(columns = ["Iata_code", "Distance", "Drive_time", "Drive_distance"])

                for i in nearest_5[1:5]:
                    row = []
                    row.append(airports.iloc[i, 13].values[0])
                    
                    dist, eu261 = greatcirclecalc(departure_iata,airports.iloc[i, 13].values[0])
                    
                    row.append(dist)
                    
                    deplat = airports[airports['iata_code']==departure_iata].iloc[:,4]
                    deplong = airports[airports['iata_code']==departure_iata].iloc[:,5]
                    arrlat = airports[airports['iata_code']==arrival_iata].iloc[:,4]
                    arrlong = airports[airports['iata_code']==arrival_iata].iloc[:,5]
                    altdeplat = airports.iloc[i, 4]
                    altdeplong = airports.iloc[i, 5]
                    travel_mode = "Driving"
                    query_url = r"http://dev.virtualearth.net/REST/v1/Routes/" + travel_mode + "?o=json&wp.0=" + str(deplat.values[0]) + "," + str(deplong.values[0]) +"&wp.1=" + str(altdeplat.values[0]) + "," + str(altdeplong.values[0]) + "&key=AghfZWXQ0nEiSbp6zjT2N3OUppVxXwv1dmUb3gfcvimiYhAqzhNvqXVAIyy_7Rk7"
                    response = requests.get(query_url)
                    response.close()
                    df = pd.DataFrame(response.json()['resourceSets'])
                    df1 = pd.DataFrame(df['resources'][0])['travelDuration'][0]
                    row.append(df1)
                    df2 = pd.DataFrame(df['resources'][0])['travelDistance'][0]
                    row.append(df2)
                    nearest_5_dep_df.loc[len(nearest_5_dep_df)] = row



                #get the closest 5 airports to arrival (euclidean but hopefully accurate enough)
                array = np.array(airports.iloc[:, 4:6])
                coords = np.array([(airports[airports['iata_code']==arrival_iata].iloc[:,4], airports[airports['iata_code']==arrival_iata].iloc[:,5])])[0]
                coords.shape = (1, 2)
                distances = np.linalg.norm(array-coords, axis=1)
                idx = np.argpartition(distances.ravel(), 6)
                nearest_5 = np.array(np.unravel_index(idx, distances.shape))[:, range(6)].transpose().tolist()

                nearest_5_arr_df = pd.DataFrame(columns = ["Iata_code", "Distance", "Drive_time", "Drive_distance"])

                for i in nearest_5[1:5]:
                    row = []
                    row.append(airports.iloc[i, 13].values[0])
                    
                    dist, eu261 = greatcirclecalc(departure_iata,airports.iloc[i, 13].values[0])
                    
                    row.append(dist)
                    
                    deplat = airports[airports['iata_code']==departure_iata].iloc[:,4]
                    deplong = airports[airports['iata_code']==departure_iata].iloc[:,5]
                    arrlat = airports[airports['iata_code']==arrival_iata].iloc[:,4]
                    arrlong = airports[airports['iata_code']==arrival_iata].iloc[:,5]
                    altarrlat = airports.iloc[i, 4]
                    altarrlong = airports.iloc[i, 5]
                    travel_mode = "Driving"
                    query_url = r"http://dev.virtualearth.net/REST/v1/Routes/" + travel_mode + "?o=json&wp.0=" + str(arrlat.values[0]) + "," + str(arrlong.values[0]) +"&wp.1=" + str(altarrlat.values[0]) + "," + str(altarrlong.values[0]) + "&key=AghfZWXQ0nEiSbp6zjT2N3OUppVxXwv1dmUb3gfcvimiYhAqzhNvqXVAIyy_7Rk7"
                    response = requests.get(query_url)
                    response.close()
                    df = pd.DataFrame(response.json()['resourceSets'])
                    df1 = pd.DataFrame(df['resources'][0])['travelDuration'][0]
                    row.append(df1)
                    df2 = pd.DataFrame(df['resources'][0])['travelDistance'][0]
                    row.append(df2)
                    nearest_5_arr_df.loc[len(nearest_5_arr_df)] = row
            
                
                
                nearest_5_arr_df = nearest_5_arr_df.sort_values("Drive_time").reset_index()
                nearest_5_dep_df = nearest_5_dep_df.sort_values("Drive_time").reset_index()

                nearest_5_arr_df["Drive_time"] = pd.to_datetime(nearest_5_arr_df["Drive_time"], unit='s').dt.strftime("%H:%M")
                nearest_5_dep_df["Drive_time"] = pd.to_datetime(nearest_5_dep_df["Drive_time"], unit='s').dt.strftime("%H:%M")

                #set variables for alternative departure airports for the html
                altdepiata1 = nearest_5_dep_df.iloc[0, 1]
                altdepdist1 = round(nearest_5_dep_df.iloc[0, 4], 2)
                altdeptime1 = nearest_5_dep_df.iloc[0, 3]
                altdepiata2 = nearest_5_dep_df.iloc[1, 1]
                altdepdist2 = round(nearest_5_dep_df.iloc[1, 4], 2)
                altdeptime2 = nearest_5_dep_df.iloc[1, 3]
                altdepiata3 = nearest_5_dep_df.iloc[2, 1]
                altdepdist3 = round(nearest_5_dep_df.iloc[2, 4], 2)
                altdeptime3 = nearest_5_dep_df.iloc[2, 3]
                altdepiata4 = nearest_5_dep_df.iloc[3, 1]
                altdepdist4 = round(nearest_5_dep_df.iloc[3, 4], 2)
                altdeptime4 = nearest_5_dep_df.iloc[3, 3]

                #and for alternative arrivals for the html
                altarriata1 = nearest_5_arr_df.iloc[0, 1]
                altarrdist1 = round(nearest_5_arr_df.iloc[0, 4], 2)
                altarrtime1 = nearest_5_arr_df.iloc[0, 3]
                altarriata2 = nearest_5_arr_df.iloc[1, 1]
                altarrdist2 = round(nearest_5_arr_df.iloc[1, 4], 2)
                altarrtime2 = nearest_5_arr_df.iloc[1, 3]
                altarriata3 = nearest_5_arr_df.iloc[2, 1]
                altarrdist3 = round(nearest_5_arr_df.iloc[2, 4], 2)
                altarrtime3 = nearest_5_arr_df.iloc[2, 3]
                altarriata4 = nearest_5_arr_df.iloc[3, 1]
                altarrdist4 = round(nearest_5_arr_df.iloc[3, 4], 2)
                altarrtime4 = nearest_5_arr_df.iloc[3, 3]

                session['altairports'] = [altdepiata1, altdepiata2, altdepiata3, altdepiata4, altarriata1, altarriata2, altarriata3, altarriata4]

            if len(flights) > 0:
                fdist, feu261 = greatcirclecalc(query_departure_iata, query_arrival_iata)
                fdist = round(fdist, 2)

                n_direct = len(directalternatives)
                n_indirect = len(indirectalternatives)


            if session['daf_requested'] == True and session['iaf_requested'] == True:
                session['print_template'] = render_template('fullquery.html', n_direct = n_direct, n_indirect = n_indirect, altdepiata1 = altdepiata1, altdepiata2 = altdepiata2,altdepiata3 = altdepiata3, altdepiata4 = altdepiata4, altdepdist1 = altdepdist1, altdepdist2 = altdepdist2, altdepdist3 = altdepdist3, altdepdist4 = altdepdist4, altdeptime1 = altdeptime1, altdeptime2 = altdeptime2, altdeptime3 = altdeptime3, altdeptime4 = altdeptime4, altarriata1 = altarriata1, altarriata2 = altarriata2,altarriata3 = altarriata3, altarriata4 = altarriata4, altarrdist1 = altarrdist1, altarrdist2 = altarrdist2, altarrdist3 = altarrdist3, altarrdist4 = altarrdist4, altarrtime1 = altarrtime1, altarrtime2 = altarrtime2, altarrtime3 = altarrtime3, altarrtime4 = altarrtime4,  fdist = fdist, feu261 = feu261, flights = flights, directalternatives = directalternatives, indirectalternatives=indirectalternatives)
                #pdf_name = "test"#requested_flight_iata + " " + str(requested_flight_date) + " direct/indirect alternatives"

            elif session['daf_requested'] == True and session['iaf_requested'] == False:
                session['print_template'] = render_template('directalternative pdf export.html', list = session['altroutesrequested'], user = session['username'], n_direct = n_direct, n_indirect = n_indirect, altdepiata1 = altdepiata1, altdepiata2 = altdepiata2,altdepiata3 = altdepiata3, altdepiata4 = altdepiata4, altdepdist1 = altdepdist1, altdepdist2 = altdepdist2, altdepdist3 = altdepdist3, altdepdist4 = altdepdist4, altdeptime1 = altdeptime1, altdeptime2 = altdeptime2, altdeptime3 = altdeptime3, altdeptime4 = altdeptime4, altarriata1 = altarriata1, altarriata2 = altarriata2,altarriata3 = altarriata3, altarriata4 = altarriata4, altarrdist1 = altarrdist1, altarrdist2 = altarrdist2, altarrdist3 = altarrdist3, altarrdist4 = altarrdist4, altarrtime1 = altarrtime1, altarrtime2 = altarrtime2, altarrtime3 = altarrtime3, altarrtime4 = altarrtime4,  fdist = fdist, feu261 = feu261, flights = flights, directalternatives = directalternatives, indirectalternatives=indirectalternatives)
                #pdf_name = "test"#requested_flight_iata + " " + str(requested_flight_date) + " direct alternatives"

            elif session['daf_requested'] == False and session['iaf_requested'] == True:
                session['print_template'] =  render_template('indirectalternative.html', n_direct = n_direct, n_indirect = n_indirect, altdepiata1 = altdepiata1, altdepiata2 = altdepiata2,altdepiata3 = altdepiata3, altdepiata4 = altdepiata4, altdepdist1 = altdepdist1, altdepdist2 = altdepdist2, altdepdist3 = altdepdist3, altdepdist4 = altdepdist4, altdeptime1 = altdeptime1, altdeptime2 = altdeptime2, altdeptime3 = altdeptime3, altdeptime4 = altdeptime4, altarriata1 = altarriata1, altarriata2 = altarriata2,altarriata3 = altarriata3, altarriata4 = altarriata4, altarrdist1 = altarrdist1, altarrdist2 = altarrdist2, altarrdist3 = altarrdist3, altarrdist4 = altarrdist4, altarrtime1 = altarrtime1, altarrtime2 = altarrtime2, altarrtime3 = altarrtime3, altarrtime4 = altarrtime4,  fdist = fdist, feu261 = feu261, flights = flights, directalternatives = directalternatives, indirectalternatives=indirectalternatives)
                #pdf_name = "test"#requested_flight_iata + " " + str(requested_flight_date) + " indirect alternatives"

            elif session['daf_requested'] == False and session['iaf_requested'] == False:
                session['print_template'] =  str(render_template('flight.html', n_direct = n_direct, n_indirect = n_indirect, fdist = fdist, feu261 = feu261, flights = flights, directalternatives = directalternatives, indirectalternatives=indirectalternatives))
                #pdf_name = "test"#requested_flight_iata + " " + str(requested_flight_date) + " flight info"  
            
            

            if session['daf_requested'] == True and session['iaf_requested'] == True:
                return render_template('fullquery.html', n_direct = n_direct, n_indirect = n_indirect, altdepiata1 = altdepiata1, altdepiata2 = altdepiata2,altdepiata3 = altdepiata3, altdepiata4 = altdepiata4, altdepdist1 = altdepdist1, altdepdist2 = altdepdist2, altdepdist3 = altdepdist3, altdepdist4 = altdepdist4, altdeptime1 = altdeptime1, altdeptime2 = altdeptime2, altdeptime3 = altdeptime3, altdeptime4 = altdeptime4, altarriata1 = altarriata1, altarriata2 = altarriata2,altarriata3 = altarriata3, altarriata4 = altarriata4, altarrdist1 = altarrdist1, altarrdist2 = altarrdist2, altarrdist3 = altarrdist3, altarrdist4 = altarrdist4, altarrtime1 = altarrtime1, altarrtime2 = altarrtime2, altarrtime3 = altarrtime3, altarrtime4 = altarrtime4,  fdist = fdist, feu261 = feu261, flights = flights, directalternatives = directalternatives, indirectalternatives=indirectalternatives)

            elif session['daf_requested'] == True and session['iaf_requested'] == False:
                return render_template('directalternative.html', n_direct = n_direct, n_indirect = n_indirect, altdepiata1 = altdepiata1, altdepiata2 = altdepiata2,altdepiata3 = altdepiata3, altdepiata4 = altdepiata4, altdepdist1 = altdepdist1, altdepdist2 = altdepdist2, altdepdist3 = altdepdist3, altdepdist4 = altdepdist4, altdeptime1 = altdeptime1, altdeptime2 = altdeptime2, altdeptime3 = altdeptime3, altdeptime4 = altdeptime4, altarriata1 = altarriata1, altarriata2 = altarriata2,altarriata3 = altarriata3, altarriata4 = altarriata4, altarrdist1 = altarrdist1, altarrdist2 = altarrdist2, altarrdist3 = altarrdist3, altarrdist4 = altarrdist4, altarrtime1 = altarrtime1, altarrtime2 = altarrtime2, altarrtime3 = altarrtime3, altarrtime4 = altarrtime4,  fdist = fdist, feu261 = feu261, flights = flights, directalternatives = directalternatives, indirectalternatives=indirectalternatives)

            elif session['daf_requested'] == False and session['iaf_requested'] == True:
                return render_template('indirectalternative.html', n_direct = n_direct, n_indirect = n_indirect, altdepiata1 = altdepiata1, altdepiata2 = altdepiata2,altdepiata3 = altdepiata3, altdepiata4 = altdepiata4, altdepdist1 = altdepdist1, altdepdist2 = altdepdist2, altdepdist3 = altdepdist3, altdepdist4 = altdepdist4, altdeptime1 = altdeptime1, altdeptime2 = altdeptime2, altdeptime3 = altdeptime3, altdeptime4 = altdeptime4, altarriata1 = altarriata1, altarriata2 = altarriata2,altarriata3 = altarriata3, altarriata4 = altarriata4, altarrdist1 = altarrdist1, altarrdist2 = altarrdist2, altarrdist3 = altarrdist3, altarrdist4 = altarrdist4, altarrtime1 = altarrtime1, altarrtime2 = altarrtime2, altarrtime3 = altarrtime3, altarrtime4 = altarrtime4,  fdist = fdist, feu261 = feu261, flights = flights, directalternatives = directalternatives, indirectalternatives=indirectalternatives)
            
            elif session['daf_requested'] == False and session['iaf_requested'] == False:
                return render_template('flight.html', n_direct = n_direct, n_indirect = n_indirect, fdist = fdist, feu261 = feu261, flights = flights, directalternatives = directalternatives, indirectalternatives=indirectalternatives)
        else:
            return render_template('flightnotfound.html')
        

@app.route("/login")
def login():
    # Technically we could use empty list [] as scopes to do just sign in,
    # here we choose to also collect end user consent upfront
    session["flow"] = _build_auth_code_flow(scopes=app_config.SCOPE)
    return render_template("login.html", auth_url=session["flow"]["auth_uri"], version=msal.__version__)


@app.route(app_config.REDIRECT_PATH)  # Its absolute URL must match your app's redirect_uri set in AAD
def authorized():
    try:
        cache = _load_cache()
        result = _build_msal_app(cache=cache).acquire_token_by_auth_code_flow(
            session.get("flow", {}), request.args)
        if "error" in result:
            return render_template("auth_error.html", result=result)
        session["user"] = result.get("id_token_claims")
        _save_cache(cache)
    except ValueError:  # Usually caused by CSRF
        pass  # Simply ignore them
    return redirect(url_for("index"))

@app.route("/usage")
def usage():

    #flights = flight.query.with_entities(flight.queried_by, func.count(flight.queried_by).label("number of queries")).group_by(flight.queried_by).all()
    engine = create_engine('sqlite:///test.db').connect()

    flight_df = pd.read_sql_table('flight', engine)
    da_df = pd.read_sql_table('directalternative', engine)
    ia_df = pd.read_sql_table('indirectalternative', engine)

    flight_df['count'] = 1
    flight_df_month = flight_df[flight_df['date_created'] > datetime.today().replace(day=1)]
    flight_df = flight_df[['queried_by', 'count']].groupby(by='queried_by', as_index=False).sum()
    flight_df_month = flight_df_month[['queried_by', 'count']].groupby(by='queried_by', as_index=False).sum()

    da_df['count'] = 1
    da_df = da_df[da_df['separate_query']=="True"]
    da_df_month = da_df[da_df['date_created'] > datetime.today().replace(day=1)]
    da_df = da_df[['routeid', 'queried_by', 'count']].groupby(by=['routeid', 'queried_by'], as_index=False).sum()
    da_df_month = da_df_month[['routeid', 'queried_by', 'count']].groupby(by=['routeid', 'queried_by'], as_index=False).sum()
    da_df['count'] = np.ceil((da_df['count']/100))
    da_df_month['count'] = np.ceil(da_df_month['count']/100)
    da_df = da_df[['queried_by', 'count']].groupby(by='queried_by', as_index=False).sum()
    da_df_month = da_df_month[['queried_by', 'count']].groupby(by='queried_by', as_index=False).sum()
    
    ia_df['count'] = 1
    ia_df_month = ia_df[ia_df['date_created'] > datetime.today().replace(day=1)]
    ia_df = ia_df[['routeid', 'queried_by', 'count']].groupby(by=['routeid', 'queried_by'], as_index=False).sum()
    ia_df_month = ia_df_month[['routeid', 'queried_by', 'count']].groupby(by=['routeid', 'queried_by'], as_index=False).sum()
    ia_df['count'] = np.ceil(ia_df['count']/100)
    ia_df_month['count'] = np.ceil(ia_df_month['count']/100)
    ia_df = ia_df[['queried_by', 'count']].groupby(by='queried_by', as_index=False).sum()
    ia_df_month = ia_df_month[['queried_by', 'count']].groupby(by='queried_by', as_index=False).sum()
    
    join_all_df = flight_df.merge(da_df, how= 'left', on='queried_by', suffixes=['', '_da'])
    join_month_df = flight_df_month.merge(da_df_month, how= 'left', on='queried_by', suffixes=['', '_da'])
    join_all_df = join_all_df.merge(ia_df, how='left', on='queried_by', suffixes =['', '_ia'])
    join_month_df = join_month_df.merge(ia_df_month, how='left', on='queried_by', suffixes =['', '_ia'])
    join_all_df = join_all_df.fillna(0)
    join_month_df = join_month_df.fillna(0)
    join_all_df['total_queries'] = join_all_df['count'] + join_all_df['count_da'] + join_all_df['count_ia']
    join_month_df['total_queries'] = join_month_df['count'] + join_month_df['count_da'] + join_month_df['count_ia']
    join_all_df = join_all_df[["queried_by", "total_queries"]]
    join_month_df = join_month_df[["queried_by", "total_queries"]]
    join_month_df["total_queries"] = join_month_df["total_queries"].astype(int)

    queries_remaining = int(100 - join_all_df['total_queries'].sum())

    return render_template("usage.html", queries_remaining = queries_remaining, column_names = join_month_df.columns.values, row_data=list(join_month_df.values.tolist()), link_column = "queried_by", zip=zip)

app.jinja_env.globals.update(_build_auth_code_flow=_build_auth_code_flow)  # Used in template

if __name__ == "__main__":
    app.run(debug=True)


