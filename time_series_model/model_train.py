from fbprophet import Prophet
from google.cloud import bigquery
import pickle


def train_time_series_model():

    PROJECT_ID = "tesla-iot-challenge"

    client = bigquery.Client(project=PROJECT_ID)

    # select all solar data from the real-time table, and store as pandas dataframe
    train_solar = client.query('''
    SELECT event_timestamp, site, SITE_SM_solarInstPower FROM `tesla-iot-challenge.telemetry.energy_telemetry_logging`
  ''').to_dataframe()

    # ETL to transform data to the form that fbprophet accepts
    train_solar['SITE_SM_solarInstPower'] = train_solar.SITE_SM_solarInstPower.fillna(
        0).apply(lambda x: max(0, x))
    train_solar['event_timestamp'] = train_solar.event_timestamp.apply(
        lambda x: str(x)[:-6])

    train_solar_data_dict = {}
    site_list = train_solar.site.unique()
    for site in site_list:
        train_solar_data_dict[site] = train_solar.loc[train_solar['site'] == site][[
            'event_timestamp', 'SITE_SM_solarInstPower']]

    solar_model_dict = {}
    for site in site_list:
        m = Prophet()
        m.fit(train_solar_data_dict[site].rename(
            columns={'event_timestamp': 'ds', 'SITE_SM_solarInstPower': 'y'}))
        solar_model_dict[site] = m

    pickle.dump(solar_model_dict, open("solar_model_dict.p", "wb"))

    return solar_model_dict
