
from google.oauth2 import service_account
import os
from ingest_api_data import main
import create_bigquery_db
from time_series_model import model_train


# Set Googlg Cloud Project Credential. Please use your own creential file if you'd like to reuse the code
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./tesla-iot-challenge-credentials.json"

# Create Bigquery table "tesla-iot-challenge.telemetry.energy_telemetry_logging"
# If dataset or table already exists, creation process will automatically skip
create_bigquery_db.create_bigquery_table()

# Invoke web API, perform ETL, and then push data into Pub/Sub
main.execute(None)

# train model
solar_model_dict = model_train.train_time_series_model()
