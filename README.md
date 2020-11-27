# Tesla Data Challenge - A Severless Solution with GCP 

### Summary 
This project aims to solve the [Tesla take-home data challenge](https://te-data-test.herokuapp.com/) by leveraging the latest severless service by Google Cloud Platform. The chart below provides an overview of the data flow:
![Image](https://github.com/tomchen29/tesla_data_challenge_gcp_severless/blob/main/images/project_overall_architect.png)
The main advantage of this design is, given that everything is severless, the system is both light-weighted and easy to implement, yet highly scalable and can be easily extended to thousands of signal streaming with little modification.

For the core output products, we have:
* A BigQuery table called "**energy_telemetry_logging**" that serves as the raw streaming log of Tesla fleet signals. This table pulls the payload from Pub/Sub every minute, and store it into the following columns:
  * event_timestamp: partition key. This is the time when the cloud function calls the Tesla API
  * timestamp: This is the timestamp when the site-signal API actaully gets called
  * site: extracted from Tesla API payload. The device associated with this payload
  * SITE_SM_batteryInstPower: how much power is charging (negative) and discharging (positive) in and out of the home batteries
  * SITE_SM_siteInstPower: how much power is being imported (positive) from the grid or exported (negative) to the grid
  * SITE_SM_solarInstPower: how much power is being produced by solar and **should always be positive**
* A Data Studio Dashboard (https://datastudio.google.com/reporting/9efe9c81-4a24-4224-8811-5d82225038d0) that tracks:
  * number of devices that are "up" determined by real-time API response (1-min time window). This widget helps us understand the latest condition of a device's API, or if there is any potential issue on a certain device, in a real-time way
  * solar Production Condition by Device in the Past 1 Hour (1-hour Window). This widget shows us which device has anomalies on solar power production and needs immediate attention, in a near real-time way (we can shorten the time window to make it more real-time, but that will increase my bill a lot so I just built the 1-hour version)
* A Colab Notebook (https://colab.research.google.com/drive/1f3iaQZ3KgBTxf7rzEnFENCLI_I68Zukl?usp=sharing) that provides a proof-of-concept solution to identify solar power production anomalies via time-series forecasting 










