# Tesla Data Challenge - A Severless Solution with GCP 

### Summary 
This project aims to solve the [Tesla take-home data challenge](https://te-data-test.herokuapp.com/) by leveraging the latest severless service by Google Cloud Platform. It contains 5 parts:
* Ingestion: Cloud Function
* Streaming: Cloud Pub/Sub + DataFlow
* Warehouse: BigQuery
* Visualization: Data Studio
* Analytics: Google Colab (free version of Cloud Datalab)


The chart below provides an overview of the data flow:
![Image](https://github.com/tomchen29/tesla_data_challenge_gcp_severless/blob/main/images/project_overall_architect.png)

The main advantage of this design is, given that everything is severless, the system is light-weighted, easy to implement, yet highly scalable and can be easily extended to thousands of signal streaming with little modification.

