Google Ads Campaign Data Processing
This Python script integrates with the Google Ads API and BigQuery to extract, transform, and load (ETL) campaign data into BigQuery datasets for analysis. It processes various campaign reports, including keyword, search term, and conversion data.

Features
Campaign Report Generation: Extracts campaign performance data for a specified date range.
Keyword and Search Term Analysis: Collects keyword-level performance and conversion data.
BigQuery Integration: Loads processed data into BigQuery datasets with support for dataset creation and schema adjustments.
Error Handling and Retries: Implements retry mechanisms for Google Ads API requests to handle transient errors.
Requirements
Python 3.7+
Google Ads Python Client Library
Google Cloud Python SDK
Required libraries: pandas, numpy, re, google-auth, google-cloud-bigquery
Setup
1. Install Dependencies
Use pip to install the required Python libraries:

pip install google-ads google-cloud-bigquery pandas numpy

2. Configure Service Account Credentials
Obtain a Google Ads API key and BigQuery service account key.
Save the BigQuery service account key file as secrets_bq.json in the project directory.
3. Configure Google Ads Client
Create a google-ads.yaml configuration file in your project directory.
Ensure the file contains credentials for accessing the Google Ads API.
4. Set Up BigQuery Project
Ensure the BigQuery project is configured in Google Cloud Console.
Assign the appropriate service account roles for dataset and table access.
Usage
Extract and Load Campaign Data
Import the script into your Python environment or run it directly.

Initialize the Google Ads Client and specify the customer ID:

from google.ads.googleads.client import GoogleAdsClient

googleads_client = GoogleAdsClient.load_from_storage("google-ads.yaml")
client_id = "INSERT_CUSTOMER_ID"
Call the desired functions for data extraction:

Fetch Campaign Data:


getCampaignMergeData(googleads_client, client_id)

Fetch Keyword Summary:
getKeywordSummaryReport(googleads_client, client_id)

Fetch Search Term Summary:
getSearchTermSummaryReport(googleads_client, client_id)

Data will be loaded into BigQuery tables automatically.

Error Handling
The script includes retry logic to handle API errors. Errors that persist after retries will be logged and can be reviewed for debugging.

Customization
Modify queries in the script to customize the data fetched.
Update the destination fields for BigQuery tables to match your project structure.
