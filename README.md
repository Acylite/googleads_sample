Google Ads Scripts
This repository contains sample scripts for automating tasks in Google Ads using Python. It demonstrates the use of the Google Ads API to efficiently manage campaigns, analyze performance, and optimize ad spend.

Features
Campaign Management: Create, update, and pause campaigns programmatically.
Ad Group Optimization: Adjust bids and targeting to improve performance.
Performance Reporting: Fetch detailed metrics for campaigns, ad groups, and keywords.
Custom Scripts: Examples tailored for specific automation needs.
Prerequisites
Before using the scripts, ensure you have the following:

Google Ads API Access:
A Google Ads account with API access enabled.
A developer token from the Google Ads API Center.
Python Environment:
Python 3.7 or later installed.
Required libraries (see the Installation section).
OAuth2 Credentials:
A google-ads.yaml configuration file with the required credentials.
Installation
Clone the Repository:


git clone https://github.com/Acylite/googleads_sample.git
cd googleads_sample
Install Dependencies: Install the necessary Python libraries using pip:

pip install -r requirements.txt
Set Up Configuration: Create a google-ads.yaml file in the root directory with the following structure:

developer_token: YOUR_DEVELOPER_TOKEN
client_id: YOUR_CLIENT_ID
client_secret: YOUR_CLIENT_SECRET
refresh_token: YOUR_REFRESH_TOKEN
Usage
Run the Script: Execute the googleads_main.py script to perform actions:

python googleads_main.py
Customize Scripts: Modify the googleads_main.py file to include your own logic. Examples of use cases include:

Filtering campaigns by name or status.
Automating bid adjustments.
Generating custom reports.
Error Handling: Ensure proper handling of errors by customizing the provided error-handling code sections.

File Structure
googleads_sample/
│
├── googleads_main.py          # Main script for interacting with Google Ads API
├── requirements.txt           # Dependencies for the project
└── README.md                  # Documentation
Example Code
Here’s an example snippet to fetch all active campaigns:


from google.ads.google_ads.client import GoogleAdsClient

client = GoogleAdsClient.load_from_storage()
service = client.get_service("GoogleAdsService")

query = """
    SELECT
      campaign.id,
      campaign.name
    FROM
      campaign
    WHERE
      campaign.status = 'ENABLED'
"""

response = service.search(customer_id="YOUR_CUSTOMER_ID", query=query)
for row in response:
    print(f"Campaign ID: {row.campaign.id}, Name: {row.campaign.name}")
Resources
Google Ads API Documentation
Google Ads Python Client Library
