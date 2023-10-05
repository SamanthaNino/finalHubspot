import os
import requests
import json

from dotenv import load_dotenv, dotenv_values


load_dotenv()

# Set HubSpot API Key and other variables from environment variables
hubspot_api_key = os.getenv("HUBSPOT_API_KEY")
aws_bearer_token = os.getenv("AWS_BEARER_TOKEN")
aws_api_endpoint = os.getenv("AWS_API_ENDPOINT")

if not hubspot_api_key or not aws_bearer_token or not aws_api_endpoint:
    print("Please set the required environment variables.")
    exit(1)

# Fetch contact data from the AWS API
headers_aws = {
    "Authorization": f"Bearer {aws_bearer_token}"
}

try:
    response_aws = requests.get(aws_api_endpoint, headers=headers_aws)

    if response_aws.status_code == 200:
        aws_contact_data = response_aws.json()
        print("Successfully retrieved contact data from AWS API:")
        print(aws_contact_data)

        # Prepare contact data for HubSpot batch operation
        hubspot_contact_batch = []

        for aws_contact in aws_contact_data:
            # Map AWS contact data to HubSpot properties (adjust property names as needed)
            hubspot_contact_data = {
                "properties": {
                    "hs_created_by_user_id": aws_contact["id"],
                    "firstname": aws_contact["first_name"],
                    "lastname": aws_contact["last_name"],
                    "gender": aws_contact["gender"],
                    "email": aws_contact["email"],
                    "phonenumber": aws_contact["phone_number"]
                }
            }
            hubspot_contact_batch.append(hubspot_contact_data)

        # Create or update contacts in HubSpot using batch operation
        hubspot_api_url = "https://api.hubapi.com/crm/v3/objects/contacts"
        headers_hubspot = {
            "Authorization": f"Bearer {hubspot_api_key}",
            "Content-Type": "application/json"
        }

        hubspot_batch_data = {
            "inputs": hubspot_contact_batch
        }

        response_hubspot = requests.post(hubspot_api_url, json=hubspot_batch_data, headers=headers_hubspot)

        if response_hubspot.status_code == 201:
            try:
                contact_response_data = response_hubspot.json()
                print(f"Contacts created/updated in HubSpot. {len(contact_response_data)} contacts processed.")
            except json.JSONDecodeError:
                print("Successfully created/updated contacts in HubSpot, but unable to parse the response JSON.")
        else:
            print(f"Failed to create/update contacts in HubSpot. Status code: {response_hubspot.status_code}")

    else:
        print(f"Failed to retrieve contact data from AWS API. Status code: {response_aws.status_code}")

except Exception as e:
    print(f"An error occurred: {str(e)}")