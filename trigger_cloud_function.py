#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os

import requests
from dotenv import load_dotenv

load_dotenv()

# Replace with the actual URL of your Cloud Function
url = os.environ["CLOUD_FUNCTION_URL"]

# Optional: Add data to be sent in the POST request body (as a dictionary)
data = {"": ""}

# Send the POST request
response = requests.post(url, json=data)  # Use json=data if sending data

# Check for successful response
if response.status_code == 200:
    print("POST request successful!")
    # Access the response content (optional)
    print(response.text)
else:
    print(f"Error: {response.status_code}")
    print(response.text)
