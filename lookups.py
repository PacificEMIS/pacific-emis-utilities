# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.0
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
import json

def load_config(config_path="config.json"):
    """Load configuration from a JSON file."""
    with open(config_path, 'r') as file:
        config = json.load(file)
    return config['base_url'], config['username'], config['password'], config['output_directory']

# Test loading configuration
base_url, username, password, output_dir = load_config()
print("Configuration loaded successfully.")

# %%
import requests
import urllib3

# Disable warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def authenticate(base_url, username, password):
    """Authenticate and return a bearer token."""
    auth_url = f"{base_url}/api/token"
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    payload = {
        "grant_type": "password",
        "username": username,
        "password": password
    }
    response = requests.post(auth_url, headers=headers, data=payload, verify=False)
    response.raise_for_status()
    token = response.json().get("access_token")  # Adjust if necessary
    return token

# Test authentication
token = authenticate(base_url, username, password)
print("Authentication successful. Token obtained.")

# %%
import requests

def get_lookup_data(base_url, token):
    """Fetch the lookup data from the specified API endpoint."""
    lookup_url = f"{base_url}/api/lookups/collection/core"
    headers = {"Authorization": f"Bearer {token}"}
    
    response = requests.get(lookup_url, headers=headers, verify=False)
    response.raise_for_status()  # Raise an error for bad responses
    
    # Return the JSON data
    return response.json()

# Test fetching the lookup data
lookups_data = get_lookup_data(base_url, token)
print("Lookups Data (sample):", lookups_data['schoolTypes'])


# %%
import os
import pickle

# Ensure the folder exists
cache_dir = "cached-data"
os.makedirs(cache_dir, exist_ok=True)

# Save the lookup data
with open(os.path.join(cache_dir, "lookups_data.pkl"), "wb") as f:
    pickle.dump(lookups_data, f)

print("✅ lookup_data saved to cached-data/lookups_data.pkl")


# %%
lookups_data['teacherPdTypes']
lookups_data['schoolNames']


# %%
