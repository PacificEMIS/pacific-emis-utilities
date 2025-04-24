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
def get_teachers_list(base_url, token, pageSize=50):
    """Fetch the complete list of teachers with pagination handling."""
    teacher_url = f"{base_url}/api/teachers?PageSize={pageSize}"
    headers = {"Authorization": f"Bearer {token}"}
    pageNo = 1
    all_teachers = []
    
    while True:
        # Add the current page number to the URL
        paginated_url = f"{teacher_url}&PageNo={pageNo}"
        response = requests.get(paginated_url, headers=headers, verify=False)
        response.raise_for_status()
        
        data = response.json()
        result_set = data.get("ResultSet", [])
        all_teachers.extend(result_set)
        
        # Pagination information
        pagination_info = {
            "HasPageInfo": data.get("HasPageInfo", False),
            "NumResults": data.get("NumResults", 0),
            "FirstRec": data.get("FirstRec", 1),
            "LastRec": data.get("LastRec", pageSize),
            "PageSize": data.get("PageSize", pageSize),
            "PageNo": data.get("PageNo", pageNo),
            "IsLastPage": data.get("IsLastPage", True),
            "LastPage": data.get("LastPage", 1),
            "Tag": data.get("Tag", None)
        }
        
        # Check if this is the last page
        if pagination_info["IsLastPage"]:
            break
        
        # Move to the next page
        pageNo += 1

    return all_teachers, pagination_info

# Test fetching the complete teachers list with pagination
all_teachers, pagination_info = get_teachers_list(base_url, token, pageSize=50)
print(f"Total teachers retrieved: {len(all_teachers)}")
print("Pagination Info:", pagination_info)


# %%
import os
import pickle

# Ensure the folder exists
cache_dir = "cached-data"
os.makedirs(cache_dir, exist_ok=True)

# Save the variable
with open(os.path.join(cache_dir, "all_teachers.pkl"), "wb") as f:
    pickle.dump(all_teachers, f)

print("✅ all_teachers saved to cached-data/all_teachers.pkl")


# %%
all_teachers[1]

# %%
