# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.16.7
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
# Not needed for now...

def get_school_list(base_url, token, pageSize=50):
    """Fetch the complete list of schools with pagination handling."""
    school_url = f"{base_url}/api/schools?PageSize={pageSize}"
    headers = {"Authorization": f"Bearer {token}"}
    pageNo = 1
    all_schools = []
    
    while True:
        # Add the current page number to the URL
        paginated_url = f"{school_url}&PageNo={pageNo}"
        response = requests.get(paginated_url, headers=headers, verify=False)
        response.raise_for_status()
        
        data = response.json()
        result_set = data.get("ResultSet", [])
        all_schools.extend(result_set)
        
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

    return all_schools, pagination_info

# Test fetching the complete school list with pagination
all_schools, pagination_info = get_school_list(base_url, token, pageSize=50)
print(f"Total schools retrieved: {len(all_schools)}")
print("Pagination Info:", pagination_info)


# %%
def get_school_list_filter(base_url, token, pageNo=1, pageSize=50, columnSet=1, sortColumn="SchNo", sortDirection="asc"):
    """Fetch a filtered list of schools with customizable parameters and return results with pagination info."""
    school_url = f"{base_url}/api/schools/collection/filter"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*"
    }
    payload = {
        "pageNo": pageNo,
        "pageSize": pageSize,
        "columnSet": columnSet,
        "sortColumn": sortColumn,
        "sortDirection": sortDirection
    }
    response = requests.post(school_url, headers=headers, json=payload, verify=False)
    response.raise_for_status()
    
    data = response.json()
    result_set = data.get("ResultSet", [])
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
    
    return result_set, pagination_info

# Test fetching filtered school list with pagination info
filtered_school_list, pagination_info = get_school_list_filter(base_url, token, pageNo=1, pageSize=500, columnSet=1, sortColumn="SchNo", sortDirection="asc")
print(f"Retrieved {len(filtered_school_list)} schools.")
print("Pagination Info:", pagination_info)


# %%
def get_school_info_for_year(school_list, survey_year=2024):
    """Retrieve a list of dictionaries with 'schNo' and 'schName' where 'svyYear' matches the specified survey year."""
    return [{'schNo': school['schNo'], 'schName': school['schName']} for school in school_list if school['svyYear'] == survey_year]

# Assuming 'filtered_school_list' is the ResultSet from get_school_list_filter function
filtered_school_list, pagination_info = get_school_list_filter(base_url, token, pageNo=1, pageSize=500, columnSet=1, sortColumn="schNo", sortDirection="asc")

# Retrieve list of dictionaries with 'schNo' and 'schName' for 'svyYear' = 2024
school_info_2024 = get_school_info_for_year(filtered_school_list, survey_year=2024)
print("Schools with survey year 2024:", school_info_2024)


# %%
from tqdm.notebook import tqdm
import requests
import os

def reload_pdfsurvey(base_url, token, output_dir, schno, schname, survey_year):
    """Download a PDF survey for a specific school and survey year into the configured output directory."""
    # Create the output directory if it does not exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Define the URL for the PDF download
    pdf_url = f"{base_url}/api/pdfSurvey/reload/{schno}/{survey_year}?TargetYear"
    headers = {"Authorization": f"Bearer {token}"}
    
    # Make the API request to download the PDF
    response = requests.get(pdf_url, headers=headers, stream=True, verify=False)
    
    # Check if the PDF is successfully fetched
    if response.status_code == 200:
        # Create a valid filename using the school name and number, replacing spaces and restricted characters
        safe_schname = schname.replace(" ", "-").replace("/", "-")
        file_path = os.path.join(output_dir, f"{safe_schname}-{schno}-{survey_year}.pdf")
        
        # Save the PDF to the specified output directory
        with open(file_path, 'wb') as pdf_file:
            for chunk in response.iter_content(chunk_size=8192):
                pdf_file.write(chunk)
    else:
        tqdm.write(f"Failed to download PDF for school {schno}: {response.status_code}")

# Download PDFs for each school in the 2024 list, with a progress bar
survey_year = "2024"
for school in tqdm(school_info_2024, desc="Downloading PDFs"):
    reload_pdfsurvey(base_url, token, output_dir, school['schNo'], school['schName'], survey_year)

print("All PDFs downloaded for 2024.")

# Testing with a single school
reload_pdfsurvey(base_url, token, output_dir, 'KSSS009', 2024)
