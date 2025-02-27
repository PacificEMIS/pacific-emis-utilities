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
import pandas as pd
import os  # Importing os to work with paths
import json

def load_config(config_path="config.json"):
    """Load configuration from a JSON file."""
    with open(config_path, 'r') as file:
        config = json.load(file)
    return config['un_pop_division_api_token']

# Test loading configuration
un_pop_division_api_token = load_config()
print("Configuration loaded successfully.")

# Define the folder path and file name separately
folder_path = "/mnt/h/Development/Pacific EMIS/repositories-data/pacific-emis-utilities/RMI"

# Store the API Bearer Token (Replace 'your_token_here' with the actual token)
API_TOKEN = un_pop_division_api_token

# Define the base URL
BASE_URL = "https://population.un.org/dataportalapi/api/v1/data/"

# Define headers to use in all requests
HEADERS = {
    'Authorization': f'Bearer {API_TOKEN}'
}

print("API Token and Headers set successfully.")

# %%
import requests

# Base URL for retrieving indicators
base_url = "https://population.un.org/dataportalapi/api/v1/indicators"

# Initialize list to store all indicators (ID and Name)
all_indicators = []

# Start with the first page
page_number = 1

while True:
    # Make the API request with pagination
    response = requests.get(base_url, params={"pageNumber": page_number})
    
    if response.status_code == 200:
        data = response.json()
        
        # Extract ID and Name from the "data" field
        if "data" in data:
            all_indicators.extend([(item["id"], item["name"]) for item in data["data"]])

        # Check if there is another page
        if data.get("nextPage"):
            page_number += 1  # Move to the next page
        else:
            break  # No more pages, exit loop
    else:
        print(f"Error retrieving indicators: {response.status_code}, {response.text}")
        break

# Display each Indicator ID and Name side by side
print("Sample indicators:")
for ind_id, ind_name in all_indicators[:5]:
    print(f"{ind_id}: {ind_name}")

print("\nThe one we want:")
indicator_47 = next((name for ind_id, name in all_indicators if ind_id == 47), "Indicator ID 47 not found")
print(f"\n47: {indicator_47}")

# %%
# Details about indicators

import pandas as pd
import requests
import json

# Declares the base url for calling the API
base_url = "https://population.un.org/dataportalapi/api/v1"

# Creates the target URL, indicators, in this instance
target = base_url + "/indicators/"

# Get the response, which includes the first page of data as well as information on pagination and number of records
response = requests.get(target)

# Converts call into JSON
j = response.json()

# Converts JSON into a pandas DataFrame.
df = pd.json_normalize(j['data']) # pd.json_normalize flattens the JSON to accomodate nested lists within the JSON structure

# Loop until there are new pages with data
while j['nextPage'] != None:
    # Reset the target to the next page
    target = j['nextPage']

    #call the API for the next page
    response = requests.get(target)

    # Convert response to JSON format
    j = response.json()

    # Store the next page in a data frame
    df_temp = pd.json_normalize(j['data'])

    # Append next page to the data frame
    df = df.append(df_temp)

df.head(5)

# %%
import requests

# Base URL for retrieving locations
base_url = "https://population.un.org/dataportalapi/api/v1/locations"

# Initialize list to store all locations (ID and Name)
all_locations = []

# Start with the first page
page_number = 1

while True:
    # Make the API request with pagination
    response = requests.get(base_url, params={"pageNumber": page_number, "sort": "id"})
    
    if response.status_code == 200:
        data = response.json()
        
        # Extract ID and Name from the "data" field
        if "data" in data:
            all_locations.extend([(item["id"], item["name"]) for item in data["data"]])

        # Check if there is another page
        if data.get("nextPage"):
            page_number += 1  # Move to the next page
        else:
            break  # No more pages, exit loop
    else:
        print(f"Error retrieving locations: {response.status_code}, {response.text}")
        break

# Display each Location ID and Name side by side
print("Sample countries:")
for loc_id, loc_name in all_locations[:5]:
    print(f"{loc_id}: {loc_name}")

print("\nThe one we want:")
location_584 = next((name for loc_id, name in all_locations if loc_id == 584), "Location ID 584 not found")
print(f"\n47: {location_584}")

# %%
import os
import requests
import pandas as pd

# Define file paths (adjust folder_path as needed)
parquet_file_name = "population_data.parquet"
parquet_file_path = os.path.join(folder_path, parquet_file_name)
excel_file_name = "population_data.xlsx"
excel_file_path = os.path.join(folder_path, excel_file_name)

# Check if the parquet file already exists
if os.path.exists(parquet_file_path):
    print("Loading data from local parquet file...")
    df = pd.read_parquet(parquet_file_path, engine="pyarrow")
else:
    print("Local data not found. Retrieving data from API...")
    # Define parameters for the API request
    indicator_id = 47   # Population by age and sex
    location_id = 584   # Marshall Islands
    start_year = 2011
    end_year = 2030

    api_url = f"{BASE_URL}indicators/{indicator_id}/locations/{location_id}/start/{start_year}/end/{end_year}"

    # Initialize an empty list to collect all data and set the page number to start at 1
    all_data = []
    page_number = 1

    while True:
        # Construct API request with paging
        paged_url = f"{api_url}?pageNumber={page_number}"
        print(f"Retrieving from {paged_url}")
        
        response = requests.get(paged_url, headers=HEADERS)
        
        if response.status_code == 200:
            data = response.json()
            
            # Extract data if present
            if "data" in data:
                all_data.extend(data["data"])  # Append new data
            
            # Check if there are more pages
            if data.get("nextPage"):
                page_number += 1  # Move to the next page
            else:
                break  # No more pages, exit loop
        else:
            print("Error retrieving data:", response.status_code, response.text)
            break

    # Convert all collected data into a DataFrame
    df = pd.DataFrame(all_data)
    print(f"Total records retrieved: {df.shape[0]} rows, {df.shape[1]} columns")
    
    # Save the DataFrame locally as both parquet and Excel files
    print(f"Saving data to parquet at: {parquet_file_path}")
    df.to_parquet(parquet_file_path, engine="pyarrow")
    print(f"Saving data to Excel at: {excel_file_path}")
    df.to_excel(excel_file_path, index=False)

# Now df is loaded either from the local file or the API, and you can proceed with further analysis.
df.head()

# %%
# --- Step 1: Create a dynamic mapping for popmodCode ---
# Get unique variants from variantShortName
unique_variants = df["variantShortName"].unique().tolist()

mapping = {}
if "Median" in unique_variants:
    # Ensure "Median" is first
    mapping["Median"] = "UNPD24V1"
    remaining = [v for v in unique_variants if v != "Median"]
else:
    remaining = unique_variants

# Sort the remaining values (alphabetical order)
remaining = sorted(remaining)

# Start numbering at 2 if Median was found, otherwise start at 1
start_index = 2 if "Median" in mapping else 1
for i, variant in enumerate(remaining, start=start_index):
    mapping[variant] = f"UNPD24V{i}"

# Create the new popmodCode column using the dynamic mapping
df["popmodCode"] = df["variantShortName"].map(mapping)

# --- Step 2: Create the mini dataset ---
# Select the columns and drop duplicate rows
df_models = df[['popmodCode', 'variantLabel', 'variant']].drop_duplicates().copy()

# Rename columns: variantLabel -> popmodName, variant -> popmodDesc
df_models = df_models.rename(columns={'variantLabel': 'popmodName', 'variant': 'popmodDesc'})

# Save on local filesystem a copy of the df_final
parquet_file_name = "population_data_models.parquet"
parquet_file_path = os.path.join(folder_path, parquet_file_name)
excel_file_name = "population_data_models.xlsx"
excel_file_path = os.path.join(folder_path, excel_file_name)

# Save only if not already there. Manually delete if you suspect you need to update it
if not os.path.exists(parquet_file_path):    
    print(f"Saving (final) data to parquet at: {parquet_file_path}")    
    df_models.to_parquet(parquet_file_path, engine="pyarrow")
    print(f"Saving (final) data to Excel at: {excel_file_path}")    
    df_models.to_excel(excel_file_path, index=False)

df_models.reset_index(drop=True, inplace=True)
# (Optional) Display the mini dataset
df_models

# %%
# Mashup this data inline with how we use it in Pacific EMIS and save it.

#df = pd.read_parquet(parquet_file_path, engine="pyarrow")
df.columns
# of interest 'variantShortName', 'timeLabel', 'sex', 'ageLabel', 'ageStart', 'value'

# Select the columns and rename them in one step
selected_columns = ['variantShortName', 'timeLabel', 'sex', 'ageLabel', 'value']
df_final = df[selected_columns].rename(columns={
    #'variantShortName': 'popmodCode',
    'timeLabel': 'popYear',
    'sex': 'gender',
    'ageLabel': 'popAge',
    'value': 'pop'
})

# Ensure popYear is treated as an integer
df_final['popYear'] = df_final['popYear'].astype(int)

# Clean the 'popAge' column: remove '+' and convert to int
df_final['popAge'] = df_final['popAge'].str.replace('+', '', regex=False).astype(int)

# Clean the population column: round to an integer (remove decimals)
df_final['pop'] = df_final['pop'].round().astype(int)

# Create a pivot table so that genders become separate columns
df_final = df_final.pivot_table(
    index=["variantShortName", "popYear", "popAge"],
    columns="gender",
    values="pop",
    aggfunc="sum"
).reset_index()

# Adjust the mapping based on how genders are labeled in data.
gender_mapping = {"Male": "popM", "Female": "popF"}
df_final.rename(columns=gender_mapping, inplace=True)

# Drop Both sexes column, it contains small difference vs Male + Female which ultimately
# will be what is used in the EMIS, so might as well line it up now.
df_final.drop("Both sexes", axis=1, inplace=True)

# Rename columns for clarity
df_final.columns = ["variantShortName", "popYear", "popAge", "popF", "popM"]

# Ages should be int
df_final["popM"] = df_final["popM"].round().astype(int)
df_final["popF"] = df_final["popF"].round().astype(int)

# Save on local filesystem a copy of the df_final
parquet_file_name = "population_data_final.parquet"
parquet_file_path = os.path.join(folder_path, parquet_file_name)
excel_file_name = "population_data_final.xlsx"
excel_file_path = os.path.join(folder_path, excel_file_name)

# Make a minor modifications for saving into local filesystem (and eventually SQL DB)
df_final_for_saving = df_final.copy()
# Create the new popmodCode column using the dynamic mapping (use same mapping as previous cell)
df_final_for_saving["popmodCode"] = df_final_for_saving["variantShortName"].map(mapping)
df_final_for_saving.drop("variantShortName", axis=1, inplace=True)
#df_final_for_saving.columns = ["popmodCode", "popYear", "popAge", "popF", "popM"]
df_final_for_saving = df_final_for_saving[["popmodCode", "popYear", "popAge", "popF", "popM"]]

# Save only if not already there. Manually delete if you suspect you need to update it
if not os.path.exists(parquet_file_path):    
    print(f"Saving (final) data to parquet at: {parquet_file_path}")
    df_final_for_saving.to_parquet(parquet_file_path, engine="pyarrow")
    print(f"Saving (final) data to Excel at: {excel_file_path}")
    df_final_for_saving.to_excel(excel_file_path, index=False)

# Display the first few rows to verify
df_final

# %%
# Produce a quick pivot table to compare with UN website table
# https://population.un.org/dataportal/data/indicators/47/locations/584/start/2011/end/2030/table/pivotbylocation?df=4e173b6f-2262-4c28-b909-17788d1d8ca4

# Sort by popYear (ascending) and popAge (ascending)
df_sorted = df_final.sort_values(by=["popYear", "popAge"], ascending=[True, True])

# Reshape data for pivoting
df_melted = df_sorted.melt(
    id_vars=["popYear", "popAge"], 
    value_vars=["popM", "popF"], 
    var_name="Gender", 
    value_name="Population"
)

# Rename Gender values for clarity
df_melted["Gender"] = df_melted["Gender"].replace({"popM": "Male", "popF": "Female"})

# Create pivot table
pivot_table = df_melted.pivot_table(
    index="popYear", 
    columns=["Gender", "popAge"], 
    values="Population",
    aggfunc="sum"
)

# Sort columns to ensure Age (0-100) is in order within each Gender
pivot_table = pivot_table.sort_index(axis=1, level=[0, 1])

# Display pivot table
pivot_table

# %%
# This cell is likely a bit buggy but it it served its purposed at the time.

import matplotlib.pyplot as plt
import matplotlib.cm as cm
import numpy as np

# --- User Options ---
# For variant_to_plot, possible options:
#   "All"                       => Plot multiple series, one for each unique variant
#   "UNPD24V1"                    => a single variant
#   ["UNPD24V1", "UNPD24V2"] => multiple variants
variant_to_plot = "All"  
#variant_to_plot = "UNPD24V1"  
#variant_to_plot = ["UNPD24V1", "UNPD24V13"]

# For age_to_plot, possible options:
#   "All"            => Aggregate all ages (single series)
#   5                => A single age
#   [5, 10, 15]      => Multiple ages (multiple series)
#age_to_plot = [5, 10, 15]  
age_to_plot = "All"
#age_to_plot = 5

# --- Preprocess Options ---
# If variant_to_plot is "All", then convert it to a list of all unique variants.
if variant_to_plot == "All":
    variant_to_plot = df_final["variantShortName"].unique().tolist()

# Now, if both dimensions are multiple series, that's not allowed.
if isinstance(age_to_plot, list) and isinstance(variant_to_plot, list):
    raise ValueError("Cannot have both age_to_plot and variant_to_plot as multiple series. Choose one dimension to vary.")

# --- Filter by Variant ---
if isinstance(variant_to_plot, list):
    # Multiple variants: keep only rows that match these variants.
    df_variant = df_final[df_final["variantShortName"].isin(variant_to_plot)].copy()
else:
    df_variant = df_final[df_final["variantShortName"] == variant_to_plot].copy()

# --- Grouping and Pivoting ---
# Case 1: Multiple series by Age
if isinstance(age_to_plot, list):
    # Filter for the selected ages.
    df_filtered = df_variant[df_variant["popAge"].isin(age_to_plot)].copy()
    # Group by popYear and popAge so each age remains separate.
    df_grouped = df_filtered.groupby(["popYear", "popAge"])[["popM", "popF"]].sum().reset_index()
    # Compute total population (in thousands).
    df_grouped["popT"] = (df_grouped["popM"] + df_grouped["popF"]) / 1000
    # Pivot so that each age becomes its own column.
    df_plot_data = df_grouped.pivot(index="popYear", columns="popAge", values="popT")
    series_label = "Age"

# Case 2: Multiple series by Variant
elif isinstance(variant_to_plot, list):
    # Here, age_to_plot must be "All" or a single age.
    if age_to_plot == "All":
        df_grouped = df_variant.groupby(["popYear", "variantShortName"])[["popM", "popF"]].sum().reset_index()
    else:
        df_filtered = df_variant[df_variant["popAge"] == age_to_plot].copy()
        df_grouped = df_filtered.groupby(["popYear", "variantShortName"])[["popM", "popF"]].sum().reset_index()
    df_grouped["popT"] = (df_grouped["popM"] + df_grouped["popF"]) / 1000
    df_plot_data = df_grouped.pivot(index="popYear", columns="variantShortName", values="popT")
    series_label = "Variant"

# Case 3: Single series (both dimensions are single or age is "All")
else:
    if age_to_plot == "All":
        df_plot_data = df_variant.groupby("popYear")[["popM", "popF"]].sum()
    else:
        df_plot_data = df_variant[df_variant["popAge"] == age_to_plot].groupby("popYear")[["popM", "popF"]].sum()
    df_plot_data["popT"] = (df_plot_data["popM"] + df_plot_data["popF"]) / 1000

# Ensure popYear is an integer type
df_plot_data.index = df_plot_data.index.astype(int)

# --- Split Data into Interpolated and Projected Segments ---
# (Interpolated: years <= 2024, Projected: years >= 2024; 2024 appears in both for continuity)
interpolated = df_plot_data.loc[df_plot_data.index <= 2024]
projected = df_plot_data.loc[df_plot_data.index >= 2024]

# --- Determine Y-axis Limit (25% above maximum) ---
if isinstance(age_to_plot, list) or isinstance(variant_to_plot, list):
    max_population = df_plot_data.max().max()
else:
    max_population = df_plot_data["popT"].max()
y_max = max_population * 1.25

# --- Plotting ---
# Double the chart size by setting a larger figure size.
fig, ax = plt.subplots(figsize=(20, 10))
# For multiple series, use the pivoted DataFrame columns as individual series.
if isinstance(age_to_plot, list) or isinstance(variant_to_plot, list):
    series_keys = df_plot_data.columns.tolist()
    cmap = plt.get_cmap('tab10', len(series_keys))
    for idx, key in enumerate(series_keys):
        color = cmap(idx)
        # Plot the Interpolated segment (solid line)
        ax.plot(interpolated.index, interpolated[key],
                label=f"{series_label} {key} Interpolated", color=color, linestyle="-")
        # Plot the Projected segment (dashed line, same color)
        ax.plot(projected.index, projected[key],
                label=f"{series_label} {key} Projected", color=color, linestyle="--")
else:
    ax.plot(interpolated.index, interpolated["popT"], label="Interpolated", color="blue", linestyle="-")
    ax.plot(projected.index, projected["popT"], label="Projected", color="blue", linestyle="--")

ax.set_xticks(df_plot_data.index)
ax.set_ylim(0, y_max)
ax.set_xlabel("Year")
ax.set_ylabel("Population (thousands)")

# Build title information
if isinstance(variant_to_plot, list):
    variant_info = f"Variants: {', '.join(variant_to_plot)}"
else:
    variant_info = f"Variant: {variant_to_plot}"
if isinstance(age_to_plot, list):
    title_info = f"Ages: {', '.join(map(str, age_to_plot))}"
else:
    title_info = f"Age: {age_to_plot}" if age_to_plot != "All" else "All Ages"

ax.set_title(f"Population Trends ({variant_info}, {title_info})")
ax.grid(True)
# Place the legend outside the plot on the right.
ax.legend(loc="center left", bbox_to_anchor=(1, 0.5))

plt.show()


# %%
