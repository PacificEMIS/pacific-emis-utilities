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
# The data must first be retrieved before attempting to run this notebook
# Run populations.ipynb

import os
import requests
import pandas as pd

# Define the folder path and file name separately
folder_path = "/mnt/h/Development/Pacific EMIS/repositories-data/pacific-emis-utilities/RMI"

# %%
# Retrieve data
parquet_file_name = "population_data_final.parquet"
parquet_file_path = os.path.join(folder_path, parquet_file_name)

# Check if the parquet file already exists
if os.path.exists(parquet_file_path):
    print("Loading data from local parquet file...")
    df = pd.read_parquet(parquet_file_path, engine="pyarrow")

df.head(5)

# %%
# The source data does not include all the years (interpolated) for most models (only Median variant)
# Let's fill them up with the same as Median data for all missing years.

# 1) Separate out the reference (complete) model: UNPD24V1
df_ref = df[df["popmodCode"] == "UNPD24V1"].copy()

# 2) Identify the full set of years and ages from the reference model
all_years = sorted(df_ref["popYear"].unique())
all_ages = sorted(df_ref["popAge"].unique())

# 3) Build a DataFrame of all (year, age) pairs from the reference
df_index = pd.DataFrame(
    [(year, age) for year in all_years for age in all_ages],
    columns=["popYear", "popAge"]
)

# 4) Get the list of other models (i.e., everything except UNPD24V1)
other_models = df["popmodCode"].unique().tolist()
other_models = [m for m in other_models if m != "UNPD24V1"]

# We will collect a list of DataFrames and then concatenate them at the end.
list_of_dfs = []

# 5) Keep UNPD24V1 data as-is
list_of_dfs.append(df_ref[["popmodCode","popYear","popAge","popM","popF"]])

# 6) For each other model, fill missing years/ages from the reference
for model_code in other_models:
    # Get partial data for this model
    df_model = df[df["popmodCode"] == model_code][["popYear","popAge","popM","popF"]].copy()
    
    # Merge with the full index to ensure all (year, age) pairs exist
    df_filled = df_index.merge(df_model, on=["popYear","popAge"], how="left")
    
    # Merge again with reference data, renaming reference columns
    df_filled = df_filled.merge(
        df_ref[["popYear","popAge","popM","popF"]].rename(columns={"popM":"popM_ref","popF":"popF_ref"}),
        on=["popYear","popAge"],
        how="left"
    )
    
    # Fill missing popM/popF from the reference
    df_filled["popM"] = df_filled["popM"].fillna(df_filled["popM_ref"])
    df_filled["popF"] = df_filled["popF"].fillna(df_filled["popF_ref"])
    
    # Drop the reference columns
    df_filled.drop(columns=["popM_ref","popF_ref"], inplace=True)
    
    # Assign the model code
    df_filled["popmodCode"] = model_code
    
    list_of_dfs.append(df_filled)

# 7) Combine everything into a single DataFrame
df_result = pd.concat(list_of_dfs, ignore_index=True)

# 8) Reorder columns if desired
df_result = df_result[["popmodCode", "popYear", "popAge", "popM", "popF"]]

# 9) (Optional) Sort by model, then year, then age
df_result.sort_values(by=["popmodCode","popYear","popAge"], inplace=True)

# 10) Cast population columns back to integer
df_result["popM"] = df_result["popM"].astype(int)
df_result["popF"] = df_result["popF"].astype(int)

# Check the final result
#df_result[df_result['popmodCode'] == 'UNPD24V8']
df_result

# %%
# Retrieve population models
parquet_file_name = "population_data_models.parquet"
parquet_file_path = os.path.join(folder_path, parquet_file_name)

# Check if the parquet file already exists
if os.path.exists(parquet_file_path):
    print("Loading population models from local parquet file...")
    df_models = pd.read_parquet(parquet_file_path, engine="pyarrow")

df_models

# %%
# Write as SQL INSERT statements the data

# Ensure popAge is numeric, replacing "100+" with 100
#df_final["popAge"] = df_final["popAge"].apply(lambda x: int(x) if str(x).isdigit() else 100)

# Sort by popYear (ascending) and popAge (ascending)
df_sorted = df_result.sort_values(by=["popmodCode","popYear", "popAge"], ascending=[True, True, True])

# Define the SQL template WITHOUT popSum
sql_template = "INSERT INTO [dbo].[Population] ([popmodCode],[popYear],[popAge],[popM],[popF]) VALUES ('{}', {}, {}, {}, {});"

# Generate SQL statements for all records
sql_statements = [
    sql_template.format(
        str(row["popmodCode"]).replace("'", "''"),  # Ensure proper quoting for SQL
        row["popYear"], 
        row["popAge"],  # Already converted to integer in sorting step
        row["popM"], 
        row["popF"]
    )
    for _, row in df_sorted.iterrows()
]

# Define the output file path using the folder path
sql_file_name = "insert_population.sql"
sql_file_path = os.path.join(folder_path, sql_file_name)

# Save to a .sql file
with open(sql_file_path, "w") as file:
    file.write("\n".join(sql_statements))

print(f"SQL file saved at: {sql_file_path}")


# %%
# Write as SQL INSERT statements the model variants

# Sort by popYear (ascending) and popAge (ascending)
df_sorted = df_models.sort_values(by=["popmodCode"], ascending=[True])

# Define the SQL template WITHOUT popSum
sql_template = "INSERT INTO [dbo].[PopulationModel] ([popmodCode],[popmodName],[popmodDesc],[popmodDefault],[popmodEFA]) VALUES ('{}', 'UNPD 2024 Variant - {}', 'UN Population Division - Indicator Population by 1-year age groups and sex - 2024 Revision Projection Variant {}', 0, 0);"

# Generate SQL statements for all records
sql_statements = [
    sql_template.format(
        str(row["popmodCode"]).replace("'", "''"),  # Ensure proper quoting for SQL
        str(row["popmodName"]).replace("'", "''"),  # Ensure proper quoting for SQL
        str(row["popmodDesc"]).replace("'", "''")  # Ensure proper quoting for SQL
    )
    for _, row in df_sorted.iterrows()
]

# Define the output file path using the folder path
sql_file_name = "insert_population_models.sql"
sql_file_path = os.path.join(folder_path, sql_file_name)

# Save to a .sql file
with open(sql_file_path, "w") as file:
    file.write("\n".join(sql_statements))

print(f"SQL file saved at: {sql_file_path}")

# %%
