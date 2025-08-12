# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.2
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
# Pull in configuration and set a few things up
import json
import os

import pandas as pd
import random
from datetime import datetime, timedelta
import xlwings as xw
from openpyxl import load_workbook
from openpyxl.utils.cell import coordinate_from_string, column_index_from_string

def load_config(config_path="config.json"):
    """Load configuration from a JSON file."""
    with open(config_path, 'r') as file:
        config = json.load(file)
    return config['base_url'], config['username'], config['password'], config['output_directory'], config['pd_directory']

# Test loading configuration
base_url, username, password, output_dir, pd_directory = load_config()

# Filenames and paths
input_filename = "PD-source-data-workbook.xlsx"
output_filename = "PD-source-data-workbook-filled-sample.xlsx"
input_path = os.path.join(pd_directory, input_filename)
output_path = os.path.join(pd_directory, output_filename)

print("Configuration loaded successfully.")

# %%
# Extract columns and lists (i.e. used as dropdown validations)

# Use openpyxl to read list values (xlwings doesn't support that easily)
wb_ox = load_workbook(filename=input_path)
list_sheet = wb_ox["Lists"]

# Extract validation lists using header cell position
def extract_list_values_from_header(header_cell):
    col_letter, start_row = coordinate_from_string(header_cell)
    col_index = column_index_from_string(col_letter)
    values = []
    row = start_row + 1
    while list_sheet.cell(row=row, column=col_index).value:
        values.append(list_sheet.cell(row=row, column=col_index).value)
        row += 1
    return values

def extract_list_from_column(col_letter, start_row):
    values = []
    while list_sheet[f"{col_letter}{start_row}"].value:
        values.append(list_sheet[f"{col_letter}{start_row}"].value)
        start_row += 1
    return values

# Load all dropdown lists
pd_types = extract_list_values_from_header("C4")
pd_formats = extract_list_values_from_header("E4")
pd_focuses = extract_list_values_from_header("G4")
genders = extract_list_values_from_header("M4")
schools = extract_list_values_from_header("K4")
years_teaching = extract_list_values_from_header("A15")

print("pd_types:", pd_types[:5])
print("pd_formats:", pd_formats[:5])
print("pd_focuses:", pd_focuses[:5])
print("genders:", genders[:5])
print("schools:", schools[:5])
print("years_teaching:", years_teaching[:5])

# Use xlwings to read and write the PD Data sheet
wb = xw.Book(input_path)
ws = wb.sheets["PD data"]
# Read exact column headers from row 1
exact_column_order = ws.range("A1").expand("right").value
print("exact_column_order length:", len(exact_column_order))
print("exact_column_order:", exact_column_order)

# %%
# Get all teachers from EMIS. This depends on notebook teachers.ipynb which needs to run at least once
# to pickle the data locally.
import os
import pickle

cache_dir = "cached-data"

with open(os.path.join(cache_dir, "all_teachers.pkl"), "rb") as f:
    all_teachers = pickle.load(f)

# Optional: filter valid teachers right away
valid_teachers = [
    t for t in all_teachers
    if t['tPayroll'] and t['tSex'] and t['tGiven'] and t['tSurname']
]

print(f"Loaded {len(valid_teachers)} valid teachers from cache.")


# %%
# === More realistic PD data generation by school groups ===
def generate_teacher_rows_by_school(pd_type, year, schools, valid_teachers):
    from datetime import datetime, timedelta

    start_date = datetime(year, 5, 5)
    duration_days = random.choice([5, 10, 15])
    end_date = start_date + timedelta(days=duration_days - 1)
    duration_hours = duration_days * 8

    fixed_pd_format = random.choice(pd_formats)
    fixed_pd_focus = random.choice(pd_focuses)
    location = "South Tarawa"

    # Pick a realistic number of schools attending (e.g., 3 to 5)
    num_schools = random.randint(3, 5)
    selected_schools = random.sample(schools, num_schools)

    rows = []

    for sch in selected_schools:
        # Generate 5–10 teachers per selected school
        teachers_in_school = random.randint(5, 10)
        total_teachers_in_school = int(random.triangular(30, 40, 30))

        for i in range(teachers_in_school):
            use_real_teacher = random.random() < 0.5

            if use_real_teacher and valid_teachers:
                t = random.choice(valid_teachers)
                pf_number = t['tPayroll']
                first_name = t['tGiven']
                last_name = t['tSurname']
                gender = 'Male' if t['tSex'] == 'M' else 'Female'
            else:
                pf_number = random.randint(1000000, 9999999)
                first_name = f"TeacherFirst{i}"
                last_name = f"TeacherLast{i}"
                gender = random.choice(genders)

            disability = "No" if random.random() > 0.1 else "Yes"
            years_teaching_val = random.choice(years_teaching)
            attendance_80 = "Yes" if random.random() >= 0.2 else "No"
            completion = "Yes" if attendance_80 == "Yes" else "No"

            row = [
                pd_type,
                fixed_pd_format,
                fixed_pd_focus,
                location,
                year,
                start_date.date(),
                end_date.date(),
                duration_days,
                duration_hours,
                pf_number,
                first_name,
                last_name,
                gender,
                disability,
                years_teaching_val,
                attendance_80,
                completion,
                sch,
                total_teachers_in_school,
                "Optional",
                "Optional"
            ]
            rows.append(row)

    return rows

# 👇 Sample use of the function
sample_pd_type = "Positive Discipline"
sample_year = 2025

rows = generate_teacher_rows_by_school(sample_pd_type, sample_year, schools, valid_teachers)

# 🔍 Sanity check
num_teachers = len(rows)
sample_schools = set(r[18] for r in rows)  # school name/code column is index 18
num_schools = len(sample_schools)

print(f"Generated {num_teachers} teachers across {num_schools} schools.")
print("Sample row:", rows[0])

# Make sure your Excel header order is loaded
df = pd.DataFrame(rows, columns=exact_column_order)
display(df.head())

# %%
# Clear old data and write new data to workbook.
start_cell = "A2"
data_range = ws.range(start_cell).expand("table")
data_range.clear_contents()

# Write the DataFrame values (without header)
ws.range(start_cell).value = df.values.tolist()

# Save and close
wb.save(output_path)
wb.close()

# %%
# %%time
# Generate for many years and worktypes of PD
# Five years and 8 different types of PD takes about 3min 27sec on the iMac

# Excel instance for batch processing
app = xw.App(visible=False)
app.display_alerts = False
app.screen_updating = False

try:
    for year in range(2020, 2026):  # 2020 to 2025 inclusive
        for pd_type in pd_types:
            print(f"Generating for Year: {year}, PD Type: {pd_type}")
    
            # Load fresh Excel workbook
            wb = app.books.open(input_path)
            ws = wb.sheets["PD data"]
            exact_column_order = ws.range("A1").expand("right").value
    
            # 🚀 Generate realistic sample data
            rows = generate_teacher_rows_by_school(pd_type, year, schools, valid_teachers)
            df = pd.DataFrame(rows, columns=exact_column_order)
    
            # Clear and write new data
            start_cell = "A2"
            ws.range(start_cell).expand("table").clear_contents()
            ws.range(start_cell).value = df.values.tolist()
    
            # Save workbook
            safe_pd_name = pd_type.replace(" ", "_").replace("/", "_")
            filename = f"PD-{safe_pd_name}-{year}.xlsx"
            save_path = os.path.join(pd_directory, filename)
            print(f"Saving {save_path}...")
            wb.save(save_path)
            wb.close()
finally:
    app.quit()


# %%
