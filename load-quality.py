"""This module acts to load, process, and insert 
data into the database for the quality data."""

import sys
import pandas as pd
import psycopg
import numpy as np
import csv
import credentials


def cleaning_data(data_sub):
    """Cleaning negative and na values"""
    # Set negative rating value and not available value to None
    data_sub.loc[data_sub["Hospital overall rating"] < '0',
                 "Hospital overall rating"] = None
    data_sub.loc[data_sub["Hospital overall rating"] == 'Not Available',
                 "Hospital overall rating"] = None
    # Replace missing values with None
    data_sub = data_sub.replace(np.nan, None)
    return data_sub


def add_date(data_sub):
    # Implement the date from the sys arg input
    data_sub["date"] = sys.argv[1]
    return data_sub


def convert_to_string(data_sub):
    """Converting column values to string"""
    data_sub["Address"] = data_sub["Address"].astype("string")
    data_sub["City"] = data_sub["City"].astype("string")
    data_sub["ZIP Code"] = data_sub["ZIP Code"].astype("string")
    data_sub["State"] = data_sub["State"].astype("string")
    data_sub["Facility Name"] = data_sub["Facility Name"].astype("string")
    data_sub["Facility ID"] = data_sub["Facility ID"].astype("string")
    data_sub["Hospital Type"] = data_sub["Hospital Type"].astype("string")
    data_sub["Hospital Ownership"] = \
        data_sub["Hospital Ownership"].astype("string")
    data_sub["Emergency Services"] = \
        data_sub["Emergency Services"].astype("string")
    data_sub["Hospital overall rating"] = \
        data_sub["Hospital overall rating"].astype("float")
    data_sub["date"] = data_sub["date"].astype("string")
    # Any na values replaced with none
    data_sub = data_sub.fillna(np.nan).replace([np.nan], None)
    return data_sub


def insert_to_psql(data_sub):
    conn = psycopg.connect(
        host="sculptor.stat.cmu.edu", dbname=credentials.DB_USER,
        user=credentials.DB_USER, password=credentials.DB_PASSWORD
    )
    cur = conn.cursor()
    num_rows_inserted = 0
    with conn.transaction():
        with open("failed_rows.csv", "w") as f:
            f = csv.writer(f, delimiter=',', quotechar='"',
                           quoting=csv.QUOTE_MINIMAL)
        for i in range(len(data_sub)):
            try:
                with conn.transaction():
                    # Creating datasets for inserting into tables
                    data_set0 = {'hospital_pk': data_sub.loc[i, 'Facility ID'],
                                 'hospital_name': data_sub.loc[i, 'Facility Name']}
                    data_set4 = {'facility_ID': data_sub.loc[i, 'Facility ID'],
                                 'address': data_sub.loc[i, 'Address'],
                                 'city': data_sub.loc[i, 'City'],
                                 'zip': data_sub.loc[i, 'ZIP Code'],
                                 'state': data_sub.loc[i, 'State']}
                    data_set = {'facility_ID': data_sub.loc[i, 'Facility ID'],
                                'hospital_type': data_sub.loc[i, 'Hospital Type'],
                                'hospital_ownership':
                                data_sub.loc[i, 'Hospital Ownership'],
                                'emergency_services':
                                data_sub.loc[i, 'Emergency Services']}
                    data_set1 = {'facility_ID': data_sub.loc[i, 'Facility ID'],
                                 'date': data_sub.loc[i, 'date'],
                                 'hospital_overall_rating':
                                 data_sub.loc[i, 'Hospital overall rating']}
                    # Execute commands with conflicts in case Hospital/Facility ID already exists
                    cur.execute("INSERT INTO Hospital_geographic_infor_hhs (hospital_pk)"
                                "VALUES(%(hospital_pk)s)ON CONFLICT (hospital_pk) DO NOTHING", data_set0)
                    cur.execute("INSERT INTO Hospital_basic_infor (hospital_pk, hospital_name)"
                                "VALUES(%(hospital_pk)s, %(hospital_name)s)ON CONFLICT (hospital_pk) DO UPDATE SET hospital_name = EXCLUDED.hospital_name",
                                data_set0)
                    cur.execute("INSERT INTO Hospital_geographic_infor_quality (facility_ID, "
                                "address, city, zip, state) VALUES (%(facility_ID)s,"
                                "%(address)s, %(city)s, %(zip)s, %(state)s)ON CONFLICT (facility_ID) DO UPDATE SET facility_ID = EXCLUDED.facility_ID",
                                data_set4)
                    cur.execute("INSERT INTO Hospital_type_infor (facility_ID, "
                                "hospital_type, hospital_ownership, emergency_services) "
                                "VALUES (%(facility_ID)s, %(hospital_type)s, "
                                "%(hospital_ownership)s, %(emergency_services)s)ON CONFLICT (facility_ID) DO UPDATE SET facility_ID = EXCLUDED.facility_ID",
                                data_set)
                    cur.execute("INSERT INTO Quality_rating (facility_ID, "
                                "date, hospital_overall_rating) "
                                "VALUES (%(facility_ID)s, %(date)s, "
                                "%(hospital_overall_rating)s)", data_set1)
            except Exception as e:
                # Print exception line when raised and row cannot be inserted
                print("The", i,  "line can not be inserted!")
                f.writerow(data_sub.iloc[i])
            else:
                num_rows_inserted += 1
    conn.commit()
    print("The number of rows inserted: ", num_rows_inserted)
    if len(list(pd.read_csv("failed_rows.csv"))) == 0:
        print("All data have been inserted successfully")
    else:
        print("The number of rows failed to be insert: ",
              len(list(pd.read_csv("failed_rows.csv"))))


# Read in csv file from command line
df = pd.read_csv(sys.argv[2])
# Create subset with only variables needed to insert into tables
df_sub = df[["Facility ID", "Hospital Type", "Hospital Ownership",
             "Emergency Services", "Hospital overall rating", "Address", "City", "ZIP Code", "State", "Facility Name"]]
data_sub = cleaning_data(df_sub)
data_sub = add_date(data_sub)
data_sub = convert_to_string(data_sub)
insert_to_psql(data_sub)
