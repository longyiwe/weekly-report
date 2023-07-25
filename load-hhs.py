"""This module acts to load, process, and insert
data into the database for the hhs data."""
import psycopg
import pandas as pd
import sys
import numpy as np
import csv
#import credentials


def clean_geocoded_point(data_sub):
    """change geocoded points into longitude and latitude"""
    longitude = []
    latitude = []
    # Iterate through hospital addresses
    for row in data_sub.geocoded_hospital_address:
        # Split each address in two across the point
        y = str(row).strip('POINT( )').split(' ')
        longitude.append(y[0])
        latitude.append(y[-1])
    # Change lists to numpy arrays
    longitude = np.array(longitude)
    latitude = np.array(latitude)
    return longitude, latitude


def clean_collection_week(data_sub):
    """Create a pandas data frame from the collection week"""
    data_sub['collection_week'].apply(pd.to_datetime)
    return data_sub


def clean_data(data_sub):
    """Parse collection week to datetime variable (had to change schema so date is Date type not Time)"""
    data_sub = clean_collection_week(data_sub)

    """Change negative values to None for numeric variables"""
    data_sub.loc[data_sub.loc[:, "all_adult_hospital_beds_7_day_avg"] < 0,
                 "all_adult_hospital_beds_7_day_avg"] = None
    data_sub.loc[data_sub.loc[:, "all_pediatric_inpatient_beds_7_day_avg"] < 0,
                 "all_pediatric_inpatient_beds_7_day_avg"] = None
    data_sub.loc[data_sub.loc[:, "all_adult_hospital_inpatient_bed_occupied_7_day_coverage"] < 0,
                 "all_adult_hospital_inpatient_bed_occupied_7_day_coverage"] = None
    data_sub.loc[data_sub.loc[:, "all_pediatric_inpatient_bed_occupied_7_day_avg"] < 0,
                 "all_pediatric_inpatient_bed_occupied_7_day_avg"] = None
    data_sub.loc[data_sub.loc[:, "total_icu_beds_7_day_avg"] < 0,
                 "total_icu_beds_7_day_avg"] = None
    data_sub.loc[data_sub.loc[:, "icu_beds_used_7_day_avg"] < 0,
                 "icu_beds_used_7_day_avg"] = None
    data_sub.loc[data_sub.loc[:, "inpatient_beds_used_covid_7_day_avg"] < 0,
                 "inpatient_beds_used_covid_7_day_avg"] = None
    data_sub.loc[data_sub.loc[:, "staffed_icu_adult_patients_confirmed_covid_7_day_avg"] < 0,
                 "staffed_icu_adult_patients_confirmed_covid_7_day_avg"] = None

    """create two new columns in the data_sub to contain longitude and latitude"""
    longitude, latitude = clean_geocoded_point(data_sub)
    data_sub.loc[:, "longitude"] = longitude
    data_sub.loc[:, "latitude"] = latitude
    # Set nan values to None
    data_sub.loc[data_sub.loc[:, "longitude"] == 'nan',
                 "longitude"] = None
    data_sub.loc[data_sub.loc[:, "latitude"] == 'nan',
                 "latitude"] = None
    """Remove any na values in dataset and change to None"""
    data_sub = data_sub.replace(np.nan, None)
    return data_sub


def convert_to_string(data_sub):
    # Convert vars' types
    data_sub["hospital_name"] = data_sub["hospital_name"].astype("string")
    data_sub["hospital_pk"] = data_sub["hospital_pk"].astype("string")
    data_sub["fips_code"] = data_sub["fips_code"].astype("string")
    data_sub["longitude"] = data_sub["longitude"].astype("string")
    data_sub["latitude"] = data_sub["latitude"].astype("string")
    data_sub["all_adult_hospital_beds_7_day_avg"] = data_sub["all_adult_hospital_beds_7_day_avg"].astype(
        "float")
    data_sub["all_pediatric_inpatient_beds_7_day_avg"] = data_sub["all_pediatric_inpatient_beds_7_day_avg"].astype(
        "float")
    data_sub["all_pediatric_inpatient_bed_occupied_7_day_avg"] = data_sub[
        "all_pediatric_inpatient_bed_occupied_7_day_avg"].astype("float")
    data_sub["all_adult_hospital_inpatient_bed_occupied_7_day_coverage"] = data_sub[
        "all_adult_hospital_inpatient_bed_occupied_7_day_coverage"].astype("float")
    data_sub["total_icu_beds_7_day_avg"] = data_sub["total_icu_beds_7_day_avg"].astype(
        "float")
    data_sub["icu_beds_used_7_day_avg"] = data_sub["icu_beds_used_7_day_avg"].astype(
        "float")
    data_sub["inpatient_beds_used_covid_7_day_avg"] = data_sub["inpatient_beds_used_covid_7_day_avg"].astype(
        "float")
    data_sub["staffed_icu_adult_patients_confirmed_covid_7_day_avg"] = data_sub[
        "staffed_icu_adult_patients_confirmed_covid_7_day_avg"].astype("float")
    # Check again for nan values, replace with None
    data_sub = data_sub.fillna(np.nan).replace([np.nan], None)
    return data_sub


def insert_to_psql(data_sub):
    """Inserting data into SQL tables"""
    num_rows_inserted = 0
    # Connect to psycopg
    conn = psycopg.connect(
        host="sculptor.stat.cmu.edu", dbname=credentials.DB_USER,
        user=credentials.DB_USER, password=credentials.DB_PASSWORD
    )

    cur = conn.cursor()

    with conn.transaction():
        with open("failed_rows.csv", "w") as f:
            f = csv.writer(f, delimiter=',', quotechar='"',
                           quoting=csv.QUOTE_MINIMAL)
        for i in range(len(data_sub)):
            try:
                with conn.transaction():
                    # Create data sets for inserting into different tables
                    data_set1 = {'hospital_pk': data_sub.loc[i, 'hospital_pk'],
                                 'fips_code': data_sub.loc[i, 'fips_code'],
                                 'longitude': data_sub.loc[i, 'longitude'],
                                 'latitude': data_sub.loc[i, 'latitude']}
                    data_set2 = {'hospital_pk': data_sub.loc[i, 'hospital_pk'],
                                 'hospital_name': data_sub.loc[i, 'hospital_name']}
                    data_set3 = {'hospital_pk': data_sub.loc[i, 'hospital_pk'],
                                 'collection_week': data_sub.loc[i, 'collection_week'],
                                 'all_adult_hospital_beds_7_day_avg': data_sub.loc[i, 'all_adult_hospital_beds_7_day_avg'],
                                 'all_pediatric_inpatient_beds_7_day_avg': data_sub.loc[i, 'all_pediatric_inpatient_beds_7_day_avg'],
                                 'all_adult_hospital_inpatient_bed_occupied_7_day_coverage': data_sub.loc[i, 'all_adult_hospital_inpatient_bed_occupied_7_day_coverage'],
                                 'all_pediatric_inpatient_bed_occupied_7_day_avg': data_sub.loc[i, 'all_pediatric_inpatient_bed_occupied_7_day_avg'],
                                 'total_icu_beds_7_day_avg': data_sub.loc[i, 'total_icu_beds_7_day_avg'],
                                 'icu_beds_used_7_day_avg': data_sub.loc[i, 'icu_beds_used_7_day_avg'],
                                 'inpatient_beds_used_covid_7_day_avg': data_sub.loc[i, 'inpatient_beds_used_covid_7_day_avg'],
                                 'staffed_icu_adult_patients_confirmed_covid_7_day_avg': data_sub.loc[i, 'staffed_icu_adult_patients_confirmed_covid_7_day_avg']}
                    # Execute commands with conflicts in case Hospital/Facility ID already exists
                    cur.execute("INSERT INTO Hospital_geographic_infor_hhs (hospital_pk, fips_code, longitude, latitude) VALUES (%(hospital_pk)s, %(fips_code)s,%(longitude)s,%(latitude)s)ON CONFLICT (hospital_pk) DO UPDATE SET hospital_pk = EXCLUDED.hospital_pk",
                                data_set1)
                    cur.execute("INSERT INTO Hospital_basic_infor (hospital_pk, "
                                "hospital_name) VALUES (%(hospital_pk)s, %(hospital_name)s)ON CONFLICT (hospital_pk) DO UPDATE SET hospital_pk = EXCLUDED.hospital_pk",
                                data_set2)
                    cur.execute("INSERT INTO Weekly_capacity_infor (hospital_pk, collection_week, all_adult_hospital_beds_7_day_avg, all_pediatric_inpatient_beds_7_day_avg,"
                                "all_adult_hospital_inpatient_bed_occupied_7_day_coverage, all_pediatric_inpatient_bed_occupied_7_day_avg,"
                                "total_icu_beds_7_day_avg, icu_beds_used_7_day_avg,inpatient_beds_used_covid_7_day_avg,"
                                "staffed_icu_adult_patients_confirmed_covid_7_day_avg) VALUES (%(collection_week)s,%(all_adult_hospital_beds_7_day_avg)s, "
                                "%(all_pediatric_inpatient_beds_7_day_avg)s, %(all_adult_hospital_inpatient_bed_occupied_7_day_coverage)s,"
                                "%(all_pediatric_inpatient_bed_occupied_7_day_avg)s, %(total_icu_beds_7_day_avg)s,"
                                "%(icu_beds_used_7_day_avg)s, %(inpatient_beds_used_covid_7_day_avg)s, %(staffed_icu_adult_patients_confirmed_covid_7_day_avg)s)",
                                data_set3)
            # Print exception line when raised and row cannot be inserted
            except Exception as e:
                print("The", i,  "line can not be inserted!")
                f.writerow(data_sub.iloc[i])
            else:
                # Enumerate number of inserted rows to later print
                num_rows_inserted += 1
    conn.commit()
    print("The number of rows inserted: ", num_rows_inserted)
    if len(list(pd.read_csv("failed_rows.csv"))) == 0:
        print("All data have been inserted successfully")
    else:
        print("The number of rows failed to be insert: ",
              len(list(pd.read_csv("failed_rows.csv"))))


# Read in csv file from command line
df = pd.read_csv(sys.argv[1])
# Create subset with only variables needed to insert into tables
df_sub = df[['hospital_pk',
             'hospital_name',
             'fips_code',
             'geocoded_hospital_address',
             'collection_week',
             'all_adult_hospital_beds_7_day_avg',
             'all_pediatric_inpatient_beds_7_day_avg',
             'all_adult_hospital_inpatient_bed_occupied_7_day_coverage',
             'all_pediatric_inpatient_bed_occupied_7_day_avg',
             'total_icu_beds_7_day_avg', 'icu_beds_used_7_day_avg',
             'inpatient_beds_used_covid_7_day_avg',
             'staffed_icu_adult_patients_confirmed_covid_7_day_avg']]

data_sub = clean_data(df_sub)
data_sub = convert_to_string(data_sub)
insert_to_psql(data_sub)
