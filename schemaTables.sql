/*Group member: Longyi (Yutong) Wei, Sean Ream, Tianqi Liu*/

/* The basic entities in our group's schema are hospital geographic information, hospital basic information, hospital capacity(weekly update)
hospital type and quality rating, which each having their own database table. Geographic information was chosen as an entity which contains position 
information like state, address, city, zip, flips_code, geocoded_hosptial_address. It was chosen to be an entity because it contains information 
of the hospital position that is seperate from basic hospital information. Basic information which contains hospital_pk, hospital_name, 
flips_code( which serves as the reference) was chosen as an entity because it removes some redundant information about hospital position, 
and hospital basic information entity achieves normalization. Then, Weekly capacity information was chosen as an entity which contains hospital_pk as the 
reference, date(to show the time of update), and other variables showing capacity of the hospital which are update weekly. This was chosen as its own entity 
because these variables would likely need to be updated in a specific period of time, which need to be considered in a new entity to not interupt other attributed 
of the schema. Hospital type information is an as a entity which has variables like facility ID (as the reference), hospital type, hospital ownership and emergency 
services. This was chosen as an entity because these variables are trying to describe the type of the hospital which is spereate information. For Quality rating 
entity, variables like facility ID as the reference, date and hospital_overall_rating are included and it is an entity because the hospital_overall_rating needs 
periodical updates, but not weekly like the capacity information. By modeling the relationships between entities and including relevant attributes in 
each entity, we tried to obtain normalization and elimiate data redundancy for future hospital data. Redundant information is removed because there are not similar
attributes across entities. An example would be the geographic information for all data being located in one table entity instead of having hospital state in one 
table and facility state in another table like in the data description (bullet point 2 from hhs data, bullet point 2 from hospital quality dataset)
*/

CREATE TABLE Hospital_geographic_infor_hhs(
    hospital_pk TEXT UNIQUE PRIMARY KEY,
    fips_code TEXT,
    longitude TEXT,
    latitude TEXT
);

CREATE TABLE Hospital_geographic_infor_quality(
    facility_ID TEXT REFERENCES Hospital_geographic_infor_hhs (hospital_pk) PRIMARY KEY,
    address TEXT,
    city TEXT,
    zip TEXT,
    state CHAR(2)   
);

CREATE TABLE Hospital_basic_infor(
    hospital_pk TEXT PRIMARY KEY REFERENCES Hospital_geographic_infor_hhs(hospital_pk),
    hospital_name TEXT
);

CREATE TABLE Weekly_capacity_infor(
    id SERIAL PRIMARY KEY Not NULL,
    collection_week DATE,
    all_adult_hospital_beds_7_day_avg numeric,
    all_pediatric_inpatient_beds_7_day_avg numeric,
    all_adult_hospital_inpatient_bed_occupied_7_day_coverage numeric,
    all_pediatric_inpatient_bed_occupied_7_day_avg numeric,
    total_icu_beds_7_day_avg numeric,
    icu_beds_used_7_day_avg numeric,
    inpatient_beds_used_covid_7_day_avg numeric,
    staffed_icu_adult_patients_confirmed_covid_7_day_avg numeric
);

CREATE TABLE Hospital_type_infor(
    facility_ID TEXT REFERENCES Hospital_basic_infor (hospital_pk) PRIMARY KEY,
    hospital_type TEXT,
    hospital_ownership TEXT,
    emergency_services boolean DEFAULT FALSE
);

CREATE TABLE Quality_rating(
    ID SERIAL PRIMARY KEY,
    facility_ID TEXT REFERENCES Hospital_type_infor Not NULL,
    date DATE,
    hospital_overall_rating numeric
);
