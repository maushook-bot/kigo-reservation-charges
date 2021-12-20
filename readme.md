# Readme

## Multi-Processor Kigo Reservation Charges Importer API
Business Requirements:-

Special Request Reservation Charges Data Importer from Kigo PMS System.

## Introduction:-

##### Source SQL for Charges Extraction => Local/ Stage Env

    SELECT * FROM src_initial_kigo_reservation_report;
    

##### Splitting the Datasets:-

    # TOTAL HISTORICAL RECORDS => 150371
    # RUN-1 => 1 - 20000
    # RUN-2 => 20001 - 40000
    # RUN-3 => 40001 - 60000
    # RUN-4 => 60001 - 80000
    # RUN-5 => 80001 - 100000
    # RUN-6 => 100001 - 120000
    # RUN-7 => 120001 - 150376
    
    SELECT * FROM src_hist_kigo_reservation_report f;
    
    # RUN-1 => 1 - 20000
    SELECT * FROM src_hist_kigo_reservation_report f
    WHERE f.id BETWEEN 1 AND 20000;
    
    # RUN-2 => 20001 - 40000
    SELECT * FROM src_hist_kigo_reservation_report f
    WHERE f.id BETWEEN 20001 AND 40000;
    
    # RUN-3 => 40001 - 60000
    SELECT * FROM src_hist_kigo_reservation_report f
    WHERE f.id BETWEEN 40001 AND 60000;
    
    # RUN-4 => 60001 - 80000
    SELECT * FROM src_hist_kigo_reservation_report f
    WHERE f.id BETWEEN 60001 AND 80000;
    
    # RUN-5 => 80001 - 100000
    SELECT * FROM src_hist_kigo_reservation_report f
    WHERE f.id BETWEEN 80001 AND 100000;
    
    # RUN-6 => 100001 - 120000
    SELECT * FROM src_hist_kigo_reservation_report f
    WHERE f.id BETWEEN 100001 AND 120000;
    
    # RUN-7 => 120001 - 150376
    SELECT * FROM src_hist_kigo_reservation_report f
    WHERE f.id BETWEEN 120001 AND 150376;
    
    ################### SQL DUMPS #####################
    SELECT DISTINCT r.id, r.reservation_id
    FROM src_kigo_import_data_run1_1 r 
    LEFT JOIN tia_kigo_reservation_charges_import_processing_run1_1 f ON r.reservation_id = f.reservation_id
    WHERE f.reservation_id IS NULL;
    
    
    
    SELECT * FROM src_kigo_import_data_run1_2;
    
    
    SELECT DISTINCT reservation_id
    FROM tia_kigo_reservation_charges_import_processing_run1_2;
    
    
    SELECT DISTINCT r.id, r.reservation_id
    FROM src_kigo_import_data_run1_2 r 
    LEFT JOIN tia_kigo_reservation_charges_import_processing_run1_2 f ON r.reservation_id = f.reservation_id
    WHERE f.reservation_id IS NULL;
    
    
    SELECT DISTINCT r.id, r.reservation_id
    FROM src_kigo_import_data_run1_3 r 
    LEFT JOIN tia_kigo_reservation_charges_import_processing_run1_3 f ON r.reservation_id = f.reservation_id
    WHERE f.reservation_id IS NULL;
    
    
    SELECT DISTINCT r.id, r.reservation_id
    FROM src_kigo_import_data_run1_4 r 
    LEFT JOIN tia_kigo_reservation_charges_import_processing_run1_4 f ON r.reservation_id = f.reservation_id
    WHERE f.reservation_id IS NULL;
    
    
    CREATE TABLE src_kigo_import_data_run1 AS 
    SELECT * FROM src_kigo_import_data_run1_1
    union
    SELECT * FROM src_kigo_import_data_run1_2
    union
    SELECT * FROM src_kigo_import_data_run1_3
    union
    SELECT * FROM src_kigo_import_data_run1_4
    union
    SELECT * FROM src_kigo_import_data_run1_5;
    
    
    CREATE TABLE tia_kigo_reservation_charges_import_processing_run1 AS 
    SELECT * FROM tia_kigo_reservation_charges_import_processing_run1_1
    union
    SELECT * FROM tia_kigo_reservation_charges_import_processing_run1_2
    union
    SELECT * FROM tia_kigo_reservation_charges_import_processing_run1_3
    union
    SELECT * FROM tia_kigo_reservation_charges_import_processing_run1_4
    union
    SELECT * FROM tia_kigo_reservation_charges_import_processing_run1_5;
    
    
    SELECT
    DISTINCT f.reservation_id
    FROM src_kigo_import_data_run1 f
    JOIN tia_kigo_reservation_charges_import_processing_run1 r ON r.reservation_id = f.reservation_id
    


##### Output Processing Table => Local / Stage Env

    SELECT * FROM tia_kigo_reservation_charges_import_processing;
    
- api_response: 1 => Transaction Found
- api_response: -1 => Transaction is NULL   

###### Configuration Parameters - Post TRACK Master Folios Importer API:-
Configuration Parameters:-
1. domain = "realjoy"
2. local_db_name = "realjoy"
3. environment = "mash_sandbox"
4. sheet_name = ""
5. gsheet_url = ""
