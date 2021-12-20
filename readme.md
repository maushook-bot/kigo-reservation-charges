# Readme

## Multi-Processor Kigo Reservation Charges Importer API
Business Requirements:-

Special Request Reservation Charges Data Importer from Kigo PMS System.

## Introduction:-

##### Source SQL for Charges Extraction => Local/ Stage Env

    SELECT * FROM src_initial_kigo_reservation_report;
    

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
