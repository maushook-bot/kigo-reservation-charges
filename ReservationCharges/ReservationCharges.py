##########################################################################################
"""
@@@ KIGO RESERVATION CHARGES IMPORTER API @@@
@@@ version: v2.0 @@@
@@@ ReservationCharges.py @@@
"""
##########################################################################################

# Importing the Modules:-
import pandas as pd
import requests as r
import multiprocessing
import time
import warnings
from sqlalchemy import create_engine
from decouple import config

warnings.filterwarnings("ignore")


class ReservationCharges:
    def __init__(self):
        print("\n@@@ Inside Kigo Reservation Charges Importer class @@@")

    @staticmethod
    def get_engine_connection(local_DB, domain):

        # Decode the Environment variables: DB Connection Strings:-
        ### Staging ###
        MYSQL_USER_TIA = config('MYSQL_USER_TIA')
        PASSWORD_STAGE = config('MYSQL_PASSWORD_TIA')
        HOST_STAGE = config('HOST_TIA')

        ### Sandbox/ Stage3 ###
        MYSQL_USER_TRACK_STAGE = config('MYSQL_USER_TRACK_STAGE')
        PASSWORD_SANDBOX = config('MYSQL_PASSWORD_TRACK_STAGE')
        HOST_SANDBOX = config('HOST_TRACK_STAGE')

        ### Prod ###
        MYSQL_USER_TRACK_PROD = config('MYSQL_USER_TRACK_PROD')
        PASSWORD_PROD = config('MYSQL_PASSWORD_TRACK_PROD')
        HOST_PROD = config('HOST_TRACK_PROD')

        ### LOCAL ###
        LOCAL_USER = config('LOCAL_USER')
        LOCAL_PASSWORD = config('LOCAL_PASSWORD')
        LOCAL_HOST = config('LOCAL_HOST')

        # DB Engine Parameters:-
        # Get DB Params Initialized
        app_core = create_engine(f"mysql+pymysql://{MYSQL_USER_TIA}:{PASSWORD_STAGE}@{HOST_STAGE}/app_core")
        uuid = pd.read_sql(f"SELECT c.tia_uuid FROM customer c WHERE c.domain = '{domain}';", con=app_core)
        prod_db = pd.read_sql(f"SELECT c.db_name_prod FROM customer c WHERE c.domain = '{domain}';", con=app_core)
        print("App Core DB: Connection Established!")
        uuid = uuid['tia_uuid'][0]
        prod_db = prod_db['db_name_prod'][0]
        print('UUID:', uuid)
        print('Track DB:', prod_db)

        # Initialize the Engine params:-
        local = create_engine(f"mysql+pymysql://{LOCAL_USER}:{LOCAL_PASSWORD}@{LOCAL_HOST}/{local_DB}")
        stage = create_engine(f"mysql+pymysql://{MYSQL_USER_TIA}:{PASSWORD_STAGE}@{HOST_STAGE}/{uuid}")
        sandbox = create_engine(f"mysql+pymysql://{MYSQL_USER_TRACK_STAGE}:{PASSWORD_SANDBOX}@{HOST_SANDBOX}/")
        prod = create_engine(f"mysql+pymysql://{MYSQL_USER_TRACK_PROD}:{PASSWORD_PROD}@{HOST_PROD}/{prod_db}")

        return local, stage

    @staticmethod
    def get_environment():

        # Set Environment Params:-
        print('Inside Kigo Environment')
        env = "connect.bookt.com/ws/default.aspx"
        api_key = config('API_SECRET_KEY')

        return api_key, env

    @staticmethod
    def destination_table_init(local, stage):

        # Intialize the Output Processing dict:-
        processing_dict = {
            'reservation_id': pd.Series([], dtype='int'),
            'ID': pd.Series([], dtype='int'),
            'Type': pd.Series([], dtype='str'),
            'Status': pd.Series([], dtype='str'),
            'Quantity': pd.Series([], dtype='int'),
            'Amount': pd.Series([], dtype='float'),
            'SubTotal': pd.Series([], dtype='float'),
            'Notes': pd.Series([], dtype='str'),
            'FriendlyType': pd.Series([], dtype='str'),
            'ShowOnStatement': pd.Series([], dtype='str'),
            'api_response': pd.Series([], dtype='int'),
            'remarks': pd.Series([], dtype='str'),
            'api_links': pd.Series([], dtype='str')
        }

        df_out = pd.DataFrame(processing_dict)
        df_out.to_sql(f"tia_kigo_reservation_charges_import_processing", con=local, index=False, if_exists="replace")
        df_out.to_sql(f"tia_kigo_reservation_charges_import_processing", con=stage, index=False, if_exists="replace")

    @staticmethod
    def processor_configurator(source_count):
        global chunks, blocks
        if source_count >= 20000:
            blocks = 20
            chunks = int(source_count / blocks) + int(source_count % blocks)
        elif 500 <= source_count < 20000:
            blocks = 20
            chunks = int(source_count / blocks) + int(source_count % blocks)
        elif 20 <= source_count < 500:
            blocks = 10
            chunks = int(source_count / blocks) + int(source_count % blocks)
        elif 0 <= source_count < 20:
            blocks = 1
            chunks = int(source_count / blocks) + int(source_count % blocks)
        return chunks, blocks

    @staticmethod
    def chunker(df_processed, chunk_size, blocks):
        j = 1
        splits = []
        for j in range(blocks + 1):
            splits.append(df_processed[chunk_size * (j - 1):chunk_size * j])
        return splits

    @staticmethod
    def prepare_source_data(local, stage):

        # Prepare Source data for Master Folios Importer API
        print(f'$ Preparing Source data for Kigo Reservation Charges Importer API')
        sql = '''
                SELECT * FROM src_initial_kigo_reservation_report;
        '''

        df_src = pd.read_sql(sql, con=local)
        df_src.to_sql('src_kigo_import_data', con=local, index=False, if_exists='replace')
        # df_src.to_sql('src_kigo_import_data', con=stage, index=False, if_exists='replace')
        print(f'$ Established Connection with Source DB')
        return df_src

    @staticmethod
    def source_count(local, stage):
        sql = "SELECT COUNT(1) FROM src_kigo_import_data;"
        count = pd.read_sql(sql, con=local)
        print('$ Source Records Count: ', count['count(1)'][0])
        return count['count(1)'][0]

    def start_multiprocessor(self, chunks, blocks, df_src, api_key, env, local_DB):

        # Creating Multi-processes for the splits and multiple rest api calls-
        print('Multi-Processor Start')
        processes = []

        j = 1
        for j in range(blocks + 1):
            s = multiprocessing.Process(target=self.reservation_charges_multiprocessor, args=(j, chunks, blocks, df_src,
                                                                                              api_key, env,
                                                                                              local_DB))
            print(f'Process-Initialized: {j}')
            processes.append(s)

        for process in processes:
            process.start()
            time.sleep(2)

        for process in processes:
            process.join()

    def reservation_charges_multiprocessor(self, j, chunks, blocks, df_src, api_key, env, local_DB):

        # Track Worker:-
        print(f"~~~ Worker Tracker: {j} ~~~")

        # Create Chunks of df:-
        splits = self.chunker(df_src, chunks, blocks)
        df_split = splits[j]

        # Call Chunks of Master Function:-
        self.get_reservation_charges(df_split, env, api_key, local_DB)

    def get_reservation_charges(self, df, env, api_key, local_DB):

        local = create_engine(f"mysql+pymysql://root:admin@127.0.0.1:3306/{local_DB}")

        for index, row in df.iterrows():
            # start of master loop:-
            print(f"~ Inside Master loop: {index}")

            # headers:-
            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json"
            }

            # Endpoint-router:-
            print("$$ Master KIGO Reservation Charges Processing $$")
            self.reservation_charges_processing(df, local, env, api_key, index, headers)

            # End of master Loop

        print("\n@@ KIGO Reservation Charges Extraction Complete!")

    @staticmethod
    def reservation_charges_processing(df, local, env, api_key, index, headers):

        print("** GET: Kigo Reservation Charges")

        # 1. url body:-
        url = f"https://{env}?method=get" \
              f"&loadconfig=1" \
              f"&includeSDRelatedToObjs=1" \
              f"&nocache=1" \
              f"&transactions=1" \
              f"&apiKey={api_key}" \
              f"&entity=booking" \
              f"&ids={df['reservation_id'][index]}"

        # 2. response body:-
        response = r.get(url, headers=headers)
        response_data = response.json()
        print("**", response.status_code, '|', response_data)

        # 3. Get the Length of Details Object:-
        print('** Extracting Details Object **')
        try:
            details = response_data['result'][0]['Statement']['Details']
            print('**', len(details), '=>', details)
        except Exception as e:
            details = None

        # 4. Looping through Details Dict:-

        if details is not None:
            ID_list = [int(detail['ID']) for count, detail in enumerate(details)]
            type_list = [str(detail['Type']) for count, detail in enumerate(details)]
            status_list = [str(detail['Status']) for count, detail in enumerate(details)]
            qty_list = [int(detail['Quantity']) for count, detail in enumerate(details)]
            amount_list = [float(detail['Amount']) for count, detail in enumerate(details)]
            sub_total_list = [float(detail['SubTotal']) for count, detail in enumerate(details)]
            notes_list = [str(detail['Notes']) for count, detail in enumerate(details)]
            ShowOnStatement_list = [str(detail['ShowOnStatement']) for count, detail in enumerate(details)]
            friendly_type_list = [str(detail['FriendlyType']) for count, detail in enumerate(details)]
            reservation_id_list = [int(df['reservation_id'][index]) for count, detail in enumerate(details)]

            if response.status_code == 200:
                api_response_list = [int(1) for count, detail in enumerate(details)]
                api_links_list = [str(f'https://{env}/transactions') for count, detail in enumerate(details)]
                remarks_list = [str('Transaction Found') for count, detail in enumerate(details)]
            else:
                api_response_list = [int(-1) for count, detail in enumerate(details)]
                api_links_list = [str(f'https://{env}/transactions') for count, detail in enumerate(details)]
                remarks_list = [str('Data Extract Failed') for count, detail in enumerate(details)]

            # 5. Prepare Processing Dict with Data:-
            processing_dict = {
                'reservation_id': reservation_id_list,
                'ID': ID_list,
                'Type': type_list,
                'Status': status_list,
                'Quantity': qty_list,
                'Amount': amount_list,
                'SubTotal': sub_total_list,
                'Notes': notes_list,
                'FriendlyType': friendly_type_list,
                'ShowOnStatement': ShowOnStatement_list,
                'api_response': api_response_list,
                'remarks': remarks_list,
                'api_links': api_links_list
            }

        else:
            # 5. Prepare Processing Dict with Data => NULL:-
            processing_dict = {
                'reservation_id': [df['reservation_id'][index]],
                'ID': [0],
                'Type': [""],
                'Status': [""],
                'Quantity': [0],
                'Amount': [0.0],
                'SubTotal': [0.0],
                'Notes': [""],
                'FriendlyType': [""],
                'ShowOnStatement': [""],
                'api_response': [-1],
                'remarks': ["Data Extract Failed"],
                'api_links': [f'https://{env}/transactions']
            }

        print('**', processing_dict)

        print("@@ Loading Output Processing table => Local/Stage @@\n")
        df_out = pd.DataFrame(processing_dict)
        df_out.to_sql(f"tia_kigo_reservation_charges_import_processing", con=local, index=False, if_exists="append")

        return processing_dict

    @staticmethod
    def reset_list_data():

        processing_dict = {
            'reservation_id': pd.Series([], dtype='int'),
            'ID': pd.Series([], dtype='int'),
            'Type': pd.Series([], dtype='str'),
            'Status': pd.Series([], dtype='str'),
            'Quantity': pd.Series([], dtype='int'),
            'Amount': pd.Series([], dtype='float'),
            'SubTotal': pd.Series([], dtype='float'),
            'Notes': pd.Series([], dtype='str'),
            'FriendlyType': pd.Series([], dtype='str'),
            'ShowOnStatement': pd.Series([], dtype='str'),
            'api_response': pd.Series([], dtype='int'),
            'remarks': pd.Series([], dtype='str'),
            'api_links': pd.Series([], dtype='str')
        }

        reservation_id_list = []
        ID_list = []
        type_list = []
        status_list = []
        qty_list = []
        amount_list = []
        sub_total_list = []
        notes_list = []
        friendly_type_list = []
        ShowOnStatement = []
        api_response_list = []
        api_links_list = []
        remarks_list = []

        return processing_dict, reservation_id_list, ID_list, type_list, status_list, qty_list, amount_list, \
               sub_total_list, notes_list, friendly_type_list, ShowOnStatement, api_response_list, api_links_list, \
               remarks_list
