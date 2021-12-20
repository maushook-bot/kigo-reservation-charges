##################################################################################
# connections.py : Extracts a domain's staging and production DB parameters ######
##################################################################################

# Importing the Modules:-
from sqlalchemy import create_engine
from decouple import config
import pandas as pd


class CreateConnection:
    def __init__(self):
        print("\n@@@ Inside Connections Class @@@")

    def get_engine_connection(self, local_DB, domain):

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
