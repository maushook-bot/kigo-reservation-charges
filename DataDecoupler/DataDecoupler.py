"""
@@@ Read/Write Data from/to Gsheet API Class @@@
@@@ KIGO Reservation Charges Importer API @@@
@@@ version: v2.0 @@@
@@@ DataDecoupler.py @@@
"""

# Import the Libraries:-
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from df2gspread import df2gspread as d2g
from df2gspread import gspread2df as g2d


class CoupleDecouple:

    def __init__(self):
        print("\n@@ Inside Data-Decoupler API Class @@")

    def url_trimmer(self, gsheet_url):
        print("$$ Trimming GSHEET URL --> GSHEET-ID $$")

        start_pos = 39
        end_pos = gsheet_url.find('/', start_pos)
        gsheet_id = gsheet_url[start_pos: end_pos]
        print("** GSHSEET_ID: ", gsheet_id)
        return gsheet_id

    @staticmethod
    def data_processor(gsheet_id, stage, local):
        # Service and creds Initialization:-
        SCOPES = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
                  "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
        CREDS = ServiceAccountCredentials.from_json_keyfile_name("DataDecoupler/creds.json", SCOPES)

        # Authorize the Service Creds and Get the Spreadsheet Metadata :-
        client = gspread.authorize(CREDS)
        GSHEET_META_ID = gsheet_id

        # Intialize the data dict:-
        data_dict = {
                      'reservation_id': pd.Series([], dtype='int')
                     }
        df = pd.DataFrame(data_dict)
        df.to_sql(f"src_kigo_import_data", con=local, index=False, if_exists="replace")
        df.to_sql(f"src_kigo_import_data", con=stage, index=False, if_exists="replace")

        return GSHEET_META_ID, CREDS, data_dict

    def readgsheet(self, local, stage, sheet_name, gsheet_id):

        print("$$$ Reading Contents from Gsheet $$$")

        GSHEET_META_ID, CREDS, data_dict = self.data_processor(gsheet_id, stage, local)

        try:
            df = g2d.download(GSHEET_META_ID, wks_name=sheet_name,
                              credentials=CREDS, row_names=True, col_names=True)
        except ValueError:
            df = pd.DataFrame(data_dict)
            print("VALUE ERROR: No Sheet Available: Try Again!")
        except RuntimeError:
            df = pd.DataFrame(data_dict)
            print("RUNTIME ERROR: <NON EXISTENT TABLE> Try Again")

        df.to_sql(f"src_kigo_import_data", con=local, index=False, if_exists="append")
        df.to_sql(f"src_kigo_import_data", con=stage, index=False, if_exists="append")

        print("Data Loaded: Local/Stage")

    def write2gsheet(self, local, stage, gsheet_id, sheet_name):

        print("$$$ Writing Contents to Gsheet $$$")

        GSHEET_META_ID, CREDS, data_dict = self.data_processor(gsheet_id, stage, local)

        sql = f"""
                SELECT * FROM tia_kigo_reservation_charges_import_processing;
        """

        df_out = pd.read_sql(sql, con=local)

        try:
            d2g.upload(df_out, GSHEET_META_ID, wks_name=f"Output | {sheet_name}",
                       credentials=CREDS, row_names=True)
        except ValueError:
            print("No Matched Data in table: Skip")
