##################################################################################################################
# @@@ Kigo Reservation Charges Importer API @@@
# ~~~ [] End-Point: https://connect.bookt.com/
# @@@ version: v2.0-beta-dev @@@
# @@@ Comments: Multiprocessing feature Inclusive and Tested @@@
# @@@ main.py @@@
#################################################################################################################

# Importing the Libraries:-
from ReservationCharges.ReservationCharges import ReservationCharges
from DataDecoupler.DataDecoupler import CoupleDecouple
from configobj import ConfigObj

# Config Object
conf = ConfigObj('config.ini')

# configuration-Parameters:-
domain = conf['domain']
local_DB = conf['local_db_name']
environment = conf['environment']
sheet_name = conf['sheet_name']
gsheet_url = conf['gsheet_url']

if __name__ == '__main__':

    # Instantiate ownerPayments Class:-
    op = ReservationCharges()

    # Get API Environment Parameters:-
    local, stage = op.get_engine_connection(local_DB, domain)
    api_key, env = op.get_environment()

    # Instantiate Data-Decoupler Class:-
    cd = CoupleDecouple()
    gsheet_id = cd.url_trimmer(gsheet_url)

    # Main Function Calls:-
    op.destination_table_init(local, stage)
    df_src = op.prepare_source_data(local, stage)
    chunks, blocks = op.processor_configurator(len(df_src))
    print("Chunk-Size:", chunks, "Processes:", blocks, "Processed-data-count:", len(df_src))
    op.start_multiprocessor(chunks, blocks, df_src, api_key, env, local_DB)

    # Write Contents to gsheet:-
    #cd.write2gsheet(local, stage, gsheet_id, sheet_name)

