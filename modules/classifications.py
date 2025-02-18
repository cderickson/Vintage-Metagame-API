import pandas as pd
import psycopg2
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()
credentials = [os.getenv("DB_HOST"), os.getenv("DB_PORT"), os.getenv("DB_USER"), os.getenv("DB_PASSWORD"), os.getenv("DB_NAME")]
gsheets = [os.getenv("VINTAGE_SHEET_CURR"), os.getenv("VINTAGE_SHEET_ARCHIVE"), os.getenv("VINTAGE_GID_MATCHES"), os.getenv("VINTAGE_GID_DECK")]

# MATCH_ID      = 11000000000
# EVENT_ID      = 12000000000
# DECK_ID       = 13000000000
# EVENT_TYPE_ID = 14000000000
# LOAD_RPT_ID   = 15000000000
# EV_REJ_ID     = 16000000000
# MATCH_REJ_ID  = 17000000000

def read_credentials():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    with open("credentials.txt", "r") as file:
        return [line.strip() for line in file]
    
def parse_class_sheet():
    deck_url = f'https://docs.google.com/spreadsheets/d/{gsheets[0]}/export?format=csv&gid={gsheets[3]}'
    df = pd.read_csv(deck_url)

    # Create dataframe with valid Deck Names.
    df_decks = df[['Archetype','Subarchetype']].sort_values(['Archetype','Subarchetype']).reset_index(drop=True)
    df_decks.columns = ['ARCHETYPE','SUBARCHETYPE']
    df_decks['ARCHETYPE'] = df_decks['ARCHETYPE'].str.strip().str.upper()
    df_decks['SUBARCHETYPE'] = df_decks['SUBARCHETYPE'].str.strip().str.upper()

    # Adding rows for NA and NO SHOW.
    df_decks = pd.concat([df_decks,pd.DataFrame({'ARCHETYPE':['NA','NA','NA'],'SUBARCHETYPE':['NA','NO SHOW','INVALID_NAME']})],ignore_index=True)

    # Create dataframe with valid Event Types.
    df_events = df[['Event Types']]
    df_events.columns = ['EVENT_TYPE']
    df_events = df_events.dropna(subset=['EVENT_TYPE'])
    df_events['EVENT_TYPE'] = df_events['EVENT_TYPE'].str.strip().str.upper()

    # Adding row for NA.
    df_events = pd.concat([df_events,pd.DataFrame({'EVENT_TYPE':['INVALID_TYPE']})],ignore_index=True)

    # Add Format column to Decks table.
    df_decks['FORMAT'] = 'VINTAGE'
    df_decks = df_decks[['FORMAT','ARCHETYPE','SUBARCHETYPE']]

    # Add Format column to Events table.
    df_events['FORMAT'] = 'VINTAGE'
    df_events = df_events[['FORMAT','EVENT_TYPE']]
    
    return (df_decks,df_events)

def class_insert(df_valid_decks=None, df_valid_event_types=None):
    valid_decks_query = """
        INSERT INTO "VALID_DECKS" ("FORMAT", "ARCHETYPE", "SUBARCHETYPE", "PROC_DT")
        VALUES (%s, %s, %s, %s)
        ON CONFLICT ("FORMAT", "ARCHETYPE", "SUBARCHETYPE")
        DO NOTHING
    """
    valid_event_types_query = """
        INSERT INTO "VALID_EVENT_TYPES" ("FORMAT", "EVENT_TYPE", "PROC_DT")
        VALUES (%s, %s, %s)
        ON CONFLICT ("FORMAT", "EVENT_TYPE") 
        DO NOTHING
    """
    proc_dt = datetime.now()
    try:
        # credentials = read_credentials()
        conn = psycopg2.connect(
            host=credentials[0],
            port=credentials[1],
            user=credentials[2],
            password=credentials[3],
            database=credentials[4],
            sslmode='require'
        )
        cursor = conn.cursor()

        # Insert valid_decks
        if df_valid_decks is not None:
            values_list = [(row['FORMAT'], row['ARCHETYPE'], row['SUBARCHETYPE'], proc_dt) for index, row in df_valid_decks.iterrows()]
            for values in values_list:
                try:
                    print(values)
                    cursor.execute(valid_decks_query, values)
                except Exception as e:
                    print(f"Error inserting row into VALID_DECKS: {values} | Error: {e}")
                    continue  # Skip the row and continue with the next one
            conn.commit()

        # Insert valid_event_types
        if df_valid_event_types is not None:
            values_list = [(row['FORMAT'], row['EVENT_TYPE'], proc_dt) for index, row in df_valid_event_types.iterrows()]
            for values in values_list:
                try:
                    print(values)
                    cursor.execute(valid_event_types_query, values)
                except Exception as e:
                    print(f"Error inserting row into VALID_EVENT_TYPES: {values} | Error: {e}")
                    continue
            conn.commit()

    except Exception as e:
        print(f"Error occurred while loading data: {e}")
        conn.rollback()

    finally:
        if conn:
            cursor.close()
            conn.close()