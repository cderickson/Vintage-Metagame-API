import pandas as pd
import psycopg2

sheet_curr = '1wxR3iYna86qrdViwHjUPzHuw6bCNeMLb72M25hpUHYk'
sheet_archive = '1PxNYGMXaVrRqI0uyMQF46K7nDEG16WnDoKrFyI_qrvE'
gid_matches = '2141931777'
gid_deck = '590005429'

with open("credentials.txt", "r") as file:
    credentials = [line.strip() for line in file]

def conn(query):
    try:
        conn = psycopg2.connect(
            host=credentials[0],
            port=credentials[1],
            user=credentials[2],
            password=credentials[3],
            database=credentials[4]
        )
        cursor = conn.cursor()

        cursor.execute(query)

        conn.commit()
    except psycopg2.Error as e:
        print('Error:', e)
    finally:
        if conn:
            cursor.close()
            conn.close()
def parse_class_sheet(sheet,gid):
    deck_url = f'https://docs.google.com/spreadsheets/d/{sheet}/export?format=csv&gid={gid}'
    df = pd.read_csv(deck_url)

    # Create dataframe with valid Deck Names.
    df_decks = df[['Archetype','Subarchetype']].sort_values(['Archetype','Subarchetype']).reset_index(drop=True)
    df_decks.columns = ['ARCHETYPE','SUBARCHETYPE']
    df_decks['ARCHETYPE'] = df_decks['ARCHETYPE'].str.strip().str.upper()
    df_decks['SUBARCHETYPE'] = df_decks['SUBARCHETYPE'].str.strip().str.upper()

    # Adding Deck IDs.
    count = 1001
    df_decks['DECK_ID'] = 0
    for index, row in df_decks.iterrows():
        df_decks.at[index,'DECK_ID'] = count
        count += 1

    df_decks = pd.concat([df_decks,pd.DataFrame({'ARCHETYPE':['NA','NA'],'SUBARCHETYPE':['NA','NO SHOW'],'DECK_ID':[1998,1999]})],ignore_index=True)

    # Create dataframe with valid Event Types.
    df_events = df[['Event Types']]
    df_events.columns = ['EVENT_TYPE']
    df_events = df_events.dropna(subset=['EVENT_TYPE'])
    df_events['EVENT_TYPE'] = df_events['EVENT_TYPE'].str.strip().str.upper()

    # Adding Event Type IDs.
    count = 101
    df_events['EVENT_TYPE_ID'] = 0
    for index, row in df_events.iterrows():
        df_events.at[index,'EVENT_TYPE_ID'] = count
        count += 1

    # Add Format column.
    df_decks['FORMAT'] = 'VINTAGE'
    df_decks = df_decks[['FORMAT','ARCHETYPE','SUBARCHETYPE','DECK_ID']]

    df_events['FORMAT'] = 'VINTAGE'
    df_events = df_events[['FORMAT','EVENT_TYPE','EVENT_TYPE_ID']]
    
    return (df_decks,df_events)
def parse_matchup_sheet(sheet,gid):
    sheet_url = f'https://docs.google.com/spreadsheets/d/{sheet}/export?format=csv&gid={gid}'
    df = pd.read_csv(sheet_url)

    # Rename columns.
    df.columns = ['P1','P2','P1_WINS','P2_WINS','WINNER1','WINNER2','P1_ARCH','P2_ARCH','P1_SUBARCH','P2_SUBARCH','P1_NOTE','P2_NOTE','EVENT_DATE','EVENT_TYPE']

    # Replace null values with 'NA' string.
    df.fillna({'P1_ARCH':'NA','P2_ARCH':'NA','P1_SUBARCH':'NA','P2_SUBARCH':'NA'},inplace=True)

    # Strip whitespace from player/deck names.
    df.P1 = df.P1.str.strip()
    df.P2 = df.P2.str.strip()
    df.P1_ARCH = df.P1_ARCH.str.strip().str.upper()
    df.P2_ARCH = df.P2_ARCH.str.strip().str.upper()
    df.P1_SUBARCH = df.P1_SUBARCH.str.strip().str.upper()
    df.P2_SUBARCH = df.P2_SUBARCH.str.strip().str.upper()
    df.P1_NOTE = df.P1_NOTE.str.strip().str.upper()
    df.P2_NOTE = df.P2_NOTE.str.strip().str.upper()

    # Format EVENT_TYPE values.
    df['EVENT_TYPE'] = df['EVENT_TYPE'].str.upper().str.strip()

    # Format No Show deck name values.
    for index, row in df.iterrows():
        if row['P1_SUBARCH'].strip().upper() == 'NO SHOW':
            df.at[index,'P1_SUBARCH'] = 'NO SHOW'
        if row['P2_SUBARCH'].strip().upper() == 'NO SHOW':
            df.at[index,'P2_SUBARCH'] = 'NO SHOW'

    # Format date column.
    df.EVENT_DATE = pd.to_datetime(df.EVENT_DATE,yearfirst=False,format='mixed')

    # Adding Event_IDs.
    count = 1000001
    df['EVENT_ID'] = 0
    for index, row in reversed(list(df.iterrows())):
        df.at[index,'EVENT_ID'] = count
        if pd.notna(row['EVENT_TYPE']):
            count += 1

    # Handle empty EVENT_TYPE/EVENT_DATE values by forward-filling.
    df['EVENT_TYPE'] = df['EVENT_TYPE'].ffill()
    df['EVENT_DATE'] = df['EVENT_DATE'].ffill()

    # Ignore events with incomplete data.
    events_to_ignore = [1000007]
    df = df[~df.EVENT_ID.isin(events_to_ignore)]

    # Replace Winner1/2 columns with single Match_Winner column.
    df['MATCH_WINNER'] = df.apply(lambda row: 'P1' if ((row['WINNER1'] == 1) & (row['WINNER2'] == 0)) else ('P2' if ((row['WINNER1'] == 0) & (row['WINNER2'] == 1)) else 'NA'), axis=1)
    df.drop(columns=['WINNER1','WINNER2'],inplace=True)

    # EVENT_ID 1000067 should be OTHER
    df.loc[df['EVENT_ID'] == 1000067,'EVENT_TYPE'] = 'OTHER'

    # Convert P1/P2_WINS from float to int.
    df['P1_WINS'] = df['P1_WINS'].astype(int)
    df['P2_WINS'] = df['P2_WINS'].astype(int)

    # Abstract out Event info into its own table.
    df_events = df.groupby(['EVENT_ID','EVENT_DATE']).agg({'EVENT_TYPE':'last'}).reset_index()

    return df, df_events
def merge_matches_codes(df_matches,df_valid_decks):
    df = pd.merge(left=df_matches,right=df_valid_decks,left_on=['P1_ARCH','P1_SUBARCH'],right_on=['ARCHETYPE','SUBARCHETYPE'],how='left')
    df.rename(columns={'DECK_ID':'P1_DECK_ID'}, inplace=True)
    df = pd.merge(left=df,right=df_valid_decks,left_on=['P2_ARCH','P2_SUBARCH'],right_on=['ARCHETYPE','SUBARCHETYPE'],how='left')
    df.rename(columns={'DECK_ID':'P2_DECK_ID'}, inplace=True)
    return df[['P1','P2','P1_WINS','P2_WINS','MATCH_WINNER','P1_DECK_ID','P2_DECK_ID','P1_NOTE','P2_NOTE','EVENT_ID']]
def merge_events_codes(df_events,df_valid_events):
    df = pd.merge(left=df_events,right=df_valid_events,left_on=['EVENT_TYPE'],right_on=['EVENT_TYPE'],how='left')
    return df[['EVENT_ID','EVENT_DATE','EVENT_TYPE_ID']]

def insert(df_valid_decks,df_valid_event_types,df_matches,df_events):
    valid_decks_query = """
        INSERT INTO VALID_DECKS (FORMAT, ARCHETYPE, SUBARCHETYPE, DECK_ID)
        VALUES (%s, %s, %s, %s)
    """
    valid_event_types_query = """
        INSERT INTO VALID_EVENT_TYPES (FORMAT, EVENT_TYPE, EVENT_TYPE_ID)
        VALUES (%s, %s, %s)
    """
    events_query = """
        INSERT INTO EVENTS (EVENT_ID, EVENT_DATE, EVENT_TYPE_ID)
        VALUES (%s, %s, %s)
    """
    matches_query = """
        INSERT INTO MATCHES (P1, P2, P1_WINS, P2_WINS, MATCH_WINNER, P1_DECK_ID, P2_DECK_ID, P1_NOTE, P2_NOTE, EVENT_ID)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    try:
        for index, row in df_valid_decks.iterrows():
            values = (row['FORMAT'], row['ARCHETYPE'], row['SUBARCHETYPE'], row['DECK_ID'])
            cur.execute(insert_query, values)
        conn.commit()

    except Exception as e:
        print(f"Error occurred while loading data: {e}")
        conn.rollback()

    finally:
        if conn:
            cur.close()
            conn.close()

df_valid_decks, df_valid_event_types = parse_class_sheet(sheet_curr,gid_deck)
df_matches, df_events = parse_matchup_sheet(sheet_curr,gid_matches)
df_matches = merge_matches_codes(df_matches,df_valid_decks)
df_events = merge_events_codes(df_events,df_valid_event_types)

print(df_matches.head())
print(df_events.head())