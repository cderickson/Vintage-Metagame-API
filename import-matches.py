from modules import match_import as mi
import warnings
from datetime import datetime, timedelta
import time
import sys
import ast
warnings.filterwarnings('ignore', category=UserWarning, message="pandas only supports SQLAlchemy connectable")

start_time = time.time()

sheet_curr = '1wxR3iYna86qrdViwHjUPzHuw6bCNeMLb72M25hpUHYk'
sheet_archive = '1PxNYGMXaVrRqI0uyMQF46K7nDEG16WnDoKrFyI_qrvE'
gid_matches = '2141931777'
gid_deck = '590005429'

if len(sys.argv) > 1:
    tuple_str = sys.argv[1]
    date_tuple = ast.literal_eval(tuple_str)

    month = date_tuple[0]
    day = date_tuple[1]
    year = date_tuple[2]
else:
    month = 9
    day = 1
    year = 2024

# Day 0 is 8-25-2024.
# start_date = datetime.today().date() - timedelta(days=14)
start_date = datetime(year, month, day).date()
end_date = start_date + timedelta(days=7)

df_matches, df_events, load_rep_list, event_skipped_rej = mi.parse_matchup_sheet(sheet_curr, gid_matches, start_date=start_date, end_date=end_date)
# mi.test(df_matches, df_events)
load_rep_ins, event_rej, match_rej = mi.match_insert(df_matches, df_events, start_date=start_date, end_date=end_date)

load_report = [start_date,end_date - timedelta(days=1)] + load_rep_list + load_rep_ins
mi.insert_load_stats(load_report, event_skipped_rej + event_rej, match_rej)

print(time.time() - start_time)