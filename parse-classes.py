from modules import classifications as cl
import time

start_time = time.time()

df_valid_decks, df_valid_event_types = cl.parse_class_sheet()
cl.class_insert(df_valid_decks, df_valid_event_types)

print(time.time() - start_time)