import json
from pprint import pprint
import pandas as pd

from google.cloud import bigquery

schema = [
    bigquery.SchemaField("id", "STRING"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("scheduled", "TIMESTAMP"),  # Assuming this is a datetime string
    bigquery.SchemaField("entry_mode", "STRING"),
    bigquery.SchemaField("sr_id", "STRING"),
    bigquery.SchemaField("game_type", "STRING"),
    bigquery.SchemaField("conference_game", "BOOLEAN"),
    bigquery.SchemaField("week_id", "STRING"),
    bigquery.SchemaField("week_no", "INTEGER"),
    bigquery.SchemaField("venue_id", "STRING"),
    bigquery.SchemaField("venue_name", "STRING"),
    bigquery.SchemaField("venue_city", "STRING"),
    bigquery.SchemaField("venue_state", "STRING"),
    bigquery.SchemaField("venue_country", "STRING"),
    bigquery.SchemaField("venue_zip", "STRING"),
    bigquery.SchemaField("venue_address", "STRING"),
    bigquery.SchemaField("venue_capacity", "INTEGER"),
    bigquery.SchemaField("venue_surface", "STRING"),
    bigquery.SchemaField("venue_roof_type", "STRING"),
    bigquery.SchemaField("venue_sr_id", "STRING"),
    bigquery.SchemaField("venue_location_lat", "FLOAT"),  # Converting to FLOAT
    bigquery.SchemaField("venue_location_lng", "FLOAT"),  # Converting to FLOAT
    bigquery.SchemaField("home_id", "STRING"),
    bigquery.SchemaField("home_name", "STRING"),
    bigquery.SchemaField("home_alias", "STRING"),
    bigquery.SchemaField("home_game_number", "INTEGER"),
    bigquery.SchemaField("home_sr_id", "STRING"),
    bigquery.SchemaField("away_id", "STRING"),
    bigquery.SchemaField("away_name", "STRING"),
    bigquery.SchemaField("away_alias", "STRING"),
    bigquery.SchemaField("away_game_number", "INTEGER"),
    bigquery.SchemaField("away_sr_id", "STRING"),
    bigquery.SchemaField("broadcast_network", "STRING"),
    bigquery.SchemaField("title", "STRING"),
    bigquery.SchemaField("broadcast_internet", "STRING"),
    bigquery.SchemaField("broadcast_satellite", "STRING"),
]

file_path = 'nfl_schedule_2024.json'

with open(file_path, 'r') as sched:
    schedule = json.loads(sched.read())

weeks = schedule['weeks']
list_of_games = []
for week in weeks:
    games = week['games']
    for game in games:
        game['week_id'] = week['id']
        game['week_no'] = week['sequence']
        list_of_games.append(game)

df = pd.json_normalize(list_of_games)
df.columns = df.columns.str.replace('.', '_')
df['scheduled'] = pd.to_datetime(df['scheduled'])
df['venue_location_lat'] = pd.to_numeric(df['venue_location_lat'])
df['venue_location_lng'] = pd.to_numeric(df['venue_location_lng'])
print(df.columns)
client = bigquery.Client(project="nfl-predictor-428314")

job_config = bigquery.LoadJobConfig(
    schema=schema,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
)

table_id = "nfl-predictor-428314.season_schedules.2024"

job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
job.result()  # Wait for the job to complete

print(f"Loaded {job.output_rows} rows into {table_id}.")
