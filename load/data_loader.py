import io
import fastf1
import boto3
import pandas as pd
import datetime
from boto3.dynamodb.conditions import Attr
from fastf1.core import DataNotLoadedError
import os

dynamodb_client = boto3.client('dynamodb')
dynamodb_resource = boto3.resource('dynamodb')
events_table = 'F1EventsSchedule'
lambda_client = boto3.client('lambda')
cloudwatch_events = boto3.client('events')
bucket_name = 'race-predictor-pro'
prefix = 'f1_data'

# Define cache directory in Lambda's /tmp
cache_dir = '/tmp/.fastf1'

# Ensure the cache directory exists before doing anything with FastF1
if not os.path.exists(cache_dir):
    os.makedirs(cache_dir, mode=0o700)  # Ensure correct permissions

# Enable cache after ensuring the directory exists
fastf1.Cache.enable_cache(cache_dir)


def data_ingestion_lambda_handler(event, context):
    try:
        data_ingestion = DataIngestion(bucket_name, prefix)
        eventName = data_ingestion.fetch_and_load_latest_race()
        data_ingestion.mark_latest_events_as_processed(eventName)
        lambda_client.invoke(
            FunctionName='F1RaceSchedulerLambda',
            InvocationType='Event'
        )
        return {
            'statusCode': 200,
            'body': 'latest race date uploaded successfully'
        }

    except Exception as e:
        print(f'Error processing data: {e}')
        return {
            'statusCode': 500,
            'body': f'Error processing data: {e}'
        }


class DataIngestion:

    def __init__(self, bucket, prefix):
        self.s3_client = boto3.client('s3')
        self.bucket = bucket
        self.prefix = prefix

    def fetch_and_upload_race(self, year, race_name, session_types):
        for session_type in session_types:
            try:
                f1_session = fastf1.get_session(year, race_name, session_type)
                f1_session.load()

                # Define S3 paths
                base_path = f"{self.prefix}/{year}/{race_name.replace(' ', '-').lower()}"

                if session_type == 'R':
                    drivers = f1_session.drivers
                    dummy_df = []

                    for driver in drivers:
                        df = pd.DataFrame(f1_session.get_driver(driver)).T
                        dummy_df.append(df)

                    driver_df = pd.concat(dummy_df, ignore_index=True)
                    drivers_info_s3_path = f"{base_path}/{session_type.lower()}_drivers_info.parquet"
                    self.upload_parquet_to_s3(driver_df, drivers_info_s3_path)

                try:

                    try:
                        laps_df = f1_session.laps
                        lap_s3_path = f"{base_path}/{session_type.lower()}_laps.parquet"
                        self.upload_parquet_to_s3(laps_df, lap_s3_path)
                    except DataNotLoadedError as e:
                        print(f"Laps data not loaded: {e}")
                        laps_df = None

                    try:
                        weather_df = f1_session.weather_data
                        weather_s3_path = f"{base_path}/{session_type.lower()}_weather.parquet"
                        self.upload_parquet_to_s3(weather_df, weather_s3_path)
                    except DataNotLoadedError as e:
                        print(f"Weather data not loaded: {e}")
                        weather_df = None

                except Exception as e:
                    print(f"An unexpected error occurred: {e}")

            except ValueError as e:
                print(f"Session {session_type} does not exist for this race : {e}")
                continue

            except Exception as e:
                print(f"An unexpected error occurred: {e}")

    def upload_parquet_to_s3(self, df, s3_path):
        try:
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)
            self.s3_client.put_object(Bucket=self.bucket, Key=s3_path, Body=buffer.getvalue())
            print(f"Successfully uploaded {s3_path} to S3.")
        except Exception as e:
            print(f"Failed to upload {s3_path} to S3. Error: {e}")

    def initial_load(self, start_year, end_year):
        for year in range(start_year, end_year + 1):
            f1_schedule = fastf1.get_event_schedule(year)
            for _, row in f1_schedule.iterrows():
                race_name = row['EventName']
                if row['EventFormat'] == 'testing':
                    continue
                race_date = row['Session5DateUtc']
                if race_date < datetime.datetime.utcnow():
                    if row['F1ApiSupport']:
                        session_types = ['FP1', 'FP2', 'FP3', 'Q', 'R']
                        self.fetch_and_upload_race(year, race_name, session_types)

    def fetch_and_load_latest_race(self) -> str:
        table = dynamodb_resource.Table(events_table)

        response = table.scan(
            FilterExpression=Attr('Processed').eq(False)
        )

        events = response['Items']

        if not events:
            return ""

        # Find the latest race event
        latest_event = min(events, key=lambda x: x['EventDate'])
        session_types = ['FP1', 'FP2', 'FP3', 'Q', 'R']
        self.fetch_and_upload_race(datetime.datetime.now().year, latest_event['EventName'], session_types)
        return latest_event['EventName']

    def mark_latest_events_as_processed(self, event_name):
        table = dynamodb_resource.Table(events_table)

        response = table.scan(
            FilterExpression=Attr('EventName').eq(event_name) & Attr('Processed').eq(False)
        )
        if response['Items']:
            event = response['Items'][0]
            dynamodb_client.update_item(
                TableName='F1EventsSchedule',
                Key={
                    'EventName': {'S': event['EventName']},
                    'EventDate': {'S': event['EventDate']}
                },
                UpdateExpression='SET #P = :val1',
                ExpressionAttributeNames={
                    '#P': 'Processed'  # Escape reserved keyword
                },
                ExpressionAttributeValues={
                    ':val1': {'BOOL': True}
                },
            )

