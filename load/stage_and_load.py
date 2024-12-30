from dotenv import load_dotenv
import os
import snowflake.connector

load_dotenv("../.env.local")


def initial_load():
    # 1. Connect to Snowflake
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )
    cursor = conn.cursor()
    try:

        create_laps_table_sql = """
                CREATE OR REPLACE TABLE laps_staging (
                  circuit_name       STRING,
                  load_time          TIMESTAMP_NTZ,
                  session_type       STRING,
                  Time               STRING,
                  Driver             STRING,
                  DriverNumber       STRING,
                  LapTime            STRING,
                  LapNumber          STRING,
                  Stint              STRING,
                  PitOutTime         STRING,
                  PitInTime          STRING,
                  Sector1Time        STRING,
                  Sector2Time        STRING,
                  Sector3Time        STRING,
                  Sector1SessionTime STRING,
                  Sector2SessionTime STRING,
                  Sector3SessionTime STRING,
                  SpeedI1            STRING,
                  SpeedI2            STRING,
                  SpeedFL            STRING,
                  SpeedST            STRING,
                  IsPersonalBest     STRING,
                  Compound           STRING,
                  TyreLife           STRING,
                  FreshTyre          STRING,
                  Team               STRING,
                  LapStartTime       STRING,
                  LapStartDate       STRING,
                  TrackStatus        STRING,
                  Position           STRING,
                  Deleted            STRING,
                  DeletedReason      STRING,
                  file_name          STRING
                );
            """
        create_weather_table_sql = """
            CREATE OR REPLACE TABLE weather_staging (
                circuit_name       STRING,
                load_time          TIMESTAMP_NTZ,
                session_type       STRING, 
                Time               STRING,
                AirTemp            STRING,
                Humidity           STRING,
                Pressure           STRING,
                Rainfall           STRING,
                TrackTemp          STRING,
                WindDirection      STRING,
                WindSpeed          STRING,
                file_name          STRING
            );
        """
        create_drivers_info_table_sql = """
        CREATE OR REPLACE TABLE drivers_info_staging (
            circuit_name       STRING,
            load_time          TIMESTAMP_NTZ,
            session_type       STRING,
            DriverNumber       STRING,
            BroadcastName      STRING,
            Abbreviation       STRING,
            DriverId           STRING,
            TeamName           STRING,
            TeamColor          STRING,
            TeamId             STRING,
            FirstName          STRING,
            LastName           STRING,
            FullName           STRING,
            HeadshotUrl        STRING,
            CountryCode        STRING,
            Position           STRING,
            ClassifiedPosition STRING,
            GridPosition       STRING,
            Q1                 STRING,
            Q2                 STRING,
            Q3                 STRING,
            Time               STRING,
            Status             STRING,
            Points             STRING,
            file_name          STRING
        );
        """

        cursor.execute(create_laps_table_sql)
        cursor.execute(create_weather_table_sql)
        cursor.execute(create_drivers_info_table_sql)

        laps_sql = """
                COPY INTO laps_staging
                FROM @my_f1_stage
                FILE_FORMAT = (TYPE = PARQUET)
                PATTERN = '.*laps.parquet'
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                ON_ERROR = CONTINUE
            """

        weather_sql = """
                COPY INTO weather_staging
                FROM @my_f1_stage
                FILE_FORMAT = (TYPE = PARQUET)
                PATTERN = '.*weather.parquet'
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                ON_ERROR = CONTINUE
            """

        driver_info_sql = """
                COPY INTO drivers_info_staging
                FROM @my_f1_stage
                FILE_FORMAT = (TYPE = PARQUET)
                PATTERN = '.*drivers_info.parquet'
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                ON_ERROR = CONTINUE
            """

        cursor.execute(laps_sql)
        cursor.execute(weather_sql)
        cursor.execute(driver_info_sql)
        print("Initial load complete!")

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    initial_load()
