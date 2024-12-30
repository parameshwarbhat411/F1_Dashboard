import boto3
import pandas as pd
from io import BytesIO
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError


def list_parquet_files(bucket_name, prefix):
    """
    Recursively lists all Parquet files in an S3 bucket under the given prefix.
    """
    try:
        s3 = boto3.client("s3")
        paginator = s3.get_paginator("list_objects_v2")
        parquet_files = []

        # Paginate through all objects under the prefix
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    parquet_files.append(obj["Key"])
        return parquet_files
    except (NoCredentialsError, PartialCredentialsError) as e:
        print("AWS credentials not found or incomplete. Please check your configuration.")
        raise e
    except ClientError as e:
        print(f"Error accessing S3 bucket '{bucket_name}': {e}")
        raise e
    except Exception as e:
        print(f"Unexpected error while listing files: {e}")
        raise e


def process_and_overwrite_parquet_files(bucket_name, prefix):
    """
    Recursively processes Parquet files, adds metadata columns, and overwrites them in S3.
    """
    try:
        s3 = boto3.client("s3")

        # List all Parquet files under the given prefix
        files = list_parquet_files(bucket_name, prefix)

        if not files:
            print(f"No Parquet files found under prefix '{prefix}' in bucket '{bucket_name}'.")
            return

        for file_path in files:
            try:
                # Extract circuit_name and session_type from the file path
                parts = file_path.split("/")
                circuit_name = parts[-2]  # Adjust index based on your S3 structure
                session_type = parts[-1].split("_")[0]  # Extract session type from the file name
                year = parts[1]

                # Download the Parquet file from S3
                obj = s3.get_object(Bucket=bucket_name, Key=file_path)
                parquet_data = BytesIO(obj["Body"].read())

                # Read the Parquet file into a Pandas DataFrame
                df = pd.read_parquet(parquet_data, engine="pyarrow")

                # Add metadata columns
                # df["circuit_name"] = circuit_name
                # df["session_type"] = session_type
                df['year'] = year

                # Save the updated DataFrame to Parquet
                updated_parquet_data = BytesIO()
                df.to_parquet(updated_parquet_data, engine="pyarrow", index=False)
                updated_parquet_data.seek(0)

                # Overwrite the same file in S3
                s3.put_object(Bucket=bucket_name, Key=file_path, Body=updated_parquet_data)
                updated_parquet_data.close()
                parquet_data.close()
                print(f"Processed and overwritten: {file_path}")

            except ClientError as e:
                print(f"Error accessing file '{file_path}' in bucket '{bucket_name}': {e}")
                continue  # Skip to the next file
            except pd.errors.EmptyDataError as e:
                print(f"Parquet file '{file_path}' is empty or invalid: {e}")
                continue
            except Exception as e:
                print(f"Unexpected error while processing file '{file_path}': {e}")
                continue

    except Exception as e:
        print(f"Unexpected error during the processing of files: {e}")
        raise e


if __name__ == "__main__":
    bucket_name = "race-predictor-pro"
    input_prefix = "f1_data/"

    try:
        process_and_overwrite_parquet_files(bucket_name, input_prefix)
    except Exception as e:
        print(f"Critical failure in the processing pipeline: {e}")