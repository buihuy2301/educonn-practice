"""
ETL pipeline for mock data
"""

import logging
import os

import s3fs
from ETL import Pipeline


def create_pipeline(
    table_name: str,
    local_data_dir: str,
    bucket: str,
    fs: s3fs.S3FileSystem,
    postgress_config: dict,
):
    """
    Create ETL pipeline for mock data from local to S3 and S3 to Postgres
    Args:
        table_name (str): name of the table
        local_data_dir (str): local directory path
        bucket (str): s3 bucket name
        fs (s3fs.S3FileSystem): s3 file system client
        postgress_config (dict): postgres connection configuration
    """
    pipeline = Pipeline()
    try:
        pipeline.local_to_fs_transfer(
            local_path=f"{local_data_dir}/{table_name}.parquet",
            fs_path=f"s3://{bucket}/{table_name}",
            fs=fs,
        )
        pipeline.fs_to_postgres_transfer(
            path=f"s3://{bucket}/{table_name}",
            fs=fs,
            config=postgress_config,
            table_name=table_name,
        )
        logging.info("ETL pipeline completed for table: %s", table_name)
    except Exception as e:  # pylint: disable=broad-except
        logging.error("Error in ETL pipeline for table %s: %s", table_name, str(e))


def main():
    """
    Generate mock data for customer and loans tables and write to S3
    """
    # declare configurations
    home = os.environ["HOME"]
    local_data_dir = f"{home}/work/data"
    bucket = "landing-zone"
    table_names = ["customers", "loans"]
    fs = s3fs.S3FileSystem(
        anon=False,
        use_ssl=False,
        client_kwargs={
            "endpoint_url": os.environ["MINIO_ENDPOINT"],
            "aws_access_key_id": os.environ["MINIO_ROOT_USER"],
            "aws_secret_access_key": os.environ["MINIO_ROOT_PASSWORD"],
            "verify": False,
        },
    )
    postgress_config = {
        "database": os.environ["POSTGRES_DATABASE"],
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "host": os.environ["POSTGRES_HOST"],
        "port": os.environ["POSTGRES_PORT"],
    }

    for table_name in table_names:
        if not os.path.exists(f"{local_data_dir}/{table_name}.parquet"):
            raise FileNotFoundError(
                f"File {local_data_dir}/{table_name}.parquet not found"
            )
        else:
            create_pipeline(
                table_name=table_name,
                local_data_dir=local_data_dir,
                bucket=bucket,
                fs=fs,
                postgress_config=postgress_config,
            )

if __name__ == "__main__":
    main()
