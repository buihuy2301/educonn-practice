"""
This script is used to run the DDL.sql file to create the tables in the PostgreSQL database.
"""

import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def main():
    """
    Run the DDL.sql file to create the tables in the PostgreSQL database
    """
    # Connect to PostgreSQL
    home = os.environ["HOME"]
    sql_dir = f"{home}/work/setup"
    postgress_config = {
        "database": os.environ["POSTGRES_DATABASE"],
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "host": os.environ["POSTGRES_HOST"],
        "port": os.environ["POSTGRES_PORT"],
    }

    # Create a cursor object
    conn = psycopg2.connect(**postgress_config)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    with open(f"{sql_dir}/DDL.sql", "r", encoding="UTF-8") as f:
        sql_queries = f.read()

    cur.execute(sql_queries)

    # Close the cursor and connection
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
