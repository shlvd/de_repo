import pandas as pd
import argparse
from sqlalchemy import create_engine

parser = argparse.ArgumentParser(description='Ingest data to postgres.')

parser.add_argument('--user', required=True, help='user name for postgres')
parser.add_argument('--password', required=True, help='password for postgres')
parser.add_argument('--host', required=True, help='host for postgres')
parser.add_argument('--port', required=True, help='port for postgres')
parser.add_argument('--db', required=True, help='database name for postgres')
parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
parser.add_argument('--url', required=True, help='url of the csv file')


def main():
    """Main function to run the pipeline"""
    args = parser.parse_args()
    df = pd.read_parquet(args.url, engine="pyarrow")
    DB_URI = f"postgresql://{args.user}:{args.password}@{args.host}:{args.port}/{args.db}"
    engine = create_engine(DB_URI)
    df.to_sql(name=args.table_name, con=engine, if_exists="replace")


if __name__ == "__main__":
    main()