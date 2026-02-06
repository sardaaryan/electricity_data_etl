from sqlalchemy import create_engine, text
import pandas as pd

def get_engine(db_name="electricity_data.sqlite"):
    """Creates a connection engine to the SQLite database."""
    return create_engine(f"sqlite:///data/{db_name}")

def get_latest_date(engine, table_name):
    """Returns the most recent date in the DB as a string (YYYY-MM-DD)."""
    query = text(f"SELECT MAX(month) FROM {table_name}")
    try:
        with engine.connect() as conn:
            result = conn.execute(query).fetchone()
            # result[0] will be '2025-11-01' or None if table is empty
            return result[0] if result and result[0] else None
    except Exception as e:
        # This handles the case where the table doesn't even exist yet
        print(f"Note: Table '{table_name}' not found or empty. Starting fresh.")
        return None

def load_to_sqlite(df, table_name, engine):
    """
    Loads the DataFrame into the SQLite table.
    if_exists='append' to keep adding new months.
    """
    df.to_sql(table_name, con=engine, if_exists='append', index=False)
    print(f"Successfully loaded {len(df)} new rows into table: {table_name}")