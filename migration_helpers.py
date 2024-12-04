from sqlalchemy import text
import pymysql

def fetch_health_center_id(engine):
    """Fetch the site_id (health center ID) from the source database."""
    query = text("SELECT property_value FROM global_property WHERE property = 'current_health_center_id';")
    try:
        with engine.connect() as conn:
            result = conn.execute(query)
            row = result.fetchone()
            return row[0] if row else None
    except Exception as e:
        print(f"Error fetching health center ID: {e}")
        return None

def chunked_insert(cursor, insert_query, rows, chunk_size=1000):
    """Insert rows in chunks to avoid large memory usage."""
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i + chunk_size]
        cursor.executemany(insert_query, chunk)
        cursor.connection.commit()

def migrate_table(db_name, db_config, table_name, site_id, central_engine, disable_fk_checks=False):
    """Migrate data for a specific table from source to central database."""
    print(f"Processing table: {table_name} for site_id: {site_id}")
    try:
        # Source database connection
        source_conn = pymysql.connect(
            host=db_config["host"],
            user=db_config["user"],
            password=db_config["password"],
            database=db_config["database"],
        )
        cursor = source_conn.cursor()

        # Fetch rows from source table
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
 
        if not rows:
            print(f"No data found in {table_name}.")
            return

        print(f"Inserting {len(rows)} rows into {table_name}")

        # Prepare insert query
        columns = [col[0] for col in cursor.description]

        # Skip adding site_id for specific tables
        if table_name not in ["role_privilege", "role_role", "person_attribute", "pharmacy_obs"]:
            columns.append("site_id")

        insert_query = f""" INSERT INTO {table_name} ({', '.join(columns)}) 
                            VALUES ({', '.join(['%s'] * len(columns))}) 
                            ON DUPLICATE KEY UPDATE {', '.join([f'{col}=VALUES({col})' for col in columns])}; """

        # Append site_id to each row if necessary
        if table_name not in ["role_privilege", "role_role", "person_attribute", "pharmacy_obs"]:
            rows_with_site_id = [row + (site_id,) for row in rows]
        else:
            rows_with_site_id = rows

        # Central database connection
        with central_engine.connect() as central_conn:
            with central_conn.begin():
                if disable_fk_checks:
                    central_conn.execute(text("SET FOREIGN_KEY_CHECKS=0;"))
                chunked_insert(central_conn.connection.cursor(), insert_query, rows_with_site_id)
                if disable_fk_checks:
                    central_conn.execute(text("SET FOREIGN_KEY_CHECKS=1;"))
        print(f"Inserted {len(rows)} rows into {table_name}")

    except Exception as e:
        print(f"Error processing {db_name}.{table_name}: {e}")
    finally:
        source_conn.close()
