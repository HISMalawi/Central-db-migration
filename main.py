from db_connection import load_config, create_engine_connection
from migration_helpers import fetch_health_center_id, migrate_table

def migrate_data():
    """Main migration logic."""  
    config = load_config()
    central_db_url = config["central_db"]
    source_databases = config["source_databases"]
    tables_to_migrate = config["tables_to_migrate"]

    # Connect to the central database
    central_engine = create_engine_connection(central_db_url)

    for db_name, db_config in source_databases.items():
        print(f"Fetching data from {db_name}")

        # Fetch site_id
        source_engine = create_engine_connection(
            f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"
        )
        site_id = fetch_health_center_id(source_engine)
 
        if not site_id:
            print(f"No site_id found for {db_name}. Skipping migration.")
            continue

        for table_name in tables_to_migrate:
            migrate_table(db_name, db_config, table_name, site_id, central_engine, disable_fk_checks=True)

if __name__ == "__main__":
    migrate_data()
