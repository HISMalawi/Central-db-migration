# central_db_migration

This Python script is used to migrate data from multiple facility databases to a central database. The script ensures consistency and integrity while handling data transformations as required.

---

## Features

- Connects to multiple facility databases.
- Extracts, transforms, and loads (ETL) data into a central database.
- Supports logging and error handling for reliable data migration.
- Configurable database connections and table mappings.

---

## Prerequisites

- **Python version**: `>=3.8`
- Required Python packages (install using `pip install -r requirements.txt`):
  - `mysql-connector-python`
  - `pandas`
  - `sqlalchemy`
  - `logging`

---

## Configuration

The migration script uses a YAML configuration file to define database connections, source databases, and the tables to migrate. This file centralizes all configuration details, making the migration process flexible and easy to manage.

---

## Key Sections

### 1. **Central Database (`central_db`)**

Defines the connection string for the central database where the data will be consolidated. This is specified using SQLAlchemy's database URI format.

**Format**:  
```yaml
central_db: "mysql+pymysql://<user>:<password>@<host>/<database>"
```


### 2. Source Database 

Lists the source databases with their connection details. Each database entry includes:

```yaml
host: The hostname or IP address of the database server.
user: The username for authentication.
password: The password for authentication (leave blank if no password is required).
database: The name of the database to connect to.
```


