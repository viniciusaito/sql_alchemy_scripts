from sqlalchemy import create_engine, MetaData, Table, select, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text

# Database connection details
DATABASE_URI = 'your_database_uri_here'
TABLE_NAME = 'your_table_name_here'
COLUMN_NAME = 'your_column_name_here'

# Initialize SQLAlchemy engine and metadata
engine = create_engine(DATABASE_URI)
metadata = MetaData()

def backup_table():
    """Create a backup of the table by appending '_bak' to its name.

    This function creates a temporary backup table by copying all data from the original table.
    If a backup table already exists, it is dropped before creating a new one.

    Raises:
        SQLAlchemyError: If an error occurs during the backup process.
    """
    try:
        with engine.connect() as connection:
            # Create the backup table name
            backup_table_name = f"{TABLE_NAME}_bak"
            
            # Drop the backup table if it already exists
            connection.execute(text(f"DROP TABLE IF EXISTS {backup_table_name}"))
            
            # Create the backup table by selecting all data from the original table
            connection.execute(text(f"CREATE TABLE {backup_table_name} AS SELECT * FROM {TABLE_NAME}"))
            
            print(f"Backup created: {backup_table_name}")
    except SQLAlchemyError as e:
        print(f"Error during backup: {e}")

def update_column():
    """Update the specified column from True to False.

    This function updates the specified column in the original table, setting its value
    to False where it was previously True.

    Raises:
        SQLAlchemyError: If an error occurs during the update process.
    """
    try:
        with engine.connect() as connection:
            # Reflect the original table
            original_table = Table(TABLE_NAME, metadata, autoload_with=engine)
            
            # Update the column where the value is True
            stmt = (
                update(original_table)
                .where(original_table.c[COLUMN_NAME] == True)
                .values({COLUMN_NAME: False})
            )
            connection.execute(stmt)
            connection.commit()
            
            print(f"Updated column '{COLUMN_NAME}' from True to False.")
    except SQLAlchemyError as e:
        print(f"Error during update: {e}")

def restore_column():
    """Restore the column values from the backup table.

    This function restores the original values of the specified column from the backup table.
    If the backup table does not exist, the function exits without performing any operations.
    After restoring the values, the backup table is dropped.

    Raises:
        SQLAlchemyError: If an error occurs during the restore process.
    """
    try:
        with engine.connect() as connection:
            # Reflect the original table
            original_table = Table(TABLE_NAME, metadata, autoload_with=engine)
            backup_table_name = f"{TABLE_NAME}_bak"
            
            # Check if the backup table exists
            if not engine.dialect.has_table(connection, backup_table_name):
                print(f"No backup table found: {backup_table_name}")
                return
            
            # Reflect the backup table
            backup_table = Table(backup_table_name, metadata, autoload_with=engine)
            
            # Select the original True values from the backup table
            stmt = select(backup_table).where(backup_table.c[COLUMN_NAME] == True)
            true_values = connection.execute(stmt).fetchall()
            
            # Restore the original True values to the original table
            for row in true_values:
                stmt = (
                    update(original_table)
                    .where(original_table.c.id == row.id)  # Assuming there's an 'id' column
                    .values({COLUMN_NAME: True})
                )
                connection.execute(stmt)
            
            connection.commit()
            print(f"Restored column '{COLUMN_NAME}' values from backup.")
            
            # Drop the backup table after restoration
            connection.execute(text(f"DROP TABLE {backup_table_name}"))
            print(f"Backup table '{backup_table_name}' removed.")
    except SQLAlchemyError as e:
        print(f"Error during restore: {e}")

def main(command):
    """Handle the command to either update or restore the column values.

    Args:
        command (str): The command to execute. Must be either 'on' or 'off'.
            - 'on': Creates a backup, updates the column from True to False.
            - 'off': Restores the column values from the backup and removes the backup table.

    Raises:
        ValueError: If an invalid command is provided.
    """
    if command == 'on':
        # Backup the table only when the command is 'on'
        backup_table()
        update_column()
    elif command == 'off':
        # Restore the column values and remove the backup table
        restore_column()
    else:
        raise ValueError("Invalid command. Please use 'on' or 'off'.")

if _name_ == "_main_":
    import sys
    
    if len(sys.argv) != 2:
        print("Usage: python script.py <on|off>")
        sys.exit(1)
    
    command = sys.argv[1].lower()
    try:
        main(command)
    except ValueError as e:
        print(e)
        sys.exit(1)
