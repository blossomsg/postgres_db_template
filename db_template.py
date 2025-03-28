"""Imports for Database template"""

import abc
import logging.config
from typing import Any, Dict, Optional, Tuple, Union

# pylint: disable=import-error
import psycopg2

from utils.read_yaml_config import read_yaml_config

CONFIG_FILE = "logconfig.yaml"
logging.config.dictConfig(read_yaml_config(CONFIG_FILE))  # type: ignore


class DBTemplate(abc.ABC):
    """This is a postgres database template to create table with data."""

    logger = logging.getLogger(__name__)

    def __init__(self, service: str, table_name: Optional[str] = None) -> None:
        self.service = service  # provide a .conf file by postgres
        self.table_name = table_name
        self.backup_table_name: Optional[str] = None
        self.table_column_names: Optional[str] = None
        self.connection: Optional[psycopg2.extensions.connection] = None

    def connect(self) -> None:
        """Connect database with credentials.
        https://github.com/qgis/QGIS/issues/30027#issuecomment-503138335
        https://www.postgresql.org/docs/current/libpq-pgservice.html
        https://www.postgresql.org/docs/current/libpq-envars.html

        PGSERVICEFILE specifies the name of the per-user connection
        service file. Defaults to ~/.pg_service.conf,
        or %APPDATA%/postgresql/.pg_service.conf on Microsoft Windows.
        """
        self.connection = psycopg2.connect(service=self.service)

    @property
    def cursor(self) -> psycopg2.extensions.cursor:
        """Connect database cursor."""
        if not self.connection:
            raise psycopg2.OperationalError(
                "Database not connected. Call connect() first"
            )
        return self.connection.cursor()

    def create_table(self, table_name: str, column_names: str) -> None:
        """Create a new table in database.

        Args:
            table_name(str): Provide a Table Name.
            column_names(str): Provide column names with SQL data types.

        """

    def drop_table(self, table_name: str) -> None:
        """Delete or Drop a table from database.

        Args:
            table_name(str): Provide a Table Name.

        """

    def insert_data(self, table_name: str) -> None:
        """Insert values in columns.

        Args:
            table_name(str): Provide a Table Name.

        """

    def copy_data_to_table(
            self, table_name: str, column_names: str, file_path: str
    ) -> None:
        """Copy data from csv, xlsx to table columns.

        Args:
            table_name: Provide a Table Name.
            column_names: Provide SQL column names of a table
            file_path(str): data file path of CSV, XLSX etc.

        """

    def update_table(self, update_query: str) -> None:
        """Update or Alter table in database.

        Args:
            update_query(str): Provide a query to update database.

        """

    def query_table(
            self,
            query: str,
            params: Optional[Union[Dict[str, Any], Tuple[Union[str, int]]]] = None,
            fetch: Optional[str] = None,
    ) -> Optional[Any]:
        """Query and fetchall or fetchone from database.
        https://www.psycopg.org/psycopg3/docs/basic/params.html#query-parameters
        https://www.psycopg.org/psycopg3/docs/basic/adapt.html#types-adaptation

        Args:
            query(str): provide a query statement for database.
            params(Optional[Union[Dict[str, Any], Tuple[Union[str, int]]]]):
            parameters for database query to avoid SQL injection.
            fetch(Optional[str]): "all" or "one" values to execute fetchall and fetchone command.

        Returns:
            Optional[Union[List[Tuple[Any]], Tuple[Any]]].
            None if "all" or "one" are not mentioned else list of items.

        """

    def create_table_backup(self, table_name: str, suffix: str = "_backup") -> None:
        """Crete backup of a table.

        Args:
            table_name(str): Table name from DB.
            suffix(str): default _backup suffix to the table name.

        """

    def close(self) -> None:
        """Close database connection."""
        self.connection.close()  # type: ignore
