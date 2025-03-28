from typing import Tuple, Union, Optional, Any, Dict

from db_template import DBTemplate


class DBMovie(DBTemplate):
    """This class is to create a movie table in database."""

    def drop_table(self, table_name: str) -> None:
        """Delete or Drop a table from database.

        Args:
            table_name(str): Provide a Table Name.

        """
        with self.cursor as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.connection.commit()  # type: ignore

    def create_table(self, table_name: str, column_names: str) -> None:
        """Create a new table in database.

        Args:
            table_name(str): Provide a Table Name.
            column_names(str): Provide column names with SQL data types.

        """
        with self.cursor as cursor:
            # pylint: disable=line-too-long
            self.table_name = table_name
            table_query = f"CREATE TABLE {table_name} {column_names}"
            cursor.execute(table_query)
            self.connection.commit()  # type: ignore

    def copy_data_to_table(
            self, table_name: str, column_names: str, file_path: str
    ) -> None:
        """Copy data from csv, xlsx to table columns.

        Args:
            table_name: Provide a Table Name.
            column_names: Provide SQL column names of a table
            file_path(str): data file path of CSV, XLSX etc.

        """
        with self.cursor as cursor:
            # pylint: disable=line-too-long
            copy_to_table_query = f"COPY {table_name}{column_names} FROM '{file_path}' WITH (FORMAT CSV, HEADER)"
            cursor.execute(copy_to_table_query)
            self.connection.commit()  # type: ignore

    def create_table_backup(self, table_name: str, suffix: str = "_backup") -> None:
        """Crete backup of a table.

        Args:
            table_name(str): Table name from DB.
            suffix(str): default _backup suffix to the table name.

        """
        with self.cursor as cursor:
            self.backup_table_name = f"{self.table_name}{suffix}"
            backup_table_query = f"CREATE TABLE {self.backup_table_name} AS SELECT * FROM {self.table_name}"
            cursor.execute(backup_table_query)
            self.connection.commit()  # type: ignore

    def update_table(self, update_query: str) -> None:
        """Update or Alter table in database.

        Args:
            update_query(str): Provide a query to update database.

        """
        with self.cursor as cursor:
            cursor.execute(update_query)
            self.connection.commit()

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
            None if "all" or "one" are not mentioned else list of items.

        """
        with self.cursor as cursor:
            cursor.execute(query, params)
            if fetch == "all":
                return cursor.fetchall()
            if fetch == "one":
                return cursor.fetchone()
            return None


if __name__ == "__main__":
    # https: // www.cybertec - postgresql.com / en / pg_service - conf - the - forgotten - config - file /
    # Connect to table with the help of config
    db2 = DBMovie(service="my_service")
    db2.connect()

    # # Delete the existing table
    # db2.drop_table(table_name="movies")
    # db2.drop_table(table_name="movies_backup")

    # # Create table with columns, and copy data from csv to movies tables
    # db2.create_table(table_name="movies",
    #                  column_names="(id SERIAL PRIMARY KEY, Film VARCHAR(250) NOT NULL, Genre VARCHAR(50) NOT NULL, Lead_Studios VARCHAR(250) NOT NULL, Audience_Score_Percent INTEGER NOT NULL, Profitability DECIMAL NOT NULL, Rotten_Tomatoes_Percent INTEGER NOT NULL, Worldwide_Gross TEXT NOT NULL, Year INTEGER NOT NULL)")
    # db2.copy_data_to_table(table_name=db2.table_name,
    #                        column_names="(Film, Genre, Lead_Studios, Audience_Score_Percent, Profitability, Rotten_Tomatoes_Percent, Worldwide_Gross, Year)",
    #                        file_path="F:\\All_Projs\\Python_Proj\\postgres_db_template\\data_files\\movies.csv")

    # db2 = DBMovie(service="my_service", table_name="movies")
    # db2.connect()
    # # Create a backup of the table before an alter or update
    # db2.create_table_backup(table_name=db2.table_name)

    # # Update table column that has spelling errors
    # db2.update_table(update_query=f"UPDATE {db2.table_name} SET genre = INITCAP(genre) WHERE genre ~'(rom)'")  # capitalize first alphabet
    # db2.update_table(update_query=f"UPDATE {db2.table_name} SET genre = REGEXP_REPLACE(genre, 'Comdy', 'Comedy') WHERE genre ~ 'Comdy'")  # Comdy -> Comedy correct spelling
    # db2.update_table(update_query=f"UPDATE {db2.table_name} SET genre = REGEXP_REPLACE(genre, 'Romence', 'Romance') WHERE genre ~ 'Romence'")  # Romence -> Romance correct spelling

    # # Query - Named arguments
    # # pylint: disable=invalid-name
    # db_query1_5 = """SELECT genre FROM movies WHERE genre=%(rom)s"""
    # print(
    #     db2.query_table(
    #         query=db_query1_5,
    #         params={
    #             "rom": "Romance",
    #         },
    #         fetch="all",
    #     )
    # )
    # # Query - Passing parameters
    # # pylint: disable=invalid-name
    # db_query2 = """SELECT genre FROM movies WHERE genre = %s"""
    # print(db2.query_table(query=db_query2, params=("Romance",), fetch="all"))
    db2.close()
