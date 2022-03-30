"""
Verifies the core functionality of sqlite tasks.
"""

import os

from prefect import Flow, task
from prefect.tasks.database.sqlite import SQLiteQuery, SQLiteScript


@task
def _show_output(std_out):
    """
    Displays the task's results.
    """
    print(std_out)


DB = "prefect.db"

with Flow("sqlite") as flow:
    script = """
        CREATE TABLE IF NOT EXISTS person(
            firstname,
            lastname,
            age
        );

        CREATE TABLE IF NOT EXISTS book(
            title,
            author,
            published
        );

        INSERT INTO book(title, author, published)
        values (
            'Dirk Gently''s Holistic Detective Agency',
            'Douglas Adams',
            1987
        );

        INSERT INTO book(title, author, published)
        values (
            'Prefection',
            'Prefectionist',
            2022
        );
    """
    # insert data
    sqlite_script = SQLiteScript(DB)(script=script)
    _show_output(sqlite_script)

    # parameterized query
    query = "SELECT * FROM book WHERE published = ? AND author = ?;"
    data = (2022, "Prefectionist")
    sqlite_query = SQLiteQuery(DB)(query=query, data=data)
    _show_output(sqlite_query)


if __name__ == "__main__":
    try:
        flow.run()
    finally:
        os.remove(DB)
