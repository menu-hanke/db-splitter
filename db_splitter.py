"""
    Utility for partitioning simulation result database to n databases while keeping all information 
    related to any given stand consistently within a single database.
"""
import argparse
import os
import sqlite3
import prtpy


def partition_db(in_db: sqlite3.Connection, output_db_count: int):
    """Partition given database to output_db_count smaller databases.

    Args:
        in_db (sqlite3.Connection): open connection to simulation result database
        output_db_count (int): number of databases to split the results into
    """

    in_cur = in_db.cursor()

    # initialize output databases
    table_defs = in_cur.execute(
        """--sql
        SELECT sql FROM sqlite_master WHERE type='table';                   
        """
    ).fetchall()

    for i in range(1, output_db_count + 1):
        with sqlite3.connect(f"out_{i}.db") as out_db:
            out_cur = out_db.cursor()
            for table_def in table_defs:
                out_cur.execute(table_def[0])
            out_db.commit()

    # calculate number of tree rows for each stand
    in_cur.execute(
        """--sql
        SELECT stands.identifier, COUNT(*)
        FROM stands INNER JOIN trees
        ON stands.identifier = trees.stand AND
        stands.node = trees.node
        GROUP BY stands.identifier;
        """
    )
    row_counts: dict[str, int] = dict(in_cur)

    # partition to output_db_count bins
    partitioning: list[list[str]] = prtpy.partition(algorithm=prtpy.partitioning.greedy,
                                                    numbins=output_db_count, items=row_counts)

    # write to output dbs
    table_names = in_cur.execute(
        """--sql
        SELECT name FROM sqlite_master WHERE type='table';
        """
    ).fetchall()

    for i, stands in enumerate(partitioning, 1):
        in_cur.execute(
            f"""--sql
            ATTACH DATABASE 'out_{i}.db' as out_db;
            """
        )
        for table_ in table_names:
            table = table_[0]
            in_cur.execute(
                f"""--sql
                INSERT INTO out_db.{table}
                SELECT * FROM main.{table}
                WHERE main.{table}.{"stand" if table != "stands" else "identifier"}
                IN ({', '.join('?' for _ in stands)});
                """,
                stands
            )
        in_db.commit()
        in_cur.execute(
            """--sql
            DETACH DATABASE out_db;
            """
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('input_db', type=str)
    parser.add_argument('output_db_count', type=int)
    args = parser.parse_args()

    for j in range(1, args.output_db_count + 1):
        if os.path.isfile(f"out_{j}.db"):
            os.remove(f"out_{j}.db")

    with sqlite3.connect(args.input_db) as in_db_:
        partition_db(in_db_, args.output_db_count)
