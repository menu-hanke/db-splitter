import sqlite3
import prtpy


def partition_db(in_db: sqlite3.Connection, out_dbs: list[sqlite3.Connection]):
    # calculate number of tree rows for each stand
    in_cur = in_db.cursor()
    in_cur.execute(
        """--sql
        SELECT stands.identifier, COUNT(*)
        FROM stands INNER JOIN trees 
        ON stands.identifier == trees.stand AND
        stands.node == trees.node
        GROUP BY stands.identifier;
    """)
    row_counts: dict[str, int] = dict(in_cur)

    # partition to len(out_dbs) bins
    partitioning: list[list[str]] = prtpy.partition(algorithm=prtpy.partitioning.greedy,
                                                    numbins=len(out_dbs), items=row_counts)

    # Write to output dbs based on partition
    for out_db, stands in zip(out_dbs, partitioning):
        out_cur = out_db.cursor()

        # nodes
        in_cur.execute(
            f"""--sql
            SELECT * FROM nodes
            WHERE stand IN ({", ".join('?' for _ in stands)});
            """,
            stands
        )

        out_cur.executemany(
            """--sql
            INSERT INTO nodes
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            in_cur
        )

        # stands
        in_cur.execute(
            f"""--sql
            SELECT * FROM stands
            WHERE identifier IN ({", ".join('?' for _ in stands)});
            """,
            stands
        )

        out_cur.executemany(
            """--sql
            INSERT INTO stands
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            in_cur
        )

        # trees
        # strata
        # collected data
