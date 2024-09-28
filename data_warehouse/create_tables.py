import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
def check_tables(cur):
    table_names = ["staging_events", "staging_songs", "songplays", "users", "songs", "artists", "time"]
    for table in table_names:
        cur.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table}');")
        exists = cur.fetchone()[0]
        if exists:
            print(f"Table {table} exists.")
        else:
            print(f"Table {table} does not exist.")

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)
    check_tables(cur)
    conn.close()


if __name__ == "__main__":
    main()