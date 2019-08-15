import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    print("Checking existing tables to drop .....")
    cnt = 0
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        cnt = cnt + 1
    print(str(cnt) + " tables droped.")
    


def create_tables(cur, conn):
    print("Creating tables....")
    cnt = 0
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        cnt = cnt + 1
    print(str(cnt) + " tables created.")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()