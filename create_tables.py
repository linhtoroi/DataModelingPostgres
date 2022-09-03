import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def create_database():
    conn = psycopg2.connect(database="postgres", user='postgres', password='0908', host='127.0.0.1', port= '5432')
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute('''DROP DATABASE IF EXISTS sparkifydb''')
    cursor.execute('''CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0''')
    cursor.close()
    conn.close()

    conn = psycopg2.connect(database="sparkifydb", user='postgres', password='0908', host='127.0.0.1', port= '5432')
    cursor = conn.cursor()
    return conn, cursor

def create_tables(cursor, conn):
    for command in create_table_queries:
        cursor.execute(command)
        conn.commit()

def drop_tables(cursor, conn):
    for command in drop_table_queries:
        cursor.execute(command)
        conn.commit()

def main():
    conn, cur = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()

