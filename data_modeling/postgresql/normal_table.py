import psycopg2

def get_connection():
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)
    conn.set_session(autocommit=True)
    return cur,conn

def create_table(cur, conn):
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS music_store (transaction_id int, \
                    customer_name varchar, cashier_name varchar, \
                    year int, albums_purchased text[]);")
    except psycopg2.Error as e:
        print("Error: Issue creating music store table")
        print(e)

def add_data(cur, conn):
    try:
        cur.execute("INSERT INTO music_store (transaction_id, customer_name, cashier_name, year, albums_purchased) \
                    VALUES (1, 'Amanda', 'Sam', 2000, ARRAY['Rubber Soul', 'Let it Be']);")
    except psycopg2.Error as e:
        print("Error: Issue adding data to music_store table")
        print(e)
    
    try:
        cur.execute("INSERT INTO music_store (transaction_id, customer_name, cashier_name, year, albums_purchased) \
                    VALUES(2, 'Toby', 'Sam', 2000, ARRAY['My Generation']);")
    except psycopg2.Error as e:
        print("Error: Issue adding data to music_store table")
        print(e)
    try:
        cur.execute("INSERT INTO music_store (transaction_id, customer_name, cashier_name, year, albums_purchased) \
                    VALUES (3, 'Max', 'Bob', 2018, ARRAY['Meet the Beatles','Help!']);")
    except psycopg2.Error as e:
        print("Error: Issue adding data to music_store table")
        print(e)

def fetch_data(cur, conn):
    try:
        cur.execute("SELECT * FROM music_store;")
        rows = cur.fetchall()
        for row in rows:
            print(row)
    except psycopg2.Error as e:
        print("Error: Issue fetching data from music_library table")
        print(e)

def drop_table(cur, conn, table_name):
    print(f"Dropping table {table_name}")
    try:
        cur.execute(f"DROP TABLE IF EXISTS {table_name};")
    except psycopg2.Error as e:
        print("Error: Issue dropping table")
        print(e)

def create_first_normal_form(cur, conn):
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS music_store2 (transaction_id int, \
                    customer_name varchar, cashier_name varchar, \
                    year int, albums_purchased text);")
    except psycopg2.Error as e:
        print("Error: Issue creating music_store_customers 2 table")
        print(e)
        
def add_data_to_first_normal_form(cur, conn):
    try:
        cur.execute("INSERT INTO music_store2 (transaction_id, customer_name, cashier_name, year, albums_purchased) \
                    VALUES (1, 'Amanda', 'Sam', 2000, 'Rubber Soul');")
    except psycopg2.Error as e:
        print("Error: Issue adding data to music_store2 table")
        print(e)
    
    try:
        cur.execute("INSERT INTO music_store2 (transaction_id, customer_name, cashier_name, year, albums_purchased) \
                    VALUES (1, 'Amanda', 'Sam', 2000, 'Let it Be');")
    except psycopg2.Error as e:
        print("Error: Issue adding data to music_store2 table")
        print(e)
    
    try:
        cur.execute("INSERT INTO music_store2 (transaction_id, customer_name, cashier_name, year, albums_purchased) \
                    VALUES (2, 'Toby', 'Sam', 2000, 'My Generation');")
    except psycopg2.Error as e:
        print("Error: Issue adding data to music_store2 table")
        print(e)
    
    try:
        cur.execute("INSERT INTO music_store2 (transaction_id, customer_name, cashier_name, year, albums_purchased) \
                    VALUES (3, 'Max', 'Bob', 2018, 'Meet the Beatles');")
    except psycopg2.Error as e:
        print("Error: Issue adding data to music_store2 table")
        print(e)
    
    try:
        cur.execute("INSERT INTO music_store2 (transaction_id, customer_name, cashier_name, year, albums_purchased) \
                    VALUES (3, 'Max', 'Bob', 2018, 'Help!');")
    except psycopg2.Error as e:
        print("Error: Issue adding data to music_store2 table")
        print(e)
    
    # print data in the first normal form 
    cur.execute("SELECT * FROM music_store2;")
    row = cur.fetchone()
    while row is not None:
        print(row)
        row = cur.fetchone()

def create_second_normal_form(cur, conn):
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS transactions (transaction_id int, \
                    customer_name varchar, cashier_name varchar, \
                    year int);")
    except psycopg2.Error as e:
        print("Error: Issue creating transactions table")
        print(e)
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS albums_sold (album_id int, transaction_id int, \
                    album_name varchar);")
    except psycopg2.Error as e:
        print("Error: Issue creating albums_sold table")
        print(e)
    

def add_data_to_second_normal_form(cur, conn):
    try: 
        cur.execute("INSERT INTO transactions (transaction_id, customer_name, cashier_name, year) \
                 VALUES (%s, %s, %s, %s)", \
                 (1, "Amanda", "Sam", 2000))
    except psycopg2.Error as e: 
        print("Error: Inserting Rows")
        print (e)

    try: 
        cur.execute("INSERT INTO transactions (transaction_id, customer_name, cashier_name, year) \
                    VALUES (%s, %s, %s, %s)", \
                    (2, "Toby", "Sam", 2000))
    except psycopg2.Error as e: 
        print("Error: Inserting Rows")
        print (e)
        
    try: 
        cur.execute("INSERT INTO transactions (transaction_id, customer_name, cashier_name, year) \
                    VALUES (%s, %s, %s, %s)", \
                    (3, "Max", "Bob", 2018))
    except psycopg2.Error as e: 
        print("Error: Inserting Rows")
        print (e)
    
    try:
        cur.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name) \
                    VALUES (%s, %s, %s)", \
                    (1, 1, "Rubber Soul"))
    except psycopg2.Error as e:
        print("Error: Inserting Rows")
        print(e)
    
    try:
        cur.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name) \
                    VALUES (%s, %s, %s)", \
                    (2, 1, "Let it Be"))
    except psycopg2.Error as e:
        print("Error: Inserting Rows")
        print(e)
    
    try:
        cur.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name) \
                    VALUES (%s, %s, %s)", \
                    (3, 2, "My Generation"))
    except psycopg2.Error as e:
        print("Error: Inserting Rows")
        print(e)
    
    try:
        cur.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name) \
                    VALUES (%s, %s, %s)", \
                    (4, 3, "Meet the Beatles"))
    except psycopg2.Error as e:
        print("Error: Inserting Rows")
        print(e)
        
    try: 
        cur.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name) \
                    VALUES (%s, %s, %s)", \
                    (5, 3, "Help!"))
    except psycopg2.Error as e:
        print("Error: Inserting Rows")
        print(e)
    
    # print data in the second normal form 
    cur.execute("SELECT * FROM transactions;")
    row = cur.fetchone()
    while row is not None:
        print(row)
        row = cur.fetchone()
        
    cur.execute("SELECT * FROM albums_sold;")
    row = cur.fetchone()
    while row is not None:
        print(row)
        row = cur.fetchone()
    


        


def main():
    cur, conn = get_connection()
    create_table(cur, conn)
    add_data(cur, conn)
    fetch_data(cur, conn)

    # add first normal form
    create_first_normal_form(cur, conn)
    print("----------------------------------------------------")
    # add data to first normal form 
    add_data_to_first_normal_form(cur, conn)

    # create second normal form
    create_second_normal_form(cur, conn)
    print("----------------------------------------------------")
    # add data to second normal form 
    add_data_to_second_normal_form(cur, conn)

    #test with JOINS
    try:
        cur.execute("SELECT * FROM transactions JOIN albums_sold ON transactions.transaction_id = albums_sold.transaction_id;")
    except psycopg2.Error as e:
        print("Error: Issue fetching data from music_library table")
        print(e)
    
    rows = cur.fetchall()
    for row in rows:
        print(row)
    

    # create third normal form
    # create_third_normal_form(cur, conn)
    # print("----------------------------------------------------")
    # add data to third normal form 
    # add_data_to_third_normal_form(cur, conn)

    # drop tables in the end
    drop_table(cur, conn,"music_store")
    drop_table(cur, conn,"music_store2")
    drop_table(cur, conn,"transactions")
    drop_table(cur, conn,"albums_sold")
    conn.close()

if __name__ == "__main__":
    main()

