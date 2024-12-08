from neo4j import GraphDatabase
import psycopg2
import os

# PostgreSQL Database connection settings
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_NAME = os.getenv('DB_NAME', 'library')
DB_USER = os.getenv('DB_USER', 'librarian')
DB_PASS = os.getenv('DB_PASS', 'default_pass')  # default value for development
DB_PORT = os.getenv('DB_PORT', '5432')

# Neo4j connection settings
NEO4J_URI = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
NEO4J_USER = os.getenv('NEO4J_USER', 'neo4j')
NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD', 'default_neo4j_pass') 

# Initialize PostgreSQL connection
def get_pg_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT
        )
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

# Initialize Neo4j connection
class Neo4jConnection:
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def run_query(self, query, params=None):
        with self._driver.session() as session:
            return session.run(query, params)

neo4j_conn = Neo4jConnection(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)

def delete_all_borrowed_from_neo4j():
    try:
        query = """
        MATCH (u:User)-[r:BORROWED]->(b:Book)
        DELETE r
        """
        neo4j_conn.run_query(query)
        print("Deleted all BORROWED relationships from Neo4j.")
    except Exception as e:
        print(f"Error deleting all BORROWED relationships: {e}")
        
# Sync users
def sync_users_to_neo4j():
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, name, email, role FROM \"user\"")
    users = cur.fetchall()

    for user in users:
        query = """
        MERGE (u:User {id: $id})
        ON CREATE SET u.name = $name, u.email = $email, u.role = $role
        ON MATCH SET u.name = $name, u.email = $email, u.role = $role
        """
        neo4j_conn.run_query(query, {"id": user[0], "name": user[1], "email": user[2], "role": user[3]})

    cur.close()
    conn.close()
    print("Users synced to Neo4j.")

def sync_books_to_neo4j():
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, title, author, year_published, genre FROM book")
    books = cur.fetchall()

    for book in books:
        query = """
        MERGE (b:Book {id: $id})
        ON CREATE SET b.title = $title, b.author = $author, b.year_published = $year_published, b.genre = $genre
        ON MATCH SET b.title = $title, b.author = $author, b.year_published = $year_published, b.genre = $genre
        """
        neo4j_conn.run_query(query, {"id": book[0], "title": book[1], "author": book[2], "year_published": book[3], "genre": book[4]})

    cur.close()
    conn.close()
    print("Books with genres synced to Neo4j.")

def sync_borrowed_to_neo4j():
    conn = get_pg_connection()
    cur = conn.cursor()

    # Select borrowed records
    cur.execute("""
        SELECT DISTINCT ON (user_id, book_id) id, user_id, book_id, borrowed_date, due_date, returned_date
        FROM borrowed
        ORDER BY user_id, book_id, borrowed_date DESC
    """)
    borrowed_records = cur.fetchall()

    for record in borrowed_records:
        query = """
        MATCH (u:User {id: $user_id}), (b:Book {id: $book_id})
        MERGE (u)-[r:BORROWED {id: $id}]->(b)
        ON CREATE SET r.borrowed_date = $borrowed_date, r.due_date = $due_date, r.returned_date = $returned_date
        ON MATCH SET r.borrowed_date = $borrowed_date, r.due_date = $due_date, r.returned_date = $returned_date
        """
        try:
            # Run the query
            neo4j_conn.run_query(query, {
                "id": record[0],
                "user_id": record[1],
                "book_id": record[2],
                "borrowed_date": record[3],
                "due_date": record[4],
                "returned_date": record[5]
            })
        except Exception as e:
            print(f"Error syncing record {record[0]}: {e}")

    cur.close()
    conn.close()
    print("Borrowed records synced to Neo4j.") 

# Sync inventory
def sync_inventory_to_neo4j():
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, book_id, quantity FROM inventory")
    inventory_records = cur.fetchall()

    for record in inventory_records:
        query = """
        MATCH (b:Book {id: $book_id})
        MERGE (inv:Inventory {id: $id})
        ON CREATE SET inv.quantity = $quantity
        ON MATCH SET inv.quantity = $quantity
        MERGE (b)-[:HAS_INVENTORY]->(inv)
        """
        neo4j_conn.run_query(query, {"id": record[0], "book_id": record[1], "quantity": record[2]})

    cur.close()
    conn.close()
    print("Inventory synced to Neo4j.")

def sync_genres_and_relationships():
    conn = get_pg_connection()
    cur = conn.cursor()
    
    # Fetch books with genres
    cur.execute("SELECT id, genre FROM book WHERE genre IS NOT NULL")
    books_with_genres = cur.fetchall()
    
    for book in books_with_genres:
        book_id, genre = book

        # Create Genre node and relationship
        query = """
        MERGE (g:Genre {name: $genre})
        WITH g
        MATCH (b:Book {id: $book_id})
        MERGE (b)-[:BELONGS_TO]->(g)
        """
        neo4j_conn.run_query(query, {"genre": genre, "book_id": book_id})
    
    cur.close()
    conn.close()
    print("Genres and relationships synced to Neo4j.")

def create_similar_relationships():
    query = """
    MATCH (b1:Book), (b2:Book)
    WHERE b1.genre = b2.genre AND b1.id <> b2.id
    MERGE (b1)-[:SIMILAR_TO]->(b2)
    """
    neo4j_conn.run_query(query)
    print("SIMILAR_TO relationships created based on genre.")

# Sync all data
def sync_all():
    #sync_users_to_neo4j()
    #sync_books_to_neo4j()
    #delete_all_borrowed_from_neo4j()
    sync_borrowed_to_neo4j()
   
    #sync_inventory_to_neo4j()
    create_similar_relationships()
    sync_genres_and_relationships()

# Entry point
if __name__ == "__main__":
    print("Starting full sync...")
    sync_all()
    print("Sync complete.")