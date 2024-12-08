from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
from werkzeug.security import check_password_hash
from flask_bcrypt import Bcrypt
from sync import sync_users_to_neo4j, sync_books_to_neo4j, sync_borrowed_to_neo4j, sync_inventory_to_neo4j
from neo4j import GraphDatabase
import os
# Initialize Flask app
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "http://localhost:4200"}})
bcrypt = Bcrypt(app)

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

class Neo4jConnection:
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def run_query(self, query, params=None):
        with self._driver.session() as session:
            result = session.run(query, params)
            # Consume the result into a list or process it directly
            return [record for record in result]

# Singleton for Neo4j connection
neo4j_conn = Neo4jConnection(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)

# Establish a connection to PostgreSQL
def get_db_connection():
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
        print("Database connection failed:", str(e))
        return None

@app.route('/login', methods=['POST'])
def login():
    try:
        data = request.get_json()
        email = data.get('email')
        password = data.get('password')

        if not email or not password:
            return jsonify({"error": "Email and password are required"}), 400

        # Fetch the user from the database
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT id, email, password_hash, role FROM \"user\" WHERE email = %s", (email,))
        user = cur.fetchone()
        cur.close()
        conn.close()

        if not user:
            return jsonify({"error": "User not found"}), 404

        user_id, user_email, password_hash, role = user

        # Verify the password using bcrypt
        if not bcrypt.check_password_hash(password_hash, password):
            return jsonify({"error": "Invalid password"}), 401

        # Successful login
        return jsonify({
            "message": "Login successful",
            "user_id": user_id,
            "role": role
        }), 200

    except Exception as e:
        print(f"Error during login: {e}")
        return jsonify({"error": str(e)}), 500

# API: Get Inventory
@app.route('/inventory', methods=['GET'])
def get_inventory():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT 
                inventory.book_id, 
                book.title, 
                book.author, 
                inventory.quantity 
            FROM inventory
            JOIN book ON inventory.book_id = book.id
        """)
        inventory = cur.fetchall()
        cur.close()
        conn.close()

        inventory_list = []
        for item in inventory:
            inventory_list.append({
                "book_id": item[0],
                "title": item[1],
                "author": item[2],
                "quantity": item[3]
            })

        return jsonify({"inventory": inventory_list}), 200
    except Exception as e:
        print(f"Error fetching inventory: {e}")
        return jsonify({"error": str(e)}), 500

# API: Borrow Book
@app.route('/borrow', methods=['POST'])
def borrow_book():
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        book_id = data.get('book_id')
        due_date = data.get('due_date')

        if not user_id or not book_id or not due_date:
            return jsonify({"error": "User ID, Book ID, and Due Date are required"}), 400

        conn = get_db_connection()
        cur = conn.cursor()

        # Check if the user has already borrowed this book
        cur.execute("""
            SELECT id FROM borrowed 
            WHERE user_id = %s AND book_id = %s AND returned_date IS NULL
        """, (user_id, book_id))
        existing_borrow = cur.fetchone()
        if existing_borrow:
            cur.close()
            conn.close()
            return jsonify({"error": "You have already borrowed this book"}), 400

        # Check if the user has borrowed more than 4 books
        cur.execute("""
            SELECT COUNT(*) FROM borrowed 
            WHERE user_id = %s AND returned_date IS NULL
        """, (user_id,))
        borrow_count = cur.fetchone()[0]
        if borrow_count >= 4:
            cur.close()
            conn.close()
            return jsonify({"error": "You cannot borrow more than 4 books"}), 400

        # Check inventory for book availability
        cur.execute("SELECT quantity FROM inventory WHERE book_id = %s", (book_id,))
        inventory = cur.fetchone()
        if not inventory or inventory[0] <= 0:
            cur.close()
            conn.close()
            return jsonify({"error": "Book is not available in inventory"}), 400

        # Add borrow record
        cur.execute(
            """
            INSERT INTO borrowed (user_id, book_id, due_date)
            VALUES (%s, %s, %s) RETURNING id
            """,
            (user_id, book_id, due_date)
        )
        borrow_id = cur.fetchone()[0]

        # Update inventory
        cur.execute("UPDATE inventory SET quantity = quantity - 1 WHERE book_id = %s", (book_id,))

        conn.commit()
        cur.close()
        conn.close()
        # Sync borrowed records and inventory to Neo4j
        sync_borrowed_to_neo4j()
        sync_inventory_to_neo4j()
        return jsonify({"message": "Book borrowed successfully", "borrow_id": borrow_id}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# API: Get Borrowed Books for a User
@app.route('/borrowed/<int:user_id>', methods=['GET'])
def get_borrowed_books(user_id):
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Fetch borrowed books for the given user_id
        cur.execute("""
            SELECT 
                b.id AS book_id, 
                b.title, 
                b.author, 
                br.due_date 
            FROM borrowed br
            JOIN book b ON br.book_id = b.id
            WHERE br.user_id = %s AND br.returned_date IS NULL
        """, (user_id,))
        borrowed_books = cur.fetchall()

        cur.close()
        conn.close()

        borrowed_books_list = []
        for book in borrowed_books:
            borrowed_books_list.append({
                "book_id": book[0],
                "title": book[1],
                "author": book[2],
                "due_date": book[3]
            })

        return jsonify({"borrowed_books": borrowed_books_list}), 200
    except Exception as e:
        print(f"Error fetching borrowed books: {e}")
        return jsonify({"error": str(e)}), 500

# API: Return Book
@app.route('/return', methods=['POST'])
def return_book():
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        book_id = data.get('book_id')

        if not user_id or not book_id:
            return jsonify({"error": "User ID and Book ID are required"}), 400

        conn = get_db_connection()
        cur = conn.cursor()

        # Check if the user has borrowed the book
        cur.execute("""
            SELECT id FROM borrowed 
            WHERE user_id = %s AND book_id = %s AND returned_date IS NULL
        """, (user_id, book_id))
        borrowed_record = cur.fetchone()

        if not borrowed_record:
            return jsonify({"error": "No borrowed record found for this user and book"}), 404

        # Mark the book as returned
        cur.execute("""
            UPDATE borrowed 
            SET returned_date = CURRENT_TIMESTAMP 
            WHERE id = %s
        """, (borrowed_record[0],))

        # Update inventory
        cur.execute("""
            UPDATE inventory 
            SET quantity = quantity + 1 
            WHERE book_id = %s
        """, (book_id,))
        conn.commit()
        cur.close()
        conn.close()
        # Sync borrowed records and inventory to Neo4j
        sync_borrowed_to_neo4j()
        sync_inventory_to_neo4j()
        return jsonify({"message": "Book returned successfully"}), 200
    except Exception as e:
        print(f"Error returning book: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/reviews', methods=['POST'])
def add_review():
    try:
        data = request.get_json()
        book_id = data.get('book_id')
        user_id = data.get('user_id')
        rating = data.get('rating')
        review_text = data.get('review_text', '')

        if not book_id or not user_id or not rating:
            return jsonify({"error": "Book ID, User ID, and Rating are required"}), 400

        conn = get_db_connection()
        cur = conn.cursor()

        # Insert the review into the table
        cur.execute("""
            INSERT INTO review (book_id, user_id, rating, review_text)
            VALUES (%s, %s, %s, %s)
        """, (book_id, user_id, rating, review_text))

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({"message": "Review added successfully"}), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/reviews/<int:book_id>', methods=['GET'])
def get_reviews(book_id):
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Fetch reviews for the given book_id
        cur.execute("""
            SELECT r.id, r.rating, r.review_text, r.created_at, u.name AS user_name
            FROM review r
            JOIN "user" u ON r.user_id = u.id
            WHERE r.book_id = %s
        """, (book_id,))
        reviews = cur.fetchall()

        cur.close()
        conn.close()

        # Format the results as JSON
        review_list = [
            {"id": row[0], "rating": row[1], "review_text": row[2], "created_at": row[3], "user_name": row[4]}
            for row in reviews
        ]

        return jsonify({"reviews": review_list}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/books/<int:book_id>/rating', methods=['GET'])
def get_average_rating(book_id):
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Calculate average rating for the given book_id
        cur.execute("""
            SELECT AVG(rating) AS average_rating
            FROM review
            WHERE book_id = %s
        """, (book_id,))
        avg_rating = cur.fetchone()[0]

        cur.close()
        conn.close()

        return jsonify({"average_rating": avg_rating or 0}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/recommendations/<int:user_id>', methods=['GET'])
def get_recommendations_endpoint(user_id):
    """
    API endpoint to get book recommendations for a user based on their borrowing history.
    """
    try:
        query = """
        MATCH (u:User {id: $user_id})-[:BORROWED]->(b:Book)-[:BELONGS_TO]->(g:Genre)<-[:BELONGS_TO]-(rec:Book)
        WHERE NOT (u)-[:BORROWED]->(rec)
        RETURN DISTINCT rec.title AS title, rec.author AS author, rec.year_published AS year
        LIMIT 5
        """
        
        # Run the query in Neo4j
        recommendations = neo4j_conn.run_query(query, {"user_id": user_id})
        
        # Convert the results into a list of dictionaries
        result_list = [
            {"title": record["title"], "author": record["author"], "year": record["year"]}
            for record in recommendations
        ]
        
        # Return recommendations as JSON
        return jsonify({"recommendations": result_list}), 200

    except Exception as e:
        # Log the error and return a 500 response
        print(f"Error fetching recommendations: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)