import os
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

class DatabaseManager:
    """Handles all database operations and connections"""
    
    def __init__(self):
        self.connection = None
        self._load_config()
    
    def _load_config(self):
        """Load database configuration from environment variables"""
        load_dotenv()
        
        required_vars = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
        for var in required_vars:
            if not os.getenv(var):
                raise ValueError(f"Missing required environment variable: {var}")
        
        self.config = {
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT'),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD')
        }
    
    def connect(self):
        """Establish connection to PostgreSQL database"""
        try:
            self.connection = psycopg2.connect(**self.config)
            self.connection.autocommit = True
            logging.info("Successfully connected to PostgreSQL database")
            return True
        except Exception as e:
            logging.error(f"Error connecting to database: {e}")
            raise
    
    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logging.info("Database connection closed")
    
    def execute_query(self, query, params=None, fetch=False):
        """Execute a database query"""
        try:
            cursor = self.connection.cursor(cursor_factory=RealDictCursor if fetch else None)
            cursor.execute(query, params)
            
            if fetch:
                result = cursor.fetchall()
                cursor.close()
                return result
            else:
                result = cursor.fetchone() if cursor.description else None
                cursor.close()
                return result
                
        except Exception as e:
            logging.error(f"Error executing query: {e}")
            raise
    
    def init_database(self):
        """Initialize database tables if they don't exist"""
        try:
            # Create users table
            self.execute_query("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id SERIAL PRIMARY KEY,
                    username VARCHAR(50) UNIQUE NOT NULL,
                    email VARCHAR(100) UNIQUE NOT NULL,
                    full_name VARCHAR(100),
                    bio TEXT,
                    followers_count INTEGER DEFAULT 0,
                    following_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create tweets table
            self.execute_query("""
                CREATE TABLE IF NOT EXISTS tweets (
                    tweet_id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    content TEXT NOT NULL,
                    hashtags TEXT[],
                    mentions TEXT[],
                    likes_count INTEGER DEFAULT 0,
                    retweets_count INTEGER DEFAULT 0,
                    reply_to_tweet_id INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                    FOREIGN KEY (reply_to_tweet_id) REFERENCES tweets(tweet_id) ON DELETE SET NULL
                )
            """)
            
            # Create follows table
            self.execute_query("""
                CREATE TABLE IF NOT EXISTS follows (
                    follow_id SERIAL PRIMARY KEY,
                    follower_id INTEGER NOT NULL,
                    following_id INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (follower_id) REFERENCES users(user_id) ON DELETE CASCADE,
                    FOREIGN KEY (following_id) REFERENCES users(user_id) ON DELETE CASCADE,
                    UNIQUE(follower_id, following_id),
                    CHECK (follower_id != following_id)
                )
            """)
            
            logging.info("Database tables initialized successfully")
            
        except Exception as e:
            logging.error(f"Error initializing database: {e}")
            raise
    
    def get_user_ids(self, limit=100):
        """Get existing user IDs from database"""
        try:
            result = self.execute_query(
                "SELECT user_id FROM users LIMIT %s", 
                (limit,), 
                fetch=True
            )
            return [user['user_id'] for user in result] if result else []
        except Exception as e:
            logging.error(f"Error getting user IDs: {e}")
            return []
    
    def get_random_tweet_id(self):
        """Get a random tweet ID for replies"""
        try:
            result = self.execute_query(
                "SELECT tweet_id FROM tweets ORDER BY RANDOM() LIMIT 1",
                fetch=True
            )
            return result[0]['tweet_id'] if result else None
        except Exception as e:
            logging.error(f"Error getting random tweet ID: {e}")
            return None
    
    def check_follow_exists(self, follower_id, following_id):
        """Check if follow relationship already exists"""
        try:
            result = self.execute_query(
                "SELECT 1 FROM follows WHERE follower_id = %s AND following_id = %s",
                (follower_id, following_id),
                fetch=True
            )
            return len(result) > 0 if result else False
        except Exception as e:
            logging.error(f"Error checking follow relationship: {e}")
            return True  # Assume exists to prevent duplicates on error