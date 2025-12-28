import logging
import random
from faker import Faker
from database_manager import DatabaseManager

class DataGenerator:
    """Generates realistic Twitter-like data"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.fake = Faker()
        self.user_ids = []
    
    def load_or_create_users(self):
        """Load existing user IDs or create some initial users"""
        try:
            self.user_ids = self.db_manager.get_user_ids()
            
            if self.user_ids:
                logging.info(f"Loaded {len(self.user_ids)} existing users")
            else:
                # Create some initial users
                for _ in range(20):
                    user_id = self.create_user()
                    if user_id:
                        self.user_ids.append(user_id)
                logging.info(f"Created {len(self.user_ids)} initial users")
                
        except Exception as e:
            logging.error(f"Error loading/creating users: {e}")
    
    def create_user(self):
        """Create a new user and return user ID"""
        try:
            username = self.fake.user_name()
            email = self.fake.email()
            full_name = self.fake.name()
            bio = self.fake.text(max_nb_chars=160)
            followers_count = random.randint(0, 1000)
            following_count = random.randint(0, 500)
            
            result = self.db_manager.execute_query("""
                INSERT INTO users (username, email, full_name, bio, followers_count, following_count)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING user_id
            """, (username, email, full_name, bio, followers_count, following_count))
            
            user_id = result[0] if result else None
            if user_id:
                logging.info(f"Created new user: {username} (ID: {user_id})")
            
            return user_id
            
        except Exception as e:
            logging.error(f"Error creating user: {e}")
            return None
    
    def generate_tweet_data(self):
        """Generate data for a new tweet"""
        # Ensure we have users
        if not self.user_ids:
            self.load_or_create_users()
        
        # Sometimes create new users
        if random.random() < 0.1:  # 10% chance
            new_user_id = self.create_user()
            if new_user_id:
                self.user_ids.append(new_user_id)
        
        user_id = random.choice(self.user_ids)
        content = self.fake.text(max_nb_chars=280)
        
        # Generate hashtags
        hashtags = []
        if random.random() < 0.3:  # 30% chance of hashtags
            hashtags = [f"#{self.fake.word()}" for _ in range(random.randint(1, 3))]
        
        # Generate mentions
        mentions = []
        if random.random() < 0.2:  # 20% chance of mentions
            mention_users = random.sample(self.user_ids, min(2, len(self.user_ids)))
            mentions = [f"@user_{uid}" for uid in mention_users if uid != user_id]
        
        likes_count = random.randint(0, 1000)
        retweets_count = random.randint(0, 100)
        
        # Sometimes make it a reply (20% chance)
        reply_to_tweet_id = None
        if random.random() < 0.2:
            reply_to_tweet_id = self.db_manager.get_random_tweet_id()
        
        return {
            'user_id': user_id,
            'content': content,
            'hashtags': hashtags,
            'mentions': mentions,
            'likes_count': likes_count,
            'retweets_count': retweets_count,
            'reply_to_tweet_id': reply_to_tweet_id
        }
    
    def generate_follow_data(self):
        """Generate data for a new follow relationship"""
        # Ensure we have enough users
        if len(self.user_ids) < 2:
            self.load_or_create_users()
        
        # Sometimes create new users
        if random.random() < 0.05:  # 5% chance
            new_user_id = self.create_user()
            if new_user_id:
                self.user_ids.append(new_user_id)
        
        # Select two different users
        follower_id, following_id = random.sample(self.user_ids, 2)
        
        return {
            'follower_id': follower_id,
            'following_id': following_id
        }
    
    def insert_tweet(self):
        """Insert a new tweet into the database"""
        try:
            tweet_data = self.generate_tweet_data()
            
            result = self.db_manager.execute_query("""
                INSERT INTO tweets (user_id, content, hashtags, mentions, likes_count, retweets_count, reply_to_tweet_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING tweet_id
            """, (
                tweet_data['user_id'],
                tweet_data['content'],
                tweet_data['hashtags'],
                tweet_data['mentions'],
                tweet_data['likes_count'],
                tweet_data['retweets_count'],
                tweet_data['reply_to_tweet_id']
            ))
            
            tweet_id = result[0] if result else None
            if tweet_id:
                logging.info(f"Inserted tweet ID: {tweet_id} by user {tweet_data['user_id']}")
            
        except Exception as e:
            logging.error(f"Error inserting tweet: {e}")
    
    def insert_follow(self):
        """Insert a new follow relationship into the database"""
        try:
            follow_data = self.generate_follow_data()
            
            # Check if relationship already exists
            if self.db_manager.check_follow_exists(
                follow_data['follower_id'], 
                follow_data['following_id']
            ):
                logging.info(f"Follow relationship already exists between users {follow_data['follower_id']} and {follow_data['following_id']}")
                return
            
            result = self.db_manager.execute_query("""
                INSERT INTO follows (follower_id, following_id)
                VALUES (%s, %s)
                RETURNING follow_id
            """, (follow_data['follower_id'], follow_data['following_id']))
            
            follow_id = result[0] if result else None
            if follow_id:
                # Update follower/following counts
                self.db_manager.execute_query("""
                    UPDATE users SET following_count = following_count + 1
                    WHERE user_id = %s
                """, (follow_data['follower_id'],))
                
                self.db_manager.execute_query("""
                    UPDATE users SET followers_count = followers_count + 1
                    WHERE user_id = %s
                """, (follow_data['following_id'],))
                
                logging.info(f"Inserted follow relationship ID: {follow_id} (User {follow_data['follower_id']} follows User {follow_data['following_id']})")
            
        except Exception as e:
            logging.error(f"Error inserting follow relationship: {e}")
    
    def insert_random_data(self):
        """Insert random data - sometimes tweet, sometimes follow"""
        try:
            # 70% chance to insert tweet, 30% chance to insert follow
            if random.random() < 0.7:
                self.insert_tweet()
            else:
                self.insert_follow()
                
        except Exception as e:
            logging.error(f"Error in random data insert: {e}")