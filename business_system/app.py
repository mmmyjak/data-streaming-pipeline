import os
import logging
import signal
import sys
from apscheduler.schedulers.blocking import BlockingScheduler
from dotenv import load_dotenv
from database_manager import DatabaseManager
from data_generator import DataGenerator

class TwitterApp:
    """Main application class that orchestrates the Twitter data generation"""
    
    def __init__(self):
        self._setup_logging()
        self._load_config()
        self.db_manager = DatabaseManager()
        self.data_generator = DataGenerator(self.db_manager)
        self.scheduler = BlockingScheduler()
        self._setup_signal_handlers()
    
    def _setup_logging(self):
        """Configure application logging"""
        load_dotenv()
        log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
        
        logging.basicConfig(
            level=getattr(logging, log_level),
            format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def _load_config(self):
        """Load application configuration from environment variables"""
        self.insert_interval = int(os.getenv('INSERT_INTERVAL_SECONDS', '5'))
        self.logger.info(f"Configured to insert data every {self.insert_interval} seconds")
    
    def _setup_signal_handlers(self):
        """Set up signal handlers for graceful shutdown"""
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, sig, frame):
        """Handle interrupt signals"""
        self.logger.info("Interrupt signal received, shutting down gracefully...")
        self.shutdown()
        sys.exit(0)
    
    def initialize(self):
        """Initialize the application"""
        try:
            self.logger.info("Initializing Twitter Data Generator...")
            
            # Connect to database
            self.db_manager.connect()
            
            # Initialize database schema
            self.db_manager.init_database()
            
            # Load or create initial users
            self.data_generator.load_or_create_users()
            
            self.logger.info("Application initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error during initialization: {e}")
            return False
    
    def setup_scheduler(self):
        """Set up the periodic data insertion scheduler"""
        try:
            self.scheduler.add_job(
                func=self.data_generator.insert_random_data,
                trigger="interval",
                seconds=self.insert_interval,
                id='twitter_data_insert',
                name=f'Insert Twitter Data Every {self.insert_interval} Seconds',
                max_instances=1  # Prevent job overlap
            )
            
            self.logger.info(f"Scheduler configured to run every {self.insert_interval} seconds")
            
        except Exception as e:
            self.logger.error(f"Error setting up scheduler: {e}")
            raise
    
    def start(self):
        """Start the application"""
        try:
            if not self.initialize():
                self.logger.error("Failed to initialize application")
                return False
            
            self.setup_scheduler()
            
            self.logger.info("Starting Twitter Data Generator... Press Ctrl+C to exit")
            self.scheduler.start()
            
        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal, shutting down...")
            self.shutdown()
        except Exception as e:
            self.logger.error(f"Error starting application: {e}")
            self.shutdown()
            return False
    
    def shutdown(self):
        """Gracefully shutdown the application"""
        try:
            self.logger.info("Shutting down application...")
            
            if self.scheduler.running:
                self.scheduler.shutdown(wait=False)
                self.logger.info("Scheduler stopped")
            
            self.db_manager.disconnect()
            
            self.logger.info("Application shutdown complete")
            
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")

def main():
    """Main entry point"""
    app = TwitterApp()
    app.start()

if __name__ == "__main__":
    main()