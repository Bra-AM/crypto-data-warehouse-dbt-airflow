"""
Database Loader for Cryptocurrency Data
Uses SQLite
"""

import pandas as pd
import sqlite3
from pathlib import Path
import logging
from datetime import datetime
import glob

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CryptoDatabaseLoader:
    """Professional database loader for cryptocurrency data"""
    
    def __init__(self, db_path="crypto_warehouse.db"):
        self.db_path = db_path
        self.connection = None
    
    def connect(self):
        """Create database connection"""
        try:
            self.connection = sqlite3.connect(self.db_path)
            logger.info(f"Connected to database: {self.db_path}")
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {str(e)}")
            return False
    
    def create_schema(self):
        """Create the raw data schema and tables"""
        if not self.connection:
            logger.error("No database connection")
            return False
        
        create_tables_sql = """
        -- Create raw data table
        DROP TABLE IF EXISTS raw_crypto_prices;
        
        CREATE TABLE raw_crypto_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timestamp DATETIME NOT NULL,
            price REAL NOT NULL,
            volume REAL,
            market_cap REAL,
            extracted_at DATETIME NOT NULL,
            loaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, timestamp)
        );
        
        -- Create indexes for performance
        CREATE INDEX idx_raw_symbol_timestamp ON raw_crypto_prices(symbol, timestamp);
        CREATE INDEX idx_raw_timestamp ON raw_crypto_prices(timestamp);
        CREATE INDEX idx_raw_symbol ON raw_crypto_prices(symbol);
        
        -- Create metadata table
        CREATE TABLE IF NOT EXISTS load_metadata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL,
            records_loaded INTEGER NOT NULL,
            load_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            data_source TEXT DEFAULT 'CoinGecko'
        );
        """
        
        try:
            cursor = self.connection.cursor()
            cursor.executescript(create_tables_sql)
            self.connection.commit()
            logger.info("Database schema created successfully")
            return True
        except Exception as e:
            logger.error(f"Schema creation failed: {str(e)}")
            return False
    
    def load_csv_file(self, csv_file_path):
        """Load cryptocurrency data from CSV file"""
        if not self.connection:
            logger.error("No database connection")
            return False
        
        try:
            # Read CSV file
            logger.info(f"Loading data from {csv_file_path}")
            df = pd.read_csv(csv_file_path)
            
            # Validate required columns
            required_columns = ['symbol', 'timestamp', 'price', 'volume', 'extracted_at']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                logger.error(f"Missing required columns: {missing_columns}")
                return False
            
            # Clean and prepare data
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['extracted_at'] = pd.to_datetime(df['extracted_at'])
            
            # Add market_cap if not present
            if 'market_cap' not in df.columns:
                df['market_cap'] = df['price'] * df['volume'] * 0.001  # Approximation
            
            # Remove duplicates
            initial_count = len(df)
            df = df.drop_duplicates(subset=['symbol', 'timestamp'])
            final_count = len(df)
            
            if initial_count != final_count:
                logger.info(f"Removed {initial_count - final_count} duplicate records")
            
            # Load to database
            df.to_sql('raw_crypto_prices', self.connection, if_exists='append', index=False)
            
            # Record metadata
            metadata = pd.DataFrame([{
                'filename': Path(csv_file_path).name,
                'records_loaded': final_count,
                'data_source': 'CoinGecko'
            }])
            metadata.to_sql('load_metadata', self.connection, if_exists='append', index=False)
            
            self.connection.commit()
            logger.info(f"Successfully loaded {final_count} records from {csv_file_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error loading CSV file: {str(e)}")
            return False
    
    def get_data_summary(self):
        """Get summary of loaded data"""
        if not self.connection:
            logger.error("No database connection")
            return None
        
        summary_queries = {
            'total_records': "SELECT COUNT(*) as count FROM raw_crypto_prices",
            'symbol_counts': """
                SELECT 
                    symbol,
                    COUNT(*) as records,
                    MIN(timestamp) as earliest_date,
                    MAX(timestamp) as latest_date,
                    ROUND(MIN(price), 2) as min_price,
                    ROUND(MAX(price), 2) as max_price,
                    ROUND(AVG(price), 2) as avg_price
                FROM raw_crypto_prices 
                GROUP BY symbol 
                ORDER BY symbol
            """,
            'load_history': """
                SELECT 
                    filename,
                    records_loaded,
                    data_source,
                    load_timestamp
                FROM load_metadata 
                ORDER BY load_timestamp DESC
            """
        }
        
        try:
            results = {}
            for key, query in summary_queries.items():
                df = pd.read_sql_query(query, self.connection)
                results[key] = df
            
            return results
            
        except Exception as e:
            logger.error(f"Error getting data summary: {str(e)}")
            return None
    
    def print_summary(self, summary):
        """Print professional summary of database contents"""
        if not summary:
            print("No summary data available")
            return
        
        print("\n" + "="*70)
        print("CRYPTOCURRENCY DATABASE SUMMARY")
        print("="*70)
        
        # Total records
        total = summary['total_records']['count'].iloc[0]
        print(f"Total Records in Database: {total:,}")
        
        # Symbol breakdown
        print("\nSymbol Statistics:")
        print("-" * 70)
        symbol_df = summary['symbol_counts']
        print(symbol_df.to_string(index=False))
        
        # Load history
        print(f"\nLoad History:")
        print("-" * 70)
        load_df = summary['load_history']
        print(load_df.to_string(index=False))
        
        print("="*70)
        print(f"Database Location: {self.db_path}")
        print("Ready for dbt transformations!")
        print("="*70)
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")

def find_latest_csv():
    """Find the most recent crypto data CSV file"""
    csv_files = glob.glob("crypto_data_*.csv")
    if not csv_files:
        # Check in extract directory
        csv_files = glob.glob("extract/crypto_data_*.csv")
    
    if not csv_files:
        logger.error("No crypto data CSV files found")
        return None
    
    # Return the most recent file
    latest_file = max(csv_files, key=lambda x: Path(x).stat().st_mtime)
    logger.info(f"Found latest CSV file: {latest_file}")
    return latest_file

def main():
    """Main database loading function"""
    print("CRYPTOCURRENCY DATABASE LOADER")
    print("="*35)
    
    # Find CSV file
    csv_file = find_latest_csv()
    if not csv_file:
        print("ERROR: No CSV file found to load")
        return
    
    # Initialize loader
    loader = CryptoDatabaseLoader()
    
    try:
        # Connect to database
        if not loader.connect():
            print("ERROR: Could not connect to database")
            return
        
        # Create schema
        if not loader.create_schema():
            print("ERROR: Could not create database schema")
            return
        
        # Load data
        if not loader.load_csv_file(csv_file):
            print("ERROR: Could not load CSV data")
            return
        
        # Get and display summary
        summary = loader.get_data_summary()
        if summary:
            loader.print_summary(summary)
            print(f"\nSUCCESS: Data loaded from {csv_file}")
        else:
            print("WARNING: Could not generate summary")
            
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        print(f"ERROR: {str(e)}")
    
    finally:
        loader.close()

if __name__ == "__main__":
    main()
