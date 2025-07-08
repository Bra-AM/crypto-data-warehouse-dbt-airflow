"""
Cryptocurrency Data Extraction using CoinGecko API
"""

import pandas as pd
import requests
from datetime import datetime, timedelta
import logging
import time
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CoinGeckoExtractor:
    """Professional cryptocurrency extractor using CoinGecko API"""
    
    def __init__(self):
        self.base_url = "https://api.coingecko.com/api/v3"
        self.crypto_ids = {
            'bitcoin': 'BTC',
            'ethereum': 'ETH',
            'cardano': 'ADA',
            'solana': 'SOL',
            'polygon': 'MATIC'
        }
        
        # Headers to avoid rate limiting
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Accept': 'application/json'
        }
    
    def test_api_connection(self):
        """Test if CoinGecko API is accessible"""
        try:
            url = f"{self.base_url}/ping"
            response = requests.get(url, headers=self.headers, timeout=10)
            
            if response.status_code == 200:
                logger.info("CoinGecko API connection successful")
                return True
            else:
                logger.error(f"API connection failed: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"API connection test failed: {str(e)}")
            return False
    
    def get_current_prices(self):
        """Get current prices for all cryptocurrencies"""
        try:
            crypto_list = ','.join(self.crypto_ids.keys())
            url = f"{self.base_url}/simple/price"
            
            params = {
                'ids': crypto_list,
                'vs_currencies': 'usd',
                'include_24hr_vol': 'true',
                'include_24hr_change': 'true',
                'include_market_cap': 'true'
            }
            
            response = requests.get(url, params=params, headers=self.headers, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Successfully fetched current prices for {len(data)} cryptocurrencies")
            
            return data
            
        except Exception as e:
            logger.error(f"Error fetching current prices: {str(e)}")
            return None
    
    def get_historical_data(self, coin_id, days=90):
        """Get historical price data for a cryptocurrency"""
        try:
            url = f"{self.base_url}/coins/{coin_id}/market_chart"
            
            params = {
                'vs_currency': 'usd',
                'days': days,
                'interval': 'daily'
            }
            
            logger.info(f"Fetching {days} days of historical data for {coin_id}")
            
            response = requests.get(url, params=params, headers=self.headers, timeout=20)
            response.raise_for_status()
            
            data = response.json()
            
            # Extract price and volume data
            prices = data.get('prices', [])
            volumes = data.get('total_volumes', [])
            market_caps = data.get('market_caps', [])
            
            if not prices:
                logger.warning(f"No price data returned for {coin_id}")
                return None
            
            # Convert to DataFrame
            df_data = []
            for i, (timestamp, price) in enumerate(prices):
                # Convert timestamp from milliseconds
                date = datetime.fromtimestamp(timestamp / 1000)
                
                # Get corresponding volume and market cap
                volume = volumes[i][1] if i < len(volumes) else 0
                market_cap = market_caps[i][1] if i < len(market_caps) else 0
                
                df_data.append({
                    'symbol': self.crypto_ids[coin_id],
                    'timestamp': date,
                    'price': price,
                    'volume': volume,
                    'market_cap': market_cap,
                    'extracted_at': datetime.now()
                })
            
            df = pd.DataFrame(df_data)
            logger.info(f"Successfully processed {len(df)} records for {coin_id}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching historical data for {coin_id}: {str(e)}")
            return None
    
    def extract_all_data(self, days=90):
        """Extract data for all cryptocurrencies"""
        logger.info("Starting complete cryptocurrency data extraction")
        
        # Test API connection first
        if not self.test_api_connection():
            logger.error("Cannot connect to CoinGecko API")
            return None
        
        all_data = []
        
        # Get current prices first (for verification)
        current_prices = self.get_current_prices()
        if current_prices:
            logger.info("Current prices retrieved successfully")
            for coin_id, price_data in current_prices.items():
                logger.info(f"{coin_id}: ${price_data.get('usd', 'N/A'):,.2f}")
        
        # Get historical data for each cryptocurrency
        for coin_id in self.crypto_ids:
            logger.info(f"Processing {coin_id}...")
            
            df = self.get_historical_data(coin_id, days)
            
            if df is not None and not df.empty:
                all_data.append(df)
                logger.info(f"✓ {coin_id} completed: {len(df)} records")
                
                # Show sample
                latest_price = df['price'].iloc[-1]
                logger.info(f"  Latest price: ${latest_price:,.2f}")
            else:
                logger.error(f"✗ {coin_id} failed")
            
            # Rate limiting - be respectful to the API
            time.sleep(1)
        
        if not all_data:
            logger.error("No data extracted for any cryptocurrency")
            return None
        
        # Combine all data
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df = combined_df.sort_values(['timestamp', 'symbol']).reset_index(drop=True)
        
        logger.info(f"Combined dataset: {len(combined_df)} total records")
        
        return combined_df
    
    def save_data(self, df, filename_prefix="crypto_data"):
        """Save data to CSV file"""
        if df is None or df.empty:
            logger.error("No data to save")
            return None
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{filename_prefix}_{timestamp}.csv"
        
        try:
            df.to_csv(filename, index=False)
            logger.info(f"Data saved to {filename}")
            return filename
        except Exception as e:
            logger.error(f"Error saving data: {str(e)}")
            return None
    
    def print_summary(self, df):
        """Print professional data summary"""
        if df is None or df.empty:
            print("No data to summarize")
            return
        
        print("\n" + "="*65)
        print("COINGECKO CRYPTOCURRENCY DATA EXTRACTION SUMMARY")
        print("="*65)
        print(f"Data Source: CoinGecko API (Free Tier)")
        print(f"Total Records: {len(df):,}")
        print(f"Date Range: {df['timestamp'].min().date()} to {df['timestamp'].max().date()}")
        print(f"Symbols: {', '.join(sorted(df['symbol'].unique()))}")
        print()
        
        # Symbol statistics
        print("Symbol Summary:")
        print("-" * 55)
        print(f"{'Symbol':<8} {'Records':<8} {'Latest Price':<15} {'Total Return':<12}")
        print("-" * 55)
        
        for symbol in sorted(df['symbol'].unique()):
            symbol_data = df[df['symbol'] == symbol].copy()
            latest_price = symbol_data['price'].iloc[-1]
            first_price = symbol_data['price'].iloc[0]
            total_return = ((latest_price / first_price) - 1) * 100
            
            print(f"{symbol:<8} {len(symbol_data):<8} ${latest_price:<14,.2f} {total_return:<11.2f}%")
        
        print("\nSample Data (Most Recent 5 Records):")
        print("-" * 65)
        sample = df.tail(5)[['symbol', 'timestamp', 'price', 'volume']].copy()
        sample['price'] = sample['price'].round(2)
        sample['volume'] = sample['volume'].astype(int)
        sample['timestamp'] = sample['timestamp'].dt.strftime('%Y-%m-%d')
        print(sample.to_string(index=False))
        print("="*65)

def main():
    """Main extraction function"""
    print("COINGECKO API CRYPTOCURRENCY EXTRACTION")
    print("="*45)
    
    extractor = CoinGeckoExtractor()
    
    try:
        # Extract data (30 days to start, faster testing)
        df = extractor.extract_all_data(days=30)
        
        if df is not None:
            # Save data
            filename = extractor.save_data(df)
            
            if filename:
                # Print summary
                extractor.print_summary(df)
                
                print(f"\nSUCCESS: Real cryptocurrency data extracted!")
                print(f"Output file: {filename}")
                print(f"API Source: CoinGecko (Free)")
                print(f"Ready for database loading!")
            else:
                print("FAILED: Could not save data")
        else:
            print("FAILED: No data extracted")
            
    except KeyboardInterrupt:
        logger.info("Extraction interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        print(f"ERROR: {str(e)}")

if __name__ == "__main__":
    main()
