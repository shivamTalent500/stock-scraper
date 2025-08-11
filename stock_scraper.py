import requests
import pandas as pd
import json
import time
from datetime import datetime
import logging
from typing import List, Dict, Optional
import os

class YahooFinanceStockScraper:
    def __init__(self, delay: float = 0.5):
        """
        Yahoo Finance scraper for Indian stocks
        
        Args:
            delay: Delay between requests
        """
        self.delay = delay
        self.session = requests.Session()
        
        # Headers to mimic browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://finance.yahoo.com/'
        })
        
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('yahoo_stock_scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def get_nse_stock_list(self) -> List[str]:
        """Get list of NSE stock symbols"""
        # Common NSE stocks - you can expand this list
        nse_symbols = [
            # Nifty 50 stocks
            'RELIANCE.NS', 'TCS.NS', 'HDFCBANK.NS', 'BHARTIARTL.NS', 'ICICIBANK.NS',
            'INFOSYS.NS', 'HINDUNILVR.NS', 'ITC.NS', 'SBIN.NS', 'BAJFINANCE.NS',
            'LT.NS', 'HCLTECH.NS', 'ASIANPAINT.NS', 'MARUTI.NS', 'BAJAJFINSV.NS',
            'WIPRO.NS', 'NESTLEIND.NS', 'ULTRACEMCO.NS', 'TITAN.NS', 'AXISBANK.NS',
            'DMART.NS', 'KOTAKBANK.NS', 'SUNPHARMA.NS', 'ONGC.NS', 'NTPC.NS',
            'POWERGRID.NS', 'M&M.NS', 'TECHM.NS', 'TATAMOTORS.NS', 'JSWSTEEL.NS',
            'HINDALCO.NS', 'INDUSINDBK.NS', 'ADANIENT.NS', 'COALINDIA.NS', 'DRREDDY.NS',
            'GRASIM.NS', 'CIPLA.NS', 'BRITANNIA.NS', 'EICHERMOT.NS', 'APOLLOHOSP.NS',
            'BPCL.NS', 'DIVISLAB.NS', 'TATASTEEL.NS', 'HEROMOTOCO.NS', 'BAJAJ-AUTO.NS',
            'HDFCLIFE.NS', 'SBILIFE.NS', 'TRENT.NS', 'ADANIPORTS.NS', 'LTIM.NS',
            
            # Additional popular stocks
            'ADANIGREEN.NS', 'ADANIPOWER.NS', 'ADANITRANS.NS', 'AMBUJACEM.NS',
            'BANKBARODA.NS', 'BERGEPAINT.NS', 'BOSCHLTD.NS', 'CANBK.NS',
            'CHOLAFIN.NS', 'COLPAL.NS', 'CONCOR.NS', 'CUMMINSIND.NS',
            'DABUR.NS', 'FEDERALBNK.NS', 'GAIL.NS', 'GODREJCP.NS',
            'HAVELLS.NS', 'HDFC.NS', 'ICICIGI.NS', 'IDFCFIRSTB.NS',
            'IOC.NS', 'JINDALSTEL.NS', 'JUBLFOOD.NS', 'LUPIN.NS',
            'MARICO.NS', 'MCDOWELL-N.NS', 'MFSL.NS', 'MGL.NS',
            'MPHASIS.NS', 'MRF.NS', 'NAUKRI.NS', 'NMDC.NS',
            'PAGEIND.NS', 'PETRONET.NS', 'PIDILITIND.NS', 'PNB.NS',
            'POLYCAB.NS', 'PVR.NS', 'RAMCOCEM.NS', 'RECLTD.NS',
            'SAIL.NS', 'SHREECEM.NS', 'SIEMENS.NS', 'TORNTPHARM.NS',
            'UBL.NS', 'VEDL.NS', 'VOLTAS.NS', 'ZEEL.NS'
        ]
        
        return nse_symbols
    
    def get_stock_data(self, symbol: str) -> Optional[Dict]:
        """
        Get stock data for a single symbol from Yahoo Finance
        
        Args:
            symbol: Stock symbol (e.g., 'RELIANCE.NS')
            
        Returns:
            Dictionary with stock data or None
        """
        try:
            # Yahoo Finance API endpoint
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
            
            params = {
                'interval': '1d',
                'range': '1d',
                'includePrePost': 'false',
                'events': 'div,splits'
            }
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if 'chart' not in data or not data['chart']['result']:
                return None
            
            result = data['chart']['result'][0]
            meta = result.get('meta', {})
            
            # Extract current price data
            current_price = meta.get('regularMarketPrice')
            previous_close = meta.get('previousClose')
            
            if current_price is None:
                return None
            
            # Calculate change
            change = current_price - previous_close if previous_close else 0
            change_percent = (change / previous_close * 100) if previous_close else 0
            
            stock_data = {
                'symbol': symbol,
                'name': meta.get('longName', symbol.replace('.NS', '')),
                'price': round(current_price, 2),
                'change': round(change, 2),
                'change_percent': round(change_percent, 2),
                'volume': meta.get('regularMarketVolume'),
                'market_cap': meta.get('marketCap'),
                'previous_close': previous_close,
                'day_high': meta.get('regularMarketDayHigh'),
                'day_low': meta.get('regularMarketDayLow'),
                'open': meta.get('regularMarketOpen'),
                'fifty_two_week_high': meta.get('fiftyTwoWeekHigh'),
                'fifty_two_week_low': meta.get('fiftyTwoWeekLow'),
                'currency': meta.get('currency', 'INR'),
                'exchange': meta.get('exchangeName', 'NSE'),
                'market_state': meta.get('marketState'),
                'scraped_at': datetime.now().isoformat()
            }
            
            return stock_data
            
        except Exception as e:
            self.logger.error(f"Error fetching data for {symbol}: {str(e)}")
            return None
    
    def scrape_multiple_stocks(self, symbols: List[str]) -> pd.DataFrame:
        """
        Scrape data for multiple stocks
        
        Args:
            symbols: List of stock symbols
            
        Returns:
            DataFrame with all stock data
        """
        all_stocks = []
        total_symbols = len(symbols)
        
        self.logger.info(f"Starting to scrape {total_symbols} stocks...")
        
        for i, symbol in enumerate(symbols, 1):
            self.logger.info(f"Scraping {i}/{total_symbols}: {symbol}")
            
            stock_data = self.get_stock_data(symbol)
            if stock_data:
                all_stocks.append(stock_data)
                self.logger.info(f"✅ {symbol}: ₹{stock_data['price']} ({stock_data['change']:+.2f})")
            else:
                self.logger.warning(f"❌ Failed to get data for {symbol}")
            
            # Add delay between requests
            if i < total_symbols:
                time.sleep(self.delay)
        
        self.logger.info(f"Completed! Successfully scraped {len(all_stocks)} out of {total_symbols} stocks")
        
        return pd.DataFrame(all_stocks) if all_stocks else pd.DataFrame()
    
    def get_market_movers(self) -> Dict:
        """Get top gainers and losers"""
        try:
            # Yahoo Finance screener endpoints
            gainers_url = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?formatted=true&lang=en-US&region=US&scrIds=day_gainers_in&count=25"
            losers_url = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?formatted=true&lang=en-US&region=US&scrIds=day_losers_in&count=25"
            
            movers = {'gainers': [], 'losers': []}
            
            for category, url in [('gainers', gainers_url), ('losers', losers_url)]:
                try:
                    response = self.session.get(url, timeout=10)
                    if response.status_code == 200:
                        data = response.json()
                        if 'finance' in data and 'result' in data['finance']:
                            results = data['finance']['result'][0]['quotes']
                            movers[category] = results[:10]  # Top 10
                except:
                    pass
            
            return movers
        except:
            return {'gainers': [], 'losers': []}
    
    def save_data(self, df: pd.DataFrame, filename: Optional[str] = None) -> str:
        """Save data to various formats"""
        if filename is None:
            filename = f"yahoo_stocks_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        os.makedirs('data', exist_ok=True)
        
        # Save as CSV
        csv_path = f"data/{filename}.csv"
        df.to_csv(csv_path, index=False)
        
        # Save as JSON  
        json_path = f"data/{filename}.json"
        df.to_json(json_path, orient='records', indent=2)
        
        self.logger.info(f"Data saved to {csv_path}")
        return csv_path

def main():
    """Main function"""
    scraper = YahooFinanceStockScraper(delay=0.5)
    
    try:
        # Get stock symbols
        symbols = scraper.get_nse_stock_list()
        print(f"📊 Scraping {len(symbols)} Indian stocks from Yahoo Finance...")
        
        # Scrape stock data
        stock_data = scraper.scrape_multiple_stocks(symbols)
        
        if not stock_data.empty:
            # Save data
            csv_file = scraper.save_data(stock_data)
            
            # Print summary
            print(f"\n✅ Successfully scraped {len(stock_data)} stocks!")
            print(f"💾 Data saved to: {csv_file}")
            
            # Show top gainers and losers
            if len(stock_data) > 0:
                print(f"\n📈 Top 5 Gainers:")
                top_gainers = stock_data.nlargest(5, 'change_percent')[['symbol', 'name', 'price', 'change_percent']]
                for _, stock in top_gainers.iterrows():
                    print(f"   {stock['symbol']}: {stock['change_percent']:+.2f}%")
                
                print(f"\n📉 Top 5 Losers:")
                top_losers = stock_data.nsmallest(5, 'change_percent')[['symbol', 'name', 'price', 'change_percent']]
                for _, stock in top_losers.iterrows():
                    print(f"   {stock['symbol']}: {stock['change_percent']:+.2f}%")
                
                print(f"\n📊 Market Summary:")
                print(f"   Average Change: {stock_data['change_percent'].mean():+.2f}%")
                print(f"   Gainers: {len(stock_data[stock_data['change_percent'] > 0])}")
                print(f"   Losers: {len(stock_data[stock_data['change_percent'] < 0])}")
        
        else:
            print("❌ No stock data retrieved")
            
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
