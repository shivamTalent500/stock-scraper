import requests
import pandas as pd
import json
import time
from datetime import datetime
import logging
import os
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import yfinance as yf

class MultiSourceStockScraper:
    def __init__(self, delay: float = 0.5):
        """
        Multi-source stock scraper combining Yahoo Finance, NSE, and other sources
        
        Args:
            delay: Delay between requests
        """
        self.delay = delay
        self.session = requests.Session()
        
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive'
        })
        
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('multi_stock_scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def get_comprehensive_stock_list(self) -> List[str]:
        """Get comprehensive list of Indian stock symbols"""
        
        # Comprehensive list of Indian stocks
        symbols = {
            # Nifty 50
            'RELIANCE', 'TCS', 'HDFCBANK', 'BHARTIARTL', 'ICICIBANK', 'INFOSYS', 
            'HINDUNILVR', 'ITC', 'SBIN', 'BAJFINANCE', 'LT', 'HCLTECH', 'ASIANPAINT',
            'MARUTI', 'BAJAJFINSV', 'WIPRO', 'NESTLEIND', 'ULTRACEMCO', 'TITAN',
            'AXISBANK', 'DMART', 'KOTAKBANK', 'SUNPHARMA', 'ONGC', 'NTPC', 
            'POWERGRID', 'M&M', 'TECHM', 'TATAMOTORS', 'JSWSTEEL', 'HINDALCO',
            'INDUSINDBK', 'ADANIENT', 'COALINDIA', 'DRREDDY', 'GRASIM', 'CIPLA',
            'BRITANNIA', 'EICHERMOT', 'APOLLOHOSP', 'BPCL', 'DIVISLAB', 'TATASTEEL',
            'HEROMOTOCO', 'BAJAJ-AUTO', 'HDFCLIFE', 'SBILIFE', 'TRENT', 'ADANIPORTS',
            'LTIM',
            
            # Nifty Next 50
            'ADANIGREEN', 'ADANIPOWER', 'ADANITRANS', 'AMBUJACEM', 'BANKBARODA',
            'BERGEPAINT', 'BOSCHLTD', 'CANBK', 'CHOLAFIN', 'COLPAL', 'CONCOR',
            'CUMMINSIND', 'DABUR', 'FEDERALBNK', 'GAIL', 'GODREJCP', 'HAVELLS',
            'HDFC', 'ICICIGI', 'IDFCFIRSTB', 'IOC', 'JINDALSTEL', 'JUBLFOOD',
            'LUPIN', 'MARICO', 'MCDOWELL-N', 'MFSL', 'MGL', 'MPHASIS', 'MRF',
            'NAUKRI', 'NMDC', 'PAGEIND', 'PETRONET', 'PIDILITIND', 'PNB',
            'POLYCAB', 'RAMCOCEM', 'RECLTD', 'SAIL', 'SHREECEM', 'SIEMENS',
            'TORNTPHARM', 'UBL', 'VEDL', 'VOLTAS', 'ZEEL',
            
            # Additional popular stocks
            'ACC', 'AUROPHARMA', 'BANDHANBNK', 'BATAINDIA', 'BEL', 'BIOCON',
            'CADILAHC', 'CEATLTD', 'CHAMBLFERT', 'DLF', 'ESCORTS', 'EXIDEIND',
            'GLENMARK', 'GMRINFRA', 'GRANULES', 'HATHWAY', 'IBULHSGFIN', 'IDEA',
            'INDHOTEL', 'INDIGO', 'INFRATEL', 'INFY', 'IRB', 'JETAIRWAYS',
            'JPASSOCIAT', 'JUSTDIAL', 'L&TFH', 'LICHSGFIN', 'MANAPPURAM',
            'MOTHERSUMI', 'NATIONALUM', 'NBCC', 'OFSS', 'ONGC', 'ORIENTBANK',
            'PCJEWELLER', 'PENINLAND', 'PFC', 'PHOENIXLTD', 'RBLBANK',
            'RELCAPITAL', 'RELINFRA', 'RPOWER', 'SRTRANSFIN', 'STAR',
            'SUZLON', 'SYNDIBANK', 'TECHM', 'TVSMOTOR', 'UJJIVAN', 'UNIONBANK',
            'UNITECH', 'VEDL', 'YESBANK', 'ZEEL'
        }
        
        # Convert to Yahoo Finance format
        yahoo_symbols = [f"{symbol}.NS" for symbol in symbols]
        return list(yahoo_symbols)
    
    def get_stock_data_yahoo(self, symbol: str) -> Optional[Dict]:
        """Get stock data from Yahoo Finance"""
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            hist = ticker.history(period="1d")
            
            if hist.empty:
                return None
            
            current_price = hist['Close'].iloc[-1]
            previous_close = info.get('previousClose', hist['Close'].iloc[-1])
            
            change = current_price - previous_close
            change_percent = (change / previous_close * 100) if previous_close else 0
            
            return {
                'symbol': symbol.replace('.NS', ''),
                'yahoo_symbol': symbol,
                'name': info.get('longName', info.get('shortName', symbol.replace('.NS', ''))),
                'price': round(float(current_price), 2),
                'change': round(float(change), 2),
                'change_percent': round(float(change_percent), 2),
                'volume': int(hist['Volume'].iloc[-1]) if not hist['Volume'].empty else None,
                'market_cap': info.get('marketCap'),
                'previous_close': float(previous_close) if previous_close else None,
                'day_high': float(hist['High'].iloc[-1]) if not hist['High'].empty else None,
                'day_low': float(hist['Low'].iloc[-1]) if not hist['Low'].empty else None,
                'open': float(hist['Open'].iloc[-1]) if not hist['Open'].empty else None,
                'fifty_two_week_high': info.get('fiftyTwoWeekHigh'),
                'fifty_two_week_low': info.get('fiftyTwoWeekLow'),
                'pe_ratio': info.get('trailingPE'),
                'dividend_yield': info.get('dividendYield'),
                'market_cap_formatted': self._format_market_cap(info.get('marketCap')),
                'sector': info.get('sector'),
                'industry': info.get('industry'),
                'currency': 'INR',
                'exchange': 'NSE',
                'source': 'Yahoo Finance',
                'scraped_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.debug(f"Yahoo Finance error for {symbol}: {str(e)}")
            return None
    
    def get_stock_data_alternative_api(self, symbol: str) -> Optional[Dict]:
        """Get stock data from alternative free APIs"""
        try:
            # Remove .NS suffix for API calls
            clean_symbol = symbol.replace('.NS', '')
            
            # Try Alpha Vantage free tier (you can get free API key)
            # url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={clean_symbol}.BSE&apikey=demo"
            
            # Try a simpler approach with financial modeling prep (free tier)
            url = f"https://financialmodelingprep.com/api/v3/quote/{clean_symbol}?apikey=demo"
            
            response = self.session.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data and len(data) > 0:
                    stock = data[0]
                    return {
                        'symbol': clean_symbol,
                        'name': stock.get('name', clean_symbol),
                        'price': stock.get('price'),
                        'change': stock.get('change'),
                        'change_percent': stock.get('changesPercentage'),
                        'volume': stock.get('volume'),
                        'market_cap': stock.get('marketCap'),
                        'day_high': stock.get('dayHigh'),
                        'day_low': stock.get('dayLow'),
                        'open': stock.get('open'),
                        'previous_close': stock.get('previousClose'),
                        'source': 'Alternative API',
                        'scraped_at': datetime.now().isoformat()
                    }
        except Exception as e:
            self.logger.debug(f"Alternative API error for {symbol}: {str(e)}")
            
        return None
    
    def scrape_stocks_parallel(self, symbols: List[str], max_workers: int = 10) -> pd.DataFrame:
        """Scrape stocks in parallel for faster processing"""
        all_stocks = []
        
        self.logger.info(f"Starting parallel scraping of {len(symbols)} stocks with {max_workers} workers...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_symbol = {}
            for symbol in symbols:
                future = executor.submit(self._get_single_stock_data, symbol)
                future_to_symbol[future] = symbol
            
            # Collect results
            completed = 0
            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                completed += 1
                
                try:
                    stock_data = future.result()
                    if stock_data:
                        all_stocks.append(stock_data)
                        self.logger.info(f"✅ {completed}/{len(symbols)}: {symbol} - ₹{stock_data['price']}")
                    else:
                        self.logger.warning(f"❌ {completed}/{len(symbols)}: {symbol} - No data")
                except Exception as e:
                    self.logger.error(f"❌ {completed}/{len(symbols)}: {symbol} - Error: {str(e)}")
        
        self.logger.info(f"Parallel scraping completed! {len(all_stocks)} stocks retrieved.")
        return pd.DataFrame(all_stocks) if all_stocks else pd.DataFrame()
    
    def _get_single_stock_data(self, symbol: str) -> Optional[Dict]:
        """Get data for a single stock from multiple sources"""
        
        # Try Yahoo Finance first (most reliable)
        stock_data = self.get_stock_data_yahoo(symbol)
        if stock_data:
            return stock_data
        
        # Try alternative API
        stock_data = self.get_stock_data_alternative_api(symbol)
        if stock_data:
            return stock_data
        
        # Add small delay to avoid overwhelming servers
        time.sleep(0.1)
        return None
    
    def _format_market_cap(self, market_cap) -> Optional[str]:
        """Format market cap in readable format"""
        if not market_cap:
            return None
        
        try:
            mc = float(market_cap)
            if mc >= 1e12:
                return f"₹{mc/1e12:.2f}T"
            elif mc >= 1e9:
                return f"₹{mc/1e9:.2f}B"
            elif mc >= 1e6:
                return f"₹{mc/1e6:.2f}M"
            else:
                return f"₹{mc:.0f}"
        except:
            return None
    
    def save_data(self, df: pd.DataFrame, filename: Optional[str] = None) -> str:
        """Save data with enhanced formatting"""
        if filename is None:
            filename = f"indian_stocks_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        os.makedirs('data', exist_ok=True)
        
        # Save as CSV
        csv_path = f"data/{filename}.csv"
        df.to_csv(csv_path, index=False)
        
        # Save as JSON
        json_path = f"data/{filename}.json"
        df.to_json(json_path, orient='records', indent=2)
        
        # Create Excel file with multiple sheets
        try:
            excel_path = f"data/{filename}.xlsx"
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                # All data
                df.to_excel(writer, sheet_name='All Stocks', index=False)
                
                # Top gainers
                if len(df) > 0 and 'change_percent' in df.columns:
                    gainers = df.nlargest(25, 'change_percent')
                    gainers.to_excel(writer, sheet_name='Top Gainers', index=False)
                    
                    # Top losers
                    losers = df.nsmallest(25, 'change_percent')
                    losers.to_excel(writer, sheet_name='Top Losers', index=False)
                    
                    # High volume stocks
                    if 'volume' in df.columns:
                        high_volume = df.nlargest(25, 'volume')
                        high_volume.to_excel(writer, sheet_name='High Volume', index=False)
        except ImportError:
            self.logger.warning("openpyxl not installed. Excel file not created.")
        
        # Create summary file
        summary = {
            'total_stocks': len(df),
            'successful_scrapes': len(df[df['price'].notna()]) if 'price' in df.columns else 0,
            'scraped_at': datetime.now().isoformat(),
            'columns': list(df.columns),
            'summary_stats': {}
        }
        
        if len(df) > 0 and 'change_percent' in df.columns:
            summary['summary_stats'] = {
                'avg_change_percent': float(df['change_percent'].mean()),
                'gainers_count': int(len(df[df['change_percent'] > 0])),
                'losers_count': int(len(df[df['change_percent'] < 0])),
                'highest_gainer': df.loc[df['change_percent'].idxmax()]['symbol'] if not df.empty else None,
                'biggest_loser': df.loc[df['change_percent'].idxmin()]['symbol'] if not df.empty else None
            }
        
        summary_path = f"data/{filename}_summary.json"
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        self.logger.info(f"Data saved to {csv_path}")
        return csv_path

def main():
    """Main function"""
    scraper = MultiSourceStockScraper(delay=0.1)
    
    try:
        print("📊 Starting comprehensive Indian stock market scraping...")
        print("🔄 Using multiple sources: Yahoo Finance, Alternative APIs")
        
        # Get stock symbols
        symbols = scraper.get_comprehensive_stock_list()
        print(f"📈 Target stocks: {len(symbols)}")
        
        # Scrape stocks (parallel processing for speed)
        stock_data = scraper.scrape_stocks_parallel(symbols, max_workers=20)
        
        if not stock_data.empty:
            # Clean and process data
            stock_data = stock_data.dropna(subset=['price'])  # Remove stocks without price data
            stock_data = stock_data.drop_duplicates(subset=['symbol'], keep='first')
            
            # Save data
            csv_file = scraper.save_data(stock_data)
            
            print(f"\n🎉 SUCCESS! Scraped {len(stock_data)} stocks")
            print(f"💾 Data saved to: {csv_file}")
            
            # Market summary
            if len(stock_data) > 0:
                print(f"\n📊 Market Snapshot:")
                print(f"   📈 Total Stocks: {len(stock_data)}")
                
                if 'change_percent' in stock_data.columns:
                    avg_change = stock_data['change_percent'].mean()
                    gainers = len(stock_data[stock_data['change_percent'] > 0])
                    losers = len(stock_data[stock_data['change_percent'] < 0])
                    
                    print(f"   📊 Average Change: {avg_change:+.2f}%")
                    print(f"   🟢 Gainers: {gainers} ({gainers/len(stock_data)*100:.1f}%)")
                    print(f"   🔴 Losers: {losers} ({losers/len(stock_data)*100:.1f}%)")
                
                # Top performers
                if 'change_percent' in stock_data.columns:
                    print(f"\n🚀 Top 5 Gainers:")
                    top_gainers = stock_data.nlargest(5, 'change_percent')
                    for _, stock in top_gainers.iterrows():
                        print(f"   {stock['symbol']}: +{stock['change_percent']:.2f}% (₹{stock['price']})")
                    
                    print(f"\n📉 Top 5 Losers:")
                    top_losers = stock_data.nsmallest(5, 'change_percent')
                    for _, stock in top_losers.iterrows():
                        print(f"   {stock['symbol']}: {stock['change_percent']:.2f}% (₹{stock['price']})")
        
        else:
            print("❌ No stock data retrieved. Check your internet connection and try again.")
            return False
            
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
