import requests
import pandas as pd
import json
import time
from datetime import datetime
import logging
import os
from typing import List, Dict, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
import yfinance as yf
import io

class ComprehensiveStockScraper:
    def __init__(self, delay: float = 0.2, max_workers: int = 50):
        """
        Comprehensive stock scraper for ALL NSE and BSE stocks
        
        Args:
            delay: Delay between requests
            max_workers: Number of parallel workers
        """
        self.delay = delay
        self.max_workers = max_workers
        self.session = requests.Session()
        
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://www.nseindia.com/'
        })
        
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('comprehensive_stock_scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Initialize session with NSE
        self._initialize_session()
    
    def _initialize_session(self):
        """Initialize session by visiting NSE homepage"""
        try:
            self.logger.info("Initializing session...")
            self.session.get("https://www.nseindia.com", timeout=10)
            time.sleep(2)
            self.logger.info("Session initialized successfully")
        except Exception as e:
            self.logger.warning(f"Session initialization warning: {str(e)}")
    
    def get_all_nse_symbols(self) -> Set[str]:
        """Get ALL NSE stock symbols from multiple sources"""
        all_symbols = set()
        
        # Method 1: NSE Symbol List API
        try:
            self.logger.info("Fetching NSE symbols from official API...")
            url = "https://www.nseindia.com/api/equity-master"
            response = self.session.get(url, timeout=15)
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list):
                    symbols = [item.get('symbol') for item in data if item.get('symbol')]
                    all_symbols.update(symbols)
                    self.logger.info(f"Got {len(symbols)} symbols from NSE master list")
        except Exception as e:
            self.logger.warning(f"NSE master list error: {str(e)}")
        
        # Method 2: NSE Securities in F&O
        try:
            self.logger.info("Fetching F&O securities...")
            url = "https://www.nseindia.com/api/equity-stockIndices?index=SECURITIES%20IN%20F%26O"
            response = self.session.get(url, timeout=15)
            if response.status_code == 200:
                data = response.json()
                if 'data' in data:
                    symbols = [item.get('symbol') for item in data['data'] if item.get('symbol')]
                    all_symbols.update(symbols)
                    self.logger.info(f"Got {len(symbols)} F&O symbols")
        except Exception as e:
            self.logger.warning(f"F&O symbols error: {str(e)}")
        
        # Method 3: All NSE Indices
        indices = [
            'NIFTY%20500', 'NIFTY%20MIDCAP%20150', 'NIFTY%20SMALLCAP%20250',
            'NIFTY%20MICROCAP%20250', 'NIFTY%20LARGEMIDCAP%20250',
            'NIFTY%20MIDSMALLCAP%20400', 'NIFTY%20TOTAL%20MARKET'
        ]
        
        for index in indices:
            try:
                url = f"https://www.nseindia.com/api/equity-stockIndices?index={index}"
                response = self.session.get(url, timeout=15)
                if response.status_code == 200:
                    data = response.json()
                    if 'data' in data:
                        symbols = [item.get('symbol') for item in data['data'] if item.get('symbol')]
                        all_symbols.update(symbols)
                        self.logger.info(f"Got {len(symbols)} symbols from {index.replace('%20', ' ')}")
                time.sleep(1)  # Delay between index requests
            except Exception as e:
                self.logger.warning(f"Error fetching {index}: {str(e)}")
        
        # Method 4: Yahoo Finance screener for Indian stocks
        try:
            self.logger.info("Fetching symbols from Yahoo Finance screener...")
            yahoo_symbols = self._get_yahoo_indian_symbols()
            all_symbols.update(yahoo_symbols)
        except Exception as e:
            self.logger.warning(f"Yahoo screener error: {str(e)}")
        
        # Method 5: Additional comprehensive symbol list
        additional_symbols = self._get_additional_symbols()
        all_symbols.update(additional_symbols)
        
        # Clean symbols
        clean_symbols = {s for s in all_symbols if s and s.isalpha() and len(s) > 1}
        
        self.logger.info(f"Total unique symbols collected: {len(clean_symbols)}")
        return clean_symbols
    
    def _get_yahoo_indian_symbols(self) -> Set[str]:
        """Get Indian symbols from Yahoo Finance"""
        symbols = set()
        
        # Yahoo Finance doesn't have a direct API for all Indian stocks
        # But we can use their screener results
        try:
            # This would need Yahoo Finance screener data
            # For now, we'll add known symbols that aren't in NSE lists
            known_symbols = [
                # Banking
                'AUBANK', 'BANDHANBNK', 'CSBBANK', 'DCBBANK', 'EQUITASBNK', 
                'FEDERALBNK', 'IDFCFIRSTB', 'INDIANB', 'INDUSINDBK', 'JAMMUBANK',
                'KTKBANK', 'ORIENTBANK', 'PNB', 'RBLBANK', 'SOUTHBANK', 'UNIONBANK',
                
                # IT
                'COFORGE', 'CYIENT', 'KPITTECH', 'LTTS', 'MINDTREE', 'MPHASIS',
                'NIITLTD', 'OFSS', 'PERSISTENT', 'RAMPGREEN', 'ZENSAR',
                
                # Pharma
                'AUROPHARMA', 'BIOCON', 'CADILAHC', 'DRREDDY', 'GLENMARK',
                'GRANULES', 'LALPATHLAB', 'LUPIN', 'NATCOPHARMA', 'REDDY',
                
                # Auto
                'APOLLOTYRE', 'ASHOKLEY', 'BAJAJ-AUTO', 'BHARATFORG', 'BOSCHLTD',
                'EICHERMOT', 'ESCORTS', 'EXIDEIND', 'HEROMOTOCO', 'M&MFIN',
                'MAHINDCIE', 'MARUTI', 'MOTHERSUMI', 'MRF', 'TATAMOTORS', 'TVSMOTOR',
                
                # FMCG
                'BRITANNIA', 'COLPAL', 'DABUR', 'EMAMILTD', 'GODREJCP',
                'HINDUNILVR', 'ITC', 'MARICO', 'NESTLEIND', 'PGHH', 'TATACONSUM',
                'UBL', 'VBL',
                
                # Metals
                'ADANIENT', 'ALUMINIUM', 'COALINDIA', 'HINDALCO', 'HINDCOPPER',
                'HINDZINC', 'JSWSTEEL', 'JSPL', 'MOIL', 'NALCO', 'NMDC',
                'RATNAMANI', 'SAIL', 'TATASTEEL', 'VEDL', 'WELCORP', 'WELSPUNIND',
                
                # Energy
                'ADANIGREEN', 'ADANIPOWER', 'ADANITRANS', 'BPCL', 'GAIL', 'HINDPETRO',
                'IOC', 'NTPC', 'ONGC', 'PETRONET', 'POWERGRID', 'RELIANCE', 'TATAPOWER',
                
                # Cement
                'ACC', 'AMBUJACEM', 'JKCEMENT', 'RAMCOCEM', 'SHREECEM', 'ULTRACEMCO',
                
                # Telecom
                'BHARTIARTL', 'IDEA', 'INDUS', 'RCOM',
                
                # Realty
                'BRIGADE', 'DLF', 'GODREJPROP', 'IBREALEST', 'INDIABULLS', 'LODHA',
                'OBEROIRLTY', 'PHOENIXLTD', 'PRESTIGE', 'SOBHA',
                
                # Consumer Durables
                'BAJAJELECTR', 'BLUESTARCO', 'CROMPTON', 'HAVELLS', 'ORIENTELEC',
                'POLARIND', 'SYMPHONY', 'TITAN', 'VOLTAS', 'WHIRLPOOL'
            ]
            symbols.update(known_symbols)
            
        except Exception as e:
            self.logger.debug(f"Yahoo symbols error: {str(e)}")
        
        return symbols
    
    def _get_additional_symbols(self) -> Set[str]:
        """Get additional stock symbols from various sources"""
        symbols = set()
        
        # Add more comprehensive symbol lists
        # Small cap and micro cap stocks
        small_micro_caps = [
            '5PAISA', 'AAATECH', 'AAKASH', 'AARON', 'ABCAPITAL', 'ABFRL',
            'ABMINTLLTD', 'ABSLAMC', 'ACCELYA', 'ACE', 'ADANIENSOL', 'ADANITRANS',
            'ADFFOODS', 'ADORWELD', 'ADVENZYMES', 'AEGISCHEM', 'AFFLE', 'AGARIND',
            'AGRITECH', 'AHLEAST', 'AHLUCONT', 'AIAENG', 'AIRAN', 'AJANTPHARM',
            'AJMERA', 'AKZOINDIA', 'ALANKIT', 'ALBERTDAVD', 'ALCHEM', 'ALEMBICLTD',
            'ALKYLAMINE', 'ALLCARGO', 'ALLSEC', 'ALMONDZ', 'ALOKTEXT', 'ALPHAGREP',
            'AMARAJABAT', 'AMBER', 'AMBUJACEM', 'AMDIND', 'AMEYA', 'AMJLAND',
            'AMNPLST', 'AMRUTANJAN', 'ANANTRAJ', 'ANGELBRKG', 'ANIKINDS', 'ANKITMETAL',
            'ANTGRAPHIC', 'APCOTEXIND', 'APEX', 'APLAPOLLO', 'APOLLO', 'APOLLOHOSP',
            'APOLLOTYRE', 'ARCHIES', 'ARENTERP', 'ARIHANT', 'ARIHANTSUP', 'ARMANFIN',
            'AROMABUILD', 'ARTSON', 'ARVIND', 'ASAHIINDIA', 'ASALCBR', 'ASHAPURMIN',
            'ASHIANA', 'ASHOKA', 'ASHOKLEY', 'ASIANHOTNR', 'ASIANPAINT', 'ASTERDM',
            'ASTRAL', 'ASTRAZEN', 'ASTRON', 'ATUL', 'ATULLTD', 'AURUM', 'AUTOAXLES',
            'AUTOIND', 'AVANTIFEED', 'AVATAR', 'AVTNPL', 'AXISBANK', 'BAFNAPH',
            'BAGFILMS', 'BAJAJ-AUTO', 'BAJAJCON', 'BAJAJELECTR', 'BAJAJFINSV',
            'BAJAJHIND', 'BAJAJHLDNG', 'BAJFINANCE', 'BALAJITELE', 'BALAMINES',
            'BALKRIND', 'BALKRISHNA', 'BALMLAWRIE', 'BALRAMCHIN', 'BANCOINDIA',
            'BANG', 'BANKBARODA', 'BANKINDIA', 'BASF', 'BATAINDIA', 'BAYERCROP',
            'BBMB', 'BDL', 'BEPL', 'BERGEPAINT', 'BF', 'BFINVEST', 'BGRENERGY',
            'BHARATFORG', 'BHARATGEAR', 'BHARATRAS', 'BHARATWIRE', 'BHARTIARTL',
            'BHEL', 'BIMETAL', 'BINANIIND', 'BIOCON', 'BIRLACORPN', 'BLISSGVS',
            'BLUEBLENDS', 'BLUESTARCO', 'BOMDYEING', 'BOSCHLTD', 'BPCL', 'BPL',
            'BRIGADE', 'BRITANNIA', 'BRFL', 'BSE', 'BSOFT', 'BURNPUR', 'BUTTERFLY'
        ]
        symbols.update(small_micro_caps)
        
        return symbols
    
    def get_stock_data_yahoo(self, symbol: str) -> Optional[Dict]:
        """Get stock data from Yahoo Finance with enhanced error handling"""
        try:
            # Try both NSE and BSE suffixes
            for suffix in ['.NS', '.BO']:
                try:
                    yahoo_symbol = f"{symbol}{suffix}"
                    ticker = yf.Ticker(yahoo_symbol)
                    
                    # Get basic info first (faster)
                    info = ticker.fast_info
                    if not info:
                        continue
                    
                    # Get historical data for more details
                    hist = ticker.history(period="2d")  # Get 2 days for comparison
                    if hist.empty:
                        continue
                    
                    current_price = hist['Close'].iloc[-1]
                    if pd.isna(current_price) or current_price <= 0:
                        continue
                    
                    previous_close = hist['Close'].iloc[-2] if len(hist) > 1 else current_price
                    
                    change = current_price - previous_close
                    change_percent = (change / previous_close * 100) if previous_close else 0
                    
                    # Get additional info (may be slow/incomplete)
                    full_info = {}
                    try:
                        full_info = ticker.info
                    except:
                        pass
                    
                    return {
                        'symbol': symbol,
                        'yahoo_symbol': yahoo_symbol,
                        'name': full_info.get('longName', full_info.get('shortName', symbol)),
                        'price': round(float(current_price), 2),
                        'change': round(float(change), 2),
                        'change_percent': round(float(change_percent), 2),
                        'volume': int(hist['Volume'].iloc[-1]) if not hist['Volume'].empty else None,
                        'market_cap': full_info.get('marketCap'),
                        'previous_close': float(previous_close) if previous_close else None,
                        'day_high': float(hist['High'].iloc[-1]) if not hist['High'].empty else None,
                        'day_low': float(hist['Low'].iloc[-1]) if not hist['Low'].empty else None,
                        'open': float(hist['Open'].iloc[-1]) if not hist['Open'].empty else None,
                        'fifty_two_week_high': full_info.get('fiftyTwoWeekHigh'),
                        'fifty_two_week_low': full_info.get('fiftyTwoWeekLow'),
                        'pe_ratio': full_info.get('trailingPE'),
                        'dividend_yield': full_info.get('dividendYield'),
                        'sector': full_info.get('sector'),
                        'industry': full_info.get('industry'),
                        'exchange': 'NSE' if suffix == '.NS' else 'BSE',
                        'currency': 'INR',
                        'source': 'Yahoo Finance',
                        'scraped_at': datetime.now().isoformat()
                    }
                    
                except Exception:
                    continue
                    
        except Exception as e:
            self.logger.debug(f"Yahoo error for {symbol}: {str(e)}")
            
        return None
    
    def scrape_all_stocks_parallel(self, symbols: Set[str]) -> pd.DataFrame:
        """Scrape all stocks using parallel processing"""
        all_stocks = []
        symbols_list = list(symbols)
        total_symbols = len(symbols_list)
        
        self.logger.info(f"Starting comprehensive scraping of {total_symbols} stocks with {self.max_workers} workers...")
        
        # Process in batches to avoid overwhelming the system
        batch_size = 100
        batches = [symbols_list[i:i + batch_size] for i in range(0, len(symbols_list), batch_size)]
        
        for batch_num, batch in enumerate(batches, 1):
            self.logger.info(f"Processing batch {batch_num}/{len(batches)} ({len(batch)} stocks)")
            
            with ThreadPoolExecutor(max_workers=min(self.max_workers, len(batch))) as executor:
                # Submit all tasks in this batch
                future_to_symbol = {}
                for symbol in batch:
                    future = executor.submit(self.get_stock_data_yahoo, symbol)
                    future_to_symbol[future] = symbol
                
                # Collect results
                batch_stocks = []
                completed = 0
                for future in as_completed(future_to_symbol):
                    symbol = future_to_symbol[future]
                    completed += 1
                    
                    try:
                        stock_data = future.result()
                        if stock_data:
                            batch_stocks.append(stock_data)
                            if completed % 10 == 0 or stock_data['price'] > 1000:  # Log every 10th or high-value stocks
                                self.logger.info(f"✅ Batch {batch_num} - {completed}/{len(batch)}: {symbol} - ₹{stock_data['price']}")
                    except Exception as e:
                        if completed % 20 == 0:  # Log errors less frequently
                            self.logger.debug(f"❌ {symbol}: {str(e)}")
                
                all_stocks.extend(batch_stocks)
                self.logger.info(f"Batch {batch_num} completed: {len(batch_stocks)}/{len(batch)} stocks retrieved")
                
                # Small delay between batches to be respectful
                if batch_num < len(batches):
                    time.sleep(2)
        
        self.logger.info(f"All batches completed! Total: {len(all_stocks)}/{total_symbols} stocks retrieved")
        return pd.DataFrame(all_stocks) if all_stocks else pd.DataFrame()
    
    def save_comprehensive_data(self, df: pd.DataFrame, filename: Optional[str] = None) -> str:
        """Save comprehensive data with multiple analysis sheets"""
        if filename is None:
            filename = f"comprehensive_indian_stocks_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        os.makedirs('data', exist_ok=True)
        
        # Save main CSV
        csv_path = f"data/{filename}.csv"
        df.to_csv(csv_path, index=False)
        
        # Save JSON
        json_path = f"data/{filename}.json"
        df.to_json(json_path, orient='records', indent=2)
        
        # Create comprehensive Excel file
        try:
            excel_path = f"data/{filename}.xlsx"
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                # All stocks
                df.to_excel(writer, sheet_name='All Stocks', index=False)
                
                if len(df) > 0:
                    # Top gainers (50)
                    if 'change_percent' in df.columns:
                        gainers = df.nlargest(50, 'change_percent')
                        gainers.to_excel(writer, sheet_name='Top 50 Gainers', index=False)
                        
                        # Top losers (50)  
                        losers = df.nsmallest(50, 'change_percent')
                        losers.to_excel(writer, sheet_name='Top 50 Losers', index=False)
                    
                    # High volume stocks (50)
                    if 'volume' in df.columns:
                        high_volume = df.nlargest(50, 'volume')
                        high_volume.to_excel(writer, sheet_name='High Volume', index=False)
                    
                    # High priced stocks
                    if 'price' in df.columns:
                        high_price = df.nlargest(50, 'price')
                        high_price.to_excel(writer, sheet_name='High Price', index=False)
                    
                    # Sector-wise breakdown
                    if 'sector' in df.columns and df['sector'].notna().any():
                        sector_summary = df.groupby('sector').agg({
                            'symbol': 'count',
                            'price': 'mean',
                            'change_percent': 'mean',
                            'volume': 'sum'
                        }).round(2)
                        sector_summary.to_excel(writer, sheet_name='Sector Summary')
                    
                    # Exchange-wise breakdown
                    if 'exchange' in df.columns:
                        exchange_summary = df.groupby('exchange').agg({
                            'symbol': 'count',
                            'price': 'mean',
                            'change_percent': 'mean',
                            'volume': 'sum'
                        }).round(2)
                        exchange_summary.to_excel(writer, sheet_name='Exchange Summary')
                        
        except ImportError:
            self.logger.warning("openpyxl not installed. Excel file not created.")
        
        # Create detailed summary
        summary = {
            'scraping_info': {
                'total_stocks_found': len(df),
                'scraped_at': datetime.now().isoformat(),
                'scraper_version': 'Comprehensive v2.0'
            },
            'data_quality': {
                'stocks_with_price': int(len(df[df['price'].notna()])) if 'price' in df.columns else 0,
                'stocks_with_volume': int(len(df[df['volume'].notna()])) if 'volume' in df.columns else 0,
                'stocks_with_sector': int(len(df[df['sector'].notna()])) if 'sector' in df.columns else 0,
            },
            'market_overview': {},
            'top_performers': {},
            'exchanges': {}
        }
        
        if len(df) > 0 and 'change_percent' in df.columns:
            summary['market_overview'] = {
                'avg_change_percent': float(df['change_percent'].mean()),
                'median_change_percent': float(df['change_percent'].median()),
                'total_gainers': int(len(df[df['change_percent'] > 0])),
                'total_losers': int(len(df[df['change_percent'] < 0])),
                'total_unchanged': int(len(df[df['change_percent'] == 0])),
                'highest_gain': float(df['change_percent'].max()),
                'biggest_loss': float(df['change_percent'].min())
            }
            
            # Top performers
            if len(df) >= 10:
                top_gainer = df.loc[df['change_percent'].idxmax()]
                biggest_loser = df.loc[df['change_percent'].idxmin()]
                
                summary['top_performers'] = {
                    'biggest_gainer': {
                        'symbol': top_gainer['symbol'],
                        'change_percent': float(top_gainer['change_percent']),
                        'price': float(top_gainer['price'])
                    },
                    'biggest_loser': {
                        'symbol': biggest_loser['symbol'], 
                        'change_percent': float(biggest_loser['change_percent']),
                        'price': float(biggest_loser['price'])
                    }
                }
        
        # Exchange breakdown
        if 'exchange' in df.columns:
            exchange_counts = df['exchange'].value_counts().to_dict()
            summary['exchanges'] = {k: int(v) for k, v in exchange_counts.items()}
        
        # Save summary
        summary_path = f"data/{filename}_comprehensive_summary.json"
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        self.logger.info(f"Comprehensive data saved to {csv_path}")
        self.logger.info(f"Summary saved to {summary_path}")
        
        return csv_path

def main():
    """Main function for comprehensive stock scraping"""
    # Increase workers and reduce delay for faster processing
    scraper = ComprehensiveStockScraper(delay=0.1, max_workers=75)
    
    try:
        print("🚀 Starting COMPREHENSIVE Indian Stock Market Scraping...")
        print("📊 Target: 1000-2000+ stocks from NSE and BSE")
        print("⚡ Using high-performance parallel processing")
        
        # Get all available stock symbols
        print("\n🔍 Phase 1: Discovering all stock symbols...")
        all_symbols = scraper.get_all_nse_symbols()
        print(f"✅ Found {len(all_symbols)} unique stock symbols")
        
        if len(all_symbols) < 500:
            print("⚠️  Warning: Found fewer symbols than expected. Continuing anyway...")
        
        # Scrape all stocks
        print(f"\n📈 Phase 2: Scraping stock data...")
        print(f"⚡ Using {scraper.max_workers} parallel workers")
        
        start_time = time.time()
        stock_data = scraper.scrape_all_stocks_parallel(all_symbols)
        end_time = time.time()
        
        if not stock_data.empty:
            # Clean data
            stock_data = stock_data.dropna(subset=['price'])
            stock_data = stock_data[stock_data['price'] > 0]  # Remove invalid prices
            stock_data = stock_data.drop_duplicates(subset=['symbol'], keep='first')
            
            # Save comprehensive data
            csv_file = scraper.save_comprehensive_data(stock_data)
            
            # Success summary
            print(f"\n🎉 SUCCESS! COMPREHENSIVE SCRAPING COMPLETED")
            print(f"⏱️  Total Time: {(end_time - start_time)/60:.1f} minutes")
            print(f"📊 Stocks Scraped: {len(stock_data)}")
            print(f"💾 Data saved to: {csv_file}")
            
            # Detailed market analysis
            print(f"\n📈 COMPREHENSIVE MARKET ANALYSIS:")
            print(f"   🏢 Total Companies: {len(stock_data)}")
            
            if 'price' in stock_data.columns:
                print(f"   💰 Average Price: ₹{stock_data['price'].mean():.2f}")
                print(f"   📊 Price Range: ₹{stock_data['price'].min():.2f} - ₹{stock_data['price'].max():,.2f}")
            
            if 'change_percent' in stock_data.columns:
                avg_change = stock_data['change_percent'].mean()
                gainers = len(stock_data[stock_data['change_percent'] > 0])
                losers = len(stock_data[stock_data['change_percent'] < 0])
                unchanged = len(stock_data[stock_data['change_percent'] == 0])
                
                print(f"   📊 Market Sentiment: {avg_change:+.2f}% average change")
                print(f"   🟢 Gainers: {gainers} ({gainers/len(stock_data)*100:.1f}%)")
                print(f"   🔴 Losers: {losers} ({losers/len(stock_data)*100:.1f}%)")
                print(f"   ⚪ Unchanged: {unchanged}")
                
                # Top performers
                if len(stock_data) >= 10:
                    print(f"\n🚀 TOP 10 GAINERS:")
                    top_gainers = stock_data.nlargest(10, 'change_percent')
                    for i, (_, stock) in enumerate(top_gainers.iterrows(), 1):
                        print(f"   {i:2d}. {stock['symbol']:12s}: +{stock['change_percent']:6.2f}% (₹{stock['price']:8,.2f})")
                    
                    print(f"\n📉 TOP 10 LOSERS:")
                    top_losers = stock_data.nsmallest(10, 'change_percent')
                    for i, (_, stock) in enumerate(top_losers.iterrows(), 1):
                        print(f"   {i:2d}. {stock['symbol']:12s}: {stock['change_percent']:7.2f}% (₹{stock['price']:8,.2f})")
            
            # Exchange breakdown
            if 'exchange' in stock_data.columns:
                exchange_counts = stock_data['exchange'].value_counts()
                print(f"\n🏛️  EXCHANGE BREAKDOWN:")
                for exchange, count in exchange_counts.items():
                    print(f"   {exchange}: {count} stocks ({count/len(stock_data)*100:.1f}%)")
            
            # Data quality report
            print(f"\n📋 DATA QUALITY REPORT:")
            print(f"   ✅ Stocks with valid prices: {len(stock_data[stock_data['price'] > 0])}")
            if 'volume' in stock_data.columns:
                print(f"   📊 Stocks with volume data: {len(stock_data[stock_data['volume'].notna()])}")
            if 'sector' in stock_data.columns:
                print(f"   🏭 Stocks with sector info: {len(stock_data[stock_data['sector'].notna()])}")
            
        else:
            print("❌ No comprehensive stock data retrieved")
            print("🔧 Try running again or check your internet connection")
            return False
            
    except Exception as e:
        print(f"❌ Comprehensive scraping error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
