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

class RobustStockScraper:
    def __init__(self, delay: float = 0.2, max_workers: int = 50):
        """
        Robust and reliable stock scraper focused on getting maximum results
        
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
            'Connection': 'keep-alive'
        })
        
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('robust_stock_scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def get_comprehensive_symbol_list(self) -> Set[str]:
        """Get comprehensive list of stock symbols using multiple reliable methods"""
        all_symbols = set()
        
        # Method 1: Curated list of known active stocks (most reliable)
        known_symbols = self.get_known_active_symbols()
        all_symbols.update(known_symbols)
        self.logger.info(f"Known active symbols: {len(known_symbols)}")
        
        # Method 2: Try NSE API (with error handling)
        try:
            nse_symbols = self.get_nse_api_symbols()
            all_symbols.update(nse_symbols)
            self.logger.info(f"NSE API symbols: {len(nse_symbols)}")
        except Exception as e:
            self.logger.warning(f"NSE API failed: {str(e)}")
        
        # Method 3: Generate additional symbols based on patterns
        pattern_symbols = self.generate_additional_symbols()
        all_symbols.update(pattern_symbols)
        self.logger.info(f"Pattern-based symbols: {len(pattern_symbols)}")
        
        # Clean symbols
        clean_symbols = {s.strip().upper() for s in all_symbols 
                        if s and isinstance(s, str) and s.strip().isalpha() 
                        and 2 <= len(s.strip()) <= 20}
        
        self.logger.info(f"Total clean symbols: {len(clean_symbols)}")
        return clean_symbols

    def get_nse_api_symbols(self) -> Set[str]:
        """Try to get symbols from NSE API with robust error handling"""
        symbols = set()
        
        # Simple NSE endpoints that are more likely to work
        endpoints = [
            "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050",
            "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20500",
            "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20MIDCAP%20100",
            "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20SMALLCAP%20100"
        ]
        
        # Initialize session
        try:
            self.session.get("https://www.nseindia.com", timeout=10)
            time.sleep(2)
        except:
            pass
        
        for endpoint in endpoints:
            try:
                response = self.session.get(endpoint, timeout=15)
                if response.status_code == 200:
                    data = response.json()
                    if 'data' in data:
                        for item in data['data']:
                            if 'symbol' in item and item['symbol']:
                                symbols.add(item['symbol'])
                time.sleep(1)
            except Exception as e:
                self.logger.debug(f"NSE endpoint failed: {str(e)}")
                continue
        
        return symbols

    def generate_additional_symbols(self) -> Set[str]:
        """Generate additional stock symbols based on common patterns"""
        symbols = set()
        
        # Add variations of known companies
        base_names = [
            'BHARTI', 'BHARTIARTL', 'AIRTEL', 'RELIANCE', 'RELIANCEIND', 'RIL',
            'TCS', 'TATA', 'TATAMOTORS', 'TATAPOWER', 'TATASTEEL', 'TATACOMM',
            'HDFC', 'HDFCBANK', 'HDFCLIFE', 'ICICI', 'ICICIBANK', 'ICICIGI',
            'INFOSYS', 'INFY', 'WIPRO', 'HCL', 'HCLTECH', 'TECHM', 'LTTS',
            'BAJAJ', 'BAJFINANCE', 'BAJAJFINSV', 'BAJAJAUTO', 'BAJAJELECTR',
            'ADANI', 'ADANIPORTS', 'ADANIGREEN', 'ADANIPOWER', 'ADANIENT',
            'ASIAN', 'ASIANPAINT', 'BERGER', 'BERGEPAINT', 'NEROLAC',
            'MARUTI', 'MARUTISUZUKI', 'HYUNDAI', 'MAHINDRA', 'TVSMOTOR',
            'HERO', 'HEROMOTOCO', 'EICHER', 'EICHERMOT', 'BAJAJ-AUTO',
            'SBI', 'SBIN', 'SBILIFE', 'SBICARDS', 'PNB', 'BOB', 'UNIONBANK',
            'AXIS', 'AXISBANK', 'KOTAK', 'KOTAKBANK', 'INDUSIND', 'INDUSINDBK',
            'SUNPHARMA', 'DRREDDY', 'CIPLA', 'LUPIN', 'BIOCON', 'AUROPHARMA',
            'ONGC', 'IOC', 'BPCL', 'HINDPETRO', 'GAIL', 'NTPC', 'POWERGRID'
        ]
        
        symbols.update(base_names)
        
        # Add sector-based symbols
        sectors = {
            'BANK': ['BANK', 'BANKBARODA', 'CANARABANK', 'INDIANBANK', 'CENTRALBANK'],
            'AUTO': ['AUTO', 'MOTORS', 'TYRES', 'PARTS'],
            'PHARMA': ['PHARMA', 'DRUGS', 'HEALTH', 'MEDICAL'],
            'IT': ['TECH', 'SOFTWARE', 'SYSTEMS', 'SOLUTIONS'],
            'ENERGY': ['POWER', 'ENERGY', 'OIL', 'GAS', 'PETRO'],
            'METAL': ['STEEL', 'ALUMINIUM', 'COPPER', 'ZINC', 'IRON']
        }
        
        for sector_symbols in sectors.values():
            symbols.update(sector_symbols)
        
        return symbols

    def get_known_active_symbols(self) -> Set[str]:
        """Comprehensive list of known actively trading stocks"""
        return {
            # NIFTY 50 (Most liquid)
            'RELIANCE', 'TCS', 'HDFCBANK', 'BHARTIARTL', 'ICICIBANK', 'INFOSYS',
            'HINDUNILVR', 'ITC', 'SBIN', 'BAJFINANCE', 'LT', 'HCLTECH', 'ASIANPAINT',
            'MARUTI', 'BAJAJFINSV', 'WIPRO', 'NESTLEIND', 'ULTRACEMCO', 'TITAN',
            'AXISBANK', 'DMART', 'KOTAKBANK', 'SUNPHARMA', 'ONGC', 'NTPC',
            'POWERGRID', 'M&M', 'TECHM', 'TATAMOTORS', 'JSWSTEEL', 'HINDALCO',
            'INDUSINDBK', 'ADANIENT', 'COALINDIA', 'DRREDDY', 'GRASIM', 'CIPLA',
            'BRITANNIA', 'EICHERMOT', 'APOLLOHOSP', 'BPCL', 'DIVISLAB', 'TATASTEEL',
            'HEROMOTOCO', 'BAJAJ-AUTO', 'HDFCLIFE', 'SBILIFE', 'TRENT', 'ADANIPORTS',
            'LTIM',

            # NIFTY NEXT 50
            'ADANIGREEN', 'ADANIPOWER', 'ADANITRANS', 'AMBUJACEM', 'BANKBARODA',
            'BERGEPAINT', 'BOSCHLTD', 'CANBK', 'CHOLAFIN', 'COLPAL', 'CONCOR',
            'CUMMINSIND', 'DABUR', 'FEDERALBNK', 'GAIL', 'GODREJCP', 'HAVELLS',
            'HDFC', 'ICICIGI', 'IDFCFIRSTB', 'IOC', 'JINDALSTEL', 'JUBLFOOD',
            'LUPIN', 'MARICO', 'MCDOWELL-N', 'MFSL', 'MGL', 'MPHASIS', 'MRF',
            'NAUKRI', 'NMDC', 'PAGEIND', 'PETRONET', 'PIDILITIND', 'PNB',
            'POLYCAB', 'RAMCOCEM', 'RECLTD', 'SAIL', 'SHREECEM', 'SIEMENS',
            'TORNTPHARM', 'UBL', 'VEDL', 'VOLTAS', 'ZEEL',

            # Major Banks & Financial Services
            'SBIN', 'HDFCBANK', 'ICICIBANK', 'AXISBANK', 'KOTAKBANK', 'INDUSINDBK',
            'BANKBARODA', 'PNB', 'CANBK', 'UNIONBANK', 'IDFCFIRSTB', 'FEDERALBNK',
            'RBLBANK', 'YESBANK', 'BANDHANBNK', 'EQUITAS', 'UJJIVANSFB', 'SURYODAY',
            'BAJFINANCE', 'BAJAJFINSV', 'CHOLAFIN', 'MFSL', 'L&TFH', 'LICHSGFIN',
            'MANAPPURAM', 'MUTHOOTFIN', 'AAVAS', 'CANFINHOME', 'HDFC', 'PFC', 'RECLTD',

            # IT & Technology
            'TCS', 'INFOSYS', 'WIPRO', 'HCLTECH', 'TECHM', 'LTTS', 'LTIM',
            'MINDTREE', 'MPHASIS', 'COFORGE', 'PERSISTENT', 'CYIENT', 'KPIT',
            'INTELLECT', 'OFSS', 'RAMCO', 'NEWGEN', 'SONATA', 'ZENSAR',
            'NIITLTD', 'ROLTA', 'TATAELXSI', 'POLARIS', 'SUBEX', 'TANLA',

            # Pharmaceuticals & Healthcare
            'SUNPHARMA', 'DRREDDY', 'CIPLA', 'LUPIN', 'BIOCON', 'AUROPHARMA',
            'CADILAHC', 'DIVISLAB', 'TORNTPHARM', 'GLENMARK', 'ALKEM', 'ABBOTINDIA',
            'PFIZER', 'GLAXO', 'NOVARTIS', 'SANOFI', 'GRANULES', 'LALPATHLAB',
            'THYROCARE', 'METROPOLIS', 'APOLLOHOSP', 'FORTIS', 'NARAYANA',
            'KRSNAA', 'MEDPLUS', 'STARHEALTH',

            # Auto & Auto Components
            'MARUTI', 'TATAMOTORS', 'M&M', 'BAJAJ-AUTO', 'HEROMOTOCO', 'TVSMOTOR',
            'EICHERMOT', 'ASHOKLEY', 'ESCORTS', 'BHARATFORG', 'MOTHERSUMI',
            'BOSCHLTD', 'MRF', 'APOLLOTYRE', 'BALKRISIND', 'CEAT', 'JK_TYRE',
            'EXIDEIND', 'AMARAJABAT', 'ENDURANCE', 'SUPRAJIT', 'WABCO',

            # Oil, Gas & Energy
            'RELIANCE', 'ONGC', 'IOC', 'BPCL', 'HINDPETRO', 'GAIL', 'OIL',
            'PETRONET', 'GSPL', 'IGL', 'MGL', 'NTPC', 'POWERGRID', 'TATAPOWER',
            'ADANIGREEN', 'ADANIPOWER', 'ADANITRANS', 'SUZLON', 'INOXWIND',

            # Metals & Mining
            'TATASTEEL', 'JSWSTEEL', 'SAIL', 'HINDALCO', 'VEDL', 'HINDZINC',
            'NATIONALUM', 'COALINDIA', 'NMDC', 'JINDALSTEL', 'JSPL', 'WELCORP',
            'RATNAMANI', 'APL', 'MOIL', 'GMDC',

            # FMCG & Consumer
            'HINDUNILVR', 'ITC', 'NESTLEIND', 'BRITANNIA', 'DABUR', 'COLPAL',
            'MARICO', 'GODREJCP', 'EMAMILTD', 'UBL', 'TATACONSUM', 'VBL',
            'CCL', 'RADICO', 'MCDOWELL-N', 'JUBLFOOD', 'DEVYANI', 'WESTLIFE',

            # Cement & Construction
            'ULTRACEMCO', 'ACC', 'AMBUJACEM', 'SHREECEM', 'RAMCOCEM', 'JKCEMENT',
            'HEIDELBERG', 'INDIACEM', 'ORIENTCEM', 'LT', 'DLF', 'GODREJPROP',
            'OBEROIRLTY', 'PRESTIGE', 'BRIGADE', 'SOBHA', 'SUNTECK',

            # Textiles & Apparel
            'AARVEE', 'ARVIND', 'GRASIM', 'VARDHMAN', 'TRIDENT', 'WELSPUNIND',
            'RAYMOND', 'VIP', 'RELAXO', 'BATA', 'TRENT', 'SHOPPERSSTOP',

            # Chemicals & Fertilizers
            'UPL', 'SRF', 'PIDILITIND', 'DEEPAKNTR', 'TATACHEM', 'AARTI',
            'ALKYLAMINE', 'BALRAMCHIN', 'CHAMBLFERT', 'COROMANDEL', 'RCF',
            'GSFC', 'GNFC', 'NFL', 'FACT',

            # Telecom & Media
            'BHARTIARTL', 'IDEA', 'RCOM', 'TATACOMM', 'RAILTEL', 'GTLINFRA',
            'HATHWAY', 'SITI', 'DEN', 'ZEEL', 'SUNTV', 'JAGRAN', 'NETWK18',

            # Real Estate & REITs
            'DLF', 'GODREJPROP', 'OBEROI', 'PRESTIGE', 'BRIGADE', 'SOBHA',
            'SUNTECK', 'PHOENIXLTD', 'MAHLIFE', 'EMBASSY', 'MINDSPACE',
            'BROOKFIELD',

            # New Age & Recently Listed Companies
            'ZOMATO', 'PAYTM', 'NYKAA', 'POLICYBZR', 'CARTRADE', 'EASEMYTRIP',
            'DELHIVERY', 'NAZARA', 'ANGELONE', '5PAISA', 'RATEGAIN', 'LATENTVIEW',
            'HAPPSTMNDS', 'ROSSARI', 'CHEMCON', 'BIKAJI', 'DEVYANI', 'SAPPHIRE',
            'KRSNAA', 'MEDPLUS', 'STARHEALTH', 'IRFC', 'RAILTEL', 'MAZAGON',
            'HAL', 'BDL', 'BEL', 'MIDHANI', 'GRSE', 'BEML',

            # Small & Mid Cap Popular Stocks
            'DIXON', 'AMBER', 'CROMPTON', 'HAVELLS', 'POLYCAB', 'VGUARD',
            'SYMPHONY', 'VOLTAS', 'BLUESTARCO', 'WHIRLPOOL', 'TITAN', 'KALYAN',
            'RAJESHEXPO', 'THANGAMAYIL', 'GITANJALI', 'PCJEWELLER',

            # Additional Active Stocks
            'AAVAS', 'ABFRL', 'ACC', 'ADANIENSOL', 'AFFLE', 'AJANTPHARM',
            'ALKEM', 'ALLCARGO', 'AMARAJABAT', 'AMBER', 'APOLLOTYRE', 'ARVIND',
            'ASHOKLEY', 'ASTERDM', 'ASTRAL', 'ATUL', 'AUBANK', 'AUROPHARMA',
            'BAJAJCON', 'BAJAJELECTR', 'BALRAMCHIN', 'BANDHANBNK', 'BATAINDIA',
            'BAYERCROP', 'BEL', 'BERGEPAINT', 'BHARATFORG', 'BHARATISHIP',
            'BHEL', 'BIOCON', 'BLISSGVS', 'BLUESTARCO', 'BOMDYEING', 'BSE',
            'BSOFT', 'CADILAHC', 'CAMS', 'CANFINHOME', 'CARBORUNIV', 'CASTROLIND',
            'CCL', 'CEATLTD', 'CENTRALBK', 'CENTURYPLY', 'CENTURYTEX', 'CERA',
            'CHAMBLFERT', 'CHENNPETRO', 'CHOLAHLDNG', 'CUB', 'COFORGE', 'COROMANDEL',
            'CROMPTON', 'CUMMINSIND', 'CYIENT', 'DALBHARAT', 'DEEPAKNTR', 'DELTACORP',
            'DHANI', 'DIXON', 'EIDPARRY', 'EIHOTEL', 'ELGIEQUIP', 'EMAMILTD',
            'ENDURANCE', 'ENGINERSIN', 'EQUITAS', 'ESCORTS', 'EXIDEIND', 'FDC',
            'FINEORG', 'FSL', 'GARFIBRES', 'GICRE', 'GILLETTE', 'GLAND',
            'GLAXO', 'GLENMARK', 'GMRINFRA', 'GNFC', 'GODREJIND', 'GOODYEAR',
            'GRAVITA', 'GRINDWELL', 'GRSE', 'GSFC', 'GSPL', 'GUJALKALI',
            'GUJGASLTD', 'HAL', 'HATSUN', 'HCC', 'HEG', 'HFCL', 'HGINFRA',
            'HINDCOPPER', 'HINDZINC', 'HMVL', 'HONAUT', 'HSCL', 'HUDCO',
            'IBREALEST', 'ICICIGI', 'ICICIPRULI', 'IDEA', 'IDFC', 'IEX',
            'IFBIND', 'IGL', 'INDIABULLS', 'INDIACEM', 'INDIAMART', 'INDIANB',
            'INDIGO', 'INDOCO', 'INDOSTAR', 'INDUSTOWER', 'INFIBEAM', 'INTELLECT',
            'IOB', 'IPCALAB', 'IRB', 'IRCTC', 'ISEC', 'ITI', 'JBCHEPHARM',
            'JCHAC', 'JKCEMENT', 'JKLAKSHMI', 'JKPAPER', 'JMFINANCIL', 'JSL',
            'JSLHISAR', 'JSWENERGY', 'JUBLINGREA', 'JUSTDIAL', 'JYOTHYLAB',
            'KAJARIACER', 'KALPATPOWR', 'KANSAINER', 'KARURVYSYA', 'KEC', 'KEI',
            'KNRCON', 'KPITTECH', 'KRBL', 'LALPATHLAB', 'LAOPALA', 'LAURUSLABS',
            'LAXMIMACH', 'LICHSGFIN', 'LINDEINDIA', 'LTI', 'LTTS', 'MAHABANK',
            'MAHINDCIE', 'MANAPPURAM', 'MCX', 'METKORE', 'MIDHANI', 'MINDTREE',
            'MOIL', 'MOTHERSUMI', 'MOTILALOFS', 'MRPL', 'MUTHOOTFIN', 'NALCO',
            'NATIONALUM', 'NAVINFLUOR', 'NBCC', 'NCC', 'NH', 'NHPC', 'NIITLTD',
            'NLCINDIA', 'NOCIL', 'OBEROIRLTY', 'OIL', 'ORIENTBELL', 'ORIENTELEC',
            'PARAGMILK', 'PASHUPATI', 'PEL', 'PERSISTENT', 'PFIZER', 'PGHH',
            'PHOENIXLTD', 'PIIND', 'PNBHOUSING', 'POLYMED', 'PRAJIND', 'PRESTIGE',
            'PRSMJOHNSN', 'PTC', 'PVR', 'QUESS', 'RAIN', 'RAJESHEXPO', 'RCF',
            'REDINGTON', 'RELAXO', 'RELCAPITAL', 'RELINFRA', 'RPOWER', 'ROUTE',
            'RTNPOWER', 'SANOFI', 'SCI', 'SFL', 'SHANKARA', 'SIS', 'SJVN',
            'SKFINDIA', 'SOBHA', 'SOLARINDS', 'SONATSOFTW', 'STAR', 'STRTECH',
            'SUDARSCHEM', 'SUNDARMFIN', 'SUNDRMFAST', 'SUNTV', 'SUPRAJIT', 'SUVEN',
            'SYMPHONY', 'SYNDIBANK', 'TATACHEM', 'TATACOMM', 'TATACONSUM', 'TATAELXSI',
            'TATAGLOBAL', 'TATAPOWER', 'TEAMLEASE', 'THERMAX', 'THYROCARE', 'TITAN',
            'TORNTPOWER', 'TRIDENT', 'TTKPRESTIG', 'TVSMOTOR', 'UCOBANK', 'UJJIVAN',
            'UNIONBANK', 'UPL', 'VGUARD', 'VIPIND', 'VTL', 'WOCKPHARMA', 'YESBANK',
            'ZENSARTECH', 'ZYDUSLIFE'
        }

    def get_stock_data_yahoo_robust(self, symbol: str) -> Optional[Dict]:
        """Get stock data with robust error handling and retries"""
        max_retries = 3
        
        for suffix in ['.NS', '.BO']:
            for attempt in range(max_retries):
                try:
                    yahoo_symbol = f"{symbol}{suffix}"
                    ticker = yf.Ticker(yahoo_symbol)
                    
                    # Get basic info with timeout
                    hist = ticker.history(period="2d")
                    if hist.empty:
                        continue
                    
                    current_price = hist['Close'].iloc[-1]
                    if pd.isna(current_price) or current_price <= 0:
                        continue
                    
                    previous_close = hist['Close'].iloc[-2] if len(hist) > 1 else current_price
                    change = current_price - previous_close
                    change_percent = (change / previous_close * 100) if previous_close else 0
                    
                    # Get volume and other data
                    volume = int(hist['Volume'].iloc[-1]) if not hist['Volume'].empty and not pd.isna(hist['Volume'].iloc[-1]) else None
                    day_high = float(hist['High'].iloc[-1]) if not hist['High'].empty and not pd.isna(hist['High'].iloc[-1]) else None
                    day_low = float(hist['Low'].iloc[-1]) if not hist['Low'].empty and not pd.isna(hist['Low'].iloc[-1]) else None
                    open_price = float(hist['Open'].iloc[-1]) if not hist['Open'].empty and not pd.isna(hist['Open'].iloc[-1]) else None
                    
                    # Try to get additional info (with error handling)
                    market_cap = None
                    pe_ratio = None
                    sector = None
                    company_name = symbol
                    
                    try:
                        info = ticker.info
                        if info:
                            market_cap = info.get('marketCap')
                            pe_ratio = info.get('trailingPE')
                            sector = info.get('sector')
                            company_name = info.get('longName', info.get('shortName', symbol))
                    except:
                        pass
                    
                    return {
                        'symbol': symbol,
                        'yahoo_symbol': yahoo_symbol,
                        'name': company_name,
                        'price': round(float(current_price), 2),
                        'change': round(float(change), 2),
                        'change_percent': round(float(change_percent), 2),
                        'volume': volume,
                        'market_cap': market_cap,
                        'previous_close': float(previous_close) if previous_close else None,
                        'day_high': day_high,
                        'day_low': day_low,
                        'open': open_price,
                        'pe_ratio': pe_ratio,
                        'sector': sector,
                        'exchange': 'NSE' if suffix == '.NS' else 'BSE',
                        'currency': 'INR',
                        'source': 'Yahoo Finance',
                        'scraped_at': datetime.now().isoformat()
                    }
                    
                except Exception as e:
                    if attempt == max_retries - 1:
                        self.logger.debug(f"Final attempt failed for {symbol}{suffix}: {str(e)}")
                    time.sleep(0.1 * (attempt + 1))  # Progressive delay
                    continue
                    
        return None

    def scrape_stocks_parallel_robust(self, symbols: Set[str]) -> pd.DataFrame:
        """Robust parallel scraping with better error handling"""
        all_stocks = []
        symbols_list = list(symbols)
        total_symbols = len(symbols_list)
        
        self.logger.info(f"Starting robust scraping of {total_symbols} symbols with {self.max_workers} workers...")
        
        # Process in smaller batches for better control
        batch_size = 100
        batches = [symbols_list[i:i + batch_size] for i in range(0, len(symbols_list), batch_size)]
        
        for batch_num, batch in enumerate(batches, 1):
            self.logger.info(f"Processing batch {batch_num}/{len(batches)} ({len(batch)} symbols)")
            
            batch_stocks = []
            
            with ThreadPoolExecutor(max_workers=min(self.max_workers, len(batch))) as executor:
                # Submit tasks
                future_to_symbol = {
                    executor.submit(self.get_stock_data_yahoo_robust, symbol): symbol 
                    for symbol in batch
                }
                
                # Collect results with progress tracking
                completed = 0
                for future in as_completed(future_to_symbol, timeout=300):  # 5 minute timeout per batch
                    symbol = future_to_symbol[future]
                    completed += 1
                    
                    try:
                        stock_data = future.result()
                        if stock_data:
                            batch_stocks.append(stock_data)
                            
                            # Log progress
                            if completed % 20 == 0 or stock_data['price'] > 1000:
                                self.logger.info(
                                    f"✅ Batch {batch_num} - {completed}/{len(batch)}: "
                                    f"{symbol} = ₹{stock_data['price']} ({stock_data['change_percent']:+.1f}%)"
                                )
                    except Exception as e:
                        if completed % 50 == 0:
                            self.logger.debug(f"❌ {symbol}: {str(e)}")
            
            all_stocks.extend(batch_stocks)
            
            # Batch summary
            success_rate = (len(batch_stocks) / len(batch)) * 100
            self.logger.info(f"Batch {batch_num} completed: {len(batch_stocks)}/{len(batch)} stocks ({success_rate:.1f}% success)")
            
            # Small delay between batches
            if batch_num < len(batches):
                time.sleep(2)
        
        self.logger.info(f"Scraping completed! Total: {len(all_stocks)}/{total_symbols} stocks retrieved")
        return pd.DataFrame(all_stocks) if all_stocks else pd.DataFrame()

    def save_comprehensive_data(self, df: pd.DataFrame, filename: Optional[str] = None) -> str:
        """Save data with comprehensive analysis"""
        if filename is None:
            filename = f"comprehensive_indian_stocks_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        os.makedirs('data', exist_ok=True)
        
        # Save main CSV
        csv_path = f"data/{filename}.csv"
        df.to_csv(csv_path, index=False)
        
        # Save JSON
        json_path = f"data/{filename}.json"
        df.to_json(json_path, orient='records', indent=2)
        
        # Create Excel with multiple sheets
        try:
            excel_path = f"data/{filename}.xlsx"
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                # All stocks
                df.to_excel(writer, sheet_name='All Stocks', index=False)
                
                if len(df) > 0:
                    # Top gainers and losers
                    if 'change_percent' in df.columns:
                        gainers = df.nlargest(50, 'change_percent')
                        gainers.to_excel(writer, sheet_name='Top 50 Gainers', index=False)
                        
                        losers = df.nsmallest(50, 'change_percent')
                        losers.to_excel(writer, sheet_name='Top 50 Losers', index=False)
                    
                    # High volume and high price
                    if 'volume' in df.columns and df['volume'].notna().any():
                        high_volume = df[df['volume'].notna()].nlargest(50, 'volume')
                        high_volume.to_excel(writer, sheet_name='High Volume', index=False)
                    
                    if 'price' in df.columns:
                        high_price = df.nlargest(50, 'price')
                        high_price.to_excel(writer, sheet_name='High Price', index=False)
                    
                    # Sector-wise analysis
                    if 'sector' in df.columns and df['sector'].notna().any():
                        sector_summary = df[df['sector'].notna()].groupby('sector').agg({
                            'symbol': 'count',
                            'price': 'mean',
                            'change_percent': 'mean'
                        }).round(2).rename(columns={'symbol': 'count'})
                        sector_summary.to_excel(writer, sheet_name='Sector Analysis')
                        
        except ImportError:
            self.logger.warning("openpyxl not installed. Excel file not created.")
        
        # Create summary
        summary = {
            'total_stocks': len(df),
            'scraped_at': datetime.now().isoformat(),
            'scraper_version': 'Robust v1.0',
            'success_metrics': {}
        }
        
        if len(df) > 0:
            summary['success_metrics'] = {
                'stocks_with_price': int(len(df[df['price'] > 0])),
                'stocks_with_volume': int(len(df[df['volume'].notna()])) if 'volume' in df.columns else 0,
                'avg_price': float(df['price'].mean()),
                'avg_change_percent': float(df['change_percent'].mean()) if 'change_percent' in df.columns else 0,
                'gainers': int(len(df[df['change_percent'] > 0])) if 'change_percent' in df.columns else 0,
                'losers': int(len(df[df['change_percent'] < 0])) if 'change_percent' in df.columns else 0
            }
        
        summary_path = f"data/{filename}_summary.json"
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        self.logger.info(f"Data saved to {csv_path}")
        return csv_path

def main():
    """Main function with robust error handling"""
    try:
        # Initialize scraper with conservative settings
        scraper = RobustStockScraper(delay=0.2, max_workers=50)
        
        print("🚀 ROBUST STOCK SCRAPER v1.0")
        print("🎯 Target: Maximum stocks with high reliability")
        print("⚡ Using optimized parallel processing")
        
        # Get symbols
        print("\n🔍 Phase 1: Getting stock symbols...")
        symbols = scraper.get_comprehensive_symbol_list()
        print(f"✅ Found {len(symbols)} unique symbols")
        
        if len(symbols) == 0:
            print("❌ No symbols found. Exiting.")
            return False
        
        # Scrape data
        print(f"\n📊 Phase 2: Scraping stock data...")
        stock_data = scraper.scrape_stocks_parallel_robust(symbols)
        
        if not stock_data.empty:
            # Clean data
            print(f"\n🔧 Phase 3: Processing results...")
            stock_data = stock_data.dropna(subset=['price'])
            stock_data = stock_data[stock_data['price'] > 0]
            stock_data = stock_data.drop_duplicates(subset=['symbol'], keep='first')
            
            # Save data
            csv_file = scraper.save_comprehensive_data(stock_data)
            
            # Success summary
            print(f"\n🎉 SUCCESS! Stock scraping completed")
            print(f"📊 Stocks Retrieved: {len(stock_data)}")
            print(f"💾 Data saved to: {csv_file}")
            
            # Market summary
            if len(stock_data) >= 50:
                print(f"\n📈 MARKET SUMMARY:")
                print(f"   🏢 Total Stocks: {len(stock_data)}")
                
                if 'change_percent' in stock_data.columns:
                    avg_change = stock_data['change_percent'].mean()
                    gainers = len(stock_data[stock_data['change_percent'] > 0])
                    losers = len(stock_data[stock_data['change_percent'] < 0])
                    
                    print(f"   📊 Average Change: {avg_change:+.2f}%")
                    print(f"   🟢 Gainers: {gainers} ({gainers/len(stock_data)*100:.1f}%)")
                    print(f"   🔴 Losers: {losers} ({losers/len(stock_data)*100:.1f}%)")
                
                # Show top performers
                if len(stock_data) >= 10:
                    print(f"\n🏆 TOP 5 PERFORMERS:")
                    top_gainers = stock_data.nlargest(5, 'change_percent')
                    for i, (_, stock) in enumerate(top_gainers.iterrows(), 1):
                        print(f"   {i}. {stock['symbol']:10s}: +{stock['change_percent']:5.2f}% (₹{stock['price']:,.2f})")
            
            return True
        
        else:
            print("❌ No stock data retrieved")
            print("🔧 Check internet connection and try again")
            return False
            
    except Exception as e:
        print(f"❌ Critical error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
