import requests
import pandas as pd
import json
import time
from datetime import datetime, timedelta
import logging
import os
from typing import List, Dict, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
import yfinance as yf
import re
from io import StringIO

class ActiveTradingStockScraper:
    def __init__(self, delay: float = 0.1, max_workers: int = 100):
        """
        Scraper focused on actively trading stocks only
        
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
                logging.FileHandler('active_stocks_scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Initialize session
        self._initialize_session()
    
    def _initialize_session(self):
        """Initialize session with NSE"""
        try:
            self.logger.info("Initializing session...")
            self.session.get("https://www.nseindia.com", timeout=10)
            time.sleep(2)
            self.logger.info("Session initialized")
        except Exception as e:
            self.logger.warning(f"Session init warning: {str(e)}")
    
    def get_active_nse_symbols_from_bhavcopy(self) -> Set[str]:
        """Get actively traded symbols from NSE Bhav Copy (daily trading data)"""
        symbols = set()
        
        try:
            self.logger.info("Fetching active symbols from NSE Bhav Copy...")
            
            # Get current date and previous trading days
            current_date = datetime.now()
            
            # Try last 5 trading days to find a valid bhav copy
            for days_back in range(0, 8):
                try:
                    date = current_date - timedelta(days=days_back)
                    # Skip weekends
                    if date.weekday() >= 5:  # Saturday = 5, Sunday = 6
                        continue
                        
                    date_str = date.strftime("%d%b%Y").upper()
                    
                    # NSE Bhav Copy URL
                    url = f"https://archives.nseindia.com/content/historical/EQUITIES/{date.year}/{date.strftime('%b').upper()}/cm{date_str}bhav.csv.zip"
                    
                    self.logger.info(f"Trying Bhav Copy for {date_str}...")
                    
                    response = self.session.get(url, timeout=30)
                    if response.status_code == 200:
                        # Read the CSV content from zip
                        import zipfile
                        from io import BytesIO
                        
                        with zipfile.ZipFile(BytesIO(response.content)) as z:
                            csv_filename = f"cm{date_str}bhav.csv"
                            with z.open(csv_filename) as csv_file:
                                df = pd.read_csv(csv_file)
                                
                                # Get symbols from bhav copy
                                if 'SYMBOL' in df.columns:
                                    active_symbols = set(df['SYMBOL'].unique())
                                    symbols.update(active_symbols)
                                    self.logger.info(f"Found {len(active_symbols)} active symbols from {date_str}")
                                    break
                                    
                except Exception as e:
                    self.logger.debug(f"Bhav copy error for {date_str}: {str(e)}")
                    continue
                    
        except Exception as e:
            self.logger.warning(f"Bhav Copy method failed: {str(e)}")
        
        return symbols
    
    def get_startup_unicorn_symbols(self) -> Set[str]:
        """Get comprehensive list of ACTUALLY LISTED startup and new-age company symbols"""
        # ONLY INCLUDE COMPANIES THAT ARE ACTUALLY TRADING ON NSE/BSE
        listed_newage_companies = {
            
            # SUCCESSFULLY LISTED UNICORNS & NEW-AGE COMPANIES
            'ZOMATO',        # Food delivery - Listed 2021
            'PAYTM',         # Fintech - Listed 2021  
            'NYKAA',         # E-commerce beauty - Listed 2021
            'POLICYBZR',     # Insurance tech - Listed 2021
            'CARTRADE',      # Auto marketplace - Listed 2021
            'EASEMYTRIP',    # Travel tech - Listed 2021
            
            # FINTECH & DIGITAL PAYMENTS (LISTED)
            'ANGELONE',      # Angel Broking - Listed
            '5PAISA',        # 5paisa - Listed
            'CDSL',          # Central Depository - Listed  
            'CAMS',          # Computer Age Management - Listed
            'BSE',           # Bombay Stock Exchange - Listed
            'MCX',           # Multi Commodity Exchange - Listed
            'MSEI',          # Metropolitan Stock Exchange - Listed
            
            # RECENTLY LISTED IPOS (2020-2024)
            'IRCTC',         # Indian Railway Catering - Listed 2019
            'SBI_CARDS',     # SBI Cards - Listed 2020 (if available as SBICARDS)
            'SBICARDS',      # SBI Cards - Listed 2020
            'ROUTE',         # Route Mobile - Listed 2020
            'HAPPSTMNDS',    # Happiest Minds - Listed 2020
            'CHEMCON',       # Chemcon Speciality - Listed 2020
            'MAZAGON',       # Mazagon Dock - Listed 2020
            'LIKHITHA',      # Likhitha Infrastructure - Listed 2020
            'UJJIVAN',       # Ujjivan Financial - Listed
            'UJJIVANSFB',    # Ujjivan Small Finance Bank - Listed 2021
            'SURYODAY',      # Suryoday Small Finance Bank - Listed 2021
            'EQUITAS',       # Equitas Small Finance Bank - Listed
            
            # TECH & IT SERVICES (RECENTLY LISTED)
            'LATENTVIEW',    # Latent View Analytics - Listed 2021
            'FRESHWORKS',    # Freshworks (if trading in India)
            'MINDSPACE',     # Mindspace Business Parks - Listed 2020
            'BROOKFIELD',    # Brookfield India REIT - Listed 2021
            'EMBASSY',       # Embassy Office Parks REIT - Listed 2019
            
            # HEALTHCARE & DIAGNOSTICS (LISTED)
            'KRSNAA',        # Krsnaa Diagnostics - Listed 2021
            'MEDPLUS',       # MedPlus Health Services - Listed 2021
            'METROPOLIS',    # Metropolis Healthcare - Listed 2019
            'LALPATHLAB',    # Dr. Lal PathLabs - Listed
            'THYROCARE',     # Thyrocare Technologies - Listed
            'STARHEALTH',    # Star Health Insurance - Listed 2021
            
            # FOOD & RESTAURANT CHAINS (LISTED)
            'DEVYANI',       # Devyani International (KFC, Pizza Hut) - Listed 2021
            'WESTLIFE',      # Westlife Development (McDonald's) - Listed
            'JUBLFOOD',      # Jubilant FoodWorks (Domino's) - Listed
            'SAPPHIRE',      # Sapphire Foods (KFC, Pizza Hut) - Listed 2021
            'BIKAJI',        # Bikaji Foods - Listed 2022
            'DODLA',         # Dodla Dairy - Listed 2021
            'HATSUN',        # Hatsun Agro - Listed
            
            # LOGISTICS & SUPPLY CHAIN (LISTED)
            'DELHIVERY',     # Delhivery - Listed 2022
            'BLUEDART',      # Blue Dart Express - Listed
            'GATI',          # Gati Limited - Listed
            'ALLCARGO',      # Allcargo Logistics - Listed
            'CONCOR',        # Container Corporation - Listed
            'TCI',           # Transport Corporation - Listed
            
            # E-COMMERCE ENABLERS (LISTED)
            'INDIAMART',     # IndiaMART InterMESH - Listed 2019
            'JUSTDIAL',      # Just Dial - Listed
            'NAUKRI',        # Info Edge (Naukri.com) - Listed
            
            # DIGITAL & TECH SERVICES (LISTED)
            'TANLA',         # Tanla Platforms - Listed
            'ONMOBILE',      # OnMobile Global - Listed
            'RATEGAIN',      # RateGain Travel Technologies - Listed 2021
            'NEWGEN',        # Newgen Software - Listed
            'SUBEX',         # Subex Limited - Listed
            'SONATA',        # Sonata Software - Listed
            
            # NEW-AGE MANUFACTURING & D2C (LISTED)
            'DIXON',         # Dixon Technologies - Listed
            'AMBER',         # Amber Enterprises - Listed
            'CROMPTON',      # Crompton Greaves Consumer - Listed
            'HAVELLS',       # Havells India - Listed
            'POLYCAB',       # Polycab India - Listed
            
            # RENEWABLE ENERGY (LISTED)
            'ADANIGREEN',    # Adani Green Energy - Listed
            'TATAPOWER',     # Tata Power - Listed
            'SUZLON',        # Suzlon Energy - Listed
            'INOXWIND',      # Inox Wind - Listed
            'WEBSOL',        # Websol Energy System - Listed
            
            # ELECTRIC VEHICLES & AUTO TECH (LISTED)
            'OLECTRA',       # Olectra Greentech - Listed
            'ASHOKLEY',      # Ashok Leyland - Listed
            'TATAMOTORS',    # Tata Motors (EV division) - Listed
            'BAJAJ-AUTO',    # Bajaj Auto (EV plans) - Listed
            'HEROMOTOCO',    # Hero MotoCorp (EV plans) - Listed
            'TVSMOTOR',      # TVS Motor (Electric) - Listed
            
            # DEFENSE & AEROSPACE (RECENTLY LISTED)
            'HAL',           # Hindustan Aeronautics - Listed 2018
            'BDL',           # Bharat Dynamics - Listed 2016
            'BEL',           # Bharat Electronics - Listed
            'MIDHANI',       # Mishra Dhatu Nigam - Listed 2018
            'GRSE',          # Garden Reach Shipbuilders - Listed 2018
            'BEML',          # BEML Limited - Listed
            
            # RAILWAY & INFRASTRUCTURE (RECENTLY LISTED)
            'IRFC',          # Indian Railway Finance Corp - Listed 2021
            'RAILTEL',       # RailTel Corporation - Listed 2021
            'RITES',         # RITES Limited - Listed 2018
            'CONCOR',        # Container Corporation - Listed
            'NBCC',          # NBCC India - Listed
            
            # GAMING & ENTERTAINMENT (LISTED)
            'NAZARA',        # Nazara Technologies - Listed 2021
            'DELTATECH',     # Delta Corp (Gaming) - Listed
            'ONMOBILE',      # OnMobile Global - Listed
            
            # SMALL FINANCE BANKS (NEW-AGE BANKING)
            'UJJIVANSFB',    # Ujjivan Small Finance Bank
            'SURYODAY',      # Suryoday Small Finance Bank  
            'EQUITAS',       # Equitas Small Finance Bank
            'ESAFSFB',       # ESAF Small Finance Bank
            'FINCARE',       # Fincare Small Finance Bank
            'CAPITALSFB',    # Capital Small Finance Bank
            
            # HOUSING FINANCE (NEW-AGE)
            'AAVAS',         # Aavas Financiers - Listed 2018
            'CANFINHOME',    # Can Fin Homes - Listed
            'LICHSGFIN',     # LIC Housing Finance - Listed
            'HUDCO',         # Housing & Urban Development - Listed
            
            # MUTUAL FUNDS & ASSET MANAGEMENT
            'HDFCAMC',       # HDFC Asset Management - Listed 2018
            'NIPPONLIFE',    # Nippon Life India AM - Listed 2017
            'UTIAMC',        # UTI Asset Management - Listed 2020
            'SBIAMC',        # SBI Funds Management (if listed)
            
            # INSURANCE (RECENTLY LISTED)
            'SBILIFE',       # SBI Life Insurance - Listed 2017
            'HDFCLIFE',      # HDFC Life Insurance - Listed 2017  
            'ICICIGI',       # ICICI General Insurance - Listed 2017
            'ICICIPRULI',    # ICICI Prudential Life - Listed 2016
            'STARHEALTH',    # Star Health Insurance - Listed 2021
            
            # REAL ESTATE (NEW-AGE)
            'EMBASSY',       # Embassy Office Parks REIT - Listed 2019
            'MINDSPACE',     # Mindspace Business Parks REIT - Listed 2020
            'BROOKFIELD',    # Brookfield India REIT - Listed 2021
            
            # RENEWABLE & CLEAN ENERGY (LISTED)
            'ADANIGREEN',    # Adani Green Energy
            'ADANIENT',      # Adani Enterprises  
            'TATAPOWER',     # Tata Power Company
            'NTPC',          # NTPC Limited
            'POWERGRID',     # Power Grid Corporation
        }
        
        return listed_newage_companies
    
    def get_symbols_from_marketwatch_screener(self) -> Set[str]:
        """Get symbols from market screener APIs"""
        symbols = set()
        
        try:
            self.logger.info("Fetching symbols from market screeners...")
            
            # MoneyControl screener
            try:
                url = "https://www.moneycontrol.com/stocks/marketstats/indexcomp.php?optex=NSE&opttopic=indexcomp&index=9"
                response = self.session.get(url, timeout=15)
                if response.status_code == 200:
                    # Parse HTML to extract symbols
                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # Look for stock symbols in the page
                    links = soup.find_all('a', href=re.compile(r'/stocks/company_info/stock_news.php'))
                    for link in links:
                        href = link.get('href', '')
                        # Extract symbol from URL
                        match = re.search(r'sc_id=([A-Z0-9&]+)', href)
                        if match:
                            symbol_part = match.group(1).split('&')[0]
                            if symbol_part and len(symbol_part) <= 20:
                                symbols.add(symbol_part)
                                
            except Exception as e:
                self.logger.debug(f"MoneyControl error: {str(e)}")
            
            # Try other financial websites
            screener_urls = [
                "https://www.screener.in/api/company/search/?q=&v=3",
                "https://ticker.finology.in/market/index/nse"
            ]
            
            for url in screener_urls:
                try:
                    response = self.session.get(url, timeout=10)
                    if response.status_code == 200:
                        data = response.text
                        # Extract potential symbols using regex
                        potential_symbols = re.findall(r'\b[A-Z]{2,15}\b', data)
                        # Filter valid stock symbols
                        valid_symbols = {s for s in potential_symbols 
                                       if 2 <= len(s) <= 15 and s.isalpha()}
                        symbols.update(valid_symbols)
                        
                except Exception as e:
                    self.logger.debug(f"Screener error for {url}: {str(e)}")
                    
        except Exception as e:
            self.logger.warning(f"Market screener error: {str(e)}")
        
        return symbols
    
    def get_symbols_from_trading_APIs(self) -> Set[str]:
        """Get symbols from various trading and financial APIs"""
        symbols = set()
        
        # NSE official API endpoints for active stocks
        nse_endpoints = [
            # Market data endpoints
            "https://www.nseindia.com/api/market-data-pre-open?key=NIFTY",
            "https://www.nseindia.com/api/market-data-pre-open?key=BANKNIFTY", 
            "https://www.nseindia.com/api/market-data-pre-open?key=ALL",
            
            # Live market endpoints
            "https://www.nseindia.com/api/marketStatus",
            "https://www.nseindia.com/api/allIndices",
            
            # Equity endpoints
            "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050",
            "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20NEXT%2050",
            "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20100", 
            "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20200",
            "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20500",
            "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20MIDCAP%20100",
            "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20MIDCAP%20150",
            "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20SMALLCAP%20100",
            "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20SMALLCAP%20250",
            "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20MICROCAP%20250",
            "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20TOTAL%20MARKET",
            
            # Sectoral indices
            "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20AUTO",
            "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20BANK",
            "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20ENERGY",
            "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20FMCG",
            "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20IT",
            "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20METAL",
            "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20PHARMA",
            "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20PSU%20BANK",
            "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20REALTY",
            
            # F&O stocks (most actively traded)
            "https://www.nseindia.com/api/equity-stockIndices?index=SECURITIES%20IN%20F%26O",
        ]
        
        for endpoint in nse_endpoints:
            try:
                self.logger.info(f"Fetching from: {endpoint.split('=')[-1] if '=' in endpoint else endpoint.split('/')[-1]}")
                
                response = self.session.get(endpoint, timeout=15)
                if response.status_code == 200:
                    data = response.json()
                    
                    # Extract symbols from different response structures
                    extracted_symbols = set()
                    
                    if 'data' in data and isinstance(data['data'], list):
                        for item in data['data']:
                            if isinstance(item, dict) and 'symbol' in item:
                                symbol = item['symbol']
                                if symbol and symbol != item.get('index', ''):  # Exclude index names
                                    extracted_symbols.add(symbol)
                    
                    elif isinstance(data, list):
                        for item in data:
                            if isinstance(item, dict):
                                for key in ['symbol', 'Symbol', 'SYMBOL']:
                                    if key in item and item[key]:
                                        extracted_symbols.add(item[key])
                    
                    elif isinstance(data, dict):
                        # Look for symbols in nested structures
                        def extract_symbols_recursive(obj):
                            syms = set()
                            if isinstance(obj, dict):
                                for k, v in obj.items():
                                    if k.lower() in ['symbol', 'symbols'] and isinstance(v, (str, list)):
                                        if isinstance(v, str):
                                            syms.add(v)
                                        elif isinstance(v, list):
                                            syms.update(v)
                                    elif isinstance(v, (dict, list)):
                                        syms.update(extract_symbols_recursive(v))
                            elif isinstance(obj, list):
                                for item in obj:
                                    syms.update(extract_symbols_recursive(item))
                            return syms
                        
                        extracted_symbols.update(extract_symbols_recursive(data))
                    
                    symbols.update(extracted_symbols)
                    if extracted_symbols:
                        self.logger.info(f"Found {len(extracted_symbols)} symbols from this endpoint")
                    
                time.sleep(0.5)  # Small delay between API calls
                
            except Exception as e:
                self.logger.debug(f"API endpoint error: {str(e)}")
                continue
        
        self.logger.info(f"Total symbols from NSE APIs: {len(symbols)}")
        return symbols
    
    def get_comprehensive_symbol_list(self) -> Set[str]:
        """Get comprehensive list using all methods"""
        all_symbols = set()
        
        # Method 1: NSE APIs (most reliable for active stocks)
        nse_symbols = self.get_symbols_from_trading_APIs()
        all_symbols.update(nse_symbols)
        self.logger.info(f"NSE APIs: {len(nse_symbols)} symbols")
        
        # Method 2: Bhav Copy (actual trading data)
        bhav_symbols = self.get_active_nse_symbols_from_bhavcopy()
        all_symbols.update(bhav_symbols) 
        self.logger.info(f"Bhav Copy: {len(bhav_symbols)} symbols")
        
        # Method 3: Market screeners
        screener_symbols = self.get_symbols_from_marketwatch_screener()
        all_symbols.update(screener_symbols)
        self.logger.info(f"Screeners: {len(screener_symbols)} symbols")
        
        # Method 4: Add startup and unicorn symbols
        startup_symbols = self.get_startup_unicorn_symbols()
        all_symbols.update(startup_symbols)
        self.logger.info(f"Startups & Unicorns: {len(startup_symbols)} symbols")
        
        # Method 5: Add known active stocks that might be missing
        known_active = self.get_known_active_symbols()
        all_symbols.update(known_active)
        self.logger.info(f"Known active: {len(known_active)} symbols")
        
        # Clean and validate symbols
        clean_symbols = set()
        for symbol in all_symbols:
            if symbol and isinstance(symbol, str):
                # Clean symbol
                clean_symbol = symbol.strip().upper()
                # Validate: alphabetic, reasonable length
                if (clean_symbol.isalpha() and 
                    2 <= len(clean_symbol) <= 20 and 
                    clean_symbol not in ['INDEX', 'NIFTY', 'SENSEX', 'BSE', 'NSE']):
                    clean_symbols.add(clean_symbol)
        
        self.logger.info(f"Total unique clean symbols: {len(clean_symbols)}")
        return clean_symbols
    
    def get_known_active_symbols(self) -> Set[str]:
        """Get known active trading symbols including new-age startups and recent IPOs"""
        # Extensive list of known actively traded stocks including unicorns and recent IPOs
        return {
            # Large Cap
            'RELIANCE', 'TCS', 'HDFCBANK', 'BHARTIARTL', 'ICICIBANK', 'INFOSYS', 
            'HINDUNILVR', 'ITC', 'SBIN', 'BAJFINANCE', 'LT', 'HCLTECH', 'ASIANPAINT',
            'MARUTI', 'BAJAJFINSV', 'WIPRO', 'NESTLEIND', 'ULTRACEMCO', 'TITAN',
            'AXISBANK', 'DMART', 'KOTAKBANK', 'SUNPHARMA', 'ONGC', 'NTPC',
            
            # NEW-AGE STARTUPS & UNICORNS (ACTUALLY LISTED ON NSE/BSE)
            'ZOMATO', 'PAYTM', 'NYKAA', 'POLICYBZR', 'CARTRADE', 'EASEMYTRIP',
            'RATEGAIN', 'FINO', 'LATENTVIEW', 'DELHIVERY', 'MINDSPACE', 
            'STARHEALTH', 'DEVYANI', 'SAPPHIRE', 'TATVA', 'KRSNAA',
            'CAREINSURE', 'MEDPLUS', 'CAMPUS', 'APTUS', 'ANUPAMRASH',
            'BIKAJI', 'DERBY', 'DODLA', 'CHEMCON', 'IRFC', 'RAILTEL',
            'HAPPSTMNDS', 'ROSSARI', 'NEOGEN', 'MAZAGON', 'INDIAMART', 'ROUTE',
            
            # UPCOMING & RECENTLY LISTED IPOS (Only those actually trading)
            'NAZARA', 'ANGELONE', '5PAISA', 'BSE', 'MCX', 'CDSL', 'CAMS',
            'UJJIVANSFB', 'SURYODAY', 'EQUITAS', 'AAVAS', 'HDFCAMC',
            
            # RECENT SUCCESSFUL IPOS & LISTINGS
            'IRFC', 'IRCTC', 'SBI_CARDS', 'SBICARDS', 'HAL', 'MAZAGONDOCK',
            'RAILTEL', 'INDIAMART', 'ROUTE', 'BSOFT', 'MINDSPACE',
            'BROOKFIELD', 'EMBASSY', 'MINDTECK', 'SONATA', 'DATAPATTNS',
            'INTELLECT', 'CYIENT', 'PERSISTENT', 'LTTS', 'LTIM',
            'TATATECH', 'TATAELXSI', 'TATACOMM', 'TATASTEEL', 'TATAPOWER',
            
            # FINTECH & DIGITAL COMPANIES
            '5PAISA', 'ANGELONE', 'CDSL', 'CAMS', 'KFINTECH', 'COMPUAGE',
            'MSTCLTD', 'ONMOBILE', 'TANLA', 'ROUTE', 'RATETECH',
            'TECHNO', 'NEWGEN', 'RAMCO', 'KPIT', 'ZENSAR', 'CYIENT',
            
            # E-COMMERCE & DIGITAL PLATFORMS  
            'NYKAA', 'FSN', 'FSNL', 'INDIAMART', 'JUSTDIAL', 'IRCTC',
            'SPICEJET', 'INDIGO', 'INTERGLOB', 'THOMAS', 'QUESS',
            
            # HEALTHCARE & BIOTECH UNICORNS
            'METROPOLIS', 'LALPATHLAB', 'THYROCARE', 'KRSNAA', 'MEDPLUS',
            'SUVEN', 'NEULAND', 'SEQUENT', 'STRIDES', 'DRREDDYS',
            'STARHEALTH', 'CAREINSURE', 'APOLLO', 'FORTIS', 'MAXHEALTH',
            
            # ED-TECH & LEARNING PLATFORMS
            'VEDANTU', 'UNACADEMY', 'BYJUS', 'EXTRAMARKS', 'NIIT',
            'APTECH', 'CAREER', 'TREEHOUSE', 'MINDTREE', 'ZENSAR',
            
            # RENEWABLE ENERGY & EV
            'TATAPOWER', 'ADANIGREEN', 'SUZLON', 'INOXWIND', 'WEBSOL',
            'OLECTRA', 'ASHOKLEY', 'TATAMOTORS', 'MAHINDRA', 'BAJAJ-AUTO',
            'HEROMOTOCO', 'TVSMOTORS', 'EICHERMOT', 'ESCORTS',
            
            # LOGISTICS & SUPPLY CHAIN
            'BLUEDART', 'GATI', 'MAHLOG', 'SNOWMAN', 'COLDEX',
            'AEGISCHEM', 'ALLCARGO', 'CONCOR', 'GATEWAY', 'TCI',
            
            # FOOD & BEVERAGE STARTUPS
            'JUBLFOOD', 'BIKAJI', 'HALDIRAMS', 'BRITANNIA', 'NESTLE',
            'GODREJCP', 'MARICO', 'DABUR', 'EMAMI', 'VBL', 'CCL',
            'DEVYANI', 'WESTLIFE', 'SAPPHIRE', 'DODLA', 'HATSUN',
            
            # Mid Cap
            'ADANIGREEN', 'ADANIPORTS', 'ADANIPOWER', 'ADANITRANS', 'AMBUJACEM',
            'APOLLOHOSP', 'BAJAJ-AUTO', 'BANKBARODA', 'BERGEPAINT', 'BIOCON',
            'BOSCHLTD', 'BPCL', 'BRITANNIA', 'CADILAHC', 'CANBK', 'CHOLAFIN',
            'CIPLA', 'COALINDIA', 'COLPAL', 'CONCOR', 'CUMMINSIND', 'DABUR',
            'DIVISLAB', 'DRREDDY', 'EICHERMOT', 'FEDERALBNK', 'GAIL', 'GLAND',
            'GODREJCP', 'GRASIM', 'HAVELLS', 'HDFCLIFE', 'HEROMOTOCO', 'HINDALCO',
            'HINDPETRO', 'HINDUNILVR', 'ICICIPRULI', 'IDFCFIRSTB', 'INDUSINDBK',
            'IOC', 'JSWSTEEL', 'JUBLFOOD', 'KOTAKBANK', 'LUPIN', 'M&M',
            'MARICO', 'MCDOWELL-N', 'MFSL', 'MGL', 'MPHASIS', 'MRF',
            'NAUKRI', 'NMDC', 'PAGEIND', 'PETRONET', 'PIDILITIND', 'PNB',
            'POWERGRID', 'RAMCOCEM', 'RECLTD', 'SAIL', 'SBILIFE', 'SHREECEM',
            'SIEMENS', 'TATAMOTORS', 'TATASTEEL', 'TECHM', 'TORNTPHARM', 'TRENT',
            'UBL', 'ULTRACEMCO', 'VEDL', 'VOLTAS', 'WIPRO', 'ZEEL',
            
            # Small Cap (Popular)
            'AAVAS', 'ABFRL', 'ACC', 'ADANIENSOL', 'AFFLE', 'AJANTPHARM',
            'ALKEM', 'AMARAJABAT', 'APOLLOTYRE', 'AUROPHARMA', 'AVANTI',
            'BAJAJCON', 'BAJAJELECTR', 'BALRAMCHIN', 'BATAINDIA', 'BEL',
            'BHARATFORG', 'BHARTIHEXA', 'BLUESTARCO', 'CANFINHOME', 'CEATLTD',
            'CENTRALBK', 'CHAMBLFERT', 'COFORGE', 'CROMPTON', 'CYIENT',
            'DEEPAKNTR', 'DELTACORP', 'DLF', 'ESCORTS', 'EXIDEIND',
            'FINEORG', 'FSL', 'GLENMARK', 'GMRINFRA', 'GODREJPROP',
            'GRANULES', 'HINDZINC', 'IBREALEST', 'IDEA', 'INDIACEM',
            'INDIAMART', 'INDIGO', 'INDUSTOWER', 'INTELLECT', 'IRB',
            'IRCTC', 'JINDALSTEL', 'JKCEMENT', 'JSL', 'JSWENERGY',
            'KANSAINER', 'KEI', 'L&TFH', 'LALPATHLAB', 'LICHSGFIN',
            'LTTS', 'MANAPPURAM', 'MFSL', 'MINDTREE', 'MOTHERSUMI',
            'NATIONALUM', 'NAUKRI', 'NAVINFLUOR', 'NBCC', 'NH',
            'OFSS', 'OIL', 'PERSISTENT', 'PFC', 'PHOENIXLTD',
            'POLYCAB', 'PVRINOX', 'QUESS', 'RAIN', 'RAJESHEXPO',
            'RBLBANK', 'RELAXO', 'SANOFI', 'SCHAEFFLER', 'SRF',
            'STAR', 'SUNDARMFIN', 'SUNDRMFAST', 'SUNTV', 'SUPRAJIT',
            'SYMPHONY', 'TEAMLEASE', 'THERMAX', 'THYROCARE', 'TORNTPOWER',
            'TVSMOTOR', 'UJJIVAN', 'UNIONBANK', 'VGUARD', 'WHIRLPOOL',
            'YESBANK', 'ZYDUSLIFE',
            
            # Additional Active Stocks (Including Recent IPOs & Startups)
            'AAVAS', 'ABBOTINDIA', 'ABCAPITAL', 'ABFRL', 'ACC', 'ADANIENT',
            'ADANIPORTS', 'AFFLE', 'AIAENG', 'AJANTPHARM', 'AKZOINDIA', 'ALEMBICLTD',
            'ALKYLAMINE', 'ALLCARGO', 'AMARAJABAT', 'AMBER', 'AMBUJACEM', 'ANGELONE',
            
            # CRYPTO & BLOCKCHAIN COMPANIES
            'TANLA', 'RATEGAIN', 'MINDTECK', 'NEWGEN', 'SUBEX', 'SONATA',
            'RAMCO', 'INTELLECT', 'CYIENT', 'KPIT', 'ZENSAR', 'PERSISTENT',
            
            # GAMING & ENTERTAINMENT
            'NAZARA', 'DELTA', 'ONMOBILE', 'SITI', 'HATHWAY', 'DEN',
            'GTLINFRA', 'RCOM', 'IDEA', 'VODAFONE', 'BHARTI', 'RAILTEL',
            
            # SPACE-TECH & DEFENSE
            'HAL', 'BDL', 'BEL', 'MIDHANI', 'GRSE', 'COCHINSHIP',
            'GARDENREACH', 'MAZAGON', 'BEML', 'BHEL', 'ORDNANCE',
            
            # AGRI-TECH & FOOD PROCESSING
            'ADVANTA', 'RALLIS', 'COROMANDEL', 'CHAMBLFERT', 'GSFC',
            'RCF', 'NFL', 'FACT', 'GNFC', 'MADRAS', 'ZUARI',
            
            # REAL ESTATE & PROP-TECH
            'DLF', 'GODREJPROP', 'OBEROI', 'PRESTIGE', 'BRIGADE',
            'SOBHA', 'LODHA', 'SUNTECK', 'RADICO', 'RADIANT',
            'AARTIDRUGS', 'AARTIIND', 'AAVAS', 'ABB', 'ABBOTINDIA', 'ABCAPITAL',
            'ABFRL', 'ACC', 'ACLGATI', 'ADANIENT', 'ADANIPORTS', 'AFFLE',
            'AIAENG', 'AJANTPHARM', 'AKZOINDIA', 'ALEMBICLTD', 'ALKYLAMINE',
            'ALLCARGO', 'AMARAJABAT', 'AMBER', 'AMBUJACEM', 'ANGELONE',
            'ANURAS', 'APLLTD', 'APOLLOHOSP', 'APOLLOTYRE', 'ARVINDFASN',
            'ASHOKLEY', 'ASIANPAINT', 'ASTERDM', 'ASTRAL', 'ATUL',
            'AUBANK', 'AUROPHARMA', 'AVANTIFEED', 'AXISBANK', 'BAJAJ-AUTO',
            'BAJAJCON', 'BAJAJELECTR', 'BAJAJFINSV', 'BAJFINANCE', 'BALMLAWRIE',
            'BALRAMCHIN', 'BANDHANBNK', 'BANKBARODA', 'BANKINDIA', 'BATAINDIA',
            'BAYERCROP', 'BDL', 'BEL', 'BERGEPAINT', 'BHARATFORG',
            'BHARATISHIP', 'BHARTIARTL', 'BHEL', 'BIOCON', 'BIRLACORPN',
            'BLISSGVS', 'BLUESTARCO', 'BOMDYEING', 'BOSCHLTD', 'BPCL',
            'BRITANNIA', 'BSE', 'BSOFT', 'CADILAHC', 'CAMS',
            'CANBK', 'CANFINHOME', 'CAPLIPOINT', 'CARBORUNIV', 'CARERATING',
            'CASTROLIND', 'CCL', 'CEATLTD', 'CENTRALBK', 'CENTURYPLY',
            'CENTURYTEX', 'CERA', 'CHAMBLFERT', 'CHENNPETRO', 'CHOLAHLDNG',
            'CHOLAFIN', 'CIPLA', 'CUB', 'COALINDIA', 'COFORGE',
            'COLPAL', 'CONCOR', 'COROMANDEL', 'CROMPTON', 'CUB',
            'CUMMINSIND', 'CYIENT', 'DABUR', 'DALBHARAT', 'DEEPAKNTR',
            'DELTACORP', 'DHANI', 'DISHTV', 'DIVISLAB', 'DIXON',
            'DLF', 'DRREDDY', 'EICHERMOT', 'EIDPARRY', 'EIHOTEL',
            'ELGIEQUIP', 'EMAMILTD', 'ENDURANCE', 'ENGINERSIN', 'EQUITAS',
            'ESCORTS', 'ESSELPACK', 'EXIDEIND', 'FDC', 'FEDERALBNK',
            'FINEORG', 'FINPIPE', 'FSL', 'GAIL', 'GARFIBRES',
            'GICRE', 'GILLETTE', 'GLAND', 'GLAXO', 'GLENMARK',
            'GLOBALVECT', 'GMRINFRA', 'GNFC', 'GODREJCP', 'GODREJIND',
            'GODREJPROP', 'GOODYEAR', 'GRASIM', 'GRAVITA', 'GRINDWELL',
            'GRSE', 'GSFC', 'GSPL', 'GUJALKALI', 'GUJGASLTD',
            'HAL', 'HAVELLS', 'HATSUN', 'HCC', 'HCL-INSYS',
            'HCLTECH', 'HDFC', 'HDFCAMC', 'HDFCBANK', 'HDFCLIFE',
            'HEG', 'HEROMOTOCO', 'HFCL', 'HGINFRA', 'HINDALCO',
            'HINDCOPPER', 'HINDPETRO', 'HINDUNILVR', 'HINDZINC', 'HMVL',
            'HONAUT', 'HSCL', 'HUDCO', 'IBREALEST', 'ICICIBANK',
            'ICICIGI', 'ICICIPRULI', 'IDEA', 'IDFC', 'IDFCFIRSTB',
            'IEX', 'IFBIND', 'IGL', 'INDIABULLS', 'INDIACEM',
            'INDIAMART', 'INDIANB', 'INDIGO', 'INDOCO', 'INDOSTAR',
            'INDUSINDBK', 'INDUSTOWER', 'INFIBEAM', 'INFOSYS', 'INFY',
            'INTELLECT', 'IOB', 'IOC', 'IPCALAB', 'IRB',
            'IRCTC', 'ISEC', 'ITC', 'ITI', 'J&KBANK',
            'JBCHEPHARM', 'JCHAC', 'JETAIRWAYS', 'JKCEMENT', 'JKLAKSHMI',
            'JKPAPER', 'JMFINANCIL', 'JSL', 'JSLHISAR', 'JSWENERGY',
            'JSWSTEEL', 'JUBLFOOD', 'JUBLINGREA', 'JUSTDIAL', 'JYOTHYLAB',
            'KAJARIACER', 'KALPATPOWR', 'KANSAINER', 'KARURVYSYA', 'KEC',
            'KEI', 'KNRCON', 'KOTAKBANK', 'KPITTECH', 'KRBL',
            'L&TFH', 'LALPATHLAB', 'LAOPALA', 'LAURUSLABS', 'LAXMIMACH',
            'LICHSGFIN', 'LINDEINDIA', 'LT', 'LTI', 'LTTS',
            'LUPIN', 'M&M', 'M&MFIN', 'MAHABANK', 'MAHINDCIE',
            'MANAPPURAM', 'MARICO', 'MARUTI', 'MCDOWELL-N', 'MCX',
            'METKORE', 'MFSL', 'MGL', 'MINDTREE', 'MIDHANI',
            'MOIL', 'MOTHERSUMI', 'MOTILALOFS', 'MPHASIS', 'MRF',
            'MRPL', 'MUTHOOTFIN', 'NALCO', 'NATIONALUM', 'NAUKRI',
            'NAVINFLUOR', 'NBCC', 'NCC', 'NESTLEIND', 'NH',
            'NHPC', 'NIITLTD', 'NLCINDIA', 'NMDC', 'NOCIL',
            'NTPC', 'OBEROIRLTY', 'OIL', 'ONGC', 'ORIENTBELL',
            'ORIENTELEC', 'PAGEIND', 'PARAGMILK', 'PASHUPATI', 'PEL',
            'PERSISTENT', 'PETRONET', 'PFC', 'PFIZER', 'PGHH',
            'PHOENIXLTD', 'PIDILITIND', 'PIIND', 'PNB', 'PNBHOUSING',
            'POLYCAB', 'POLYMED', 'POWERGRID', 'PRAJIND', 'PRESTIGE',
            'PRSMJOHNSN', 'PTC', 'PVR', 'QUESS', 'RAIN',
            'RAJESHEXPO', 'RAMCOCEM', 'RBLBANK', 'RCF', 'RECLTD',
            'REDINGTON', 'RELAXO', 'RELCAPITAL', 'RELIANCE', 'RELINFRA',
            'RPOWER', 'ROUTE', 'RTNPOWER', 'SAIL', 'SANOFI',
            'SBIN', 'SBILIFE', 'SCHAEFFLER', 'SCI', 'SFL',
            'SHANKARA', 'SHREECEM', 'SIEMENS', 'SIS', 'SJVN',
            'SKFINDIA', 'SOBHA', 'SOLARINDS', 'SONATSOFTW', 'SRF',
            'STAR', 'STRTECH', 'SUDARSCHEM', 'SUNDARMFIN', 'SUNDRMFAST',
            'SUNPHARMA', 'SUNTV', 'SUPRAJIT', 'SUVEN', 'SYMPHONY',
            'SYNDIBANK', 'TATACHEM', 'TATACOMM', 'TATACONSUM', 'TATAELXSI',
            'TATAGLOBAL', 'TATAMOTORS', 'TATAPOWER', 'TATASTEEL', 'TCS',
            'TEAMLEASE', 'TECHM', 'THERMAX', 'THYROCARE', 'TITAN',
            'TORNTPHARM', 'TORNTPOWER', 'TRENT', 'TRIDENT', 'TTKPRESTIG',
            'TVSMOTOR', 'UBL', 'UCOBANK', 'UJJIVAN', 'ULTRACEMCO',
            'UNIONBANK', 'UPL', 'VEDL', 'VGUARD', 'VIPIND',
            'VOLTAS', 'VTL', 'WHIRLPOOL', 'WIPRO', 'WOCKPHARMA',
            'YESBANK', 'ZEEL', 'ZENSARTECH', 'ZYDUSLIFE'
        }
    
    def get_stock_data_yahoo_fast(self, symbol: str) -> Optional[Dict]:
        """Optimized Yahoo Finance data retrieval"""
        try:
            # Try both NSE and BSE
            for suffix in ['.NS', '.BO']:
                try:
                    yahoo_symbol = f"{symbol}{suffix}"
                    ticker = yf.Ticker(yahoo_symbol)
                    
                    # Use fast_info for basic data (much faster)
                    try:
                        fast_info = ticker.fast_info
                        if not fast_info:
                            continue
                            
                        # Get current price
                        current_price = fast_info.get('lastPrice')
                        if not current_price or current_price <= 0:
                            continue
                        
                        previous_close = fast_info.get('previousClose', current_price)
                        market_cap = fast_info.get('marketCap')
                        
                        change = current_price - previous_close
                        change_percent = (change / previous_close * 100) if previous_close else 0
                        
                        # Get minimal additional data
                        basic_info = {}
                        try:
                            # Only get essential info to avoid timeouts
                            hist = ticker.history(period="1d")
                            if not hist.empty:
                                basic_info['volume'] = int(hist['Volume'].iloc[-1]) if not hist['Volume'].empty else None
                                basic_info['day_high'] = float(hist['High'].iloc[-1]) if not hist['High'].empty else None
                                basic_info['day_low'] = float(hist['Low'].iloc[-1]) if not hist['Low'].empty else None
                                basic_info['open'] = float(hist['Open'].iloc[-1]) if not hist['Open'].empty else None
                        except:
                            pass
                        
                        return {
                            'symbol': symbol,
                            'yahoo_symbol': yahoo_symbol,
                            'name': symbol,  # Use symbol as name for speed
                            'price': round(float(current_price), 2),
                            'change': round(float(change), 2),
                            'change_percent': round(float(change_percent), 2),
                            'volume': basic_info.get('volume'),
                            'market_cap': market_cap,
                            'previous_close': float(previous_close) if previous_close else None,
                            'day_high': basic_info.get('day_high'),
                            'day_low': basic_info.get('day_low'), 
                            'open': basic_info.get('open'),
                            'exchange': 'NSE' if suffix == '.NS' else 'BSE',
                            'currency': 'INR',
                            'source': 'Yahoo Finance Fast',
                            'scraped_at': datetime.now().isoformat()
                        }
                        
                    except Exception:
                        continue
                        
                except Exception:
                    continue
                    
        except Exception as e:
            self.logger.debug(f"Yahoo fast error for {symbol}: {str(e)}")
            
        return None
    
    def scrape_active_stocks_parallel(self, symbols: Set[str]) -> pd.DataFrame:
        """High-performance parallel scraping focused on active stocks"""
        all_stocks = []
        symbols_list = list(symbols)
        total_symbols = len(symbols_list)
        
        self.logger.info(f"Starting high-performance scraping of {total_symbols} symbols with {self.max_workers} workers...")
        
        # Use smaller batches for better memory management
        batch_size = 200
        batches = [symbols_list[i:i + batch_size] for i in range(0, len(symbols_list), batch_size)]
        
        for batch_num, batch in enumerate(batches, 1):
            self.logger.info(f"Processing batch {batch_num}/{len(batches)} ({len(batch)} symbols)")
            
            start_time = time.time()
            batch_stocks = []
            
            with ThreadPoolExecutor(max_workers=min(self.max_workers, len(batch))) as executor:
                # Submit all tasks
                future_to_symbol = {
                    executor.submit(self.get_stock_data_yahoo_fast, symbol): symbol 
                    for symbol in batch
                }
                
                # Collect results with progress tracking
                completed = 0
                success_count = 0
                
                for future in as_completed(future_to_symbol):
                    symbol = future_to_symbol[future]
                    completed += 1
                    
                    try:
                        stock_data = future.result()
                        if stock_data:
                            batch_stocks.append(stock_data)
                            success_count += 1
                            
                            # Log progress every 25 stocks or for notable stocks
                            if (completed % 25 == 0 or 
                                stock_data['price'] > 1000 or 
                                abs(stock_data['change_percent']) > 5):
                                self.logger.info(
                                    f"✅ Batch {batch_num} - {completed}/{len(batch)}: "
                                    f"{symbol} = ₹{stock_data['price']} ({stock_data['change_percent']:+.1f}%)"
                                )
                    except Exception as e:
                        if completed % 50 == 0:  # Log errors less frequently
                            self.logger.debug(f"❌ {symbol}: {str(e)}")
            
            # Batch summary
            batch_time = time.time() - start_time
            success_rate = (success_count / len(batch)) * 100
            
            self.logger.info(
                f"Batch {batch_num} completed in {batch_time:.1f}s: "
                f"{success_count}/{len(batch)} stocks ({success_rate:.1f}% success rate)"
            )
            
            all_stocks.extend(batch_stocks)
            
            # Short delay between batches
            if batch_num < len(batches):
                time.sleep(1)
        
        self.logger.info(f"All batches completed! Total: {len(all_stocks)}/{total_symbols} stocks retrieved")
        return pd.DataFrame(all_stocks) if all_stocks else pd.DataFrame()

def main():
    """Main function optimized for maximum stock coverage"""
    # High-performance settings
    scraper = ActiveTradingStockScraper(delay=0.05, max_workers=150)
    
    try:
        print("🚀 ACTIVE TRADING STOCKS SCRAPER v3.0")
        print("🎯 Target: 1000-2000+ actively traded stocks")
        print("⚡ Ultra-high performance mode: 150 parallel workers")
        
        # Phase 1: Symbol Discovery
        print("\n🔍 PHASE 1: Discovering actively traded symbols...")
        print("📊 Sources: NSE APIs, Bhav Copy, Market Screeners, Known Active Stocks")
        
        start_discovery = time.time()
        active_symbols = scraper.get_comprehensive_symbol_list()
        discovery_time = time.time() - start_discovery
        
        print(f"✅ Symbol discovery completed in {discovery_time:.1f}s")
        print(f"📈 Found {len(active_symbols)} unique active trading symbols")
        
        if len(active_symbols) < 800:
            print("⚠️  Warning: Fewer symbols than expected, but continuing...")
        
        # Phase 2: Data Scraping  
        print(f"\n📊 PHASE 2: Scraping stock data...")
        print(f"⚡ Ultra-fast parallel processing with {scraper.max_workers} workers")
        print("🎯 Focusing on actively traded stocks only")
        
        start_scraping = time.time()
        stock_data = scraper.scrape_active_stocks_parallel(active_symbols)
        scraping_time = time.time() - start_scraping
        
        if not stock_data.empty:
            # Clean and validate data
            print(f"\n🔧 PHASE 3: Data cleaning and validation...")
            initial_count = len(stock_data)
            
            # Remove invalid data
            stock_data = stock_data.dropna(subset=['price'])
            stock_data = stock_data[stock_data['price'] > 0]
            stock_data = stock_data.drop_duplicates(subset=['symbol'], keep='first')
            
            final_count = len(stock_data)
            if initial_count != final_count:
                print(f"🧹 Cleaned data: {initial_count} → {final_count} stocks")
            
            # Save data
            scraper_instance = ActiveTradingStockScraper()
            csv_file = scraper_instance.save_comprehensive_data(stock_data)
            
            # Success metrics
            total_time = discovery_time + scraping_time
            stocks_per_minute = (final_count / total_time) * 60
            
            print(f"\n🎉 SUCCESS! ACTIVE STOCKS SCRAPING COMPLETED")
            print(f"⏱️  Total Time: {total_time/60:.1f} minutes")
            print(f"📊 Active Stocks Found: {final_count}")
            print(f"⚡ Processing Speed: {stocks_per_minute:.1f} stocks/minute")
            print(f"💾 Data saved to: {csv_file}")
            
            # Market Analysis
            if len(stock_data) >= 100:
                print(f"\n📈 ACTIVE MARKET ANALYSIS:")
                print(f"   🏢 Total Active Companies: {len(stock_data)}")
                
                if 'price' in stock_data.columns:
                    avg_price = stock_data['price'].mean()
                    median_price = stock_data['price'].median()
                    max_price = stock_data['price'].max()
                    min_price = stock_data['price'].min()
                    
                    print(f"   💰 Price Statistics:")
                    print(f"      Average: ₹{avg_price:.2f}")
                    print(f"      Median: ₹{median_price:.2f}")
                    print(f"      Range: ₹{min_price:.2f} - ₹{max_price:,.2f}")
                
                if 'change_percent' in stock_data.columns:
                    avg_change = stock_data['change_percent'].mean()
                    gainers = len(stock_data[stock_data['change_percent'] > 0])
                    losers = len(stock_data[stock_data['change_percent'] < 0])
                    big_movers = len(stock_data[abs(stock_data['change_percent']) > 3])
                    
                    print(f"   📊 Market Sentiment:")
                    print(f"      Overall Change: {avg_change:+.2f}%")
                    print(f"      🟢 Gainers: {gainers} ({gainers/len(stock_data)*100:.1f}%)")
                    print(f"      🔴 Losers: {losers} ({losers/len(stock_data)*100:.1f}%)")
                    print(f"      🚀 Big Movers (±3%): {big_movers}")
                
                # Exchange breakdown
                if 'exchange' in stock_data.columns:
                    exchange_counts = stock_data['exchange'].value_counts()
                    print(f"   🏛️  Exchange Distribution:")
                    for exchange, count in exchange_counts.items():
                        print(f"      {exchange}: {count} stocks ({count/len(stock_data)*100:.1f}%)")
                
                # Top performers showcase
                print(f"\n🏆 TOP 10 PERFORMERS:")
                
                if len(stock_data) >= 10:
                    print(f"   📈 BIGGEST GAINERS:")
                    top_gainers = stock_data.nlargest(10, 'change_percent')
                    for i, (_, stock) in enumerate(top_gainers.iterrows(), 1):
                        print(f"      {i:2d}. {stock['symbol']:12s}: +{stock['change_percent']:5.1f}% (₹{stock['price']:7,.2f})")
                    
                    print(f"\n   📉 BIGGEST LOSERS:")
                    top_losers = stock_data.nsmallest(10, 'change_percent')
                    for i, (_, stock) in enumerate(top_losers.iterrows(), 1):
                        print(f"      {i:2d}. {stock['symbol']:12s}: {stock['change_percent']:6.1f}% (₹{stock['price']:7,.2f})")
                
                # Volume leaders (if available)
                if 'volume' in stock_data.columns and stock_data['volume'].notna().any():
                    print(f"\n   📊 VOLUME LEADERS:")
                    volume_leaders = stock_data.nlargest(5, 'volume')
                    for i, (_, stock) in enumerate(volume_leaders.iterrows(), 1):
                        if pd.notna(stock['volume']):
                            print(f"      {i}. {stock['symbol']:12s}: {stock['volume']:,} shares")
                
                print(f"\n✅ Successfully scraped {len(stock_data)} actively traded stocks!")
                
                if len(stock_data) >= 1000:
                    print("🎯 TARGET ACHIEVED: 1000+ stocks!")
                elif len(stock_data) >= 500:
                    print("📊 Good coverage: 500+ stocks captured")
                else:
                    print("📈 Coverage achieved, market may have fewer active stocks today")
        
        else:
            print("❌ No active stock data retrieved")
            print("🔧 Check internet connection and try again")
            return False
            
    except Exception as e:
        print(f"❌ Critical error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
