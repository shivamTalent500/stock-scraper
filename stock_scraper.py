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
import string
import itertools

class ComprehensiveSymbolScraper:
    def __init__(self, delay: float = 0.15, max_workers: int = 100):
        """
        Comprehensive scraper that discovers symbols dynamically and scrapes maximum stocks
        
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
                logging.FileHandler('comprehensive_scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def discover_symbols_systematically(self) -> Set[str]:
        """Discover symbols using multiple systematic approaches"""
        all_symbols = set()
        
        # Method 1: Known high-probability symbols
        known_symbols = self.get_high_probability_symbols()
        all_symbols.update(known_symbols)
        self.logger.info(f"High probability symbols: {len(known_symbols)}")
        
        # Method 2: Symbol pattern generation
        pattern_symbols = self.generate_systematic_patterns()
        all_symbols.update(pattern_symbols)
        self.logger.info(f"Pattern-based symbols: {len(pattern_symbols)}")
        
        # Method 3: Company name variations
        name_variations = self.generate_company_name_variations()
        all_symbols.update(name_variations)
        self.logger.info(f"Name variation symbols: {len(name_variations)}")
        
        # Method 4: Sector-based symbol generation
        sector_symbols = self.generate_sector_based_symbols()
        all_symbols.update(sector_symbols)
        self.logger.info(f"Sector-based symbols: {len(sector_symbols)}")
        
        # Method 5: Alphanumeric exploration (for remaining gaps)
        alpha_symbols = self.generate_alphanumeric_combinations()
        all_symbols.update(alpha_symbols)
        self.logger.info(f"Alphanumeric symbols: {len(alpha_symbols)}")
        
        # Clean and validate
        clean_symbols = self.clean_and_validate_symbols(all_symbols)
        self.logger.info(f"Total clean symbols for testing: {len(clean_symbols)}")
        
        return clean_symbols

    def get_high_probability_symbols(self) -> Set[str]:
        """Get symbols with highest probability of being valid"""
        return {
            # Top liquid stocks (99% success rate)
            'RELIANCE', 'TCS', 'HDFCBANK', 'BHARTIARTL', 'ICICIBANK', 'INFOSYS',
            'HINDUNILVR', 'ITC', 'SBIN', 'BAJFINANCE', 'LT', 'HCLTECH', 'ASIANPAINT',
            'MARUTI', 'BAJAJFINSV', 'WIPRO', 'NESTLEIND', 'ULTRACEMCO', 'TITAN',
            'AXISBANK', 'DMART', 'KOTAKBANK', 'SUNPHARMA', 'ONGC', 'NTPC',
            'POWERGRID', 'TECHM', 'TATAMOTORS', 'JSWSTEEL', 'HINDALCO', 'INDUSINDBK',
            'ADANIENT', 'COALINDIA', 'DRREDDY', 'GRASIM', 'CIPLA', 'BRITANNIA',
            'EICHERMOT', 'APOLLOHOSP', 'BPCL', 'DIVISLAB', 'TATASTEEL', 'HEROMOTOCO',
            'BAJAJ-AUTO', 'HDFCLIFE', 'SBILIFE', 'TRENT', 'ADANIPORTS', 'LTIM',
            
            # High probability banks
            'BANKBARODA', 'PNB', 'CANBK', 'UNIONBANK', 'IDFCFIRSTB', 'FEDERALBNK',
            'RBLBANK', 'YESBANK', 'BANDHANBNK', 'EQUITAS', 'UJJIVANSFB', 'SURYODAY',
            'AUBANK', 'DCBBANK', 'KARURBANK', 'CITYUNION', 'SOUTHBANK', 'INDIANBANK',
            
            # IT sector (high success rate)
            'MINDTREE', 'MPHASIS', 'COFORGE', 'PERSISTENT', 'CYIENT', 'KPIT',
            'INTELLECT', 'OFSS', 'RAMCO', 'NEWGEN', 'SONATA', 'ZENSAR',
            'LTTS', 'TATAELXSI', 'NIITLTD', 'SUBEX', 'TANLA', 'ONMOBILE',
            
            # Pharma (high success rate) 
            'AUROPHARMA', 'CADILAHC', 'GLENMARK', 'ALKEM', 'LUPIN', 'BIOCON',
            'TORNTPHARM', 'ABBOTINDIA', 'PFIZER', 'GLAXO', 'SANOFI', 'NOVARTIS',
            'GRANULES', 'SEQUENT', 'STRIDES', 'NEULAND', 'SUVEN', 'LALPATHLAB',
            'THYROCARE', 'METROPOLIS', 'KRSNAA', 'MEDPLUS', 'STARHEALTH',
            
            # Auto sector
            'ASHOKLEY', 'ESCORTS', 'BHARATFORG', 'MOTHERSUMI', 'BOSCHLTD',
            'MRF', 'APOLLOTYRE', 'BALKRISIND', 'CEAT', 'EXIDEIND', 'AMARAJABAT',
            'ENDURANCE', 'SUPRAJIT', 'WABCO', 'MINDA', 'SUNDRAM', 'GABRIEL',
            
            # Energy & Oil
            'IOC', 'HINDPETRO', 'GAIL', 'OIL', 'PETRONET', 'GSPL', 'IGL',
            'MGL', 'TATAPOWER', 'ADANIGREEN', 'ADANIPOWER', 'SUZLON', 'INOXWIND',
            
            # Metals & Mining
            'SAIL', 'VEDL', 'HINDZINC', 'NATIONALUM', 'NMDC', 'JINDALSTEL',
            'JSPL', 'WELCORP', 'RATNAMANI', 'APL', 'MOIL', 'GMDC', 'WELSPUNIND',
            
            # FMCG & Consumer
            'DABUR', 'COLPAL', 'MARICO', 'GODREJCP', 'EMAMILTD', 'UBL',
            'TATACONSUM', 'VBL', 'CCL', 'RADICO', 'MCDOWELL-N', 'JUBLFOOD',
            'DEVYANI', 'WESTLIFE', 'SAPPHIRE', 'BIKAJI', 'DODLA', 'HATSUN',
            
            # Cement & Construction
            'ACC', 'AMBUJACEM', 'SHREECEM', 'RAMCOCEM', 'JKCEMENT', 'HEIDELBERG',
            'INDIACEM', 'ORIENTCEM', 'DLF', 'GODREJPROP', 'OBEROIRLTY', 'PRESTIGE',
            'BRIGADE', 'SOBHA', 'SUNTECK', 'PHOENIXLTD',
            
            # Chemicals & Fertilizers
            'UPL', 'SRF', 'PIDILITIND', 'DEEPAKNTR', 'TATACHEM', 'AARTI',
            'ALKYLAMINE', 'BALRAMCHIN', 'CHAMBLFERT', 'COROMANDEL', 'RCF',
            'GSFC', 'GNFC', 'NFL', 'FACT', 'MANGALAM', 'ZUARI',
            
            # New-age & Recent IPOs
            'ZOMATO', 'PAYTM', 'NYKAA', 'POLICYBZR', 'CARTRADE', 'EASEMYTRIP',
            'DELHIVERY', 'NAZARA', 'ANGELONE', '5PAISA', 'RATEGAIN', 'LATENTVIEW',
            'HAPPSTMNDS', 'ROSSARI', 'CHEMCON', 'IRFC', 'RAILTEL', 'MAZAGON',
            'HAL', 'BDL', 'BEL', 'MIDHANI', 'GRSE', 'BEML', 'INDIAMART', 'ROUTE',
            
            # Additional high-probability stocks
            'DIXON', 'AMBER', 'CROMPTON', 'HAVELLS', 'POLYCAB', 'VGUARD',
            'SYMPHONY', 'VOLTAS', 'BLUESTARCO', 'WHIRLPOOL', 'RAJESHEXPO',
            'AAVAS', 'ABFRL', 'AFFLE', 'AJANTPHARM', 'ALLCARGO', 'ASTERDM',
            'ASTRAL', 'ATUL', 'BAJAJCON', 'BAJAJELECTR', 'BATAINDIA', 'BAYERCROP',
            'BERGEPAINT', 'BHARATISHIP', 'BHEL', 'BLISSGVS', 'BSE', 'BSOFT',
            'CAMS', 'CANFINHOME', 'CARBORUNIV', 'CASTROLIND', 'CEATLTD',
            'CENTRALBK', 'CENTURYPLY', 'CERA', 'CHENNPETRO', 'CHOLAHLDNG',
            'CHOLAFIN', 'COROMANDEL', 'CUMMINSIND', 'DALBHARAT', 'DEEPAKNTR',
            'DELTACORP', 'DHANI', 'EIDPARRY', 'EIHOTEL', 'ELGIEQUIP', 'ENDURANCE',
            'ENGINERSIN', 'EXIDEIND', 'FDC', 'FINEORG', 'FSL', 'GARFIBRES',
            'GICRE', 'GILLETTE', 'GLAND', 'GMRINFRA', 'GODREJIND', 'GOODYEAR',
            'GRAVITA', 'GRINDWELL', 'GUJALKALI', 'GUJGASLTD', 'HATHWAY', 'HCC',
            'HEG', 'HFCL', 'HGINFRA', 'HINDCOPPER', 'HMVL', 'HONAUT', 'HSCL',
            'HUDCO', 'IBREALEST', 'ICICIGI', 'ICICIPRULI', 'IDEA', 'IDFC',
            'IEX', 'IFBIND', 'INDIABULLS', 'INDIACEM', 'INDIANB', 'INDIGO',
            'INDOCO', 'INDOSTAR', 'INDUSTOWER', 'INFIBEAM', 'IOB', 'IPCALAB',
            'IRB', 'IRCTC', 'ISEC', 'ITI', 'JBCHEPHARM', 'JCHAC', 'JKLAKSHMI',
            'JKPAPER', 'JMFINANCIL', 'JSL', 'JSLHISAR', 'JSWENERGY', 'JUBLINGREA',
            'JUSTDIAL', 'JYOTHYLAB', 'KAJARIACER', 'KALPATPOWR', 'KANSAINER',
            'KARURVYSYA', 'KEC', 'KEI', 'KNRCON', 'KPITTECH', 'KRBL', 'LAOPALA',
            'LAURUSLABS', 'LAXMIMACH', 'LICHSGFIN', 'LINDEINDIA', 'LTI', 'MAHABANK',
            'MAHINDCIE', 'MANAPPURAM', 'MCX', 'METKORE', 'MINDSPACE', 'MIDHANI',
            'MOIL', 'MOTILALOFS', 'MRPL', 'MUTHOOTFIN', 'NALCO', 'NAVINFLUOR',
            'NBCC', 'NCC', 'NH', 'NHPC', 'NLCINDIA', 'NOCIL', 'ORIENTBELL',
            'ORIENTELEC', 'PARAGMILK', 'PASHUPATI', 'PEL', 'PETRONET', 'PFIZER',
            'PGHH', 'PIIND', 'PNBHOUSING', 'POLYMED', 'PRAJIND', 'PRSMJOHNSN',
            'PTC', 'PVR', 'QUESS', 'RAIN', 'REDINGTON', 'RELAXO', 'RELCAPITAL',
            'RELINFRA', 'RPOWER', 'RTNPOWER', 'SCI', 'SFL', 'SHANKARA', 'SIS',
            'SJVN', 'SKFINDIA', 'SOLARINDS', 'SONATSOFTW', 'STAR', 'STRTECH',
            'SUDARSCHEM', 'SUNDARMFIN', 'SUNDRMFAST', 'SUNTV', 'SUVEN', 'SYNDIBANK',
            'TATACOMM', 'TATAGLOBAL', 'TEAMLEASE', 'THERMAX', 'TORNTPOWER',
            'TRIDENT', 'TTKPRESTIG', 'TVSMOTOR', 'UCOBANK', 'UJJIVAN', 'VTL',
            'WOCKPHARMA', 'ZENSARTECH', 'ZYDUSLIFE'
        }

    def generate_systematic_patterns(self) -> Set[str]:
        """Generate symbols based on common Indian company patterns"""
        patterns = set()
        
        # Common prefixes and suffixes
        prefixes = ['ASIAN', 'BHARAT', 'HINDU', 'INDIA', 'TATA', 'RELIANCE', 'ADANI', 'BAJAJ', 'GODREJ', 'MAHINDRA']
        suffixes = ['IND', 'LTD', 'CORP', 'INDUSTRIES', 'MOTORS', 'STEEL', 'POWER', 'BANK', 'FINANCE', 'PHARMA']
        
        # Generate combinations
        for prefix in prefixes:
            patterns.add(prefix)
            for suffix in suffixes:
                patterns.add(f"{prefix}{suffix}")
                patterns.add(f"{prefix}_{suffix}")
        
        # Common business words
        business_words = [
            'AUTO', 'MOTORS', 'STEEL', 'POWER', 'ENERGY', 'BANK', 'FINANCE', 'PHARMA',
            'CHEMICALS', 'TEXTILES', 'CEMENT', 'REALTY', 'INFRA', 'TECH', 'SYSTEMS',
            'SOLUTIONS', 'SERVICES', 'PRODUCTS', 'INDUSTRIES', 'ENTERPRISES', 'CORP',
            'LIMITED', 'COMPANY', 'GROUP', 'HOLDINGS', 'INVESTMENTS'
        ]
        
        patterns.update(business_words)
        
        # City/State based companies
        locations = [
            'MUMBAI', 'DELHI', 'BANGALORE', 'CHENNAI', 'KOLKATA', 'PUNE', 'HYDERABAD',
            'GUJARAT', 'RAJASTHAN', 'MAHARASHTRA', 'KARNATAKA', 'TAMILNADU', 'KERALA',
            'PUNJAB', 'HARYANA', 'BIHAR', 'ODISHA', 'WEST', 'EAST', 'NORTH', 'SOUTH'
        ]
        
        for location in locations:
            patterns.add(location)
            patterns.add(f"{location}BANK")
            patterns.add(f"{location}POWER")
        
        return patterns

    def generate_company_name_variations(self) -> Set[str]:
        """Generate variations of well-known company names"""
        base_companies = [
            'RELIANCE', 'TATA', 'BAJAJ', 'ADANI', 'GODREJ', 'MAHINDRA', 'BIRLA',
            'AMBANI', 'MITTAL', 'HINDUJA', 'BHARTI', 'ESSAR', 'VEDANTA', 'JSW',
            'JINDAL', 'ANIL', 'MUKESH', 'RATAN', 'KUMAR', 'ADITYA'
        ]
        
        variations = set()
        
        suffixes = ['INDUSTRIES', 'ENTERPRISES', 'CORP', 'LTD', 'LIMITED', 'GROUP',
                   'HOLDINGS', 'INVESTMENTS', 'FINANCE', 'CAPITAL', 'VENTURES']
        
        for company in base_companies:
            variations.add(company)
            for suffix in suffixes:
                variations.add(f"{company}{suffix}")
                variations.add(f"{company}_{suffix}")
        
        return variations

    def generate_sector_based_symbols(self) -> Set[str]:
        """Generate symbols based on different sectors"""
        sectors = {
            'banking': ['BANK', 'BANKING', 'FINANCIAL', 'FINANCE', 'CAPITAL', 'CREDIT', 'LOAN'],
            'auto': ['AUTO', 'MOTORS', 'CARS', 'BIKES', 'TYRES', 'PARTS', 'COMPONENTS'],
            'pharma': ['PHARMA', 'DRUGS', 'MEDICINE', 'HEALTH', 'CARE', 'MEDICAL', 'BIO'],
            'it': ['TECH', 'SOFTWARE', 'SYSTEMS', 'SOLUTIONS', 'INFO', 'DATA', 'DIGITAL'],
            'energy': ['POWER', 'ENERGY', 'OIL', 'GAS', 'SOLAR', 'WIND', 'COAL'],
            'metals': ['STEEL', 'IRON', 'ALUMINIUM', 'COPPER', 'ZINC', 'METAL', 'MINING'],
            'fmcg': ['FOODS', 'BEVERAGES', 'CONSUMER', 'PRODUCTS', 'GOODS', 'BRANDS'],
            'textiles': ['TEXTILES', 'COTTON', 'FABRIC', 'GARMENTS', 'APPAREL', 'FASHION'],
            'cement': ['CEMENT', 'CONCRETE', 'CONSTRUCTION', 'BUILDING', 'MATERIALS'],
            'chemicals': ['CHEMICALS', 'PETRO', 'FERTILIZER', 'PESTICIDE', 'POLYMER']
        }
        
        sector_symbols = set()
        
        for sector, keywords in sectors.items():
            for keyword in keywords:
                sector_symbols.add(keyword)
                # Add common prefixes
                for prefix in ['INDIAN', 'BHARAT', 'NATIONAL', 'SUPREME', 'UNITED', 'GLOBAL']:
                    sector_symbols.add(f"{prefix}{keyword}")
        
        return sector_symbols

    def generate_alphanumeric_combinations(self) -> Set[str]:
        """Generate alphanumeric combinations for comprehensive coverage"""
        combinations = set()
        
        # 3-letter combinations (common for stock symbols)
        letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
        
        # Generate common 3-letter patterns
        for first in ['A', 'B', 'C', 'D', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'P', 'R', 'S', 'T', 'U', 'V', 'W']:
            for second in ['A', 'E', 'I', 'O', 'U']:  # Vowels as second letter
                for third in letters:
                    if len(f"{first}{second}{third}") == 3:
                        combinations.add(f"{first}{second}{third}")
                    if len(combinations) > 500:  # Limit to avoid too many
                        break
                if len(combinations) > 500:
                    break
            if len(combinations) > 500:
                break
        
        # Add some 4-letter combinations
        common_4letter_patterns = []
        for prefix in ['BANK', 'TECH', 'AUTO', 'PHAR']:
            for suffix in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
                common_4letter_patterns.append(f"{prefix[0]}{prefix[1]}{prefix[2]}{suffix}")
                if len(common_4letter_patterns) > 200:
                    break
        
        combinations.update(common_4letter_patterns[:200])
        
        return combinations

    def clean_and_validate_symbols(self, symbols: Set[str]) -> Set[str]:
        """Clean and validate symbol list"""
        clean_symbols = set()
        
        for symbol in symbols:
            if symbol and isinstance(symbol, str):
                # Clean symbol
                clean_symbol = symbol.strip().upper()
                # Remove special characters except hyphen and underscore
                clean_symbol = ''.join(c for c in clean_symbol if c.isalnum() or c in ['-', '_'])
                
                # Validate length and format
                if 2 <= len(clean_symbol) <= 20 and clean_symbol.replace('-', '').replace('_', '').isalnum():
                    clean_symbols.add(clean_symbol)
        
        return clean_symbols

    def test_symbols_batch(self, symbols_batch: List[str]) -> List[Dict]:
        """Test a batch of symbols and return valid ones with data"""
        valid_stocks = []
        
        with ThreadPoolExecutor(max_workers=min(50, len(symbols_batch))) as executor:
            future_to_symbol = {
                executor.submit(self.test_single_symbol, symbol): symbol 
                for symbol in symbols_batch
            }
            
            for future in as_completed(future_to_symbol, timeout=120):
                symbol = future_to_symbol[future]
                try:
                    result = future.result()
                    if result:
                        valid_stocks.append(result)
                except Exception as e:
                    self.logger.debug(f"Symbol test failed for {symbol}: {str(e)}")
        
        return valid_stocks

    def test_single_symbol(self, symbol: str) -> Optional[Dict]:
        """Test if a single symbol is valid and return its data"""
        for suffix in ['.NS', '.BO']:
            try:
                yahoo_symbol = f"{symbol}{suffix}"
                ticker = yf.Ticker(yahoo_symbol)
                
                # Quick test with minimal data
                hist = ticker.history(period="1d")
                if hist.empty:
                    continue
                
                current_price = hist['Close'].iloc[-1]
                if pd.isna(current_price) or current_price <= 0:
                    continue
                
                # Get volume to ensure it's actively traded
                volume = hist['Volume'].iloc[-1] if not hist['Volume'].empty else 0
                if volume <= 0:
                    continue
                
                previous_close = current_price  # Simplified for speed
                change = 0
                change_percent = 0
                
                # Try to get previous day data
                if len(hist) > 1:
                    previous_close = hist['Close'].iloc[-2]
                    change = current_price - previous_close
                    change_percent = (change / previous_close * 100) if previous_close else 0
                
                return {
                    'symbol': symbol,
                    'yahoo_symbol': yahoo_symbol,
                    'name': symbol,  # Will get proper name later if needed
                    'price': round(float(current_price), 2),
                    'change': round(float(change), 2),
                    'change_percent': round(float(change_percent), 2),
                    'volume': int(volume),
                    'exchange': 'NSE' if suffix == '.NS' else 'BSE',
                    'currency': 'INR',
                    'source': 'Yahoo Finance Discovery',
                    'scraped_at': datetime.now().isoformat()
                }
                
            except Exception:
                continue
                
        return None

    def discover_and_scrape_comprehensively(self) -> pd.DataFrame:
        """Main method to discover symbols and scrape data comprehensively"""
        all_valid_stocks = []
        
        # Get all potential symbols
        print("🔍 Phase 1: Systematic symbol discovery...")
        potential_symbols = self.discover_symbols_systematically()
        print(f"✅ Generated {len(potential_symbols)} potential symbols to test")
        
        # Test symbols in batches
        print(f"\n🧪 Phase 2: Testing symbols for validity...")
        symbols_list = list(potential_symbols)
        batch_size = 200
        total_batches = len(symbols_list) // batch_size + (1 if len(symbols_list) % batch_size else 0)
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(symbols_list))
            batch_symbols = symbols_list[start_idx:end_idx]
            
            print(f"Testing batch {batch_num + 1}/{total_batches} ({len(batch_symbols)} symbols)...")
            
            batch_results = self.test_symbols_batch(batch_symbols)
            all_valid_stocks.extend(batch_results)
            
            if batch_results:
                print(f"✅ Found {len(batch_results)} valid stocks in batch {batch_num + 1}")
            
            # Small delay between batches
            time.sleep(1)
        
        print(f"\n🎯 Phase 3: Discovery complete!")
        print(f"📊 Total valid stocks discovered: {len(all_valid_stocks)}")
        
        return pd.DataFrame(all_valid_stocks) if all_valid_stocks else pd.DataFrame()

    def save_comprehensive_data(self, df: pd.DataFrame, filename: Optional[str] = None) -> str:
        """Save discovered data with analysis"""
        if filename is None:
            filename = f"comprehensive_discovered_stocks_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        os.makedirs('data', exist_ok=True)
        
        # Save main files
        csv_path = f"data/{filename}.csv"
        df.to_csv(csv_path, index=False)
        
        json_path = f"data/{filename}.json"
        df.to_json(json_path, orient='records', indent=2)
        
        # Create comprehensive Excel analysis
        try:
            excel_path = f"data/{filename}.xlsx"
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='All Discovered Stocks', index=False)
                
                if len(df) > 0 and 'change_percent' in df.columns:
                    # Top performers
                    gainers = df.nlargest(50, 'change_percent')
                    gainers.to_excel(writer, sheet_name='Top Gainers', index=False)
                    
                    losers = df.nsmallest(50, 'change_percent')
                    losers.to_excel(writer, sheet_name='Top Losers', index=False)
                
                if 'volume' in df.columns and df['volume'].notna().any():
                    high_volume = df[df['volume'] > 0].nlargest(50, 'volume')
                    high_volume.to_excel(writer, sheet_name='High Volume', index=False)
                
                if 'exchange' in df.columns:
                    # Exchange analysis
                    exchange_summary = df.groupby('exchange').agg({
                        'symbol': 'count',
                        'price': 'mean',
                        'volume': 'sum'
                    }).round(2)
                    exchange_summary.to_excel(writer, sheet_name='Exchange Analysis')
                    
        except ImportError:
            self.logger.warning("openpyxl not available, Excel file not created")
        
        # Save discovery summary
        summary = {
            'discovery_completed_at': datetime.now().isoformat(),
            'total_stocks_discovered': len(df),
            'discovery_method': 'Comprehensive Symbol Discovery',
            'exchanges_covered': df['exchange'].unique().tolist() if 'exchange' in df.columns else [],
            'price_range': {
                'min': float(df['price'].min()) if len(df) > 0 else 0,
                'max': float(df['price'].max()) if len(df) > 0 else 0,
                'avg': float(df['price'].mean()) if len(df) > 0 else 0
            } if 'price' in df.columns else {},
            'volume_stats': {
                'total_volume': int(df['volume'].sum()) if 'volume' in df.columns and df['volume'].notna().any() else 0,
                'avg_volume': float(df['volume'].mean()) if 'volume' in df.columns and df['volume'].notna().any() else 0
            }
        }
        
        summary_path = f"data/{filename}_discovery_summary.json"
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        self.logger.info(f"Comprehensive data saved to {csv_path}")
        return csv_path

def main():
    """Main comprehensive discovery and scraping function"""
    try:
        # Initialize comprehensive scraper
        scraper = ComprehensiveSymbolScraper(delay=0.1, max_workers=100)
        
        print("🚀 COMPREHENSIVE SYMBOL DISCOVERY & STOCK SCRAPER")
        print("🎯 Target: Discover and scrape MAXIMUM possible stocks")
        print("⚡ Using systematic symbol discovery + high-speed testing")
        print("🔍 Method: Pattern generation + validation testing")
        
        # Run comprehensive discovery
        start_time = time.time()
        discovered_stocks = scraper.discover_and_scrape_comprehensively()
        end_time = time.time()
        
        if not discovered_stocks.empty:
            # Clean and process
            discovered_stocks = discovered_stocks.drop_duplicates(subset=['symbol'], keep='first')
            discovered_stocks = discovered_stocks[discovered_stocks['price'] > 0]
            
            # Save results
            csv_file = scraper.save_comprehensive_data(discovered_stocks)
            
            # Success summary
            discovery_time = (end_time - start_time) / 60
            print(f"\n🎉 COMPREHENSIVE DISCOVERY COMPLETED!")
            print(f"⏱️  Total Discovery Time: {discovery_time:.1f} minutes")
            print(f"📊 Stocks Discovered: {len(discovered_stocks)}")
            print(f"💾 Data saved to: {csv_file}")
            
            # Analysis
            if len(discovered_stocks) > 0:
                print(f"\n📈 DISCOVERY ANALYSIS:")
                print(f"   🏢 Total Unique Stocks: {len(discovered_stocks)}")
                
                if 'price' in discovered_stocks.columns:
                    avg_price = discovered_stocks['price'].mean()
                    max_price = discovered_stocks['price'].max()
                    min_price = discovered_stocks['price'].min()
                    print(f"   💰 Price Range: ₹{min_price:.2f} - ₹{max_price:,.2f} (avg: ₹{avg_price:.2f})")
                
                if 'change_percent' in discovered_stocks.columns:
                    gainers = len(discovered_stocks[discovered_stocks['change_percent'] > 0])
                    losers = len(discovered_stocks[discovered_stocks['change_percent'] < 0])
                    print(f"   📊 Market: {gainers} gainers, {losers} losers")
                
                if 'exchange' in discovered_stocks.columns:
                    exchanges = discovered_stocks['exchange'].value_counts()
                    print(f"   🏛️  Exchanges: {dict(exchanges)}")
                
                # Show sample discoveries
                print(f"\n🔍 SAMPLE DISCOVERED STOCKS:")
                sample_stocks = discovered_stocks.head(10)
                for _, stock in sample_stocks.iterrows():
                    print(f"   📈 {stock['symbol']:10s}: ₹{stock['price']:8.2f} ({stock['change_percent']:+5.2f}%) [{stock['exchange']}]")
                
                if len(discovered_stocks) >= 1000:
                    print(f"\n🎯 TARGET ACHIEVED: 1000+ stocks discovered!")
                elif len(discovered_stocks) >= 500:
                    print(f"\n📊 Great success: 500+ stocks discovered!")
                else:
                    print(f"\n✅ Discovery completed with {len(discovered_stocks)} stocks")
            
            return True
            
        else:
            print("❌ No stocks discovered")
            return False
            
    except Exception as e:
        print(f"❌ Discovery error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
