import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import json
from datetime import datetime
import logging
import os
from typing import List, Dict, Optional
import re

class FivePaisaStockScraper:
    def __init__(self, delay: float = 1.0):
        """
        Initialize the scraper with configurable delay between requests
        
        Args:
            delay: Delay in seconds between requests to be respectful to the server
        """
        self.base_url = "https://www.5paisa.com"
        self.stocks_url = "https://www.5paisa.com/stocks/all"
        self.delay = delay
        self.session = requests.Session()
        
        # Set up headers to mimic a real browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        })
        
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('stock_scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def get_page_content(self, url: str) -> Optional[BeautifulSoup]:
        """
        Fetch and parse a web page with retry logic
        
        Args:
            url: URL to fetch
            
        Returns:
            BeautifulSoup object or None if failed
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.logger.info(f"Fetching {url} (attempt {attempt + 1})")
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                return soup
                
            except requests.RequestException as e:
                self.logger.error(f"Error fetching {url} (attempt {attempt + 1}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                
        return None

    def extract_stock_data_from_api(self) -> List[Dict]:
        """
        Try to extract data from API endpoints that the website might use
        """
        api_endpoints = [
            "https://www.5paisa.com/api/v1/stocks/all",
            "https://www.5paisa.com/api/stocks/list",
            "https://api.5paisa.com/v1/stocks"
        ]
        
        for endpoint in api_endpoints:
            try:
                self.logger.info(f"Trying API endpoint: {endpoint}")
                response = self.session.get(endpoint, timeout=15)
                if response.status_code == 200:
                    data = response.json()
                    if isinstance(data, list) and len(data) > 0:
                        self.logger.info(f"Found {len(data)} stocks from API")
                        return self.process_json_stock_data(data)
                    elif isinstance(data, dict) and 'data' in data:
                        stocks = data['data']
                        if isinstance(stocks, list) and len(stocks) > 0:
                            self.logger.info(f"Found {len(stocks)} stocks from API")
                            return self.process_json_stock_data(stocks)
            except Exception as e:
                self.logger.debug(f"API endpoint {endpoint} failed: {str(e)}")
                continue
        
        return []

    def extract_stock_data_from_table(self, soup: BeautifulSoup) -> List[Dict]:
        """
        Extract stock data from the HTML page
        
        Args:
            soup: BeautifulSoup object of the page
            
        Returns:
            List of dictionaries containing stock data
        """
        stocks_data = []
        
        try:
            # First try to find data in script tags (JSON data)
            script_tags = soup.find_all('script')
            for script in script_tags:
                if script.string and ('stock' in script.string.lower() or 'symbol' in script.string.lower()):
                    try:
                        # Look for JSON arrays or objects
                        json_patterns = [
                            r'stockData\s*[:=]\s*(\[.*?\])',
                            r'stocks\s*[:=]\s*(\[.*?\])',
                            r'data\s*[:=]\s*(\[.*?\])',
                            r'window\.__INITIAL_STATE__\s*=\s*(\{.*?\})',
                            r'(\[{".*?".*?\}\])',
                            r'(\{".*?".*?\})'
                        ]
                        
                        for pattern in json_patterns:
                            matches = re.findall(pattern, script.string, re.DOTALL)
                            for match in matches:
                                try:
                                    data = json.loads(match)
                                    if isinstance(data, list) and len(data) > 0:
                                        processed_data = self.process_json_stock_data(data)
                                        if processed_data:
                                            return processed_data
                                    elif isinstance(data, dict):
                                        # Look for stock arrays within the object
                                        for key, value in data.items():
                                            if isinstance(value, list) and len(value) > 0:
                                                processed_data = self.process_json_stock_data(value)
                                                if processed_data:
                                                    return processed_data
                                except json.JSONDecodeError:
                                    continue
                    except Exception as e:
                        continue
            
            # If no JSON data found, try to extract from HTML tables
            tables = soup.find_all('table')
            for table in tables:
                rows = table.find_all('tr')
                if len(rows) < 2:  # Need at least header and one data row
                    continue
                
                headers = []
                header_row = rows[0]
                for th in header_row.find_all(['th', 'td']):
                    headers.append(th.get_text(strip=True))
                
                if not headers:
                    continue
                
                # Extract data rows
                for row in rows[1:]:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:  # Need at least symbol and one other field
                        stock_data = {}
                        for i, cell in enumerate(cells):
                            header = headers[i] if i < len(headers) else f'column_{i}'
                            text = cell.get_text(strip=True)
                            
                            # Try to extract links if present
                            link = cell.find('a')
                            if link and link.get('href'):
                                stock_data[f'{header}_link'] = link['href']
                            
                            stock_data[header] = text
                        
                        # Only add if we have meaningful data
                        if any(key.lower() in ['symbol', 'name', 'price', 'ltp'] for key in stock_data.keys()):
                            stocks_data.append(stock_data)
            
            # Try to find stock data in div elements with specific classes
            if not stocks_data:
                stock_divs = soup.find_all('div', {'class': re.compile(r'stock|symbol|price', re.I)})
                for div in stock_divs[:10]:  # Check first 10 matches
                    text = div.get_text(strip=True)
                    if len(text) > 3 and len(text) < 50:  # Reasonable length for stock symbol
                        stocks_data.append({'symbol': text, 'source': 'div_extraction'})
            
            return stocks_data
            
        except Exception as e:
            self.logger.error(f"Error extracting stock data: {str(e)}")
            return []

    def process_json_stock_data(self, data: List[Dict]) -> List[Dict]:
        """
        Process JSON stock data and standardize the format
        
        Args:
            data: Raw JSON data
            
        Returns:
            Processed stock data
        """
        processed_data = []
        
        for item in data:
            if isinstance(item, dict):
                stock_data = {}
                
                # Common field mappings for Indian stock websites
                field_mappings = {
                    'symbol': ['symbol', 'Symbol', 'SYMBOL', 'stock_symbol', 'stockSymbol', 'scrip_cd', 'code', 'ticker'],
                    'name': ['name', 'Name', 'NAME', 'company_name', 'companyName', 'title', 'long_name', 'fullName'],
                    'price': ['price', 'Price', 'PRICE', 'ltp', 'LTP', 'last_price', 'lastPrice', 'close', 'Close'],
                    'change': ['change', 'Change', 'CHANGE', 'price_change', 'priceChange', 'net_change', 'netChange'],
                    'change_percent': ['change_percent', 'changePercent', 'CHANGE_PER', 'change_per', 'changePer', 'pct_change'],
                    'volume': ['volume', 'Volume', 'VOLUME', 'traded_volume', 'tradedVolume', 'vol'],
                    'high': ['high', 'High', 'HIGH', 'day_high', 'dayHigh', 'high_price'],
                    'low': ['low', 'Low', 'LOW', 'day_low', 'dayLow', 'low_price'],
                    'open': ['open', 'Open', 'OPEN', 'day_open', 'dayOpen', 'open_price'],
                    'market_cap': ['market_cap', 'marketCap', 'MARKET_CAP', 'mcap'],
                    'pe_ratio': ['pe_ratio', 'peRatio', 'PE_RATIO', 'pe', 'PE']
                }
                
                # Map the fields
                for standard_key, possible_keys in field_mappings.items():
                    for key in possible_keys:
                        if key in item:
                            value = item[key]
                            # Clean numeric values
                            if isinstance(value, str) and standard_key in ['price', 'change', 'change_percent', 'volume', 'high', 'low', 'open']:
                                value = re.sub(r'[^\d.-]', '', value)
                                try:
                                    value = float(value) if value else None
                                except ValueError:
                                    pass
                            stock_data[standard_key] = value
                            break
                
                # Only add if we have at least symbol or name
                if stock_data.get('symbol') or stock_data.get('name'):
                    processed_data.append(stock_data)
        
        return processed_data

    def find_pagination_info(self, soup: BeautifulSoup) -> Dict:
        """
        Find pagination information on the page
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            Dictionary with pagination info
        """
        pagination_info = {
            'has_next': False,
            'next_url': None,
            'total_pages': 1,
            'current_page': 1
        }
        
        try:
            # Look for pagination elements
            pagination_selectors = [
                'div[class*="pagin"]',
                'div[class*="page"]',
                'nav[class*="pagin"]',
                'ul[class*="pagin"]',
                '.pagination',
                '.page-nav'
            ]
            
            pagination = None
            for selector in pagination_selectors:
                pagination = soup.select_one(selector)
                if pagination:
                    break
            
            if pagination:
                # Look for next button
                next_selectors = [
                    'a[class*="next"]',
                    'a[aria-label*="next"]',
                    'a:contains("Next")',
                    'a:contains(">")',
                    'button[class*="next"]'
                ]
                
                for selector in next_selectors:
                    next_btn = pagination.select_one(selector)
                    if next_btn and next_btn.get('href'):
                        pagination_info['has_next'] = True
                        next_url = next_btn['href']
                        if not next_url.startswith('http'):
                            next_url = self.base_url + next_url
                        pagination_info['next_url'] = next_url
                        break
        
        except Exception as e:
            self.logger.error(f"Error finding pagination info: {str(e)}")
        
        return pagination_info

    def scrape_all_stocks(self) -> pd.DataFrame:
        """
        Scrape all stocks from all pages
        
        Returns:
            DataFrame containing all stock data
        """
        all_stocks = []
        
        self.logger.info("Starting stock data scraping...")
        
        # First try API endpoints
        api_stocks = self.extract_stock_data_from_api()
        if api_stocks:
            all_stocks.extend(api_stocks)
            self.logger.info(f"Found {len(api_stocks)} stocks from API")
        
        # If no API data, scrape HTML pages
        if not all_stocks:
            current_url = self.stocks_url
            page_num = 1
            max_pages = 50  # Safety limit
            
            while current_url and page_num <= max_pages:
                self.logger.info(f"Scraping page {page_num}: {current_url}")
                
                # Add delay between requests
                if page_num > 1:
                    time.sleep(self.delay)
                
                soup = self.get_page_content(current_url)
                if not soup:
                    self.logger.error(f"Failed to fetch page {page_num}")
                    break
                
                # Extract stock data from current page
                page_stocks = self.extract_stock_data_from_table(soup)
                
                if page_stocks:
                    all_stocks.extend(page_stocks)
                    self.logger.info(f"Found {len(page_stocks)} stocks on page {page_num}")
                else:
                    self.logger.warning(f"No stock data found on page {page_num}")
                    # If first page has no data, something is wrong
                    if page_num == 1:
                        break
                
                # Find next page
                pagination_info = self.find_pagination_info(soup)
                
                if pagination_info['has_next'] and pagination_info['next_url']:
                    current_url = pagination_info['next_url']
                    page_num += 1
                else:
                    self.logger.info("No more pages to scrape")
                    break
        
        self.logger.info(f"Scraping completed. Total stocks found: {len(all_stocks)}")
        
        if all_stocks:
            df = pd.DataFrame(all_stocks)
            
            # Add metadata
            df['scraped_at'] = datetime.now().isoformat()
            df['source'] = '5paisa.com'
            
            # Remove duplicates if any (based on symbol)
            if 'symbol' in df.columns:
                initial_count = len(df)
                df = df.drop_duplicates(subset=['symbol'], keep='first')
                if len(df) < initial_count:
                    self.logger.info(f"Removed {initial_count - len(df)} duplicate entries")
            
            return df
        else:
            self.logger.warning("No stocks data found - creating empty DataFrame")
            return pd.DataFrame()

    def save_data(self, df: pd.DataFrame, filename: Optional[str] = None) -> str:
        """
        Save the scraped data to various formats
        
        Args:
            df: DataFrame to save
            filename: Base filename (without extension)
            
        Returns:
            Path to saved CSV file
        """
        if filename is None:
            filename = f"5paisa_stocks_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Ensure data directory exists
        os.makedirs('data', exist_ok=True)
        
        # Save as CSV
        csv_path = f"data/{filename}.csv"
        df.to_csv(csv_path, index=False)
        self.logger.info(f"Data saved to {csv_path}")
        
        # Save as JSON
        json_path = f"data/{filename}.json"
        df.to_json(json_path, orient='records', indent=2)
        self.logger.info(f"Data saved to {json_path}")
        
        # Save summary
        summary = {
            "total_records": len(df),
            "columns": list(df.columns),
            "scraped_at": datetime.now().isoformat(),
            "file_size_mb": round(os.path.getsize(csv_path) / (1024*1024), 2)
        }
        
        summary_path = f"data/{filename}_summary.json"
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        return csv_path

    def get_summary_stats(self, df: pd.DataFrame) -> Dict:
        """
        Get summary statistics of the scraped data
        
        Args:
            df: DataFrame with stock data
            
        Returns:
            Dictionary with summary statistics
        """
        if df.empty:
            return {"total_stocks": 0, "status": "no_data"}
        
        stats = {
            "total_stocks": len(df),
            "timestamp": datetime.now().isoformat(),
            "columns": list(df.columns),
            "status": "success"
        }
        
        # Add sample data
        if len(df) > 0:
            stats["sample_data"] = df.head(3).to_dict('records')
        
        # Add column statistics
        stats["column_info"] = {}
        for col in df.columns:
            non_null_count = df[col].notna().sum()
            stats["column_info"][col] = {
                "non_null_count": int(non_null_count),
                "null_count": int(len(df) - non_null_count),
                "data_type": str(df[col].dtype)
            }
        
        return stats

def main():
    """
    Main function to run the scraper
    """
    scraper = FivePaisaStockScraper(delay=1.0)
    
    try:
        # Scrape all stock data
        stock_data = scraper.scrape_all_stocks()
        
        if not stock_data.empty:
            # Save the data
            csv_file = scraper.save_data(stock_data)
            
            # Print summary
            stats = scraper.get_summary_stats(stock_data)
            print(f"\n✅ Scraping Summary:")
            print(f"Total stocks scraped: {stats['total_stocks']}")
            print(f"Data saved to: {csv_file}")
            print(f"Columns found: {len(stats['columns'])}")
            print(f"Status: {stats['status']}")
            
            # Show sample data
            if stats.get('sample_data'):
                print("\n📊 Sample data:")
                for i, stock in enumerate(stats['sample_data'][:3]):
                    print(f"{i+1}. {stock}")
            
            return True
        
        else:
            print("❌ No data was scraped. Please check the website structure or network connection.")
            return False
    
    except Exception as e:
        print(f"❌ An error occurred: {str(e)}")
        logging.error(f"Main execution error: {str(e)}")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
