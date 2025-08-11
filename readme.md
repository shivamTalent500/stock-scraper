# 5paisa Stock Scraper

This repository automatically scrapes stock data from 5paisa.com daily using GitHub Actions.

## 🚀 Features

- **Automated Daily Scraping**: Runs every day at 6:00 PM IST
- **Multi-format Output**: Saves data as CSV, JSON, and Excel
- **Error Handling**: Robust error handling with detailed logging
- **Data Validation**: Removes duplicates and validates data quality
- **Multiple Data Sources**: Tries API endpoints first, falls back to HTML scraping
- **Pagination Support**: Automatically handles multi-page data
- **Manual Trigger**: Can be triggered manually from GitHub Actions tab

## 📊 Data Output

The scraper collects the following information for each stock:
- Stock Symbol
- Company Name  
- Current Price (LTP)
- Price Change
- Percentage Change
- Volume
- Day High/Low
- Opening Price
- Market Cap (if available)
- PE Ratio (if available)

## 📁 File Structure

```
├── .github/workflows/
│   └── daily-stock-scraper.yml    # GitHub Actions workflow
├── data/                          # Generated data files (created by scraper)
│   ├── 5paisa_stocks_YYYYMMDD_HHMMSS.csv
│   ├── 5paisa_stocks_YYYYMMDD_HHMMSS.json
│   └── 5paisa_stocks_YYYYMMDD_HHMMSS_summary.json
├── stock_scraper.py              # Main scraper script
├── requirements.txt              # Python dependencies
├── stock_scraper.log            # Scraper logs
└── README.md                    # This file
```

## 🔧 Setup Instructions

### 1. Fork or Clone this Repository

```bash
git clone https://github.com/yourusername/stock-scraper.git
cd stock-scraper
```

### 2. Enable GitHub Actions

1. Go to your repository on GitHub
2. Click the "Actions" tab
3. If prompted, click "I understand my workflows, go ahead and enable them"

### 3. Manual Test Run

1. Go to Actions tab → "Daily Stock Scraper"
2. Click "Run workflow" → "Run workflow"
3. Wait for completion and check the results

## 📈 Accessing Data

### Method 1: Download Artifacts
1. Go to Actions tab
2. Click on a completed workflow run
3. Scroll down to "Artifacts" section
4. Download "latest-stock-data" or specific dated artifacts

### Method 2: Check Repository (if enabled)
Data files are automatically committed to the `data/` folder (optional feature)

### Method 3: Use GitHub API
```bash
# Get latest release artifacts via API
curl -H "Accept: application/vnd.github.v3+json" \
     https://api.github.com/repos/yourusername/stock-scraper/actions/artifacts
```

## ⚙️ Configuration

### Change Schedule
Edit `.github/workflows/daily-stock-scraper.yml`:
```yaml
schedule:
  - cron: '30 12 * * *'  # Current: 6:00 PM IST daily
```

Use [crontab.guru](https://crontab.guru) to generate custom schedules.

### Adjust Scraping Parameters
Edit `stock_scraper.py`:
```python
# Change delay between requests
scraper = FivePaisaStockScraper(delay=2.0)  # 2 seconds instead of 1

# Change timeout
response = self.session.get(url, timeout=45)  # 45 seconds
```

## 📋 Local Development

### Run Locally
```bash
# Install dependencies
pip install -r requirements.txt

# Run scraper
python stock_scraper.py

# Check results
ls data/
```

### Test Specific Functions
```python
from stock_scraper import FivePaisaStockScraper

scraper = FivePaisaStockScraper()

# Test single page
soup = scraper.get_page_content("https://www.5paisa.com/stocks/all")
stocks = scraper.extract_stock_data_from_table(soup)
print(f"Found {len(stocks)} stocks")
```

## 🔍 Monitoring & Troubleshooting

### Check Workflow Status
- Green ✅: Successful run
- Red ❌: Failed run (check logs)
- Yellow ⚠️: In progress

### Common Issues & Solutions

1. **No data scraped**
   - Website structure may have changed
   - Check logs for specific errors
   - Test the scraper locally

2. **Rate limiting**
   - Increase delay in scraper
   - Website may be blocking automated requests

3. **Workflow fails**
   - Check Python dependencies
   - Verify website accessibility
   - Review error logs in Actions tab

### View Detailed Logs
1. Go to Actions tab
2. Click on failed/completed run  
3. Expand job steps to see detailed logs

## 📊 Data Analysis Examples

### Using Python/Pandas
```python
import pandas as pd

# Load latest data
df = pd.read_csv('data/latest_stocks.csv')

# Top 10 gainers
top_gainers = df.nlargest(10, 'change_percent')

# Market summary
print(f"Total stocks: {len(df)}")
print(f"Average change: {df['change_percent'].mean():.2f}%")
```

### Using Excel
1. Download CSV from artifacts
2. Open in Excel/Google Sheets
3. Create pivot tables and charts

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Test your changes locally
4. Submit a pull request

## ⚠️ Legal Notice

- This scraper is for educational and personal use only
- Always respect website terms of service
- Consider using official APIs when available
- Be mindful of scraping frequency and server load

## 📧 Support

If you encounter issues:
1. Check the troubleshooting section above
2. Review workflow logs in GitHub Actions
3. Test the scraper locally first
4. Create an issue with detailed error information

## 🔄 Workflow Status

| Status | Description |
|--------|-------------|
| ![Scraper Status](https://github.com/yourusername/stock-scraper/workflows/Daily%20Stock%20Scraper/badge.svg) | Latest run status |

---

**Happy Stock Tracking! 📈**
