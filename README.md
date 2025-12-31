# Automated Nifty Index Data Fetcher

This project provides automated scripts to fetch Nifty index data without manual cookie management.

## Setup

1. Create and activate a virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate
```

2. Install the required dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Fetch All Indices (Recommended)
```bash
source venv/bin/activate
cd scripts
python3 automated_index_fetcher.py
```

This will:
- Automatically get fresh cookies
- Fetch data for all indices in `index list.json`
- Save data to `../index data/` directory
- Handle retries and errors automatically

### Fetch Single Index
```bash
source venv/bin/activate
cd scripts
python3 fetch_single_index.py "NIFTY 50"
```

## Features

✅ **Automatic Cookie Management**: No more manual cookie updates!  
✅ **Retry Logic**: Automatically retries failed requests  
✅ **Error Handling**: Graceful handling of server errors  
✅ **Progress Tracking**: Shows progress and success/failure counts  
✅ **Data Validation**: Checks for empty responses and retries  
✅ **Respectful Scraping**: Includes delays between requests  

## Troubleshooting

If you encounter issues:
1. Check your internet connection
2. Verify that the Nifty Indices website is accessible
3. Try running the script again (cookies will be refreshed automatically)

## Output

Data is saved as JSON files in the `../index data/` directory with the same naming convention as before. 