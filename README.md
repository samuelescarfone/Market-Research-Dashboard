# Company Catalyst Dashboard

A clean Flask dashboard that ingests regulatory events (NHTSA recalls, FDA adverse events, SEC filings) and overlays them on stock price charts to identify potential trading catalysts.

## Features

- **Two-stage data pipeline**: Backfill historical data (12-24 months) + daily incremental updates
- **Multiple data sources**: NHTSA, FDA (drug/device), SEC EDGAR filings
- **Visual correlation**: Events overlaid on stock price charts
- **Clean interface**: Minimal Flask frontend with Chart.js visualization

## Quick Start

1. **Install dependencies**:
```bash
pip install -r requirements.txt
```

2. **Update configuration**:
Edit `ingest.py` and change `SEC_USER_AGENT_EMAIL` to your email address (required by SEC API).

3. **Run initial backfill**:
```bash
python ingest.py backfill 12  # 12 months of historical data
```

4. **Start the dashboard**:
```bash
python app.py
```

5. **View dashboard**:
Open http://localhost:3000 in your browser.

## Data Pipeline

### Backfill (Historical)
```bash
python ingest.py backfill [months]  # Default: 12 months
```

### Incremental (Daily)
```bash
python ingest.py  # Run daily via cron
```

### Cron Setup
Add to crontab for daily updates:
```bash
0 6 * * * cd /path/to/project && python ingest.py
```

## Data Sources

- **NHTSA**: Vehicle recalls via `api.nhtsa.gov/recalls/recallsByVehicle`
- **FDA Drug**: Adverse events via `api.fda.gov/drug/event.json`
- **FDA Device**: Device events via `api.fda.gov/device/event.json`
- **SEC**: 8-K and Form 4 filings via `data.sec.gov/submissions/`
- **Stock Data**: Yahoo Finance via yfinance library

## Database Schema

SQLite database with unified event schema:
```sql
CREATE TABLE events (
    id INTEGER PRIMARY KEY,
    timestamp TEXT,      -- ISO format
    source TEXT,         -- NHTSA, FDA-DRUG, FDA-DEVICE, SEC
    ticker TEXT,         -- Stock symbol
    company TEXT,        -- Company name
    event_text TEXT,     -- Event description
    link TEXT,           -- Source URL
    created_at TIMESTAMP
);
```

## Configuration

Edit `companies.csv` or modify the default companies in `ingest.py`:
```csv
ticker,company,cik
TSLA,Tesla,0001318605
AAPL,Apple,0000320193
MRK,Merck,0000310158
JNJ,Johnson & Johnson,0000200406
```

## API Endpoints

- `GET /` - Dashboard interface
- `GET /api/companies` - List tracked companies
- `GET /api/events/<ticker>` - Events for ticker
- `GET /api/stock/<ticker>` - Stock price data
- `GET /api/chart-data/<ticker>` - Combined stock + events data

## Usage Tips

1. **SEC API**: Requires valid email in User-Agent header
2. **Rate limiting**: Built-in delays between API calls
3. **Deduplication**: Events are deduplicated by timestamp + source + ticker + text
4. **Date normalization**: Handles various date formats from different APIs
5. **Error handling**: Graceful failures for individual API calls

## Next Steps

- Switch to PostgreSQL for production
- Add more data sources (Twitter, news APIs)
- Implement alert system for high-impact events
- Add options pricing data integration
- Build backtesting framework for event-driven strategies