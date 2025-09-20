#!/usr/bin/env python3
"""
Data Ingestion Pipeline for Company Catalyst Events
Two-stage: backfill (12-24 months) + incremental (daily)
"""

import sqlite3
import requests
import json
import re
import csv
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional
import time
import os

# Configuration
SEC_USER_AGENT_EMAIL = "your-email@example.com"  # Update this!
FDA_LIMIT_PER_QUERY = 50
REQUEST_TIMEOUT = 30

class EventIngester:
    def __init__(self, db_path='events.db'):
        self.db_path = db_path
        self.init_db()
    
    def init_db(self):
        """Initialize database"""
        conn = sqlite3.connect(self.db_path)
        conn.execute('''
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                source TEXT NOT NULL,
                ticker TEXT NOT NULL,
                company TEXT NOT NULL,
                event_text TEXT NOT NULL,
                link TEXT,
                insider_name TEXT,
                transaction_details TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(timestamp, source, ticker, event_text)
            )
        ''')
        conn.commit()
        conn.close()
    
    def add_event(self, timestamp, source, ticker, company, event_text, link=None, insider_name=None, transaction_details=None):
        """Add event to database with deduplication"""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute('''
                INSERT OR IGNORE INTO events 
                (timestamp, source, ticker, company, event_text, link, insider_name, transaction_details)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (timestamp, source, ticker.upper(), company, event_text, link, insider_name, transaction_details))
            conn.commit()
        except Exception as e:
            print(f"Error adding event: {e}")
        finally:
            conn.close()
    
    def normalize_date(self, date_str: str) -> Optional[str]:
        """Normalize various date formats to ISO format"""
        if not date_str:
            return None
        
        date_str = date_str.strip()
        formats = [
            "%Y-%m-%d",
            "%Y%m%d", 
            "%m/%d/%Y",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%fZ"
        ]
        
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.date().isoformat() + "T00:00:00Z"
            except ValueError:
                continue
        
        # Try to extract YYYYMMDD from string
        match = re.search(r'\b(20\d{2})(\d{2})(\d{2})\b', date_str)
        if match:
            y, m, d = match.groups()
            return f"{y}-{m}-{d}T00:00:00Z"
        
        return None
    
    def is_within_range(self, date_str: str, days_back: int) -> bool:
        """Check if date is within specified range"""
        if not date_str:
            return False
        
        try:
            event_date = datetime.fromisoformat(date_str.replace('Z', '')).date()
            cutoff_date = date.today() - timedelta(days=days_back)
            return event_date >= cutoff_date
        except:
            return False
    
    def fetch_nhtsa_recalls(self, company_name: str, days_back: int = 30) -> List[Dict]:
        """Fetch NHTSA recalls for company"""
        url = "https://api.nhtsa.gov/recalls/recallsByVehicle"
        
        # Try multiple years to get more comprehensive results
        current_year = date.today().year
        years_to_search = [str(year) for year in range(current_year - 5, current_year + 1)]
        
        all_recalls = []
        
        for year in years_to_search:
            try:
                params = {"make": company_name.upper(), "modelYear": year}
                response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    for recall in data.get("results", []):
                        recall_date = self.normalize_date(recall.get("ReportReceivedDate"))
                        if recall_date and self.is_within_range(recall_date, days_back):
                            component = recall.get('Component', 'Unknown Component')
                            summary = recall.get('Summary', '')[:200]
                            
                            all_recalls.append({
                                'timestamp': recall_date,
                                'event_text': f"Recall: {component} - {summary}{'...' if len(recall.get('Summary', '')) > 200 else ''}",
                                'link': f"https://www.nhtsa.gov/recalls"
                            })
                
            except Exception as e:
                print(f"NHTSA fetch failed for {company_name} year {year}: {e}")
                continue
        
        return all_recalls
    
    def fetch_fda_drug_events(self, company_name: str, days_back: int = 30) -> List[Dict]:
        """Fetch FDA drug adverse events"""
        url = "https://api.fda.gov/drug/event.json"
        
        # Calculate date range for search
        end_date = date.today()
        start_date = end_date - timedelta(days=days_back)
        date_range = f"[{start_date.strftime('%Y%m%d')} TO {end_date.strftime('%Y%m%d')}]"
        
        # Try multiple search strategies
        search_terms = [
            f'patient.drug.medicinalproduct:"{company_name}" AND receivedate:{date_range}',
            f'patient.drug.openfda.manufacturer_name:"{company_name}" AND receivedate:{date_range}',
            f'receivedate:{date_range} AND patient.drug.medicinalproduct:*{company_name}*'
        ]
        
        all_events = []
        
        for search_term in search_terms:
            try:
                params = {"search": search_term, "limit": FDA_LIMIT_PER_QUERY}
                response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    for event in data.get("results", []):
                        event_date = self.normalize_date(event.get("receivedate"))
                        if event_date and self.is_within_range(event_date, days_back):
                            reactions = event.get("patient", {}).get("reaction", [])
                            reaction = reactions[0].get("reactionmeddrapt", "Adverse Event") if reactions else "Adverse Event"
                            
                            all_events.append({
                                'timestamp': event_date,
                                'event_text': f"Drug Adverse Event: {reaction}",
                                'link': "https://open.fda.gov/apis/"
                            })
                    
                    # If we found events with this search term, don't try others
                    if data.get("results"):
                        break
                        
            except Exception as e:
                print(f"FDA drug search failed for '{search_term}': {e}")
                continue
        
        return all_events
    
    def fetch_fda_device_events(self, company_name: str, days_back: int = 30) -> List[Dict]:
        """Fetch FDA device adverse events"""
        url = "https://api.fda.gov/device/event.json"
        
        # Calculate date range for search
        end_date = date.today()
        start_date = end_date - timedelta(days=days_back)
        date_range = f"[{start_date.strftime('%Y%m%d')} TO {end_date.strftime('%Y%m%d')}]"
        
        # Try multiple search strategies
        search_terms = [
            f'manufacturer_d_name:"{company_name}" AND date_received:{date_range}',
            f'manufacturer_d_name:*{company_name}* AND date_received:{date_range}',
            f'date_received:{date_range} AND manufacturer_d_name:"{company_name}"'
        ]
        
        all_events = []
        
        for search_term in search_terms:
            try:
                params = {"search": search_term, "limit": FDA_LIMIT_PER_QUERY}
                response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    for event in data.get("results", []):
                        event_date = self.normalize_date(event.get("date_received"))
                        if event_date and self.is_within_range(event_date, days_back):
                            event_type = event.get("event_type", "Device Event")
                            device_name = event.get("device", [{}])[0].get("generic_name", "Medical Device") if event.get("device") else "Medical Device"
                            
                            all_events.append({
                                'timestamp': event_date,
                                'event_text': f"Device Adverse Event: {event_type} - {device_name}",
                                'link': "https://open.fda.gov/apis/"
                            })
                    
                    # If we found events with this search term, don't try others
                    if data.get("results"):
                        break
                        
            except Exception as e:
                print(f"FDA device search failed for '{search_term}': {e}")
                continue
        
        return all_events
    
    def parse_form4_content(self, filing_url: str, headers: dict) -> Dict:
        """Parse Form 4 filing to extract insider trading details"""
        try:
            # Get the filing content
            response = requests.get(filing_url, headers=headers, timeout=REQUEST_TIMEOUT)
            if response.status_code != 200:
                return {}
            
            content = response.text
            
            # Extract key information using regex patterns
            insider_info = {}
            
            # Extract insider name - try multiple patterns
            name_patterns = [
                r'<rptOwnerName>([^<]+)</rptOwnerName>',
                r'<reportingOwnerName>([^<]+)</reportingOwnerName>',
                r'<name>([^<]+)</name>',
                r'OWNER:\s*([^\n]+)',
            ]
            
            for pattern in name_patterns:
                match = re.search(pattern, content, re.IGNORECASE)
                if match:
                    insider_info['insider_name'] = match.group(1).strip()
                    break
            
            # Extract transaction details - look for non-derivative transactions first
            shares_match = re.search(r'<transactionShares>.*?<value>([^<]+)</value>.*?</transactionShares>', content, re.DOTALL)
            price_match = re.search(r'<transactionPricePerShare>.*?<value>([^<]+)</value>.*?</transactionPricePerShare>', content, re.DOTALL)
            action_match = re.search(r'<transactionAcquiredDisposedCode>.*?<value>([^<]+)</value>.*?</transactionAcquiredDisposedCode>', content, re.DOTALL)
            
            # Alternative patterns for simpler XML structure
            if not shares_match:
                shares_match = re.search(r'<transactionShares>([^<]+)</transactionShares>', content)
            if not price_match:
                price_match = re.search(r'<transactionPricePerShare>([^<]+)</transactionPricePerShare>', content)
            if not action_match:
                action_match = re.search(r'<transactionAcquiredDisposedCode>([^<]+)</transactionAcquiredDisposedCode>', content)
            
            if shares_match:
                shares = shares_match.group(1).strip()
                price = price_match.group(1).strip() if price_match else "N/A"
                action = action_match.group(1).strip() if action_match else "N/A"
                
                # A = Acquired, D = Disposed
                action_text = "Bought" if action == "A" else "Sold" if action == "D" else action
                
                try:
                    shares_num = float(shares.replace(',', ''))
                    if price != "N/A" and price.replace('.', '').replace(',', '').isdigit():
                        price_num = float(price.replace(',', ''))
                        value = shares_num * price_num
                        insider_info['transaction'] = f"{action_text} {shares_num:,.0f} shares at ${price_num:.2f} (${value:,.0f})"
                    else:
                        insider_info['transaction'] = f"{action_text} {shares_num:,.0f} shares"
                except ValueError:
                    insider_info['transaction'] = f"{action_text} {shares} shares"
            
            # If no transaction found, look for derivative transactions
            if 'transaction' not in insider_info:
                derivative_shares = re.search(r'<derivativeTransaction>.*?<transactionShares>.*?<value>([^<]+)</value>', content, re.DOTALL)
                if derivative_shares:
                    insider_info['transaction'] = f"Derivative transaction: {derivative_shares.group(1)} shares"
            
            return insider_info
            
        except Exception as e:
            print(f"Error parsing Form 4 content from {filing_url}: {e}")
            return {}
    
    def fetch_sec_filings(self, cik: str, days_back: int = 30) -> List[Dict]:
        """Fetch SEC filings by CIK with enhanced Form 4 parsing"""
        if not cik:
            return []
        
        url = f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"
        headers = {
            "User-Agent": SEC_USER_AGENT_EMAIL,
            "Accept-Encoding": "gzip, deflate"
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            
            recent = data.get("filings", {}).get("recent", {})
            forms = recent.get("form", [])
            dates = recent.get("filingDate", [])
            accessions = recent.get("accessionNumber", [])
            primary_docs = recent.get("primaryDocument", [])
            
            filings = []
            for form, filing_date, accession, primary_doc in zip(forms, dates, accessions, primary_docs):
                if form in ["8-K", "4"]:
                    normalized_date = self.normalize_date(filing_date)
                    if normalized_date and self.is_within_range(normalized_date, days_back):
                        # Construct the filing URL
                        accession_no_dash = accession.replace('-', '')
                        filing_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession_no_dash}/{primary_doc}"
                        
                        # For Form 4, try to get the XML version instead of HTML
                        if form == "4":
                            # The primary_doc often points to an HTML version in a subfolder
                            # Try to construct the direct XML URL
                            if '/' in primary_doc:
                                # Extract the filename from the path
                                filename = primary_doc.split('/')[-1]
                                if filename.endswith('.xml'):
                                    # Try direct XML access
                                    xml_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession_no_dash}/{filename}"
                                    try:
                                        test_response = requests.head(xml_url, headers=headers, timeout=5)
                                        if test_response.status_code == 200:
                                            filing_url = xml_url
                                    except:
                                        pass
                        
                        event_text = f"SEC {form} Filing"
                        
                        insider_name = None
                        transaction_details = None
                        
                        # For Form 4 filings, try to extract insider trading details
                        if form == "4":
                            print(f"Parsing Form 4: {filing_url}")
                            insider_info = self.parse_form4_content(filing_url, headers)
                            
                            if insider_info.get('insider_name'):
                                insider_name = insider_info['insider_name']
                                
                            if insider_info.get('transaction'):
                                transaction_details = insider_info['transaction']
                                event_text = f"SEC Form 4: {insider_name} - {transaction_details}"
                            elif insider_name:
                                event_text = f"SEC Form 4: {insider_name} - Insider Trading"
                            else:
                                event_text = "SEC Form 4: Insider Trading"
                        
                        filings.append({
                            'timestamp': normalized_date,
                            'event_text': event_text,
                            'link': filing_url,
                            'insider_name': insider_name,
                            'transaction_details': transaction_details
                        })
                        
                        # Add a small delay to be respectful to SEC servers
                        if form == "4":
                            time.sleep(0.5)
            
            return filings
        except Exception as e:
            print(f"SEC filings fetch failed for CIK {cik}: {e}")
            return []
    
    def load_companies(self) -> List[Dict]:
        """Load company configuration"""
        companies = [
            {"ticker": "TSLA", "company": "Tesla", "cik": "0001318605"},
            {"ticker": "AAPL", "company": "Apple", "cik": "0000320193"},
            {"ticker": "MRK", "company": "Merck", "cik": "0000310158"},
            {"ticker": "JNJ", "company": "Johnson & Johnson", "cik": "0000200406"}
        ]
        
        # Create CSV if it doesn't exist
        if not os.path.exists('companies.csv'):
            with open('companies.csv', 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=['ticker', 'company', 'cik'])
                writer.writeheader()
                writer.writerows(companies)
            print("Created companies.csv with sample data")
        
        return companies
    
    def run_backfill(self, months_back: int = 12):
        """Run backfill ingestion for historical data"""
        print(f"Starting backfill for {months_back} months...")
        days_back = months_back * 30
        companies = self.load_companies()
        
        for company in companies:
            ticker = company['ticker']
            company_name = company['company']
            cik = company.get('cik', '')
            
            print(f"Processing {ticker} ({company_name})...")
            
            # NHTSA recalls
            nhtsa_events = self.fetch_nhtsa_recalls(company_name, days_back)
            for event in nhtsa_events:
                self.add_event(event['timestamp'], 'NHTSA', ticker, company_name, 
                             event['event_text'], event['link'])
            
            # FDA drug events
            fda_drug_events = self.fetch_fda_drug_events(company_name, days_back)
            for event in fda_drug_events:
                self.add_event(event['timestamp'], 'FDA-DRUG', ticker, company_name,
                             event['event_text'], event['link'])
            
            # FDA device events
            fda_device_events = self.fetch_fda_device_events(company_name, days_back)
            for event in fda_device_events:
                self.add_event(event['timestamp'], 'FDA-DEVICE', ticker, company_name,
                             event['event_text'], event['link'])
            
            # SEC filings
            sec_events = self.fetch_sec_filings(cik, days_back)
            for event in sec_events:
                self.add_event(event['timestamp'], 'SEC', ticker, company_name,
                             event['event_text'], event['link'], 
                             event.get('insider_name'), event.get('transaction_details'))
            
            print(f"Completed {ticker}: {len(nhtsa_events + fda_drug_events + fda_device_events + sec_events)} events")
            time.sleep(1)  # Be respectful to APIs
        
        print("Backfill completed!")
    
    def run_incremental(self):
        """Run incremental daily ingestion"""
        print("Starting incremental ingestion...")
        companies = self.load_companies()
        
        for company in companies:
            ticker = company['ticker']
            company_name = company['company']
            cik = company.get('cik', '')
            
            print(f"Processing daily update for {ticker}...")
            
            # Fetch last 2 days to catch any delayed reports
            nhtsa_events = self.fetch_nhtsa_recalls(company_name, 2)
            fda_drug_events = self.fetch_fda_drug_events(company_name, 2)
            fda_device_events = self.fetch_fda_device_events(company_name, 2)
            sec_events = self.fetch_sec_filings(cik, 2)
            
            total_events = len(nhtsa_events + fda_drug_events + fda_device_events + sec_events)
            
            for event in nhtsa_events:
                self.add_event(event['timestamp'], 'NHTSA', ticker, company_name,
                             event['event_text'], event['link'])
            
            for event in fda_drug_events:
                self.add_event(event['timestamp'], 'FDA-DRUG', ticker, company_name,
                             event['event_text'], event['link'])
            
            for event in fda_device_events:
                self.add_event(event['timestamp'], 'FDA-DEVICE', ticker, company_name,
                             event['event_text'], event['link'])
            
            for event in sec_events:
                self.add_event(event['timestamp'], 'SEC', ticker, company_name,
                             event['event_text'], event['link'],
                             event.get('insider_name'), event.get('transaction_details'))
            
            if total_events > 0:
                print(f"Added {total_events} new events for {ticker}")
            
            time.sleep(0.5)
        
        print("Incremental ingestion completed!")

if __name__ == "__main__":
    import sys
    
    ingester = EventIngester()
    
    if len(sys.argv) > 1 and sys.argv[1] == "backfill":
        months = int(sys.argv[2]) if len(sys.argv) > 2 else 12
        ingester.run_backfill(months)
    else:
        ingester.run_incremental()