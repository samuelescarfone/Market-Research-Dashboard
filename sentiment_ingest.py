#!/usr/bin/env python3
"""
Sentiment Analysis Pipeline for Stock Market Data
Collects text from Reddit, Yahoo Finance, StockTwits and analyzes sentiment
"""

import sqlite3
import requests
import praw
import yfinance as yf
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional
import time
import re
import os
from dotenv import load_dotenv
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification
import pandas as pd

# Load environment variables
load_dotenv()

# Configuration
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT", "catalyst-dashboard-app")

class SentimentAnalyzer:
    def __init__(self, db_path='events.db'):
        self.db_path = db_path
        self.init_db()
        self.init_analyzers()
        self.init_reddit()
    
    def init_db(self):
        """Initialize sentiment database tables"""
        conn = sqlite3.connect(self.db_path)
        conn.execute('''
            CREATE TABLE IF NOT EXISTS sentiment_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                ticker TEXT NOT NULL,
                source TEXT NOT NULL,
                text_content TEXT NOT NULL,
                vader_score REAL,
                finbert_score REAL,
                finbert_label TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(timestamp, ticker, source, text_content)
            )
        ''')
        
        conn.execute('''
            CREATE TABLE IF NOT EXISTS daily_sentiment (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                ticker TEXT NOT NULL,
                avg_vader_score REAL,
                avg_finbert_score REAL,
                post_count INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(date, ticker)
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def init_analyzers(self):
        """Initialize sentiment analysis models"""
        print("Initializing sentiment analyzers...")
        
        # VADER for social media sentiment
        self.vader = SentimentIntensityAnalyzer()
        
        # FinBERT for financial sentiment
        try:
            model_name = "ProsusAI/finbert"
            self.finbert_tokenizer = AutoTokenizer.from_pretrained(model_name)
            self.finbert_model = AutoModelForSequenceClassification.from_pretrained(model_name)
            self.finbert_pipeline = pipeline(
                "sentiment-analysis",
                model=self.finbert_model,
                tokenizer=self.finbert_tokenizer,
                return_all_scores=True
            )
            print("FinBERT loaded successfully")
        except Exception as e:
            print(f"Warning: Could not load FinBERT: {e}")
            self.finbert_pipeline = None
    
    def init_reddit(self):
        """Initialize Reddit API client"""
        try:
            self.reddit = praw.Reddit(
                client_id=REDDIT_CLIENT_ID,
                client_secret=REDDIT_CLIENT_SECRET,
                user_agent=REDDIT_USER_AGENT
            )
            print("Reddit API initialized")
        except Exception as e:
            print(f"Warning: Could not initialize Reddit API: {e}")
            self.reddit = None
    
    def analyze_sentiment(self, text: str) -> Dict:
        """Analyze sentiment using both VADER and FinBERT"""
        results = {}
        
        # VADER analysis
        vader_scores = self.vader.polarity_scores(text)
        results['vader_score'] = vader_scores['compound']
        
        # FinBERT analysis
        if self.finbert_pipeline:
            try:
                # Truncate text to avoid token limits
                text_truncated = text[:512]
                finbert_result = self.finbert_pipeline(text_truncated)
                
                # Convert to compound score (-1 to 1)
                for score_dict in finbert_result[0]:
                    if score_dict['label'] == 'positive':
                        pos_score = score_dict['score']
                    elif score_dict['label'] == 'negative':
                        neg_score = score_dict['score']
                    elif score_dict['label'] == 'neutral':
                        neu_score = score_dict['score']
                
                # Create compound score
                results['finbert_score'] = pos_score - neg_score
                results['finbert_label'] = max(finbert_result[0], key=lambda x: x['score'])['label']
                
            except Exception as e:
                print(f"FinBERT analysis failed: {e}")
                results['finbert_score'] = None
                results['finbert_label'] = None
        else:
            results['finbert_score'] = None
            results['finbert_label'] = None
        
        return results
    
    def collect_reddit_data(self, ticker: str, days_back: int = 7) -> List[Dict]:
        """Collect Reddit posts about a ticker"""
        if not self.reddit:
            return []
        
        posts = []
        subreddits = ['stocks', 'investing', 'SecurityAnalysis', 'ValueInvesting', 'wallstreetbets']
        
        try:
            for subreddit_name in subreddits:
                subreddit = self.reddit.subreddit(subreddit_name)
                
                # Search for ticker mentions
                for submission in subreddit.search(f"${ticker}", time_filter="week", limit=20):
                    # Check if post is within date range
                    post_date = datetime.fromtimestamp(submission.created_utc).date()
                    if (date.today() - post_date).days <= days_back:
                        
                        # Combine title and selftext
                        text_content = f"{submission.title} {submission.selftext}"
                        
                        if len(text_content.strip()) > 10:  # Filter out very short posts
                            posts.append({
                                'timestamp': post_date.isoformat() + "T00:00:00Z",
                                'source': f'reddit_{subreddit_name}',
                                'text_content': text_content[:1000],  # Limit length
                                'ticker': ticker.upper()
                            })
                
                time.sleep(1)  # Rate limiting
                
        except Exception as e:
            print(f"Reddit collection failed for {ticker}: {e}")
        
        return posts
    
    def collect_yahoo_headlines(self, ticker: str, days_back: int = 7) -> List[Dict]:
        """Collect Yahoo Finance news headlines"""
        headlines = []
        
        try:
            stock = yf.Ticker(ticker)
            news = stock.news
            
            cutoff_date = date.today() - timedelta(days=days_back)
            
            for article in news:
                # Convert timestamp to date
                article_date = datetime.fromtimestamp(article['providerPublishTime']).date()
                
                if article_date >= cutoff_date:
                    headlines.append({
                        'timestamp': article_date.isoformat() + "T00:00:00Z",
                        'source': 'yahoo_finance',
                        'text_content': article['title'],
                        'ticker': ticker.upper()
                    })
                    
        except Exception as e:
            print(f"Yahoo Finance collection failed for {ticker}: {e}")
        
        return headlines
    
    def collect_stocktwits_data(self, ticker: str, days_back: int = 7) -> List[Dict]:
        """Collect StockTwits messages (using public API)"""
        messages = []
        
        try:
            url = f"https://api.stocktwits.com/api/2/streams/symbol/{ticker}.json"
            response = requests.get(url, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                cutoff_date = date.today() - timedelta(days=days_back)
                
                for message in data.get('messages', []):
                    # Parse created_at timestamp
                    created_at = datetime.strptime(message['created_at'], '%Y-%m-%dT%H:%M:%SZ').date()
                    
                    if created_at >= cutoff_date:
                        messages.append({
                            'timestamp': created_at.isoformat() + "T00:00:00Z",
                            'source': 'stocktwits',
                            'text_content': message['body'],
                            'ticker': ticker.upper()
                        })
                        
        except Exception as e:
            print(f"StockTwits collection failed for {ticker}: {e}")
        
        return messages
    
    def store_sentiment_data(self, data_points: List[Dict]):
        """Store sentiment analysis results in database"""
        conn = sqlite3.connect(self.db_path)
        
        for point in data_points:
            try:
                conn.execute('''
                    INSERT OR IGNORE INTO sentiment_data 
                    (timestamp, ticker, source, text_content, vader_score, finbert_score, finbert_label)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    point['timestamp'],
                    point['ticker'],
                    point['source'],
                    point['text_content'],
                    point.get('vader_score'),
                    point.get('finbert_score'),
                    point.get('finbert_label')
                ))
            except Exception as e:
                print(f"Error storing sentiment data: {e}")
        
        conn.commit()
        conn.close()
    
    def aggregate_daily_sentiment(self, ticker: str, days_back: int = 30):
        """Aggregate sentiment scores by day"""
        conn = sqlite3.connect(self.db_path)
        
        # Get daily aggregates
        cursor = conn.execute('''
            SELECT 
                DATE(timestamp) as date,
                AVG(vader_score) as avg_vader,
                AVG(finbert_score) as avg_finbert,
                COUNT(*) as post_count
            FROM sentiment_data 
            WHERE ticker = ? AND timestamp >= ?
            GROUP BY DATE(timestamp)
            ORDER BY date
        ''', (ticker.upper(), (date.today() - timedelta(days=days_back)).isoformat()))
        
        for row in cursor.fetchall():
            try:
                conn.execute('''
                    INSERT OR REPLACE INTO daily_sentiment 
                    (date, ticker, avg_vader_score, avg_finbert_score, post_count)
                    VALUES (?, ?, ?, ?, ?)
                ''', row)
            except Exception as e:
                print(f"Error storing daily sentiment: {e}")
        
        conn.commit()
        conn.close()
    
    def run_sentiment_collection(self, tickers: List[str], days_back: int = 7):
        """Run complete sentiment collection pipeline"""
        for ticker in tickers:
            print(f"Collecting sentiment data for {ticker}...")
            
            all_data = []
            
            # Collect from all sources
            reddit_data = self.collect_reddit_data(ticker, days_back)
            yahoo_data = self.collect_yahoo_headlines(ticker, days_back)
            stocktwits_data = self.collect_stocktwits_data(ticker, days_back)
            
            all_data.extend(reddit_data)
            all_data.extend(yahoo_data)
            all_data.extend(stocktwits_data)
            
            print(f"Collected {len(all_data)} text samples for {ticker}")
            
            # Analyze sentiment for each text sample
            for data_point in all_data:
                sentiment_results = self.analyze_sentiment(data_point['text_content'])
                data_point.update(sentiment_results)
            
            # Store in database
            self.store_sentiment_data(all_data)
            
            # Aggregate daily scores
            self.aggregate_daily_sentiment(ticker, days_back)
            
            print(f"Completed sentiment analysis for {ticker}")
            time.sleep(2)  # Rate limiting between tickers

def main():
    import sys
    
    analyzer = SentimentAnalyzer()
    
    # Default tickers
    tickers = ['TSLA', 'AAPL', 'MRK', 'JNJ']
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "backfill":
            days = int(sys.argv[2]) if len(sys.argv) > 2 else 30
            analyzer.run_sentiment_collection(tickers, days)
        else:
            # Single ticker
            analyzer.run_sentiment_collection([sys.argv[1].upper()], 7)
    else:
        # Default: collect last 7 days
        analyzer.run_sentiment_collection(tickers, 7)

if __name__ == "__main__":
    main()