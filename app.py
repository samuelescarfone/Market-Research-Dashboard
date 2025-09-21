#!/usr/bin/env python3
"""
Company Catalyst Dashboard - Flask Frontend
Displays regulatory events overlaid on stock price charts
"""

from flask import Flask, render_template, jsonify, request
import sqlite3
import json
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd

app = Flask(__name__)

def get_db_connection():
    conn = sqlite3.connect('events.db')
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Initialize the database with events table"""
    conn = get_db_connection()
    conn.execute('''
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            source TEXT NOT NULL,
            ticker TEXT NOT NULL,
            company TEXT NOT NULL,
            event_text TEXT NOT NULL,
            link TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

@app.route('/')
def dashboard():
    """Main dashboard page"""
    return render_template('dashboard.html')

@app.route('/api/companies')
def get_companies():
    """Get list of tracked companies"""
    conn = get_db_connection()
    companies = conn.execute('''
        SELECT DISTINCT ticker, company 
        FROM events 
        ORDER BY ticker
    ''').fetchall()
    conn.close()
    
    return jsonify([dict(row) for row in companies])

@app.route('/api/events/<ticker>')
def get_events(ticker):
    """Get events for a specific ticker"""
    days = request.args.get('days', 90, type=int)
    since_date = (datetime.now() - timedelta(days=days)).isoformat()
    
    conn = get_db_connection()
    events = conn.execute('''
        SELECT * FROM events 
        WHERE ticker = ? AND timestamp >= ?
        ORDER BY timestamp DESC
    ''', (ticker.upper(), since_date)).fetchall()
    conn.close()
    
    return jsonify([dict(row) for row in events])

@app.route('/api/stock/<ticker>')
def get_stock_data(ticker):
    """Get stock price data for charting"""
    try:
        days = request.args.get('days', 90, type=int)
        stock = yf.Ticker(ticker)
        hist = stock.history(period=f"{days}d")
        
        # Convert to format suitable for charting
        data = []
        for date, row in hist.iterrows():
            data.append({
                'date': date.strftime('%Y-%m-%d'),
                'open': round(row['Open'], 2),
                'high': round(row['High'], 2),
                'low': round(row['Low'], 2),
                'close': round(row['Close'], 2),
                'volume': int(row['Volume'])
            })
        
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/chart-data/<ticker>')
def get_chart_data(ticker):
    """Get combined stock and events data for charting"""
    days = request.args.get('days', 90, type=int)
    
    # Get stock data
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period=f"{days}d")
        stock_data = []
        for date, row in hist.iterrows():
            stock_data.append({
                'date': date.strftime('%Y-%m-%d'),
                'close': round(row['Close'], 2),
                'volume': int(row['Volume'])
            })
    except Exception as e:
        stock_data = []
    
    # Get events data
    since_date = (datetime.now() - timedelta(days=days)).isoformat()
    conn = get_db_connection()
    events = conn.execute('''
        SELECT timestamp, source, event_text, link 
        FROM events 
        WHERE ticker = ? AND timestamp >= ?
        ORDER BY timestamp DESC
    ''', (ticker.upper(), since_date)).fetchall()
    conn.close()
    
    events_data = []
    for event in events:
        # Extract date from timestamp
        event_date = event['timestamp'][:10] if event['timestamp'] else None
        events_data.append({
            'date': event_date,
            'source': event['source'],
            'text': event['event_text'][:100] + '...' if len(event['event_text']) > 100 else event['event_text'],
            'link': event['link']
        })
    
    return jsonify({
        'stock': stock_data,
        'events': events_data
    })

@app.route('/api/sec-filings')
def get_sec_filings():
    """Get all SEC Form 4 filings with insider trading details"""
    days = request.args.get('days', 90, type=int)
    since_date = (datetime.now() - timedelta(days=days)).isoformat()
    
    conn = get_db_connection()
    filings = conn.execute('''
        SELECT timestamp, ticker, company, event_text, link, insider_name, 
               transaction_type, shares, price, total_value
        FROM events 
        WHERE source = 'SEC' AND timestamp >= ?
        ORDER BY timestamp DESC
    ''', (since_date,)).fetchall()
    conn.close()
    
    return jsonify([dict(row) for row in filings])

@app.route('/api/fda-events')
def get_fda_events():
    """Get all FDA events (drug and device)"""
    days = request.args.get('days', 90, type=int)
    since_date = (datetime.now() - timedelta(days=days)).isoformat()
    
    conn = get_db_connection()
    events = conn.execute('''
        SELECT timestamp, source, ticker, company, event_text, link 
        FROM events 
        WHERE (source = 'FDA-DRUG' OR source = 'FDA-DEVICE') AND timestamp >= ?
        ORDER BY timestamp DESC
    ''', (since_date,)).fetchall()
    conn.close()
    
    return jsonify([dict(row) for row in events])

@app.route('/api/nhtsa-recalls')
def get_nhtsa_recalls():
    """Get all NHTSA vehicle recalls"""
    days = request.args.get('days', 90, type=int)
    since_date = (datetime.now() - timedelta(days=days)).isoformat()
    
    conn = get_db_connection()
    recalls = conn.execute('''
        SELECT timestamp, ticker, company, event_text, link 
        FROM events 
        WHERE source = 'NHTSA' AND timestamp >= ?
        ORDER BY timestamp DESC
    ''', (since_date,)).fetchall()
    conn.close()
    
    return jsonify([dict(row) for row in recalls])

if __name__ == '__main__':
    init_db()
    app.run(debug=True, host='0.0.0.0', port=3000)