from binance.client import Client
import pandas as pd
import numpy as np
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

def calculate_ema(data, period):
    return data.ewm(span=period, adjust=False).mean()

def smooth_range(source, period, multiplier):
    # Calculate absolute price changes
    abs_changes = abs(source - source.shift(1))
    
    # Calculate initial EMA
    weighted_period = period * 2 - 1
    avg_range = calculate_ema(abs_changes, period)
    
    # Calculate final smoothed range
    smooth_range = calculate_ema(avg_range, weighted_period) * multiplier
    return smooth_range

def range_filter(source, range_val):
    filt = source.copy()
    for i in range(1, len(source)):
        prev_filt = filt.iloc[i-1]
        curr_price = source.iloc[i]
        curr_range = range_val.iloc[i]
        
        if curr_price > prev_filt:
            filt.iloc[i] = prev_filt if curr_price - curr_range < prev_filt else curr_price - curr_range
        else:
            filt.iloc[i] = prev_filt if curr_price + curr_range > prev_filt else curr_price + curr_range
    
    return filt

def calculate_twin_range_filter(df, fast_period=12, fast_range=1, slow_period=4, slow_range=2):
    source = df['close']
    
    # Calculate smooth ranges
    smrng1 = smooth_range(source, fast_period, fast_range)
    smrng2 = smooth_range(source, slow_period, slow_range)
    smrng = (smrng1 + smrng2) / 2
    
    # Calculate range filter
    filt = range_filter(source, smrng)
    
    # Calculate upward and downward trends
    upward = pd.Series(0, index=source.index)
    downward = pd.Series(0, index=source.index)
    
    for i in range(1, len(filt)):
        if filt.iloc[i] > filt.iloc[i-1]:
            upward.iloc[i] = upward.iloc[i-1] + 1
            downward.iloc[i] = 0
        elif filt.iloc[i] < filt.iloc[i-1]:
            downward.iloc[i] = downward.iloc[i-1] + 1
            upward.iloc[i] = 0
        else:
            upward.iloc[i] = upward.iloc[i-1]
            downward.iloc[i] = downward.iloc[i-1]
    
    # Calculate trading conditions
    long_cond = ((source > filt) & (source > source.shift(1)) & (upward > 0)) | \
                ((source > filt) & (source < source.shift(1)) & (upward > 0))
    
    short_cond = ((source < filt) & (source < source.shift(1)) & (downward > 0)) | \
                 ((source < filt) & (source > source.shift(1)) & (downward > 0))
    
    # Initialize condition
    cond_ini = pd.Series(0, index=source.index)
    for i in range(1, len(source)):
        if long_cond.iloc[i]:
            cond_ini.iloc[i] = 1
        elif short_cond.iloc[i]:
            cond_ini.iloc[i] = -1
        else:
            cond_ini.iloc[i] = cond_ini.iloc[i-1]
    
    # Generate final signals
    long_signals = long_cond & (cond_ini.shift(1) == -1)
    short_signals = short_cond & (cond_ini.shift(1) == 1)
    
    return long_signals, short_signals

def process_symbol(symbol, client):
    try:
        # Get klines data for supported timeframes
        timeframes = {
            '1h': Client.KLINE_INTERVAL_1HOUR,
            '2h': Client.KLINE_INTERVAL_2HOUR,
            '4h': Client.KLINE_INTERVAL_4HOUR
        }
        
        # Store data for each timeframe
        dfs = {}
        emas = {}
        
        # Fetch data for each timeframe
        for tf, interval in timeframes.items():
            klines = client.futures_klines(
                symbol=symbol,
                interval=interval,
                limit=100
            )
            
            df = pd.DataFrame(klines, columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_volume', 'trades', 'taker_buy_base',
                'taker_buy_quote', 'ignore'
            ])
            
            # Convert price columns to float
            df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
            
            # Store the dataframe
            dfs[tf] = df
            
            # Calculate and store EMA
            emas[tf] = calculate_ema(df['close'], 9)
        
        # Calculate Twin Range Filter signals using 1H data
        long_signals, short_signals = calculate_twin_range_filter(
            dfs['1h'],
            fast_period=12,
            fast_range=1,
            slow_period=4,
            slow_range=2
        )
        
        # Check if all timeframes meet the conditions for LONG
        def all_timeframes_above_ema():
            for tf in timeframes.keys():
                close = dfs[tf]['close'].iloc[-1]
                ema = emas[tf].iloc[-1]
                if close <= ema:
                    return False
            return True
        
        # Check if all timeframes meet the conditions for SHORT
        def all_timeframes_below_ema():
            for tf in timeframes.keys():
                close = dfs[tf]['close'].iloc[-1]
                ema = emas[tf].iloc[-1]
                if close >= ema:
                    return False
            return True
        
        # Check signals
        def is_candle_closed_above_ema():
            """Check if the candle at signal time is closed above EMA in all timeframes"""
            for tf in timeframes.keys():
                close = dfs[tf]['close'].iloc[-3]  # Signal candle close price
                ema = emas[tf].iloc[-3]  # Signal candle EMA
                if close <= ema:
                    return False
            return True
        
        def is_candle_closed_below_ema():
            """Check if the candle at signal time is closed below EMA in all timeframes"""
            for tf in timeframes.keys():
                close = dfs[tf]['close'].iloc[-3]  # Signal candle close price
                ema = emas[tf].iloc[-3]  # Signal candle EMA
                if close >= ema:
                    return False
            return True
        
        if long_signals.iloc[-3]:  # Long signal in two previous candle
            if is_candle_closed_above_ema():
                last_update = datetime.fromtimestamp(dfs['1h']['timestamp'].iloc[-3] / 1000).strftime('%Y-%m-%d %H:%M:%S')
                signal_price = dfs['1h']['close'].iloc[-3]  # Signal candle close price
                print(f"🟢 {symbol} [1H] - LONG @ {signal_price:.4f} - Signal Time: {last_update}")
                
        elif short_signals.iloc[-3]:  # Short signal in two previous candle
            if is_candle_closed_below_ema():
                last_update = datetime.fromtimestamp(dfs['1h']['timestamp'].iloc[-3] / 1000).strftime('%Y-%m-%d %H:%M:%S')
                signal_price = dfs['1h']['close'].iloc[-3]  # Signal candle close price
                print(f"🔴 {symbol} [1H] - SHORT @ {signal_price:.4f} - Signal Time: {last_update}")
    except Exception as e:
        print(f"An error occurred for {symbol}: {e}")

def get_futures_data():
    # Initialize Binance client
    client = Client()
    try:
        # Get all perpetual futures exchange information
        futures_exchange_info = client.futures_exchange_info()
        # Filter for USDT perpetual pairs
        usdt_perpetual_symbols = [
            symbol['symbol'] for symbol in futures_exchange_info['symbols']
            if symbol['symbol'].endswith('USDT') and symbol['contractType'] == 'PERPETUAL'
        ]
        print(f"\nFound {len(usdt_perpetual_symbols)} USDT perpetual pairs")
        print("Checking signals from previous 1-hour candle...\n")
        max_workers = 50  # Aynı anda çalışacak thread sayısı (daha hızlı)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(process_symbol, symbol, client) for symbol in usdt_perpetual_symbols]
            for future in as_completed(futures):
                pass  # Sonuçlar zaten print ile yazılıyor, burada bir şey yapmaya gerek yok
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    print("\nBinance Futures Scanner with Twin Range Filter")
    print("Scanning all USDT perpetual pairs...")
    get_futures_data()  # Tek seferlik tarama 