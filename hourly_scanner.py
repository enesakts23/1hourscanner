from binance.client import Client
import pandas as pd
import numpy as np
from datetime import datetime
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
import requests

def calculate_ema(data, period):
    return data.ewm(span=period, adjust=False).mean()

def smooth_range(source, period, multiplier):

    abs_changes = abs(source - source.shift(1))
    
    weighted_period = period * 2 - 1
    avg_range = calculate_ema(abs_changes, period)
    
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
    
    smrng1 = smooth_range(source, fast_period, fast_range)
    smrng2 = smooth_range(source, slow_period, slow_range)
    smrng = (smrng1 + smrng2) / 2
    
    filt = range_filter(source, smrng)
    
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
    
    long_cond = ((source > filt) & (source > source.shift(1)) & (upward > 0)) | \
                ((source > filt) & (source < source.shift(1)) & (upward > 0))
    
    short_cond = ((source < filt) & (source < source.shift(1)) & (downward > 0)) | \
                 ((source < filt) & (source > source.shift(1)) & (downward > 0))
    
    cond_ini = pd.Series(0, index=source.index)
    for i in range(1, len(source)):
        if long_cond.iloc[i]:
            cond_ini.iloc[i] = 1
        elif short_cond.iloc[i]:
            cond_ini.iloc[i] = -1
        else:
            cond_ini.iloc[i] = cond_ini.iloc[i-1]
    
    long_signals = long_cond & (cond_ini.shift(1) == -1)
    short_signals = short_cond & (cond_ini.shift(1) == 1)
    
    return long_signals, short_signals

def process_symbol(symbol, client):
    try:
        timeframes = {
            '1h': Client.KLINE_INTERVAL_1HOUR,
            '2h': Client.KLINE_INTERVAL_2HOUR,
            '4h': Client.KLINE_INTERVAL_4HOUR
        }
        dfs = {}
        emas = {}
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
            df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
            dfs[tf] = df
            emas[tf] = calculate_ema(df['close'], 9)
        long_signals, short_signals = calculate_twin_range_filter(
            dfs['1h'],
            fast_period=12,
            fast_range=1,
            slow_period=4,
            slow_range=2
        )
        def is_candle_closed_above_ema():
            for tf in timeframes.keys():
                close = dfs[tf]['close'].iloc[-3]
                ema = emas[tf].iloc[-3]
                if close <= ema:
                    return False
            return True
        def is_candle_closed_below_ema():
            for tf in timeframes.keys():
                close = dfs[tf]['close'].iloc[-3]
                ema = emas[tf].iloc[-3]
                if close >= ema:
                    return False
            return True
        if long_signals.iloc[-3]:
            if is_candle_closed_above_ema():
                last_update = datetime.fromtimestamp(dfs['1h']['timestamp'].iloc[-3] / 1000).strftime('%Y-%m-%d %H:%M:%S')
                signal_price = dfs['1h']['close'].iloc[-3]
                return f"🟢 {symbol} [1H] - LONG @ {signal_price:.4f} - Signal Time: {last_update}"
        elif short_signals.iloc[-3]:
            if is_candle_closed_below_ema():
                last_update = datetime.fromtimestamp(dfs['1h']['timestamp'].iloc[-3] / 1000).strftime('%Y-%m-%d %H:%M:%S')
                signal_price = dfs['1h']['close'].iloc[-3]
                return f"🔴 {symbol} [1H] - SHORT @ {signal_price:.4f} - Signal Time: {last_update}"
    except Exception as e:
        return f"An error occurred for {symbol}: {e}"
    return None

def get_futures_data():
    client = Client()
    results = []
    try:
        futures_exchange_info = client.futures_exchange_info()
        usdt_perpetual_symbols = [
            symbol['symbol'] for symbol in futures_exchange_info['symbols']
            if symbol['symbol'].endswith('USDT') and symbol['contractType'] == 'PERPETUAL'
        ]
        max_workers = 50
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(process_symbol, symbol, client) for symbol in usdt_perpetual_symbols]
            for future in as_completed(futures):
                result = future.result()
                if result:
                    results.append(result)
    except Exception as e:
        results.append(f"An error occurred: {e}")
    return results

TELEGRAM_BOT_TOKEN = "8431469304:AAEQWtSbfnrjJ5NM9UUu-q5D3yPO2iQ5GHA"
TELEGRAM_CHAT_ID = "@SaatlikTarama"  

def send_telegram_message(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML"
    }
    try:
        response = requests.post(url, json=payload)
        if response.status_code != 200:
            print(f"Telegram error: {response.text}")
    except Exception as e:
        print(f"Telegram send error: {e}")

# Flask ve scheduler
app = Flask(__name__)
scheduler = BackgroundScheduler()

def get_coin_icon(symbol):
    # CoinGecko API ile sembolden ikon url'si al
    try:
        url = f"https://api.coingecko.com/api/v3/coins/list"
        response = requests.get(url)
        if response.status_code == 200:
            coins = response.json()
            symbol_lower = symbol.lower().replace('usdt', '')
            for coin in coins:
                if coin['symbol'] == symbol_lower:
                    coin_id = coin['id']
                    info_url = f"https://api.coingecko.com/api/v3/coins/{coin_id}"
                    info_resp = requests.get(info_url)
                    if info_resp.status_code == 200:
                        icon_url = info_resp.json()['image']['thumb']
                        return icon_url
        return None
    except Exception:
        return None

def scan_and_notify():
    tz = pytz.timezone("Europe/Istanbul")
    now = datetime.now(tz)
    print(f"Tarama zamanı: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    results = get_futures_data()
    if results:
        message = "<b>📊 Saatlik Tarama Sonuçları</b>\n\n"
        for result in results:
            if result.startswith("🟢") or result.startswith("🔴"):
                parts = result.split()
                symbol = parts[1].replace("USDT", "")
                emoji = "🟢" if result.startswith("🟢") else "🔴"
                message += f"{emoji} <b>{symbol}USDT</b>\n"
            else:
                message += result + "\n"
        send_telegram_message(message)
    else:
        send_telegram_message("Saatlik taramada sinyal bulunamadı.")

def schedule_job():
    scheduler.add_job(scan_and_notify, 'cron', minute='0,5,10,15,20,25,30,35,40,45,50,55', timezone='Europe/Istanbul')
    scheduler.start()

@app.route("/")
def home():
    return "Saatlik Tarama Botu Çalışıyor!"

if __name__ == "__main__":
    print("Saatlik Tarama Botu başlatılıyor...")
    schedule_job()
    app.run(host="0.0.0.0", port=5010)