import redis

import numpy as np 
import pandas as pd
import lightgbm as lgb

from datetime import datetime
from binance.client import Client

import numpy as np 
import pandas as pd
import lightgbm as lgb

import json

from datetime import datetime
from binance.client import Client

import logging
import sys

from model_update_eth_v5 import feature_window_size_list, label_window_size
from model_update_eth_v5 import make_df_main, make_df_features

logger = logging.getLogger()
logger.setLevel(logging.INFO)

version = "eth_v5"
model_name = "gbm_%s" % version

dt = datetime.utcnow().strftime("%Y%m%d")
logger.addHandler(logging.FileHandler("data/log_run_model_%s_%s.txt" % (version, dt)))
logger.addHandler(logging.StreamHandler(sys.stdout))


from utils import get_now_ts, log_order, get_position
from init import init_client


def load_gbm_list(version):
    import os
    # dt = get_dt()
    with open("model/%s/latest_dt.txt" % version) as f:
        dt = f.read().strip()
        
    model_path = "model/%s/%s" % (version, dt)

    with open(model_path + "/bars.json") as f:
        sell_bar, buy_bar = json.load(f)
    
    gbm_list = []
    
    for i in range(5):
        gbm = lgb.Booster(model_file=model_path + '/gbm_%s.txt' % i)
        gbm_list.append(gbm)
        
    return gbm_list, sell_bar, buy_bar


def ensemble_predict(gbm_list, df):
    res = df[[]].copy()
    for i, gbm in gbm_list:
        res[i] = gbm.predict(df)

    return (res[0]+res[1]+res[2]+res[3]+res[4]) / 5.


def update(client, klines):
    now_ts = get_now_ts()
    start_ts = klines[-2][6] + 1
    
    updated_flag = False
    
    if now_ts >= (start_ts + 65 * 1000):
        kline_generator = client.futures_historical_klines_generator(symbol, Client.KLINE_INTERVAL_1MINUTE, start_ts)
        
        n = 0
        
        new_klines = []

        for kline in kline_generator:
            if len(kline) == 12:
              new_klines.append(kline)
          
        if len(new_klines) <= 1:
            updated_flag = False
        else:
            idx = len(klines) - 1
            while(klines[idx][0] != new_klines[0][0]):
              idx -= 1
              
            j = 0
            
            while( (idx + j < len(klines)) and (klines[idx + j][0] == new_klines[j][0]) ):
              klines[idx + j] = new_klines[j]
              
              logging.info("---updated kline: %s" % klines[idx + j])
              j += 1
              
            while( j < len(new_klines) ): 
              klines.append(new_klines[j])
              
              logging.info("---appended kline: %s" % klines[idx + j])
              j += 1
              
            updated_flag = True

        klines = klines[-60*24 - 1:]
    
    return klines, updated_flag
    
def get_df_pred(klines, gbm_list):
    assert len(klines) == 60*24

    df_main = make_df_main(klines)
    df_features = make_df_features(df_main, feature_window_size_list).iloc[-1:]
    
    df_res = df_features[[]].copy()
    df_res["pred_avg"] = 0.
    
    for i, gbm in enumerate(gbm_list):
        df_res["pred_%s" % i] = gbm.predict(df_features)
        df_res["pred_avg"] += (df_res["pred_%s" % i] / len(gbm_list))
    
    return df_res

def init_klines(client):
    kline_generator = client.futures_historical_klines_generator(symbol, Client.KLINE_INTERVAL_1MINUTE, "1 day ago UTC")
    klines = []

    for kline in kline_generator:
        klines.append(kline)

    klines = klines[-60*24-1:]
    
    return klines
    
import time
import random
from datetime import datetime, timedelta

# from tasks1 import hello
from create_order import create_order

client = Client("", "")

symbol = "ETHUSDT"

gbm_list, sell_bar, buy_bar = load_gbm_list(version)

klines = init_klines(client)

is_open = True
client_key = "demo"

# milliseconds = 1000 * 60 * (60 * 8 - 1)
milliseconds = 1000 * 60 * (label_window_size - 1)


def calculate_quantity(price, quantity_u):
    return (quantity_u * 1000 // float(price)) / 1000.

quantity_u = 10

quantity_base = calculate_quantity(klines[-1][1], quantity_u)

client_conf_list = [
    ("demo", quantity_base * 5),
    # ("cq", quantity_base * 1),
    # ("lzc", quantity_base * 1),
]

print("price_now", klines[-1][1])
print("client_conf_list", client_conf_list)

cnt = 0

while(True):
    klines, updated_flag = update(client, klines)
    
    if updated_flag:
        cnt = (cnt + 1) % 120
        
        logging.info("---updated ts: %s" % klines[-1][6])
        
        df_pred = get_df_pred(klines[:-1], gbm_list)
        pred_now = df_pred.iloc[-1].pred_avg
        logging.info("---pred_score: %s" % pred_now)

        price = klines[-2][4]
        
        
        if pred_now > 0.95 or pred_now < 0.05:
            logging.error("---pred_now abnormal, stop")
            logging.error(klines[-5:])
            #break
        elif pred_now > buy_bar and pred_now > sell_bar:
            init_client()
            
            logging.warning("---ready to buy")
            
            eta = datetime.utcnow().replace(second=0, microsecond=0) + timedelta(seconds=52)
            
            eta_ts = eta.timestamp()
            
            side = "BUY"
            positionSide = "LONG"
            
            for client_key, quantity in client_conf_list:
                
                create_order.apply_async(
                    (eta_ts, quantity, side, positionSide, client_key, symbol, model_name), 
                    eta=eta + timedelta(milliseconds=random.randint(-200, +200))
                )
                
                log_order(pred_now, eta_ts, quantity, side, positionSide, client_key, symbol, model_name, price)
    
            eta2 = eta + timedelta(milliseconds=milliseconds)
            eta2_ts = eta2.timestamp()
        
            side = "SELL"
            positionSide = "LONG"

            for client_key, quantity in client_conf_list:
                create_order.apply_async(
                    (eta2_ts, quantity, side, positionSide, client_key, symbol, model_name), 
                    eta=eta2 + timedelta(milliseconds=random.randint(-200, +200))
                )
                
                log_order(pred_now, eta2_ts, quantity, side, positionSide, client_key, symbol, model_name, price)
        
        elif pred_now < buy_bar and pred_now < sell_bar:
            logging.warning("---ready to sell")
            
            init_client()
            
            eta = datetime.utcnow().replace(second=0, microsecond=0) + timedelta(seconds=52)
            eta_ts = eta.timestamp()
            
            
            side = "SELL"
            positionSide = "SHORT"
            
            for client_key, quantity in client_conf_list:
                create_order.apply_async(
                    (eta_ts, quantity, side, positionSide, client_key, symbol, model_name), 
                    eta=eta + timedelta(milliseconds=random.randint(-200, +200))
                )
                
                log_order(pred_now, eta_ts, quantity, side, positionSide, client_key, symbol, model_name, price)
                
            eta2 = eta + timedelta(milliseconds=milliseconds)
            eta2_ts = eta2.timestamp()
            
            side = "BUY"
            positionSide = "SHORT"

            for client_key, quantity in client_conf_list:
                create_order.apply_async(
                    (eta2_ts, quantity, side, positionSide, client_key, symbol, model_name), 
                    eta=eta2 + timedelta(milliseconds=random.randint(-200, +200))
                )
                
                log_order(pred_now, eta2_ts, quantity, side, positionSide, client_key, symbol, model_name, price)
        
        
    if cnt == 119:
        break
    
    time.sleep(5)
