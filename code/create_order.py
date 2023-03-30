from celery import Celery
from celery_once import QueueOnce

app = Celery('create_order', broker='redis://localhost:6379/6')

app.conf.ONCE = {
  'backend': 'celery_once.backends.Redis',
  'settings': {
    'url': 'redis://localhost:6379/6',
    'default_timeout': 60 * 60 * 10
  }
}

@app.task(base=QueueOnce, once={'graceful': False})
def create_order(eta, quantity, side, positionSide, client_key, symbol="ETHUSDT", model_name="default"):
    
    from datetime import datetime, timedelta
#     import binance
    from binance.client import Client
    
    import redis
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    
    client = Client(
         r.get(client_key + "_key").decode(),
         r.get(client_key + "_sec").decode()
    )
    
    # symbol = "ETHUSDT"
    
    
    order_key = "%s-%s-%s-%s-%s-%s-%s" % (eta, quantity, side, positionSide, client_key, symbol, model_name)
    state = r.incr(order_key)
    
    if state <= 1:
        
        order = client.futures_create_order(
            quantity=quantity,
            symbol=symbol,
            side=side,
            positionSide=positionSide,
            type="MARKET"
        )
        
        return order, order_key, eta, quantity, side, positionSide, client_key, symbol, model_name

