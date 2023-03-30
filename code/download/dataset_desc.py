import json

with open("dataset_ETHUSDT_latest.json") as f:
    klines = json.load(f)

print(len(klines))


