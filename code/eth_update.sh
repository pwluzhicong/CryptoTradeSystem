#!/bin/sh

. /etc/profile
. ~/.bash_profile
cd /root/project
cd ./download
python update_dataset_eth.py
cd ..
python model_update_eth.py
python model_update_eth_v4.py
python model_update_eth_v5.py
