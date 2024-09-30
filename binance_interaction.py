#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 18 00:43:38 2023

@author: martin
"""
from binance.spot import Spot
from binance.client import Client
import pandas as pd
from pandas import json_normalize


# Function to query Binance and retrieve status
def query_binance_status():
    # Query for system status
    status = Spot().system_status()
    if status['status'] == 0:
        return True
    else:
        raise ConnectionError
        
# Function to query Binance account
def query_account(api_key, secret_key):
    return Spot(api_key, secret_key).account()

# Function to query Binance for candlestick data
def get_candlestick_data(symbol, timeframe, qty):
    # Retrieve the raw data
    raw_data = Spot().klines(symbol=symbol, interval=timeframe, limit=qty)
    # Set up the return array
    converted_data = []
    # Convert each element into a Python dictionary object, then add to converted_data
    for candle in raw_data:
        # Dictionary object
        converted_candle = {
            'time': candle[0],
            'open': float(candle[1]),
            'high': float(candle[2]),
            'low': float(candle[3]),
            'close': float(candle[4]),
            'volume': float(candle[5]),
            'close_time': candle[6],
            'quote_asset_volume': float(candle[7]),
            'number_of_trades': int(candle[8]),
            'taker_buy_base_asset_volume': float(candle[9]),
            'taker_buy_quote_asset_volume': float(candle[10])
        }
        # Add to converted_data
        converted_data.append(converted_candle)
    # Return converted data
    return converted_data

# Function to query Binance for all symbols with a base asset of BUSD
def query_quote_asset_list(quote_asset_symbol):
    # Retrieve a list of symbols from Binance. Returns as a dictionary
    symbol_dictionary = Spot().exchange_info()
    # Convert into a dataframe
    symbol_dataframe = pd.DataFrame(symbol_dictionary['symbols'])
    # Extract only those symbols with a base asset of BUSD and status of TRADING
    quote_symbol_dataframe = symbol_dataframe.loc[symbol_dataframe['quoteAsset'] == quote_asset_symbol]
    quote_symbol_dataframe = quote_symbol_dataframe.loc[quote_symbol_dataframe['status'] == "TRADING"]
    # Return base_symbol_dataframe
    return quote_symbol_dataframe

# Function to make a trade if params provided
def make_trade_with_params(params, project_settings):
    # See if we're testing. Default to yes.
    if project_settings['Testing'] == "False":
        print("Real Trade")
        # Set the API Key
        api_key = project_settings['BinanceKeys']['API_Key']
        # Set the secret key
        secret_key = project_settings['BinanceKeys']['Secret_Key']
        # Setup the client
        client = Spot(api_key, secret_key)
    else:
        print("Testing Trading")
        # Set the Test API Key
        api_key = project_settings['TestKeys']['Test_API_Key']
        # Set the Test Secret Key
        secret_key = project_settings['TestKeys']['Test_Secret_Key']
        client = Spot(api_key, secret_key, base_url="https://testnet.binance.vision")

    # Make the trade
    try:
        response = client.new_order(**params)
        return response
    except ConnectionRefusedError as error:
        print(f"Found error. {error}")
        
# Function to query open trades
def query_open_trades(project_settings):
    # See if we're testing. Default to yes.
    if project_settings['Testing'] == "False":
        # Set the API Key
        api_key = project_settings['BinanceKeys']['API_Key']
        # Set the secret key
        secret_key = project_settings['BinanceKeys']['Secret_Key']
        # Setup the client
        client = Spot(api_key, secret_key)
    else:
        # Set the Test API Key
        api_key = project_settings['TestKeys']['Test_API_Key']
        # Set the Test Secret Key
        secret_key = project_settings['TestKeys']['Test_Secret_Key']
        client = Spot(api_key, secret_key, base_url="https://testnet.binance.vision")

    # Cancel the trade
    try:
        response = client.get_open_orders()
        return response
    except ConnectionRefusedError as error:
        print(f"Found error {error}")
        
# Function to cancel a trade
def cancel_order_by_symbol(symbol, project_settings):
    # See if we're testing. Default to yes.
    if project_settings['Testing'] == "False":
        # Set the API Key
        api_key = project_settings['BinanceKeys']['API_Key']
        # Set the secret key
        secret_key = project_settings['BinanceKeys']['Secret_Key']
        # Setup the client
        client = Spot(api_key, secret_key)
    else:
        print("Testing Trading")
        # Set the Test API Key
        api_key = project_settings['TestKeys']['Test_API_Key']
        # Set the Test Secret Key
        secret_key = project_settings['TestKeys']['Test_Secret_Key']
        client = Spot(api_key, secret_key, base_url="https://testnet.binance.vision")

    # Cancel the trade
    try:
        response = client.cancel_open_orders(symbol=symbol)
        return response
    except ConnectionRefusedError as error:
        print(f"Found error {error}")
        
def query_orderbook(project_settings, symbol= 'BUSDUSDT', multiple=False):
    if project_settings['Testing'] == "False":
        # Set the API Key
        api_key = project_settings['BinanceKeys']['API_Key']
        # Set the secret key
        secret_key = project_settings['BinanceKeys']['Secret_Key']
        client = Client(api_key=api_key, api_secret=secret_key)
    else:
        print("Testing Trading")
        # Set the Test API Key
        api_key = project_settings['TestKeys']['Test_API_Key']
        # Set the Test Secret Key
        secret_key = project_settings['TestKeys']['Test_Secret_Key']
        client = Client(api_key=api_key, api_secret=secret_key, testnet=True)
    if multiple:
        order_book = client.get_orderbook_tickers()
    else:
        order_book = client.get_orderbook_tickers(symbol=symbol)
    order_book = json_normalize(order_book)
    return order_book

def make_test_client(project_settings):
    # Set the Test API Key
    api_key = project_settings['TestKeys']['Test_API_Key']
    # Set the Test Secret Key
    secret_key = project_settings['TestKeys']['Test_Secret_Key']
    client = Client(api_key=api_key, api_secret=secret_key, testnet=True)
    return client
    
    
    