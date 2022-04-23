# data from https://nonfungible.com/, an NFT real time tracking platform

import requests, os, time
import pandas as PD
import numpy as NP
store_path = 'data'
if not os.path.exists(store_path):
    os.mkdir(store_path)

# decode as json
# [{"id":"saleType","value":""}]&internal=true&length=10&sort=[{"id":"blockTimestamp","desc":true}]&
base_url = 'https://nonfungible.com/api/v4/market/history/mekaverse?filter=%5B%7B%22id%22%3A%22saleType%22%2C%22value%22%3A%22%22%7D%5D&internal=true&length=10&sort=%5B%7B%22id%22%3A%22blockTimestamp%22%2C%22desc%22%3Atrue%7D%5D&start='

blockchain, transactionHash, blockNumber, blockTimestamp = [], [], [], []
assetId, project, nftAddress, nftTicker, marketAddress, usdPrice = [], [], [], [], [], []
tokenAddress, tokenTicker, totalPrice, totalDecimalPrice = [], [], [], []
feeUSDPrice, feeTotalPrice, feeTotalDecimalPrice, feeCollectors = [], [], [], []
saleType, buyer, seller, meta, image = [], [], [], [], []

offset = 0
while True:
    print(f'\rProcessing {offset}', end='')
    full_url = f'{base_url}{str(offset)}'
    response = requests.get(full_url)
    # try:
    if response.status_code == 200:
        response_log = response.json()
    else:
        print(f'\nEnd at offset {offset} failing [json], get {len(blockchain)} records')
        break
    if response_log['success']:
        total_records = response_log['data']['sales'][0]['sales']
        if len(total_records) == 0:
            print(f'End at offset {offset} failing [success], get {len(blockchain)} records')
            break
        for single_record in total_records:
            # single_record.keys()
            # single_record = total_records[0]
            blockchain.append(single_record['blockchain'])
            transactionHash.append(single_record['transactionHash'])
            blockNumber.append(single_record['blockNumber'])
            blockTimestamp.append(single_record['blockTimestamp'])
            assetId.append(single_record['assetId'])
            project.append(single_record['project'])
            nftAddress.append(single_record['nftAddress'])
            nftTicker.append(single_record['nftTicker'])
            marketAddress.append(single_record['marketAddress'])
            usdPrice.append(single_record['usdPrice'])
            tokenAddress.append(single_record['tokenAddress'])
            tokenTicker.append(single_record['tokenTicker'])
            totalPrice.append(single_record['totalPrice'])
            totalDecimalPrice.append(single_record['totalDecimalPrice'])
            feeUSDPrice.append(single_record['feeUSDPrice'])
            feeTotalPrice.append(single_record['feeTotalPrice'])
            feeTotalDecimalPrice.append(single_record['feeTotalDecimalPrice'])
            feeCollectors.append(single_record['feeCollectors'])
            saleType.append(single_record['saleType'])
            buyer.append(single_record['buyer'])
            seller.append(single_record['seller'])
            if 'meta' in single_record.keys():
                meta.append(single_record['meta'])
            else:
                meta.append(NP.nan)
            if 'image' in single_record.keys():
                image.append(single_record['image'])
            else:
                image.append(NP.nan)
        offset += len(total_records)
        time.sleep(5)
    else:
        print(f'End at offset {offset} failing [success], get {len(blockchain)} records')
        break
    # except:
    #     print(f'End at offset {offset} failing [json], get {len(blockchain)} records')
    #     break

transaction_records = PD.DataFrame({
    'blockchain': blockchain,
    'transactionHash': transactionHash,
    'blockNumber': blockNumber,
    'blockTimestamp': blockTimestamp,
    'assetId': assetId,
    'project': project,
    'nftAddress': nftAddress,
    'nftTicker': nftTicker,
    'marketAddress': marketAddress,
    'usdPrice': usdPrice,
    'tokenAddress': tokenAddress,
    'tokenTicker': tokenTicker,
    'totalPrice': totalPrice,
    'totalDecimalPrice': totalDecimalPrice,
    'feeUSDPrice': feeUSDPrice,
    'feeTotalPrice': feeTotalPrice,
    'feeTotalDecimalPrice': feeTotalDecimalPrice,
    'feeCollectors': feeCollectors,
    'saleType': saleType,
    'buyer': buyer,
    'seller': seller,
    'meta': meta,
    'image': image
})
transaction_records.to_csv(f'{store_path}/mekaverse_transaction.csv')