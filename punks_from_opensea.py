import requests, os
import pandas as PD
import numpy as NP

basic_url = 'https://api.opensea.io/api/v1/events'
headers = {
    "Accept": "application/json",
    "X-API-KEY": "52243987b9c140b1bd865f075499ce2c"
}

asset_contract_address = '0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb'
event_type = "successful"
only_openSea = "false"
limit = 20
occurred_after = "1609462800" # from this year, 1th Jan 2021

approved_account_collection, token_id_collection, num_sales_collection = [], [], []
background_color_collection, image_url_collection, image_preview_collection = [], [], []
animation_url_collection, name_collection, description_collection, owner_name_collection = [], [], [], []
owner_address_collection, created_date_collection, event_type_collection = [], [], []
from_account_collection = []
payment_symbol_collection, payment_quantity_collection, eth_price_collection = [], [], []
usd_price_collection, seller_name_collection, seller_address_collection = [], [], []
starting_price_collection, total_price_collection = [], []
transaction_block_number_collection, transaction_timestamp_collection, transaction_from_name_collection = [], [], []
transaction_from_address_collection, transaction_to_name_collection, transaction_to_address_collection = [], [], []
transaction_hash_collection, winner_name_collection, winner_address_collection = [], [], []

offset = 0

while True:
    print(f'Processing offset: {offset}')
    full_url = (f"{basic_url}?asset_contract_address={asset_contract_address}"
            + f"&only_opensea={only_openSea}&event_type={event_type}"
            + f"&offset={offset}&limit={limit}&occurred_after={occurred_after}")
    response = requests.request("GET", full_url, headers=headers)
    if response.status_code != 200:
        print(f'Invalid response: {response.status_code}')
        break
    try:
        result = response.json()
    except:
        print(f"Unable to dump json: {response.text}")
        break
    if 'asset_events' not in result:
        print(f"No valid records: result")
        break
    result = result['asset_events']

    for index in range(len(result)):
        single_result = result[index]
        approved_account_collection.append(single_result['approved_account'])
        if single_result['asset']:
            token_id_collection.append(single_result['asset']['token_id'])
            num_sales_collection.append(single_result['asset']['num_sales'])
            background_color_collection.append(single_result['asset']['background_color'])
            image_url_collection.append(single_result['asset']['image_url'])
            image_preview_collection.append(single_result['asset']['image_preview_url'])
            animation_url_collection.append(single_result['asset']['animation_url'])
            name_collection.append(single_result['asset']['name'])
            description_collection.append(single_result['asset']['description'])
            if single_result['asset']['owner']['user']:
                owner_name_collection.append(single_result['asset']['owner']['user']['username'])
            else:
                owner_name_collection.append(NP.nan)
            owner_address_collection.append(single_result['asset']['owner']['address'])
        else: # for asset bundle, bundles are groups of items for sale on OpenSea. You can buy them all at once in one transaction
            token_id_collection.append([_['token_id'] for _ in single_result['asset_bundle']['assets']])
            num_sales_collection.append([_['num_sales'] for _ in single_result['asset_bundle']['assets']])
            background_color_collection.append([_['background_color'] for _ in single_result['asset_bundle']['assets']])
            image_url_collection.append([_['image_url'] for _ in single_result['asset_bundle']['assets']])
            image_preview_collection.append([_['image_preview_url'] for _ in single_result['asset_bundle']['assets']])
            animation_url_collection.append([_['animation_url'] for _ in single_result['asset_bundle']['assets']])
            name_collection.append([_['name'] for _ in single_result['asset_bundle']['assets']])
            description_collection.append([_['description'] for _ in single_result['asset_bundle']['assets']])
            owner_name_collection.append([_['owner']['user']['username'] if _['owner']['user'] else NP.nan for _ in single_result['asset_bundle']['assets']])
            owner_address_collection.append([_['owner']['address'] for _ in single_result['asset_bundle']['assets']])

        created_date_collection.append(single_result['created_date'])
        event_type_collection.append(single_result['event_type'])


        from_account_collection.append(single_result['from_account'])
        payment_symbol_collection.append(single_result['payment_token']['symbol'])
        payment_quantity_collection.append(single_result['quantity'])
        eth_price_collection.append(single_result['payment_token']['eth_price'])
        usd_price_collection.append(single_result['payment_token']['usd_price'])

        if single_result['seller']['user']:
            seller_name_collection.append(single_result['seller']['user']['username'])
        else:
            seller_name_collection.append(NP.nan)

        seller_address_collection.append(single_result['seller']['address'])
        starting_price_collection.append(single_result['starting_price'])
        total_price_collection.append(single_result['total_price'])

        transaction_block_number_collection.append(single_result['transaction']['block_number'])
        transaction_timestamp_collection.append(single_result['transaction']['timestamp'])

        if single_result['transaction']['from_account']['user']:
            transaction_from_name_collection.append(single_result['transaction']['from_account']['user']['username'])
        else:
             transaction_from_name_collection.append(NP.nan)

        transaction_from_address_collection.append(single_result['transaction']['from_account']['address'])

        if single_result['transaction']['to_account']['user']:
            transaction_to_name_collection.append(single_result['transaction']['to_account']['user']['username'])
        else:
            transaction_to_name_collection.append(NP.nan)

        transaction_to_address_collection.append(single_result['transaction']['to_account']['address'])
        transaction_hash_collection.append(single_result['transaction']['transaction_hash'])

        if single_result['winner_account']['user']:
            winner_name_collection.append(single_result['winner_account']['user']['username'])
        else:
            winner_name_collection.append(NP.nan)

        winner_address_collection.append(single_result['winner_account']['address'])

    offset += limit

    # if offset >= 100:
    #     break

store_path = 'openSeaRecords'
if not os.path.exists(store_path):
    os.mkdir(store_path)

final_results = PD.DataFrame({
            'approved_account': approved_account_collection,
            'token_id': token_id_collection,
            'num_sales': num_sales_collection,
            'background_color': background_color_collection,
            'image_url': image_url_collection,
            'image_preview': image_preview_collection,
            'animation_url': animation_url_collection,
            'name': name_collection,
            'description': description_collection,
            'owner_name': owner_name_collection,
            'owner_address': owner_address_collection,
            'created_date': created_date_collection,
            'event_type': event_type_collection,
            'from_account': from_account_collection,
            'payment_symbol': payment_symbol_collection,
            'payment_quantity': payment_quantity_collection,
            'eth_price': eth_price_collection,
            'usd_price': usd_price_collection,
            'seller_name': seller_name_collection,
            'seller_address': seller_address_collection,
            'starting_price': starting_price_collection,
            'total_price': total_price_collection,
            'transaction_block_number': transaction_block_number_collection,
            'transaction_timestamp': transaction_timestamp_collection,
            'transaction_from_name': transaction_from_name_collection,
            'transaction_from_address': transaction_from_address_collection,
            'transaction_to_name': transaction_to_name_collection,
            'transaction_to_address': transaction_to_address_collection,
            'transaction_hash': transaction_hash_collection,
            'winner_name': winner_name_collection,
            'winner_address': winner_address_collection,
        })
final_results.to_csv(f'{store_path}/punks_{event_type}.csv')