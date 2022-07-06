
import requests
import pandas as pd
import numpy as np
rpc_api = "https://near-mainnet--rpc--full.datahub.figment.io/apikey/5adf4eb5412ee7ef65a98c9c783f7662/"
rpc_indexer_api = "https://near--indexer.datahub.figment.io/apikey/5adf4eb5412ee7ef65a98c9c783f7662/"

def view_account(account_id):
    url = rpc_indexer_api + "accounts/" + account_id
    response = requests.get(url)
    return response.json()

def near_decode(account):
    response_account = view_account(account)
    df = pd.DataFrame(np.array([[account, int(response_account["amount"])/10**24, int(response_account["staked_amount"])/10**24, response_account["block_height"], "1" if response_account["code_hash"]!="11111111111111111111111111111111" else "0"]]),
     columns=["account_id", "balance", "staked", "block", "is_smart_contract"])
    return df


print(near_decode('contract.main.burrow.near'))
