from faulthandler import disable
import streamlit as st
import pandas as pd
from subgrounds.subgrounds import Subgrounds
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
from psycopg2 import Error
import requests
import json
from my_pickledb import LoadPickleDB
# Refresh every 30 seconds
REFRESH_INTERVAL_SEC = 30
sg = Subgrounds()
subgraph = sg.load_subgraph("https://api.thegraph.com/subgraphs/name/azizaiden/burrow-tst1")


#fetch liquidations
@st.experimental_memo(ttl=86400)
def fetch_data_burrow_liqs():
    liqs = subgraph.Query.liquidates(orderBy="timestamp", orderDirection="asc", first=30000)
    
    daily_liq = sg.query_df([
        liqs.id,
        liqs.collateralSum,
        liqs.timestamp])
    daily_liq = daily_liq.rename(columns= lambda x: x[len("liquidates_"):])
    daily_liq["date"] = pd.to_datetime(daily_liq["timestamp"], unit="ns")
    daily_liq = daily_liq.drop(columns=["id", "timestamp"])
    daily_liq = daily_liq.resample("D", on="date").sum()
    daily_liq.collateralSum = (daily_liq.collateralSum).astype(float)
    return daily_liq
#fetch deposits from burrow-tst1    
@st.experimental_memo(ttl=86400)
def fetch_data_burrow_near():
    dailysnapshot = subgraph.Query.deposits(orderBy="timestamp", where={"asset":"wrap.near"}, orderDirection="asc", first=30000)
    
    daily_df = sg.query_df([
        dailysnapshot.id,
        dailysnapshot.amount,
        dailysnapshot.asset,
        dailysnapshot.timestamp])
    daily_df = daily_df.rename(columns= lambda x: x[len("deposits_"):])
    daily_df["date"] = pd.to_datetime(daily_df["timestamp"], unit="s")
    daily_df = daily_df.drop(columns=["id", "timestamp", "asset"])
    daily_df = daily_df.resample("D", on="date").sum()
    daily_df.amount = (daily_df.amount / 10**24).astype(int)
    return daily_df
@st.experimental_memo(ttl=86400)
def fetch_data_burrow_eth():
    dailysnapshot = subgraph.Query.deposits(
        orderBy="timestamp", where={"asset":"aurora"}, orderDirection="asc", first=30000)
        
    daily_df = sg.query_df([
        dailysnapshot.id,
        dailysnapshot.amount,
        dailysnapshot.asset,
        dailysnapshot.timestamp])
    daily_df = daily_df.rename(columns= lambda x: x[len("deposits_"):])
    daily_df["date"] = pd.to_datetime(daily_df["timestamp"], unit="s")
    daily_df = daily_df.drop(columns=["id", "timestamp", "asset"])
    daily_df = daily_df.resample("D", on="date").sum()
    daily_df.amount = (daily_df.amount / 10**18).astype(int)
    daily_df.asset = "eth"
    return daily_df
@st.experimental_memo(ttl=86400)
def fetch_data_burrow_usn():
    dailysnapshot = subgraph.Query.deposits(
        orderBy="timestamp", where={"asset":"usn"}, orderDirection="asc", first=30000)
        
    daily_df = sg.query_df([
        dailysnapshot.id,
        dailysnapshot.amount,
        dailysnapshot.asset,
        dailysnapshot.timestamp])
    print('done')
    daily_df = daily_df.rename(columns= lambda x: x[len("deposits_"):])
    daily_df["date"] = pd.to_datetime(daily_df["timestamp"], unit="s")
    daily_df = daily_df.drop(columns=["id", "timestamp", "asset"])
    # daily_df["date"] = daily_df["date"].astype('datetime64[ns]')
    # print(daily_df)
    daily_df = daily_df.resample("D", on="date").sum()
    daily_df.amount = (daily_df.amount / 10**18).astype(int)
    daily_df.asset = "usn"
    return daily_df
@st.experimental_memo(ttl=86400)
def fetch_data_burrow_dai():
    dailysnapshot = subgraph.Query.deposits(
        orderBy="timestamp", where={"asset":"6b175474e89094c44da98b954eedeac495271d0f.factory.bridge.near"}, orderDirection="asc", first=30000)
        
    daily_df = sg.query_df([
        dailysnapshot.id,
        dailysnapshot.amount,
        dailysnapshot.asset,
        dailysnapshot.timestamp])
    print('done')
    daily_df = daily_df.rename(columns= lambda x: x[len("deposits_"):])
    daily_df["date"] = pd.to_datetime(daily_df["timestamp"], unit="s")
    daily_df = daily_df.drop(columns=["id", "timestamp", "asset"])
    # daily_df["date"] = daily_df["date"].astype('datetime64[ns]')
    # print(daily_df)
    daily_df = daily_df.resample("D", on="date").sum()
    daily_df.amount = (daily_df.amount / 10**18).astype(int)
    daily_df.asset = "dai"
    return daily_df
@st.experimental_memo(ttl=86400)
def fetch_data_burrow_usdt():
    dailysnapshot = subgraph.Query.deposits(
        orderBy="timestamp", where={"asset":"dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near"}, orderDirection="asc", first=30000)
        
    daily_df = sg.query_df([
        dailysnapshot.id,
        dailysnapshot.amount,
        dailysnapshot.asset,
        dailysnapshot.timestamp])
    # print('done')
    daily_df = daily_df.rename(columns= lambda x: x[len("deposits_"):])
    daily_df["date"] = pd.to_datetime(daily_df["timestamp"], unit="s")
    daily_df = daily_df.drop(columns=["id", "timestamp", "asset"])
    # daily_df["date"] = daily_df["date"].astype('datetime64[ns]')
    # print(daily_df)
    daily_df = daily_df.resample("D", on="date").sum()
    daily_df.amount = (daily_df.amount / 10**18).astype(int)
    daily_df.asset = "usdt"
    return daily_df
def fetch_data_burrow_near_sg(acc, wd=False):
    if wd:
        dailysnapshot = subgraph.Query.withdraws(orderBy="timestamp", where={"asset":"wrap.near", "signerId": acc}, orderDirection="asc", first=1000)
    else:
        dailysnapshot = subgraph.Query.deposits(orderBy="timestamp", where={"asset":"wrap.near", "signerId": acc}, orderDirection="asc", first=1000)
    
    daily_df = sg.query_df([
        dailysnapshot.id,
        dailysnapshot.amount,
        dailysnapshot.asset,
        dailysnapshot.timestamp])
    if daily_df.empty:
        return None
    if wd:
        daily_df = daily_df.rename(columns= lambda x: x[len("withdraws_"):])
        daily_df["date"] = pd.to_datetime(daily_df["timestamp"], unit='ns')

    else:
        daily_df = daily_df.rename(columns= lambda x: x[len("deposits_"):])
        daily_df["date"] = pd.to_datetime(daily_df["timestamp"], unit='s')
    daily_df = daily_df.drop(columns=["id", "timestamp", "asset"])
    daily_df = daily_df.resample("D", on="date").sum()
    daily_df.amount = (daily_df.amount / 10**24).astype(int)
    return daily_df
def fetch_data_burrow_eth_sg(acc,wd=False):
    if wd:
        dailysnapshot = subgraph.Query.withdraws(orderBy="timestamp", where={"asset":"aurora", "signerId": acc}, orderDirection="asc", first=1000)
    else:
        dailysnapshot = subgraph.Query.deposits(
            orderBy="timestamp", where={"asset":"aurora", "signerId": acc}, orderDirection="asc", first=100)
        
    daily_df = sg.query_df([
        dailysnapshot.id,
        dailysnapshot.amount,
        dailysnapshot.asset,
        dailysnapshot.timestamp])
    if daily_df.empty:
        return None
    if wd:
        daily_df = daily_df.rename(columns= lambda x: x[len("withdraws_"):])
        daily_df["date"] = pd.to_datetime(daily_df["timestamp"], unit='ns')

    else:

        daily_df = daily_df.rename(columns= lambda x: x[len("deposits_"):])
        daily_df["date"] = pd.to_datetime(daily_df["timestamp"], unit='s')
    daily_df = daily_df.drop(columns=["id", "timestamp", "asset"])
    daily_df = daily_df.resample("D", on="date").sum()
    daily_df.amount = (daily_df.amount / 10**18).astype(int)
    return daily_df
def fetch_data_burrow_usn_sg(acc, wd=False):
    if wd:
        dailysnapshot = subgraph.Query.withdraws(orderBy="timestamp", where={"asset":"usn", "signerId": acc}, orderDirection="asc", first=1000)
    else:
        dailysnapshot = subgraph.Query.deposits(orderBy="timestamp", where={"asset":"usn", "signerId": acc}, orderDirection="asc", first=30000)
        
    daily_df = sg.query_df([
        dailysnapshot.id,
        dailysnapshot.amount,
        dailysnapshot.asset,
        dailysnapshot.timestamp])
    if daily_df.empty:
        return None
    if wd:
        daily_df = daily_df.rename(columns= lambda x: x[len("withdraws_"):])
        daily_df["date"] = pd.to_datetime(daily_df["timestamp"], unit='ns')
    else:
        daily_df = daily_df.rename(columns= lambda x: x[len("deposits_"):])
        daily_df["date"] = pd.to_datetime(daily_df["timestamp"], unit="s")
    daily_df = daily_df.drop(columns=["id", "timestamp", "asset"])
    # daily_df["date"] = daily_df["date"].astype('datetime64[ns]')
    # print(daily_df)
    daily_df = daily_df.resample("D", on="date").sum()
    daily_df.amount = (daily_df.amount / 10**18).astype(int)
    return daily_df
def fetch_data_burrow_dai_sg(acc,wd=False):
    
    if wd:
        dailysnapshot = subgraph.Query.withdraws(orderBy="timestamp", where={"asset":"6b175474e89094c44da98b954eedeac495271d0f.factory.bridge.near", "signerId": acc}, orderDirection="asc", first=1000)
    else:
        dailysnapshot = subgraph.Query.deposits(
            orderBy="timestamp", where={"signerId":acc, "asset":"6b175474e89094c44da98b954eedeac495271d0f.factory.bridge.near"}, orderDirection="asc", first=30000)
        
    daily_df = sg.query_df([
        dailysnapshot.id,
        dailysnapshot.amount,
        dailysnapshot.asset,
        dailysnapshot.timestamp])
    if daily_df.empty:
        return None
    if wd:
        daily_df = daily_df.rename(columns= lambda x: x[len("withdraws_"):])
        daily_df["date"] = pd.to_datetime(daily_df["timestamp"], unit='ns')
    else:
        daily_df = daily_df.rename(columns= lambda x: x[len("deposits_"):])
        daily_df["date"] = pd.to_datetime(daily_df["timestamp"], unit="s")
    daily_df = daily_df.drop(columns=["id", "timestamp", "asset"])
    # daily_df["date"] = daily_df["date"].astype('datetime64[ns]')
    # print(daily_df)
    daily_df = daily_df.resample("D", on="date").sum()
    daily_df.amount = (daily_df.amount / 10**18).astype(int)
    return daily_df
    
def fetch_data_burrow_usdt_sg(acc,wd=False):
    if wd:
        dailysnapshot = subgraph.Query.withdraws(orderBy="timestamp", where={"asset":"dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near", "signerId": acc}, orderDirection="asc", first=1000)
    else:
        dailysnapshot = subgraph.Query.deposits(
            orderBy="timestamp", where={"signerId":acc, "asset":"dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near"}, orderDirection="asc", first=30000)
        
    daily_df = sg.query_df([
        dailysnapshot.id,
        dailysnapshot.amount,
        dailysnapshot.asset,
        dailysnapshot.timestamp])
    if daily_df.empty:
        return None
    if wd:
        daily_df = daily_df.rename(columns= lambda x: x[len("withdraws_"):])
        daily_df["date"] = pd.to_datetime(daily_df["timestamp"], unit='ns')
    else:
        daily_df = daily_df.rename(columns= lambda x: x[len("deposits_"):])
        daily_df["date"] = pd.to_datetime(daily_df["timestamp"], unit="s")
    daily_df = daily_df.drop(columns=["id", "timestamp", "asset"])
    # daily_df["date"] = daily_df["date"].astype('datetime64[ns]')
    # print(daily_df)
    daily_df = daily_df.resample("D", on="date").sum()
    daily_df.amount = (daily_df.amount / 10**18).astype(int)
    return daily_df


#fetch withdraws from burrow
@st.experimental_memo(ttl=86400)
def fetch_data_wd_burrow_near():
    dailysnapshot = subgraph.Query.withdraws(orderBy="timestamp", where={"asset":"wrap.near"}, orderDirection="asc", first=30000)
    
    daily_df = sg.query_df([
        dailysnapshot.id,
        dailysnapshot.amount,
        dailysnapshot.asset,
        dailysnapshot.timestamp])
    daily_df = daily_df.rename(columns= lambda x: x[len("withdraws_"):])
    daily_df["date"] = pd.to_datetime(daily_df["timestamp"], unit="ns")
    daily_df = daily_df.drop(columns=["id", "timestamp", "asset"])
    daily_df = daily_df.resample("D", on="date").sum()
    daily_df.amount = (daily_df.amount / 10**24).astype(int)
    return daily_df
@st.experimental_memo(ttl=86400)
def fetch_data_wd_burrow_eth():
    dailysnapshot = subgraph.Query.withdraws(
        orderBy="timestamp", where={"asset":"aurora"}, orderDirection="asc", first=30000)
        
    daily_df = sg.query_df([
        dailysnapshot.id,
        dailysnapshot.amount,
        dailysnapshot.asset,
        dailysnapshot.timestamp])
    daily_df = daily_df.rename(columns= lambda x: x[len("withdraws_"):])
    daily_df["date"] = pd.to_datetime(daily_df["timestamp"], unit="ns")
    daily_df = daily_df.drop(columns=["id", "timestamp", "asset"])
    daily_df = daily_df.resample("D", on="date").sum()
    daily_df.amount = (daily_df.amount / 10**18).astype(int)
    daily_df.asset = "eth"
    return daily_df
@st.experimental_memo(ttl=86400)
def fetch_data_wd_burrow_usn():
    dailysnapshot = subgraph.Query.withdraws(
        orderBy="timestamp", where={"asset":"usn"}, orderDirection="asc", first=30000)
        
    daily_df = sg.query_df([
        dailysnapshot.id,
        dailysnapshot.amount,
        dailysnapshot.asset,
        dailysnapshot.timestamp])
    print('done')
    daily_df = daily_df.rename(columns= lambda x: x[len("withdraws_"):])
    daily_df["date"] = pd.to_datetime(daily_df["timestamp"], unit="ns")
    daily_df = daily_df.drop(columns=["id", "timestamp", "asset"])
    # daily_df["date"] = daily_df["date"].astype('datetime64[ns]')
    # print(daily_df)
    daily_df = daily_df.resample("D", on="date").sum()
    daily_df.amount = (daily_df.amount / 10**18).astype(int)
    daily_df.asset = "usn"
    return daily_df
@st.experimental_memo(ttl=86400)
def fetch_data_wd_burrow_dai():
    dailysnapshot = subgraph.Query.withdraws(
        orderBy="timestamp", where={"asset":"6b175474e89094c44da98b954eedeac495271d0f.factory.bridge.near"}, orderDirection="asc", first=30000)
        
    daily_df = sg.query_df([
        dailysnapshot.id,
        dailysnapshot.amount,
        dailysnapshot.asset,
        dailysnapshot.timestamp])
    print('done')
    daily_df = daily_df.rename(columns= lambda x: x[len("withdraws_"):])
    daily_df["date"] = pd.to_datetime(daily_df["timestamp"], unit="ns")
    daily_df = daily_df.drop(columns=["id", "timestamp", "asset"])
    # daily_df["date"] = daily_df["date"].astype('datetime64[ns]')
    # print(daily_df)
    daily_df = daily_df.resample("D", on="date").sum()
    daily_df.amount = (daily_df.amount / 10**18).astype(int)
    daily_df.asset = "dai"
    return daily_df
@st.experimental_memo(ttl=86400)
def fetch_data_wd_burrow_usdt():

    dailysnapshot = subgraph.Query.withdraws(
        orderBy="timestamp", where={"asset":"dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near"}, orderDirection="asc", first=30000)
        
    daily_df = sg.query_df([
        dailysnapshot.id,
        dailysnapshot.amount,
        dailysnapshot.asset,
        dailysnapshot.timestamp])
    # print('done')
    daily_df = daily_df.rename(columns= lambda x: x[len("withdraws_"):])
    print(daily_df)
    daily_df["date"] = pd.to_datetime(daily_df["timestamp"], unit="ns")
    daily_df = daily_df.drop(columns=["id", "timestamp", "asset"])
    # daily_df["date"] = daily_df["date"].astype('datetime64[ns]')
    # print(daily_df)
    daily_df = daily_df.resample("D", on="date").sum()
    daily_df.amount = (daily_df.amount / 10**18).astype(int)
    daily_df.asset = "usdt"
    return daily_df


#get TVL from defillama
defillama_api = "https://api.llama.fi/protocol/burrow"

@st.experimental_memo(ttl=86400)
def fetch_data_tvl_defillama():
    data = requests.get(defillama_api).json()
    data_tvl = data["chainTvls"]["Near"]["tvl"]
    data_borrowed = data["chainTvls"]["borrowed"]["tvl"]
    pd_tvl = pd.json_normalize(data_tvl)
    pd_tvl.set_index("date", inplace=True)
    pd_borrowed = pd.json_normalize(data_borrowed)
    pd_borrowed = pd_borrowed.rename(columns={"totalLiquidityUSD": "borrowed"})
    pd_borrowed.set_index("date", inplace=True)
    pd_tvl["totalLiquidityUSD"] = pd_tvl["totalLiquidityUSD"] + pd_borrowed["borrowed"]
    pd_tvl = pd.merge(pd_tvl, pd_borrowed, left_index=True, right_index=True)
    print(pd_tvl)
    pd_tvl.index = pd.to_datetime(pd_tvl.index, unit="s")    
    return pd_tvl
def plot_dau(df):
    fig = px.line(df, labels={
                     "index": ""
                     
                 },)
    fig.update_layout(
    
    showlegend=False, 
    margin=dict(
        l=5,
        r=5,
        b=20,
        t=20,
        pad=4
    ),
    template="plotly_dark",
    xaxis_tickfont_size=14,
    xaxis=dict(
        rangeselector=dict(
            buttons=list([
                dict(count=7,
                     label="1w",
                     step="day",
                     stepmode="backward"),
                dict(count=1,
                     label="1m",
                     step="month",
                     stepmode="backward"),
                dict(count=3,
                     label="3m",
                     step="month",
                     stepmode="backward"),

                dict(step="all")
            ]),
            font={"color": "black"}
        ),
        type="date",
        tickcolor='#8e8e8e',
    ),
    yaxis=dict(
        title='Daily Users',
        titlefont_size=16,
        tickfont_size=14,
        scaleanchor= "x"
    ),
    legend=dict(
        x=0,
        y=1.0,
        bgcolor='rgba(255, 255, 255, 0)',
        bordercolor='rgba(255, 255, 255, 0)'
    ),
    plot_bgcolor='rgba(255, 0, 0, 0)',
    barmode='group',
    height=300,
    bargap=0.15, # gap between bars of adjacent location coordinates.
    bargroupgap=0.1) # gap between bars of the same location coordinate.)

    st.plotly_chart(fig, use_container_width=True)

def plot_liqs(df):
    fig = px.line(df, labels={
                     "index": ""
                     
                 },)
    fig.update_layout(
    
    showlegend=False, 
    margin=dict(
        l=5,
        r=5,
        b=20,
        t=20,
        pad=4
    ),
    template="plotly_dark",
    xaxis_tickfont_size=14,
    xaxis=dict(
        rangeselector=dict(
            buttons=list([
                dict(count=7,
                     label="1w",
                     step="day",
                     stepmode="backward"),
                dict(count=1,
                     label="1m",
                     step="month",
                     stepmode="backward"),
                dict(count=3,
                     label="3m",
                     step="month",
                     stepmode="backward"),

                dict(step="all")
            ]),
            font={"color": "black"}
        ),
        type="date",
        tickcolor='#8e8e8e',
    ),
    yaxis=dict(
        title='Liquidations (USD)',
        titlefont_size=16,
        tickfont_size=14,
        scaleanchor= "x"
    ),
    legend=dict(
        x=0,
        y=1.0
    ),
    plot_bgcolor='rgba(255, 0, 0, 0)',
    height=300)
    # bargap=0.15, # gap between bars of adjacent location coordinates.
    # bargroupgap=0.1) # gap between bars of the same location coordinate.)

    st.plotly_chart(fig, use_container_width=True)


def plot_tvl(df):
    fig = px.line(df, labels={"variable": ""})
    fig.update_layout(

    showlegend=True, 
    margin=dict(
        l=5,
        r=5,
        b=20,
        t=20,
        pad=4
    ),
    template="plotly_dark",
    xaxis_tickfont_size=14,
    xaxis=dict(
        rangeselector=dict(
            buttons=list([
                dict(count=7,
                     label="1w",
                     step="day",
                     stepmode="backward"),
                dict(count=1,
                     label="1m",
                     step="month",
                     stepmode="backward"),
                dict(count=3,
                     label="3m",
                     step="month",
                     stepmode="backward"),

                dict(step="all")
            ]),
            font={"color": "black"}
        ),
        type="date",
        tickcolor='#8e8e8e',
    ),
    yaxis=dict(
        title='totalLiquidityUSD',
        titlefont_size=16,
        tickfont_size=14,
        scaleanchor= "x"
    ),
    legend=dict(
        x=0.8,
        y=1.0,
        bgcolor='rgba(255, 255, 255, 0)',
        bordercolor='rgba(255, 255, 255, 0)'
    ),
    plot_bgcolor='rgba(255, 0, 0, 0)',
    barmode='group',
    height=300,
    bargap=0.15, # gap between bars of adjacent location coordinates.
    bargroupgap=0.1) # gap between bars of the same location coordinate.)

    st.plotly_chart(fig, use_container_width=True)
def plot_deposits(df,dict_dfs, dfs=None):
    fig = go.Figure()

    if dfs is None:
        dfs = {asset: dict_dfs[asset] for asset in assets}

    fig = go.Figure()
    for asset, df in dfs.items():
        if df is not None:
            fig.add_trace(go.Bar(x=df.index, y=df.amount, name=asset))
    fig.update_layout(
    showlegend=True, 
    margin=dict(
        l=5,
        r=5,
        b=60,
        t=20,
        pad=4
    ),
    template="plotly_dark",
    xaxis_tickfont_size=14,
    xaxis=dict(
        rangeselector=dict(
            buttons=list([
                dict(count=7,
                     label="1w",
                     step="day",
                     stepmode="backward"),
                dict(count=1,
                     label="1m",
                     step="month",
                     stepmode="backward"),
                dict(count=3,
                     label="3m",
                     step="month",
                     stepmode="backward"),

                dict(step="all")
            ]),
            font={"color": "black"}
        ),
        type="date",
        tickcolor='#8e8e8e',
    ),
    yaxis=dict(
        title='Amount (Tokens)',
        titlefont_size=16,
        tickfont_size=14,
    ),
    legend=dict(
        x=0,
        y=1.0,
        bgcolor='rgba(255, 255, 255, 0)',
        bordercolor='rgba(255, 255, 255, 0)'
    ),
    plot_bgcolor='rgba(255, 0, 0, 0)',
    barmode='group',
    height=300,
    bargap=0.15, # gap between bars of adjacent location coordinates.
    bargroupgap=0.1) # gap between bars of the same location coordinate.)
    st.plotly_chart(fig, use_container_width=True)

def Convert(tup, di):
    di = dict(tup)
    return di
      

def get_burrow_users():
    try:
        # Connect to an existing database
        connection = psycopg2.connect(user="public_readonly",
                                    password="nearprotocol",
                                    host="mainnet.db.explorer.indexer.near.dev",
                                    port="5432",
                                    database="mainnet_explorer")

        # Create a cursor to perform database operations
        cursor = connection.cursor()
        # Print PostgreSQL details
        print("PostgreSQL server information")
        print(connection.get_dsn_parameters(), "\n")
        # Executing a SQL query
        cursor.execute("""SELECT 
    public.transactions.signer_account_id, 
    public.transactions.block_timestamp::float
FROM 
    public.receipts 
INNER JOIN 
    public.transactions 
ON 
    ( 
        public.receipts.originated_from_transaction_hash = public.transactions.transaction_hash) 
WHERE 
    public.receipts.receiver_account_id = 'contract.main.burrow.near'
AND public.receipts.included_in_block_timestamp > '1654162538025899914'
LIMIT 100000;""")
        # Fetch result
        record = cursor.fetchall()
        fields = ['signerid', 'timestamp']
        dicts = [dict(zip(fields, d)) for d in record]
        return json.dumps(dicts)
        # print(json_res)

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

def save_on_db_users():
    db = LoadPickleDB("./burrow.db")
    jsonn = get_burrow_users()
    a = pd.read_json(jsonn, orient='records')
    a.timestamp = a.timestamp.dt.floor('D')
    print(a)
    a.drop_duplicates(subset=['signerid', 'timestamp'], inplace=True)
    print(a.groupby('timestamp').count().to_json(orient='index'))
    db.set('burrow_users', a.groupby('timestamp').count().to_json(orient='index'))
    db.save.as_json()


