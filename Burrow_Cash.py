from faulthandler import disable
import time
import streamlit as st
import pandas as pd
from subgrounds.subgrounds import Subgrounds
import subgrounds
import plotly.graph_objects as go
from helpers.burrow import *
from helpers.helpers import *
from my_pickledb import LoadPickleDB
# from test import *
# Refresh every 30 seconds
st.set_page_config(page_icon="🦔", page_title="Burrow Dashboard", layout="centered")
db = LoadPickleDB("./helpers/burrow.db")

sg = Subgrounds()
subgraph = sg.load_subgraph("https://api.thegraph.com/subgraphs/name/azizaiden/burrow-tst1")
dailysnapshot = subgraph.Query.deposits(
    orderBy="timestamp", where={"asset":"wrap.near"}, orderDirection="asc", first=30000)
    
# ticker = st_autorefresh(interval=REFRESH_INTERVAL_SEC * 1000, key="ticker")
st.header("🦔 Burrow Dashboard")

def accountShow():
    
    with st.spinner('Loading data...'):
        with st.expander("Account explorer", expanded=True):
            account_near = st.session_state.get('account_input', None)
            try:
                near_helper = near_decode(account_near)
            except:
                st.write("Account not found")
                return
            if near_helper.is_smart_contract[0] == "1":
                st.header("Smart Contract Info for " + account_near)
            else:
                st.header("Account Info for " + account_near)
            col1, col2, col3 = st.columns(3)
            col1.metric("Balance", str(round(float(near_helper.balance[0]), 2)) + " NEAR")
            col2.metric("Staked", str(round(float(near_helper.staked[0]), 2)) + " NEAR")
            col3.metric("Block Height", near_helper.block[0])
            # st.write(near_helper)
            st.write("")
            st.markdown("#### Burrow")
            col1, col2 = st.columns(2)
            
            with col1: 
                try:
                    st.markdown("##### Deposits")
                    df_near_acc = fetch_data_burrow_near_sg(account_near)
                    df_eth_acc = fetch_data_burrow_eth_sg(account_near)
                    df_usn_acc = fetch_data_burrow_usn_sg(account_near)
                    df_dai_acc = fetch_data_burrow_dai_sg(account_near)
                    df_usdt_acc = fetch_data_burrow_usdt_sg(account_near)
                    df_acc = pd.concat([df_near_acc, df_eth_acc, df_usn_acc, df_dai_acc, df_usdt_acc])
                    plot_deposits(df_acc, dict_dfs={"near": df_near_acc, "eth": df_eth_acc, "usn": df_usn_acc, "dai": df_dai_acc, "usdt": df_usdt_acc}, 
                dfs={"near": df_near_acc, "eth": df_eth_acc, "usn": df_usn_acc, "dai": df_dai_acc, "usdt": df_usdt_acc})
                except:
                    st.write("No deposits found")
            with col2:
                try:
                    st.markdown("##### Withdraws")
                    df_near_acc = fetch_data_burrow_near_sg(account_near, True)
                    df_eth_acc = fetch_data_burrow_eth_sg(account_near, True)
                    df_usn_acc = fetch_data_burrow_usn_sg(account_near, True)
                    df_dai_acc = fetch_data_burrow_dai_sg(account_near, True)
                    df_usdt_acc = fetch_data_burrow_usdt_sg(account_near, True)
                    df_acc = pd.concat([df_near_acc, df_eth_acc, df_usn_acc, df_dai_acc, df_usdt_acc])
                    plot_deposits(df_acc, dict_dfs={"near": df_near_acc, "eth": df_eth_acc, "usn": df_usn_acc, "dai": df_dai_acc, "usdt": df_usdt_acc}, 
                    dfs={"near": df_near_acc, "eth": df_eth_acc, "usn": df_usn_acc, "dai": df_dai_acc, "usdt": df_usdt_acc})
                except:
                    st.write("No withdraws found")               
            

#import deposits from burrow
df_near_dep = fetch_data_burrow_near()
df_eth_dep = fetch_data_burrow_eth()
df_usn_dep = fetch_data_burrow_usn()
df_dai_dep = fetch_data_burrow_dai()
df_usdt_dep = fetch_data_burrow_usdt()
dict_dfs_dep = {"near": df_near_dep, "eth": df_eth_dep, "usn": df_usn_dep, "dai": df_dai_dep, "usdt": df_usdt_dep}
df_dep = pd.concat([df_near_dep, df_eth_dep, df_usn_dep, df_dai_dep, df_usdt_dep])

#import withdraws from burrow
df_near_wd = fetch_data_wd_burrow_near()
df_eth_wd = fetch_data_wd_burrow_eth()
df_usn_wd = fetch_data_wd_burrow_usn()
df_dai_wd = fetch_data_wd_burrow_dai()
df_usdt_wd = fetch_data_wd_burrow_usdt()
dict_dfs_wd = {"near": df_near_wd, "eth": df_eth_wd, "usn": df_usn_wd, "dai": df_dai_wd, "usdt": df_usdt_wd}
df_wd = pd.concat([df_near_wd, df_eth_wd, df_usn_wd, df_dai_wd, df_usdt_wd])

df_liqs = fetch_data_burrow_liqs()

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
        b=0,
        t=0,
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


with st.sidebar:
    # st.image('https://cdn-icons-png.flaticon.com/512/7016/7016535.png', width= 80)
    st.header("Near Analytics")
    account = st.text_input('Look up an account', placeholder='wrap.near', on_change=accountShow, key="account_input")
    st.write("")
    option = st.selectbox(
     'Select a protocol',
     ('Burrow Cash', 'Ref Finance', 'Rainbow Bridge', "Jumbo Exchange"), index=0, disabled=True)
    st.write("")
    


st.write("")
st.write("")
dau_users = db.get("burrow_users")
dau_users_pd = pd.read_json(dau_users, orient='index')
col1, col2 = st.columns(2)
col1.subheader("DAU")

with col1:
    plot_dau(dau_users_pd)

col2.subheader("Liquidations")
with col2:
    plot_liqs(df_liqs)

st.write("")
st.header("TVL")
df_tvl = fetch_data_tvl_defillama()
plot_tvl(df_tvl)


st.write("")
st.subheader("Deposits by Asset")
asset_list = ["near", "eth", "usn", "dai", "usdt"]
assets = st.multiselect("Select assets", asset_list, default=["near"])



wd_enable = st.checkbox("Show Withdraws", value=False, key="withdraws")
st.write("")
st.write("")

plot_deposits(df_dep, dict_dfs_dep)
if wd_enable:
    st.subheader("Withdraws by Asset")
    plot_deposits(df_wd, dict_dfs_wd)

hide_streamlit_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            </style>
            """
st.markdown(hide_streamlit_style, unsafe_allow_html=True) 