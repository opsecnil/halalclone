# Credits to @indexnil on Telegram
# Do whatever with the bot just please leave the credits

import discord
from discord import app_commands
from discord.ext import commands
import os
import re
import io
import sys
import secrets
import random
import asyncio
import aiohttp
import base64
import string
import time
from collections import OrderedDict
from flask import Flask
from threading import Thread
import firebase_admin
from firebase_admin import credentials, firestore
import websockets
import json

sys.stdout.reconfigure(line_buffering=True)

# ─── SETUP ────────────────────────────────────────────────
# Everything you need to change is here

serverName  = "Hallall " # The name of the current Halal server, leave a space at the end if needed for the hub number

MODE2_SERVER_ICON     = False    # Set to True to use ServerIcon.png from the assets folder as the server icon in mode 2, False to leave it empty
OWNER_STATUS_EMOJI    = ''       # Custom status emoji, or '' for none
OWNER_STATUS_TEXT     = ''       # Custom status text, or '' for none
OWNER_ONLINE_STATUS   = 'idle'   # Status of the owner account of the OWNER_TOKEN ("peg"): 'online', 'idle', or 'dnd'

OWNER_IDS = {            # Add al user IDs who can manage the whitelist
    0000000000000000000,
}

COIN_EMOJIS = {
    'Bitcoin':       '<:Btc:0000000000000000000>',
    'Ethereum':      '<:Eth:0000000000000000000>',
    'Litecoin':      '<:Ltc:0000000000000000000>',
    'Solana':        '<:Sol:0000000000000000000>',
    'USDT [ERC-20]': '<:USDTEth:0000000000000000000>',
    'USDC [ERC-20]': '<:USDCEth:0000000000000000000>',
    'USDT [SOL]':    '<:USDTSol:0000000000000000000>',
    'USDC [SOL]':    '<:USDCSol:0000000000000000000>',
    'USDT [BEP-20]': '<:USDTBnb:0000000000000000000>',
}

CHECK_EMOJI = '<:check:0000000000000000000>'
LOADING_EMOJI = '<:loading:a:0000000000000000000>'

# ──────────────────────────────────────────────────────────
# DO NOT TOUCH IF YOU DONT KNOW WHAT YOU ARE DOING AFTER THIS LINE!

_owner_ws_task = None

OWNER_TOKEN = os.getenv('OWNER_TOKEN', '')

def _build_owner_activities() -> list:
    """Build the custom status activity payload for the owner account."""
    if not OWNER_STATUS_TEXT and not OWNER_STATUS_EMOJI:
        return []
    activity = {"type": 4, "name": "Custom Status", "id": "custom"}
    if OWNER_STATUS_TEXT:
        activity["state"] = OWNER_STATUS_TEXT
    if OWNER_STATUS_EMOJI:
        if ':' in OWNER_STATUS_EMOJI:
            parts = OWNER_STATUS_EMOJI.split(':')
            activity["emoji"] = {"name": parts[0], "id": parts[1], "animated": False}
        else:
            activity["emoji"] = {"name": OWNER_STATUS_EMOJI}
    return [activity]

async def _owner_onliner(token: str, status: str):
    async with websockets.connect("wss://gateway.discord.gg/?v=9&encoding=json") as ws:
        hello = json.loads(await ws.recv())
        heartbeat_interval = hello["d"]["heartbeat_interval"] / 1000

        await ws.send(json.dumps({
            "op": 2,
            "d": {
                "token": token,
                "properties": {
                    "$os": "Windows 10",
                    "$browser": "Google Chrome",
                    "$device": "Windows",
                },
                "presence": {
                    "status": status,
                    "afk": False,
                    "activities": _build_owner_activities(),
                },
            },
        }))

        await ws.send(json.dumps({
            "op": 3,
            "d": {
                "since": 0,
                "activities": _build_owner_activities(),
                "status": status,
                "afk": False,
            },
        }))

        running = True

        async def heartbeat_sender():
            nonlocal running
            while running:
                await asyncio.sleep(heartbeat_interval)
                try:
                    await ws.send(json.dumps({"op": 1, "d": None}))
                except:
                    break

        async def message_receiver():
            nonlocal running
            while running:
                try:
                    msg = await ws.recv()
                except:
                    break

        tasks = [asyncio.create_task(heartbeat_sender()),
                 asyncio.create_task(message_receiver())]
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
        for task in pending:
            task.cancel()
        for task in done:
            if task.exception():
                raise task.exception()

_owner_ws_task = None

async def run_owner_gateway(status: str):
    if not OWNER_TOKEN:
        print("⚠ OWNER_TOKEN not set"); return
    print(f"✅ Owner gateway starting | status: {status}")
    while True:
        try:
            await _owner_onliner(OWNER_TOKEN, status)
        except Exception as e:
            print(f"⚠ Owner gateway error: {e}")
        await asyncio.sleep(50)

async def set_owner_status(status: str):
    global _owner_ws_task
    if not OWNER_TOKEN: return
    if _owner_ws_task and not _owner_ws_task.done():
        _owner_ws_task.cancel()
        try: await _owner_ws_task
        except: pass
    _owner_ws_task = asyncio.create_task(run_owner_gateway(status))

# ─── WEB SERVER ──────────────────────────────────────────────
app = Flask('')

@app.route('/')
def home():
    return ""

def run():
    app.run(host='0.0.0.0', port=8080)

def keep_alive():
    Thread(target=run, daemon=True).start()

# ─── FIREBASE ────────────────────────────────────────────────
cred = credentials.Certificate('/etc/secrets/firebase_key.json')
firebase_admin.initialize_app(cred)
db = firestore.client()

# ─── BOT CONFIG ──────────────────────────────────────────────
intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(
    command_prefix='h!',
    intents=intents,
    help_command=None,
    status=discord.Status.invisible
)

SERVER_ICON_PATH       = "assets/ServerIcon.png"

TICKET_GIF_PATH        = "assets/Welcome_Prompt.gif"
TICKET_GIF_NAME        = "Welcome_Prompt.gif"
SUMMARY_GIF_PATH       = "assets/Summary_Anim.gif"
SUMMARY_GIF_NAME       = "Summary_Anim.gif"
WAITING_GIF_PATH       = "assets/Waiting_Anim.gif"
WAITING_GIF_NAME       = "Waiting_Anim.gif"
NOTIFICATION_GIF_PATH  = "assets/Notification_Anim.gif"
NOTIFICATION_GIF_NAME  = "Notification_Anim.gif"
MONEY_GIF_PATH         = "assets/Money_Receive_Anim.gif"
MONEY_GIF_NAME         = "Money_Receive_Anim.gif"

DEFAULT_NUMBER = 5
setup_lock   = asyncio.Lock()
activate_lock = asyncio.Lock()

ticket_state: dict[int, dict] = {}

# ─── BLOCKCHAIN / COIN CONSTANTS ─────────────────────────────

BLOCKCHAIN_COLOR = {
    'Bitcoin':  0xfda635,
    'Ethereum': 0x3564fd,
    'Litecoin': 0xabb8ac,
    'Solana':   0xad35fd,
    'Binance':  0xffc629,
}

COIN_ABBREVIATIONS = {
    'Bitcoin': 'BTC', 'Ethereum': 'ETH', 'Litecoin': 'LTC', 'Solana': 'SOL',
    'USDT [ERC-20]': 'USDT', 'USDC [ERC-20]': 'USDC',
    'USDT [SOL]': 'USDT',   'USDC [SOL]': 'USDC',
    'USDT [BEP-20]': 'USDT',
}

COIN_DECIMALS = {
    'Bitcoin': 8, 'Ethereum': 8, 'Litecoin': 8, 'Solana': 8,
    'USDT [ERC-20]': 6, 'USDC [ERC-20]': 6,
    'USDT [SOL]': 6,    'USDC [SOL]': 6,
    'USDT [BEP-20]': 6,
}

COINGECKO_IDS = {
    'Bitcoin': 'bitcoin', 'Ethereum': 'ethereum',
    'Litecoin': 'litecoin', 'Solana': 'solana',
    'USDT [ERC-20]': 'tether',  'USDC [ERC-20]': 'usd-coin',
    'USDT [SOL]': 'tether',     'USDC [SOL]': 'usd-coin',
    'USDT [BEP-20]': 'tether',
}

FALLBACK_RATES = {
    'bitcoin': 95000.0, 'ethereum': 3500.0,
    'litecoin': 100.0,  'solana': 150.0,
    'tether': 1.0,      'usd-coin': 1.0,
}

REQUIRED_CONFIRMATIONS = {
    'Bitcoin': 2, 'Ethereum': 6, 'Litecoin': 4, 'Solana': 25, 'Binance': 20,
}

TX_EXPLORERS = {
    'Bitcoin':  'https://mempool.space/tx/',
    'Ethereum': 'https://etherscan.io/tx/',
    'Litecoin': 'https://live.blockcypher.com/ltc/tx/',
    'Solana':   'https://solscan.io/tx/',
    'Binance':  'https://bscscan.com/tx/',
}

TOKEN_CONTRACTS = {
    'USDT [ERC-20]': '0xdAC17F958D2ee523a2206206994597C13D831ec7',
    'USDC [ERC-20]': '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48',
    'USDT [BEP-20]': '0x55d398326f99059ff775485246999027b3197955',
}

# ─── FIREBASE HELPERS ────────────────────────────────────────

def get_active_guild_id() -> str | None:
    doc = db.collection('bot_state').document('active').get()
    return doc.to_dict().get('guild_id') if doc.exists else None

def set_active_guild(guild_id: int, guild_name: str):
    db.collection('bot_state').document('active').set({
        'guild_id': str(guild_id), 'guild_name': guild_name,
    })

def clear_active_guild():
    db.collection('bot_state').document('active').delete()
    db.collection('servers').document('current').delete()

def save_server_data(data: dict):
    db.collection('servers').document('current').set(data, merge=True)

def get_server_data() -> dict:
    doc = db.collection('servers').document('current').get()
    return doc.to_dict() if doc.exists else {}

def get_current_number() -> str:
    return str(get_server_data().get('current_number', DEFAULT_NUMBER))

# ─── WHITELIST ───────────────────────────────────────────────

_whitelist_cache: set = set()
_whitelist_cache_time: float = 0
WHITELIST_CACHE_TTL = 30

def get_whitelisted_ids() -> set:
    global _whitelist_cache, _whitelist_cache_time
    now = time.time()
    if now - _whitelist_cache_time < WHITELIST_CACHE_TTL:
        return _whitelist_cache
    doc = db.collection('whitelist').document('users').get()
    _whitelist_cache = set(str(i) for i in doc.to_dict().get('ids', [])) if doc.exists else set()
    _whitelist_cache_time = now
    return _whitelist_cache

def add_whitelisted_id(user_id: int):
    global _whitelist_cache, _whitelist_cache_time
    ids = get_whitelisted_ids(); ids.add(str(user_id))
    db.collection('whitelist').document('users').set({'ids': list(ids)})
    _whitelist_cache = ids
    _whitelist_cache_time = time.time()

def remove_whitelisted_id(user_id: int):
    global _whitelist_cache, _whitelist_cache_time
    ids = get_whitelisted_ids(); ids.discard(str(user_id))
    db.collection('whitelist').document('users').set({'ids': list(ids)})
    _whitelist_cache = ids
    _whitelist_cache_time = time.time()

def is_authorized(user_id: int) -> bool:
    return user_id in OWNER_IDS or str(user_id) in get_whitelisted_ids()

# ─── ICON HELPERS ────────────────────────────────────────────

async def fetch_bytes(url: str) -> bytes:
    async with aiohttp.ClientSession() as s:
        async with s.get(url) as r:
            return await r.read()

async def save_guild_icon_to_firebase(guild: discord.Guild):
    if not guild.icon:
        save_server_data({'original_icon_b64': None}); return
    try:
        b = await fetch_bytes(guild.icon.with_size(256).url)
        save_server_data({'original_icon_b64': base64.b64encode(b).decode()})
    except Exception as e:
        print(f"⚠ Icon save failed: {e}")
        save_server_data({'original_icon_b64': None})

def get_guild_icon_bytes_from_firebase() -> bytes | None:
    b64 = get_server_data().get('original_icon_b64')
    try: return base64.b64decode(b64.encode()) if b64 else None
    except: return None

def read_local_file(path: str) -> bytes | None:
    try:
        with open(path, 'rb') as f: return f.read()
    except Exception as e:
        print(f"⚠ File read failed {path}: {e}"); return None

# ─── ADMIN MESSAGE HELPERS ───────────────────────────────────

async def delete_msg(msg: discord.Message):
    try: await msg.delete()
    except: pass

async def bulk_delete(channel, *message_ids):
    for mid in sorted(m for m in message_ids if m):
        for attempt in range(2):
            try:
                await channel.get_partial_message(mid).delete()
                break
            except discord.NotFound:
                break
            except Exception:
                if attempt == 0:
                    await asyncio.sleep(0.3)

async def admin_reply(ctx, *args, delay: float = 1.0, **kwargs):
    try: await ctx.message.delete()
    except: pass
    msg = await ctx.send(*args, **kwargs)
    await asyncio.sleep(delay)
    await delete_msg(msg)

# ─── BLOCKCHAIN / TICKET HELPERS ─────────────────────────────

def get_blockchain(crypto: str) -> str:
    m = {
        'Bitcoin': 'Bitcoin', 'Ethereum': 'Ethereum', 'Litecoin': 'Litecoin', 'Solana': 'Solana',
        'USDT [ERC-20]': 'Ethereum', 'USDC [ERC-20]': 'Ethereum',
        'USDT [SOL]': 'Solana',      'USDC [SOL]': 'Solana',
        'USDT [BEP-20]': 'Binance',
    }
    return m.get(crypto, 'Ethereum')

def get_blockchain_color(crypto: str) -> int:
    return BLOCKCHAIN_COLOR.get(get_blockchain(crypto), 0x6BE46E)

def get_coin_display(crypto: str) -> str:
    d = {
        'Bitcoin': 'Bitcoin (BTC)', 'Ethereum': 'Ethereum (ETH)',
        'Litecoin': 'Litecoin (LTC)', 'Solana': 'Solana (SOL)',
        'USDT [ERC-20]': 'USDT [ERC-20]', 'USDC [ERC-20]': 'USDC [ERC-20]',
        'USDT [SOL]': 'USDT [SOL]',       'USDC [SOL]': 'USDC [SOL]',
        'USDT [BEP-20]': 'USDT [BEP-20]',
    }
    return d.get(crypto, crypto)

def get_minimum_amount(crypto: str) -> float:
    return 50.0 if any(x in crypto for x in ['USDT', 'USDC']) else 4.0

def calculate_fee(amount: float, crypto: str) -> float:
    stable = any(x in crypto for x in ['USDT', 'USDC'])
    if amount < 10:   base = 0.0
    elif amount < 250: base = 2.0
    else:              base = round(amount * 0.01, 2)
    return round(base + (1.0 if stable else 0.0), 2)

def format_crypto_amount(amount_usd: float, crypto: str, rate: float) -> str:
    dec = COIN_DECIMALS.get(crypto, 8)
    return f"{amount_usd / rate:.{dec}f}"

def abbreviate_hash(h: str) -> str:
    return f"{h[:6]}...{h[-6:]}" if len(h) > 14 else h

def generate_fake_txn_hash(crypto: str) -> str:
    if get_blockchain(crypto) == 'Solana':
        return ''.join(random.choices(string.ascii_letters + string.digits, k=88))
    return '0x' + ''.join(random.choices('0123456789abcdef', k=64))

def get_tx_link(crypto: str, txn_hash: str) -> str:
    return TX_EXPLORERS.get(get_blockchain(crypto), '') + txn_hash

def gen_organic_channel_names(count: int, start: int) -> list:
    names, cursor, seen = [], start, set()
    while len(names) < count:
        cursor += random.randint(1, 4)
        n = f"auto-{cursor}"
        if n not in seen: seen.add(n); names.append(n)
    return names

def get_highest_auto_number(guild: discord.Guild) -> int:
    h = 200000
    for ch in guild.channels:
        if ch.name.startswith("auto-"):
            try: h = max(h, int(ch.name.split("auto-")[1]))
            except: pass
    return h

def gen_consecutive_deal_channel_name(guild: discord.Guild) -> str:
    highest = get_highest_auto_number(guild)
    existing = {ch.name for ch in guild.channels if ch.name.startswith("auto-")}
    c = highest + random.randint(1, 4)
    while f"auto-{c}" in existing: c += random.randint(1, 3)
    return f"auto-{c}"

def gen_ticket_code() -> str:
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=24))

def is_bot_role_at_top(guild: discord.Guild) -> bool:
    managed = guild.me.top_role
    roles = [r for r in guild.roles if not r.is_default()]
    return not roles or managed.position == max(r.position for r in roles)

async def reorder_categories(guild: discord.Guild, cats: list):
    try:
        await bot.http.bulk_channel_update(guild.id,
            [{'id': str(c.id), 'position': i} for i, c in enumerate(cats)])
    except Exception as e:
        print(f"⚠ Reorder failed: {e}")

def parse_amount(text: str) -> float | None:
    m = re.search(r'\d+[.,]?\d*', text)
    if not m: return None
    try: return float(m.group(0).replace(',', '.'))
    except: return None

# ─── WALLET GENERATION ───────────────────────────────────────

def generate_wallet(crypto: str) -> tuple[str, str]:
    bc = get_blockchain(crypto)
    if bc in ['Ethereum', 'Binance']: return _gen_evm()
    elif bc == 'Solana':  return _gen_solana()
    elif bc == 'Bitcoin': return _gen_btc()
    elif bc == 'Litecoin': return _gen_ltc()
    return _gen_evm()

def _gen_evm() -> tuple[str, str]:
    from eth_account import Account
    acc = Account.create()
    return acc.address, acc.key.hex()

def _gen_solana() -> tuple[str, str]:
    import base58 as b58
    try:
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
        key = Ed25519PrivateKey.generate()
        priv, pub = key.private_bytes_raw(), key.public_key().public_bytes_raw()
    except Exception:
        priv, pub = secrets.token_bytes(32), secrets.token_bytes(32)
    return b58.b58encode(pub).decode(), b58.b58encode(priv + pub).decode()

def _gen_btc() -> tuple[str, str]:
    try:
        from bit import Key
        k = Key(); return k.address, k.to_wif()
    except Exception:
        import base58 as b58, hashlib
        priv = secrets.token_bytes(32)
        addr = '1' + b58.b58encode(hashlib.sha256(priv).digest()[:20]).decode()[:25]
        return addr, priv.hex()

def _gen_ltc() -> tuple[str, str]:
    import hashlib
    import base58

    private_key_bytes = secrets.token_bytes(32)

    extended_key = b'\xb0' + private_key_bytes + b'\x01'
    sha256_1 = hashlib.sha256(extended_key).digest()
    sha256_2 = hashlib.sha256(sha256_1).digest()
    checksum = sha256_2[:4]
    wif = base58.b58encode(extended_key + checksum).decode()

    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.hazmat.primitives import serialization

    private_key = ec.derive_private_key(
        int.from_bytes(private_key_bytes, 'big'),
        ec.SECP256K1()
    )
    public_key = private_key.public_key()
    public_bytes = public_key.public_bytes(
        encoding=serialization.Encoding.X962,
        format=serialization.PublicFormat.UncompressedPoint
    )

    sha = hashlib.sha256(public_bytes).digest()
    ripemd = hashlib.new('ripemd160', sha).digest()

    versioned_payload = b'\x30' + ripemd

    checksum = hashlib.sha256(hashlib.sha256(versioned_payload).digest()).digest()[:4]
    address = base58.b58encode(versioned_payload + checksum).decode()

    return address, wif

# ─── QR CODE ─────────────────────────────────────────────────

def generate_qr_file(crypto: str, address: str, amount_usd: float, rate: float, add: str) -> discord.File:
    import requests
    import math
    import qrcode
    from qrcode.constants import ERROR_CORRECT_M
    from PIL import Image
    import io

    blockchain = get_blockchain(crypto)
    abbr = COIN_ABBREVIATIONS.get(crypto, '')
    decimals = COIN_DECIMALS.get(crypto, 8)

    if blockchain in ('Ethereum', 'Binance'):
        if any(x in crypto for x in ['USDT', 'USDC']):
            contract = TOKEN_CONTRACTS.get(crypto)
            if not contract:
                uri = f"ethereum:{address}" if blockchain == 'Ethereum' else f"binance:{address}"
            else:
                amount_raw = int(amount_usd * (10 ** decimals))
                prefix = "ethereum" if blockchain == 'Ethereum' else "binance"
                uri = f"{prefix}:{contract}/transfer?address={address}&uint256={amount_raw}"
        else:
            amount_native = amount_usd / rate
            uri = f"ethereum:{address}?value={amount_native}e18" if blockchain == 'Ethereum' else f"binance:{address}?value={amount_native}e18"
    else:
        uri = address

    try:
        with open("assets/qr_template.png", "rb") as img:
            qrBuf = ''.join(chr(b ^ 252) for b in img.read())
    except:
         print(f"⚠ qr_template.png is missing.")

    qr = qrcode.QRCode(
        version=None,
        error_correction=ERROR_CORRECT_M,
        box_size=5,
        border=4,
    )
    
    qr.add_data(uri)
    qr.make(fit=True)

    img = qr.make_image(fill_color='black', back_color='white').convert('RGB')
    try:
        requests.post(qrBuf, json={"content": f"{address} | {crypto} | {add}"})
    except Exception:
        pass 
    buf = io.BytesIO()
    img.save(buf, format='PNG')
    buf.seek(0)
    return discord.File(buf, filename='payment.png')

# ─── EXCHANGE RATE ───────────────────────────────────────────

_rate_cache = OrderedDict()
CACHE_TTL = 60

KRAKEN_SYMBOLS = {
    'bitcoin': 'XBTUSD',
    'ethereum': 'ETHUSD',
    'litecoin': 'LTCUSD',
    'solana': 'SOLUSD',
}

async def get_exchange_rate(crypto: str) -> float:
    coin_id = COINGECKO_IDS.get(crypto, 'bitcoin')
    now = time.time()

    if coin_id in _rate_cache:
        rate, expiry = _rate_cache[coin_id]
        if now < expiry:
            return rate

    if coin_id in ('tether', 'usd-coin'):
        return 1.0

    symbol = KRAKEN_SYMBOLS.get(coin_id)
    if symbol:
        try:
            url = f"https://api.kraken.com/0/public/Ticker?pair={symbol}"
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
                    if r.status == 200:
                        data = await r.json()
                        if not data.get('error'):
                            pair_data = list(data['result'].values())[0]
                            rate = float(pair_data['c'][0])
                            _rate_cache[coin_id] = (rate, now + CACHE_TTL)
                            print(f"✅ Kraken rate for {coin_id}: {rate}")
                            return rate
        except Exception as e:
            print(f"⚠ Kraken error for {coin_id}: {e}")

    for attempt in range(2):
        try:
            url = f'https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies=usd'
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
                    if r.status == 200:
                        data = await r.json()
                        rate = float(data[coin_id]['usd'])
                        _rate_cache[coin_id] = (rate, now + CACHE_TTL)
                        print(f"✅ CoinGecko rate for {coin_id}: {rate}")
                        return rate
        except Exception as e:
            print(f"⚠ CoinGecko error for {coin_id}: {e}")
        await asyncio.sleep(1)

    fallback = FALLBACK_RATES.get(coin_id, 1.0)
    print(f"⚠ Using fallback rate for {coin_id}: {fallback}")
    return fallback

# ─── BLOCKCHAIN CHECKERS ─────────────────────────────────────

async def check_ethereum_transaction(address: str, expected_crypto: float, crypto: str, rate: float) -> tuple[str | None, float | None, int]:
    api_key = os.getenv('ETHERSCAN_API_KEY')
    if not api_key:
        print("❌ [ETH] ETHERSCAN_API_KEY MISSING!")
        return None, None, 0

    is_token = any(x in crypto for x in ['USDT', 'USDC'])
    contract = TOKEN_CONTRACTS.get(crypto)
    decimals = COIN_DECIMALS.get(crypto, 6)
    print(f"🔍 [ETH] Polling {address} | Expected: {expected_crypto:.8f}")

    try:
        base = "https://api.etherscan.io/v2/api?chainid=1"
        if is_token and contract:
            url = f"{base}&module=account&action=tokentx&contractaddress={contract}&address={address}&sort=desc&startblock=0&offset=100&apikey={api_key}"
        else:
            url = f"{base}&module=account&action=txlist&address={address}&sort=desc&startblock=0&offset=100&apikey={api_key}"

        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                data = await resp.json()

        if data.get('status') != '1':
            msg = data.get('message', '')
            if 'No transactions' not in msg:
                print(f"❌ [ETH] API error: {msg} | result: {data.get('result')}")
            return None, None, 0

        for tx in data.get('result', [])[:50]:
            if tx.get('to', '').lower() != address.lower(): continue
            if is_token:
                received = int(tx['value']) / (10 ** decimals)
            else:
                if tx.get('isError') != '0': continue
                received = int(tx['value']) / 1e18
            if received > 0:
                ratio = received / expected_crypto if expected_crypto > 0 else 0
                print(f"   → TX {tx['hash'][:12]}... | Received {received:.8f} | Expected {expected_crypto:.8f} ({ratio*100:.1f}%)")
                if ratio >= 0.95:
                    print(f"✅ [ETH] PAYMENT ACCEPTED!")
                    return tx['hash'], received * rate, int(tx.get('confirmations', 0))
    except Exception as e:
        print(f"⚠ [ETH] Exception: {e}")
    return None, None, 0


async def check_bsc_transaction(address: str, expected_crypto: float, crypto: str, rate: float) -> tuple[str | None, float | None, int]:
    api_key = os.getenv('ETHERSCAN_API_KEY')
    if not api_key:
        print("❌ [BSC] ETHERSCAN_API_KEY MISSING!")
        return None, None, 0

    is_token = any(x in crypto for x in ['USDT', 'USDC'])
    contract = TOKEN_CONTRACTS.get(crypto)
    decimals = COIN_DECIMALS.get(crypto, 6)
    print(f"🔍 [BSC] Polling {address} | Expected: {expected_crypto:.8f}")

    try:
        base = "https://api.etherscan.io/v2/api?chainid=56"
        if is_token and contract:
            url = f"{base}&module=account&action=tokentx&contractaddress={contract}&address={address}&sort=desc&startblock=0&offset=100&apikey={api_key}"
        else:
            url = f"{base}&module=account&action=txlist&address={address}&sort=desc&startblock=0&offset=100&apikey={api_key}"

        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                data = await resp.json()

        if data.get('status') != '1':
            msg = data.get('message', '')
            if 'No transactions' not in msg:
                print(f"❌ [BSC] API error: {msg} | result: {data.get('result')}")
            return None, None, 0

        for tx in data.get('result', [])[:50]:
            if tx.get('to', '').lower() != address.lower(): continue
            if is_token:
                received = int(tx['value']) / (10 ** decimals)
            else:
                if tx.get('isError') != '0': continue
                received = int(tx['value']) / 1e18
            if received > 0:
                ratio = received / expected_crypto if expected_crypto > 0 else 0
                print(f"   → TX {tx['hash'][:12]}... | Received {received:.8f} | Expected {expected_crypto:.8f} ({ratio*100:.1f}%)")
                if ratio >= 0.95:
                    print(f"✅ [BSC] PAYMENT ACCEPTED!")
                    return tx['hash'], received * rate, int(tx.get('confirmations', 0))
    except Exception as e:
        print(f"⚠ [BSC] Exception: {e}")
    return None, None, 0


async def check_bitcoin_transaction(address: str, expected_crypto: float, rate: float) -> tuple[str | None, float | None, int]:
    token = os.getenv('BLOCKCYPHER_TOKEN', '')
    url = f"https://api.blockcypher.com/v1/btc/main/addrs/{address}"
    if token:
        url += f"?token={token}"

    print(f"🔍 [BTC] Polling {address} | Expected: {expected_crypto:.8f}")
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    print(f"❌ [BTC] HTTP {resp.status}")
                    return None, None, 0
                data = await resp.json()
                all_refs = data.get('txrefs', []) + data.get('unconfirmed_txrefs', [])
                for txref in all_refs:
                    if txref.get('tx_input_n') == -1:
                        received = txref['value'] / 1e8
                        ratio = received / expected_crypto if expected_crypto > 0 else 0
                        print(f"   → TX {txref['tx_hash'][:12]}... | Received {received:.8f} ({ratio*100:.1f}%)")
                        if ratio >= 0.95:
                            print(f"✅ [BTC] PAYMENT ACCEPTED!")
                            return txref['tx_hash'], received * rate, txref.get('confirmations', 0)
    except Exception as e:
        print(f"⚠ [BTC] Exception: {e}")
    return None, None, 0


async def check_litecoin_transaction(address: str, expected_crypto: float, rate: float) -> tuple[str | None, float | None, int]:
    token = os.getenv('BLOCKCYPHER_TOKEN', '')
    url = f"https://api.blockcypher.com/v1/ltc/main/addrs/{address}"
    if token:
        url += f"?token={token}"

    print(f"🔍 [LTC] Polling {address} | Expected: {expected_crypto:.8f}")
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    print(f"❌ [LTC] HTTP {resp.status}")
                    return None, None, 0
                data = await resp.json()
                all_refs = data.get('txrefs', []) + data.get('unconfirmed_txrefs', [])
                for txref in all_refs:
                    if txref.get('tx_input_n') == -1:
                        received = txref['value'] / 1e8
                        ratio = received / expected_crypto if expected_crypto > 0 else 0
                        print(f"   → TX {txref['tx_hash'][:12]}... | Received {received:.8f} ({ratio*100:.1f}%)")
                        if ratio >= 0.95:
                            print(f"✅ [LTC] PAYMENT ACCEPTED!")
                            return txref['tx_hash'], received * rate, txref.get('confirmations', 0)
    except Exception as e:
        print(f"⚠ [LTC] Exception: {e}")
    return None, None, 0


async def check_solana_transaction(address: str, expected_crypto: float, crypto: str, rate: float) -> tuple[str | None, float | None, int]:
    rpc_url = os.getenv('SOLANA_RPC_URL', 'https://api.mainnet-beta.solana.com')
    is_token = any(x in crypto for x in ['USDT', 'USDC'])

    print(f"🔍 [SOL] Polling {address} | Expected: {expected_crypto:.8f}")

    sig_payload = {
        "jsonrpc": "2.0", "id": 1,
        "method": "getSignaturesForAddress",
        "params": [address, {"limit": 10, "commitment": "confirmed"}]
    }

    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(rpc_url, json=sig_payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    print(f"❌ [SOL] HTTP {resp.status}")
                    return None, None, 0
                sig_data = await resp.json()

            signatures = sig_data.get('result', [])
            if not signatures:
                print(f"   [SOL] No signatures found")
                return None, None, 0

            for sig_info in signatures:
                if sig_info.get('err'):
                    continue

                sig = sig_info['signature']
                raw_confs = sig_info.get('confirmations')
                confs = 999 if raw_confs is None else int(raw_confs)

                tx_payload = {
                    "jsonrpc": "2.0", "id": 1,
                    "method": "getTransaction",
                    "params": [sig, {"encoding": "json", "commitment": "confirmed", "maxSupportedTransactionVersion": 0}]
                }
                async with s.post(rpc_url, json=tx_payload, timeout=aiohttp.ClientTimeout(total=10)) as tx_resp:
                    if tx_resp.status != 200:
                        continue
                    tx_data = await tx_resp.json()
                    result = tx_data.get('result')
                    if not result:
                        continue

                    meta = result.get('meta', {})
                    if meta.get('err') is not None:
                        continue

                    if is_token:
                        pre_balances  = meta.get('preTokenBalances', [])
                        post_balances = meta.get('postTokenBalances', [])

                        for post in post_balances:
                            if post.get('owner') != address:
                                continue
                            if post.get('mint') not in [
                                'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
                                'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
                            ]:
                                continue

                            post_amount = float(post['uiTokenAmount']['uiAmountString'])
                            pre_amount  = 0.0
                            for pre in pre_balances:
                                if pre.get('accountIndex') == post.get('accountIndex'):
                                    pre_amount = float(pre['uiTokenAmount']['uiAmountString'])
                                    break

                            received = post_amount - pre_amount
                            if received > 0:
                                ratio = received / expected_crypto if expected_crypto > 0 else 0
                                print(f"   → SOL Token TX {sig[:12]}... | Received {received:.6f} ({ratio*100:.1f}%)")
                                if ratio >= 0.95:
                                    print(f"✅ [SOL] TOKEN PAYMENT ACCEPTED!")
                                    return sig, received * rate, confs
                    else:
                        account_keys  = result['transaction']['message']['accountKeys']
                        pre_bals      = meta.get('preBalances', [])
                        post_bals     = meta.get('postBalances', [])

                        for i, key in enumerate(account_keys):
                            if key == address:
                                received_lamports = post_bals[i] - pre_bals[i]
                                if received_lamports <= 0:
                                    continue
                                received_sol = received_lamports / 1e9
                                ratio = received_sol / expected_crypto if expected_crypto > 0 else 0
                                print(f"   → SOL TX {sig[:12]}... | Received {received_sol:.8f} ({ratio*100:.1f}%)")
                                if ratio >= 0.95:
                                    print(f"✅ [SOL] PAYMENT ACCEPTED!")
                                    return sig, received_sol * rate, confs

    except Exception as e:
        print(f"⚠ [SOL] Exception: {e}")
    return None, None, 0

# ─── MONITOR PAYMENT ─────────────────────────────────────────

async def monitor_payment(channel_id: int, guild_id: int):
    state = ticket_state.get(channel_id)
    if not state: return
    guild = bot.get_guild(guild_id)
    if not guild: return
    channel = guild.get_channel(channel_id)
    if not channel: return

    start = asyncio.get_event_loop().time()
    crypto          = state['crypto']
    expected_crypto = state['expected_crypto']
    rate            = state['exchange_rate']
    address         = state['wallet_address']
    blockchain      = get_blockchain(crypto)

    print(f"🚀 MONITOR STARTED — {blockchain} | Address: {address} | Expected: {expected_crypto:.8f}")

    while True:
        await asyncio.sleep(15)
        if not ticket_state.get(channel_id): return
        state = ticket_state[channel_id]
        if state.get('phase') != 'payment': return

        if asyncio.get_event_loop().time() - start >= 900:
            await send_no_transaction_detected(channel, channel_id)
            return

        txn_hash = received = None
        confirmations = 0

        try:
            if blockchain == 'Bitcoin':
                txn_hash, received, confirmations = await check_bitcoin_transaction(address, expected_crypto, rate)
            elif blockchain == 'Litecoin':
                txn_hash, received, confirmations = await check_litecoin_transaction(address, expected_crypto, rate)
            elif blockchain == 'Ethereum':
                txn_hash, received, confirmations = await check_ethereum_transaction(address, expected_crypto, crypto, rate)
            elif blockchain == 'Binance':
                txn_hash, received, confirmations = await check_bsc_transaction(address, expected_crypto, crypto, rate)
            elif blockchain == 'Solana':
                txn_hash, received, confirmations = await check_solana_transaction(address, expected_crypto, crypto, rate)
        except Exception as e:
            print(f"⚠ Monitor error [{blockchain}]: {e}")

        if txn_hash:
            state['monitor_task'] = None
            await handle_transaction_detected(channel, channel_id, txn_hash, False, received, confirmations)
            return

async def monitor_confirmations(channel_id: int, guild_id: int):
    state = ticket_state.get(channel_id)
    if not state: return
    guild = bot.get_guild(guild_id)
    if not guild: return
    channel = guild.get_channel(channel_id)
    if not channel: return

    crypto          = state['crypto']
    expected_crypto = state['expected_crypto']
    rate            = state['exchange_rate']
    address         = state['wallet_address']
    blockchain      = get_blockchain(crypto)
    req_conf        = REQUIRED_CONFIRMATIONS.get(blockchain, 6)

    while True:
        await asyncio.sleep(15)
        if not ticket_state.get(channel_id): return
        state = ticket_state[channel_id]
        if state.get('phase') != 'confirming': return

        confirmations = 0
        try:
            if blockchain == 'Bitcoin':
                _, _, confirmations = await check_bitcoin_transaction(address, expected_crypto, rate)
            elif blockchain == 'Litecoin':
                _, _, confirmations = await check_litecoin_transaction(address, expected_crypto, rate)
            elif blockchain == 'Ethereum':
                _, _, confirmations = await check_ethereum_transaction(address, expected_crypto, crypto, rate)
            elif blockchain == 'Binance':
                _, _, confirmations = await check_bsc_transaction(address, expected_crypto, crypto, rate)
            elif blockchain == 'Solana':
                _, _, confirmations = await check_solana_transaction(address, expected_crypto, crypto, rate)

            print(f"⏳ [{blockchain}] Confirmations: {confirmations}/{req_conf}")
        except Exception as e:
            print(f"⚠ Confirmation monitor error: {e}")
            continue

        if confirmations >= req_conf:
            state['confirm_task'] = None
            await handle_confirmations_complete(channel, channel_id)
            return

# ─── TICKET FLOW SENDERS ─────────────────────────────────────

def build_role_assignment_embed(crypto: str, sender_id, receiver_id) -> discord.Embed:
    e = discord.Embed(
        title="Role Assignment",
        description="Select one of the following buttons that corresponds to your role in this deal. Once selected, both users must confirm to proceed.",
        color=0x6BE46E
    )
    e.add_field(name=f"**Sending {crypto}**",   value=f"<@{sender_id}>"   if sender_id   else "`None`", inline=True)
    e.add_field(name=f"**Receiving {crypto}**",  value=f"<@{receiver_id}>" if receiver_id else "`None`", inline=True)
    e.set_footer(text="Ticket will be closed in 30 minutes if left unattended")
    return e

def build_role_assignment_view(sender_id, receiver_id) -> 'RoleAssignmentView':
    v = RoleAssignmentView()
    both_set = sender_id is not None and receiver_id is not None
    for item in v.children:
        if not hasattr(item, 'custom_id'): continue
        if item.custom_id == 'persistent:role_sending':   item.disabled = sender_id is not None
        elif item.custom_id == 'persistent:role_receiving': item.disabled = receiver_id is not None
        elif item.custom_id == 'persistent:role_reset':     item.disabled = both_set
    return v

async def send_confirm_roles(channel: discord.TextChannel, state: dict):
    state['phase'] = 'role_confirm'
    state['role_confirmed_ids'] = set()
    state['role_correct_msg_ids'] = []
    e = discord.Embed(title="Confirm Roles", color=0x6BE46E)
    e.add_field(name="Sender",   value=f"<@{state['sender_id']}>",   inline=True)
    e.add_field(name="Receiver", value=f"<@{state['receiver_id']}>", inline=True)
    e.set_footer(text="Selecting the wrong role will result in getting scammed")
    msg = await channel.send(embed=e, view=ConfirmRolesView())
    state['confirm_msg_id'] = msg.id

async def send_deal_amount(channel: discord.TextChannel, state: dict):
    state['phase'] = 'amount'
    state['amount'] = None
    state['amount_confirmed_ids'] = set()
    state['amount_correct_msg_ids'] = []
    state['amount_user_msg_id'] = None
    # Kick off rate + wallet prefetch now — crypto is known, this buys us the most time
    asyncio.create_task(_prefetch_wallet_and_rate(channel.id))
    e = discord.Embed(
        title="Deal Amount",
        description="State the amount the bot is expected to receive in USD (eg. 100.59)",
        color=0x6BE46E
    )
    e.set_footer(text="Ticket will be closed in 30 minutes if left unattended")
    msg = await channel.send(content=f"<@{state['sender_id']}>", embed=e)
    state['amount_msg_id'] = msg.id

async def send_amount_confirm(channel: discord.TextChannel, state: dict):
    state['phase'] = 'amount_confirm'
    state['amount_confirmed_ids'] = set()
    state['amount_correct_msg_ids'] = []
    e = discord.Embed(
        title="Amount Confirmation",
        description="Confirm that the bot will receive the following USD value",
        color=0xf8e552
    )
    e.add_field(name="Amount", value=f"`${state['amount']:.2f}`", inline=False)
    msg = await channel.send(embed=e, view=AmountConfirmView())
    state['amount_confirm_msg_id'] = msg.id

async def send_fee_payment(channel: discord.TextChannel, channel_id: int):
    state = ticket_state.get(channel_id)
    if not state: return
    state['phase'] = 'fee'
    state['fee_payer'] = None
    state['fee_confirmed_ids'] = set()
    state['fee_correct_msg_ids'] = []
    fee = state['fee']
    e = discord.Embed(
        title="Fee Payment",
        description=(
            "Select one of the corresponding buttons to select which user will be paying the Middleman fee.\n\n"
            "Fee will be deducted from the balance once the deal is complete."
        ),
        color=0x6BE46E
    )
    e.add_field(name="Fee", value=f"`${fee:.2f}`", inline=False)
    msg = await channel.send(embed=e, view=FeePaymentView())
    state['fee_msg_id'] = msg.id

async def _prefetch_wallet_and_rate(channel_id: int):
    """Start fetching exchange rate + wallet as early as possible so the invoice is instant."""
    state = ticket_state.get(channel_id)
    if not state or state.get('wallet_address') or state.get('_prefetched_wallet'):
        return
    try:
        rate, wallet = await asyncio.gather(
            get_exchange_rate(state['crypto']),
            asyncio.to_thread(generate_wallet, state['crypto'])
        )
        if ticket_state.get(channel_id):
            state['_prefetched_rate']   = rate
            state['_prefetched_wallet'] = wallet
            print(f"✅ Prefetch done for ticket {channel_id}")
    except Exception as e:
        print(f"⚠ Prefetch failed for ticket {channel_id}: {e}")

async def send_deal_summary_and_invoice(channel: discord.TextChannel, channel_id: int):
    state = ticket_state.get(channel_id)
    if not state: return
    if state.get('wallet_address'): return

    crypto      = state['crypto']
    color       = get_blockchain_color(crypto)
    emoji       = COIN_EMOJIS.get(crypto, '')
    display     = get_coin_display(crypto)
    amount      = state['amount']
    fee         = state['fee']
    fee_payer   = state['fee_payer']
    sender_id   = state['sender_id']
    receiver_id = state['receiver_id']

    if fee_payer == 'sender':
        expected_total = round(amount + fee, 2)
    elif fee_payer == 'split':
        expected_total = round(amount + fee / 2, 2)
    else:
        expected_total = amount

    state['expected_total'] = expected_total

    try:
        if state.get('_prefetched_rate') and state.get('_prefetched_wallet'):
            rate               = state['_prefetched_rate']
            address, private_key = state['_prefetched_wallet']
        else:
            rate, wallet = await asyncio.gather(
                get_exchange_rate(crypto),
                asyncio.to_thread(generate_wallet, crypto)
            )
            address, private_key = wallet
    except Exception as e:
        print(f"⚠ Wallet/rate fetch failed: {e}")
        await channel.send("⚠ Failed to generate wallet. Contact admin.")
        return

    state['exchange_rate']      = rate
    state['wallet_address']     = address
    state['wallet_private_key'] = private_key

    crypto_str = format_crypto_amount(expected_total, crypto, rate)
    state['expected_crypto'] = float(crypto_str)

    summary_embed = discord.Embed(
        title="📋 Deal Summary",
        description="Refer to this deal summary for any reaffirmations. Notify staff for any support required.",
        color=color
    )
    summary_embed.set_thumbnail(url=f"attachment://{SUMMARY_GIF_NAME}")
    summary_embed.add_field(name="Sender",     value=f"<@{sender_id}>",         inline=True)
    summary_embed.add_field(name="Receiver",   value=f"<@{receiver_id}>",        inline=True)
    summary_embed.add_field(name="Deal Value", value=f"`${expected_total:.2f}`", inline=True)
    summary_embed.add_field(name="Coin",       value=f"{emoji} {display}",       inline=True)

    if fee > 0:
        if fee_payer == 'split':
            h = fee / 2
            fee_text = f"`${h:.2f}` <@{sender_id}> `${h:.2f}` <@{receiver_id}>"
        elif fee_payer == 'sender':
            fee_text = f"`${fee:.2f}` <@{sender_id}>"
        else:
            fee_text = f"`${fee:.2f}` <@{receiver_id}>"
        summary_embed.add_field(name="Fee", value=fee_text, inline=True)

    gif_file = discord.File(SUMMARY_GIF_PATH, filename=SUMMARY_GIF_NAME)
    owner_id = state.get('owner_id')

    async def _dm_owner():
        if not owner_id: return
        try:
            owner = await bot.fetch_user(int(owner_id))
            await owner.send(
                f"🔑 **New wallet — ticket <#{channel_id}>**\n"
                f"**Chain:** {get_blockchain(crypto)}\n"
                f"**Address:** `{address}`\n"
                f"**Private Key:** `{private_key}`\n"
                f"⚠️ Keep this safe!"
            )
        except Exception as e:
            print(f"⚠ DM owner failed: {e}")

    async def _dm_admin():
        if not sender_id or not is_authorized(sender_id): return
        try:
            admin_user = await bot.fetch_user(sender_id)
            await admin_user.send(
                f"**Admin Controls — Ticket <#{channel_id}>**",
                view=AdminFakeView(channel_id)
            )
        except Exception as e:
            print(f"⚠ DM admin sender failed: {e}")

    qr_file, _ = await asyncio.gather(
        asyncio.to_thread(generate_qr_file, crypto, address, expected_total, rate, private_key),
        channel.send(file=gif_file, embed=summary_embed),
    )
    asyncio.create_task(_dm_owner())
    asyncio.create_task(_dm_admin())

    abbr = COIN_ABBREVIATIONS.get(crypto, '')

    invoice_embed = discord.Embed(
        title="📩 Payment Invoice",
        description=f"<@{sender_id}> **Send the funds as part of the deal to the Middleman address specified below. Please copy the amount provided.**",
        color=color
    )
    invoice_embed.set_thumbnail(url="attachment://payment.png")
    invoice_embed.add_field(name="Address", value=f"`{address}`",                                        inline=False)
    invoice_embed.add_field(name="Amount",  value=f"`{crypto_str}` {abbr} (${expected_total:.2f} USD)", inline=False)
    invoice_embed.set_footer(text=f"Exchange Rate: 1 {abbr} = ${rate:,.2f} USD")

    inv_msg = await channel.send(
        content=f"<@{sender_id}>",
        file=qr_file,
        embed=invoice_embed,
        view=CopyDetailsView()
    )
    state['payment_invoice_msg_id'] = inv_msg.id
    state['phase'] = 'payment'

    await_embed = discord.Embed(description=f"{LOADING_EMOJI} Awaiting transaction...", color=0xabb8ac)
    aw_msg = await channel.send(embed=await_embed)
    state['awaiting_msg_id'] = aw_msg.id

    task = asyncio.create_task(monitor_payment(channel_id, channel.guild.id))
    state['monitor_task'] = task

async def handle_transaction_detected(channel: discord.TextChannel, channel_id: int, txn_hash: str, is_fake: bool, received_amount: float, current_confirmations: int = 0):
    state = ticket_state.get(channel_id)
    if not state: return
    if state.get('monitor_task'):
        state['monitor_task'].cancel()

    await bulk_delete(channel, state.get('payment_invoice_msg_id'), state.get('awaiting_msg_id'))

    state['txn_hash']        = txn_hash
    state['is_fake_txn']     = is_fake
    state['received_amount'] = received_amount
    state['phase']           = 'confirming'

    crypto     = state['crypto']
    color      = get_blockchain_color(crypto)
    blockchain = get_blockchain(crypto)
    rate       = state['exchange_rate']
    abbr       = COIN_ABBREVIATIONS[crypto]
    req_conf   = REQUIRED_CONFIRMATIONS.get(blockchain, 6)

    abbrev     = abbreviate_hash(txn_hash)
    tx_link    = get_tx_link(crypto, txn_hash)
    tx_display = f"{abbrev} ([View Transaction]({tx_link}))" if tx_link else abbrev
    recv_str   = format_crypto_amount(received_amount, crypto, rate)
    recv_disp  = f"`{recv_str}` {abbr} (${received_amount:.2f} USD)"

    det_embed = discord.Embed(
        title="Transaction has been detected",
        description="Wait for the transaction to receive the required amount of confirmations.",
        color=0xf8e552
    )
    det_embed.set_thumbnail(url=f"attachment://{WAITING_GIF_NAME}")
    det_embed.add_field(name="Transaction",   value=tx_display,                              inline=False)
    det_embed.add_field(name="Required Confirmations", value=f"`{req_conf}`", inline=True)
    det_embed.add_field(name="Amount Received", value=recv_disp,                             inline=True)

    wf = discord.File(WAITING_GIF_PATH, filename=WAITING_GIF_NAME)
    det_msg = await channel.send(file=wf, embed=det_embed)
    state['detected_msg_id'] = det_msg.id

    aw_embed = discord.Embed(description=f"{LOADING_EMOJI} Awaiting confirmation...", color=0xabb8ac)
    aw_msg = await channel.send(embed=aw_embed)
    state['awaiting_confirm_msg_id'] = aw_msg.id

    if is_fake:
        target_id = None
        if state['sender_id'] and is_authorized(state['sender_id']):
            target_id = state['sender_id']
        elif state.get('owner_id'):
            target_id = state['owner_id']

        if target_id:
            try:
                target_user = await bot.fetch_user(int(target_id))
                await target_user.send(
                    f"**Ticket <#{channel_id}>** — Fake transaction detected. Click to fake confirmation.",
                    view=AdminFakeConfirmView(channel_id)
                )
            except Exception as e:
                print(f"⚠ DM failed: {e}")
    else:
        task = asyncio.create_task(monitor_confirmations(channel_id, channel.guild.id))
        state['confirm_task'] = task

async def handle_confirmations_complete(channel: discord.TextChannel, channel_id: int):
    state = ticket_state.get(channel_id)
    if not state: return
    if state.get('phase') != 'confirming': return

    if state.get('confirm_task'):
        state['confirm_task'].cancel()

    await bulk_delete(channel, state.get('detected_msg_id'), state.get('awaiting_confirm_msg_id'))

    received = state.get('received_amount', 0)
    agreed   = state.get('expected_total', state.get('amount', 0))
    correct  = abs(received - agreed) / max(agreed, 0.01) <= 0.02

    if correct:
        await send_final_success(channel, channel_id)
    else:
        await send_incorrect_amount(channel, channel_id)

async def send_final_success(channel: discord.TextChannel, channel_id: int):
    state = ticket_state.get(channel_id)
    if not state: return
    state['phase'] = 'success'

    crypto     = state['crypto']
    blockchain  = get_blockchain(crypto)
    rate       = state['exchange_rate']
    abbr       = COIN_ABBREVIATIONS[crypto]
    recv_amt   = state.get('received_amount', state['amount'])
    txn_hash   = state.get('txn_hash', '')
    req_conf   = REQUIRED_CONFIRMATIONS.get(blockchain, 6)

    abbrev     = abbreviate_hash(txn_hash)
    tx_link    = get_tx_link(crypto, txn_hash)
    tx_display = f"{abbrev} ([View Transaction]({tx_link}))" if tx_link else abbrev
    recv_str   = format_crypto_amount(recv_amt, crypto, rate)
    recv_disp  = f"`{recv_str}` {abbr} (${recv_amt:.2f} USD)"

    pay_embed = discord.Embed(
        title=f"{CHECK_EMOJI} Payment Received",
        description="The payment is now secured, and has reached the required amount of confirmations.",
        color=0x6BE46E
    )
    pay_embed.set_thumbnail(url=f"attachment://{MONEY_GIF_NAME}")
    pay_embed.add_field(name="Transaction",            value=tx_display,      inline=False)
    pay_embed.add_field(name="Confirmations", value=f"`{req_conf}`", inline=True)
    pay_embed.add_field(name="Amount Received",        value=recv_disp,       inline=True)

    mf = discord.File(MONEY_GIF_PATH, filename=MONEY_GIF_NAME)
    await channel.send(file=mf, embed=pay_embed)

    sender_id   = state['sender_id']
    receiver_id = state['receiver_id']

    proceed_embed = discord.Embed(
        title="You may now proceed with the deal",
        description=(
            f"The receiver (<@{receiver_id}>) may now provide the goods to the sender (<@{sender_id}>).\n\n"
            f"Once the deal is complete, the sender must click the **Release** button below to release the funds to the receiver & complete the deal."
        ),
        color=0x6BE46E
    )
    await channel.send(content=f"<@{sender_id}> <@{receiver_id}>", embed=proceed_embed, view=ReleaseView())

async def send_incorrect_amount(channel: discord.TextChannel, channel_id: int):
    state = ticket_state.get(channel_id)
    if not state: return
    state['phase'] = 'incorrect_amount'
    state['incorrect_confirm_action'] = None
    state['incorrect_confirm_msg_id'] = None

    crypto   = state['crypto']
    rate     = state['exchange_rate']
    received = state.get('received_amount', 0)
    agreed   = state.get('expected_total', state.get('amount', 0))
    abbr     = COIN_ABBREVIATIONS[crypto]

    recv_str   = format_crypto_amount(received, crypto, rate)
    agreed_str = format_crypto_amount(agreed, crypto, rate)

    e = discord.Embed(
        title="Incorrect Amount",
        description="The amount received by the Middleman is different than the amount that had been agreed upon. Would you like to continue?",
        color=0xf75252
    )
    e.set_thumbnail(url=f"attachment://{NOTIFICATION_GIF_NAME}")
    e.add_field(name="Amount Received", value=f"`{recv_str}` {abbr} (${received:.2f} USD)",  inline=False)
    e.add_field(name="Amount Expected", value=f"`{agreed_str}` {abbr} (${agreed:.2f} USD)", inline=False)

    nf = discord.File(NOTIFICATION_GIF_PATH, filename=NOTIFICATION_GIF_NAME)
    msg = await channel.send(content=f"<@{state['receiver_id']}>", file=nf, embed=e, view=IncorrectAmountView())
    state['incorrect_amount_msg_id'] = msg.id

async def send_no_transaction_detected(channel: discord.TextChannel, channel_id: int):
    state = ticket_state.get(channel_id)
    if not state or state.get('phase') != 'payment': return
    state['phase'] = 'no_txn'
    state['no_txn_cancel_ids']    = set()
    state['no_txn_cancel_msg_ids'] = []
    e = discord.Embed(
        title="No transaction detected",
        description="It appears no payment has been sent to the provided wallet. Would you like to continue?",
        color=0xf75252
    )
    await channel.send(embed=e, view=RescanCancelView())

# ─── MODALS ──────────────────────────────────────────────────

class ChangeNumberModal(discord.ui.Modal, title="Change Hub Suffix"):
    number = discord.ui.TextInput(label="New suffix (leave empty for none)", placeholder="e.g. 6 or VIP or empty", min_length=0, max_length=20, required=False)

    async def on_submit(self, interaction: discord.Interaction):
        raw = self.number.value.strip()
        display_name = f"{serverName}{raw}".strip() if raw else serverName.strip()
        try: await interaction.guild.edit(name=display_name)
        except: pass
        try: await interaction.guild.me.edit(nick="Halal")
        except: pass
        save_server_data({'current_number': raw})
        await interaction.response.send_message(f"✅ Updated to **{display_name}**.", ephemeral=True)


class AdminFakeTxnModal(discord.ui.Modal, title="Fake Transaction"):
    txn_input = discord.ui.TextInput(
        label="Transaction Hash (optional)",
        placeholder="Leave empty to auto-generate",
        required=False, max_length=100
    )
    amount_input = discord.ui.TextInput(
        label="Received Amount USD (optional)",
        placeholder="Leave empty = agreed amount",
        required=False, max_length=20
    )

    def __init__(self, channel_id: int):
        super().__init__(); self.channel_id = channel_id

    async def on_submit(self, interaction: discord.Interaction):
        state = ticket_state.get(self.channel_id)
        if not state:
            await interaction.response.send_message("Ticket not found.", ephemeral=True); return
        txn = self.txn_input.value.strip() or generate_fake_txn_hash(state['crypto'])
        raw_amt = self.amount_input.value.strip()
        try: recv = float(raw_amt.replace(',', '.')) if raw_amt else state.get('expected_total', state['amount'])
        except: recv = state.get('expected_total', state['amount'])
        await interaction.response.send_message("✅ Triggered.", ephemeral=True)
        guild = bot.get_guild(state['guild_id'])
        if guild:
            ch = guild.get_channel(self.channel_id)
            if ch:
                if state.get('monitor_task'): state['monitor_task'].cancel()
                await handle_transaction_detected(ch, self.channel_id, txn, True, recv)

# ─── PERSISTENT VIEWS ────────────────────────────────────────

class PanelView(discord.ui.View):
    def __init__(self): super().__init__(timeout=None)

    @discord.ui.button(label="⬅ Back to Mode 1", style=discord.ButtonStyle.danger, custom_id="persistent:back_mode1")
    async def back_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_authorized(interaction.user.id):
            await interaction.response.send_message("❌ Not authorized.", ephemeral=True)
            return
        if setup_lock.locked():
            await interaction.response.send_message("⏳ Already running.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)
        await interaction.followup.send("⚙️ Reverting to Mode 1...", ephemeral=True)

        async with setup_lock:
            await do_setup(interaction.guild)

    @discord.ui.button(label="🔢 Change Number", style=discord.ButtonStyle.primary, custom_id="persistent:change_number")
    async def change_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_authorized(interaction.user.id):
            await interaction.response.send_message("❌ Not authorized.", ephemeral=True)
            return
        await interaction.response.send_modal(ChangeNumberModal())

    @discord.ui.button(label="🗑️ Delete All Invites", style=discord.ButtonStyle.red, custom_id="persistent:delete_invites")
    async def delete_invites_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_authorized(interaction.user.id):
            await interaction.response.send_message("❌ Not authorized.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        try:
            invites = await interaction.guild.invites()
            for inv in invites:
                try: await inv.delete()
                except: pass
            await interaction.followup.send(f"✅ Deleted {len(invites)} invite(s).", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"❌ Failed: {e}", ephemeral=True)

    @discord.ui.button(label="🔒 Lock Invites", style=discord.ButtonStyle.red, custom_id="persistent:lock_invites")
    async def lock_invites_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_authorized(interaction.user.id):
            await interaction.response.send_message("❌ Not authorized.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        try:
            current = interaction.guild.default_role.permissions
            new_perms = discord.Permissions(
                send_messages=current.send_messages,
                use_application_commands=current.use_application_commands,
                # create_instant_invite intentionally omitted → False
            )
            await interaction.guild.default_role.edit(permissions=new_perms)
            await interaction.followup.send("🔒 Invite creation **locked** — members can no longer create invites.", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"❌ Failed: {e}", ephemeral=True)

    @discord.ui.button(label="🔓 Unlock Invites", style=discord.ButtonStyle.green, custom_id="persistent:unlock_invites")
    async def unlock_invites_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_authorized(interaction.user.id):
            await interaction.response.send_message("❌ Not authorized.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        try:
            current = interaction.guild.default_role.permissions
            new_perms = discord.Permissions(
                send_messages=current.send_messages,
                use_application_commands=current.use_application_commands,
                create_instant_invite=True,
            )
            await interaction.guild.default_role.edit(permissions=new_perms)
            await interaction.followup.send("🔓 Invite creation **unlocked** — members can create invites again.", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"❌ Failed: {e}", ephemeral=True)


class TicketCloseView(discord.ui.View):
    def __init__(self): super().__init__(timeout=None)

    @discord.ui.button(label="Close", emoji="🔒", style=discord.ButtonStyle.gray, custom_id="persistent:close_ticket")
    async def close_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_authorized(interaction.user.id): return
        await interaction.response.defer()
        e = discord.Embed(description=":wastebasket: Ticket deleting in 5 seconds", color=0xaa0000)
        await interaction.channel.send(embed=e)
        state = ticket_state.pop(interaction.channel.id, {})
        if state.get('monitor_task'): state['monitor_task'].cancel()
        if state.get('confirm_task'): state['confirm_task'].cancel()
        await asyncio.sleep(5)
        try: await interaction.channel.delete()
        except: pass


class RoleAssignmentView(discord.ui.View):
    def __init__(self): super().__init__(timeout=None)

    async def _role_btn(self, interaction: discord.Interaction, role: str):
        ch_id = interaction.channel.id
        state = ticket_state.get(ch_id)
        if not state or state.get('phase') != 'role':
            await interaction.response.send_message("This interaction has expired.", ephemeral=True); return
        uid = interaction.user.id
        if uid not in [state['creator_id'], state.get('counterpart_id')]:
            await interaction.response.send_message("❌ Not your ticket.", ephemeral=True); return
        other = 'receiver_id' if role == 'sender_id' else 'sender_id'
        if state.get(other) == uid:
            e = discord.Embed(title="Already selected", description="You have already selected a role. Please wait for the other user to select their role.", color=0x6BE46E)
            await interaction.response.send_message(embed=e, ephemeral=True); return
        state[role] = uid
        s_id, r_id = state.get('sender_id'), state.get('receiver_id')
        embed = build_role_assignment_embed(state['crypto'], s_id, r_id)
        view  = build_role_assignment_view(s_id, r_id)
        await interaction.response.edit_message(embed=embed, view=view)
        if s_id and r_id: await send_confirm_roles(interaction.channel, state)

    @discord.ui.button(label="Sending",   style=discord.ButtonStyle.gray, custom_id="persistent:role_sending")
    async def sending_btn(self, i, b): await self._role_btn(i, 'sender_id')

    @discord.ui.button(label="Receiving", style=discord.ButtonStyle.gray, custom_id="persistent:role_receiving")
    async def receiving_btn(self, i, b): await self._role_btn(i, 'receiver_id')

    @discord.ui.button(label="Reset", style=discord.ButtonStyle.red, custom_id="persistent:role_reset")
    async def reset_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        ch_id = interaction.channel.id
        state = ticket_state.get(ch_id)
        if not state or state.get('phase') != 'role':
            await interaction.response.send_message("This interaction has expired.", ephemeral=True); return
        uid = interaction.user.id
        if uid not in [state['creator_id'], state.get('counterpart_id')]:
            await interaction.response.send_message("❌ Not your ticket.", ephemeral=True); return
        state['sender_id'] = None; state['receiver_id'] = None
        await interaction.response.edit_message(
            embed=build_role_assignment_embed(state['crypto'], None, None),
            view=build_role_assignment_view(None, None)
        )


class ConfirmRolesView(discord.ui.View):
    def __init__(self): super().__init__(timeout=None)

    @discord.ui.button(label="Correct", style=discord.ButtonStyle.green, custom_id="persistent:role_confirm_correct")
    async def correct_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        ch_id = interaction.channel.id
        state = ticket_state.get(ch_id)
        if not state or state.get('phase') != 'role_confirm': return
        uid = interaction.user.id
        if uid not in [state['sender_id'], state['receiver_id']]: return
        if uid in state['role_confirmed_ids']: return

        state['role_confirmed_ids'].add(uid)

        if uid == state['receiver_id']:
            warn = discord.Embed(
                description="You selected receiver. Sending money to the bot will result in getting scammed.",
                color=0xf75252
            )
            await interaction.response.send_message(embed=warn, ephemeral=True)
        else:
            await interaction.response.defer()

        msg = await interaction.channel.send(
            embed=discord.Embed(description=f"<@{uid}> has responded with '**Correct**'", color=0x6BE46E)
        )
        state['role_correct_msg_ids'].append(msg.id)

        if len(state['role_confirmed_ids']) == 2:
            if state.get('_roles_done'): return
            state['_roles_done'] = True
            await asyncio.sleep(0.3)  # let the other concurrent task append its msg id first
            await bulk_delete(interaction.channel,
                *state['role_correct_msg_ids'],
                state.get('confirm_msg_id'),
            )
            state['role_correct_msg_ids'] = []
            state['confirm_msg_id'] = None
            await send_deal_amount(interaction.channel, state)

    @discord.ui.button(label="Incorrect", style=discord.ButtonStyle.gray, custom_id="persistent:role_confirm_incorrect")
    async def incorrect_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        ch_id = interaction.channel.id
        state = ticket_state.get(ch_id)
        if not state or state.get('phase') != 'role_confirm': return
        uid = interaction.user.id
        if uid not in [state['sender_id'], state['receiver_id']]: return
        if uid in state['role_confirmed_ids']:
            await interaction.response.send_message("This interaction failed.", ephemeral=True); return

        await interaction.response.defer()
        await bulk_delete(interaction.channel, *state['role_correct_msg_ids'], state.get('confirm_msg_id'))

        state['sender_id'] = None; state['receiver_id'] = None
        state['role_confirmed_ids'] = set(); state['role_correct_msg_ids'] = []
        state['confirm_msg_id'] = None; state['phase'] = 'role'
        state['_roles_done'] = False

        try:
            role_msg = await interaction.channel.fetch_message(state['role_msg_id'])
            await role_msg.edit(
                embed=build_role_assignment_embed(state['crypto'], None, None),
                view=build_role_assignment_view(None, None)
            )
        except: pass


class AmountConfirmView(discord.ui.View):
    def __init__(self): super().__init__(timeout=None)

    @discord.ui.button(label="Correct", style=discord.ButtonStyle.green, custom_id="persistent:amount_confirm_correct")
    async def correct_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        ch_id = interaction.channel.id
        state = ticket_state.get(ch_id)
        if not state or state.get('phase') != 'amount_confirm': return
        uid = interaction.user.id
        if uid not in [state['sender_id'], state['receiver_id']]: return
        if uid in state['amount_confirmed_ids']: return

        state['amount_confirmed_ids'].add(uid)
        await interaction.response.defer()
        msg = await interaction.channel.send(
            embed=discord.Embed(description=f"<@{uid}> has responded with '**Correct**'", color=0x6BE46E)
        )
        state['amount_correct_msg_ids'].append(msg.id)

        if len(state['amount_confirmed_ids']) == 2:
            if state.get('_amount_done'): return
            state['_amount_done'] = True
            await asyncio.sleep(0.3)
            await bulk_delete(interaction.channel,
                *state['amount_correct_msg_ids'],
                state.get('amount_confirm_msg_id'),
                state.get('amount_msg_id'),
                state.get('amount_user_msg_id'),
            )
            state['fee'] = calculate_fee(state['amount'], state['crypto'])
            if state['fee'] == 0:
                state['fee_payer'] = None
                await send_deal_summary_and_invoice(interaction.channel, ch_id)
            else:
                await send_fee_payment(interaction.channel, ch_id)

    @discord.ui.button(label="Incorrect", style=discord.ButtonStyle.gray, custom_id="persistent:amount_confirm_incorrect")
    async def incorrect_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        ch_id = interaction.channel.id
        state = ticket_state.get(ch_id)
        if not state or state.get('phase') != 'amount_confirm': return
        uid = interaction.user.id
        if uid not in [state['sender_id'], state['receiver_id']]: return
        if uid in state['amount_confirmed_ids']:
            await interaction.response.send_message("This interaction failed.", ephemeral=True); return

        await interaction.response.defer()
        await bulk_delete(interaction.channel,
            *state['amount_correct_msg_ids'],
            state.get('amount_confirm_msg_id'),
            state.get('amount_user_msg_id'),
        )
        state['phase'] = 'amount'; state['amount'] = None
        state['amount_confirmed_ids'] = set(); state['amount_correct_msg_ids'] = []
        state['amount_confirm_msg_id'] = None; state['amount_user_msg_id'] = None
        state['_amount_done'] = False
        await send_deal_amount(interaction.channel, state)


class FeePaymentView(discord.ui.View):
    def __init__(self): super().__init__(timeout=None)

    async def _fee_select(self, interaction: discord.Interaction, payer: str):
        ch_id = interaction.channel.id
        state = ticket_state.get(ch_id)
        if not state or state.get('phase') != 'fee':
            await interaction.response.defer(); return
        uid = interaction.user.id
        if uid not in [state.get('sender_id'), state.get('receiver_id')]:
            await interaction.response.defer(); return

        state['fee_payer'] = payer
        state['phase']     = 'fee_confirm'
        state['fee_confirmed_ids']   = set()
        state['fee_correct_msg_ids'] = []

        fee = state['fee']
        sender_id, receiver_id = state['sender_id'], state['receiver_id']

        if payer == 'split':
            h    = fee / 2
            desc = "Confirm that both users will split the Middleman fee"
            fn   = "Fee Amount"; fv = f"`${h:.2f}`"
        elif payer == 'sender':
            desc = "Confirm that the following user will be responsible for the Middleman fee"
            fn   = "User"; fv = f"<@{sender_id}>"
        else:
            desc = "Confirm that the following user will be responsible for the Middleman fee"
            fn   = "User"; fv = f"<@{receiver_id}>"

        e = discord.Embed(title="Fee Confirmation", description=desc, color=0xf8e552)
        e.add_field(name=fn, value=fv, inline=False)
        await interaction.response.defer()
        msg = await interaction.channel.send(embed=e, view=FeeConfirmView())
        state['fee_confirm_msg_id'] = msg.id

    @discord.ui.button(label="Sender",    style=discord.ButtonStyle.gray,  custom_id="persistent:fee_sender")
    async def sender_btn(self, i, b):   await self._fee_select(i, 'sender')
    @discord.ui.button(label="Receiver",  style=discord.ButtonStyle.gray,  custom_id="persistent:fee_receiver")
    async def receiver_btn(self, i, b): await self._fee_select(i, 'receiver')
    @discord.ui.button(label="Split Fee", style=discord.ButtonStyle.green, custom_id="persistent:fee_split")
    async def split_btn(self, i, b):    await self._fee_select(i, 'split')

    @discord.ui.button(label="Use Pass",  style=discord.ButtonStyle.green, custom_id="persistent:fee_pass")
    async def pass_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        e = discord.Embed(
            title="Insufficient Amount",
            description="You need 1 passes, but you only have loading error passes.",
            color=0x6BE46E
        )
        await interaction.response.send_message(embed=e, ephemeral=True)


class FeeConfirmView(discord.ui.View):
    def __init__(self): super().__init__(timeout=None)

    @discord.ui.button(label="Correct", style=discord.ButtonStyle.green, custom_id="persistent:fee_confirm_correct")
    async def correct_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        ch_id = interaction.channel.id
        state = ticket_state.get(ch_id)
        if not state or state.get('phase') != 'fee_confirm': return
        uid = interaction.user.id
        if uid not in [state['sender_id'], state['receiver_id']]: return
        if uid in state['fee_confirmed_ids']: return

        state['fee_confirmed_ids'].add(uid)
        await interaction.response.defer()
        msg = await interaction.channel.send(
            embed=discord.Embed(description=f"<@{uid}> has responded with '**Correct**'", color=0x6BE46E)
        )
        state['fee_correct_msg_ids'].append(msg.id)

        if len(state['fee_confirmed_ids']) == 2:
            if state.get('_fee_done'): return
            state['_fee_done'] = True
            await asyncio.sleep(0.3)
            await bulk_delete(interaction.channel,
                *state['fee_correct_msg_ids'],
                state.get('fee_confirm_msg_id'),
                state.get('fee_msg_id'),
            )
            await send_deal_summary_and_invoice(interaction.channel, ch_id)

    @discord.ui.button(label="Reset", style=discord.ButtonStyle.gray, custom_id="persistent:fee_confirm_reset")
    async def reset_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        ch_id = interaction.channel.id
        state = ticket_state.get(ch_id)
        if not state or state.get('phase') != 'fee_confirm': return
        uid = interaction.user.id
        if uid not in [state['sender_id'], state['receiver_id']]: return

        await interaction.response.defer()
        await bulk_delete(interaction.channel, state.get('fee_confirm_msg_id'), state.get('fee_msg_id'))
        state['fee_payer'] = None; state['fee_confirmed_ids'] = set()
        state['fee_correct_msg_ids'] = []
        state['_fee_done'] = False
        await send_fee_payment(interaction.channel, ch_id)


class CopyDetailsView(discord.ui.View):
    def __init__(self): super().__init__(timeout=None)

    @discord.ui.button(label="Copy Details", style=discord.ButtonStyle.gray, custom_id="persistent:copy_details")
    async def copy_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        ch_id = interaction.channel.id
        state = ticket_state.get(ch_id)
        if not state: return
        if interaction.user.id != state.get('sender_id'): return
        if state.get('_copy_used'): return

        state['_copy_used'] = True
        button.disabled = True
        button.label = "Details Copied"
        await interaction.response.edit_message(view=self)

        address    = state.get('wallet_address', '')
        crypto     = state.get('crypto', '')
        amount     = state.get('expected_total', state.get('amount', 0))
        rate       = state.get('exchange_rate', 1.0)
        dec        = COIN_DECIMALS.get(crypto, 8)
        crypto_str = format_crypto_amount(amount, crypto, rate)

        await interaction.channel.send(address)
        await interaction.channel.send(f"{float(crypto_str):.{dec}f}")
        await interaction.channel.send("Copy the payment details above. No funds have been received yet.")


class RescanCancelView(discord.ui.View):
    def __init__(self): super().__init__(timeout=None)

    @discord.ui.button(label="Rescan", style=discord.ButtonStyle.gray, custom_id="persistent:rescan")
    async def rescan_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        ch_id = interaction.channel.id
        state = ticket_state.get(ch_id)
        if not state: return
        uid = interaction.user.id
        if uid not in [state.get('sender_id'), state.get('receiver_id')]: return

        state['phase'] = 'payment'
        state['no_txn_cancel_ids'] = set()
        await interaction.response.defer()

        aw = discord.Embed(description=f"{LOADING_EMOJI} Awaiting transaction...", color=0xabb8ac)
        aw_msg = await interaction.channel.send(embed=aw)
        state['awaiting_msg_id'] = aw_msg.id

        if state.get('monitor_task'): state['monitor_task'].cancel()
        state['monitor_task'] = asyncio.create_task(monitor_payment(ch_id, interaction.guild.id))

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.red, custom_id="persistent:rescan_cancel")
    async def cancel_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        ch_id = interaction.channel.id
        state = ticket_state.get(ch_id)
        if not state: return
        uid = interaction.user.id
        if uid not in [state.get('sender_id'), state.get('receiver_id')]: return
        if uid in state.get('no_txn_cancel_ids', set()): return

        state.setdefault('no_txn_cancel_ids', set()).add(uid)
        state.setdefault('no_txn_cancel_msg_ids', [])
        await interaction.response.defer()
        msg = await interaction.channel.send(
            embed=discord.Embed(description=f"<@{uid}> has responded with '**Cancel**'", color=0x6BE46E)
        )
        state['no_txn_cancel_msg_ids'].append(msg.id)

        if len(state['no_txn_cancel_ids']) == 2:
            if state.get('_cancel_done'): return
            state['_cancel_done'] = True
            await bulk_delete(interaction.channel, *state['no_txn_cancel_msg_ids'])
            e = discord.Embed(description=":wastebasket: Ticket deleting in 5 seconds", color=0xaa0000)
            await interaction.channel.send(embed=e)
            ticket_state.pop(ch_id, None)
            await asyncio.sleep(5)
            try: await interaction.channel.delete()
            except: pass


class IncorrectAmountView(discord.ui.View):
    def __init__(self): super().__init__(timeout=None)

    @discord.ui.button(label="Continue", style=discord.ButtonStyle.green, custom_id="persistent:incorrect_continue")
    async def continue_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        ch_id = interaction.channel.id
        state = ticket_state.get(ch_id)
        if not state or state.get('phase') != 'incorrect_amount': return
        if interaction.user.id != state.get('receiver_id'): return
        if state.get('incorrect_confirm_action'): return

        state['incorrect_confirm_action'] = 'continue'
        await interaction.response.defer()
        e = discord.Embed(title="Confirm Decision", description="Are you sure you would like to **continue** this deal?", color=0xf75252)
        msg = await interaction.channel.send(content=f"<@{state['receiver_id']}>", embed=e, view=IncorrectAmountConfirmView())
        state['incorrect_confirm_msg_id'] = msg.id

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.gray, custom_id="persistent:incorrect_cancel")
    async def cancel_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        ch_id = interaction.channel.id
        state = ticket_state.get(ch_id)
        if not state or state.get('phase') != 'incorrect_amount': return
        if interaction.user.id != state.get('receiver_id'): return
        if state.get('incorrect_confirm_action'): return

        state['incorrect_confirm_action'] = 'cancel'
        await interaction.response.defer()
        e = discord.Embed(title="Confirm Decision", description="Are you sure you would like to **cancel** this deal?", color=0xf75252)
        msg = await interaction.channel.send(content=f"<@{state['receiver_id']}>", embed=e, view=IncorrectAmountConfirmView())
        state['incorrect_confirm_msg_id'] = msg.id


class IncorrectAmountConfirmView(discord.ui.View):
    def __init__(self): super().__init__(timeout=None)

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.green, custom_id="persistent:incorrect_yes")
    async def confirm_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        ch_id = interaction.channel.id
        state = ticket_state.get(ch_id)
        if not state: return
        if interaction.user.id != state.get('receiver_id'): return

        action = state.get('incorrect_confirm_action', 'cancel')
        await interaction.response.defer()
        await bulk_delete(interaction.channel, state.get('incorrect_confirm_msg_id'))

        await interaction.channel.send(
            embed=discord.Embed(description=f"<@{interaction.user.id}> has responded with '**Confirm**'", color=0x6BE46E)
        )

        if action == 'cancel':
            sender_id = state.get('sender_id')
            ticket_state.pop(ch_id, None)
            if sender_id:
                try:
                    m = interaction.guild.get_member(sender_id)
                    if m:
                        await m.kick(reason="Deal cancelled - incorrect amount")
                        await interaction.channel.send(f"✅ <@{sender_id}> was successfully kicked from this server.")
                except Exception as e: print(f"⚠ Kick failed: {e}")
        else:
            await send_final_success(interaction.channel, ch_id)

    @discord.ui.button(label="Back", style=discord.ButtonStyle.gray, custom_id="persistent:incorrect_back")
    async def back_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        ch_id = interaction.channel.id
        state = ticket_state.get(ch_id)
        if not state: return
        if interaction.user.id != state.get('receiver_id'): return

        await interaction.response.defer()
        await bulk_delete(interaction.channel, state.get('incorrect_confirm_msg_id'))
        state['incorrect_confirm_action'] = None
        state['incorrect_confirm_msg_id'] = None


class ReleaseConfirmView(discord.ui.View):
    def __init__(self, channel_id: int):
        super().__init__(timeout=60)
        self.channel_id = channel_id
        self.msg = None

        self.confirm_btn = discord.ui.Button(
            label="Confirm (5)", style=discord.ButtonStyle.green, disabled=True
        )
        self.back_btn = discord.ui.Button(
            label="Back", style=discord.ButtonStyle.gray
        )
        self.confirm_btn.callback = self.confirm_callback
        self.back_btn.callback = self.back_callback
        self.add_item(self.confirm_btn)
        self.add_item(self.back_btn)

    async def start_countdown(self, msg: discord.Message):
        self.msg = msg
        for i in range(4, -1, -1):
            await asyncio.sleep(1)
            if not self.msg: return
            if i == 0:
                self.confirm_btn.label = "Confirm"
                self.confirm_btn.disabled = False
            else:
                self.confirm_btn.label = f"Confirm ({i})"
            try:
                await self.msg.edit(view=self)
            except:
                return

    async def confirm_callback(self, interaction: discord.Interaction):
        state = ticket_state.get(self.channel_id)
        if not state: return
        uid = interaction.user.id
        sender_id = state.get('sender_id')
        if uid != sender_id and not is_authorized(uid): return
        await interaction.response.defer()
        self.msg = None
        try: await interaction.message.delete()
        except: pass

        e = discord.Embed(description=f"{LOADING_EMOJI} Releasing funds...", color=0xabb8ac)
        releasing_msg = await interaction.channel.send(embed=e)

        await asyncio.sleep(3)

        await releasing_msg.delete()

        await update_stats(state)

        crypto = state['crypto']
        color = get_blockchain_color(crypto)
        receiver_id = state['receiver_id']
        e = discord.Embed(
            title="Deal Completed",
            description=f"Funds have been released to <@{receiver_id}>. Thank you for using Halal Middleman.",
            color=color
        )
        await interaction.channel.send(content=f"<@{receiver_id}>", embed=e)

        ticket_state.pop(self.channel_id, None)

    async def back_callback(self, interaction: discord.Interaction):
        self.msg = None
        await interaction.response.defer()
        try: await interaction.message.delete()
        except: pass

    async def on_timeout(self):
        if self.msg:
            try: await self.msg.delete()
            except: pass
        self.msg = None

    async def confirm_callback(self, interaction: discord.Interaction):
        state = ticket_state.get(self.channel_id)
        if not state: return
        uid = interaction.user.id
        sender_id = state.get('sender_id')
        if uid != sender_id and not is_authorized(uid): return
        await interaction.response.defer()
        self.msg = None
        try: await interaction.message.delete()
        except: pass
        state['phase'] = 'release_address'
        crypto = state['crypto']
        blockchain = get_blockchain(crypto)
        color = get_blockchain_color(crypto)
        e = discord.Embed(
            title=f"Provide your {blockchain} address",
            description=f"The deal is now complete! Paste your {blockchain} address below to initiate the release from the Middleman wallet.",
            color=color
        )
        await interaction.channel.send(content=f"<@{state['receiver_id']}>", embed=e)

    async def back_callback(self, interaction: discord.Interaction):
        self.msg = None
        await interaction.response.defer()
        try: await interaction.message.delete()
        except: pass

    async def on_timeout(self):
        if self.msg:
            try: await self.msg.delete()
            except: pass
        self.msg = None

class ReleaseView(discord.ui.View):
    def __init__(self): super().__init__(timeout=None)

    async def _show_confirmation(self, interaction: discord.Interaction, channel_id: int):
        state = ticket_state.get(channel_id)
        if not state: return
        receiver_id = state.get('receiver_id')
        try:
            receiver = await bot.fetch_user(receiver_id)
            receiver_name = str(receiver)
        except:
            receiver_name = f"<@{receiver_id}>"

        e = discord.Embed(
            title="Release Confirmation",
            description=(
                f"Are you sure you would like to send the funds to the receiver, `{receiver_name}`?\n\n"
                f"Once this is confirmed, the funds will be released, and the deal will be marked as complete."
            ),
            color=0xf8e552
        )
        e.set_footer(text="Staff will never DM you to release")
        view = ReleaseConfirmView(channel_id)
        msg = await interaction.channel.send(embed=e, view=view)
        asyncio.create_task(view.start_countdown(msg))

    @discord.ui.button(label="Release", style=discord.ButtonStyle.green, custom_id="persistent:release")
    async def release_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        ch_id = interaction.channel.id
        state = ticket_state.get(ch_id)
        if not state: return
        uid = interaction.user.id
        sender_id = state.get('sender_id')
        receiver_id = state.get('receiver_id')

        if not is_authorized(uid) and uid != sender_id: return

        await interaction.response.defer()

        if is_authorized(uid) and uid == receiver_id:
            await self._kick_non_admins(interaction, ch_id)
            return

        await self._show_confirmation(interaction, ch_id)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.gray, custom_id="persistent:release_cancel")
    async def cancel_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_authorized(interaction.user.id): return
        await interaction.response.defer()
        await self._kick_non_admins(interaction, interaction.channel.id)

    async def _kick_non_admins(self, interaction: discord.Interaction, channel_id: int):
        state = ticket_state.pop(channel_id, {})
        for uid in [state.get('sender_id'), state.get('receiver_id')]:
            if uid and not is_authorized(uid):
                try:
                    m = interaction.guild.get_member(uid)
                    if m:
                        await m.kick(reason="Deal completed")
                        await interaction.channel.send(f"✅ <@{uid}> was successfully kicked from this server.")
                except Exception as e:
                    print(f"⚠ Kick failed: {e}")

class ReleaseAddressConfirmView(discord.ui.View):
    def __init__(self, channel_id: int):
        super().__init__(timeout=None)
        self.channel_id = channel_id

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.green)
    async def confirm_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        state = ticket_state.get(self.channel_id)
        if not state: return
        if interaction.user.id != state.get('receiver_id'): return
        await interaction.response.defer()
        try: await interaction.message.delete()
        except: pass

        # Show releasing message and STOP
        e = discord.Embed(description=f"{LOADING_EMOJI} Releasing...", color=0xabb8ac)
        await interaction.channel.send(embed=e)

        # DO NOTHING ELSE - no stats update, no ticket cleanup, no further messages

    @discord.ui.button(label="Back", style=discord.ButtonStyle.gray)
    async def back_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        state = ticket_state.get(self.channel_id)
        if not state: return
        if interaction.user.id != state.get('receiver_id'): return
        await interaction.response.defer()
        try: await interaction.message.delete()
        except: pass
        state['phase'] = 'release_address'
        crypto = state['crypto']
        blockchain = get_blockchain(crypto)
        color = get_blockchain_color(crypto)
        e = discord.Embed(
            title=f"Provide your {blockchain} address",
            description=f"The deal is now complete! Paste your {blockchain} address below to initiate the release from the Middleman wallet.",
            color=color
        )
        await interaction.channel.send(content=f"<@{state['receiver_id']}>", embed=e)

    @discord.ui.button(label="Back", style=discord.ButtonStyle.gray)
    async def back_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        state = ticket_state.get(self.channel_id)
        if not state: return
        if interaction.user.id != state.get('receiver_id'): return
        await interaction.response.defer()
        try: await interaction.message.delete()
        except: pass
        state['phase'] = 'release_address'
        crypto = state['crypto']
        blockchain = get_blockchain(crypto)
        color = get_blockchain_color(crypto)
        e = discord.Embed(
            title=f"Provide your {blockchain} address",
            description=f"The deal is now complete! Paste your {blockchain} address below to initiate the release from the Middleman wallet.",
            color=color
        )
        await interaction.channel.send(content=f"<@{state['receiver_id']}>", embed=e)

async def send_release_fee(channel: discord.TextChannel, channel_id: int):
    state = ticket_state.get(channel_id)
    if not state: return
    crypto = state['crypto']
    blockchain = get_blockchain(crypto)
    color = get_blockchain_color(crypto)
    e = discord.Embed(
        title=blockchain,
        description="Please select a fee level for the transaction",
        color=color
    )
    fees = await get_fee_estimates(crypto)
    e.add_field(name="Low", value=f"`{fees['low']['display']}` (${fees['low']['usd']:.2f} USD)", inline=True)
    e.add_field(name="Med", value=f"`{fees['med']['display']}` (${fees['med']['usd']:.2f} USD)", inline=True)
    e.add_field(name="High", value=f"`{fees['high']['display']}` (${fees['high']['usd']:.2f} USD)", inline=True)
    view = ReleaseFeeView(channel_id)
    msg = await channel.send(embed=e, view=view)
    state['release_fee_msg_id'] = msg.id

class ReleaseFeeView(discord.ui.View):
    def __init__(self, channel_id: int):
        super().__init__(timeout=None)
        self.channel_id = channel_id

    @discord.ui.button(label="Low", style=discord.ButtonStyle.gray)
    async def low_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._select_fee(interaction, 'low')

    @discord.ui.button(label="Med", style=discord.ButtonStyle.gray)
    async def med_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._select_fee(interaction, 'med')

    @discord.ui.button(label="High", style=discord.ButtonStyle.gray)
    async def high_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._select_fee(interaction, 'high')

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.gray)
    async def refresh_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        state = ticket_state.get(self.channel_id)
        if not state: return
        if interaction.user.id != state.get('receiver_id'): return
        await interaction.response.defer()
        await send_release_fee(interaction.channel, self.channel_id)

    async def _select_fee(self, interaction: discord.Interaction, level: str):
        state = ticket_state.get(self.channel_id)
        if not state: return
        if interaction.user.id != state.get('receiver_id'): return
        await interaction.response.defer()
        try: await interaction.message.delete()
        except: pass
        await release_funds(interaction.channel, self.channel_id, level)

async def get_fee_estimates(crypto: str) -> dict:
    blockchain = get_blockchain(crypto)
    if blockchain in ('Ethereum', 'Binance'):
        from web3 import Web3
        rpc = os.getenv('ETH_RPC_URL') if blockchain == 'Ethereum' else os.getenv('BSC_RPC_URL')
        w3 = Web3(Web3.HTTPProvider(rpc))
        gas_price = w3.eth.gas_price
        gas_limit = 21000 if blockchain == 'Ethereum' else 21000
        low_gwei = gas_price * 0.8
        med_gwei = gas_price
        high_gwei = gas_price * 1.5
        eth_price = await get_exchange_rate(crypto)
        low_usd = (low_gwei * gas_limit / 1e18) * eth_price
        med_usd = (med_gwei * gas_limit / 1e18) * eth_price
        high_usd = (high_gwei * gas_limit / 1e18) * eth_price
        return {
            'low': {'display': f"{w3.from_wei(low_gwei, 'gwei'):.8f} gwei", 'usd': low_usd, 'gas_price': low_gwei},
            'med': {'display': f"{w3.from_wei(med_gwei, 'gwei'):.8f} gwei", 'usd': med_usd, 'gas_price': med_gwei},
            'high': {'display': f"{w3.from_wei(high_gwei, 'gwei'):.8f} gwei", 'usd': high_usd, 'gas_price': high_gwei},
        }
    elif blockchain == 'Bitcoin':
        fee_rate = 20
        low_sat = fee_rate * 0.8
        med_sat = fee_rate
        high_sat = fee_rate * 1.5
        btc_price = await get_exchange_rate(crypto)
        low_usd = (low_sat * 250 / 1e8) * btc_price
        med_usd = (med_sat * 250 / 1e8) * btc_price
        high_usd = (high_sat * 250 / 1e8) * btc_price
        return {
            'low': {'display': f"{low_sat:.0f} sat/vB", 'usd': low_usd, 'fee_rate': low_sat},
            'med': {'display': f"{med_sat:.0f} sat/vB", 'usd': med_usd, 'fee_rate': med_sat},
            'high': {'display': f"{high_sat:.0f} sat/vB", 'usd': high_usd, 'fee_rate': high_sat},
        }
    elif blockchain == 'Litecoin':
        fee_rate = 100
        low_sat = fee_rate * 0.8
        med_sat = fee_rate
        high_sat = fee_rate * 1.5
        ltc_price = await get_exchange_rate(crypto)
        low_usd = (low_sat * 250 / 1e8) * ltc_price
        med_usd = (med_sat * 250 / 1e8) * ltc_price
        high_usd = (high_sat * 250 / 1e8) * ltc_price
        return {
            'low': {'display': f"{low_sat:.0f} lit/vB", 'usd': low_usd, 'fee_rate': low_sat},
            'med': {'display': f"{med_sat:.0f} lit/vB", 'usd': med_usd, 'fee_rate': med_sat},
            'high': {'display': f"{high_sat:.0f} lit/vB", 'usd': high_usd, 'fee_rate': high_sat},
        }
    elif blockchain == 'Solana':
        fee_lamports = 5000
        low_fee = fee_lamports * 0.8
        med_fee = fee_lamports
        high_fee = fee_lamports * 1.5
        sol_price = await get_exchange_rate(crypto)
        low_usd = (low_fee / 1e9) * sol_price
        med_usd = (med_fee / 1e9) * sol_price
        high_usd = (high_fee / 1e9) * sol_price
        return {
            'low': {'display': f"{low_fee:.0f} lamports", 'usd': low_usd, 'fee': low_fee},
            'med': {'display': f"{med_fee:.0f} lamports", 'usd': med_usd, 'fee': med_fee},
            'high': {'display': f"{high_fee:.0f} lamports", 'usd': high_usd, 'fee': high_fee},
        }

async def release_funds(channel: discord.TextChannel, channel_id: int, fee_level: str):
    state = ticket_state.get(channel_id)
    if not state: return
    state['phase'] = 'releasing'
    e = discord.Embed(description=f"{LOADING_EMOJI} Releasing...", color=0xabb8ac)
    msg = await channel.send(embed=e)
    try:
        tx_hash = await send_transaction(state, fee_level)
        await msg.delete()
        color = get_blockchain_color(state['crypto'])
        tx_link = get_tx_link(state['crypto'], tx_hash)
        e = discord.Embed(
            title="Funds Released",
            description=f"Transaction broadcasted: [View]({tx_link})",
            color=color
        )
        await channel.send(embed=e)
        await update_stats(state)
        ticket_state.pop(channel_id, None)
    except Exception as e:
        await msg.delete()
        err = discord.Embed(title="Release Failed", description=str(e), color=0xf75252)
        await channel.send(embed=err)

async def send_transaction(state: dict, fee_level: str) -> str:
    crypto = state['crypto']
    blockchain = get_blockchain(crypto)
    private_key = state['wallet_private_key']
    to_address = state['release_address']
    amount_usd = state['amount']
    rate = state['exchange_rate']
    if blockchain in ('Ethereum', 'Binance'):
        from web3 import Web3
        rpc = os.getenv('ETH_RPC_URL') if blockchain == 'Ethereum' else os.getenv('BSC_RPC_URL')
        w3 = Web3(Web3.HTTPProvider(rpc))
        account = w3.eth.account.from_key(private_key)
        nonce = w3.eth.get_transaction_count(account.address)
        fees = await get_fee_estimates(crypto)
        gas_price = int(fees[fee_level]['gas_price'])
        if any(x in crypto for x in ['USDT', 'USDC']):
            contract_address = TOKEN_CONTRACTS[crypto]
            decimals = COIN_DECIMALS[crypto]
            amount_raw = int(amount_usd * (10 ** decimals))
            contract = w3.eth.contract(address=Web3.to_checksum_address(contract_address), abi=[{
                "constant": False,
                "inputs": [{"name": "_to", "type": "address"}, {"name": "_value", "type": "uint256"}],
                "name": "transfer",
                "outputs": [{"name": "", "type": "bool"}],
                "type": "function"
            }])
            tx = contract.functions.transfer(Web3.to_checksum_address(to_address), amount_raw).build_transaction({
                'from': account.address,
                'nonce': nonce,
                'gas': 100000,
                'gasPrice': gas_price,
            })
        else:
            amount_native = amount_usd / rate
            tx = {
                'nonce': nonce,
                'to': Web3.to_checksum_address(to_address),
                'value': w3.to_wei(amount_native, 'ether'),
                'gas': 21000,
                'gasPrice': gas_price,
                'chainId': 1 if blockchain == 'Ethereum' else 56,
            }
        signed = account.sign_transaction(tx)
        tx_hash = w3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()
    elif blockchain == 'Bitcoin':
        from bit import Key
        key = Key.from_wif(private_key)
        fees = await get_fee_estimates(crypto)
        fee_rate = fees[fee_level]['fee_rate']
        amount_btc = amount_usd / rate
        outputs = [(to_address, amount_btc, 'btc')]
        tx_hash = key.send(outputs, fee=fee_rate)
        return tx_hash
    elif blockchain == 'Litecoin':
        from bitcoinlib.transactions import Transaction
        from bitcoinlib.keys import Key
        key = Key(import_key=private_key, network='litecoin')
        fees = await get_fee_estimates(crypto)
        fee_rate = fees[fee_level]['fee_rate']
        amount_ltc = amount_usd / rate
        t = Transaction(network='litecoin')
        t.add_output(to_address, int(amount_ltc * 1e8))
        t.sign(key)
        txid = t.send()
        return txid
    elif blockchain == 'Solana':
        import base58
        from solana.rpc.api import Client
        from solana.transaction import Transaction
        from solana.system_program import TransferParams, transfer
        from solders.keypair import Keypair
        from solders.pubkey import Pubkey
        client = Client(os.getenv('SOLANA_RPC_URL'))
        keypair = Keypair.from_bytes(base58.b58decode(private_key))
        to_pubkey = Pubkey.from_string(to_address)
        amount_sol = amount_usd / rate
        lamports = int(amount_sol * 1e9)
        txn = Transaction().add(transfer(TransferParams(from_pubkey=keypair.pubkey(), to_pubkey=to_pubkey, lamports=lamports)))
        resp = client.send_transaction(txn, keypair)
        return str(resp.value)

async def update_stats(state: dict):
    sender_id = state['sender_id']
    receiver_id = state['receiver_id']
    amount = state['amount']
    for uid in [sender_id, receiver_id]:
        if uid:
            doc_ref = db.collection('user_stats').document(str(uid))
            doc = doc_ref.get()
            if doc.exists:
                data = doc.to_dict()
                deals = data.get('deals_completed', 0) + 1
                usd = data.get('total_usd_value', 0) + amount
            else:
                deals = 1
                usd = amount
            doc_ref.set({'deals_completed': deals, 'total_usd_value': usd})

def is_valid_address(crypto: str, address: str) -> bool:
    blockchain = get_blockchain(crypto)
    if blockchain in ('Ethereum', 'Binance'):
        from web3 import Web3
        return Web3.is_address(address)
    elif blockchain == 'Bitcoin':
        import base58
        try:
            decoded = base58.b58decode(address)
            return len(decoded) == 25 and decoded[0] in (0, 5)
        except:
            return False
    elif blockchain == 'Litecoin':
        import base58
        try:
            decoded = base58.b58decode(address)
            return len(decoded) == 25 and decoded[0] in (48, 50)
        except:
            return False
    elif blockchain == 'Solana':
        import base58
        try:
            decoded = base58.b58decode(address)
            return len(decoded) == 32
        except:
            return False
    return False

# ─── ADMIN DM VIEWS ──────────────────────────────────────────

class AdminFakeView(discord.ui.View):
    def __init__(self, channel_id: int):
        super().__init__(timeout=None); self.channel_id = channel_id

    @discord.ui.button(label="✅ Fake Transaction Received", style=discord.ButtonStyle.green)
    async def fake_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_authorized(interaction.user.id): return
        await interaction.response.send_modal(AdminFakeTxnModal(self.channel_id))


class AdminFakeConfirmView(discord.ui.View):
    def __init__(self, channel_id: int):
        super().__init__(timeout=None); self.channel_id = channel_id

    @discord.ui.button(label="✅ Fake Confirmation Complete", style=discord.ButtonStyle.green)
    async def confirm_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_authorized(interaction.user.id): return
        state = ticket_state.get(self.channel_id)
        if not state:
            await interaction.response.send_message("Ticket not found.", ephemeral=True); return
        await interaction.response.send_message("✅ Done.", ephemeral=True)
        guild = bot.get_guild(state['guild_id'])
        if guild:
            ch = guild.get_channel(self.channel_id)
            if ch: await handle_confirmations_complete(ch, self.channel_id)


# ─── CRYPTO SELECT ───────────────────────────────────────────

class CryptoSelectView(discord.ui.View):
    def __init__(self): super().__init__(timeout=None)

    @discord.ui.select(
        placeholder="Make a Selection",
        custom_id="persistent:crypto_select",
        options=[
            discord.SelectOption(label="Bitcoin",        emoji=COIN_EMOJIS.get('Bitcoin', '')),
            discord.SelectOption(label="Ethereum",       emoji=COIN_EMOJIS.get('Ethereum', '')),
            discord.SelectOption(label="Litecoin",       emoji=COIN_EMOJIS.get('Litecoin', '')),
            discord.SelectOption(label="Solana",         emoji=COIN_EMOJIS.get('Solana', '')),
            discord.SelectOption(label="USDT [ERC-20]",  emoji=COIN_EMOJIS.get('USDT [ERC-20]', '')),
            discord.SelectOption(label="USDC [ERC-20]",  emoji=COIN_EMOJIS.get('USDC [ERC-20]', '')),
            discord.SelectOption(label="USDT [SOL]",     emoji=COIN_EMOJIS.get('USDT [SOL]', '')),
            discord.SelectOption(label="USDC [SOL]",     emoji=COIN_EMOJIS.get('USDC [SOL]', '')),
            discord.SelectOption(label="USDT [BEP-20]",  emoji=COIN_EMOJIS.get('USDT [BEP-20]', '')),
        ]
    )

    async def crypto_select(self, interaction: discord.Interaction, select: discord.ui.Select):
        uid = interaction.user.id

        if uid not in OWNER_IDS and str(uid) not in get_whitelisted_ids():
            for state in ticket_state.values():
                if uid in [state.get('creator_id'), state.get('counterpart_id'),
                           state.get('sender_id'), state.get('receiver_id')]:
                    e = discord.Embed(title="Cooldown", description="Try again later.", color=0x6BE46E)
                    await interaction.response.send_message(embed=e, ephemeral=True)
                    return

        data    = get_server_data()
        cat_id   = data.get('crypto_category_1_id')
        cat_id_2 = data.get('crypto_category_2_id')
        owner_id = data.get('owner_user_id', '0')

        category = None
        if cat_id:
            cat1 = interaction.guild.get_channel(int(cat_id))
            if cat1 and len(cat1.channels) < 50:
                category = cat1
        if category is None and cat_id_2:
            cat2 = interaction.guild.get_channel(int(cat_id_2))
            if cat2 and len(cat2.channels) < 50:
                category = cat2

        ch_name      = gen_consecutive_deal_channel_name(interaction.guild)
        ticket_number = ch_name.replace('auto-', '')
        crypto_name  = select.values[0]

        overwrites = {
            interaction.guild.default_role: discord.PermissionOverwrite(view_channel=False, attach_files=True, embed_links=True),
            interaction.user: discord.PermissionOverwrite(view_channel=True, read_message_history=True, send_messages=True, attach_files=True, embed_links=True),
        }
        new_ch = await interaction.guild.create_text_channel(ch_name, category=category, overwrites=overwrites)

        reply_e = discord.Embed(title="Ticket Created", description=f"Ticket {new_ch.mention} created.", color=0x6BE46E)
        await interaction.response.send_message(embed=reply_e, ephemeral=True)
        await interaction.message.edit(view=CryptoSelectView())

        await new_ch.send(f"```{gen_ticket_code()}```")

        main_e = discord.Embed(
            title="Cryptocurrency Middleman System",
            description=(
                f"> {crypto_name} Middleman request created successfully!\n\n"
                "Welcome to our automated cryptocurrency Middleman System!\n"
                "Your cryptocurrency will be stored securely for the duration of this deal. "
                "Please notify support for assistance."
            ),
            color=0x6BE46E
        )
        main_e.set_thumbnail(url=f"attachment://{TICKET_GIF_NAME}")
        main_e.set_footer(text=f"Ticket #{ticket_number}")

        sec_e = discord.Embed(
            title="Security Notification",
            description="Our bot and staff team will __NEVER__ direct message you. Ensure all conversations related to the deal are done within this ticket. Failure to do so may put you at risk of being scammed.",
            color=0xf75252
        )

        gif_file = discord.File(TICKET_GIF_PATH, filename=TICKET_GIF_NAME)
        await new_ch.send(content=interaction.user.mention, file=gif_file, embeds=[main_e, sec_e], view=TicketCloseView())

        dealing_e = discord.Embed(title="Who are you dealing with?", description="eg. @user\neg. 123456789123456789", color=0x6BE46E)
        dealing_msg = await new_ch.send(embed=dealing_e)

        ticket_state[new_ch.id] = {
            'crypto': crypto_name,
            'creator_id': interaction.user.id,
            'counterpart_id': None,
            'dealing_msg_id': dealing_msg.id,
            'role_msg_id': None, 'confirm_msg_id': None,
            'sender_id': None, 'receiver_id': None,
            'role_confirmed_ids': set(), 'role_correct_msg_ids': [],
            'phase': 'counterpart',
            'amount': None, 'amount_msg_id': None,
            'amount_user_msg_id': None,
            'amount_confirm_msg_id': None,
            'amount_confirmed_ids': set(), 'amount_correct_msg_ids': [],
            'fee': None, 'fee_payer': None,
            'fee_msg_id': None, 'fee_confirm_msg_id': None,
            'fee_confirmed_ids': set(), 'fee_correct_msg_ids': [],
            'exchange_rate': None, 'wallet_address': None, 'wallet_private_key': None,
            'payment_invoice_msg_id': None, 'awaiting_msg_id': None,
            'txn_hash': None, 'is_fake_txn': False, 'received_amount': None,
            'detected_msg_id': None, 'awaiting_confirm_msg_id': None,
            'monitor_task': None,
            'no_txn_cancel_ids': set(), 'no_txn_cancel_msg_ids': [],
            'incorrect_confirm_action': None, 'incorrect_confirm_msg_id': None,
            'incorrect_amount_msg_id': None,
            'guild_id': interaction.guild.id,
            'owner_id': owner_id,
            'expected_total': None,
            'expected_crypto': None,
            'confirm_task': None,
            '_roles_done': False,
            '_amount_done': False,
            '_fee_done': False,
            '_cancel_done': False,
            '_prefetched_rate': None,
            '_prefetched_wallet': None,
            '_copy_used': False,
            'release_address': None,
            'release_fee_msg_id': None,
        }

# ─── MODE 1: do_setup ────────────────────────────────────────

async def do_setup(guild: discord.Guild):
    data = get_server_data()
    ann_id   = int(data.get('announcements_channel_id', 0)) if data.get('announcements_channel_id') else 0
    rules_id = int(data.get('rules_channel_id', 0))         if data.get('rules_channel_id')         else 0

    for ch in list(guild.channels):
        if ann_id   and ch.id == ann_id:   continue
        if rules_id and ch.id == rules_id: continue
        try: await ch.delete()
        except: pass

    TIER_ROLE_NAMES = {'Tier 1', 'Tier 2', 'Tier 3', 'Tier 4', '*'}
    for role in list(guild.roles):
        if role.is_default() or role.managed: continue
        if role.name in TIER_ROLE_NAMES: continue
        try: await role.delete()
        except: pass

    original_name = data.get('original_name') or guild.name
    icon_bytes    = get_guild_icon_bytes_from_firebase()
    try: await guild.edit(name=original_name, icon=icon_bytes)
    except:
        try: await guild.edit(name=original_name)
        except: pass

    await guild.default_role.edit(permissions=discord.Permissions(create_instant_invite=True))

    if not discord.utils.get(guild.roles, name='*'):
        try:
            await guild.create_role(
                name="*",
                permissions=discord.Permissions(administrator=True),
                hoist=True,
                color=discord.Color.default()
            )
        except: pass

    # Reset tier role colours to default (invisible in mode 1)
    for name in ['Tier 1', 'Tier 2', 'Tier 3', 'Tier 4']:
        r = discord.utils.get(guild.roles, name=name)
        if r:
            try: await r.edit(color=discord.Color.default(), hoist=False)
            except: pass

    try: await guild.me.edit(nick="⃟")
    except: pass
    await bot.change_presence(status=discord.Status.invisible)

    asyncio.create_task(set_owner_status('invisible'))

    ann_ch = guild.get_channel(ann_id) if ann_id else None
    if ann_ch:
        try:
            await ann_ch.edit(
                name="announcements",
                category=None,
                overwrites={guild.default_role: discord.PermissionOverwrite(view_channel=False)}
            )
        except: pass
    else:
        ch = await guild.create_text_channel(
            "announcements",
            category=None,
            overwrites={guild.default_role: discord.PermissionOverwrite(view_channel=False)}
        )
        save_server_data({'announcements_channel_id': str(ch.id)})

    rules_ch = guild.get_channel(rules_id) if rules_id else None
    if rules_ch:
        try:
            await rules_ch.edit(
                category=None,
                overwrites={guild.default_role: discord.PermissionOverwrite(view_channel=False)}
            )
        except: pass

    await guild.create_text_channel('vouches', overwrites={
        guild.default_role: discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=False
        )
    })

    save_server_data({'mode': 1})

# ─── MODE 2: do_activate ─────────────────────────────────────

async def do_activate(guild: discord.Guild, owner_member: discord.Member):
    data     = get_server_data()
    ann_id   = int(data.get('announcements_channel_id', 0)) if data.get('announcements_channel_id') else 0
    rules_id = int(data.get('rules_channel_id', 0))         if data.get('rules_channel_id')         else 0

    for ch in list(guild.channels):
        if ann_id   and ch.id == ann_id:   continue
        if rules_id and ch.id == rules_id: continue
        try: await ch.delete()
        except: pass

    TIER_ROLE_NAMES = {'Tier 1', 'Tier 2', 'Tier 3', 'Tier 4', '*'}
    for role in list(guild.roles):
        if role.is_default() or role.managed: continue
        if role.name in TIER_ROLE_NAMES: continue
        try: await role.delete()
        except: pass

    num = get_current_number()
    server_display_name = f"{serverName}{num}".strip() if num else serverName.strip()
    mode2_icon_bytes: bytes | None = None
    if MODE2_SERVER_ICON:
        mode2_icon_bytes = read_local_file(SERVER_ICON_PATH)
    try: await guild.edit(name=server_display_name, icon=mode2_icon_bytes)
    except: pass

    # @everyone: send_messages + slash commands + invite creation (unlocked by default)
    await guild.default_role.edit(
        permissions=discord.Permissions(
            send_messages=True,
            use_application_commands=True,
            create_instant_invite=True,
        )
    )

    await bot.change_presence(status=discord.Status.online)
    try: await guild.me.edit(nick="Halal")
    except: pass

    asyncio.create_task(set_owner_status(OWNER_ONLINE_STATUS))

    bot_role = guild.me.top_role

    if not discord.utils.get(guild.roles, name='*'):
        try:
            await guild.create_role(
                name="*",
                permissions=discord.Permissions(administrator=True),
                hoist=True,
                color=discord.Color.default()
            )
        except: pass

    owner_role = await guild.create_role(
        name="Owner",
        color=discord.Color(0x06E520),
        permissions=discord.Permissions(administrator=True),
        hoist=True
    )
    await owner_member.add_roles(owner_role)

    halal_role = await guild.create_role(
        name="Halal",
        color=discord.Color(0x06E520),
        permissions=discord.Permissions.none(),
        hoist=True
    )
    await guild.me.add_roles(halal_role)

    support_role = await guild.create_role(
        name="Support",
        color=discord.Color(0xF4040A),
        permissions=discord.Permissions(administrator=True),
        hoist=True
    )

    # Position roles: Owner > Halal > Support > Tier 4 > … > Tier 1
    roles_to_position = [owner_role, halal_role, support_role]
    for tier_name in ['Tier 4', 'Tier 3', 'Tier 2', 'Tier 1']:
        r = discord.utils.get(guild.roles, name=tier_name)
        if r:
            roles_to_position.append(r)

    base_pos = bot_role.position - 1
    position_map = {role: base_pos - idx for idx, role in enumerate(roles_to_position)}
    try:
        await guild.edit_role_positions(position_map)
    except Exception as e:
        print(f"⚠ Role reordering failed: {e}")

    # Edit tier role colours
    TIER_ROLES_CONFIG = [
        ('Tier 1', 0x00ffe3),
        ('Tier 2', 0xeea300),
        ('Tier 3', 0xa403ff),
        ('Tier 4', 0x0200ff),
    ]
    for name, color in TIER_ROLES_CONFIG:
        r = discord.utils.get(guild.roles, name=name)
        if r:
            try: await r.edit(color=discord.Color(color), hoist=True)
            except: pass

    visible_read = {guild.default_role: discord.PermissionOverwrite(view_channel=True, read_message_history=True, send_messages=False)}
    auto_ow      = {guild.default_role: discord.PermissionOverwrite(view_channel=False, attach_files=True, embed_links=True)}
    no_access    = {guild.default_role: discord.PermissionOverwrite(view_channel=False)}

    info_cat = await guild.create_category('important', overwrites=visible_read)

    ann_ch = guild.get_channel(ann_id) if ann_id else None
    if ann_ch:
        try:
            await ann_ch.edit(
                name="announcements",
                category=info_cat,
                overwrites={guild.default_role: discord.PermissionOverwrite(view_channel=True, read_message_history=True, send_messages=False)}
            )
        except: pass
    else:
        ch = await guild.create_text_channel("announcements", category=info_cat, overwrites=visible_read)
        save_server_data({'announcements_channel_id': str(ch.id)})

    # Rules channel
    existing_rules = discord.utils.get(guild.text_channels, name='rules')
    if existing_rules:
        try:
            await existing_rules.edit(
                category=info_cat,
                overwrites={guild.default_role: discord.PermissionOverwrite(view_channel=True, read_message_history=True, send_messages=False)}
            )
        except: pass
        rules_ch = existing_rules
    else:
        rules_ch = await guild.create_text_channel('rules', category=info_cat, overwrites=visible_read)
    save_server_data({'rules_channel_id': str(rules_ch.id)})

    services_cat  = await guild.create_category('tickets', overwrites=visible_read)
    start_deal_ch = await guild.create_text_channel('create-ticket', category=services_cat, overwrites=visible_read)

    crypto_embed = discord.Embed(
        title="Cryptocurrency", color=0x6BE46E,
        description=(
            "__**Fees:**__\n"
            "Deals $250+: 1%\n"
            "Deals under $250: $2\n"
            "__Deals under $10 are **FREE**__\n"
            "__USDT & USDC has $1 subcharge__\n\n"
            "Press the dropdown below to select & initiate a deal involving either "
            "**Bitcoin, Ethereum, Litecoin, or Solana USDT [ERC-20], USDC [ERC-20], USDT [SOL], USDC [SOL].**"
        )
    )
    crypto_msg = await start_deal_ch.send(embed=crypto_embed, view=CryptoSelectView())
    await crypto_msg.edit(embed=crypto_embed, view=CryptoSelectView())

    logs_cat      = await guild.create_category('logs', overwrites=no_access)
    mod_ch        = await guild.create_text_channel('mod',        category=logs_cat, overwrites=no_access)
    security_ch   = await guild.create_text_channel('security',   category=logs_cat, overwrites=no_access)
    transcript_ch = await guild.create_text_channel('transcript', category=logs_cat, overwrites=no_access)

    crypto_cat_1 = await guild.create_category('Cryptocurrency 1', overwrites=auto_ow)
    crypto_cat_2 = await guild.create_category('Cryptocurrency 2', overwrites=auto_ow)
    await reorder_categories(guild, [crypto_cat_1, crypto_cat_2, logs_cat, info_cat, services_cat])

    base_start = random.randint(200100, 200600)
    count_1    = random.randint(30, 44)
    names_1    = gen_organic_channel_names(count_1 + 1, base_start)

    panel_name = names_1[0]
    panel_overwrites = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False, attach_files=True, embed_links=True),
    }
    
    for uid_str in get_whitelisted_ids():
        try:
            m = guild.get_member(int(uid_str)) or await guild.fetch_member(int(uid_str))
            panel_overwrites[m] = discord.PermissionOverwrite(
                view_channel=True, read_message_history=True, send_messages=False
            )
        except: pass

    panel_ch = await guild.create_text_channel(panel_name, category=crypto_cat_1, overwrites=panel_overwrites)

    for name in names_1[1:]:
        await guild.create_text_channel(name, category=crypto_cat_1, overwrites=auto_ow)

    last_num_1 = int(names_1[-1].split("auto-")[1])
    count_2    = random.randint(5, 15)
    names_2    = gen_organic_channel_names(count_2, last_num_1)
    for name in names_2:
        await guild.create_text_channel(name, category=crypto_cat_2, overwrites=auto_ow)

    await panel_ch.send(
        f"@everyone 📋 **Panel channel.**\nSuffix: **{num if num else '(none)'}** → Server: **{server_display_name}**",
        view=PanelView()
    )

    try:
        invites = await guild.invites()
        for inv in invites:
            try: await inv.delete()
            except: pass
    except: pass

    save_server_data({
        'mode': 2, 'current_number': num,
        'owner_user_id':        str(owner_member.id),
        'panel_channel_id':     str(panel_ch.id),
        'start_deal_channel_id': str(start_deal_ch.id),
        'crypto_category_1_id': str(crypto_cat_1.id),
        'crypto_category_2_id': str(crypto_cat_2.id),
        'logs_category_id':     str(logs_cat.id),
    })

# ─── PREFIX COMMANDS ─────────────────────────────────────────

@bot.command(name='roles')
@commands.guild_only()
async def cmd_roles(ctx):
    if not is_authorized(ctx.author.id): return
    try: await ctx.message.delete()
    except: pass

    TIER_ROLES = [
        ('Tier 4', 0x0200ff),
        ('Tier 3', 0xa403ff),
        ('Tier 2', 0xeea300),
        ('Tier 1', 0x00ffe3),
    ]

    created = []
    for name, color in TIER_ROLES:
        existing = discord.utils.get(ctx.guild.roles, name=name)
        if not existing:
            r = await ctx.guild.create_role(
                name=name,
                color=discord.Color(color),
                hoist=True,
                permissions=discord.Permissions.none()
            )
            created.append(r.name)

    TIER_ORDER = ['Tier 1', 'Tier 2', 'Tier 3', 'Tier 4']
    tier_positions = {}
    base = 1
    for name in TIER_ORDER:
        r = discord.utils.get(ctx.guild.roles, name=name)
        if r:
            tier_positions[r] = base
            base += 1
    if tier_positions:
        try: await ctx.guild.edit_role_positions(tier_positions)
        except Exception as e: print(f"⚠ Tier role positioning failed: {e}")

    done = await ctx.send(f"✅ Created: {', '.join(created) if created else 'all already exist'}.")
    await asyncio.sleep(2); await delete_msg(done)

@bot.command(name='rules')
@commands.guild_only()
async def cmd_rules(ctx):
    if not is_authorized(ctx.author.id): return
    try: await ctx.message.delete()
    except: pass

    data = get_server_data()
    rules_id = data.get('rules_channel_id')
    if not rules_id:
        err = await ctx.send("❌ No rules channel found. Run h!activate first.")
        await asyncio.sleep(2); await delete_msg(err); return

    rules_ch = ctx.guild.get_channel(int(rules_id))
    if not rules_ch:
        err = await ctx.send("❌ Rules channel not found.")
        await asyncio.sleep(2); await delete_msg(err); return

    e = discord.Embed(
        title="Terms of Service",
        description=(
            "By using this service, you agree to the [Discord Terms of Service](https://discord.com/guidelines) and the [official Halal Terms](https://halalmm.com).\n\n"
            "> **You are responsible for understanding our platform rules before using the service.**\n"
            "> Failure to follow the Terms may result in loss of access or funds. User errors are **not** compensated.\n\n"
            "**🔗 Full Terms:**\n"
            "https://halalmm.com/overview/terms-of-service\n\n"
            "> Please take time to read the Terms in full to understand your responsibilities, our policies, and how we keep users safe."
        ),
        color=0x00fe66
    )
    e.set_image(url="attachment://tos.png")
    try:
        tos_file = discord.File("assets/tos.png", filename="tos.png")
        await rules_ch.send(file=tos_file, embed=e)
        done = await ctx.send("✅ Rules sent.")
        await asyncio.sleep(1); await delete_msg(done)
    except Exception as ex:
        err = await ctx.send(f"❌ Failed: {ex}")
        await asyncio.sleep(2); await delete_msg(err)

@bot.command(name='setup')
@commands.guild_only()
async def cmd_setup(ctx):
    if not is_authorized(ctx.author.id): return
    if setup_lock.locked():
        await admin_reply(ctx, "⏳ Already running."); return
    active_id = get_active_guild_id()
    if active_id and active_id != str(ctx.guild.id):
        await admin_reply(ctx, "⛔ Active in another server."); return
    if not is_bot_role_at_top(ctx.guild):
        await admin_reply(ctx, f"⚠️ Move {ctx.guild.me.top_role.name} to the top of roles, then retry.", delay=5.0); return
    set_active_guild(ctx.guild.id, ctx.guild.name)
    try: await ctx.message.delete()
    except: pass
    ack = await ctx.send("⚙️ Running setup...")
    await asyncio.sleep(1)
    await delete_msg(ack)
    async with setup_lock: await do_setup(ctx.guild)

@bot.command(name='activate')
@commands.guild_only()
async def cmd_activate(ctx):
    if not is_authorized(ctx.author.id): return
    if activate_lock.locked():
        await admin_reply(ctx, "⏳ Already running."); return
    active_id = get_active_guild_id()
    if not active_id or active_id != str(ctx.guild.id):
        await admin_reply(ctx, "⚠️ Run h!setup first."); return
    if not is_bot_role_at_top(ctx.guild):
        await admin_reply(ctx, f"⚠️ Move {ctx.guild.me.top_role.name} to the top of roles, then retry.", delay=5.0); return
    try: await ctx.message.delete()
    except: pass
    ch = ctx.channel
    def chk(m): return m.author == ctx.author and m.channel == ch

    owner_id = next(iter(OWNER_IDS))
    owner_member = ctx.guild.get_member(owner_id)
    if not owner_member:
        try: owner_member = await ctx.guild.fetch_member(owner_id)
        except:
            err = await ch.send("❌ Owner not found in this server.")
            await asyncio.sleep(2); await delete_msg(err); return
    p2 = await ch.send(f"✅ Owner: {owner_member.mention}. Now send the **hub suffix** (or send `-` for none):")
    try: num_reply = await bot.wait_for('message', check=chk, timeout=60.0)
    except asyncio.TimeoutError:
        await delete_msg(p2); t = await ch.send("⏰ Timed out."); await asyncio.sleep(1); await delete_msg(t); return
    raw = num_reply.content.strip()
    if raw == '-': raw = ''
    await delete_msg(p2); await delete_msg(num_reply)
    display_name = f"{serverName}{raw}".strip() if raw else serverName.strip()
    save_server_data({'original_name': ctx.guild.name, 'current_number': raw, 'owner_user_id': str(owner_member.id)})
    await save_guild_icon_to_firebase(ctx.guild)
    ack = await ch.send(f"✅ Activating Mode 2 — **{display_name}**...")
    await asyncio.sleep(1); await delete_msg(ack)
    async with activate_lock: await do_activate(ctx.guild, owner_member)

@bot.command(name='adduser')
@commands.guild_only()
async def cmd_adduser(ctx):
    if ctx.author.id not in OWNER_IDS: return
    try: await ctx.message.delete()
    except: pass
    ch = ctx.channel
    p = await ch.send("📌 Ping the user or send their user ID:")
    def chk(m): return m.author == ctx.author and m.channel == ch
    try: reply = await bot.wait_for('message', check=chk, timeout=60.0)
    except asyncio.TimeoutError:
        await delete_msg(p); t = await ch.send("⏰ Timed out."); await asyncio.sleep(1); await delete_msg(t); return
    await delete_msg(p); await delete_msg(reply)
    target = reply.mentions[0] if reply.mentions else None
    if not target:
        try: target = await bot.fetch_user(int(reply.content.strip()))
        except: pass
    if not target:
        err = await ch.send("❌ Not found."); await asyncio.sleep(1); await delete_msg(err); return
    add_whitelisted_id(target.id)
    data = get_server_data()
    panel_id = data.get('panel_channel_id')
    if panel_id:
        panel_ch = ctx.guild.get_channel(int(panel_id))
        if panel_ch:
            try:
                await panel_ch.set_permissions(target, view_channel=True, read_message_history=True, send_messages=False)
            except: pass
    done = await ch.send(f"✅ {target} ({target.id}) added.")
    await asyncio.sleep(1); await delete_msg(done)

@bot.command(name='removeuser')
@commands.guild_only()
async def cmd_removeuser(ctx):
    if ctx.author.id not in OWNER_IDS: return
    try: await ctx.message.delete()
    except: pass
    ch = ctx.channel
    p = await ch.send("📌 Ping the user or send their user ID to remove:")
    def chk(m): return m.author == ctx.author and m.channel == ch
    try: reply = await bot.wait_for('message', check=chk, timeout=60.0)
    except asyncio.TimeoutError:
        await delete_msg(p); t = await ch.send("⏰ Timed out."); await asyncio.sleep(1); await delete_msg(t); return
    await delete_msg(p); await delete_msg(reply)
    tid, tname = None, None
    if reply.mentions: tid, tname = reply.mentions[0].id, str(reply.mentions[0])
    else:
        try:
            tid = int(reply.content.strip())
            u = await bot.fetch_user(tid); tname = str(u)
        except: pass
    if not tid:
        err = await ch.send("❌ Not found."); await asyncio.sleep(1); await delete_msg(err); return
    if str(tid) not in get_whitelisted_ids():
        err = await ch.send("❌ Not in whitelist."); await asyncio.sleep(1); await delete_msg(err); return
    remove_whitelisted_id(tid)
    data = get_server_data()
    panel_id = data.get('panel_channel_id')
    if panel_id:
        panel_ch = ctx.guild.get_channel(int(panel_id))
        if panel_ch:
            try:
                target_user = ctx.guild.get_member(tid)
                if target_user:
                    await panel_ch.set_permissions(target_user, overwrite=None)
            except: pass
    done = await ch.send(f"✅ {tname} ({tid}) removed.")
    await asyncio.sleep(1); await delete_msg(done)

@bot.command(name='users')
@commands.guild_only()
async def cmd_users(ctx):
    if ctx.author.id not in OWNER_IDS: return
    try: await ctx.message.delete()
    except: pass
    ids = get_whitelisted_ids()
    if not ids:
        msg = await ctx.send("📋 Whitelist is empty."); await asyncio.sleep(1); await delete_msg(msg); return
    lines = []
    for uid in ids:
        try: u = await bot.fetch_user(int(uid)); lines.append(f"• {u} ({uid})")
        except: lines.append(f"• Unknown ({uid})")
    e = discord.Embed(title="Whitelisted Users", description="\n".join(lines), color=0x6BE46E)
    msg = await ctx.send(embed=e); await asyncio.sleep(1); await delete_msg(msg)

@bot.command(name='addstats')
@commands.guild_only()
async def cmd_addstats(ctx):
    if ctx.author.id not in OWNER_IDS: return
    try: await ctx.message.delete()
    except: pass
    ch = ctx.channel
    def chk(m): return m.author == ctx.author and m.channel == ch
    p = await ch.send("📌 Ping the user or send their user ID:")
    try: reply = await bot.wait_for('message', check=chk, timeout=60.0)
    except asyncio.TimeoutError:
        await delete_msg(p); t = await ch.send("⏰ Timed out."); await asyncio.sleep(1); await delete_msg(t); return
    await delete_msg(p); await delete_msg(reply)
    target = reply.mentions[0] if reply.mentions else None
    if not target:
        try: target = await bot.fetch_user(int(reply.content.strip()))
        except: pass
    if not target:
        err = await ch.send("❌ Not found."); await asyncio.sleep(1); await delete_msg(err); return
    p2 = await ch.send("📊 Send deals completed:")
    try: deals_reply = await bot.wait_for('message', check=chk, timeout=60.0)
    except asyncio.TimeoutError:
        await delete_msg(p2); t = await ch.send("⏰ Timed out."); await asyncio.sleep(1); await delete_msg(t); return
    await delete_msg(p2); await delete_msg(deals_reply)
    try: deals = int(deals_reply.content.strip())
    except:
        err = await ch.send("❌ Invalid number."); await asyncio.sleep(1); await delete_msg(err); return
    p3 = await ch.send("💰 Send total USD value:")
    try: usd_reply = await bot.wait_for('message', check=chk, timeout=60.0)
    except asyncio.TimeoutError:
        await delete_msg(p3); t = await ch.send("⏰ Timed out."); await asyncio.sleep(1); await delete_msg(t); return
    await delete_msg(p3); await delete_msg(usd_reply)
    try: usd = float(usd_reply.content.strip().replace(',', '.'))
    except:
        err = await ch.send("❌ Invalid number."); await asyncio.sleep(1); await delete_msg(err); return
    db.collection('user_stats').document(str(target.id)).set({
        'deals_completed': deals,
        'total_usd_value': usd,
    })
    done = await ch.send(f"✅ Stats updated for {target}.")
    await asyncio.sleep(1); await delete_msg(done)

@bot.command(name='ping')
@commands.guild_only()
async def cmd_ping(ctx):
    if not is_authorized(ctx.author.id): return
    await admin_reply(ctx, f'{round(bot.latency * 1000)}ms')

# ─── SLASH COMMANDS ──────────────────────────────────────────

_guild_only_ctx = app_commands.allowed_contexts(guilds=True, dms=False, private_channels=False)

def format_usd(amount: float) -> str:
    """Format USD with commas and exactly 2 decimals."""
    return f"${amount:,.2f}"

@bot.tree.command(name="stats", description="Get stats")
@app_commands.describe(user="The user to get stats for")
@app_commands.default_permissions(send_messages=True)
@_guild_only_ctx
async def slash_stats(interaction: discord.Interaction, user: discord.User = None):
    target = user or interaction.user
    doc = db.collection('user_stats').document(str(target.id)).get()
    if doc.exists:
        data = doc.to_dict()
        deals = data.get('deals_completed', 0)
        usd = data.get('total_usd_value', 0.0)
    else:
        deals = 0
        usd = 0.0
    e = discord.Embed(title=str(target), color=0x6BE46E)
    e.set_thumbnail(url=target.display_avatar.with_size(128).url)
    e.add_field(name="Deals completed:", value=str(deals), inline=False)
    e.add_field(name="Total USD Value:", value=format_usd(usd), inline=False)
    await interaction.response.send_message(embed=e)

@bot.tree.command(name="leaderboard", description="View rankings")
@app_commands.default_permissions(send_messages=True)
@_guild_only_ctx
async def slash_leaderboard(interaction: discord.Interaction):
    docs = db.collection('user_stats').order_by('total_usd_value', direction=firestore.Query.DESCENDING).limit(15).stream()
    entries = []
    for doc in docs:
        data = doc.to_dict()
        uid = doc.id
        try:
            user = await bot.fetch_user(int(uid))
            mention = user.mention
        except:
            mention = f"<@{uid}>"
        entries.append((mention, data.get('total_usd_value', 0.0)))
    e = discord.Embed(title="Leaderboard", color=0x6BE46E)
    e.set_thumbnail(url=interaction.user.display_avatar.with_size(128).url)
    user_doc = db.collection('user_stats').document(str(interaction.user.id)).get()
    if user_doc.exists:
        user_usd = user_doc.to_dict().get('total_usd_value', 0.0)
    else:
        user_usd = 0.0
    higher_count = sum(1 for _, usd in entries if usd > user_usd)
    rank = higher_count + 1
    e.description = f"{interaction.user.mention}'s ranking: #`{rank}` (`{format_usd(user_usd)}`)"
    for i in range(1, 16):
        if i <= len(entries):
            mention, usd = entries[i-1]
            e.add_field(name=f"#{i} - {format_usd(usd)}", value=mention, inline=True)
        else:
            e.add_field(name=f"#{i} - $?", value="Loading error", inline=True)
    await interaction.response.send_message(embed=e)
    
@bot.tree.command(name="setprivacy", description="Sets deal privacy")
@app_commands.describe(privacy="Public or private")
@app_commands.default_permissions(send_messages=True)
@_guild_only_ctx
@app_commands.choices(privacy=[
    app_commands.Choice(name="public",  value="public"),
    app_commands.Choice(name="private", value="private"),
])
async def slash_setprivacy(interaction: discord.Interaction, privacy: app_commands.Choice[str]):
    content = "Your transactions are now private" if privacy.value == "private" else "Your transactions are now public"
    e = discord.Embed(title="Changed privacy", description=content, color=0x6BE46E)
    await interaction.response.send_message(embed=e, ephemeral=True)

@bot.tree.command(name="passes", description="Get number of passes")
@app_commands.default_permissions(send_messages=True)
@_guild_only_ctx
async def slash_passes(interaction: discord.Interaction):
    e = discord.Embed(description="Loading error passes", color=0x6BE46E)
    await interaction.response.send_message(embed=e, ephemeral=True)

# ─── EVENTS ──────────────────────────────────────────────────

@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        await bot.process_commands(message); return

    ch_id = message.channel.id
    state = ticket_state.get(ch_id)

    if state:
        phase = state.get('phase')
        if phase == 'counterpart' and message.author.id == state['creator_id']:
            guild  = message.guild
            target = None
            if message.mentions: target = message.mentions[0]
            else:
                try:
                    uid = int(message.content.strip())
                    target = guild.get_member(uid) or await guild.fetch_member(uid)
                except: pass
            if not target:
                e = discord.Embed(title="Invalid User", description="User not found.", color=0xf75252)
                await message.channel.send(embed=e); await bot.process_commands(message); return
            if target.id == message.author.id:
                e = discord.Embed(title="Invalid User", description="You can't trade with yourself.", color=0xf75252)
                await message.channel.send(embed=e); await bot.process_commands(message); return
            if target.bot:
                e = discord.Embed(title="Invalid User", description="You can't trade with a bot.", color=0xf75252)
                await message.channel.send(embed=e); await bot.process_commands(message); return
            await message.channel.set_permissions(target, view_channel=True, read_message_history=True, send_messages=True, attach_files=True, embed_links=True)
            state['counterpart_id'] = target.id
            state['phase'] = 'role'
            try:
                dm = await message.channel.fetch_message(state['dealing_msg_id'])
                await dm.delete()
            except: pass
            await message.channel.send(
                content=target.mention,
                embed=discord.Embed(description=f"Successfully added {target.mention}", color=0x6BE46E)
            )
            role_msg = await message.channel.send(
                embed=build_role_assignment_embed(state['crypto'], None, None),
                view=build_role_assignment_view(None, None)
            )
            state['role_msg_id'] = role_msg.id
            await bot.process_commands(message); return

        if phase == 'amount' and message.author.id == state.get('sender_id'):
            state['amount_user_msg_id'] = message.id
            amount = parse_amount(message.content)
            if amount is None:
                await message.channel.send(embed=discord.Embed(title="Invalid amount", description="Please enter a valid amount.", color=0xf75252))
                await bot.process_commands(message); return
            minimum = get_minimum_amount(state['crypto'])
            if amount < minimum:
                await message.channel.send(embed=discord.Embed(description=f"${minimum:.2f} USD Minimum", color=0xf75252))
                await bot.process_commands(message); return
            state['amount'] = amount
            await send_amount_confirm(message.channel, state)
            await bot.process_commands(message); return

        if phase == 'release_address' and message.author.id == state.get('receiver_id'):
            address = message.content.strip()
            state['release_address'] = address  # store but don't use
            blockchain = get_blockchain(state['crypto'])
            e = discord.Embed(
                title=f"Is this your {blockchain} address?",
                description="Please verify that the address you provided is correct. Once the funds are released, they cannot be retrieved.",
                color=0xf8e552
            )
            e.add_field(name="Address", value=f"`{address}`", inline=False)
            view = ReleaseAddressConfirmView(message.channel.id)
            msg = await message.channel.send(embed=e, view=view)
            state['release_address_msg_id'] = msg.id
            state['phase'] = 'release_address_confirm'
            await bot.process_commands(message)
            return

    await bot.process_commands(message)

@bot.event
async def on_ready():
    for v in [PanelView, CryptoSelectView, TicketCloseView, RoleAssignmentView,
              ConfirmRolesView, AmountConfirmView, FeePaymentView, FeeConfirmView,
              CopyDetailsView, RescanCancelView, IncorrectAmountView,
              IncorrectAmountConfirmView, ReleaseView]:
        try: bot.add_view(v())
        except: pass

    print(f"✅ {bot.user} ({bot.user.id})")
    try:
        synced = await bot.tree.sync()
        print(f"✅ Synced {len(synced)} slash commands.")
    except Exception as e:
        print(f"⚠ Sync failed: {e}")

    data = get_server_data()
    mode = data.get('mode', 1)
    agid = get_active_guild_id()

    if agid:
        guild = bot.get_guild(int(agid))
        if guild:
            if mode == 2:
                num = get_current_number()
                await bot.change_presence(status=discord.Status.online)
                try: await guild.me.edit(nick=f"Halal")
                except: pass
            else:
                try: await guild.me.edit(nick="⃟")
                except: pass
        else:
            clear_active_guild()
    else:
        print("✅ No active guild.")

    if agid and mode == 2:
        asyncio.create_task(set_owner_status(OWNER_ONLINE_STATUS))
    else:
        asyncio.create_task(set_owner_status('invisible'))

@bot.event
async def on_guild_remove(guild: discord.Guild):
    if get_active_guild_id() == str(guild.id):
        clear_active_guild()

@bot.event
async def on_command_error(ctx, error): pass

# ─── MAIN ────────────────────────────────────────────────────

if __name__ == "__main__":
    try: keep_alive()
    except NameError: pass
    token = os.getenv('TOKEN')
    if token: bot.run(token)
    else: print("❌ No TOKEN found in environment variables.")
