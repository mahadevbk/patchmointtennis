import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
import re
import uuid
import time
import os
import base64
import json
import requests
import psycopg2
from sqlalchemy import create_engine, text
from psycopg2.extras import RealDictCursor, execute_values
from datetime import datetime, timedelta
from collections import defaultdict
import io
import zipfile
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module='pandas')
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# --- Configuration & Setup ---
SPORT_TYPE = st.secrets.get("SPORT_TYPE", "Tennis")
LOGO_URL = "https://raw.githubusercontent.com/mahadevbk/patchmointtennis/main/logo.png"

st.set_page_config(page_title=f"Patch Moint {SPORT_TYPE} League", layout="centered")
os.environ["STREAMLIT_SERVER_FILE_WATCHER_TYPE"] = "none"

# --- CHECK SECRETS ---
if "NEON_DATABASE_URL" not in st.secrets:
    st.error("Missing secrets! Please configure NEON_DATABASE_URL, GITHUB_TOKEN, and GITHUB_REPO in .streamlit/secrets.toml")
    st.stop()

# --- REMOTE CONNECTION SETUP ---
def get_connection():
    return psycopg2.connect(st.secrets["NEON_DATABASE_URL"])

# --- DATABASE INITIALIZATION ---
def init_db():
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            # 1. Create tables if they don't exist
            # Removed UNIQUE from name to allow same names across different sports
            queries = [
                "CREATE TABLE IF NOT EXISTS chapters (id TEXT PRIMARY KEY, name TEXT, admin_password TEXT, created_at TEXT, config TEXT, sport TEXT, title_image_url TEXT, last_active_date TEXT, admin_name TEXT, admin_email TEXT)",
                "CREATE TABLE IF NOT EXISTS players (name TEXT, profile_image_url TEXT, birthday TEXT, chapter_id TEXT, password TEXT, gender TEXT, is_admin BOOLEAN DEFAULT FALSE, initial_utr NUMERIC DEFAULT NULL)",
                "CREATE TABLE IF NOT EXISTS matches (match_id TEXT PRIMARY KEY, date TEXT, match_type TEXT, team1_player1 TEXT, team1_player2 TEXT, team2_player1 TEXT, team2_player2 TEXT, set1 TEXT, set2 TEXT, set3 TEXT, winner TEXT, match_image_url TEXT, chapter_id TEXT)",
                "CREATE TABLE IF NOT EXISTS bookings (booking_id TEXT PRIMARY KEY, date TEXT, time TEXT, match_type TEXT, court_name TEXT, player1 TEXT, player2 TEXT, player3 TEXT, player4 TEXT, standby_player TEXT, screenshot_url TEXT, chapter_id TEXT)",
                "CREATE TABLE IF NOT EXISTS courts (chapter_id TEXT, name TEXT, url TEXT)"
            ]
            for q in queries:
                cur.execute(q)
            conn.commit()

            # 2. Run Migrations
            migrations = [
                "ALTER TABLE players ADD COLUMN IF NOT EXISTS initial_utr NUMERIC DEFAULT NULL",
                "ALTER TABLE players ADD COLUMN IF NOT EXISTS is_admin BOOLEAN DEFAULT FALSE",
                "ALTER TABLE chapters ADD COLUMN IF NOT EXISTS sport TEXT",
                "ALTER TABLE chapters ADD COLUMN IF NOT EXISTS last_active_date TEXT DEFAULT ''",
                "ALTER TABLE chapters ADD COLUMN IF NOT EXISTS title_image_url TEXT DEFAULT ''",
                "ALTER TABLE chapters ADD COLUMN IF NOT EXISTS admin_name TEXT DEFAULT ''",
                "ALTER TABLE chapters ADD COLUMN IF NOT EXISTS admin_email TEXT DEFAULT ''",
                "ALTER TABLE chapters DROP CONSTRAINT IF EXISTS chapters_name_key"
            ]
            
            for migration in migrations:
                try:
                    cur.execute(migration)
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    # print(f"Migration skipped or failed: {e}") 

        conn.close()
    except Exception as e:
        st.error(f"Database Initialization Error: {e}")

init_db()

def send_email(to_email, admin_name, chapter_name, admin_password):
    # Ensure secrets are available
    smtp_server = st.secrets.get("SMTP_SERVER", "smtp.gmail.com")
    smtp_port = st.secrets.get("SMTP_PORT", 587)
    smtp_user = st.secrets.get("SMTP_USER")
    smtp_pass = st.secrets.get("SMTP_PASS")

    if not all([smtp_user, smtp_pass]):
        st.warning("SMTP credentials not configured. Email not sent.")
        return False

    msg = MIMEMultipart()
    msg['From'] = smtp_user
    msg['To'] = to_email
    msg['Subject'] = f"Welcome to Patch Moint - {chapter_name}"

    body = f"""Hi {admin_name},

Welcome to the Patch Moint League system! Your chapter '{chapter_name}' has been created successfully.

Your Admin Password is: {admin_password}

You can use this password to access the Chapter Settings and manage your players and matches.

Best regards,
The Patch Moint Team"""
    msg.attach(MIMEText(body, 'plain'))

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.send_message(msg)
        server.quit()
        return True
    except Exception as e:
        st.error(f"Failed to send email: {e}")
        return False

st.markdown("""
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Turret+Road:wght@200;300;400;500;700;800&display=swap" rel="stylesheet">
<style>
    .glow-square {
            width: 100px; 
            height: 100px;
            border: 3px solid #ccff00;
            border-radius: 12px;
            overflow: hidden;
            display: flex;
            justify-content: center;
            align-items: center;
            background-color: #262626;
            box-shadow: 0 0 15px rgba(204, 255, 0, 0.4);
            margin: 0 auto;
        }
        .glow-square img {
            width: 100%;
            height: 100%;
            object-fit: contain; /* Ensures the whole image fits without cropping */
            padding: 5px; /* Creates a small gap between image and border */
        }
html, body, [class*="st-"], .stApp, h1, h2, h3, h4, h5, h6 {
    font-family: 'Turret Road', sans-serif !important;
}
.mobile-card {
    background: linear-gradient(135deg, #071a3d 0%, #0c0014 100%);
    border: 1px solid rgba(255, 245, 0, 0.2);
    border-radius: 15px;
    padding: 15px;
    margin-bottom: 15px;
    box-shadow: 0 4px 15px rgba(0,0,0,0.5);
}
.rank-badge {
    background: #fff500;
    color: #041136;
    font-weight: bold;
    border-radius: 5px;
    padding: 2px 8px;
    font-size: 14px;
}
.trend-dot {
    height: 10px; width: 10px; border-radius: 50%; display: inline-block; margin-right: 3px;
}
.dot-w { background-color: #00ff88; box-shadow: 0 0 5px #00ff88; }
.dot-l { background-color: #ff4b4b; }
.stApp {
  background-image: url("https://raw.githubusercontent.com/mahadevbk/patchmointtennis/main/assets/background/background.jpg");
  background-size: cover;
  background-position: center;
  background-attachment: fixed;
}
@media print {
  html, body { -webkit-print-color-adjust: exact !important; print-color-adjust: exact !important; }
  body { background-color: #041136 !important; height: 100vh; margin: 0; padding: 0; }
  header, .stToolbar { display: none; }
}
[data-testid="stHeader"] {
    background: black !important;
    background-image: none !important;
    border-bottom: 1px solid #333;
}
.profile-image:hover { transform: scale(1.1); }
.court-card {
    background: linear-gradient(to bottom, #031827, #07314f); border: 1px solid #fff500;
    border-radius: 10px; padding: 15px; margin: 10px 0; box-shadow: 0 4px 8px rgba(0, 0, 0, 0.2);
    transition: transform 0.2s, box-shadow 0.2s; text-align: center;
    min-height: 120px; display: flex; flex-direction: column; justify-content: center; align-items: center;
}
.court-card:hover { transform: scale(1.05); box-shadow: 0 6px 12px rgba(255, 245, 0, 0.3); }
.court-card h4 { color: #fff500; margin-bottom: 10px; }
.court-card a {
    background-color: #fff500; color: #031827; padding: 8px 16px; border-radius: 5px;
    text-decoration: none; font-weight: bold; display: inline-block; margin-top: 10px;
    transition: background-color 0.2s;
}
.court-card a:hover { background-color: #ffd700; }
h1 { font-size: 24px !important; }
h2 { font-size: 22px !important; }
h3 { font-size: 16px !important; }
.rankings-table-container {
    width: 100%; margin-top: 0px !important; padding: 5px;
}
.ranking-row {
    display: block; padding: 15px; margin-bottom: 15px; border: 1px solid rgba(255, 255, 255, 0.2);
    border-radius: 12px; box-shadow: 0 4px 6px rgba(0,0,0,0.3);
    background: linear-gradient(135deg, rgba(255, 255, 255, 0.30) 0%, rgba(255, 255, 255, 0.26) 100%);
    overflow: visible; transition: transform 0.2s;
}
.ranking-row:hover { transform: translateY(-2px); border-color: rgba(255, 245, 0, 0.5); }
.rank-profile-player-group { display: flex; align-items: center; margin-bottom: 15px; border-bottom: 1px solid rgba(255,255,255,0.1); padding-bottom: 10px; }
.rank-col { font-size: 2em; font-weight: bold; color: #fff500; margin-right: 15px; min-width: 40px; text-align: center; }
.player-col { font-size: 1.4em; font-weight: bold; color: #ffffff; flex-grow: 1; }
.badge { background: #fff500; color: black; padding: 2px 8px; 
    border-radius: 10px; font-size: 0.75em; font-weight: bold; margin-left: 5px;
}
.stat-box {
    background: rgba(255,255,255,0.30); padding: 15px; border-radius: 10px; 
    border-left: 4px solid #fff500; margin-bottom: 10px;
}
.stat-label { font-size: 0.7em; color: #aaa; text-transform: uppercase; }
.metric-value { font-size: 1.1em; font-weight: bold; }
.stat-highlight { color: #fff500; }
[data-testid="stMetric"] > div:nth-of-type(1) { color: #FF7518 !important; }
.block-container { display: flex; flex-wrap: wrap; justify-content: center; }
[data-testid="stHorizontalBlock"] { flex: 1 1 100% !important; margin: 10px 0; }
.chapter-card {
    background-image: url("https://raw.githubusercontent.com/mahadevbk/patchmointtennis/main/assets/background/cardbg.png") !important;
    background-size: cover;
    background-position: center;
    border: 2px solid #fff500;
    border-radius: 12px;
    text-align: center;
    transition: transform 0.2s, box-shadow 0.2s;
    box-shadow: 0 0 10px #fff500;
    display: flex;
    flex-direction: column;
    height: 100%;
    padding: 0;
    overflow: hidden;
}
.chapter-card:hover {
    transform: translateY(-5px);
    border-color: #fff500;
    box-shadow: 0 0 20px #fff500;
}
.card-content {
    padding: 15px;
    display: flex;
    flex-direction: column;
    flex-grow: 1;
}
.card-image-container {
    height: 150px;
    width: 100%;
    overflow: hidden;
    display: flex;
    align-items: center;
    justify-content: center;
    background-color: rgba(255, 255, 255, 0.30);
}
.card-image-container img {
    width: 100%;
    height: 100%;
    object-fit: contain;
}
.chapter-card h3 {
    color: #fff500;
    margin-top: 10px;
    margin-bottom: 10px;
    font-size: 24px !important; /* Added font-size (16px * 1.5 = 24px) */
    font-weight: 700;           /* Optional: makes it bold for better visibility */
}
.chapter-card p {
    color: #fff500 !important;
    font-size: 16px;
    font-weight: 500;
    margin-bottom: 15px;
    opacity: 1; /* Ensures it is fully bright */
}
.enter-button {
    background-color: #fff500;
    color: #031827;
    padding: 8px 16px;
    border-radius: 5px;
    text-decoration: none;
    font-weight: bold;
    display: block;
    margin-top: auto; /* Pushes button to the bottom */
    transition: background-color 0.2s;
    width: 100%;
    box-sizing: border-box;
}
.enter-button:hover {
    background-color: #ffd700;
}
.stat-container {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
        margin-top: 10px;
    }
    .stat-chip {
        padding: 4px 12px;
        border-radius: 15px;
        font-weight: bold;
        font-size: 0.85rem;
        color: white;
        box-shadow: 0 2px 4px rgba(0,0,0,0.2);
    }
    .win-rate { background: linear-gradient(135deg, #28a745, #1e7e34); }
    .matches { background: linear-gradient(135deg, #007bff, #0056b3); }
    .points { background: linear-gradient(135deg, #fd7e14, #d96101); }
</style>
""", unsafe_allow_html=True)

# --- Constants ---
PLAYERS_TABLE = "players"
MATCHES_TABLE = "matches"
BOOKINGS_TABLE = "bookings"
AVAILABILITY_TABLE = "availability"
# Generic avatar placeholder (SVG base64) similar to WhatsApp default
#DEFAULT_AVATAR = "data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAyNCAyNCI+PGNpcmNsZSBjeD0iMTIiIGN5PSIxMiIgcj0iMTIiIGZpbGw9IiNFMEUwRTAiLz48cGF0aCBkPSZNMTIgMTJjMi4yMSAwIDQtMS43OSA0LTRzLTEuNzktNC00LTQtNCAxLjc5LTQgNCAxLjc5IDQgNCA0em0wIDJjLTIuNjcgMC04IDEuMzQtOCA0djJoMTZ2LTJjMC0yLjY2LTUuMzMtNC04LTR6IiBmaWxsPSIjRkZGRkZGIi8+PC9zdmc+"
DEFAULT_AVATAR = "https://raw.githubusercontent.com/mahadevbk/patchmointtennis/main/assets/players/default.png"

# --- Session State Init ---
if 'current_chapter' not in st.session_state:
    st.session_state.current_chapter = None
if 'is_admin' not in st.session_state:
    st.session_state.is_admin = False
if 'can_write' not in st.session_state:
    st.session_state.can_write = False
if 'is_master_admin' not in st.session_state:
    st.session_state.is_master_admin = False
if 'chapter_config' not in st.session_state:
    st.session_state.chapter_config = {}
if 'temp_selected_chapter' not in st.session_state:
    st.session_state.temp_selected_chapter = None
if 'logged_in_player' not in st.session_state:
    st.session_state.logged_in_player = None

# Stats DFs
if 'players_df' not in st.session_state:
    st.session_state.players_df = pd.DataFrame(columns=["name", "profile_image_url", "birthday", "chapter_id", "password", "gender"])
if 'matches_df' not in st.session_state:
    st.session_state.matches_df = pd.DataFrame(columns=["match_id", "date", "match_type", "team1_player1", "team1_player2", "team2_player1", "team2_player2", "set1", "set2", "set3", "winner", "match_image_url", "chapter_id"])
if 'bookings_df' not in st.session_state:
    st.session_state.bookings_df = pd.DataFrame(columns=["booking_id", "date", "time", "match_type", "court_name", "player1", "player2", "player3", "player4", "screenshot_url", "chapter_id"])
if 'availability_df' not in st.session_state:
    st.session_state.availability_df = pd.DataFrame(columns=["id", "player_name", "date", "comment", "chapter_id"])
if 'form_key_suffix' not in st.session_state:
    st.session_state.form_key_suffix = 0
if 'match_post_key' not in st.session_state:
    st.session_state.match_post_key = 0

# --- Helper Functions ---

@st.cache_resource
def get_sqlalchemy_engine():
    db_url = st.secrets["NEON_DATABASE_URL"]
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
    return create_engine(db_url)

def fetch_data(table_name, chapter_id=None):
    try:
        engine = get_sqlalchemy_engine()
        query = f"SELECT * FROM {table_name}"
        params = {}
        if chapter_id:
            query += " WHERE chapter_id = :chapter_id"
            params = {"chapter_id": chapter_id}
        
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params=params)

        # Ensure columns exist if empty
        if df.empty:
            if table_name == "players": return pd.DataFrame(columns=["name", "profile_image_url", "birthday", "chapter_id", "password", "gender"])
            if table_name == "matches": return pd.DataFrame(columns=["match_id", "date", "match_type", "team1_player1", "team1_player2", "team2_player1", "team2_player2", "set1", "set2", "set3", "winner", "match_image_url", "chapter_id"])
            if table_name == "bookings": return pd.DataFrame(columns=["booking_id", "date", "time", "match_type", "court_name", "player1", "player2", "player3", "player4", "screenshot_url", "chapter_id"])
        return df
    except Exception as e:
        return pd.DataFrame()

def load_players():
    cid = st.session_state.current_chapter['id'] if st.session_state.current_chapter else None
    st.session_state.players_df = fetch_data("players", cid)

def save_players(df):
    cid = st.session_state.current_chapter['id']
    if cid and not df.empty:
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                # Sync: Delete all for chapter and re-insert
                cur.execute("DELETE FROM players WHERE chapter_id = %s", (cid,))
                
                # Convert NaN to None for SQL compatibility
                df_clean = df.where(pd.notnull(df), None)
                records = [tuple(x) for x in df_clean.to_numpy()]
                cols = ",".join(list(df.columns))
                
                if records:
                    query = f"INSERT INTO players ({cols}) VALUES %s"
                    execute_values(cur, query, records)
            conn.commit()
        except Exception as e:
            st.error(f"Save error: {e}")
        finally:
            conn.close()

def update_player_password(player_name, new_pass, chapter_id=None):
    try:
        cid = chapter_id if chapter_id else st.session_state.current_chapter['id']
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("UPDATE players SET password = %s WHERE name = %s AND chapter_id = %s", (new_pass, player_name, cid))
        conn.commit()
        conn.close()
        
        if 'players_df' in st.session_state and not st.session_state.players_df.empty:
            idx = st.session_state.players_df[st.session_state.players_df['name'] == player_name].index
            if not idx.empty:
                st.session_state.players_df.loc[idx, 'password'] = new_pass
        return True
    except Exception as e:
        return False

def update_chapter_admin_password(chapter_id, new_pass):
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("UPDATE chapters SET admin_password = %s WHERE id = %s", (new_pass, chapter_id))
        conn.commit()
        conn.close()
        return True
    except: return False

def load_matches():
    cid = st.session_state.current_chapter['id'] if st.session_state.current_chapter else None
    st.session_state.matches_df = fetch_data("matches", cid)

def save_matches(df):
    if df.empty:
        return

    # Filter for only the current chapter to be safe
    # Correctly access the 'id' from the current_chapter dictionary
    chapter_id = st.session_state.current_chapter['id']
    df = df[df['chapter_id'] == chapter_id]
    
    if df.empty:
        return

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # OPTION 1: Safe Insert (Append Only)
            # This SQL statement inserts rows but does nothing if a row with the same ID already exists.
            # This prevents duplicates without needing to delete anything.
            
            # Prepare the data for insertion
            data_tuples = []
            for _, row in df.iterrows():
                # Ensure we handle NaN/None correctly for SQL
                t1p2 = row.get('team1_player2')
                t2p2 = row.get('team2_player2')
                t1p2 = t1p2 if pd.notna(t1p2) and t1p2 else None
                t2p2 = t2p2 if pd.notna(t2p2) and t2p2 else None

                # Map to correct table columns: set1, set2, set3, match_image_url
                data_tuples.append((
                    str(row['match_id']),
                    row['date'],
                    row['match_type'],
                    row['team1_player1'],
                    t1p2,
                    row['team2_player1'],
                    t2p2,
                    row.get('set1'),
                    row.get('set2'),
                    row.get('set3'),
                    row['winner'],
                    row.get('match_image_url'),
                    chapter_id
                ))

            # Correct SQL Query matching table schema
            query = """
                INSERT INTO matches (
                    match_id, date, match_type, 
                    team1_player1, team1_player2, 
                    team2_player1, team2_player2, 
                    set1, set2, set3, 
                    winner, match_image_url, chapter_id
                ) VALUES %s
                ON CONFLICT (match_id) DO NOTHING;
            """
            
            execute_values(cur, query, data_tuples)
            conn.commit()
            
    except Exception as e:
        st.error(f"Error saving matches: {e}")
    finally:
        conn.close()

def delete_match_from_db(match_id):
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM matches WHERE match_id = %s", (match_id,))
        conn.commit()
        conn.close()
        if "matches_df" in st.session_state:
            st.session_state.matches_df = st.session_state.matches_df[st.session_state.matches_df["match_id"] != match_id]
        st.success(f"Match {match_id} deleted locally.")
    except Exception as e: st.error(f"Error: {e}")

def delete_player_from_db(player_name):
    try:
        cid = st.session_state.current_chapter['id']
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM players WHERE name = %s AND chapter_id = %s", (player_name, cid))
        conn.commit()
        conn.close()
    except Exception as e: st.error(f"Error: {e}")

def delete_chapter_fully(chapter_id):
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            for t in ["players", "matches", "bookings", "courts"]:
                cur.execute(f"DELETE FROM {t} WHERE chapter_id = %s", (chapter_id,))
            cur.execute("DELETE FROM chapters WHERE id = %s", (chapter_id,))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Error: {e}")
        return False

def get_default_config():
    return {
        "ranking_systems": {"Elo (Hybrid)": True, "Points": True, "UTR": False},
        "match_type_settings": {
            "Singles": {"enabled": True, "win_points": 2, "loss_points": 1, "min_sets": "Best of 3"},
            "Doubles": {"enabled": True, "win_points": 2, "loss_points": 1, "min_sets": "Best of 3"},
            "Mixed Doubles": {"enabled": False, "win_points": 3, "loss_points": 0, "min_sets": "Best of 3"}
        },
        "match_image_required": True
    }

def load_chapter_config(chapter_id):
    try:
        conn = get_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT config FROM chapters WHERE id = %s", (chapter_id,))
            data = cur.fetchone()
        conn.close()
        
        if data and data['config']:
            conf = json.loads(data['config'])
            
            # Migration for ranking_systems
            if "ranking_systems" not in conf or isinstance(conf["ranking_systems"], list):
                old_ranking_systems = conf.get("ranking_systems", ["Elo (Hybrid)"])
                if isinstance(old_ranking_systems, list):
                    conf["ranking_systems"] = {
                        "Elo (Hybrid)": "Elo (Hybrid)" in old_ranking_systems,
                        "Points": "Points" in old_ranking_systems,
                        "UTR": "UTR" in old_ranking_systems or "DUPR" in old_ranking_systems,
                    }
                # if it's already a dict, do nothing
            elif "DUPR" in conf["ranking_systems"]:
                conf["ranking_systems"]["UTR"] = conf["ranking_systems"].pop("DUPR")
            
            # Migration for match_type_settings
            if "match_type_settings" not in conf:
                default_settings = get_default_config()["match_type_settings"]
                old_match_types = conf.get("match_types", ["Doubles", "Singles"])
                old_win = conf.get("points_win", 3)
                old_loss = conf.get("points_loss", 1)
                old_sets = conf.get("sets_modes", {"Singles": "Best of 3", "Doubles": "Best of 3", "Mixed Doubles": "Best of 3"})

                conf["match_type_settings"] = {}
                for mt in ["Singles", "Doubles", "Mixed Doubles"]:
                    conf["match_type_settings"][mt] = {
                        "enabled": mt in old_match_types,
                        "win_points": old_win,
                        "loss_points": old_loss,
                        "min_sets": old_sets.get(mt, "Best of 3")
                    }

            # Ensure all keys from default are present
            default_conf = get_default_config()
            for key in default_conf:
                if key not in conf:
                    conf[key] = default_conf[key]
            
            return conf
    except Exception as e:
        # st.error(f"Config load error: {e}") # Optional: for debugging
        pass
    return get_default_config()

def save_chapter_config(chapter_id, config_dict):
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("UPDATE chapters SET config = %s WHERE id = %s", (json.dumps(config_dict), chapter_id))
    conn.commit()
    conn.close()
    st.session_state.chapter_config = config_dict

def save_remote_image(uploaded_file, file_id, image_type="match"):
    if uploaded_file is None: return ""
    
    # GitHub Config
    token = st.secrets.get("GITHUB_TOKEN")
    repo = st.secrets.get("GITHUB_REPO")
    branch = st.secrets.get("GITHUB_BRANCH", "main")
    
    if not token or not repo:
        st.error("GitHub secrets missing. Please check your secrets.toml file.")
        return ""

    # Clean file extension
    file_ext = uploaded_file.name.split('.')[-1] if '.' in uploaded_file.name else 'jpg'
    file_path = f"assets/{image_type}s/{file_id}.{file_ext}" # e.g. assets/matches/123.jpg
    
    # API URL
    url = f"https://api.github.com/repos/{repo}/contents/{file_path}"
    headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}
    
    # Check if exists to get SHA for update
    r = requests.get(url, headers=headers)
    sha = r.json().get("sha") if r.status_code == 200 else None
    
    # Encode content
    try:
        content_b64 = base64.b64encode(uploaded_file.getvalue()).decode("utf-8")
        data = {
            "message": f"Upload {file_path}",
            "content": content_b64,
            "branch": branch
        }
        if sha: data["sha"] = sha
        
        # Upload
        resp = requests.put(url, headers=headers, json=data)
        
        if resp.status_code in [200, 201]:
            st.toast(f"Image uploaded successfully!", icon="✅")
            # Return raw URL (MUST BE PUBLIC REPO)
            return f"https://raw.githubusercontent.com/{repo}/{branch}/{file_path}"
        else:
            st.error(f"GitHub Upload Failed ({resp.status_code}): {resp.json().get('message')}")
            return ""
    except Exception as e:
        st.error(f"Upload Logic Error: {e}")
        return ""

def get_img_src(path_or_url):
    if path_or_url:
        return path_or_url
    return DEFAULT_AVATAR

def render_footer():
    # Icons for Tennis, Pickleball, Padel
    active_color = "#ccff00"
    inactive_color = "#888888"

    # Define icons (Simple SVG paths)
    icons = {
        "Tennis": f'<svg width="30" height="30" viewBox="0 0 24 24" fill="{active_color if SPORT_TYPE == "Tennis" else inactive_color}"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm0 18c-4.41 0-8-3.59-8-8s3.59-8 8-8 8 3.59 8 8-3.59 8-8 8zm0-14c-3.31 0-6 2.69-6 6s2.69 6 6 6 6-2.69 6-6-2.69-6-6-6z"/></svg>',
        "Pickleball": f'<svg width="30" height="30" viewBox="0 0 24 24" fill="{active_color if SPORT_TYPE == "Pickleball" else inactive_color}"><circle cx="12" cy="12" r="10" stroke="currentColor" stroke-width="2" fill="none"/><circle cx="12" cy="12" r="1"/><circle cx="12" cy="8" r="1"/><circle cx="12" cy="16" r="1"/><circle cx="8" cy="12" r="1"/><circle cx="16" cy="12" r="1"/><circle cx="9" cy="9" r="1"/><circle cx="15" cy="15" r="1"/><circle cx="9" cy="15" r="1"/><circle cx="15" cy="9" r="1"/></svg>',
        "Padel": f'<svg width="30" height="30" viewBox="0 0 24 24" fill="{active_color if SPORT_TYPE == "Padel" else inactive_color}"><path d="M12 2L4 10l2 2 6-6 6 6 2-2-8-8zM6 14v6h12v-6H6z"/></svg>'
    }

    st.markdown(f"""
    <div style="text-align: center; margin-top: 30px; border-top: 1px solid rgba(255,255,255,0.1); padding-top: 20px;">
        <div style="display: flex; justify-content: center; gap: 20px; margin-bottom: 15px;">
            <a href="https://patchmoint-tennis.streamlit.app/" target="_blank" title="Tennis">{icons['Tennis']}</a>
            <a href="https://patchmoint-pickleball.streamlit.app/" target="_blank" title="Pickleball">{icons['Pickleball']}</a>
            <a href="https://patchmoint-padel.streamlit.app/" target="_blank" title="Padel">{icons['Padel']}</a>
        </div>
        <div style="color: #888; font-size: 0.8em;">Patch Moint League system is free and Open source. Hosted on GitHub and Powered by Streamlit.</div>
    </div>
    """, unsafe_allow_html=True)

def create_radar_chart(row):
    try:
        win_rate = row.get('Win %', 0)
        clutch = row.get('Clutch Factor', 0)
        cons_idx = row.get('Consistency Index', 0)
        consistency = max(0, 100 - (cons_idx * 15))
        gda = row.get('Game Diff Avg', 0)
        dominance = 50 + (gda * 16)
        dominance = max(0, min(100, dominance))
        matches = row.get('Matches', 0)
        experience = min(100, matches * 5)
        categories = ['Win Rate', 'Consistency', 'Dominance', 'Clutch', 'Experience']
        values = [win_rate, consistency, dominance, clutch, experience]
        fig = go.Figure()
        fig.add_trace(go.Scatterpolar(r=values, theta=categories, fill='toself', name=row['Player'],
            line=dict(color='#CCFF00'), fillcolor='rgba(204, 255, 0, 0.3)'))
        fig.update_layout(polar=dict(radialaxis=dict(visible=True, range=[0, 100], showticklabels=False, linecolor='rgba(255,255,255,0.3)'),
                angularaxis=dict(tickfont=dict(size=10, color='#aaa')), bgcolor='rgba(0,0,0,0)'),
            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', margin=dict(l=30, r=30, t=20, b=20), height=220, showlegend=False)
        return fig
    except: return None

@st.dialog("Chapter Login")
def login_modal(chapter):
    st.subheader(f"Accessing: {chapter['name']}")
    
    # Use a unique key for the input inside the modal
    pwd = st.text_input("Enter Password", type="password", key=f"login_pwd_{chapter['id']}")
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Login", use_container_width=True):
            # Check for Master Admin
            if pwd == st.secrets.get("MASTER_PASSWORD"):
                st.session_state.current_chapter = chapter
                st.session_state.is_master_admin = True
                st.session_state.is_admin = True
                st.session_state.can_write = True
                st.rerun()
            # Check for Chapter Admin
            elif pwd == chapter.get('admin_password'):
                st.session_state.current_chapter = chapter
                st.session_state.is_admin = True
                st.session_state.can_write = True
                st.rerun()
            # Check for Player password
            else:
                chapter_players_df = fetch_data("players", chapter_id=chapter['id'])
                player_match = chapter_players_df[chapter_players_df['password'] == pwd]
                if not player_match.empty:
                    player_row = player_match.iloc[0]
                    player_name = player_row['name']
                    is_player_admin = player_row.get('is_admin', False)

                    st.session_state.current_chapter = chapter
                    st.session_state.is_admin = is_player_admin
                    st.session_state.can_write = True 
                    st.session_state.logged_in_player = player_name
                    st.session_state.chapter_config = load_chapter_config(chapter['id'])
                    
                    if is_player_admin:
                        st.success(f"Welcome Admin {player_name}!")
                    else:
                        st.success(f"Welcome {player_name}!")
                    time.sleep(0.5); st.rerun()
                else:
                    st.error("Invalid Password")
    with col2:
        # Standard user entry (No password needed to view)
        if st.button("Enter as Guest", use_container_width=True):
            st.session_state.current_chapter = chapter
            st.session_state.is_admin = False
            st.session_state.can_write = False
            st.rerun()
            
# --- Business Logic ---
def get_valid_scores():
    if SPORT_TYPE == "Pickleball":
        # Pickleball scores: typically games to 11, 15, or 21. Win by 2.
        scores = []
        # Games to 11
        for i in range(10): scores.extend([f"11-{i}", f"{i}-11"])
        scores.extend(["12-10", "10-12", "13-11", "11-13", "14-12", "12-14"]) # Common tie breaks
        # Games to 15
        for i in range(14): scores.extend([f"15-{i}", f"{i}-15"])
        scores.extend(["16-14", "14-16", "17-15", "15-17"])
        return scores
    else:
        # Tennis / Padel scores
        scores = ["6-0", "6-1", "6-2", "6-3", "6-4", "7-5", "7-6", "0-6", "1-6", "2-6", "3-6", "4-6", "5-7", "6-7"]
        for i in range(10): scores.extend([f"Tie Break 7-{i}", f"Tie Break {i}-7"])
        for i in range(6): scores.extend([f"Tie Break 10-{i}", f"Tie Break {i}-10"])
        return scores

def generate_match_id(matches_df, match_datetime):
    year = match_datetime.year
    month = match_datetime.month
    quarter = f"Q{(month-1)//3 + 1}"
    if not matches_df.empty:
        dates = pd.to_datetime(matches_df['date'], errors='coerce')
        mask = (dates.dt.year == year) & ((dates.dt.month-1)//3 + 1 == (month-1)//3 + 1)
        serial = mask.sum() + 1
    else: serial = 1
    while True:
        new_id = f"MMD{quarter}{year}-{serial:02d}"
        if matches_df.empty or new_id not in matches_df['match_id'].values: return new_id
        serial += 1

def get_player_stats_template():
    return {'wins': 0, 'losses': 0, 'matches': 0, 'games_won': 0, 'gd_sum': 0, 'clutch_wins': 0, 'clutch_matches': 0, 'gd_list': [], 'points': 0, 'singles_wins': 0, 'singles_matches': 0, 'doubles_wins': 0, 'doubles_matches': 0}

@st.cache_data(show_spinner=False)
def calculate_rankings(matches_to_rank):
    stats = defaultdict(get_player_stats_template)
    current_streaks = defaultdict(int)
    last_active_dates = {}
    elo_ratings = {} 
    utr_ratings = {} 
    last_elo_changes = defaultdict(float) 
    K_FACTOR = 32 
    
    UTR_DEFAULT_RATING = 4.0
    UTR_K_FACTOR = 0.05 
    UTR_SCALE = 3.0   
    UTR_MIN = 1.0
    UTR_MAX = 16.5

    players_df = st.session_state.players_df
    config = st.session_state.chapter_config
    match_type_settings = config.get("match_type_settings", get_default_config()["match_type_settings"])

    for _, player_row in players_df.iterrows():
        player_name = player_row['name']
        initial_utr = player_row.get('initial_utr')
        if pd.notna(initial_utr) and initial_utr is not None:
            starting_elo = (initial_utr - 4.0) * 110.0 + 1200.0
            elo_ratings[player_name] = float(starting_elo)
            utr_ratings[player_name] = float(initial_utr)
        else:
            elo_ratings[player_name] = 1200.0
            utr_ratings[player_name] = UTR_DEFAULT_RATING

    elo_ratings = defaultdict(lambda: 1200.0, elo_ratings) 
    utr_ratings = defaultdict(lambda: UTR_DEFAULT_RATING, utr_ratings)

    if not matches_to_rank.empty: 
        matches_to_rank = matches_to_rank.sort_values('date')

    for row in matches_to_rank.itertuples(index=False):
        t1 = [p for p in [row.team1_player1, row.team1_player2] if p and str(p).strip() and str(p).upper() != "VISITOR"]
        t2 = [p for p in [row.team2_player1, row.team2_player2] if p and str(p).strip() and str(p).upper() != "VISITOR"]
        if not t1 or not t2: continue
        
        match_type = row.match_type
        type_config = match_type_settings.get(match_type, {"enabled": False})
        if not type_config.get("enabled", False):
            continue

        pts_win = type_config.get("win_points", 1)
        pts_loss = type_config.get("loss_points", 0)

        current_match_date = row.date
        for p in t1 + t2: 
            last_active_dates[p] = current_match_date

        is_clutch = False
        t1_total_games, t2_total_games = 0, 0
        
        for s in [row.set1, row.set2, row.set3]:
            if not s or str(s).lower() == 'nan': continue
            s_str = str(s)
            t1_g, t2_g = 0, 0
            
            if "Tie Break" in s_str:
                is_clutch = True
                nums = [int(x) for x in re.findall(r'\d+', s_str)]
                if len(nums) >= 2:
                    if nums[0] > nums[1]: t1_g, t2_g = 7, 6
                    else: t1_g, t2_g = 6, 7
            elif '-' in s_str:
                try: 
                    p_score = s_str.split('-')
                    t1_g, t2_g = int(p_score[0]), int(p_score[1])
                except: continue
            
            if SPORT_TYPE == "Pickleball" and not is_clutch:
                if abs(t1_g - t2_g) <= 2 and max(t1_g, t2_g) >= 10:
                    is_clutch = True

            t1_total_games += t1_g
            t2_total_games += t2_g

        total_match_games = t1_total_games + t2_total_games
        if total_match_games == 0: continue

        t1_elo_avg = sum(elo_ratings[p] for p in t1) / len(t1)
        t2_elo_avg = sum(elo_ratings[p] for p in t2) / len(t2)
        t1_utr_avg = sum(utr_ratings[p] for p in t1) / len(t1)
        t2_utr_avg = sum(utr_ratings[p] for p in t2) / len(t2)

        t1_won = row.winner == "Team 1"

        def update_elo(players, own_elo_avg, opp_elo_avg, actual_score):
            expected = 1 / (1 + 10 ** ((opp_elo_avg - own_elo_avg) / 400))
            elo_change = K_FACTOR * (actual_score - expected)
            for p in players:
                elo_ratings[p] += elo_change
                last_elo_changes[p] = round(elo_change, 1)
        
        def update_utr(players, own_utr_avg, opp_utr_avg, actual_gwp):
            utr_diff = own_utr_avg - opp_utr_avg
            expected_gwp = 1 / (1 + np.exp(-utr_diff / UTR_SCALE))
            utr_change = UTR_K_FACTOR * (actual_gwp - expected_gwp)
            for p in players:
                utr_ratings[p] = max(UTR_MIN, min(UTR_MAX, utr_ratings[p] + utr_change))

        def update_common_stats(players, games_won, total_games, is_winner, match_type):
            for p in players:
                stats[p]['matches'] += 1
                stats[p]['games_won'] += games_won
                stats[p]['gd_sum'] += (games_won - (total_games - games_won))
                stats[p]['gd_list'].append(games_won - (total_games - games_won))
                if is_clutch: stats[p]['clutch_matches'] += 1

                if match_type == "Singles":
                    stats[p]['singles_matches'] += 1
                else: # Doubles and Mixed Doubles
                    stats[p]['doubles_matches'] += 1

                if is_winner:
                    stats[p]['wins'] += 1
                    if is_clutch: stats[p]['clutch_wins'] += 1
                    if match_type == "Singles": stats[p]['singles_wins'] += 1
                    else: stats[p]['doubles_wins'] += 1
                    current_streaks[p] = max(0, current_streaks[p]) + 1
                    stats[p]['points'] += pts_win
                else:
                    stats[p]['losses'] += 1
                    current_streaks[p] = min(0, current_streaks[p]) - 1
                    stats[p]['points'] += pts_loss

        if t1_won:
            update_common_stats(t1, t1_total_games, total_match_games, True, match_type)
            update_common_stats(t2, t2_total_games, total_match_games, False, match_type)
            update_elo(t1, t1_elo_avg, t2_elo_avg, 1.0); update_elo(t2, t2_elo_avg, t1_elo_avg, 0.0)
            update_utr(t1, t1_utr_avg, t2_utr_avg, t1_total_games / total_match_games)
            update_utr(t2, t2_utr_avg, t1_utr_avg, t2_total_games / total_match_games)
        else:
            update_common_stats(t1, t1_total_games, total_match_games, False, match_type)
            update_common_stats(t2, t2_total_games, total_match_games, True, match_type)
            update_elo(t1, t1_elo_avg, t2_elo_avg, 0.0); update_elo(t2, t2_elo_avg, t1_elo_avg, 1.0)
            update_utr(t1, t1_utr_avg, t2_utr_avg, t1_total_games / total_match_games)
            update_utr(t2, t2_utr_avg, t1_utr_avg, t2_total_games / total_match_games)

    rank_data = []
    for p, s in stats.items():
        m_played = s['matches']
        if m_played == 0: continue
        
        clutch_pct = (s['clutch_wins'] / s['clutch_matches'] * 100) if s['clutch_matches'] > 0 else 0
        consistency = np.std(s['gd_list']) if len(s['gd_list']) > 1 else 0
        l_date = last_active_dates.get(p, "")
        if l_date:
            try: l_date = pd.to_datetime(l_date).strftime("%d %b %y")
            except: pass
        
        badges = []
        streak = current_streaks[p]
        if streak >= 3: badges.append("🔥 Hot Hand")
        elif streak <= -3: badges.append("❄️ Cold Snap")
        if m_played >= 5:
            if consistency < 1.5: badges.append("🤖 Machine")
            if clutch_pct > 66 and s['clutch_matches'] >= 3: badges.append("🧊 Clutch")
            if (s['wins']/m_played) > 0.75: badges.append("🦁 Dominant")

        score_elo = round(elo_ratings[p], 1)
        current_utr = round(utr_ratings[p], 2)
        
        singles_perf = round((s['singles_wins'] / s['singles_matches']) * 100, 1) if s['singles_matches'] > 0 else 0
        doubles_perf = round((s['doubles_wins'] / s['doubles_matches']) * 100, 1) if s['doubles_matches'] > 0 else 0

        rank_data.append({
            "Player": p, "Points": s['points'], "Score": score_elo, "Label": "Elo", "Elo": score_elo, 
            "Score_Elo (Hybrid)": score_elo, "Score_Points": s['points'], 
            "Score_UTR": current_utr, "Last Change": last_elo_changes.get(p, 0),
            "Wins": s['wins'], "Losses": s['losses'], "Games Won": s['games_won'],
            "Win %": round((s['wins']/m_played)*100, 1), "Matches": m_played, 
            "Game Diff Avg": round(s['gd_sum']/m_played, 2) if m_played > 0 else 0,
            "Clutch Factor": round(clutch_pct, 1), 
            "Consistency Index": round(consistency, 2), "Last Active": l_date if l_date else "N/A",
            "Badges": badges, 
            "Profile": players_df.set_index('name')['profile_image_url'].get(p, DEFAULT_AVATAR),
            "Record": f"{s['wins']}W-{s['losses']}L",
            "Singles Perf": singles_perf,
            "Doubles Perf": doubles_perf,
        })
        
    df = pd.DataFrame(rank_data)
    if not df.empty:
        df = df.sort_values(by=["Score_Elo (Hybrid)", "Win %"], ascending=[False, False])
        df["Rank_Elo (Hybrid)"] = range(1, len(df) + 1)
        df = df.sort_values(by=["Score_Points", "Win %"], ascending=[False, False])
        df["Rank_Points"] = range(1, len(df) + 1)
        df = df.sort_values(by=["Score_UTR", "Win %"], ascending=[False, False])
        df["Rank_UTR"] = range(1, len(df) + 1)
        
        # Set default rank based on Elo Hybrid
        df = df.sort_values(by="Score_Elo (Hybrid)", ascending=False).reset_index(drop=True)
        df["Rank"] = df.index + 1
    return df


@st.cache_data(ttl=300)
def plot_player_performance(player_name, matches_df):
    if matches_df.empty: return None
    mask = (matches_df['team1_player1'] == player_name) | (matches_df['team1_player2'] == player_name) | \
            (matches_df['team2_player1'] == player_name) | (matches_df['team2_player2'] == player_name)
    df = matches_df[mask].copy()
    if df.empty: return None
    df['date'] = pd.to_datetime(df['date']); df = df.sort_values('date')
    history = []
    cum_gd = 0
    matches_count = 0
    for row in df.itertuples():
        is_t1 = player_name in [row.team1_player1, row.team1_player2]
        match_gd = 0
        for s in [row.set1, row.set2, row.set3]:
            if not s: continue
            s_str = str(s); t1_g, t2_g = 0, 0
            if "Tie Break" in s_str: 
                nums = re.findall(r'\d+', s_str)
                if len(nums) >= 2:
                    if int(nums[0]) > int(nums[1]): t1_g, t2_g = 7, 6
                    else: t1_g, t2_g = 6, 7
            elif '-' in s_str:
                try: p = s_str.split('-'); t1_g, t2_g = int(p[0]), int(p[1])
                except: continue
            if is_t1: match_gd += (t1_g - t2_g)
            else: match_gd += (t2_g - t1_g)
        cum_gd += match_gd; matches_count += 1
        w = row.winner; res = "Tie"
        if w == "Team 1": res = "Win" if is_t1 else "Loss"
        elif w == "Team 2": res = "Win" if not is_t1 else "Loss"
        history.append({"Date": row.date, "Match": f"Match {matches_count}", "Cumulative Game Diff": cum_gd, "Result": res})
    fig = px.line(history, x="Match", y="Cumulative Game Diff", hover_data=["Date", "Result"], title=f"Trend - {player_name}", markers=True)
    fig.update_layout(height=300, margin=dict(l=20, r=20, t=40, b=20))
    return fig

def load_bookings():
    cid = st.session_state.current_chapter['id'] if st.session_state.current_chapter else None
    df = fetch_data(BOOKINGS_TABLE, cid)
    cols = ['booking_id', 'date', 'time', 'match_type', 'court_name', 'player1', 'player2', 'player3', 'player4', 'standby_player', 'screenshot_url', 'chapter_id']
    for c in cols: 
        if c not in df.columns: df[c] = None
    if not df.empty:
        try: df['dt_combo'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['time'].astype(str), format='%Y-%m-%d %H:%M', errors='coerce')
        except: df['dt_combo'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['time'].astype(str), errors='coerce')
        if isinstance(df['dt_combo'].dtype, pd.DatetimeTZDtype): df['dt_combo'] = df['dt_combo'].dt.tz_convert('Asia/Dubai')
        else: df['dt_combo'] = df['dt_combo'].dt.tz_localize('Asia/Dubai', ambiguous='infer')
        cutoff = pd.Timestamp.now(tz='Asia/Dubai') - timedelta(hours=4)
        expired_ids = df[df['dt_combo'] < cutoff]['booking_id'].tolist()
        if expired_ids:
            try:
                conn = get_connection()
                with conn.cursor() as cur:
                    format_strings = ','.join(['%s'] * len(expired_ids))
                    cur.execute(f"DELETE FROM bookings WHERE booking_id IN ({format_strings})", tuple(expired_ids))
                conn.commit()
                conn.close()
                df = df[df['dt_combo'] >= cutoff]
            except: pass
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d'); df = df.fillna("")
    st.session_state.bookings_df = df[cols]

def save_bookings(df):
    cid = st.session_state.current_chapter['id']
    if cid and not df.empty:
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM bookings WHERE chapter_id = %s", (cid,))
                df_clean = df.where(pd.notnull(df), None)
                records = [tuple(x) for x in df_clean.to_numpy()]
                cols = ",".join(list(df.columns))
                if records:
                    query = f"INSERT INTO bookings ({cols}) VALUES %s"
                    execute_values(cur, query, records)
            conn.commit()
        except Exception as e:
            st.error(f"Save bookings error: {e}")
        finally:
            conn.close()

def delete_booking_from_db(booking_id):
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM bookings WHERE booking_id = %s", (booking_id,))
        conn.commit()
        conn.close()
        if "bookings_df" in st.session_state:
            st.session_state.bookings_df = st.session_state.bookings_df[st.session_state.bookings_df.booking_id != booking_id]
    except: pass

def display_hall_of_fame():
    st.header("🏆 Hall of Fame")
    st.info("Requires cloud.")

def load_courts():
    cid = st.session_state.current_chapter['id']
    conn = get_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT name, url FROM courts WHERE chapter_id = %s", (cid,))
        data = cur.fetchall()
    conn.close()
    return data

def add_court_db(name, url):
    cid = st.session_state.current_chapter['id']
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("INSERT INTO courts (chapter_id, name, url) VALUES (%s, %s, %s)", (cid, name, url))
    conn.commit()
    conn.close()

def remove_court_db(name):
    cid = st.session_state.current_chapter['id']
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM courts WHERE chapter_id = %s AND name = %s", (cid, name))
    conn.commit()
    conn.close()

# --- CHAPTER SELECTION & LANDING PAGE ---
def check_chapter_selected():
    if 'new_chapter_created' in st.session_state: return False
    if st.session_state.is_master_admin and st.session_state.current_chapter is None: return True
    if st.session_state.current_chapter is None: return False
    return True

if not check_chapter_selected():
    if 'new_chapter_created' in st.session_state:
        st.balloons()
        st.success(f"Chapter '{st.session_state.new_chapter_created['name']}' Created Successfully!")
        st.info(f"**ADMIN PASSWORD:** `{st.session_state.new_chapter_created['password']}`")
        st.warning("Please copy this password now. It will not be shown again.")
        st.divider()
        st.subheader("⚙️ Initial Chapter Setup")
        with st.form("initial_setup_form"):
            st.subheader("Ranking Systems")
            ranking_systems = {}
            for rs in ["Elo (Hybrid)", "Points", "UTR"]:
                ranking_systems[rs] = st.toggle(rs, value=(rs == "Elo (Hybrid)"))

            st.subheader("Match Type Settings")
            match_type_settings = {}
            set_options = ["Single Set", "Best of 3", "Best of 5"]
            
            for mt in ["Singles", "Doubles", "Mixed Doubles"]:
                st.markdown(f"**{mt}**")
                cols = st.columns([1, 1, 1, 2])
                enabled = cols[0].checkbox("Enabled", value=(mt in ["Singles", "Doubles"]), key=f"en_{mt}")
                win_points = cols[1].number_input("Win Pts", value=2, min_value=0, key=f"wp_{mt}")
                loss_points = cols[2].number_input("Loss Pts", value=1, min_value=0, key=f"lp_{mt}")
                min_sets = cols[3].selectbox("Min Sets", options=set_options, index=1, key=f"ms_{mt}")
                match_type_settings[mt] = {
                    "enabled": enabled,
                    "win_points": win_points,
                    "loss_points": loss_points,
                    "min_sets": min_sets
                }

            if st.form_submit_button("Save Settings & Enter Chapter", type="primary"):
                new_conf = {
                    "ranking_systems": ranking_systems,
                    "match_type_settings": match_type_settings,
                    "match_image_required": True # Default value
                }
                cid = st.session_state.new_chapter_created['id']
                save_chapter_config(cid, new_conf)
                st.session_state.current_chapter = {'id': cid, 'name': st.session_state.new_chapter_created['name']}
                st.session_state.chapter_config = new_conf
                st.session_state.is_admin = True
                st.session_state.can_write = True
                del st.session_state.new_chapter_created
                st.rerun()
    else:
        # Use the LOGO_URL directly
        st.markdown(f'<div style="text-align: left;"><img src="{LOGO_URL}" style="height:150px; margin-bottom: 10px;"></div>', unsafe_allow_html=True)
        
        st.header("Create & Manage your own Tennis league.")
        st.write("""Patch Moint allows Singles, Doubles and Mixed doubles matches with Rankings by Points per match or Elo or UTR. Patch Moint is Free and Open Source.""")

        # --- LOAD CHAPTERS FROM NEON ---
        try:
            engine = get_sqlalchemy_engine()
            with engine.connect() as conn:
                # Fetch all chapters, players, and matches to calculate stats
                chap_df = pd.read_sql(text("SELECT * FROM chapters"), conn)
                all_players = pd.read_sql(text("SELECT chapter_id FROM players"), conn)
                all_matches = pd.read_sql(text("SELECT chapter_id FROM matches"), conn)
            
            player_counts = all_players.groupby('chapter_id').size().to_dict()
            match_counts = all_matches.groupby('chapter_id').size().to_dict()
            
            if 'last_active_date' in chap_df.columns:
                chap_df['last_active_date'] = pd.to_datetime(chap_df['last_active_date'], errors='coerce')
                chap_df['last_active_date'] = chap_df['last_active_date'].fillna(pd.Timestamp.min)
            else:
                chap_df['last_active_date'] = pd.Timestamp.min # Add column if not exists, fill with min date

            if 'created_at' in chap_df.columns:
                chap_df['created_at'] = pd.to_datetime(chap_df['created_at'], errors='coerce')
            else:
                chap_df['created_at'] = pd.Timestamp.min # Add column if not exists, fill with min date

            chap_df = chap_df.sort_values(by=['last_active_date', 'created_at'], ascending=[False, False])

        except Exception as e:
            chap_df = pd.DataFrame()
            player_counts = {}
            match_counts = {}

        # --- LOGIN FORM (MOVED ABOVE CHAPTERS) ---
        if st.session_state.temp_selected_chapter:
            target = st.session_state.temp_selected_chapter
            st.divider()
            with st.container(border=True):
                st.markdown(f"### Login to: {target['name']}")
                
                # Fetch players for this chapter to check passwords
                chapter_players_df = fetch_data("players", chapter_id=target['id'])
                
                pw = st.text_input("Password", type="password", key="login_pw")
                st.caption("Hint: Leave password blank for Guest Login")
                
                c1, c2 = st.columns([2,1])

                if c1.button("Login"):
                    # 1. Check for empty password (Guest)
                    if not pw:
                        st.session_state.current_chapter = {'id': target['id'], 'name': target['name']}
                        st.session_state.is_admin = False
                        st.session_state.can_write = False # Guests can't write
                        st.session_state.logged_in_player = None
                        st.session_state.chapter_config = load_chapter_config(target['id'])
                        st.session_state.temp_selected_chapter = None
                        st.info("Guest Login")
                        time.sleep(0.5); st.rerun()

                    # 2. Check for Admin password
                    elif pw == target['admin_password']:
                        st.session_state.current_chapter = {'id': target['id'], 'name': target['name']}
                        st.session_state.is_admin = True
                        st.session_state.can_write = True
                        st.session_state.logged_in_player = None
                        st.session_state.chapter_config = load_chapter_config(target['id'])
                        st.session_state.temp_selected_chapter = None
                        st.success("Admin Login Success")
                        time.sleep(0.5); st.rerun()

                    # 3. Check for Player password
                    else:
                        player_match = chapter_players_df[chapter_players_df['password'] == pw]
                        if not player_match.empty:
                            player_row = player_match.iloc[0]
                            player_name = player_row['name']
                            is_player_admin = player_row.get('is_admin', False)

                            st.session_state.current_chapter = {'id': target['id'], 'name': target['name']}
                            st.session_state.is_admin = is_player_admin
                            st.session_state.can_write = True 
                            st.session_state.logged_in_player = player_name
                            st.session_state.chapter_config = load_chapter_config(target['id'])
                            st.session_state.temp_selected_chapter = None
                            
                            if is_player_admin:
                                st.success(f"Welcome Admin {player_name}!")
                            else:
                                st.success(f"Welcome {player_name}!")
                            time.sleep(0.5); st.rerun()
                        else:
                            st.error("Invalid Credentials")

                if c2.button("Cancel Selection"):
                    st.session_state.temp_selected_chapter = None
                    st.rerun()
        
        # --- ACTIVE CHAPTERS ---
        if not chap_df.empty:
            if 'sport' in chap_df.columns:
                # Filter by sport. Legacy NULLs/empty are treated as 'Tennis' (the original sport)
                if SPORT_TYPE == "Tennis":
                    chap_df = chap_df[(chap_df['sport'] == "Tennis") | (chap_df['sport'].isna()) | (chap_df['sport'] == "")]
                else:
                    chap_df = chap_df[chap_df['sport'] == SPORT_TYPE]
            else:
                # Sport column missing. If we are in Pickleball app, don't show any legacy chapters
                if SPORT_TYPE != "Tennis":
                    chap_df = pd.DataFrame()

            if not chap_df.empty:
                st.subheader("Active Chapters")
                cols = st.columns(3)
                
                # Use enumerate to ensure idx starts at 0 for clean column distribution
                for i, (idx, row) in enumerate(chap_df.iterrows()):
                    with cols[i % 3]:
                        img_container_content = ''
                        if row.get("title_image_url"):
                            img_src = get_img_src(row.get("title_image_url"))
                            img_container_content = f'<img src="{img_src}" style="width:100%">'
                        
                        img_html = (
                            '<div class="card-image-container">'
                            f'{img_container_content}'
                            '</div>'
                        )
                        
                        title_html = f'<h3>{row["name"]}</h3>'
                        num_players = player_counts.get(row['id'], 0)
                        num_matches = match_counts.get(row['id'], 0)
                        stats_html = f'<p style="margin: 10px 0; color: #aaa; font-size: 0.9em;">{num_players} players / {num_matches} games</p>'
                        
                        card_html = (
                            '<div class="chapter-card" style="height: auto; min-height: 200px; padding-bottom: 10px;">'
                            f'{img_html}'
                            '<div class="card-content">'
                            f'{title_html}'
                            f'{stats_html}'
                            '</div>'
                            '</div>'
                        )
                        
                        st.markdown(card_html, unsafe_allow_html=True)
                        
                        # The button appears immediately under the HTML card
                        #if st.button("Enter", key=f"ent_{row['id']}", width='stretch'):
                        #    st.session_state.temp_selected_chapter = row.to_dict()
                        #    st.rerun()

                        # Locate the block you mentioned and change it to:
                        if st.button("Enter", key=f"ent_{row['id']}", width='stretch'):
                            # Instead of setting state and rerunning, open the modal
                            login_modal(row.to_dict())
                            
            else:
                st.info(f"No active {SPORT_TYPE} chapters found. Create one below!")

                
        
        with st.expander("Explore Ranking Systems", expanded=False,icon="🏆"):
            st.markdown("""
            * **🏆 ELO Hybrid:** Best for highly competitive groups.
            * **📈 UTR System:** For serious club-level play—the punishing standard.
            * **🤝 Points Per Game:** For social games where grinders are rewarded!
            * **🔥 The Trifecta:** Go wild and use all three to measure your tribe.
            """)

        st.info("🔑 **Note:** Use the admin-provided password to log in to your Chapter.")
        
        st.divider()
        with st.expander("Create New Chapter", expanded=False, icon="➡️"):
            new_chap_name = st.text_input("New Chapter Name")
            new_admin_name = st.text_input("Admin Name")
            new_admin_email = st.text_input("Admin Email Address")
            if st.button("Create Chapter"):
                if new_chap_name and new_admin_name and new_admin_email:
                    if not chap_df.empty and new_chap_name in chap_df['name'].values:
                        st.error("Name exists")
                    else:
                        nid = str(uuid.uuid4()); npass = str(uuid.uuid4().hex)[:8]
                        conn = get_connection()
                        try:
                            # Try insert, assuming init_db fixed columns
                            with conn.cursor() as cur:
                                cur.execute("INSERT INTO chapters (id, name, admin_password, created_at, config, sport, last_active_date, admin_name, admin_email) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                                            (nid, new_chap_name, npass, datetime.now().isoformat(), json.dumps(get_default_config()), SPORT_TYPE, datetime.now().isoformat(), new_admin_name, new_admin_email))
                            conn.commit()
                            conn.close()
                            
                            # Send welcome email
                            if send_email(new_admin_email, new_admin_name, new_chap_name, npass):
                                st.success(f"Chapter '{new_chap_name}' Created! Admin password sent to {new_admin_email}.")
                            else:
                                st.warning(f"Chapter Created, but failed to send email. Admin Password: {npass}")
                                
                            st.session_state.new_chapter_created = {'name': new_chap_name, 'id': nid, 'password': npass}
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error creating chapter: {e}")
                            st.warning("If this persists, please refresh the page to ensure database migrations have run.")
                else:
                    st.warning("Please fill in all fields (Name, Admin Name, and Email).")
        
        with st.expander("Master Admin Login", expanded=False, icon="➡️"):
            m_pass = st.text_input("Master Password", type="password", key="ma_pass")
            if st.button("Login Master") and m_pass == st.secrets.get("madminpwd", "magic1"):
                st.session_state.is_master_admin = True; st.rerun()
    render_footer()
    st.stop()



def export_full_database():
    try:
        engine = get_sqlalchemy_engine()
        with engine.connect() as conn:
            # Fetch all data from all tables
            chapters_df = pd.read_sql("SELECT * FROM chapters", conn)
            players_df = pd.read_sql("SELECT * FROM players", conn)
            matches_df = pd.read_sql("SELECT * FROM matches", conn)
        
        # Create a buffer to hold the ZIP file
        zip_buffer = io.BytesIO()
        
        with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
            # Write each dataframe to a CSV inside the ZIP
            zip_file.writestr("chapters_export.csv", chapters_df.to_csv(index=False))
            zip_file.writestr("players_export.csv", players_df.to_csv(index=False))
            zip_file.writestr("matches_export.csv", matches_df.to_csv(index=False))
            
        return zip_buffer.getvalue()
    except Exception as e:
        st.error(f"Export failed: {e}")
        return None




# --- MASTER ADMIN DASHBOARD ---
if st.session_state.is_master_admin and st.session_state.current_chapter is None:
    st.title("🛡️ Master Admin Dashboard")
    
    # 1. Header Actions
    col_header_1, col_header_2 = st.columns([1, 1])
    with col_header_1:
        if st.button("Logout Master Admin", width='stretch'): 
            st.session_state.is_master_admin = False
            st.rerun()

    # 2. Database Stats & Connection
    try:
        engine = get_sqlalchemy_engine()
        with engine.connect() as conn:
            chapters = pd.read_sql("SELECT * FROM chapters", conn)
            total_players = pd.read_sql("SELECT COUNT(*) FROM players", conn).iloc[0, 0]
            total_matches = pd.read_sql("SELECT COUNT(*) FROM matches", conn).iloc[0, 0]
    except Exception as e:
        st.error(f"Error fetching dashboard stats: {e}")
        chapters = pd.DataFrame()
        total_players, total_matches = 0, 0
    
    # 3. Metrics Row
    st.markdown("### System-Wide Statistics")
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Chapters", len(chapters))
    c2.metric("Total Players", total_players) 
    c3.metric("Total Matches", total_matches) 

    # 4. System Backup Section
    st.divider()
    st.subheader("💾 System Backup")
    st.info("Download a complete snapshot of the database (all chapters, players, and matches) as a ZIP file.")
    
    zip_data = export_full_database()
    if zip_data:
        st.download_button(
            label="📥 Download Full Database (ZIP)",
            data=zip_data,
            file_name=f"patchmoint_backup_{datetime.now().strftime('%Y%m%d_%H%M')}.zip",
            mime="application/zip",
            type="primary",
            width='stretch'
        )
    st.divider()

    # 5. Chapter Management List
    st.subheader("All Active Chapters")
    
    if chapters.empty:
        st.info("No chapters created yet.")
    else:
        for idx, row in chapters.iterrows():
            with st.container(border=True):
                col_info, col_act = st.columns([3, 2])
                
                with col_info:
                    st.markdown(f"### {row['name']}")
                    st.markdown(f"**Sport:** {row.get('sport', 'Tennis')}")
                    st.markdown(f"**Admin:** {row.get('admin_name', 'N/A')} ({row.get('admin_email', 'N/A')})")
                    st.caption(f"ID: `{row['id']}` | Admin Pass: `{row['admin_password']}`")
                
                with col_act:
                    # Enter Chapter as Admin
                    if st.button(f"Enter Admin", key=f"ma_ent_{row['id']}", width='stretch'):
                        st.session_state.current_chapter = {'id': row['id'], 'name': row['name']}
                        st.session_state.chapter_config = load_chapter_config(row['id'])
                        st.session_state.is_admin = True
                        st.session_state.can_write = True
                        st.rerun()
                    
                    # Delete Chapter
                    delete_key = f"confirm_delete_{row['id']}"
                    if st.session_state.get(delete_key):
                        st.warning(f"Are you sure you want to permanently delete **{row['name']}** and all its data? This cannot be undone.")
                        c1, c2 = st.columns(2)
                        if c1.button("CONFIRM DELETION", key=f"ma_conf_del_{row['id']}", type="primary", width='stretch'):
                            if delete_chapter_fully(row['id']):
                                st.success(f"Deleted {row['name']}")
                                st.session_state[delete_key] = False
                                st.rerun()
                        if c2.button("Cancel", key=f"ma_canc_del_{row['id']}", width='stretch'):
                            st.session_state[delete_key] = False
                            st.rerun()
                    else:
                        if st.button(f"DELETE CHAPTER", key=f"ma_del_{row['id']}", type="primary", width='stretch'):
                            st.session_state[delete_key] = True
                            st.rerun()

                # Password & Admin Info Reset inside each Chapter card
                with st.expander(f"Manage Security & Admin for {row['name']}", expanded=False):
                    new_a_name = st.text_input("Admin Name", value=row.get('admin_name', ''), key=f"ana_{row['id']}")
                    new_a_email = st.text_input("Admin Email", value=row.get('admin_email', ''), key=f"aem_{row['id']}")
                    if st.button("Update Admin Info", key=f"uai_{row['id']}"):
                        conn = get_connection()
                        with conn.cursor() as cur:
                            cur.execute("UPDATE chapters SET admin_name = %s, admin_email = %s WHERE id = %s", (new_a_name, new_a_email, row['id']))
                        conn.commit()
                        conn.close()
                        st.success("Admin info updated!")
                        st.rerun()

                    st.divider()
                    npw = st.text_input("New Admin Password", key=f"nap_{row['id']}")
                    if st.button("Update Admin Password", key=f"rap_{row['id']}"):
                        if npw:
                            # Assuming update_chapter_admin_password is defined in your script
                            update_chapter_admin_password(row['id'], npw)
                            st.success("Password updated!")
                        else:
                            st.warning("Enter a password first.")

    render_footer()
    st.stop()

# --- MAIN APP LOGIC ---
if st.session_state.current_chapter:
    if not st.session_state.chapter_config:
        st.session_state.chapter_config = load_chapter_config(st.session_state.current_chapter['id'])

load_players()
load_matches()
load_bookings()

rank_df = pd.DataFrame()
if not st.session_state.matches_df.empty:
    rank_df = calculate_rankings(st.session_state.matches_df)

# Fetch chapter metadata
try:
    conn = get_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM chapters WHERE id = %s", (st.session_state.current_chapter['id'],))
        data = cur.fetchone()
    conn.close()
    chap_data = pd.DataFrame([data]) if data else pd.DataFrame()
except: chap_data = pd.DataFrame()

# Use the LOGO_URL directly in main app
st.markdown(f'<div style="text-align: left;"><img src="{LOGO_URL}" style="height:50px; margin-bottom: 10px;"></div>', unsafe_allow_html=True)

if not chap_data.empty and chap_data.iloc[0]['title_image_url']:
    img_path = chap_data.iloc[0]['title_image_url']
    src = get_img_src(img_path)
    st.markdown(f'<img src="{src}" style="height:150px; width:auto; object-fit:contain; margin-bottom:10px;">', unsafe_allow_html=True)
else:
    st.title(f"{st.session_state.current_chapter['name']}")



tab_names = ["Rankings", "Matches", "Player Profile", "Court Locations", "Bookings", "Hall of Fame"]
if st.session_state.is_admin: tab_names.append("Chapter Settings")
tabs = st.tabs(tab_names)


with tabs[0]:
    conf = st.session_state.chapter_config
    st.header(f"Rankings")
    
    active_systems_dict = conf.get("ranking_systems", {"Elo (Hybrid)": True})
    active_systems = [k for k, v in active_systems_dict.items() if v]
    if not active_systems: active_systems = ["Elo (Hybrid)"] 
    
    view_system = st.radio("Ranking System", active_systems, horizontal=True) if len(active_systems) > 1 else active_systems[0]
    
    # --- Ranking System Explanations ---
    # This part can be improved by dynamically creating descriptions based on match_type_settings
    pts_desc = "Varies by match type"
    if "match_type_settings" in conf:
        s_pts = conf["match_type_settings"].get("Singles", {})
        d_pts = conf["match_type_settings"].get("Doubles", {})
        pts_desc = f"S:{s_pts.get('win_points',0)}W/{s_pts.get('loss_points',0)}L, D:{d_pts.get('win_points',0)}W/{d_pts.get('loss_points',0)}L"

    ranking_descriptions = {
        "Elo (Hybrid)": {
            "desc": "A dynamic rating system that adjusts based on opponent quality. This hybrid version rewards Game Difference.",
            "scenario": "Best for competitive leagues."
        },
        "Points": {
            "desc": f"Cumulative system based on match type. ({pts_desc})",
            "scenario": "Ideal for social leagues."
        },
        "UTR": {
            "desc": "Universal Tennis Rating simulation. Focuses on game score margins.",
            "scenario": "Best for technical assessment."
        }
    }
    
    current_desc = ranking_descriptions.get(view_system, {"desc": "Custom ranking system.", "scenario": "General usage."})
    
    with st.expander(f"About {view_system}", expanded=False, icon="ℹ️"):
        st.markdown(f"**How it works:** {current_desc['desc']}")
        st.markdown(f"**Best for:** *{current_desc['scenario']}*")

    ranking_view = st.radio("View", ["Combined", "Doubles", "Singles", "Table View"], horizontal=True)
    display_rank_df = rank_df.copy() if not rank_df.empty else pd.DataFrame()

    if not st.session_state.matches_df.empty:
        if ranking_view == "Doubles": display_rank_df = calculate_rankings(st.session_state.matches_df[st.session_state.matches_df.match_type.isin(["Doubles", "Mixed Doubles"])])
        elif ranking_view == "Singles": display_rank_df = calculate_rankings(st.session_state.matches_df[st.session_state.matches_df.match_type == "Singles"])

    if display_rank_df.empty: 
        st.info("No matches.")
    else:
        sys_key = f"Score_{view_system}"
        if sys_key in display_rank_df.columns:
            display_rank_df = display_rank_df.sort_values(by=[sys_key, "Win %"], ascending=[False, False]).reset_index(drop=True)
            display_rank_df['Rank'] = [i+1 for i in display_rank_df.index]
            display_rank_df['Score'] = display_rank_df[sys_key]
            display_rank_df['Label'] = view_system

        if ranking_view == "Table View":
            cols = ['Rank', 'Profile', 'Player', 'Score', 'Label', 'Win %', 'Matches', 'Game Diff Avg', 'Singles Perf', 'Doubles Perf']
            st.dataframe(display_rank_df[cols], hide_index=True, width='stretch', 
                         column_config={"Profile": st.column_config.ImageColumn("PIC"), 
                                        "Win %": st.column_config.ProgressColumn(format="%.1f%%", min_value=0, max_value=100),
                                        "Singles Perf": st.column_config.ProgressColumn(format="%.1f%%", min_value=0, max_value=100),
                                        "Doubles Perf": st.column_config.ProgressColumn(format="%.1f%%", min_value=0, max_value=100)})
        else:
            # --- OPTIC YELLOW PODIUM ---
            if len(display_rank_df) >= 3:
                top3 = display_rank_df.head(3).to_dict('records')
                pod_order = [
                    {"p": top3[1], "color": "#C0C0C0", "icon": "🥈", "height": "210px"},
                    {"p": top3[0], "color": "#ccff00", "icon": "🥇", "height": "250px"},
                    {"p": top3[2], "color": "#CD7F32", "icon": "🥉", "height": "190px"}
                ]
                
                pod_html = '<div style="display:flex; align-items:flex-end; gap:12px; margin-bottom:40px; justify-content:center;">'
                for item in pod_order:
                    p = item["p"]
                    pod_html += f"""
                    <div style="flex:1; background:rgba(255,255,255,0.08); border-radius:15px; border-bottom:4px solid {item['color']}; padding:15px; text-align:center; height:{item['height']}; display:flex; flex-direction:column; justify-content:center;">
                        <div style="font-size:1.5em; margin-bottom:5px;">{item['icon']}</div>
                        <div class="glow-square" style="border-color:{item['color']}; width:80px; height:80px; box-shadow: 0 0 10px {item['color']}66;">
                            <img src="{get_img_src(p['Profile'])}">
                        </div>
                        <div style="color:white; font-weight:bold; font-size:0.9em; margin-top:10px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis;">{p['Player']}</div>
                        <div style="color:{item['color']}; font-weight:bold; font-size:1.2em;">{p['Score']:.1f}</div>
                    </div>"""
                pod_html += '</div>'
                st.markdown(pod_html, unsafe_allow_html=True)

            # --- RANKING PLAYER LIST ---
            for idx, row in display_rank_df.iterrows():
                ch = row.get('Last Change', 0)
                cc = "#00ff88" if ch >= 0 else "#ff4b4b"
                trend_arrow = "▲" if ch > 0 else "▼" if ch < 0 else "—"
                cd_html = f"<span style='color:{cc}; font-size:0.8em;'>{trend_arrow} {abs(ch)}</span>" if row['Label'] != 'Points' else ""
                badges_html = "".join([f"<span class='badge'>{b}</span>" for b in row.get('Badges', [])])

                with st.container(border=True):
                    c1, c2, c3 = st.columns([1.5, 2.5, 1.8])
                    
                    with c1:
                        st.markdown(f"""
                        <div style="text-align:center;">
                            <div style="font-size:1.8em; font-weight:bold; color:#ccff00; line-height:1;">🏆 #{row['Rank']}</div>
                            <div class="glow-square" style="margin-top:8px;">
                                <img src="{get_img_src(row['Profile'])}">
                            </div>
                            <div style="font-weight:bold; color:white; font-size:1.1em; margin-top:10px;">{row['Player']}</div>
                            <div style="color:#aaa; font-size:0.8em;">{row['Score']:.2f} {cd_html}</div>
                            <div style="margin-top:5px;">{badges_html}</div>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with c2:
                        st.markdown(f"""
                        <div style="display:grid; grid-template-columns:1fr 1fr 1fr; gap:8px; margin-top:15px; align-items: stretch; height:100%;">
                            <div style="border-left:3px solid #00FF88; background:rgba(0,255,136,0.05); padding:8px; border-radius:4px;"><div style="font-size:0.6em; color:#aaa; text-transform:uppercase;">Win %</div><div style="color:#00FF88; font-weight:bold; font-size:1.0em;">{row['Win %']}%</div></div>
                            <div style="border-left:3px solid #00C0F2; background:rgba(0,192,242,0.05); padding:8px; border-radius:4px;"><div style="font-size:0.6em; color:#aaa; text-transform:uppercase;">Record</div><div style="color:#00C0F2; font-weight:bold; font-size:1.0em;">{row['Record']}</div></div>
                            <div style="border-left:3px solid #FF4B4B; background:rgba(255,75,75,0.05); padding:8px; border-radius:4px;"><div style="font-size:0.6em; color:#aaa; text-transform:uppercase;">Clutch</div><div style="color:#FF4B4B; font-weight:bold; font-size:1.0em;">{row.get('Clutch Factor', 0)}%</div></div>
                            <div style="border-left:3px solid #ccff00; background:rgba(204,255,0,0.05); padding:8px; border-radius:4px;"><div style="font-size:0.6em; color:#aaa; text-transform:uppercase;">{row['Label']}</div><div style="color:#ccff00; font-weight:bold; font-size:1.0em;">{row.get('Score', 0)}</div></div>
                            <div style="border-left:3px solid #FFA500; background:rgba(255,165,0,0.05); padding:8px; border-radius:4px;"><div style="font-size:0.6em; color:#aaa; text-transform:uppercase;">GDA</div><div style="color:#FFA500; font-weight:bold; font-size:1.0em;">{row.get('Game Diff Avg', 0):+.2f}</div></div>
                            <div style="border-left:3px solid #FFFFFF; background:rgba(255,255,255,0.05); padding:8px; border-radius:4px;"><div style="font-size:0.6em; color:#aaa; text-transform:uppercase;">Games Won</div><div style="color:#FFFFFF; font-weight:bold; font-size:1.0em;">{row.get('Games Won', 0)}</div></div>
                            <div style="border-left:3px solid #9400D3; background:rgba(148,0,211,0.05); padding:8px; border-radius:4px;"><div style="font-size:0.6em; color:#aaa; text-transform:uppercase;">Consistency</div><div style="color:#9400D3; font-weight:bold; font-size:1.0em;">{row.get('Consistency Index', 0):.2f}</div></div>
                            <div style="border-left:3px solid #32CD32; background:rgba(50,205,50,0.05); padding:8px; border-radius:4px;"><div style="font-size:0.6em; color:#aaa; text-transform:uppercase;">Singles Perf</div><div style="color:#32CD32; font-weight:bold; font-size:1.0em;">{row.get('Singles Perf', 0)}%</div></div>
                            <div style="border-left:3px solid #1E90FF; background:rgba(30,144,255,0.05); padding:8px; border-radius:4px;"><div style="font-size:0.6em; color:#aaa; text-transform:uppercase;">Doubles Perf</div><div style="color:#1E90FF; font-weight:bold; font-size:1.0em;">{row.get('Doubles Perf', 0)}%</div></div>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with c3:
                        st.plotly_chart(create_radar_chart(row), width='stretch', config={'displayModeBar': False}, key=f"rd_{idx}")
                    
                    # --- DATA DISPLAY BELOW COLUMNS ---
                    st.divider() # Subtle line separating main stats from form
                    
                    p_name = row['Player']
                    m_df = st.session_state.matches_df
                    player_matches = m_df[
                        (m_df['team1_player1'] == p_name) | (m_df['team1_player2'] == p_name) |
                        (m_df['team2_player1'] == p_name) | (m_df['team2_player2'] == p_name)
                    ].copy()
                    
                    # 1. Recent Form Guide
                    if not player_matches.empty:
                        player_matches['dt'] = pd.to_datetime(player_matches['date'], errors='coerce')
                        player_matches = player_matches.sort_values('dt', ascending=False).head(5)

                        streak_html = '<div style="display:flex; gap:12px; justify-content:center; margin-bottom:10px;">'
                        for _, m in player_matches.iterrows():
                            is_t1 = (m['team1_player1'] == p_name or m['team1_player2'] == p_name)
                            won = (is_t1 and m['winner'] == "Team 1") or (not is_t1 and m['winner'] == "Team 2")
                            color = "#00FF88" if won else "#FF4B4B"
                            label = "W" if won else "L"
                            streak_html += f'<div style="width:30px; height:30px; border-radius:50%; background:{color}22; border:2px solid {color}; color:{color}; display:flex; justify-content:center; align-items:center; font-weight:bold; font-size:0.8em; box-shadow:0 0 8px {color}33;">{label}</div>'
                        streak_html += '</div>'
                        st.markdown(streak_html, unsafe_allow_html=True)
                    
                    # 2. Power Level Bar
                    max_score = display_rank_df['Score'].max() if not display_rank_df.empty else 1
                    current_score = row['Score']
                    percent_of_max = min((current_score / max_score) * 100, 100)
                    
                    st.markdown(f"""
                    <div style="padding: 0 10px 10px 10px;">
                        <div style="display:flex; justify-content:space-between; font-size:0.65em; color:#aaa; margin-bottom:4px;">
                            <span style="letter-spacing:1px; font-weight:bold;">PLAYER POTENTIAL / LEAGUE STANDING</span>
                            <span style="color:#ccff00; font-weight:bold;">{percent_of_max:.1f}%</span>
                        </div>
                        <div style="width:100%; height:6px; background:rgba(255,255,255,0.05); border-radius:10px; overflow:hidden; border:1px solid rgba(255,255,255,0.1);">
                            <div style="width:{percent_of_max}%; height:100%; background:linear-gradient(90deg, #ccff00, #00FF88); border-radius:10px; box-shadow:0 0 12px #ccff00aa;"></div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)





with tabs[1]:
    st.header("Matches")
    
    # --- Custom CSS for Modern Match Cards ---
    st.markdown("""
    <style>
        .modern-match-card {
            background: linear-gradient(145deg, rgba(255,255,255,0.08) 0%, rgba(255,255,255,0.02) 100%);
            border: 1px solid rgba(255,255,255,0.1);
            border-radius: 16px;
            margin-bottom: 24px;
            overflow: hidden;
            transition: all 0.3s ease;
            box-shadow: 0 4px 6px rgba(0,0,0,0.2);
        }
        .modern-match-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 15px rgba(0,0,0,0.4);
            border-color: rgba(255, 95, 31, 0.4); /* Orange border on hover */
        }
        .mmc-header {
            display: flex;
            justify-content: space-between;
            padding: 12px 20px;
            background: rgba(0,0,0,0.2);
            font-size: 0.85em;
            color: #888;
            border-bottom: 1px solid rgba(255,255,255,0.05);
        }
        .mmc-body {
            display: flex;
            align-items: center;
            padding: 20px 10px; /* Reduced side padding */
            position: relative;
        }
        .mmc-team {
            flex: 1;
            text-align: center;
            display: flex;
            flex-direction: column;
            align-items: center;
            z-index: 2;
        }
        .mmc-avatar {
            width: 100px;
            height: 120px;
            border-radius: 15%;
            border: 2px solid #444;
            object-fit: cover;
            margin-bottom: 8px;
            background: #222;
        }
        .mmc-winner-img {
            border-color: #FF5F1F; /* Orange border for winner */
            box-shadow: 0 0 15px rgba(255, 95, 31, 0.4);
        }
        .mmc-name {
            font-weight: bold;
            font-size: 1.0em;
            color: #eee;
            line-height: 1.2;
        }
        .mmc-winner-text {
            color: #FF5F1F; /* Orange text for winner */
            text-shadow: 0 0 10px rgba(255, 95, 31, 0.2);
        }
        .mmc-vs-container {
            flex: 0 0 140px; /* Wider container for the score */
            text-align: center;
            z-index: 2;
            display: flex;
            flex-direction: column;
            justify-content: center;
        }
        .mmc-vs-label {
            font-size: 0.7em;
            color: #666;
            font-weight: bold;
            margin-bottom: 2px;
            letter-spacing: 2px;
        }
        .mmc-score-main {
            font-size: 2.2em; /* BIGGER */
            font-weight: 900;
            color: #FF5F1F; /* BRIGHT ORANGE */
            letter-spacing: 1px;
            line-height: 1.1;
            text-shadow: 0 0 20px rgba(255, 95, 31, 0.3); /* GLOW */
            white-space: nowrap;
        }
        .mmc-footer {
            padding: 12px 20px;
            background: rgba(255,255,255,0.03);
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-top: 1px solid rgba(255,255,255,0.05);
        }
        .mmc-tag {
            background: rgba(255, 95, 31, 0.15);
            color: #FF5F1F;
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 0.75em;
            font-weight: bold;
            text-transform: uppercase;
        }
        .mmc-stat {
            color: #aaa;
            font-size: 0.9em;
        }
    </style>
    """, unsafe_allow_html=True)

    config = st.session_state.chapter_config
    is_img_required = config.get("match_image_required", True)
    
    # --- POST RESULT SECTION ---
    if st.session_state.can_write:
        with st.expander("➕ Post Result", expanded=False, icon="➡️"):
            if st.session_state.players_df.empty: 
                st.warning("Add players first.")
            else:
                pk = st.session_state.match_post_key
                pnames = sorted([p for p in st.session_state.players_df["name"].dropna().tolist() if p != "Visitor"])
                
                match_type_settings = config.get("match_type_settings", get_default_config()["match_type_settings"])
                ui_opts = []
                if match_type_settings.get("Singles", {}).get("enabled"): ui_opts.append("Singles")
                if match_type_settings.get("Doubles", {}).get("enabled") or match_type_settings.get("Mixed Doubles", {}).get("enabled"): ui_opts.append("Doubles")
                
                if not ui_opts: 
                    st.warning("No match types enabled in Chapter Settings.")
                    ui_opts = ["Singles"]

                mt = st.radio("Type", ui_opts, horizontal=True, key=f"mt_{pk}")
                
                is_mixed = False
                if mt == "Doubles" and match_type_settings.get("Mixed Doubles", {}).get("enabled"):
                    is_mixed = st.checkbox("This is a Mixed Doubles match", key=f"mixed_{pk}")
                
                final_match_type = "Mixed Doubles" if is_mixed else mt
                md = st.date_input("Date", datetime.now(), key=f"md_{pk}")
                
                c1, c2 = st.columns(2)
                if mt == "Doubles":
                    opts = [""] + pnames + ["Visitor"]
                    t1p1 = c1.selectbox("T1 P1", opts, key=f"1_{pk}"); t1p2 = c1.selectbox("T1 P2", opts, key=f"2_{pk}")
                    t2p1 = c2.selectbox("T2 P1", opts, key=f"3_{pk}"); t2p2 = c2.selectbox("T2 P2", opts, key=f"4_{pk}")
                else:
                    opts = [""] + pnames
                    t1p1 = c1.selectbox("P1", opts, key=f"1s_{pk}"); t2p1 = c2.selectbox("P2", opts, key=f"2s_{pk}")
                    t1p2, t2p2 = None, None
                
                sc1, sc2, sc3 = st.columns(3)
                s_list = [""] + get_valid_scores()
                s1 = sc1.selectbox("Set 1", s_list, key=f"s1_{pk}"); s2 = sc2.selectbox("Set 2", s_list, key=f"s2_{pk}"); s3 = sc3.selectbox("Set 3", s_list, key=f"s3_{pk}")
                win = st.radio("Winner", ["Team 1", "Team 2"], horizontal=True, key=f"w_{pk}")
                img = st.file_uploader("Photo", type=["jpg", "png"], key=f"im_{pk}")

                if st.button("Post Match", key=f"bp_{pk}"):
                    if s1 and (img or not is_img_required):
                        mid = str(uuid.uuid4())
                        path = save_remote_image(img, mid, "match") if img else ""
                        new_row = {
                            "match_id": mid, "date": md.strftime('%Y-%m-%d'), "match_type": final_match_type, 
                            "team1_player1": t1p1, "team1_player2": t1p2, "team2_player1": t2p1, "team2_player2": t2p2, 
                            "set1": s1, "set2": s2, "set3": s3, "winner": win, "match_image_url": path, 
                            "chapter_id": st.session_state.current_chapter['id']
                        }
                        new_row_df = pd.DataFrame([new_row])
                        save_matches(new_row_df) 
                        st.session_state.matches_df = pd.concat([st.session_state.matches_df, new_row_df], ignore_index=True)
                        st.session_state.match_post_key += 1
                        st.success(f"Saved as {mt}"); time.sleep(1); st.rerun()
                    else: st.error("Score & Photo required")

    # --- MATCH HISTORY DISPLAY ---
    player_imgs = {}
    if not st.session_state.players_df.empty:
        for _, p_row in st.session_state.players_df.iterrows():
            player_imgs[p_row['name']] = p_row.get('profile_image_url')

    m_hist = st.session_state.matches_df.copy()
    if not m_hist.empty:
        m_hist['date'] = pd.to_datetime(m_hist['date'], errors='coerce')
        m_hist = m_hist.sort_values('date', ascending=False)
        
        for row in m_hist.itertuples():
            t1_p1_name = row.team1_player1
            t1_p2_name = getattr(row, 'team1_player2', '')
            t2_p1_name = row.team2_player1
            t2_p2_name = getattr(row, 'team2_player2', '')

            def get_p_img(name):
                return get_img_src(player_imgs.get(name, ''))

            # Stats Calculation
            t1_games_total = 0
            t2_games_total = 0
            sets_played = 0
            set_scores_display = []
            
            for s in [getattr(row, 'set1',''), getattr(row, 'set2',''), getattr(row, 'set3','')]:
                if s:
                    sets_played += 1
                    s_str = str(s)
                    set_scores_display.append(s_str)
                    
                    g1, g2 = 0, 0
                    if "Tie Break" in s_str:
                         nums = re.findall(r'\d+', s_str)
                         if len(nums) >= 2:
                             if int(nums[0]) > int(nums[1]): g1, g2 = 7, 6
                             else: g1, g2 = 6, 7
                    elif '-' in s_str:
                        try:
                            parts = s_str.split('-')
                            g1, g2 = int(parts[0]), int(parts[1])
                        except: pass
                    t1_games_total += g1
                    t2_games_total += g2

            game_diff = abs(t1_games_total - t2_games_total)
            
            # Winner Logic
            t1_won = (row.winner == "Team 1")
            t1_class = "mmc-winner-text" if t1_won else ""
            t2_class = "mmc-winner-text" if not t1_won else ""
            t1_img_class = "mmc-winner-img" if t1_won else ""
            t2_img_class = "mmc-winner-img" if not t1_won else ""
            
            if t1_p2_name:
                t1_html = f"""<div style="display:flex; gap:5px; justify-content:center;">
                                <img src="{get_p_img(t1_p1_name)}" class="mmc-avatar {t1_img_class}">
                                <img src="{get_p_img(t1_p2_name)}" class="mmc-avatar {t1_img_class}">
                              </div>
                              <div class="mmc-name {t1_class}">{t1_p1_name}<br>& {t1_p2_name}</div>"""
            else:
                t1_html = f"""<img src="{get_p_img(t1_p1_name)}" class="mmc-avatar {t1_img_class}">
                              <div class="mmc-name {t1_class}">{t1_p1_name}</div>"""

            if t2_p2_name:
                t2_html = f"""<div style="display:flex; gap:5px; justify-content:center;">
                                <img src="{get_p_img(t2_p1_name)}" class="mmc-avatar {t2_img_class}">
                                <img src="{get_p_img(t2_p2_name)}" class="mmc-avatar {t2_img_class}">
                              </div>
                              <div class="mmc-name {t2_class}">{t2_p1_name}<br>& {t2_p2_name}</div>"""
            else:
                t2_html = f"""<img src="{get_p_img(t2_p1_name)}" class="mmc-avatar {t2_img_class}">
                              <div class="mmc-name {t2_class}">{t2_p1_name}</div>"""

            # Badges
            badges = []
            if game_diff >= 8: badges.append("DOMINATION")
            if game_diff <= 2: badges.append("NAIL BITER")
            if sets_played == 2 and (t1_won and t1_games_total > t2_games_total): badges.append("STRAIGHT SETS")
            
            badges_html = "".join([f'<span class="mmc-tag" style="margin-right:5px;">{b}</span>' for b in badges])
            
            # Format Score String (with line breaks if 3 sets to keep it readable)
            if len(set_scores_display) == 3:
                # If 3 sets, break line for neatness
                scores_str = f"{set_scores_display[0]} {set_scores_display[1]}<br>{set_scores_display[2]}"
            else:
                scores_str = " ".join(set_scores_display)
            
            # Render Card
            st.markdown(f"""
            <div class="modern-match-card">
                <div class="mmc-header">
                    <div>📅 {row.date.strftime('%d %b %Y') if pd.notnull(row.date) else ''}</div>
                    <div style="font-weight:bold; color:#FF5F1F;">{getattr(row, 'match_type', 'Match').upper()}</div>
                </div>
                <div class="mmc-body">
                    <div class="mmc-team">{t1_html}</div>
                    <div class="mmc-vs-container">
                        <div class="mmc-vs-label">VS</div>
                        <div class="mmc-score-main">{scores_str}</div>
                    </div>
                    <div class="mmc-team">{t2_html}</div>
                </div>
                <div class="mmc-footer">
                    <div>{badges_html}</div>
                    <div class="mmc-stat">Game Diff: <span style="color:#FF5F1F; font-weight:bold;">{game_diff}</span></div>
                </div>
            </div>
            """, unsafe_allow_html=True)

            # Match Photo Expander
            img_url = getattr(row, 'match_image_url', '')
            if img_url:
                with st.expander("📷 View Match Photo", expanded=False):
                    st.image(img_url, use_container_width=True)

            # Edit/Delete Logic
            can_edit_match = False
            if st.session_state.is_admin or st.session_state.is_master_admin: can_edit_match = True
            elif st.session_state.get('logged_in_player'):
                me = st.session_state.logged_in_player
                if me in [t1_p1_name, t1_p2_name, t2_p1_name, t2_p2_name]: can_edit_match = True
            
            if can_edit_match:
                with st.expander(f"⚙️ Manage Result ({row.match_id})", expanded=False):
                    if st.button("Delete Match Record", key=f"del_{row.match_id}"): 
                        delete_match_from_db(row.match_id)
                        st.rerun()                         

with tabs[2]:
    st.header("Player Profile")
    if st.session_state.is_admin:
        with st.expander("Manage Players", expanded=False, icon="➡️"):
            new_p = st.text_input("New Name")
            gend = st.selectbox("Gender (Optional)", ["", "Male", "Female"])
            
            # Check if new_p already exists to determine if it's a new player or a potential duplicate check for the UI
            # For the purpose of this form, we assume 'new_p' is a new player until added
            
            initial_utr_input = st.number_input("Initial UTR (Optional, for new players)", min_value=1.0, max_value=16.5, value=None, format="%.2f", help="Enter UTR if player has not played any games yet. Max 16.5, Min 1.0. This can only be set when the player has not played any matches.")
            
            if st.button("Add", key="add_player_btn"):
                if new_p:
                    # Check if player already exists in the current chapter
                    if new_p in st.session_state.players_df['name'].tolist():
                        st.error(f"Player '{new_p}' already exists in this chapter.")
                    else:
                        # Check if the player name has played any matches globally (across chapters, or just within this chapter for a robust check)
                        # For simplicity, we'll check against current chapter's matches_df
                        player_has_played = (
                            (st.session_state.matches_df['team1_player1'] == new_p) |
                            (st.session_state.matches_df['team1_player2'] == new_p) |
                            (st.session_state.matches_df['team2_player1'] == new_p) |
                            (st.session_state.matches_df['team2_player2'] == new_p)
                        ).any()
                        
                        if initial_utr_input is not None and player_has_played:
                            st.warning(f"Initial UTR can only be set for players who have not played any matches. '{new_p}' has played matches.")
                        else:
                            pw = str(uuid.uuid4().hex)[:8]
                            final_gender = gend if gend else None
                            new_player_data = {
                                "name": new_p,
                                "profile_image_url": "",
                                "birthday": "",
                                "chapter_id": st.session_state.current_chapter['id'],
                                "password": pw,
                                "gender": final_gender,
                                "initial_utr": initial_utr_input if initial_utr_input is not None else None
                            }
                            st.session_state.players_df = pd.concat([st.session_state.players_df, pd.DataFrame([new_player_data])], ignore_index=True)
                            save_players(st.session_state.players_df); load_players()
                            st.success(f"Added '{new_p}'! Password: {pw}" + (f" (Initial UTR: {initial_utr_input})" if initial_utr_input is not None else ""))
                            st.session_state.form_key_suffix += 1 # Increment to reset form state
                            st.rerun()
            st.markdown("---")
            if not st.session_state.players_df.empty:
                player_names = [""] + st.session_state.players_df['name'].tolist()
                sel = st.selectbox("Edit Player", options=player_names, index=0)

                if sel:
                    row_index = st.session_state.players_df[st.session_state.players_df['name'] == sel].index[0]
                    row = st.session_state.players_df.loc[row_index]

                    with st.form(key=f"edit_player_{sel}"):
                        st.subheader(f"Editing: {sel}")

                        # Image uploader
                        ni = st.file_uploader("Profile Image", type=["jpg", "png"], key=f"pu_{sel}")

                        # Initial UTR
                        current_utr = row.get('initial_utr')
                        new_utr = st.number_input(
                            "Starting UTR",
                            value=float(current_utr) if pd.notna(current_utr) else None,
                            min_value=1.0, max_value=16.5, step=0.1, format="%.2f"
                        )

                        # Admin status
                        is_p_admin = st.checkbox("Player is Admin", value=row.get('is_admin', False))

                        # Password reset
                        new_pass = st.text_input("Reset Password", placeholder="Leave blank to keep current")

                        # --- Action Buttons ---
                        c1, c2 = st.columns([1,1])
                        update_button = c1.form_submit_button("Update Player", use_container_width=True, type="primary")
                        delete_button = c2.form_submit_button("Delete Player", use_container_width=True)

                        if update_button:
                            # --- Update Logic ---
                            has_changed = False

                            # 1. Update Image
                            if ni is not None:
                                path = save_remote_image(ni, sel, "profile")
                                if path:
                                    st.session_state.players_df.at[row_index, 'profile_image_url'] = path
                                    has_changed = True

                            # 2. Update UTR
                            utr_changed = (pd.isna(current_utr) and pd.notna(new_utr)) or \
                                          (pd.notna(current_utr) and pd.isna(new_utr)) or \
                                          (pd.notna(current_utr) and pd.notna(new_utr) and float(current_utr) != new_utr)
                            if utr_changed:
                                st.session_state.players_df.at[row_index, 'initial_utr'] = new_utr
                                has_changed = True

                            # 3. Update Admin Status
                            if is_p_admin != row.get('is_admin', False):
                                st.session_state.players_df.at[row_index, 'is_admin'] = is_p_admin
                                has_changed = True

                            # 4. Save player changes to DB
                            if has_changed:
                                save_players(st.session_state.players_df)
                                st.toast(f"Player {sel} updated!")
                            
                            # 5. Update Password
                            if new_pass:
                                update_player_password(sel, new_pass)
                                st.toast(f"Password for {sel} updated.")
                            
                            if has_changed or new_pass:
                                st.rerun()

                        if delete_button:
                            delete_player_from_db(sel)
                            st.session_state.players_df = st.session_state.players_df[st.session_state.players_df['name'] != sel]
                            st.rerun()

    
    
    
    for idx, row in st.session_state.players_df.sort_values("name").iterrows():
            p_name = row['name']
            
            # Prepare data for profile view using default/first active ranking system
            # This ensures we have the Score/Label/Rank fields populated
            profile_view_system = "Elo (Hybrid)"
            active_systems_dict = st.session_state.chapter_config.get("ranking_systems", {"Elo (Hybrid)": True})
            active_systems = [k for k, v in active_systems_dict.items() if v]
            if active_systems: profile_view_system = active_systems[0]

            display_profile_rank_df = rank_df.copy() if not rank_df.empty else pd.DataFrame()
            if not display_profile_rank_df.empty:
                sys_key = f"Score_{profile_view_system}"
                if sys_key in display_profile_rank_df.columns:
                    display_profile_rank_df = display_profile_rank_df.sort_values(by=[sys_key, "Win %"], ascending=[False, False]).reset_index(drop=True)
                    display_profile_rank_df['Rank'] = display_profile_rank_df.index + 1
                    display_profile_rank_df['Score'] = display_profile_rank_df[sys_key]
                    display_profile_rank_df['Label'] = profile_view_system
            
            p_stats = display_profile_rank_df[display_profile_rank_df['Player'] == p_name] if not display_profile_rank_df.empty else pd.DataFrame()
            has_stats = not p_stats.empty
            s = p_stats.iloc[0] if has_stats else {}
    
            if has_stats:
                # --- RENDER CARD (MATCHING RANKINGS TAB DESIGN) ---
                ch = s.get('Last Change', 0)
                cc = "#00ff88" if ch >= 0 else "#ff4b4b"
                trend_arrow = "▲" if ch > 0 else "▼" if ch < 0 else "—"
                cd_html = f"<span style='color:{cc}; font-size:0.8em;'>{trend_arrow} {abs(ch)}</span>" if s['Label'] != 'Points' else ""
                badges_html = "".join([f"<span class='badge'>{b}</span>" for b in s.get('Badges', [])])

                with st.container(border=True):
                    c1, c2, c3 = st.columns([1.5, 2.5, 1.8])
                    
                    with c1:
                        st.markdown(f"""
                        <div style="text-align:center;">
                            <div style="font-size:1.8em; font-weight:bold; color:#ccff00; line-height:1;">🏆 #{s['Rank']}</div>
                            <div class="glow-square" style="margin-top:8px;">
                                <img src="{get_img_src(s['Profile'])}">
                            </div>
                            <div style="font-weight:bold; color:white; font-size:1.1em; margin-top:10px;">{s['Player']}</div>
                            <div style="color:#aaa; font-size:0.8em;">{s['Score']:.2f} {cd_html}</div>
                            <div style="margin-top:5px;">{badges_html}</div>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with c2:
                        st.markdown(f"""
                        <div style="display:grid; grid-template-columns:1fr 1fr 1fr; gap:8px; margin-top:15px; align-items: stretch; height:100%;">
                            <div style="border-left:3px solid #00FF88; background:rgba(0,255,136,0.05); padding:8px; border-radius:4px;"><div style="font-size:0.6em; color:#aaa; text-transform:uppercase;">Win %</div><div style="color:#00FF88; font-weight:bold; font-size:1.0em;">{s['Win %']}%</div></div>
                            <div style="border-left:3px solid #00C0F2; background:rgba(0,192,242,0.05); padding:8px; border-radius:4px;"><div style="font-size:0.6em; color:#aaa; text-transform:uppercase;">Record</div><div style="color:#00C0F2; font-weight:bold; font-size:1.0em;">{s['Record']}</div></div>
                            <div style="border-left:3px solid #FF4B4B; background:rgba(255,75,75,0.05); padding:8px; border-radius:4px;"><div style="font-size:0.6em; color:#aaa; text-transform:uppercase;">Clutch</div><div style="color:#FF4B4B; font-weight:bold; font-size:1.0em;">{s.get('Clutch Factor', 0)}%</div></div>
                            <div style="border-left:3px solid #ccff00; background:rgba(204,255,0,0.05); padding:8px; border-radius:4px;"><div style="font-size:0.6em; color:#aaa; text-transform:uppercase;">{s['Label']}</div><div style="color:#ccff00; font-weight:bold; font-size:1.0em;">{s.get('Score', 0)}</div></div>
                            <div style="border-left:3px solid #FFA500; background:rgba(255,165,0,0.05); padding:8px; border-radius:4px;"><div style="font-size:0.6em; color:#aaa; text-transform:uppercase;">GDA</div><div style="color:#FFA500; font-weight:bold; font-size:1.0em;">{s.get('Game Diff Avg', 0):+.2f}</div></div>
                            <div style="border-left:3px solid #FFFFFF; background:rgba(255,255,255,0.05); padding:8px; border-radius:4px;"><div style="font-size:0.6em; color:#aaa; text-transform:uppercase;">Games Won</div><div style="color:#FFFFFF; font-weight:bold; font-size:1.0em;">{s.get('Games Won', 0)}</div></div>
                            <div style="border-left:3px solid #9400D3; background:rgba(148,0,211,0.05); padding:8px; border-radius:4px;"><div style="font-size:0.6em; color:#aaa; text-transform:uppercase;">Consistency</div><div style="color:#9400D3; font-weight:bold; font-size:1.0em;">{s.get('Consistency Index', 0):.2f}</div></div>
                            <div style="border-left:3px solid #32CD32; background:rgba(50,205,50,0.05); padding:8px; border-radius:4px;"><div style="font-size:0.6em; color:#aaa; text-transform:uppercase;">Singles Perf</div><div style="color:#32CD32; font-weight:bold; font-size:1.0em;">{s.get('Singles Perf', 0)}%</div></div>
                            <div style="border-left:3px solid #1E90FF; background:rgba(30,144,255,0.05); padding:8px; border-radius:4px;"><div style="font-size:0.6em; color:#aaa; text-transform:uppercase;">Doubles Perf</div><div style="color:#1E90FF; font-weight:bold; font-size:1.0em;">{s.get('Doubles Perf', 0)}%</div></div>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with c3:
                        st.plotly_chart(create_radar_chart(s), width='stretch', config={'displayModeBar': False}, key=f"rp_rd_{idx}")

                    # --- DATA DISPLAY BELOW COLUMNS (FORM & POWER) ---
                    st.divider() 
                    
                    # 1. Recent Form Guide
                    m_df = st.session_state.matches_df
                    player_matches = m_df[
                        (m_df['team1_player1'] == p_name) | (m_df['team1_player2'] == p_name) |
                        (m_df['team2_player1'] == p_name) | (m_df['team2_player2'] == p_name)
                    ].copy()
                    
                    if not player_matches.empty:
                        player_matches['dt'] = pd.to_datetime(player_matches['date'], errors='coerce')
                        player_matches = player_matches.sort_values('dt', ascending=False).head(5)

                        streak_html = '<div style="display:flex; gap:12px; justify-content:center; margin-bottom:10px;">'
                        for _, m in player_matches.iterrows():
                            is_t1 = (m['team1_player1'] == p_name or m['team1_player2'] == p_name)
                            won = (is_t1 and m['winner'] == "Team 1") or (not is_t1 and m['winner'] == "Team 2")
                            color = "#00FF88" if won else "#FF4B4B"
                            label = "W" if won else "L"
                            streak_html += f'<div style="width:30px; height:30px; border-radius:50%; background:{color}22; border:2px solid {color}; color:{color}; display:flex; justify-content:center; align-items:center; font-weight:bold; font-size:0.8em; box-shadow:0 0 8px {color}33;">{label}</div>'
                        streak_html += '</div>'
                        st.markdown(streak_html, unsafe_allow_html=True)
                    
                    # 2. Power Level Bar
                    max_score = display_profile_rank_df['Score'].max() if not display_profile_rank_df.empty else 1
                    current_score = s['Score']
                    percent_of_max = min((current_score / max_score) * 100, 100)
                    
                    st.markdown(f"""
                    <div style="padding: 0 10px 10px 10px;">
                        <div style="display:flex; justify-content:space-between; font-size:0.65em; color:#aaa; margin-bottom:4px;">
                            <span style="letter-spacing:1px; font-weight:bold;">PLAYER POTENTIAL / LEAGUE STANDING</span>
                            <span style="color:#ccff00; font-weight:bold;">{percent_of_max:.1f}%</span>
                        </div>
                        <div style="width:100%; height:6px; background:rgba(255,255,255,0.05); border-radius:10px; overflow:hidden; border:1px solid rgba(255,255,255,0.1);">
                            <div style="width:{percent_of_max}%; height:100%; background:linear-gradient(90deg, #ccff00, #00FF88); border-radius:10px; box-shadow:0 0 12px #ccff00aa;"></div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
            else:
                # --- NO STATS FALLBACK ---
                img_src = get_img_src(row['profile_image_url'])
                with st.container(border=True):
                    c1, c2 = st.columns([1, 4])
                    with c1:
                        st.markdown(f'<div class="glow-square" style="width:80px; height:80px; margin:0 auto;"><img src="{img_src}"></div><div style="text-align:center; font-weight:bold; color:white; margin-top:5px; font-size:0.9em;">{p_name}</div>', unsafe_allow_html=True)
                    with c2:
                        st.info("No stats yet. Play a match to get started!")
            st.divider()

with tabs[3]:
    st.header("Courts")
    if st.session_state.is_admin:
        with st.expander("Add Court", expanded=False, icon="➡️"):
            n, u = st.text_input("Name"), st.text_input("URL")
            if st.button("Add", key="add_court_btn"): add_court_db(n, u); st.rerun()
    courts = load_courts()
    if courts:
        cols = st.columns(3)
        for i, c in enumerate(courts):
             with cols[i%3]: st.markdown(f"""<div class="court-card"><h4>{c.get('name')}</h4><a href="{c.get('url')}" target="_blank">Map</a></div>""", unsafe_allow_html=True)

with tabs[4]:
    st.header("Bookings")
    if st.session_state.can_write:
        with st.expander("New Booking", expanded=False, icon="➡️"):
            with st.form("nb"):
                d = st.date_input("Date"); t = st.selectbox("Time", [f"{h}:00" for h in range(6, 23)])
                court_names = [c['name'] for c in courts] if courts else []
                c = st.selectbox("Court", court_names)
                if st.form_submit_button("Book"):
                    bid = str(uuid.uuid4())
                    st.session_state.bookings_df = pd.concat([st.session_state.bookings_df, pd.DataFrame([{"booking_id": bid, "date": d.isoformat(), "time": f"{t}:00", "court_name": c, "chapter_id": st.session_state.current_chapter['id']}])], ignore_index=True)
                    save_bookings(st.session_state.bookings_df); st.rerun()
    if not st.session_state.bookings_df.empty:
        st.dataframe(st.session_state.bookings_df[['date', 'time', 'court_name']], hide_index=True)

with tabs[5]: display_hall_of_fame()

if st.session_state.is_admin:
    with tabs[6]:
        st.header("Settings")
        with st.form("sets"):
            st.subheader("Ranking Systems")
            current_ranking_systems = st.session_state.chapter_config.get("ranking_systems", {})
            ranking_systems = {}
            for rs in ["Elo (Hybrid)", "Points", "UTR"]:
                ranking_systems[rs] = st.toggle(rs, value=current_ranking_systems.get(rs, False))

            st.subheader("Match Type Settings")
            current_match_settings = st.session_state.chapter_config.get("match_type_settings", get_default_config()["match_type_settings"])
            match_type_settings = {}
            set_options = ["Single Set", "Best of 3", "Best of 5"]

            for mt in ["Singles", "Doubles", "Mixed Doubles"]:
                st.markdown(f"--- \n**{mt}**")
                cols = st.columns([1, 1, 1, 2])
                mt_config = current_match_settings.get(mt, {"enabled": False, "win_points": 0, "loss_points": 0, "min_sets": "Best of 3"})
                enabled = cols[0].checkbox("Enabled", value=mt_config.get("enabled", False), key=f"en_edit_{mt}")
                win_points = cols[1].number_input("Win Pts", value=mt_config.get("win_points", 0), min_value=0, key=f"wp_edit_{mt}")
                loss_points = cols[2].number_input("Loss Pts", value=mt_config.get("loss_points", 0), min_value=0, key=f"lp_edit_{mt}")
                
                try:
                    set_index = set_options.index(mt_config.get("min_sets", "Best of 3"))
                except ValueError:
                    set_index = 1 # Default to "Best of 3"
                
                min_sets = cols[3].selectbox("Min Sets", options=set_options, index=set_index, key=f"ms_edit_{mt}")
                
                match_type_settings[mt] = {
                    "enabled": enabled,
                    "win_points": win_points,
                    "loss_points": loss_points,
                    "min_sets": min_sets
                }

            img_req = st.checkbox("Require Match Photo Evidence?", value=st.session_state.chapter_config.get("match_image_required", True))

            if st.form_submit_button("Save Settings"):
                # Ensure at least one ranking system is enabled
                if not any(ranking_systems.values()):
                    st.error("At least one Ranking System must be enabled.")
                else:
                    st.session_state.chapter_config['ranking_systems'] = ranking_systems
                    st.session_state.chapter_config['match_type_settings'] = match_type_settings
                    st.session_state.chapter_config['match_image_required'] = img_req
                    save_chapter_config(st.session_state.current_chapter['id'], st.session_state.chapter_config)
                    st.success("Settings saved successfully!")
                    st.rerun()
        
        st.subheader("Branding")
        ut = st.file_uploader("Chapter Title Graphic", type=["png", "jpg"])
        if ut and st.button("Upload Graphic"):
            path = save_remote_image(ut, f"title_{st.session_state.current_chapter['id']}", "title")
            conn = get_connection()
            with conn.cursor() as cur:
                 cur.execute("UPDATE chapters SET title_image_url = %s WHERE id = %s", (path, st.session_state.current_chapter['id']))
            conn.commit()
            conn.close()
            st.success("Updated"); st.rerun()
        
        
        st.subheader("Player Management")
        with st.expander("Manage player roles and passwords", expanded=True):
            if not st.session_state.players_df.empty:
                # Password Reset
                st.markdown("#### Generate New Password")
                players = st.session_state.players_df["name"].tolist()
                selected_player = st.selectbox("Select Player", players, key="player_select_for_password")
                if st.button("Generate New Password"):
                    new_password = str(uuid.uuid4().hex)[:8]
                    if update_player_password(selected_player, new_password):
                        st.success(f"New password for {selected_player}: `{new_password}`")
                        load_players() # Refresh player data
                    else:
                        st.error("Failed to update password.")
                st.divider()

                # Display all player passwords for admin
                st.markdown("#### Current Player Passwords")
                for i, r in st.session_state.players_df.iterrows():
                    st.code(f"{r['name']}: {r['password']}")
                st.divider()

                # Manage Player Roles
                st.markdown("#### Manage Player Roles")
                for idx, player in st.session_state.players_df.iterrows():
                    is_player_admin = player.get('is_admin', False)
                    new_status = st.toggle(f"Promote {player['name']} to Admin", value=is_player_admin, key=f"admin_toggle_{player['name']}")
                    
                    if new_status != is_player_admin:
                        st.session_state.players_df.loc[idx, 'is_admin'] = new_status
                        save_players(st.session_state.players_df)
                        st.success(f"{player['name']}'s admin status updated.")
                        st.rerun()
            else:
                st.info("No players to manage yet.")


if st.button("Switch Chapter" if not st.session_state.is_master_admin else "Return Master"):
    st.session_state.current_chapter = None
    st.session_state.chapter_config = {}
    if not st.session_state.is_master_admin: st.session_state.is_admin = False
    st.session_state.can_write = False
    st.session_state.temp_selected_chapter = None
    st.query_params.clear()
    st.rerun()
st.markdown("----")
st.info("Cloud Version running with Neon (PostgreSQL) & GitHub.")
render_footer()
