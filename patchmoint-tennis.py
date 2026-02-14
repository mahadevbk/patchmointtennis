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
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module='pandas')

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
            queries = [
                "CREATE TABLE IF NOT EXISTS chapters (id TEXT PRIMARY KEY, name TEXT UNIQUE, admin_password TEXT, created_at TEXT, config TEXT, sport TEXT, title_image_url TEXT, last_active_date TEXT)",
                "CREATE TABLE IF NOT EXISTS players (name TEXT, profile_image_url TEXT, birthday TEXT, chapter_id TEXT, password TEXT, gender TEXT, is_admin BOOLEAN DEFAULT FALSE, initial_utr NUMERIC DEFAULT NULL)",
                "CREATE TABLE IF NOT EXISTS matches (match_id TEXT PRIMARY KEY, date TEXT, match_type TEXT, team1_player1 TEXT, team1_player2 TEXT, team2_player1 TEXT, team2_player2 TEXT, set1 TEXT, set2 TEXT, set3 TEXT, winner TEXT, match_image_url TEXT, chapter_id TEXT)",
                "CREATE TABLE IF NOT EXISTS bookings (booking_id TEXT PRIMARY KEY, date TEXT, time TEXT, match_type TEXT, court_name TEXT, player1 TEXT, player2 TEXT, player3 TEXT, player4 TEXT, standby_player TEXT, screenshot_url TEXT, chapter_id TEXT)",
                "CREATE TABLE IF NOT EXISTS courts (chapter_id TEXT, name TEXT, url TEXT)"
            ]
            for q in queries:
                cur.execute(q)
            conn.commit()

            # 2. Run Migrations (Add columns if they are missing from existing tables)
            # using IF NOT EXISTS which is supported in Neon/Postgres
            migrations = [
                "ALTER TABLE players ADD COLUMN IF NOT EXISTS initial_utr NUMERIC DEFAULT NULL",
                "ALTER TABLE players ADD COLUMN IF NOT EXISTS is_admin BOOLEAN DEFAULT FALSE",
                "ALTER TABLE chapters ADD COLUMN IF NOT EXISTS sport TEXT DEFAULT 'Tennis'",
                "ALTER TABLE chapters ADD COLUMN IF NOT EXISTS last_active_date TEXT DEFAULT ''",
                "ALTER TABLE chapters ADD COLUMN IF NOT EXISTS title_image_url TEXT DEFAULT ''"
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

st.markdown("""
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Turret+Road:wght@200;300;400;500;700;800&display=swap" rel="stylesheet">
<style>
html, body, [class*="st-"], .stApp, h1, h2, h3, h4, h5, h6 {
    font-family: 'Turret Road', sans-serif !important;
}
[data-testid="stMetricLabel"] {
    color: #000000 !important;
}
[data-testid="stMetricValue"] {
    color: #000000 !important;
}
[data-testid="stMetricDelta"] {
    color: #1a1a1a !important; 
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
  background: linear-gradient(to bottom, #0b0c1f, #01010f);
}
@media print {
  html, body { -webkit-print-color-adjust: exact !important; print-color-adjust: exact !important; }
  body { background-color: #041136 !important; height: 100vh; margin: 0; padding: 0; }
  header, .stToolbar { display: none; }
}
[data-testid="stHeader"] { background-color: #041136 !important; }
.profile-image {
    width: 80px; height: 80px; object-fit: cover; border: 2px solid #fff500;
    border-radius: 15px; margin-right: 15px; vertical-align: middle;
    transition: transform 0.2s; box-shadow: 0 4px 8px rgba(0, 0, 0, 0.4), 0 0 15px rgba(255, 245, 0, 0.6);
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
    background: linear-gradient(135deg, rgba(255, 255, 255, 0.05) 0%, rgba(255, 255, 255, 0.01) 100%);
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
    background: rgba(255,255,255,0.05); padding: 15px; border-radius: 10px; 
    border-left: 4px solid #fff500; margin-bottom: 10px;
}
.stat-label { font-size: 0.7em; color: #aaa; text-transform: uppercase; }
.metric-value { font-size: 1.1em; font-weight: bold; }
.stat-highlight { color: #fff500; }
[data-testid="stMetric"] > div:nth-of-type(1) { color: #FF7518 !important; }
.block-container { display: flex; flex-wrap: wrap; justify-content: center; }
[data-testid="stHorizontalBlock"] { flex: 1 1 100% !important; margin: 10px 0; }
.chapter-card {
    background: #222222;
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
    background-color: rgba(255, 255, 255, 0.05);
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
}
.enter-button {
    background-color: #fff500;
    color: #031827;
    padding: 8px 16px;
    border-radius: 5px;
    text-decoration: none;
    font-weight: bold;
    display: block;
    margin-top: auto;
    transition: background-color 0.2s;
    width: 100%;
    box-sizing: border-box;
}
.enter-button:hover {
    background-color: #ffd700;
}
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
    cid = st.session_state.current_chapter['id']
    if cid and not df.empty:
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM matches WHERE chapter_id = %s", (cid,))
                df_clean = df.where(pd.notnull(df), None)
                records = [tuple(x) for x in df_clean.to_numpy()]
                cols = ",".join(list(df.columns))
                if records:
                    query = f"INSERT INTO matches ({cols}) VALUES %s"
                    execute_values(cur, query, records)
                
                # NEW: Update last_active_date for the chapter
                cur.execute("UPDATE chapters SET last_active_date = %s WHERE id = %s", (datetime.now().isoformat(), cid))
            conn.commit()
        except Exception as e:
            st.error(f"Save matches error: {e}")
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
        "ranking_systems": ["Elo (Hybrid)"],
        "match_types": ["Doubles", "Singles"],
        "sets_modes": {"Singles": "Best of 3", "Doubles": "Best of 3", "Mixed Doubles": "Best of 3"},
        "points_win": 3, "points_loss": 1,
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
            if "sets_modes" not in conf:
                old = conf.get("sets_mode", "Best of 3")
                conf["sets_modes"] = {"Singles": old, "Doubles": old, "Mixed Doubles": old}
            if "ranking_systems" not in conf:
                conf["ranking_systems"] = [conf.get("ranking_system", "Elo (Hybrid)")]
            if "match_image_required" not in conf:
                conf["match_image_required"] = True
            return conf
    except: pass
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
    st.markdown('<div style="text-align: center; margin-top: 20px; margin-bottom: 20px; color: #888; font-size: 0.8em;">Patch Moint League system is free and Open source. Hosted on GitHub and Powered by Streamlit.</div>', unsafe_allow_html=True)

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
    return {'wins': 0, 'losses': 0, 'matches': 0, 'games_won': 0, 'gd_sum': 0, 'clutch_wins': 0, 'clutch_matches': 0, 'gd_list': [], 'points': 0}

@st.cache_data(show_spinner=False)
def calculate_rankings(matches_to_rank):
    stats = defaultdict(get_player_stats_template)
    current_streaks = defaultdict(int)
    last_active_dates = {}
    elo_ratings = {} # Initialize as a regular dict first
    utr_ratings = {} # Initialize UTR ratings
    last_elo_changes = defaultdict(float) 
    K_FACTOR = 32 
    
    # UTR Constants
    UTR_DEFAULT_RATING = 4.0
    UTR_K_FACTOR = 0.05 # How much UTR changes per match (smaller for slower change)
    UTR_SCALE = 3.0   # Factor to convert UTR difference to expected game win percentage
    UTR_MIN = 1.0
    UTR_MAX = 16.5

    players_df = st.session_state.players_df
    config = st.session_state.chapter_config
    ranking_systems = config.get("ranking_systems", ["Elo (Hybrid)"])
    pts_win = config.get("points_win", 3)
    pts_loss = config.get("points_loss", 1)

    # Initialize ELO and UTR ratings for all players based on initial_utr or default
    for _, player_row in players_df.iterrows():
        player_name = player_row['name']
        initial_utr = player_row.get('initial_utr')
        if pd.notna(initial_utr) and initial_utr is not None:
            starting_elo = (initial_utr - 4.0) * 110.0 + 1200.0 # Inverse of original UTR mapping
            elo_ratings[player_name] = float(starting_elo)
            utr_ratings[player_name] = float(initial_utr)
        else:
            elo_ratings[player_name] = 1200.0 # Default ELO
            utr_ratings[player_name] = UTR_DEFAULT_RATING # Default UTR

    # Convert to defaultdict after initial population
    elo_ratings = defaultdict(lambda: 1200.0, elo_ratings) 
    utr_ratings = defaultdict(lambda: UTR_DEFAULT_RATING, utr_ratings)

    if not matches_to_rank.empty: 
        matches_to_rank = matches_to_rank.sort_values('date')

    for row in matches_to_rank.itertuples(index=False):
        t1 = [p for p in [row.team1_player1, row.team1_player2] if p and str(p).strip() and str(p).upper() != "VISITOR"]
        t2 = [p for p in [row.team2_player1, row.team2_player2] if p and str(p).strip() and str(p).upper() != "VISITOR"]
        if not t1 or not t2: continue

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
        if total_match_games == 0: continue # Avoid division by zero

        t1_elo_avg = sum(elo_ratings[p] for p in t1) / len(t1)
        t2_elo_avg = sum(elo_ratings[p] for p in t2) / len(t2)
        t1_utr_avg = sum(utr_ratings[p] for p in t1) / len(t1)
        t2_utr_avg = sum(utr_ratings[p] for p in t2) / len(t2)

        t1_won = row.winner == "Team 1"

        # ELO Calculation
        def update_elo(players, own_elo_avg, opp_elo_avg, actual_score, is_winner):
            expected = 1 / (1 + 10 ** ((opp_elo_avg - own_elo_avg) / 400))
            elo_change = K_FACTOR * (actual_score - expected)
            for p in players:
                elo_ratings[p] += elo_change
                last_elo_changes[p] = round(elo_change, 1)
        
        # UTR Calculation
        def update_utr(players, own_utr_avg, opp_utr_avg, actual_gwp):
            utr_diff = own_utr_avg - opp_utr_avg
            expected_gwp = 1 / (1 + np.exp(-utr_diff / UTR_SCALE)) # Logistic function
            utr_change = UTR_K_FACTOR * (actual_gwp - expected_gwp)

            for p in players:
                utr_ratings[p] += utr_change
                utr_ratings[p] = max(UTR_MIN, min(UTR_MAX, utr_ratings[p])) # Clamp UTR
        
        # Update stats common to both ELO and UTR
        def update_common_stats(players, games_won, total_games, is_winner):
            for p in players:
                stats[p]['matches'] += 1
                stats[p]['games_won'] += games_won
                stats[p]['gd_sum'] += (games_won - (total_games - games_won))
                stats[p]['gd_list'].append(games_won - (total_games - games_won))
                if is_clutch: stats[p]['clutch_matches'] += 1
                if is_winner:
                    stats[p]['wins'] += 1
                    if is_clutch: stats[p]['clutch_wins'] += 1
                    current_streaks[p] = max(0, current_streaks[p]) + 1
                    stats[p]['points'] += pts_win
                else:
                    stats[p]['losses'] += 1
                    current_streaks[p] = min(0, current_streaks[p]) - 1
                    stats[p]['points'] += pts_loss

        # Apply updates
        if t1_won:
            update_common_stats(t1, t1_total_games, total_match_games, True)
            update_common_stats(t2, t2_total_games, total_match_games, False)
            
            update_elo(t1, t1_elo_avg, t2_elo_avg, 1.0, True) # Elo uses win/loss (1.0 for win)
            update_elo(t2, t2_elo_avg, t1_elo_avg, 0.0, False) # (0.0 for loss)

            update_utr(t1, t1_utr_avg, t2_utr_avg, t1_total_games / total_match_games)
            update_utr(t2, t2_utr_avg, t1_utr_avg, t2_total_games / total_match_games)
        else: # Team 2 won
            update_common_stats(t1, t1_total_games, total_match_games, False)
            update_common_stats(t2, t2_total_games, total_match_games, True)

            update_elo(t1, t1_elo_avg, t2_elo_avg, 0.0, False) # Elo uses win/loss (0.0 for loss)
            update_elo(t2, t2_elo_avg, t1_elo_avg, 1.0, True) # (1.0 for win)
            
            update_utr(t1, t1_utr_avg, t2_utr_avg, t1_total_games / total_match_games)
            update_utr(t2, t2_utr_avg, t1_utr_avg, t2_total_games / total_match_games)

    rank_data = []
    for p, s in stats.items():
        m_played = s['matches']
        if m_played == 0: continue
        clutch_pct = (s['clutch_wins'] / s['clutch_matches'] * 100) if s['clutch_matches'] > 0 else 0
        consistency = np.std(s['gd_list']) if s['gd_list'] else 0
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
        current_utr = round(utr_ratings[p], 2) # Use the newly calculated UTR

        rank_data.append({
            "Player": p, "Score": score_elo, "Label": "Elo", "Elo": score_elo, 
            "Score_Elo (Hybrid):": score_elo, "Score_Points": s['points'], 
            "Score_UTR": current_utr, "Last Change": last_elo_changes.get(p, 0),
            "Wins": s['wins'], "Losses": s['losses'], "Games Won": s['games_won'],
            "Win %": round((s['wins']/m_played)*100, 1), "Matches": m_played, 
            "Game Diff Avg": round(s['gd_sum']/m_played, 2), "Clutch Factor": round(clutch_pct, 1), 
            "Consistency Index": round(consistency, 2), "Last Active": l_date if l_date else "N/A",
            "Badges": badges, 
            "Profile": pd.Series(players_df.profile_image_url.values, index=players_df.name).to_dict().get(p, "")
        })
        
    df = pd.DataFrame(rank_data)
    if not df.empty:
        df = df.sort_values(by=["Score_Elo (Hybrid)", "Win %"], ascending=[False, False])
        df["Rank_Elo (Hybrid)"] = range(1, len(df) + 1)
        df = df.sort_values(by=["Score_Points", "Win %"], ascending=[False, False])
        df["Rank_Points"] = range(1, len(df) + 1)
        df = df.sort_values(by=["Score_UTR", "Win %"], ascending=[False, False])
        df["Rank_UTR"] = range(1, len(df) + 1)
        
        df = df.sort_values(by=["Score_Elo (Hybrid)", "Win %"], ascending=[False, False]).reset_index(drop=True)
        df["Rank"] = [f"🏆 {i+1}" for i in df.index]
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
            r_sys = st.multiselect("Ranking Systems", ["Elo (Hybrid)", "Points", "UTR"], default=["Elo (Hybrid)"])
            if not r_sys: r_sys = ["Elo (Hybrid)"]
            c1, c2 = st.columns(2)
            p_win = c1.number_input("Points per Win", value=3, min_value=1)
            p_loss = c2.number_input("Points per Loss", value=1, min_value=0)
            m_types = st.multiselect("Allowed Match Types", ["Singles", "Doubles", "Mixed Doubles"], default=["Doubles", "Singles"])
            c1, c2, c3 = st.columns(3)
            sm_s = c1.selectbox("Singles Sets", ["Single Set", "Best of 3", "Best of 5"], index=1)
            sm_d = c2.selectbox("Doubles Sets", ["Single Set", "Best of 3", "Best of 5"], index=1)
            sm_m = c3.selectbox("Mixed Sets", ["Single Set", "Best of 3", "Best of 5"], index=1)
            if st.form_submit_button("Save Settings & Enter Chapter", type="primary"):
                new_conf = {"ranking_systems": r_sys, "match_types": m_types, "sets_modes": {"Singles": sm_s, "Doubles": sm_d, "Mixed Doubles": sm_m}, "points_win": p_win, "points_loss": p_loss}
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
        
        st.write("Welcome! Select an active chapter or create a new one.")
        st.caption("Free and Open Source • Create your league and push yourself to get better.")

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
                chap_df = chap_df[chap_df['sport'] == SPORT_TYPE]
            else:
                if SPORT_TYPE != "Tennis":
                    chap_df = pd.DataFrame()

            if not chap_df.empty:
                st.subheader("Active Chapters")
                cols = st.columns(3)
                for idx, row in chap_df.iterrows():
                    with cols[idx % 3]:
                        img_container_content = ''
                        if row.get("title_image_url"):
                            img_src = get_img_src(row.get("title_image_url"))
                            img_container_content = f'<img src="{img_src}">'
                        
                        img_html = (
                            '<div class="card-image-container">'
                            f'{img_container_content}'
                            '</div>'
                        )
                        title_html = f'<h3>{row["name"]}</h3>'
                        num_players = player_counts.get(row['id'], 0)
                        num_matches = match_counts.get(row['id'], 0)
                        stats_html = f'<p style="margin: 10px 0; color: #aaa; font-size: 0.9em;">{num_players} players / {num_matches} games</p>'
                        
                        # Use HTML for the top part, but native Streamlit button for selection to avoid page reload
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
                        if st.button("Enter", key=f"ent_{row['id']}", use_container_width=True):
                            st.session_state.temp_selected_chapter = row.to_dict()
                            st.rerun()
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
            if st.button("Create Chapter"):
                if new_chap_name:
                    if not chap_df.empty and new_chap_name in chap_df['name'].values:
                        st.error("Name exists")
                    else:
                        nid = str(uuid.uuid4()); npass = str(uuid.uuid4().hex)[:8]
                        conn = get_connection()
                        try:
                            # Try insert, assuming init_db fixed columns
                            with conn.cursor() as cur:
                                cur.execute("INSERT INTO chapters (id, name, admin_password, created_at, config, sport, last_active_date) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                                            (nid, new_chap_name, npass, datetime.now().isoformat(), json.dumps(get_default_config()), SPORT_TYPE, datetime.now().isoformat()))
                            conn.commit()
                            conn.close()
                            st.session_state.new_chapter_created = {'name': new_chap_name, 'id': nid, 'password': npass}
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error creating chapter: {e}")
                            st.warning("If this persists, please refresh the page to ensure database migrations have run.")
        
        with st.expander("Master Admin Login", expanded=False, icon="➡️"):
            m_pass = st.text_input("Master Password", type="password", key="ma_pass")
            if st.button("Login Master") and m_pass == st.secrets.get("madminpwd", "magic1"):
                st.session_state.is_master_admin = True; st.rerun()
    render_footer()
    st.stop()

# --- MASTER ADMIN DASHBOARD ---
if st.session_state.is_master_admin and st.session_state.current_chapter is None:
    st.title("🛡️ Master Admin Dashboard")
    if st.button("Logout Master Admin"): st.session_state.is_master_admin = False; st.rerun()
    
    conn = get_connection()
    try:
        chapters = pd.read_sql("SELECT * FROM chapters", conn)
    except:
        chapters = pd.DataFrame()
    conn.close()
    
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Chapters", len(chapters))
    c2.metric("Total Players", "Unknown") 
    c3.metric("Total Matches", "Unknown") 

    st.subheader("All Chapters")
    st.dataframe(chapters) # Debug view
    
    for idx, row in chapters.iterrows():
        with st.container(border=True):
            col_info, col_act = st.columns([3, 2])
            with col_info:
                st.markdown(f"### {row['name']} ({row.get('sport', 'Legacy')})")
                st.caption(f"ID: {row['id']} | Pass: `{row['admin_password']}`")
            with col_act:
                if st.button(f"Enter Admin", key=f"ma_ent_{row['id']}"):
                    st.session_state.current_chapter = {'id': row['id'], 'name': row['name']}
                    st.session_state.chapter_config = load_chapter_config(row['id'])
                    st.session_state.is_admin = True; st.session_state.can_write = True; st.rerun()
                if st.button(f"DELETE", key=f"ma_del_{row['id']}", type="primary"):
                    delete_chapter_fully(row['id']); st.rerun()
            with st.expander(f"Manage Passwords {row['name']}", expanded=False, icon="➡️"):
                npw = st.text_input("New Admin Pass", key=f"nap_{row['id']}")
                if st.button("Reset Admin", key=f"rap_{row['id']}"): update_chapter_admin_password(row['id'], npw)
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
    active_systems = conf.get("ranking_systems", ["Elo (Hybrid)"])
    view_system = st.radio("Ranking System", active_systems, horizontal=True) if len(active_systems) > 1 else active_systems[0]
    
    # --- ADDED: Ranking System Explanations ---
    pts_win = conf.get("points_win", 3)
    pts_loss = conf.get("points_loss", 1)

    ranking_descriptions = {
        "Elo (Hybrid)": {
            "desc": "A dynamic rating system that adjusts based on the quality of your opponent. It calculates relative skill levels in zero-sum games. This hybrid version rewards Game Difference, meaning a 6-0 win is worth more than a 7-6 win.",
            "scenario": "Best for competitive leagues with varying skill levels. It heavily penalizes high-ranked players for losing to lower-ranked ones and provides a fair mathematical assessment of win probability."
        },
        "Points": {
            "desc": f"A cumulative accumulation system. You get **{pts_win}** points for every win and **{pts_loss}** point(s) for every loss. Your rating never decreases, it only grows with activity.",
            "scenario": "Ideal for social leagues or seasonal activity drives. It rewards the 'Grinder' who plays the most matches, regardless of who they play against."
        },
        "UTR": {
            "desc": "Universal Tennis Rating simulation. It measures your skill on a specific scale based on game reliability against the opponent's rating.",
            "scenario": "The standard for technical assessment. Use this to find practice partners of exactly equal skill level, as it focuses on game score margins rather than just wins/losses."
        }
    }
    
    current_desc = ranking_descriptions.get(view_system, {"desc": "Custom ranking system.", "scenario": "General usage."})
    
    with st.expander(f"About {view_system}", expanded=False, icon="ℹ️"):
        st.markdown(f"**How it works:** {current_desc['desc']}")
        st.markdown(f"**Best for:** *{current_desc['scenario']}*")
    # ------------------------------------------

    ranking_view = st.radio("View", ["Combined", "Doubles", "Singles", "Table View"], horizontal=True)
    display_rank_df = rank_df.copy() if not rank_df.empty else pd.DataFrame()

    if not st.session_state.matches_df.empty:
        if ranking_view == "Doubles": display_rank_df = calculate_rankings(st.session_state.matches_df[st.session_state.matches_df.match_type.isin(["Doubles", "Mixed Doubles"])])
        elif ranking_view == "Singles": display_rank_df = calculate_rankings(st.session_state.matches_df[st.session_state.matches_df.match_type == "Singles"])

    if display_rank_df.empty: st.info("No matches.")
    else:
        sys_key, rank_key = f"Score_{view_system}", f"Rank_{view_system}"
        if sys_key in display_rank_df.columns:
            display_rank_df = display_rank_df.sort_values(by=[sys_key, "Win %"], ascending=[False, False]).reset_index(drop=True)
            display_rank_df['Rank'] = [f"🏆 {i+1}" for i in display_rank_df.index]
            display_rank_df['Score'] = display_rank_df[sys_key]; display_rank_df['Label'] = view_system

        if ranking_view == "Table View":
            cols = ['Rank', 'Profile', 'Player', 'Score', 'Label', 'Win %', 'Matches', 'Game Diff Avg']
            st.dataframe(display_rank_df[cols], hide_index=True, width='stretch', column_config={"Profile": st.column_config.ImageColumn("PIC"), "Win %": st.column_config.ProgressColumn(format="%.1f%%", min_value=0, max_value=100)})
        else:
            if len(display_rank_df) >= 3:
                top3 = display_rank_df.head(3).to_dict('records')
                podium = [{"p": top3[1], "m": "35px", "c": "#C0C0C0"}, {"p": top3[0], "m": "0px", "c": "#FFD700"}, {"p": top3[2], "m": "45px", "c": "#CD7F32"}]
                html = ""
                for i in podium:
                    p = i["p"]; ch = p.get('Last Change', 0); cc = "#00ff88" if ch >= 0 else "#ff4b4b"; ct = f"({'+' if ch > 0 else ''}{ch})" if p.get('Label') != 'Points' else ""
                    html += f"""<div style="flex:1; margin-top:{i['m']}; text-align:center; background:rgba(255,255,255,0.05); border-radius:12px; border:1px solid {i['c']}; padding:8px;"><div style="color:{i['c']}; font-weight:bold;">{p['Rank']}</div><img src="{get_img_src(p['Profile'])}" style="width:60px; height:60px; border-radius:50%; object-fit:cover; border:2px solid {i['c']};"><div style="color:#fff500; font-size:0.8em; margin-top:5px;">{p['Player']}</div><div style="color:white; font-weight:bold;">{p['Score']:.1f}</div><div style="color:{cc}; font-size:10px;">{ct}</div></div>"""
                st.markdown(f'<div style="display:flex; gap:8px; margin-bottom:30px;">{html}</div>', unsafe_allow_html=True)

            for idx, row in display_rank_df.iterrows():
                with st.container(border=True):
                    c1, c2, c3 = st.columns([1.2, 2, 2])
                    ch = row.get('Last Change', 0); cc = "#00ff88" if ch >= 0 else "#ff4b4b"; cd = f"<span style='color:{cc};'>({'+' if ch > 0 else ''}{ch})</span>" if row['Label'] != 'Points' else ""
                    badges = "".join([f"<span class='badge'>{b}</span>" for b in row.get('Badges', [])])
                    with c1: st.markdown(f"""<div style="text-align:center;"><img src="{get_img_src(row['Profile'])}" style="width:80px; height:80px; border-radius:50%; border:3px solid #CCFF00; object-fit:cover; margin-bottom:10px;"><div style="font-size:1.5em; font-weight:bold; color:#CCFF00;">{row['Rank']}</div><div style="font-size:1.2em; font-weight:bold; color:white;">{row['Player']}</div><div style="font-size:0.8em; color:#aaa;">{row['Label']}: {row['Score']:.1f} {cd}</div><div>{badges}</div></div>""", unsafe_allow_html=True)
                    with c2: st.markdown(f"""<div style="display:grid; grid-template-columns:1fr 1fr; gap:10px; text-align:center;"><div style="background:rgba(255,255,255,0.05); padding:8px; border-radius:8px;"><div class="stat-label">Win %</div><div style="color:#00ff88;">{row['Win %']}%</div></div><div style="background:rgba(255,255,255,0.05); padding:8px; border-radius:8px;"><div class="stat-label">Record</div><div>{row['Wins']}W - {row['Losses']}L</div></div><div style="background:rgba(255,255,255,0.05); padding:8px; border-radius:8px;"><div class="stat-label">GDA</div><div>{row.get('Game Diff Avg', 0):+.2f}</div></div><div style="background:rgba(255,255,255,0.05); padding:8px; border-radius:8px;"><div class="stat-label">Clutch</div><div>{row.get('Clutch Factor', 0)}%</div></div></div>""", unsafe_allow_html=True)
                    with c3: st.plotly_chart(create_radar_chart(row), width=300, config={'displayModeBar': False}, key=f"rd_{idx}")
                    with st.expander("📈 Trend", expanded=False, icon="➡️"): st.plotly_chart(plot_player_performance(row['Player'], st.session_state.matches_df), width=600, key=f"tr_{idx}")

with tabs[1]:
    st.header("Matches")
    config = st.session_state.chapter_config
    is_img_required = config.get("match_image_required", True)
    if st.session_state.can_write:
        with st.expander("➕ Post Result", expanded=False, icon="➡️"):
            if st.session_state.players_df.empty: st.warning("Add players first.")
            else:
                pk = st.session_state.match_post_key
                pnames = sorted([p for p in st.session_state.players_df["name"].dropna().tolist() if p != "Visitor"])
                
                # --- START OF CHANGE ---
                allowed_raw = config.get("match_types", ["Doubles", "Singles"])
                ui_opts = []
                if "Doubles" in allowed_raw or "Mixed Doubles" in allowed_raw: ui_opts.append("Doubles")
                if "Singles" in allowed_raw: ui_opts.append("Singles")
                if not ui_opts: ui_opts = ["Doubles", "Singles"]
                
                mt = st.radio("Type", ui_opts, horizontal=True, key=f"mt_{pk}")
                # --- END OF CHANGE ---

                md = st.date_input("Date", datetime.now(), key=f"md_{pk}")
                c1, c2 = st.columns(2)
                if mt == "Doubles":
                    opts = [""] + pnames + ["Visitor"]
                    t1p1 = c1.selectbox("T1 P1", opts, key=f"1_{pk}"); t1p2 = c1.selectbox("T1 P2", opts, key=f"2_{pk}")
                    t2p1 = c2.selectbox("T2 P1", opts, key=f"3_{pk}"); t2p2 = c2.selectbox("T2 P2", opts, key=f"4_{pk}")
                else:
                    opts = [""] + pnames
                    t1p1 = c1.selectbox("P1", opts, key=f"1s_{pk}"); t2p1 = c2.selectbox("P2", opts, key=f"2s_{pk}")
                    t1p2, t2p2 = "", ""
                
                sc1, sc2, sc3 = st.columns(3)
                s_list = [""] + get_valid_scores()
                s1 = sc1.selectbox("Set 1", s_list, key=f"s1_{pk}"); s2 = sc2.selectbox("Set 2", s_list, key=f"s2_{pk}"); s3 = sc3.selectbox("Set 3", s_list, key=f"s3_{pk}")
                win = st.radio("Winner", ["Team 1", "Team 2"], horizontal=True, key=f"w_{pk}")
                img = st.file_uploader("Photo", type=["jpg", "png"], key=f"im_{pk}")
                if not is_img_required:
                    st.caption("Photo is optional for this chapter.")

                if st.button("Post Match", key=f"bp_{pk}"):
                    if s1 and (img or not is_img_required):
                        mid = generate_match_id(st.session_state.matches_df, datetime.combine(md, datetime.min.time()))
                        path = save_remote_image(img, mid, "match") if img else ""
                        
                        final_mt = mt
                        if mt == "Doubles":
                            # Auto-detect Mixed Doubles
                            def get_gender_val(pname):
                                if not pname or pname == "Visitor": return None
                                try:
                                    g_row = st.session_state.players_df[st.session_state.players_df['name'] == pname]
                                    if not g_row.empty:
                                        g = g_row.iloc[0]['gender']
                                        return str(g).lower().strip() if g else None
                                except: return None
                                return None

                            g1, g2 = get_gender_val(t1p1), get_gender_val(t1p2)
                            g3, g4 = get_gender_val(t2p1), get_gender_val(t2p2)

                            def is_pair_mixed(ga, gb):
                                if not ga or not gb: return False
                                s = {ga, gb}
                                return "male" in s and "female" in s
                            
                            if is_pair_mixed(g1, g2) and is_pair_mixed(g3, g4):
                                final_mt = "Mixed Doubles"

                        new_row = {"match_id": mid, "date": md.strftime('%Y-%m-%d'), "match_type": final_mt, "team1_player1": t1p1, "team1_player2": t1p2, "team2_player1": t2p1, "team2_player2": t2p2, "set1": s1, "set2": s2, "set3": s3, "winner": win, "match_image_url": path, "chapter_id": st.session_state.current_chapter['id']}
                        st.session_state.matches_df = pd.concat([st.session_state.matches_df, pd.DataFrame([new_row])], ignore_index=True)
                        save_matches(st.session_state.matches_df)
                        st.session_state.match_post_key += 1
                        st.success(f"Saved as {final_mt}"); st.rerun()
                    else: st.error("Score & Photo required" if is_img_required else "Score required")

    m_hist = st.session_state.matches_df.copy()
    if not m_hist.empty:
        m_hist['date'] = pd.to_datetime(m_hist['date']); m_hist = m_hist.sort_values('date', ascending=False)
        for row in m_hist.itertuples():
            t1 = f"{row.team1_player1}/{row.team1_player2}" if row.team1_player2 else row.team1_player1
            t2 = f"{row.team2_player1}/{row.team2_player2}" if row.team2_player2 else row.team2_player1
            scores = " | ".join([s for s in [row.set1, row.set2, row.set3] if s])
            img_h = f'<div style="display:flex; justify-content:center;"><img src="{get_img_src(row.match_image_url)}" style="max-height:400px; width:100%; object-fit:contain;"></div>' if row.match_image_url else ""
            st.markdown(f"""<div style="background:rgba(255,255,255,0.05); border-radius:12px; margin-bottom:20px; border:1px solid rgba(255,255,255,0.1); overflow:hidden;">{img_h}<div style="padding:15px; text-align:center;"><div style="color:#888;">{row.date.strftime('%d %b %Y')}</div><div style="font-size:1.1em; margin:5px 0;">{t1} vs {t2}</div><div style="font-size:0.9em; color:#CCFF00; margin-bottom:5px; font-weight:bold; letter-spacing:1px; text-transform:uppercase;">{row.match_type}</div><div style="color:#FF7518; font-weight:bold;">{scores}</div><div style="margin-top:5px; font-weight:bold; color:#fff500;">Winner: {row.winner}</div></div></div>""", unsafe_allow_html=True)
            can_edit_match = False
            if st.session_state.is_admin or st.session_state.is_master_admin:
                can_edit_match = True
            elif st.session_state.get('logged_in_player'):
                player_name = st.session_state.logged_in_player
                if player_name in [row.team1_player1, row.team1_player2, row.team2_player1, row.team2_player2]:
                    can_edit_match = True
            
            if can_edit_match:
                with st.expander(f"Edit {row.match_id}", expanded=False, icon="➡️"):
                    if st.button("Delete", key=f"del_{row.match_id}"): delete_match_from_db(row.match_id); st.rerun()

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
                sel = st.selectbox("Edit Player", st.session_state.players_df['name'].tolist())
                if sel:
                    row = st.session_state.players_df[st.session_state.players_df['name'] == sel].iloc[0]
                    ni = st.file_uploader("Image", type=["jpg", "png"], key="pu")
                    if st.button("Save Img"):
                        path = save_remote_image(ni, sel, "profile")
                        idx = st.session_state.players_df[st.session_state.players_df['name'] == sel].index[0]
                        st.session_state.players_df.at[idx, 'profile_image_url'] = path
                        save_players(st.session_state.players_df); st.rerun()
                    if st.button("Delete Player"): delete_player_from_db(sel); st.session_state.players_df = st.session_state.players_df[st.session_state.players_df['name'] != sel]; st.rerun()

    for idx, row in st.session_state.players_df.sort_values("name").iterrows():
        p_name = row['name']
        p_stats = rank_df[rank_df['Player'] == p_name] if not rank_df.empty else pd.DataFrame()
        has_stats = not p_stats.empty
        s = p_stats.iloc[0] if has_stats else {}

        with st.container():
            c1, c2, c3 = st.columns([1.2, 2, 2]) # Keep the 3-column layout

            with c1:
                img_src = get_img_src(row['profile_image_url']) # Uses remote URL
                st.markdown(f"""
                    <div style="text-align: center;">
                        <div style="
                            width: 120px; 
                            height: 120px; 
                            background-color: #262626; 
                            border-radius: 15px; 
                            border: 3px solid #fff500; 
                            display: flex; 
                            justify-content: center; 
                            align-items: center; 
                            overflow: hidden; 
                            margin: 0 auto;
                        ">
                            <img src="{img_src}" style="
                                max-width: 100%; 
                                max-height: 100%; 
                                object-fit: contain;
                            ">
                        </div>
                        <div style="margin-top: 10px; font-weight: bold; font-size: 1.2em;">{p_name}</div>
                    </div>
                """, unsafe_allow_html=True)

            with c2:
                if has_stats:
                    badges_html = "".join([f"<span class='badge'>{b}</span>" for b in s.get('Badges', [])])
                    st.markdown(f"""
                    <div class="stat-box">
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px; border-bottom: 1px solid rgba(255,255,255,0.1); padding-bottom: 5px;">
                            <span style="color: #fff500; font-weight: bold; font-size: 1.1em;">Rank: {s.get('Rank', 'N/A')}</span>
                            <div>{badges_html}</div>
                        </div>
                        <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; text-align: center;">
                            <div><div class="metric-label">Games Won</div><div class="metric-value">{s.get('Games Won', 0)}</div></div>
                            <div><div class="metric-label">GD Avg</div><div class="metric-value">{s.get('Game Diff Avg', 0)}</div></div>
                            <div><div class="metric-label">Clutch</div><div class="metric-value">{s.get('Clutch Factor', 0)}%</div></div>
                            <div><div class="metric-label">Consistency</div><div class="metric-value">{s.get('Consistency Index', 0)}</div></div>
                            <div><div class="metric-label">Win %</div><div class="metric-value">{s.get('Win %', 0)}%</div></div>
                            <div><div class="metric-label">Record</div><div class="metric-value">{s.get('Wins', 0)}W-{s.get('Losses', 0)}L</div></div>
                            <div><div class="metric-label">Matches</div><div class="metric-value">{s.get('Matches', 0)}</div></div>
                            <div><div class="metric-label">Elo</div><div class="metric-value">{s.get('Elo', 0)}</div></div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                else: st.info("No stats")
            with c3: # Radar Chart (kept from patchmoint-tennis.py)
                if has_stats: 
                    st.plotly_chart(create_radar_chart(s), width=300, config={'displayModeBar': False}, key=f"rp_{idx}")
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
            rs = st.multiselect("Ranking Systems", ["Elo (Hybrid)", "Points", "UTR"], default=st.session_state.chapter_config.get("ranking_systems"))
            img_req = st.checkbox("Require Match Photo Evidence?", value=st.session_state.chapter_config.get("match_image_required", True))
            
            if st.form_submit_button("Save"):
                if not rs: rs = ["Elo (Hybrid)"]
                st.session_state.chapter_config['ranking_systems'] = rs
                st.session_state.chapter_config['match_image_required'] = img_req
                save_chapter_config(st.session_state.current_chapter['id'], st.session_state.chapter_config)
                st.success("Saved"); st.rerun()
        
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
        
        
        st.subheader("Passwords")
        for i, r in st.session_state.players_df.iterrows():
            st.code(f"{r['name']}: {r['password']}")

        st.subheader("Manage Player Roles")
        if not st.session_state.players_df.empty:
            for idx, player in st.session_state.players_df.iterrows():
                # Ensure 'is_admin' column exists and has boolean type
                is_player_admin = player.get('is_admin', False)
                new_status = st.toggle(f"Promote {player['name']} to Admin", value=is_player_admin, key=f"admin_toggle_{player['name']}")
                
                if new_status != is_player_admin:
                    # Update the dataframe
                    st.session_state.players_df.loc[idx, 'is_admin'] = new_status
                    # Save the updated dataframe to the database
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
