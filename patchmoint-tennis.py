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
<style>
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
  background: linear-gradient(to bottom, #041136, #21000a);
  background-attachment: scroll;
}
@media print {
  html, body { -webkit-print-color-adjust: exact !important; print-color-adjust: exact !important; }
  body { background: linear-gradient(to bottom, #21000a, #041136) !important; height: 100vh; margin: 0; padding: 0; }
  header, .stToolbar { display: none; }
}
[data-testid="stHeader"] { background: linear-gradient(to top, #041136 , #21000a) !important; }
.profile-image {
    width: 80px; height: 80px; object-fit: cover; border: 2px solid #fff500;
    border-radius: 15px; margin-right: 15px; vertical-align: middle;
    transition: transform 0.2s; box-shadow: 0 4px 8px rgba(0, 0, 0, 0.4), 0 0 15px rgba(255, 245, 0, 0.6);
}
.profile-image:hover { transform: scale(1.1); }
.birthday-banner {
    background: linear-gradient(45deg, #FFFF00, #EEE8AA); color: #950606; padding: 15px;
    border-radius: 10px; text-align: center; font-size: 1.2em; font-weight: bold;
    margin-bottom: 20px; box-shadow: 0 4px 15px rgba(0,0,0,0.2);
    display: flex; justify-content: center; align-items: center;
}
.whatsapp-share, .calendar-share {
    background-color: #25D366; color: white !important; padding: 5px 10px; border-radius: 5px; 
    text-decoration: none; font-weight: bold; display: inline-flex; align-items: center;
    font-size: 0.8em; border: none; cursor: pointer; margin-top: 5px;
}
.whatsapp-share img { width: 18px; vertical-align: middle; margin-right: 5px; filter: brightness(0) invert(1); }
.court-card {
    background: linear-gradient(to bottom, #031827, #07314f); border: 1px solid #fff500;
    border-radius: 10px; padding: 15px; margin: 10px 0; box-shadow: 0 4px 8px rgba(0, 0, 0, 0.2);
    transition: transform 0.2s, box-shadow 0.2s; text-align: center;
}
.court-card:hover { transform: scale(1.05); box-shadow: 0 6px 12px rgba(255, 245, 0, 0.3); }
.court-card h4 { color: #fff500; margin-bottom: 10px; }
.court-card a {
    background-color: #fff500; color: #031827; padding: 8px 16px; border-radius: 5px;
    text-decoration: none; font-weight: bold; display: inline-block; margin-top: 10px;
    transition: background-color 0.2s;
}
.court-card a:hover { background-color: #ffd700; }
@import url('https://fonts.googleapis.com/css2?family=Offside&display=swap');
html, body, [class*="st-"], h1, h2, h3, h4, h5, h6 { font-family: 'Offside', sans-serif !important; }
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
.badge { background: rgba(255, 215, 0, 0.2); color: #ffd700; padding: 2px 6px; border-radius: 4px; font-size: 0.6em; margin-right: 5px; border: 1px solid rgba(255, 215, 0, 0.4); vertical-align: middle; }
.stat-box { flex: 1; min-width: 100px; text-align: center; padding: 5px; }
.stat-label { font-size: 0.75em; color: #aaa; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 2px; }
.stat-value { font-size: 1.1em; color: #fff; font-weight: bold; }
.stat-highlight { color: #fff500; }
[data-testid="stMetric"] > div:nth-of-type(1) { color: #FF7518 !important; }
.block-container { display: flex; flex-wrap: wrap; justify-content: center; }
[data-testid="stHorizontalBlock"] { flex: 1 1 100% !important; margin: 10px 0; }
[data-testid="stExpander"] i, [data-testid="stExpander"] span.icon { font-family: 'Material Icons' !important; }
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
                df_clean = df.where(pd.notnull(df), None)
                records = [tuple(x) for x in df_clean.to_numpy()]
                cols = ",".join(list(df.columns))
                
                # Create the SET part of the UPDATE statement
                update_cols = ", ".join([f"{col} = EXCLUDED.{col}" for col in df.columns if col != 'match_id'])
                
                if records:
                    query = f"INSERT INTO matches ({cols}) VALUES %s ON CONFLICT (match_id) DO UPDATE SET {update_cols}"
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

def create_radar_chart(player_data):
    """Generates a small radar chart for player stats."""
    categories = ['Win %', 'Clutch', 'Consistency', 'GDA', 'Exp']
    
    # Normalize stats for visual balance (0-100 scale)
    # Consistency: Lower is better, so we invert it (0 index = 100 score)
    consistency_score = max(0, 100 - (player_data.get('Consistency Index', 0) * 10))
    
    # GDA: Assume +3.0 is a perfect score
    gda_score = min(100, max(0, (player_data.get('Game Diff Avg', 0) + 1) * 25))
    
    values = [
        player_data.get('Win %', 0),
        player_data.get('Clutch Factor', 0),
        consistency_score,
        gda_score,
        min(100, (player_data.get('Matches', 0) / 15) * 100) # Experience cap at 15 matches
    ]
    
    # Close the polygon by repeating the first value
    values += values[:1]
    categories += categories[:1]

    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(
        r=values,
        theta=categories,
        fill='toself',
        fillcolor='rgba(255, 245, 0, 0.3)',
        line=dict(color='#fff500', width=2),
        hoverinfo='r+theta'
    ))

    fig.update_layout(
        polar=dict(
            bgcolor='rgba(0,0,0,0)',
            radialaxis=dict(visible=False, range=[0, 100]),
            angularaxis=dict(
                gridcolor="rgba(255,255,255,0.1)", 
                linecolor="rgba(255,255,255,0.1)",
                tickfont=dict(size=9, color="#aaa")
            )
        ),
        showlegend=False,
        margin=dict(l=25, r=25, t=10, b=10),
        height=140, # Compact height for mobile cards
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
    )
    return fig

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

def get_partner_stats_inner_template():
    return {'wins': 0, 'losses': 0, 'ties': 0, 'matches': 0, 'game_diff_sum': 0}

def get_partner_stats_template():
    return defaultdict(get_partner_stats_inner_template)

@st.cache_data(show_spinner=False)
def calculate_rankings(matches_to_rank):
    stats = defaultdict(get_player_stats_template)
    current_streaks = defaultdict(int)
    last_active_dates = {}
    elo_ratings = {} # Initialize as a regular dict first
    utr_ratings = {} # Initialize UTR ratings
    last_elo_changes = defaultdict(float) 
    K_FACTOR = 32 
    
    perf_breakdown = defaultdict(lambda: {'singles_w': 0, 'singles_m': 0, 'doubles_w': 0, 'doubles_m': 0}) # ADDED
    
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
    
    partner_stats = defaultdict(get_partner_stats_template) # Added this line

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

        match_gd = t1_total_games - t2_total_games # Define match_gd here

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

                # Update perf_breakdown
                if row.match_type == 'Singles':
                    perf_breakdown[p]['singles_m'] += 1
                else:
                    perf_breakdown[p]['doubles_m'] += 1
                
                if is_winner:
                    stats[p]['wins'] += 1
                    if is_clutch: stats[p]['clutch_wins'] += 1
                    if row.match_type == 'Singles': perf_breakdown[p]['singles_w'] += 1
                    else: perf_breakdown[p]['doubles_w'] += 1
                    if current_streaks[p] < 0: current_streaks[p] = 0 # reset streak if it was negative
                    current_streaks[p] += 1
                    stats[p]['points'] += pts_win
                else:
                    stats[p]['losses'] += 1
                    if current_streaks[p] > 0: current_streaks[p] = 0 # reset streak if it was positive
                    current_streaks[p] -= 1
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

        # Partner Stats (Original Logic from mmd.py)
        # Assuming match_type is available in the row
        if row.match_type in ['Doubles', 'Mixed Doubles'] and len(t1) == 2 and len(t2) == 2:
            # Determine winner for partner stats
            p_stats_winner = row.winner
            
            for team, code, gd_val in [(t1, 1, match_gd), (t2, 2, -match_gd)]:
                p1, p2 = team[0], team[1]
                for a, b in [(p1, p2), (p2, p1)]:
                    ps = partner_stats[a][b]
                    ps['matches'] += 1
                    ps['game_diff_sum'] += gd_val
                    if p_stats_winner == "Tie": ps['ties'] += 1
                    elif (p_stats_winner == "Team 1" and code == 1) or (p_stats_winner == "Team 2" and code == 2):
                        ps['wins'] += 1
                    else: ps['losses'] += 1


    rank_data = []
    for p, s in stats.items():
        m_played = s['matches']
        if m_played == 0: continue
        clutch_pct = (s['clutch_wins'] / s['clutch_matches'] * 100) if s['clutch_matches'] > 0 else 0
        consistency = np.std(s['gd_list']) if s['gd_list'] else 0
        
        # Performance breakdown from mmd.py
        pb = perf_breakdown[p]
        s_perf = (pb['singles_w'] / pb['singles_m'] * 100) if pb['singles_m'] > 0 else 0
        d_perf = (pb['doubles_w'] / pb['doubles_m'] * 100) if pb['doubles_m'] > 0 else 0

        # Recent Trend from mmd.py
        p_gd_list = s['gd_list'][-5:]
        trend_html = "".join([f'<span class="trend-dot {"dot-w" if gd > 0 else "dot-l"}"></span>' for gd in p_gd_list])

        badges = []
        if clutch_pct > 70 and s['clutch_matches'] >= 3: badges.append("🎯 Clutch")
        if consistency < 2.5 and m_played >= 5: badges.append("📉 Steady")
        if current_streaks[p] >= 3: badges.append("🔥 Hot")
        
        score_elo = round(elo_ratings[p], 1)
        current_utr = round(utr_ratings[p], 2) # Use the newly calculated UTR

        rank_data.append({
            "Player": p, 
            "Points": s['points'], # Using the chapter configured points
            "Elo": score_elo,
            "Last Change": last_elo_changes.get(p, 0), # NEW: Added for UI
            "Win %": round((s['wins']/m_played)*100, 1),
            "Recent Trend": trend_html, # Added
            "Matches": m_played, 
            "Wins": s['wins'], 
            "Losses": s['losses'],
            "Games Won": s['games_won'], 
            "Game Diff Avg": round(s['gd_sum']/m_played, 2),
            "Clutch Factor": round(clutch_pct, 1), 
            "Consistency Index": round(consistency, 2),
            "Singles Perf": round(s_perf, 1), 
            "Doubles Perf": round(d_perf, 1), # Added
            "Badges": badges, 
            "Profile": players_df.set_index('name')['profile_image_url'].get(p, DEFAULT_AVATAR)
        })
        
    df = pd.DataFrame(rank_data)
    
    if not df.empty:
        # Default sort by Elo Hybrid as primary for the main view as in mmd.py
        df = df.sort_values(
            by=["Elo", "Win %", "Game Diff Avg", "Player"],
            ascending=[False, False, False, True]
        ).reset_index(drop=True)
        df["Rank"] = [f"🏆 {i+1}" for i in df.index]
        
        # Additional sorting for specific ranking system views
        df = df.sort_values(by=["Score_Elo (Hybrid)", "Win %"], ascending=[False, False])
        df["Rank_Elo (Hybrid)"] = range(1, len(df) + 1)
        df = df.sort_values(by=["Score_Points", "Win %"], ascending=[False, False])
        df["Rank_Points"] = range(1, len(df) + 1)
        df = df.sort_values(by=["Score_UTR", "Win %"], ascending=[False, False])
        df["Rank_UTR"] = range(1, len(df) + 1)


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

def get_birthday_banner(players_df):
    if players_df.empty:
        return
    
    today = datetime.now()
    today_str = today.strftime("%d-%m")
    
    birthday_people = []
    for _, row in players_df.iterrows():
        if pd.notna(row['birthday']) and str(row['birthday']).strip() != "":
            normalized_bday = "-".join([part.lstrip('0') for part in str(row['birthday']).split('-')])
            normalized_today = "-".join([part.lstrip('0') for part in today_str.split('-')])
            
            if normalized_bday == normalized_today:
                birthday_people.append(row['name'])

    if birthday_people:
        names = " & ".join(birthday_people)
        st.markdown(f"""
            <div style="
                background: linear-gradient(90deg, #fff500, #ff0055);
                padding: 15px;
                border-radius: 10px;
                text-align: center;
                margin-bottom: 25px;
                animation: pulse 2s infinite;
                box-shadow: 0 4px 15px rgba(255, 245, 0, 0.4);
            ">
                <h2 style="color: white; margin: 0; font-size: 1.5em;">🎂 Happy Birthday, {names}! 🥳</h2>
                <p style="color: white; margin: 5px 0 0 0; opacity: 0.9;">Wishing you a great day on and off the court!</p>
            </div>
            <style>
            @keyframes pulse {{
                0% {{ transform: scale(1); }}
                50% {{ transform: scale(1.02); }}
                100% {{ transform: scale(1); }}
            }}
            </style>
        """, unsafe_allow_html=True)


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
partner_stats_global = {} # Initialize it here
if not st.session_state.matches_df.empty:
    rank_df, partner_stats_global = calculate_rankings(st.session_state.matches_df)

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

# ADDED: Call to birthday banner
get_birthday_banner(st.session_state.players_df)



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
        if ranking_view == "Doubles": 
            display_rank_df, _ = calculate_rankings(st.session_state.matches_df[st.session_state.matches_df.match_type.isin(["Doubles", "Mixed Doubles"])])
        elif ranking_view == "Singles": 
            display_rank_df, _ = calculate_rankings(st.session_state.matches_df[st.session_state.matches_df.match_type == "Singles"])

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

            # --- A. RESTORED & FIXED: Podium for Top 3 ---
            if len(display_rank_df) >= 3:
                top3 = display_rank_df.head(3).to_dict('records')
                
                # Define the order: Rank 2 (Left), Rank 1 (Center), Rank 3 (Right)
                podium_order = [
                    {"p": top3[1], "margin": "40px"}, # Rank 2
                    {"p": top3[0], "margin": "0px"},  # Rank 1
                    {"p": top3[2], "margin": "40px"}  # Rank 3
                ]
                
                podium_html_content = ""
                for item in podium_order:
                    player = item["p"]
                    
                    # Logic for score and change indicator
                    ch_val = player.get('Last Change', 0)
                    ch_color = "#00ff88" if ch_val >= 0 else "#ff4b4b"
                    ch_txt = f"{'+' if ch_val > 0 else ''}{int(ch_val)}"
                    use_elo=True
                    ch_indicator = f"<span style='color: {ch_color}; font-size: 10px;'>({ch_txt})</span>" if use_elo else ""
                    metric_col = "Elo" if use_elo else "Points"
                    metric_label = "ELO" if use_elo else "pts"
                    score_str = f"{int(player[metric_col])}" if use_elo else f"{player[metric_col]:g}"
                    photo = player["Profile"] if player["Profile"] else "https://via.placeholder.com/100?text=Player"

                    podium_html_content += f"""
                    <div style="flex: 1; margin-top: {item['margin']}; min-width: 0; display: flex; flex-direction: column;">
                        <div style="flex-grow: 1; text-align: center; padding: 10px 2px; background: rgba(255,255,255,0.05); border-radius: 12px; border: 1px solid rgba(255,215,0,0.3); box-shadow: 0 4px 10px rgba(0,0,0,0.3);">
                            <div style="font-size: 1.2em; margin-bottom: 5px; color: #FFD700; font-weight: bold;">{player['Rank']}</div>
                            <div style="display: flex; justify-content: center; margin-bottom: 5px;">
                                <img src="{photo}" style="width: clamp(50px, 20vw, 90px); height: clamp(50px, 20vw, 90px); border-radius: 15px; object-fit: cover; border: 2px solid #fff500; box-shadow: 0 0 15px rgba(255,245,0,0.6);">
                            </div>
                            <div style="margin: 5px 0; color: #fff500; font-size: 0.9em; font-weight: bold; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; padding: 0 2px;">{player['Player']}</div>
                            <div style="color: white; font-weight: bold; font-size: 0.8em;">{score_str} {metric_label} {ch_indicator}</div>
                            <div style="color: #aaa; font-size: 0.7em;">{player['Win %']}% Win</div>
                        </div>
                    </div>
                    """
                
                # Wrap everything in a single flex container
                st.markdown(f"""
                    <div style="display: flex; flex-direction: row; flex-wrap: nowrap; justify-content: center; align-items: flex-start; gap: 8px; margin-bottom: 25px; width: 100%;">
                        {podium_html_content}
                    </div>
                """, unsafe_allow_html=True)

            # --- B. Detailed Player Cards ---
            for idx, row in display_rank_df.iterrows():
                with st.container(border=True):
                    profile_pic = row['Profile'] if row['Profile'] else 'https://via.placeholder.com/100'
                    trend = row.get('Recent Trend', '')
                    badges_list = row.get('Badges', [])
                    badges_html = ' '.join([f'<span title="{b}" style="font-size:16px; margin-left: 5px;">{b.split()[0]}</span>' for b in badges_list])
                    
                    # Metric calculation for cards
                    ch_val = row.get('Last Change', 0)
                    ch_color = "#00ff88" if ch_val >= 0 else "#ff4b4b"
                    ch_txt = f"{'+' if ch_val > 0 else ''}{int(ch_val)}"
                    use_elo=True
                    ch_indicator = f"<span style='color: {ch_color}; font-size: 11px; margin-left: 5px; font-weight: normal;'>({ch_txt})</span>" if use_elo else ""
                    metric_col = "Elo" if use_elo else "Points"
                    metric_label = "ELO" if use_elo else "pts"
                    score_display = f"{int(row[metric_col])}" if use_elo else f"{row[metric_col]:g}"

                    st.markdown(f"""
                    <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 15px;">
                        <div style="display: flex; align-items: center;">
                            <img src="{profile_pic}" style="width: 110px; height: 110px; border-radius: 12px; margin-right: 15px; object-fit: contain; border: 3px solid #CCFF00; box-shadow: 0 0 15px rgba(204, 255, 0, 0.5);">
                            <div>
                                <div style="font-size: 22px; font-weight: bold; color: white; line-height: 1.1;">{row['Player']}</div>
                                <div style="font-size: 13px; color: #00ff88; margin-top: 5px; font-weight: 500;">{trend}</div>
                            </div>
                        </div>
                        <div style="text-align: right;">
                            <div style="background: #CCFF00; color: #041136; font-weight: bold; border-radius: 6px; padding: 4px 10px; font-size: 16px; display: inline-block;">
                                {row['Rank']}
                            </div>
                            <div style="color: #ccc; font-size: 13px; margin-top: 6px; font-weight: bold;">
                                {score_display} {metric_label} {ch_indicator}
                            </div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

                    # Content Section (Radar + Stats)
                    col_chart, col_stats = st.columns([1.8, 1])
                    with col_chart:
                        if 'create_radar_chart' in globals():
                            fig = create_radar_chart(row)
                            fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), height=250)
                            st.plotly_chart(fig, config={'displayModeBar': False}, use_container_width=True, key=f"radar_{row['Player']}_{idx}")
                        
                    with col_stats:
                        stats_html = f"""
                            <div style="text-align: right; padding-right: 5px;">
                                <div style="margin-bottom: 12px;">
                                    <div style="font-size: 10px; color: #888; letter-spacing: 1px;">WIN RATE</div>
                                    <div style="font-size: 24px; font-weight: bold; color: #CCFF00;">{row['Win %']}%</div>
                                </div>
                                <div style="display: flex; justify-content: flex-end; gap: 15px; margin-bottom: 12px;">
                                    <div>
                                        <div style="font-size: 9px; color: #888;">MATCHES</div>
                                        <div style="font-size: 16px; font-weight: bold; color: #eee;">{row['Matches']}</div>
                                    </div>
                                    <div>
                                        <div style="font-size: 9px; color: #888;">W/L</div>
                                        <div style="font-size: 16px; font-weight: bold; color: #eee;">{row['Wins']}/{row['Losses']}</div>
                                    </div>
                                </div>
                                <div style="margin-bottom: 12px;">
                                    <div style="font-size: 10px; color: #888; letter-spacing: 1px;">AVG GDA</div>
                                    <div style="font-size: 18px; font-weight: bold; color: #eee;">{row.get('Game Diff Avg', 0)}</div>
                                </div>
                                <div style="display: flex; justify-content: flex-end; gap: 12px; margin-bottom: 12px;">
                                    <div>
                                        <div style="font-size: 9px; color: #888;">CLUTCH</div>
                                        <div style="font-size: 14px; font-weight: bold; color: #00ff88;">{row.get('Clutch Factor', 0)}%</div>
                                    </div>
                                    <div>
                                        <div style="font-size: 9px; color: #888;">CONSISTENCY</div>
                                        <div style="font-size: 14px; font-weight: bold; color: #ff4b4b;">{row.get('Consistency Index', 0)}</div>
                                    </div>
                                </div>
                                <div style="margin-top: 8px;">{badges_html}</div>
                            </div>
                        """
                        st.markdown(stats_html, unsafe_allow_html=True)


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
            winner_text = ""
            if row.winner == "Team 1":
                if row.team1_player2: # It's a doubles match
                    winner_text = f"{row.team1_player1} & {row.team1_player2}"
                else: # It's a singles match
                    winner_text = row.team1_player1
            elif row.winner == "Team 2":
                if row.team2_player2: # It's a doubles match
                    winner_text = f"{row.team2_player1} & {row.team2_player2}"
                else: # It's a singles match
                    winner_text = row.team2_player1

            st.markdown(f"""<div style="background:rgba(255,255,255,0.30); border-radius:12px; margin-bottom:20px; border:1px solid rgba(255,255,255,0.1); overflow:hidden;">{img_h}<div style="padding:15px; text-align:center;"><div style="color:#888;">{row.date.strftime('%d %b %Y')}</div><div style="font-size:1.1em; margin:5px 0;">{t1} vs {t2}</div><div style="font-size:0.9em; color:#CCFF00; margin-bottom:5px; font-weight:bold; letter-spacing:1px; text-transform:uppercase;">{row.match_type}</div><div style="color:#FF7518; font-weight:bold;">{scores}</div><div style="margin-top:5px; font-weight:bold; color:#fff500;">Winner: {winner_text}</div></div></div>""", unsafe_allow_html=True)
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

    for idx, row in disp.iterrows():
        p_name = row['name']
        p_stats = rank_df[rank_df['Player'] == p_name] if not rank_df.empty else pd.DataFrame()
        has_stats = not p_stats.empty
        s = p_stats.iloc[0] if has_stats else {}

        with st.container():
            c1, c2 = st.columns([1, 3])

            with c1:
                img = row['profile_image_url'] or "https://via.placeholder.com/150"
                bday_str = f"🎂 {row['dt_birthday'].strftime('%d %b')}" if pd.notna(row['dt_birthday']) else ""
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
                            <img src="{img}" style="
                                max-width: 100%; 
                                max-height: 100%; 
                                object-fit: contain;
                            ">
                        </div>
                        <div style="margin-top: 10px; font-weight: bold; font-size: 1.2em;">{p_name}</div>
                        <div style="color: #ffd700; font-size: 0.85em;">{bday_str}</div>
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
                            <div><div class="metric-label">Doubles Perf</div><div class="metric-value" style="color: #00ff00;">{s.get('Doubles Perf', 0)}%</div></div>
                            <div><div class="metric-label">Singles Perf</div><div class="metric-value" style="color: #00bfff;">{s.get('Singles Perf', 0)}%</div></div>
                            <div><div class="metric-label">Win %</div><div class="metric-value">{s.get('Win %', 0)}%</div></div>
                            <div><div class="metric-label">Record</div><div class="metric-value">{s.get('Wins', 0)}W-{s.get('Losses', 0)}L</div></div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    with st.expander("Details & Partners", expanded=False, icon="➡️"):
                        t1, t2 = st.tabs(["Trends", "Partners"])
                        with t1:
                            fig = plot_player_performance(p_name, st.session_state.matches_df)
                            if fig: st.plotly_chart(fig, use_container_width=True, key=f"p_{idx}")
                        with t2:
                            if p_name in partner_stats_global:
                                pstats = sorted(partner_stats_global[p_name].items(), key=lambda item: item[1]['matches'], reverse=True)
                                for partner, data in pstats:
                                    wr = (data['wins'] / data['matches'] * 100) if data['matches'] > 0 else 0
                                    st.text(f"{partner}: {data['wins']}W {data['losses']}L ({data['matches']} matches, {wr:.0f}% WR)")
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
