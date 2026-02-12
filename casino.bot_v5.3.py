import os
import re
import time
import uuid
import base64
import sqlite3
import random
import threading
from dataclasses import dataclass
from html import escape as html_escape
from typing import Optional, List, Tuple, Dict

from telebot import TeleBot
from telebot.types import (
    InlineQuery,
    InlineQueryResultArticle,
    InputTextMessageContent,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
)

# CONFIG
OWNER_ID = int(os.environ.get("OWNER_ID", "7739179390"))
MAX_LIFE_STAKES = 3  # сколько раз можно поставить жизнь
BOT_TOKEN = "8524213683:AAF35em5inOqEXXrOH0pw1Y4TqL2waE2OO8"

bot = TeleBot(BOT_TOKEN, threaded=True, num_threads=8)

# Global edit limiter
import heapq
import itertools as _itertools

class _EditJob:
    __slots__ = ("due", "target", "req_id", "text", "reply_markup", "parse_mode", "inline_id", "chat_id", "msg_id")
    def __init__(self, due, target, req_id, text, reply_markup, parse_mode, inline_id, chat_id, msg_id):
        self.due = due
        self.target = target
        self.req_id = req_id
        self.text = text
        self.reply_markup = reply_markup
        self.parse_mode = parse_mode
        self.inline_id = inline_id
        self.chat_id = chat_id
        self.msg_id = msg_id

class EditLimiter:
    """Serializes + rate-limits edit_message_text globally and per-message.

    Key features:
    - Global gap between edits (avoids overall flood).
    - Per-target gap (avoids 'message is not modified' / 'too frequent' issues).
    - Coalescing: if many edits queued for the same target (animation), only the latest is applied.
    - Handles 429 retry_after by rescheduling the same edit.
    """
    def __init__(self, bot_obj, global_gap_sec=0.12, per_target_gap_sec=1.05):
        self.bot = bot_obj
        self.global_gap = float(global_gap_sec)
        self.per_target_gap = float(per_target_gap_sec)
        self._lock = threading.RLock()
        self._cv = threading.Condition(self._lock)
        self._pq = []  
        self._counter = _itertools.count()
        self._latest_req = {} 
        self._last_global = 0.0
        self._last_target = {}
        self._running = True
        self._thr = threading.Thread(target=self._run, daemon=True)
        self._thr.start()

    def stop(self):
        with self._lock:
            self._running = False
            self._cv.notify_all()

    def _parse_retry_after(self, exc: Exception) -> float:
        s = str(exc) # pyTelegramBotAPI often includes 'retry after X' in text
        m = re.search(r"retry after (\d+(?:\.\d+)?)", s, re.IGNORECASE)
        if m:
            try:
                return float(m.group(1))
            except Exception:
                return 0.0
        return 0.0

    def _compute_due(self, target: tuple) -> float:
        now = time.time()
        due = now
        due = max(due, self._last_global + self.global_gap)
        due = max(due, self._last_target.get(target, 0.0) + self.per_target_gap)
        return due

    def edit_text(self, *, text: str, reply_markup=None, parse_mode: str = None,
                  inline_id: str = None, chat_id: int = None, msg_id: int = None):
        if inline_id:
            target = ("inline", inline_id)
        else:
            target = ("chat", int(chat_id), int(msg_id))

        with self._lock:
            due = self._compute_due(target)
            req_id = next(self._counter)
            self._latest_req[target] = req_id
            job = _EditJob(due, target, req_id, text, reply_markup, parse_mode, inline_id, chat_id, msg_id)
            heapq.heappush(self._pq, (job.due, next(self._counter), job))
            self._cv.notify()
        return True

    def _run(self):
        while True:
            with self._lock:
                if not self._running:
                    return
                if not self._pq:
                    self._cv.wait(timeout=0.5)
                    continue
                due, _, job = self._pq[0]
                now = time.time()
                if due > now:
                    self._cv.wait(timeout=min(0.5, due - now))
                    continue
                heapq.heappop(self._pq)

                if self._latest_req.get(job.target) != job.req_id:
                    continue

            try:
                if job.inline_id:
                    self.bot.edit_message_text(
                        job.text,
                        inline_message_id=job.inline_id,
                        reply_markup=job.reply_markup,
                        parse_mode=job.parse_mode
                    )
                else:
                    self.bot.edit_message_text(
                        job.text,
                        chat_id=job.chat_id,
                        message_id=job.msg_id,
                        reply_markup=job.reply_markup,
                        parse_mode=job.parse_mode
                    )

                with self._lock:
                    t = time.time()
                    self._last_global = t
                    self._last_target[job.target] = t

            except Exception as e:
                ra = self._parse_retry_after(e)
                if ra > 0:
                    with self._lock:
                        self._latest_req[job.target] = job.req_id
                        job.due = time.time() + ra + 0.15
                        heapq.heappush(self._pq, (job.due, next(self._counter), job))
                        self._cv.notify()
                continue

# Global instance
EDIT_LIMITER = EditLimiter(bot, global_gap_sec=0.12, per_target_gap_sec=1.05)

def limited_edit_message_text(*, text: str, reply_markup=None, parse_mode: str = None,
                              inline_id: str = None, chat_id: int = None, msg_id: int = None):
    """Enqueue an edit_message_text through the global limiter."""
    try:
        EDIT_LIMITER.edit_text(text=text, reply_markup=reply_markup, parse_mode=parse_mode,
                               inline_id=inline_id, chat_id=chat_id, msg_id=msg_id)
    except Exception:
        try:
            if inline_id:
                bot.edit_message_text(text, inline_message_id=inline_id, reply_markup=reply_markup, parse_mode=parse_mode)
            else:
                bot.edit_message_text(text, chat_id=chat_id, message_id=msg_id, reply_markup=reply_markup, parse_mode=parse_mode)
        except Exception:
            pass
ME = bot.get_me()
BOT_USERNAME = ME.username

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, "contest_bot.db")
CONTRACT_PATH = os.path.join(SCRIPT_DIR, "contract.txt")
JOBS_PATH = os.path.join(SCRIPT_DIR, "jobs.txt")
PREFIX_LEN = 12

# DB
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
conn.execute("PRAGMA journal_mode=WAL;")
conn.execute("PRAGMA synchronous=NORMAL;")
conn.execute("PRAGMA busy_timeout=5000;")

cur = conn.cursor()

DB_LOCK = threading.RLock()

def db_one(sql: str, params=()):
    with DB_LOCK:
        c = conn.cursor()
        try:
            c.execute(sql, params)
            return c.fetchone()
        finally:
            try: c.close()
            except: pass

def db_all(sql: str, params=()):
    with DB_LOCK:
        c = conn.cursor()
        try:
            c.execute(sql, params)
            return c.fetchall()
        finally:
            try: c.close()
            except: pass

def db_exec(sql: str, params=(), commit: bool = False):
    with DB_LOCK:
        c = conn.cursor()
        try:
            c.execute(sql, params)
            if commit:
                conn.commit()
        finally:
            try: c.close()
            except: pass

cur.execute("""
CREATE TABLE IF NOT EXISTS users (
  user_id INTEGER PRIMARY KEY,
  username TEXT,
  short_name TEXT,
  created_ts INTEGER,
  contract_ts INTEGER,
  balance_cents INTEGER DEFAULT 0,          -- текущий капитал в "центах"
  demo_gift_cents INTEGER DEFAULT 0,        -- стартовые 1000$ (в центах), НЕ участвуют в топе
  demon INTEGER DEFAULT 0                   -- 1 если демон
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS reg_state (
  user_id INTEGER PRIMARY KEY,
  stage TEXT,           -- 'await_open' | 'await_name' | NULL
  msg_id INTEGER,       -- id сообщения в ЛС, которое мы редактируем
  last_ts INTEGER
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS daily_mail (
  user_id INTEGER PRIMARY KEY,
  next_ts INTEGER NOT NULL,
  intro_sent INTEGER DEFAULT 0,
  stopped INTEGER DEFAULT 0,
  pending_amt_cents INTEGER DEFAULT 0,
  pending_kind TEXT,
  pending_msg_id INTEGER DEFAULT 0
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS game_stats (
  user_id INTEGER PRIMARY KEY,
  games_total INTEGER DEFAULT 0,
  wins INTEGER DEFAULT 0,
  losses INTEGER DEFAULT 0,
  max_win_cents INTEGER DEFAULT 0,
  max_lose_cents INTEGER DEFAULT 0
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS game_type_stats (
  user_id INTEGER,
  game_type TEXT,
  cnt INTEGER DEFAULT 0,
  PRIMARY KEY (user_id, game_type)
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS slavery (
  slave_id INTEGER,
  owner_id INTEGER,
  share_bp INTEGER DEFAULT 2000,  -- доля в базисных пунктах (2000=20%)
  PRIMARY KEY (slave_id, owner_id)
)
""")

#SLAVERY EXTENSIONS / BUY OFFERS
try:
    cur.execute("ALTER TABLE slavery ADD COLUMN earned_cents INTEGER DEFAULT 0")
except Exception:
    pass

try:
    cur.execute("ALTER TABLE slavery ADD COLUMN acquired_ts INTEGER DEFAULT 0")
except Exception:
    pass

cur.execute("""
CREATE TABLE IF NOT EXISTS slave_earn_log (
  slave_id INTEGER,
  owner_id INTEGER,
  ts INTEGER,
  amount_cents INTEGER DEFAULT 0
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS slave_meta (
  slave_id INTEGER PRIMARY KEY,
  buyout_cents INTEGER DEFAULT 0,
  strikes INTEGER DEFAULT 0,
  life_uses INTEGER DEFAULT 0
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS demon_loot (
  winner_id INTEGER,
  loser_id INTEGER,
  slave_id INTEGER,
  ts INTEGER,
  taken INTEGER DEFAULT 0,
  PRIMARY KEY (winner_id, loser_id, slave_id)
)
""")

# ensure slave_meta has life_uses column (migration)
try:
    cur.execute("ALTER TABLE slave_meta ADD COLUMN life_uses INTEGER DEFAULT 0")
except Exception:
    pass

cur.execute("""
CREATE TABLE IF NOT EXISTS buy_offers (
  offer_id TEXT PRIMARY KEY,
  slave_id INTEGER,
  buyer_id INTEGER,
  price_cents INTEGER,
  created_ts INTEGER,
  active INTEGER DEFAULT 1
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS buy_offer_resp (
  offer_id TEXT,
  owner_id INTEGER,
  status INTEGER DEFAULT 0,
  PRIMARY KEY (offer_id, owner_id)
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS work_stats (
  user_id INTEGER,
  job_key TEXT,
  shifts INTEGER DEFAULT 0,        -- сколько раз ходил на эту работу
  days INTEGER DEFAULT 0,          -- стаж по этой работе (1 смена = 1 день стажа)
  earned_cents INTEGER DEFAULT 0,  -- всего заработано на этой работе
  PRIMARY KEY (user_id, job_key)
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS work_shift (
  user_id INTEGER PRIMARY KEY,
  job_key TEXT,
  started_ts INTEGER,
  ends_ts INTEGER,
  salary_full_cents INTEGER DEFAULT 0,   -- рассчитанная "полная" зарплата (со стажем)
  success_pct INTEGER DEFAULT 0
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS work_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER,
  job_key TEXT,
  started_ts INTEGER,
  ends_ts INTEGER,
  success INTEGER,               -- 1/0
  paid_cents INTEGER,
  text TEXT
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS shop_inv (
    user_id INTEGER NOT NULL,
    item_key TEXT NOT NULL,
    qty INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(user_id, item_key)
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS shop_active (
    user_id INTEGER NOT NULL,
    item_key TEXT NOT NULL,
    remaining_games INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(user_id, item_key)
)
""")
cur.execute("""
CREATE TABLE IF NOT EXISTS shop_bind (
    user_id INTEGER PRIMARY KEY,
    game_id TEXT NOT NULL,
    bound_ts INTEGER NOT NULL
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS shop_catalog (
    user_id INTEGER PRIMARY KEY,
    cycle_start_ts INTEGER NOT NULL,
    keys_csv TEXT NOT NULL
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS games (
  game_id TEXT PRIMARY KEY,
  group_key TEXT,
  creator_id INTEGER,
  state TEXT,                    -- 'lobby'|'choose_format'|'playing'|'finished'|'cancelled'
  stake_cents INTEGER,
  created_ts INTEGER,
  reg_ends_ts INTEGER,
  reg_extended INTEGER DEFAULT 0,
  roulette_format TEXT,          -- '1x3'|'3x3'|'3x5'
  turn_index INTEGER DEFAULT 0
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS game_players (
  game_id TEXT,
  user_id INTEGER,
  status TEXT,          -- 'pending'|'ready'|'anon_pending'
  PRIMARY KEY (game_id, user_id)
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS game_results (
  game_id TEXT,
  user_id INTEGER,
  delta_cents INTEGER DEFAULT 0,
  finished INTEGER DEFAULT 0,
  PRIMARY KEY (game_id, user_id)
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS continue_tokens (
  group_key TEXT,
  user_id INTEGER,
  token TEXT,
  ts INTEGER,
  PRIMARY KEY (group_key, user_id)
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS spins (
  game_id TEXT,
  user_id INTEGER,
  stage TEXT,              -- 'ready'|'spinning'|'done'
  msg_chat_id INTEGER,
  msg_id INTEGER,
  inline_id TEXT,
  grid_text TEXT,          -- текущий вид слотов
  started_ts INTEGER,
  PRIMARY KEY (game_id, user_id)
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS rematch_votes (
  game_id TEXT,
  user_id INTEGER,
  vote TEXT,          -- 'yes'|'no'
  PRIMARY KEY (game_id, user_id)
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS life_wait (
  game_id TEXT,
  user_id INTEGER,
  stake_cents INTEGER,
  PRIMARY KEY (game_id, user_id)
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS demon_streak (
  user_id INTEGER PRIMARY KEY,
  streak INTEGER DEFAULT 0,
  best INTEGER DEFAULT 0,
  updated_ts INTEGER DEFAULT 0
)
""")

conn.commit()

def ensure_game_origin_columns():
    for sql in [
        "ALTER TABLE games ADD COLUMN origin_chat_id INTEGER",
        "ALTER TABLE games ADD COLUMN origin_message_id INTEGER",
        "ALTER TABLE games ADD COLUMN origin_inline_id TEXT",
        "ALTER TABLE games ADD COLUMN game_type TEXT DEFAULT 'roulette'",
        "ALTER TABLE games ADD COLUMN cross_round INTEGER DEFAULT 1",
        "ALTER TABLE games ADD COLUMN stake_kind TEXT DEFAULT 'money'",
        "ALTER TABLE games ADD COLUMN life_demon_id INTEGER DEFAULT 0",
        "ALTER TABLE games ADD COLUMN demon_settled INTEGER DEFAULT 0",
    ]:
        try:
            conn.execute(sql)
        except sqlite3.OperationalError:
            pass  
    conn.commit()

ensure_game_origin_columns()

# Helpers
def now_ts() -> int:
    return int(time.time())

def money_to_cents(x: str) -> Optional[int]:
    """
    Поддержка: 10, 10.5, 10,50, 1000
    Тысячные НЕ поддерживаем: всё после сотых отбрасываем.
    """
    x = x.strip().replace(",", ".")
    if not re.fullmatch(r"\d+(\.\d{1,})?", x):
        return None
    if "." in x:
        a, b = x.split(".", 1)
        b = (b + "00")[:2] 
    else:
        a, b = x, "00"
    return int(a) * 100 + int(b)

def cents_to_money_str(cents: int) -> str:
    sign = "-" if cents < 0 else ""
    cents = abs(int(cents))
    return f"{sign}{cents//100}.{cents%100:02d}"

def safe_format(template: str, **kwargs) -> str:
    class DD(dict):
        def __missing__(self, key):
            return "{" + key + "}"
    return template.format_map(DD(**kwargs))

# Cross-roulette helpers
def cross_format_for_round(r: int) -> str:
    r = int(r or 1)
    if r <= 4:
        return "1x3"
    if r <= 7:
        return "3x3"
    return "3x5"

def pick_life_owner(game_id: str, loser_id: int, creator_id: int | None):
    rows = db_all("SELECT user_id, status FROM game_players WHERE game_id=? ORDER BY rowid", (game_id,))
    players = [(int(r[0]), (r[1] or "")) for r in rows]
    others = [uid for uid, _st in players if uid != int(loser_id)]
    if len(players) == 2 and others:
        return int(others[0])

    res = db_all("SELECT user_id, delta_cents FROM game_results WHERE game_id=?", (game_id,))
    if res:
        cand = [(int(uid), int(dc or 0)) for (uid, dc) in res if int(uid) != int(loser_id)]
        if cand:
            cand.sort(key=lambda x: x[1], reverse=True)
            return cand[0][0]

    if creator_id and int(creator_id) != int(loser_id):
        return int(creator_id)

    if others:
        return int(others[0])

    return None

def cross_stake_for_round(base_cents: int, r: int) -> tuple[int, int]:
    """Возвращает (stake_cents, add_cents) для раунда r.
    add_cents = 10% от базовой ставки * (r-1).
    """
    base_cents = int(base_cents or 0)
    r = int(r or 1)
    add = (base_cents * 10 // 100) * max(0, r-1)
    return base_cents + add, add

def get_game_type_and_round(game_id: str) -> tuple[str, int]:
    cur.execute("SELECT COALESCE(game_type,'roulette'), COALESCE(cross_round,1) FROM games WHERE game_id=?", (game_id,))
    row = cur.fetchone()
    if not row:
        return 'roulette', 1
    return (row[0] or 'roulette'), int(row[1] or 1)

def load_contract_text() -> str:
    if not os.path.exists(CONTRACT_PATH):
        with open(CONTRACT_PATH, "w", encoding="utf-8") as f:
            f.write("<b>𖤐༒☬𝕂𝕆ℕ𝕋ℝ𝔸𝕂𝕋☬༒𖤐</b>\n\nПодпись принята: {name}\nДата: <b>{date}</b>\n")
    with open(CONTRACT_PATH, "r", encoding="utf-8") as f:
        return f.read()

def is_registered(uid: int) -> bool:
    r = db_one("SELECT 1 FROM users WHERE user_id=?", (int(uid),))
    return r is not None

def upsert_user(uid: int, username: Optional[str]):
    db_exec("""
    INSERT INTO users (user_id, username, created_ts)
    VALUES (?,?,?)
    ON CONFLICT(user_id) DO UPDATE SET username=COALESCE(excluded.username, users.username)
    """, (int(uid), username, now_ts()), commit=True)

def set_short_name(uid: int, name: str):
    upsert_user(uid, None)
    db_exec("UPDATE users SET short_name=? WHERE user_id=?", (name, int(uid)), commit=True)

def get_user(uid: int):
    return db_one(
        "SELECT user_id, username, short_name, created_ts, contract_ts, balance_cents, demo_gift_cents, demon "
        "FROM users WHERE user_id=?",
        (int(uid),)
    )

def set_reg_state(uid: int, stage: Optional[str], msg_id: Optional[int]):
    db_exec("""
    INSERT INTO reg_state (user_id, stage, msg_id, last_ts)
    VALUES (?,?,?,?)
    ON CONFLICT(user_id) DO UPDATE SET stage=excluded.stage, msg_id=excluded.msg_id, last_ts=excluded.last_ts
    """, (int(uid), stage, msg_id, now_ts()), commit=True)

def get_reg_state(uid: int):
    row = db_one("SELECT stage, msg_id FROM reg_state WHERE user_id=?", (int(uid),))
    return row if row else (None, None)

def wipe_user(uid: int):
    uid = int(uid)
    db_exec("DELETE FROM users WHERE user_id=?", (uid,), commit=True)
    db_exec("DELETE FROM reg_state WHERE user_id=?", (uid,), commit=True)
    db_exec("DELETE FROM daily_mail WHERE user_id=?", (uid,), commit=True)
    db_exec("DELETE FROM game_stats WHERE user_id=?", (uid,), commit=True)
    db_exec("DELETE FROM slavery WHERE slave_id=? OR owner_id=?", (uid, uid), commit=True)

def add_balance(uid: int, delta_cents: int):
    upsert_user(int(uid), None)
    db_exec(
        "UPDATE users SET balance_cents = COALESCE(balance_cents,0) + ? WHERE user_id=?",
        (int(delta_cents), int(uid)),
        commit=True
    )

def set_contract_signed(uid: int, gift_cents: int):
    db_exec("""
    UPDATE users
    SET contract_ts=?, demo_gift_cents=?, balance_cents=COALESCE(balance_cents,0)+?
    WHERE user_id=?
    """, (now_ts(), int(gift_cents), int(gift_cents), int(uid)), commit=True)
    ensure_daily_mail_row(int(uid))

# Daily mail
MAIL_INTRO_DELAY_SEC = 8 * 3600
MAIL_PERIOD_SEC = 24 * 3600

def ensure_daily_mail_row(uid: int):
    db_exec(
        "INSERT OR IGNORE INTO daily_mail (user_id, next_ts, intro_sent, stopped, pending_amt_cents, pending_kind, pending_msg_id) "
        "VALUES (?,?,?,?,?,?,?)",
        (int(uid), now_ts() + MAIL_INTRO_DELAY_SEC, 0, 0, 0, None, 0),
        commit=True
    )

def stop_daily_mail(uid: int):
    db_exec(
        "UPDATE daily_mail SET stopped=1, pending_amt_cents=0, pending_kind=NULL, pending_msg_id=0 WHERE user_id=?",
        (int(uid),),
        commit=True
    )

def get_games_total(uid: int) -> int:
    row = db_one("SELECT games_total FROM game_stats WHERE user_id=?", (int(uid),))
    return int((row[0] if row else 0) or 0)

def bump_game_type_stat(uid: int, game_type: str) -> None:
    if not game_type:
        return
    uid = int(uid)
    try:
        db_exec("INSERT OR IGNORE INTO game_type_stats (user_id, game_type, cnt) VALUES (?,?,0)", (uid, game_type), commit=True)
        db_exec("UPDATE game_type_stats SET cnt=cnt+1 WHERE user_id=? AND game_type=?", (uid, game_type), commit=True)
    except Exception:
        pass

def get_favorite_game_title(uid: int) -> str:
    """Вернуть название игры, которую пользователь выбирал чаще всего."""
    try:
        cur.execute("SELECT game_type, cnt FROM game_type_stats WHERE user_id=? ORDER BY cnt DESC LIMIT 1", (uid,))
        row = cur.fetchone()
        if not row:
            return "—"
        gt = (row[0] or "").strip()
        if gt == "cross":
            return "Марафон рулетка"
        if gt == "roulette":
            return "Рулетка"
        # fallback
        return gt
    except Exception:
        return "—"

def is_registered(uid: int) -> bool:
    cur.execute("SELECT contract_ts, short_name FROM users WHERE user_id=?", (uid,))
    row = cur.fetchone()
    if not row:
        return False
    contract_ts = int(row[0] or 0)
    short_name = row[1]
    return contract_ts > 0 and bool(short_name)

def _mail_letter_text(kind: str, amount_cents: int) -> str:
    amt = cents_to_money_str(amount_cents)
    if kind.startswith("owner_finance|"):
                try:
                    raw = kind.split("|", 1)[1]
                    comment = base64.urlsafe_b64decode(raw.encode("ascii")).decode("utf-8", "ignore").strip()
                except Exception:
                    comment = ""
                if not comment:
                    comment = "Перевод средств."
                return f"{html_escape(comment)}\n<i>к письму прилагается чек на <b>{amt}</b>$</i>"
    
    if kind == "demon_pay":
        
        return (
            "Демоны всегда держат обещания. В этот раз удача на твоей стороне.\n"
            f"<i>к письму прилагается чек на <b>{amt}</b>$</i>"
        )

    if kind == "intro":
        body = "Ваш доброжелатель очень рад вашему вниманию и, в качестве поощрения будет раз в день высылать вам подарок."
    elif kind == "low":
        body = "Анонимный доброжелатель разочарован вашей отдачей."
    else:
        body = "Анонимный доброжелатель заметил вашу отдачу. Примите в качестве его благодарности скромный подарок."

    return (
        "<i>Текст письма:</i>\n"
        f"{html_escape(body)}\n"
        f"<i>К письму прилагался чек на</i> <b>{amt}</b>$"
    )

def _send_mail_prompt(uid: int, kind: str, amount_cents: int) -> None:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Открыть письмо", callback_data=cb_pack("mail:open", uid)))
    msg = bot.send_message(uid, "Вам пришло письмо. Открыть?", reply_markup=kb)
    db_exec(
        "UPDATE daily_mail SET pending_amt_cents=?, pending_kind=?, pending_msg_id=? WHERE user_id=?",
        (int(amount_cents), kind, int(msg.message_id), int(uid)),
        commit=True,
    )

def _mail_daemon():
    while True:
        try:
            now = now_ts()
            cur.execute("SELECT user_id, next_ts, intro_sent, stopped, pending_amt_cents FROM daily_mail")
            rows = cur.fetchall()
            for (uid, next_ts, intro_sent, stopped, pending_amt) in rows:
                uid = int(uid)
                if int(stopped or 0) == 1:
                    continue

                if not is_registered(uid):
                    continue

                if has_work_history(uid):
                    stop_daily_mail(uid)
                    continue

                if int(pending_amt or 0) > 0:
                    continue

                if now < int(next_ts or 0):
                    continue

                if int(intro_sent or 0) == 0:
                    kind = "intro"
                    amt = 40000
                    cur.execute("UPDATE daily_mail SET next_ts=?, intro_sent=1 WHERE user_id=?", (now + MAIL_PERIOD_SEC, uid))
                    conn.commit()
                    try:
                        _send_mail_prompt(uid, kind, amt)
                    except Exception:
                        pass
                else:
                    games = get_games_total(uid)
                    if games >= 3:
                        kind = "std"
                        amt = 40000
                    else:
                        kind = "low"
                        amt = 1000
                    cur.execute("UPDATE daily_mail SET next_ts=? WHERE user_id=?", (now + MAIL_PERIOD_SEC, uid))
                    conn.commit()
                    try:
                        _send_mail_prompt(uid, kind, amt)
                    except Exception:
                        pass
        except Exception:
            pass
        time.sleep(30)

def top_value_cents(uid: int) -> int:
    cur.execute("SELECT balance_cents, demo_gift_cents, demon FROM users WHERE user_id=?", (uid,))
    row = cur.fetchone()
    if not row:
        return 0
    bal, gift, demon = int(row[0] or 0), int(row[1] or 0), int(row[2] or 0)
    if demon == 1:
        return -10**18
    return bal - gift

def get_balance_cents(uid: int) -> int:
    r = db_one("SELECT COALESCE(balance_cents,0) FROM users WHERE user_id=?", (int(uid),))
    return int((r[0] if r else 0) or 0)

def compute_status(uid: int) -> str:
    u = get_user(uid)
    if not u:
        return "-"
    bal = int(u[5] or 0)
    demon = int(u[7] or 0)

    if demon == 1:
        return "ĐĒʋÍ£"

    statuses = []
    # капитал
    if bal >= 2_000_000_000_00:
        statuses.append("Мультимиллиардер")
    elif bal >= 1_000_000_000_00:
        statuses.append("Миллиардер")
    elif bal >= 2_000_000_00:
        statuses.append("Мультимиллионер")
    elif bal >= 1_000_000_00:
        statuses.append("Миллионер")
    elif bal <= -1_000_000 * 100:
        statuses.append("Великий должник")
    # раб
    if is_slave(uid):
        statuses.append("Раб")
    # удача/неудача по играм 
    try:
        r = db_one("SELECT wins, losses, games FROM game_stats WHERE user_id=?", (uid,))
        if r:
            wins, losses, games = int(r[0] or 0), int(r[1] or 0), int(r[2] or 0)
            if games > 0:
                if wins > losses:
                    statuses.append("Удача на твоей стороне")
                elif losses > wins:
                    statuses.append("Неудачник со стажем")
    except Exception:
        pass
    # богатейший/нищета
    try:
        rows = db_all("SELECT user_id FROM users WHERE demon=0", ())
        uids = [int(x[0]) for x in rows]
        if uids:
            uids.sort(key=lambda x: top_value_cents(x), reverse=True)
            if uid == uids[0]:
                statuses.append("Богатейший человек")
            if uid == uids[-1]:
                statuses.append("Сама нищета")
    except Exception:
        pass

    # Вечный узник: раб > полугода
    if is_slave(uid):
        try:
            r = db_one("SELECT COALESCE(MIN(acquired_ts),0) FROM slavery WHERE slave_id=?", (int(uid),))
            acq = int((r[0] if r else 0) or 0)
            if acq > 0 and (now_ts() - acq) >= 180 * 24 * 3600:
                statuses.append("Вечный узник")
        except Exception:
            pass

    # С Дьяволом на Ты: обыграть демона более 10 раз подряд
    try:
        if get_demon_streak(uid) >= 11:
            statuses.append("С Дьяволом на Ты")
    except Exception:
        pass

    # Ломаный рот этой рулетки: проиграть марафон (cross) в последнем раунде на сумму > 1,000,000$
    try:
        r = db_one("""
            SELECT 1
            FROM games g
            JOIN game_results gr ON gr.game_id=g.game_id
            WHERE g.game_type='cross'
              AND g.state='finished'
              AND COALESCE(g.cross_round,0) >= 9
              AND gr.user_id=?
              AND COALESCE(gr.delta_cents,0) <= ?
            LIMIT 1
        """, (int(uid), -1_000_000 * 100))
        if r:
            statuses.append("Ломаный рот этой рулетки")
    except Exception:
        pass

    return ", ".join(statuses) if statuses else "Без статуса"

def get_demon_streak(uid: int) -> int:
    r = db_one("SELECT COALESCE(streak,0) FROM demon_streak WHERE user_id=?", (int(uid),))
    return int((r[0] if r else 0) or 0)

def set_demon_streak(uid: int, new_streak: int):
    uid = int(uid)
    new_streak = int(new_streak)
    ts = now_ts()
    r = db_one("SELECT COALESCE(best,0) FROM demon_streak WHERE user_id=?", (uid,))
    best = int((r[0] if r else 0) or 0)
    best = max(best, new_streak)
    db_exec(
        "INSERT OR REPLACE INTO demon_streak (user_id, streak, best, updated_ts) VALUES (?,?,?,?)",
        (uid, new_streak, best, ts),
        commit=True
    )

def update_demon_streak_after_game(game_id: str):
    """
    Если в игре участвовал хотя бы один демон, то:
    - для каждого НЕ-демона участника:
        если он обыграл всех демонов по delta_cents => streak += 1
        иначе => streak = 0
    """
    rows = db_all("""
        SELECT gp.user_id, COALESCE(u.demon,0)
        FROM game_players gp
        JOIN users u ON u.user_id=gp.user_id
        WHERE gp.game_id=?
    """, (game_id,))
    if not rows:
        return

    demons = {int(uid) for uid, d in rows if int(d or 0) == 1}
    if not demons:
        return

    res = db_all("SELECT user_id, COALESCE(delta_cents,0) FROM game_results WHERE game_id=?", (game_id,))
    delta_map = {int(uid): int(dc or 0) for uid, dc in res}

    demon_best = max((delta_map.get(d, -10**18) for d in demons), default=-10**18)

    for uid, d in rows:
        uid = int(uid)
        if int(d or 0) == 1:
            continue  # демонам streak не считаем
        my_delta = delta_map.get(uid, 0)
        if my_delta > demon_best:
            set_demon_streak(uid, get_demon_streak(uid) + 1)
        else:
            set_demon_streak(uid, 0)

# SHOP: CATALOG + LOGIC
SHOP_ITEMS = {
    "magnet": {
        "title": "🧲 Магнит",
        "price_cents": 500_00,
        "max_qty": 2,
        "duration_games": 2,
        "desc": "Шанс стандартных слотов (🍒🍀🍋) +10% на 2 игры. Самый топорный метод обмануть игровой автомат рулетку, однако действенный",
    },
    "fake_clover": {
        "title": "🍀 Фальшивый клевер",
        "price_cents": 444_00,
        "max_qty": 3,
        "duration_games": 2,
        "desc": "Один слот: 50% что будет 7⃣, иначе 💀. Действует 2 игры. Каким-то образом повышает вашу удачу, однако сама удача - капризная дама",
    },
    "wine": {
        "title": "🍷 Вино",
        "price_cents": 700_00,
        "max_qty": 2,
        "duration_games": 3,
        "desc": "Шанс 7⃣ и 💀 +20% на 3 игры. «Всё или ничего».",
    },
    "devil_pepper": {
        "title": "🌶️ Перец дьявола",
        "price_cents": 666_00,
        "max_qty": 2,
        "duration_games": 1,
        "desc": "Умножает итоговый результат в рулетке на 2 (даже проигрыш). Если при активном перце выпадет 💀💀💀 в 1×3/3×3 или 💀×5 по строке в 3×5 — вы дополнительно становитесь рабом демона (если он есть).",
    },
    "insurance": {
        "title": "📜 Страхование капитала",
        "price_cents": 1000_00,
        "max_qty": 1,
        "duration_games": 1,
        "desc": "Защита ваших денежных средств в случае непредвиденных обстоятельств. Полностью возвращает сумму вашего проигрыша. Всё бы ничего, однако материал подозрительно напоминает то письмо...",
    },
}

# SHOP: dynamic pricing (balance-based)
SHOP_PRICE_STEP_CENTS = 5000_00  # each full $5000 on balance increases price
SHOP_PRICE_STEP_ADD_PCT = 50     # +50% of base price per step

def shop_price_steps_for_balance(balance_cents: int) -> int:
    try:
        bal = int(balance_cents or 0)
    except Exception:
        bal = 0
    if bal <= 0:
        return 0
    return max(0, bal // SHOP_PRICE_STEP_CENTS)

def shop_dynamic_price_cents(uid: int, key: str, balance_cents: int | None = None) -> tuple[int, int]:
    """Returns (price_cents, steps). steps = floor(balance / 5000$)."""
    if key not in SHOP_ITEMS:
        return 0, 0
    base = int(SHOP_ITEMS[key].get("price_cents", 0) or 0)
    if balance_cents is None:
        u = get_user(uid)
        balance_cents = int(u[5] or 0) if u else 0
    steps = shop_price_steps_for_balance(balance_cents)

    num = base * (2 + steps)
    price = (num + 1) // 2
    return int(price), int(steps)


SHOP_CATALOG_PERIOD_SEC = 3 * 24 * 3600 
SHOP_CATALOG_SIZE = len(SHOP_ITEMS)

def _shop_catalog_regen(uid: int) -> List[str]:
    keys = list(SHOP_ITEMS.keys())
    random.shuffle(keys)
    picks = keys[:min(SHOP_CATALOG_SIZE, len(keys))]
    cur.execute(
        """INSERT INTO shop_catalog (user_id, cycle_start_ts, keys_csv)
           VALUES (?,?,?)
           ON CONFLICT(user_id) DO UPDATE SET
             cycle_start_ts=excluded.cycle_start_ts,
             keys_csv=excluded.keys_csv
        """,
        (uid, now_ts(), ",".join(picks)),
    )
    conn.commit()
    return picks

def get_shop_catalog(uid: int) -> List[str]:
    cur.execute("SELECT cycle_start_ts, keys_csv FROM shop_catalog WHERE user_id=?", (uid,))
    row = cur.fetchone()
    if not row:
        return _shop_catalog_regen(uid)

    start_ts = int(row[0] or 0)
    if now_ts() - start_ts >= SHOP_CATALOG_PERIOD_SEC:
        return _shop_catalog_regen(uid)

    keys = [k for k in (row[1] or "").split(",") if k and k in SHOP_ITEMS]
    if not keys:
        return _shop_catalog_regen(uid)
    return keys

def shop_catalog_refresh_left(uid: int) -> int:
    cur.execute("SELECT cycle_start_ts FROM shop_catalog WHERE user_id=?", (uid,))
    row = cur.fetchone()
    if not row:
        _shop_catalog_regen(uid)
        return SHOP_CATALOG_PERIOD_SEC
    start_ts = int(row[0] or 0)
    left = SHOP_CATALOG_PERIOD_SEC - (now_ts() - start_ts)
    return max(0, int(left))

def shop_get_qty(uid: int, key: str) -> int:
    cur.execute("SELECT qty FROM shop_inv WHERE user_id=? AND item_key=?", (uid, key))
    r = cur.fetchone()
    return int(r[0] or 0) if r else 0

def shop_set_qty(uid: int, key: str, qty: int):
    qty = max(0, int(qty))
    cur.execute("""
    INSERT INTO shop_inv (user_id, item_key, qty)
    VALUES (?,?,?)
    ON CONFLICT(user_id, item_key) DO UPDATE SET qty=excluded.qty
    """, (uid, key, qty))
    conn.commit()

def shop_get_active(uid: int) -> dict:
    cur.execute("SELECT item_key, remaining_games FROM shop_active WHERE user_id=?", (uid,))
    rows = cur.fetchall()
    return {k: int(v or 0) for (k, v) in rows}

def render_active_boosts(active: dict) -> str:
    try:
        parts = []
        for k, v in (active or {}).items():
            if int(v or 0) <= 0:
                continue
            title = SHOP_ITEMS.get(k, {}).get("title", k)
            parts.append(title)
        if not parts:
            return ""
        return "Текущие усиления:\n" + " ".join(parts)
    except Exception:
        return 

def shop_set_active(uid: int, key: str, remaining: int):
    remaining = int(remaining)
    if remaining <= 0:
        cur.execute("DELETE FROM shop_active WHERE user_id=? AND item_key=?", (uid, key))
    else:
        cur.execute("""
        INSERT INTO shop_active (user_id, item_key, remaining_games)
        VALUES (?,?,?)
        ON CONFLICT(user_id, item_key) DO UPDATE SET remaining_games=excluded.remaining_games
        """, (uid, key, remaining))
    conn.commit()

def shop_get_bound_game(uid: int) -> str | None:
    row = db_one("SELECT game_id FROM shop_bind WHERE user_id=?", (uid,))
    return (row[0] if row else None)

def shop_clear_bind(uid: int):
    db_exec("DELETE FROM shop_bind WHERE user_id=?", (uid,), commit=True)

def shop_bind_to_game(uid: int, game_id: str):
    db_exec(
        "INSERT INTO shop_bind (user_id, game_id, bound_ts) VALUES (?,?,?) "
        "ON CONFLICT(user_id) DO UPDATE SET game_id=excluded.game_id, bound_ts=excluded.bound_ts",
        (uid, game_id, now_ts()),
        commit=True
    )

def shop_get_earliest_active_game(uid: int) -> str | None:
    row = db_one(
        """SELECT g.game_id
           FROM games g
           JOIN game_players gp ON gp.game_id=g.game_id
          WHERE gp.user_id=?
            AND g.state NOT IN ('finished','cancelled')
          ORDER BY g.created_ts ASC
          LIMIT 1""",
        (uid,)
    )
    return (row[0] if row else None)

def shop_get_active_for_game(uid: int, game_id: str) -> dict:
    """Активные эффекты магазина, но только если они 'привязаны' к самой ранней активной игре пользователя."""
    active = shop_get_active(uid)
    if not active:
        return {}

    bound = shop_get_bound_game(uid)
    if bound:
        row = db_one("SELECT state FROM games WHERE game_id=?", (bound,))
        if (not row) or (row[0] in ('finished', 'cancelled')):
            shop_clear_bind(uid)
            bound = None

    if bound:
        return active if bound == game_id else {}

    earliest = shop_get_earliest_active_game(uid)
    if earliest and earliest == game_id:
        shop_bind_to_game(uid, game_id)
        return active
    return {}

def shop_buy(uid: int, key: str) -> tuple[bool, str]:
    if key not in SHOP_ITEMS:
        return False, "Товар не найден."
    item = SHOP_ITEMS[key]
    have = shop_get_qty(uid, key)
    if have >= item["max_qty"]:
        return False, "У тебя уже максимальное количество этого предмета."
    u = get_user(uid)
    if not u or not u[2]:
        return False
    bal = int(u[5] or 0)
    price, price_steps = shop_dynamic_price_cents(uid, key, bal)
    if bal < price:
        return False, f"Недостаточно средств. Необходимо {cents_to_money_str(price)}$"
    add_balance(uid, -price)
    shop_set_qty(uid, key, have + 1)
    return True, "Покупка прошла успешно."

def shop_activate(uid: int, key: str) -> tuple[bool, str]:
    if key not in SHOP_ITEMS:
        return False, "Товар не найден."
    item = SHOP_ITEMS[key]
    have = shop_get_qty(uid, key)
    if have <= 0:
        return False, "У тебя нет этого предмета."
    active = shop_get_active(uid)
    if key in active and active[key] > 0:
        return False, "Этот эффект уже активен."
    shop_set_qty(uid, key, have - 1)
    shop_set_active(uid, key, int(item["duration_games"]))
    return True, f"Активировано на {item['duration_games']} игр."

def shop_tick_after_game(uid: int, game_id: str):
    """Списываем 1 'игру' со всех активных эффектов пользователя, но только если эффекты были привязаны к этой игре."""
    bound = shop_get_bound_game(uid)
    if bound and bound != game_id:
        return

    active = shop_get_active(uid)
    if not active:
        if bound == game_id:
            shop_clear_bind(uid)
        return

    for k, rem in active.items():
        shop_set_active(uid, k, rem - 1)

    if bound == game_id:
        shop_clear_bind(uid)

def shop_menu_text(uid: int) -> str:
    u = get_user(uid)
    bal = int(u[5] or 0) if u else 0
    price_steps = shop_price_steps_for_balance(bal)
    price_markup = price_steps * SHOP_PRICE_STEP_ADD_PCT

    active = shop_get_active(uid)

    act_lines = []
    for k, rem in active.items():
        title = SHOP_ITEMS.get(k, {}).get("title", k)
        act_lines.append(f"• {html_escape(title)} - осталось <b>{rem}</b> игр")

    act_block = "\n".join(act_lines) if act_lines else "Нет"

    return (
        f"<b><u>Магазин улучшений</u></b>\n\n"
        f"Ваш капитал: <b>{cents_to_money_str(bal)}</b>$\n"
        f"Надбавка к цене: <b>+{price_markup}%</b>\n\n"
        f"Активные эффекты:\n{act_block}\n\n"
        f"Выбери товар:"
    )

def shop_menu_kb(uid: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    for key in get_shop_catalog(uid):
        item = SHOP_ITEMS[key]
        have = shop_get_qty(uid, key)
        kb.add(InlineKeyboardButton(
            f"{item['title']} ×{have}",
            callback_data=cb_pack(f"shop:item:{key}", uid)
        ))
    return kb

def shop_item_text(uid: int, key: str) -> str:
    item = SHOP_ITEMS[key]
    have = shop_get_qty(uid, key)
    active = shop_get_active(uid)
    rem = active.get(key, 0)

    u = get_user(uid)
    bal = int(u[5] or 0) if u else 0
    price, price_steps = shop_dynamic_price_cents(uid, key, bal)
    markup_line = (f"Надбавка к цене: <b>+{price_steps * SHOP_PRICE_STEP_ADD_PCT}%</b>\n" if price_steps > 0 else "")

    return (
        f"{html_escape(item['title'])}\n\n"
        f"{html_escape(item['desc'])}\n\n"
        f"Цена: <b>{cents_to_money_str(int(price))}</b>$\n"
        f"{markup_line}"
        f"Количество: <b>{have}</b> из <b>{item['max_qty']}</b>\n"
        f"Активен: <b>{rem}</b> игр"
    )

def shop_item_kb(uid: int, key: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Купить", callback_data=cb_pack(f"shop:buy:{key}", uid)))
    if shop_get_qty(uid, key) > 0 and shop_get_active(uid).get(key, 0) <= 0:
        kb.add(InlineKeyboardButton("Активировать", callback_data=cb_pack(f"shop:act:{key}", uid)))
    kb.add(InlineKeyboardButton("Назад", callback_data=cb_pack("shop:open", uid)))
    return kb

# WORK / JOBS
@dataclass
class JobDef:
    key: str
    title: str
    base_salary_cents: int
    hours: int
    success_pct: int
    fail_texts: List[str]
    ranks: List[Tuple[int, str]]  

_jobs_cache: Dict[str, JobDef] = {}
_jobs_mtime: int = 0

def _normalize_job_key(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-z0-9а-яё_]+", "", s, flags=re.IGNORECASE)
    return s[:32] or "job"

def _ensure_jobs_file():
    if os.path.exists(JOBS_PATH):
        return
    sample = """[ Кассир
Зарплата 120
Длительность рабочего дня 6
Шанс на успех 80%
Опять зависла касса;Клиент устроил скандал;Пересчитали выручку - недостача
Должности:
0 - Стажёр
7 - Кассир
30 - Старший кассир
]

[ Курьер
Зарплата 90
Длительность рабочего дня 5
Шанс на успех 85%
Попал под дождь и промок;Адрес оказался неверным;Сломался велосипед
Должности:
0 - Стажёр
10 - Курьер
40 - Опытный курьер
]
"""
    with open(JOBS_PATH, "w", encoding="utf-8") as f:
        f.write(sample)

def load_jobs() -> Dict[str, JobDef]:
    global _jobs_cache, _jobs_mtime
    _ensure_jobs_file()

    try:
        mtime = int(os.path.getmtime(JOBS_PATH))
    except Exception:
        mtime = 0

    if _jobs_cache and mtime == _jobs_mtime:
        return _jobs_cache

    txt = ""
    with open(JOBS_PATH, "r", encoding="utf-8") as f:
        txt = f.read()

    blocks = re.findall(r"\[\s*(.*?)\s*\]", txt, flags=re.S)
    jobs: Dict[str, JobDef] = {}

    for block in blocks:
        lines = [ln.strip() for ln in block.splitlines() if ln.strip()]
        if not lines:
            continue

        title = lines[0]
        key = _normalize_job_key(title)

        base_salary = 0
        hours = 0
        success_pct = 0
        fail_texts: List[str] = []
        ranks: List[Tuple[int, str]] = []

        in_ranks = False
        for ln in lines[1:]:
            if ln.lower().startswith("зарплата"):
                m = re.search(r"(\d+(?:[.,]\d+)?)", ln)
                if m:
                    base_salary = money_to_cents(m.group(1)) or 0
            elif ln.lower().startswith("длительность"):
                m = re.search(r"(\d+)", ln)
                if m:
                    hours = int(m.group(1))
            elif ln.lower().startswith("шанс"):
                m = re.search(r"(\d+)\s*%", ln)
                if m:
                    success_pct = max(0, min(100, int(m.group(1))))
            elif ln.lower().startswith("должности"):
                in_ranks = True
            else:
                if in_ranks:
                    m = re.match(r"(\d+)\s*-\s*(.+)$", ln)
                    if m:
                        ranks.append((int(m.group(1)), m.group(2).strip()))
                else:
                    if ";" in ln:
                        fail_texts.extend([x.strip() for x in ln.split(";") if x.strip()])
                    else:
                        fail_texts.append(ln)

        ranks.sort(key=lambda x: x[0])
        if not ranks:
            ranks = [(0, "Стажёр")]

        if base_salary <= 0 or hours <= 0:
            continue

        jobs[key] = JobDef(
            key=key,
            title=title,
            base_salary_cents=int(base_salary),
            hours=int(hours),
            success_pct=int(success_pct or 75),
            fail_texts=fail_texts or ["Неудачный рабочий день."],
            ranks=ranks
        )

    _jobs_cache = jobs
    _jobs_mtime = mtime
    return jobs

def get_work_stats(uid: int, job_key: str) -> Tuple[int, int, int]:
    cur.execute("INSERT OR IGNORE INTO work_stats (user_id, job_key) VALUES (?,?)", (uid, job_key))
    conn.commit()
    cur.execute("SELECT shifts, days, earned_cents FROM work_stats WHERE user_id=? AND job_key=?", (uid, job_key))
    r = cur.fetchone()
    return (int(r[0] or 0), int(r[1] or 0), int(r[2] or 0))

def _rank_for_days(job: JobDef, days: int) -> str:
    rank = job.ranks[0][1]
    for need, title in job.ranks:
        if days >= need:
            rank = title
        else:
            break
    return rank

def _salary_with_seniority(job: JobDef, days: int) -> int:
    thresholds = 0
    for need, _ in job.ranks:
        if days >= need:
            thresholds += 1
    mult = 1.0 + 0.05 * max(0, thresholds - 1)
    return int(round(job.base_salary_cents * mult))

def get_current_shift(uid: int):
    cur.execute("SELECT user_id, job_key, started_ts, ends_ts, salary_full_cents, success_pct FROM work_shift WHERE user_id=?", (uid,))
    return cur.fetchone()

def start_shift(uid: int, job_key: str) -> Tuple[int, int]:
    jobs = load_jobs()
    job = jobs.get(job_key)
    if not job:
        raise ValueError("Unknown job")

    shifts, days, earned = get_work_stats(uid, job_key)

    salary_full = _salary_with_seniority(job, days)
    ends_ts = now_ts() + int(job.hours) * 3600

    cur.execute("""
    INSERT INTO work_shift (user_id, job_key, started_ts, ends_ts, salary_full_cents, success_pct)
    VALUES (?,?,?,?,?,?)
    ON CONFLICT(user_id) DO UPDATE SET
      job_key=excluded.job_key,
      started_ts=excluded.started_ts,
      ends_ts=excluded.ends_ts,
      salary_full_cents=excluded.salary_full_cents,
      success_pct=excluded.success_pct
    """, (uid, job_key, now_ts(), ends_ts, int(salary_full), int(job.success_pct)))
    conn.commit()
    return ends_ts, salary_full

def finish_shift(uid: int):
    row = get_current_shift(uid)
    if not row:
        return

    _uid, job_key, started_ts, ends_ts, salary_full_cents, success_pct = row
    if now_ts() < int(ends_ts):
        return

    jobs = load_jobs()
    job = jobs.get(job_key)
    if not job:
        job = JobDef(job_key, job_key, int(salary_full_cents), 1, int(success_pct), ["Неудача."], [(0, "Стажёр")])

    roll = random.randint(1, 100)
    success = 1 if roll <= int(success_pct) else 0
    if success:
        paid = int(salary_full_cents)
        text = "Рабочий день прошёл успешно."
    else:
        paid = int(round(int(salary_full_cents) * 0.10))
        text = random.choice(job.fail_texts) if job.fail_texts else "Неудачный день."

    paid_after_slave = apply_slave_cut(uid, paid, reason="work")

    add_balance(uid, paid_after_slave)

    cur.execute("""
    INSERT INTO work_stats (user_id, job_key, shifts, days, earned_cents)
    VALUES (?,?,?,?,?)
    ON CONFLICT(user_id, job_key) DO UPDATE SET
      shifts = work_stats.shifts + 1,
      days = work_stats.days + 1,
      earned_cents = work_stats.earned_cents + excluded.earned_cents
    """, (uid, job_key, 1, 1, int(paid_after_slave)))
    conn.commit()

    cur.execute("""
    INSERT INTO work_history (user_id, job_key, started_ts, ends_ts, success, paid_cents, text)
    VALUES (?,?,?,?,?,?,?)
    """, (uid, job_key, int(started_ts), int(ends_ts), int(success), int(paid_after_slave), text))
    conn.commit()

    cur.execute("DELETE FROM work_shift WHERE user_id=?", (uid,))
    conn.commit()

    try:
        money_s = cents_to_money_str(paid_after_slave)
        bot.send_message(uid, f"Смена завершена: <b>{html_escape(job.title)}</b>\n{text}\nНачислено на ваш счёт: <b>{money_s}</b>$", parse_mode="HTML")
    except Exception:
        pass

def has_work_history(uid: int) -> bool:
    cur.execute("SELECT 1 FROM work_history WHERE user_id=? LIMIT 1", (uid,))
    return cur.fetchone() is not None

def _format_duration(seconds: int) -> str:
    seconds = max(0, int(seconds))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    if h <= 0:
        return f"{m} мин"
    return f"{h} ч {m} мин"

# ROULETTE (1x3) helpers
R_EMO = {
    0: "💀",
    1: "🍒",
    2: "🍀",
    3: "🍋",
    4: "7⃣",
    5: "👹",
}
# 💀 1/25, 👹 1/25, 7⃣ 5/25, стандартные 🍒🍀🍋 18/25
R_WEIGHTS_1x3 = [
    (0, 1),   # 💀
    (5, 1),   # 👹
    (4, 5),   # 7⃣
    (1, 6),   # 🍒
    (2, 6),   # 🍀
    (3, 6),   # 🍋
]
# 3x3: такие же шансы, как 1x3
R_WEIGHTS_3x3 = R_WEIGHTS_1x3
# 3x5: 👹 1/35, 7⃣ 6/35, 🍋🍀🍒 суммарно 25/35 (делим 9/8/8), 💀 3/35
R_WEIGHTS_3x5 = [
    (5, 1),   # 👹
    (4, 6),   # 7⃣
    (1, 9),   # 🍒
    (2, 8),   # 🍀
    (3, 8),   # 🍋
    (0, 3),   # 💀
]

def weighted_pick(pairs):
    total = sum(w for _, w in pairs)
    r = random.randint(1, total)
    s = 0
    for val, w in pairs:
        s += w
        if r <= s:
            return val
    return pairs[-1][0]

def roulette_weights_for(uid: int, rfmt: str, game_id: str | None = None):
    """
    Возвращает модифицированные веса под активные эффекты магазина.
    Ожидается формат списка: [(code, weight), ...]
    code: 0..5 (💀🍒🍀🍋7⃣👹)
    """
    if rfmt == "1x3":
        base = list(R_WEIGHTS_1x3)
    elif rfmt == "3x3":
        base = list(R_WEIGHTS_3x3)
    else:
        base = list(R_WEIGHTS_3x5)
    active = shop_get_active_for_game(uid, game_id) if game_id else shop_get_active(uid)

    mul = {0: 1.0, 1: 1.0, 2: 1.0, 3: 1.0, 4: 1.0, 5: 1.0}

    # Магнит: стандартные 🍒🍀🍋 (1,2,3) +10%
    if active.get("magnet", 0) > 0:
        for c in (1, 2, 3):
            mul[c] *= 1.10

    # Вино: 7⃣ (4) и 💀 (0) +20%
    if active.get("wine", 0) > 0:
        mul[4] *= 1.20
        mul[0] *= 1.20

    out = []
    for code, w in base:
        code = int(code)
        w2 = int(round(float(w) * mul.get(code, 1.0)))
        out.append((code, max(1, w2)))
    return out

def apply_fake_clover_to_state(uid: int, rfmt: str, state, game_id: str | None = None):
    active = shop_get_active_for_game(uid, game_id) if game_id else shop_get_active(uid)
    if active.get("fake_clover", 0) <= 0:
        return state

    # выбираем 1 клетку и форсим: 50% 7⃣(4) иначе 💀(0)
    forced = 4 if random.random() < 0.5 else 0

    if rfmt == "1x3":
        idx = random.randrange(3)
        st = list(state)
        st[idx] = forced
        return st

    # 3x3 или 3x5
    rows = len(state)
    cols = len(state[0]) if rows else 0
    r = random.randrange(rows)
    c = random.randrange(cols)
    st = [list(row) for row in state]
    st[r][c] = forced
    return st

def render_1x3(codes: List[int]) -> str:
    return "".join(R_EMO[c] for c in codes)

def render_3x3(grid: List[List[int]]) -> str:
    return "\n".join("".join(R_EMO[c] for c in row) for row in grid)

def render_3x5(grid: List[List[int]]) -> str:
    return "\n".join("".join(R_EMO[c] for c in row) for row in grid)

def empty_grid_text(fmt: str) -> str:
    if fmt == "1x3":
        return "🔲🔲🔲"
    if fmt == "3x3":
        return "🔲🔲🔲\n🔲🔲🔲\n🔲🔲🔲"
    if fmt == "3x5":
        return "🔲🔲🔲🔲🔲\n🔲🔲🔲🔲🔲\n🔲🔲🔲🔲🔲"
    return "🔲"

def pepper_triggers_demon(state, rfmt: str) -> bool:
    """Триггер для 'Перца дьявола': 3💀 в 1×3/3×3 (по линии), 5💀 по строке в 3×5."""
    try:
        if rfmt == "1x3":
            return (isinstance(state, list) and len(state) == 3 and all(int(x) == 0 for x in state))

        if rfmt == "3x3":
            g = state
            if not g or len(g) != 3 or len(g[0]) != 3:
                return False
            # строки
            for r in range(3):
                if all(int(g[r][c]) == 0 for c in range(3)):
                    return True
            # столбцы
            for c in range(3):
                if all(int(g[r][c]) == 0 for r in range(3)):
                    return True
            # диагонали
            if all(int(g[i][i]) == 0 for i in range(3)):
                return True
            if all(int(g[i][2 - i]) == 0 for i in range(3)):
                return True
            return False

        if rfmt == "3x5":
            g = state
            if not g or len(g) != 3 or len(g[0]) != 5:
                return False
            # 5💀 подряд — трактуем как строка из пяти 💀
            for r in range(3):
                if all(int(g[r][c]) == 0 for c in range(5)):
                    return True
            return False
    except Exception:
        return False

    return False

def calc_delta_1x3(codes: List[int], stake_cents: int) -> int:
    """
    Возвращает изменение баланса (в центах) за ход.
    Правила - упрощённо/логично по твоему ТЗ:
    - если есть 💀: штраф зависит от количества 💀 (1=-2x, 2=-3x, 3=-(всё) и долг -2x) -> долг реализуем позже; сейчас: -5x
    - 7⃣: если 1 шт -> 1x, 2 -> +2x, 3 -> +3x
    - 👹: 1 -> 1x, 2 -> +4x, 3 -> +5x
    - 🍒🍀🍋:
        3 одинаковых -> +0.1x
        2 одинаковых рядом -> 0
        иначе -> -1x
    """
    stake = int(stake_cents)

    skulls = codes.count(0)
    if skulls > 0:
        if skulls == 1:
            return -2 * stake
        if skulls == 2:
            return -3 * stake
        return -5 * stake

    sevens = codes.count(4)
    if sevens > 0:
        if sevens == 1:
            return -1 * stake
        if sevens == 2:
            return +2 * stake
        return +3 * stake

    demons = codes.count(5)
    if demons > 0:
        if demons == 1:
            return -1 * stake
        if demons == 2:
            return +4 * stake
        return +5 * stake

    if codes[0] == codes[1] == codes[2]:
        return int(round(0.1 * stake))
    if codes[0] == codes[1] or codes[1] == codes[2]:
        return 0
    return -1 * stake

def calc_line_delta_len3(codes: List[int], stake_cents: int) -> int:
    return calc_delta_1x3(codes, stake_cents)

def calc_delta_3x3(grid: List[List[int]], stake_cents: int) -> int:
    stake = int(stake_cents)
    total = 0

    for r in range(3):
        total += calc_line_delta_len3(grid[r], stake)

    for c in range(3):
        col = [grid[r][c] for r in range(3)]
        total += calc_line_delta_len3(col, stake)

    d1 = [grid[i][i] for i in range(3)]
    if d1[0] == d1[1] == d1[2]:
        total += calc_line_delta_len3(d1, stake)

    d2 = [grid[i][2 - i] for i in range(3)]
    if d2[0] == d2[1] == d2[2]:
        total += calc_line_delta_len3(d2, stake)

    return total

def _max_run_len(row: List[int], sym: int) -> int:
    best = 0
    cur_run = 0
    for x in row:
        if x == sym:
            cur_run += 1
            best = max(best, cur_run)
        else:
            cur_run = 0
    return best

def _has_run_len(row: List[int], sym: int, n: int) -> bool:
    return _max_run_len(row, sym) >= n

def _skull_penalty_row5(row: List[int], stake: int) -> int:
    skulls = row.count(0)
    if skulls >= 3:
        if skulls == 3:
            return -1 * stake
        if skulls == 4:
            return -3 * stake
        # 5 skulls: "минус все и долг"
        return -5 * stake

    # 2 подряд из пяти -> -0.2 ставки
    if skulls == 2 and _has_run_len(row, 0, 2):
        return int(round(-0.2 * stake))

    # 1 skull "ничего"
    return 0

def calc_row_delta_3x5(row: List[int], stake_cents: int) -> int:
    stake = int(stake_cents)

    # сначала 💀 (они могут полностью перебить)
    skull_pen = _skull_penalty_row5(row, stake)
    if skull_pen != 0:
        return skull_pen

    # 👹 джекпот/крупные серии
    if _has_run_len(row, 5, 5):  # 👹👹👹👹👹
        return 666 * stake
    if _has_run_len(row, 5, 4):  # 👹 x4
        return 6 * stake

    # 7⃣ серии
    if _has_run_len(row, 4, 5):
        return 5 * stake
    if _has_run_len(row, 4, 4):
        return 4 * stake

    # стандарт 🍒🍀🍋 серии (любой из этих)
    for sym in (1, 2, 3):
        if _has_run_len(row, sym, 5):
            return 2 * stake
        if _has_run_len(row, sym, 4):
            return 1 * stake
        if _has_run_len(row, sym, 3):
            return int(round(0.5 * stake))

    return 0

def calc_delta_3x5(grid: List[List[int]], stake_cents: int) -> int:
    stake = int(stake_cents)
    total = 0

    for r in range(3):
        total += calc_row_delta_3x5(grid[r], stake)

    for c in range(5):
        col = [grid[r][c] for r in range(3)]
        total += calc_line_delta_len3(col, stake)

    for c0 in range(0, 3):
        d = [grid[0][c0], grid[1][c0+1], grid[2][c0+2]]
        if d[0] == d[1] == d[2]:
            total += calc_line_delta_len3(d, stake)

    for c0 in range(2, 5):
        d = [grid[0][c0], grid[1][c0-1], grid[2][c0-2]]
        if d[0] == d[1] == d[2]:
            total += calc_line_delta_len3(d, stake)

    return total

def debt_mult_from_skulls(state, rfmt: str) -> int:
    """
    Возвращает множитель долга, если выпал "долговой черепной исход".
    0 = долга нет.

    Условия по твоему ТЗ:
    - 3×3: любая линия 💀💀💀 (гор/верт/диаг) => долг 2×ставка (вайп + долг)
    - 3×5: строка 💀×5 => долг 5×ставка (вайп + долг)
           любая линия 💀💀💀 (вертикаль или диагональ длины 3) => долг 2×ставка (вайп + долг)
    - 1×3: 💀💀💀 => долг 2×ставка (вайп + долг)
    """
    # state может быть list[int] (1x3) или list[list[int]] (3x3/3x5)
    try:
        if rfmt == "1x3":
            codes = list(state)
            return 2 if len(codes) == 3 and codes.count(0) == 3 else 0

        if rfmt == "3x3":
            g = state  # 3x3 grid
            # rows
            for r in range(3):
                if g[r][0] == g[r][1] == g[r][2] == 0:
                    return 2
            # cols
            for c in range(3):
                if g[0][c] == g[1][c] == g[2][c] == 0:
                    return 2
            # diags
            if g[0][0] == g[1][1] == g[2][2] == 0:
                return 2
            if g[0][2] == g[1][1] == g[2][0] == 0:
                return 2
            return 0

        # 3x5
        g = state
        best = 0

        # строка из 5 черепов => долг 5x
        for r in range(3):
            if all(x == 0 for x in g[r]):
                best = max(best, 5)

        # вертикаль длины 3 => долг 2x
        for c in range(5):
            if g[0][c] == g[1][c] == g[2][c] == 0:
                best = max(best, 2)

        # диагонали длины 3 (лево->право)
        for c0 in range(0, 3):
            if g[0][c0] == g[1][c0+1] == g[2][c0+2] == 0:
                best = max(best, 2)

        # диагонали длины 3 (право->лево)
        for c0 in range(2, 5):
            if g[0][c0] == g[1][c0-1] == g[2][c0-2] == 0:
                best = max(best, 2)

        return best
    except Exception:
        return 0

#Shop callback
@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("shop:"))
def on_shop_callbacks(call: CallbackQuery):
    base, owner = cb_unpack(call.data)
    clicker = call.from_user.id

    if owner is not None and clicker != owner:
        bot.answer_callback_query(call.id, "Вы не можете нажать на эту кнопку", show_alert=True)
        return

    parts = base.split(":")
    action = parts[1] if len(parts) > 1 else "open"
    uid = owner if owner is not None else clicker

    u = get_user(uid)
    if not u or not u[2]:
        edit_inline_or_message(call, "Вход посторонним воспрещён", None, "HTML")
        bot.answer_callback_query(call.id)
        return

    if action == "open":
        text = shop_menu_text(uid)
        kb = shop_menu_kb(uid)
        edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        return

    if action == "item" and len(parts) >= 3:
        key = parts[2]
        if key not in SHOP_ITEMS:
            bot.answer_callback_query(call.id, "Товар не найден.", show_alert=True)
            return
        text = shop_item_text(uid, key)
        kb = shop_item_kb(uid, key)
        edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        return

    if action == "buy" and len(parts) >= 3:
        key = parts[2]
        ok, msg = shop_buy(uid, key)
        bot.answer_callback_query(call.id, msg, show_alert=not ok)
        if key in SHOP_ITEMS:
            text = shop_item_text(uid, key)
            kb = shop_item_kb(uid, key)
            edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
        return

    if action == "act" and len(parts) >= 3:
        key = parts[2]
        ok, msg = shop_activate(uid, key)
        bot.answer_callback_query(call.id, msg, show_alert=not ok)
        if key in SHOP_ITEMS:
            text = shop_item_text(uid, key)
            kb = shop_item_kb(uid, key)
            edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
        return

    bot.answer_callback_query(call.id)

# Callback protection 
CB_SEP = "|"

def cb_pack(base: str, owner_id: int) -> str:
    return f"{base}{CB_SEP}{owner_id}"

def cb_unpack(data: str) -> Tuple[str, Optional[int]]:
    if CB_SEP in data:
        base, tail = data.rsplit(CB_SEP, 1)
        if tail.isdigit():
            return base, int(tail)
    return data, None

@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("mail:open"))
def on_mail_open(call: CallbackQuery):
    base, owner = cb_unpack(call.data)
    uid = call.from_user.id
    if owner is not None and uid != owner:
        bot.answer_callback_query(call.id, "Вы не можете нажать на эту кнопкe", show_alert=True)
        return

    cur.execute("SELECT pending_amt_cents, pending_kind, pending_msg_id, stopped FROM daily_mail WHERE user_id=?", (uid,))
    row = cur.fetchone()
    if not row:
        bot.answer_callback_query(call.id, "Письмо не найдено.", show_alert=True)
        return
    amt_cents, kind, msg_id, stopped = int(row[0] or 0), (row[1] or ""), int(row[2] or 0), int(row[3] or 0)
    if stopped == 1:
        bot.answer_callback_query(call.id, "Письма больше не приходят.", show_alert=True)
        return
    if amt_cents <= 0 or msg_id == 0:
        bot.answer_callback_query(call.id, "Письмо уже открыто.", show_alert=True)
        return
    if call.message and call.message.message_id != msg_id:
        bot.answer_callback_query(call.id, "Это старое письмо.", show_alert=True)
        return

    add_balance(uid, amt_cents)

    text = _mail_letter_text(kind, amt_cents)
    try:
        limited_edit_message_text(text=text, chat_id=uid, msg_id=msg_id, reply_markup=None, parse_mode="HTML")
    except Exception:
        try:
            bot.send_message(uid, text, parse_mode="HTML")
        except Exception:
            pass

    cur.execute("UPDATE daily_mail SET pending_amt_cents=0, pending_kind=NULL, pending_msg_id=0 WHERE user_id=?", (uid,))
    conn.commit()

    bot.answer_callback_query(call.id)

def compute_group_key_from_callback(call: CallbackQuery, prefix_len=PREFIX_LEN) -> Optional[str]:
    if getattr(call, "message", None) and getattr(call.message, "chat", None):
        return f"chat:{call.message.chat.id}"
    inline_id = getattr(call, "inline_message_id", None)
    if inline_id:
        return f"inline_pref:{inline_id[:prefix_len]}"
    return None

def edit_inline_or_message(call: CallbackQuery, text: str, reply_markup=None, parse_mode: Optional[str] = None):
    inline_id = getattr(call, "inline_message_id", None)
    if inline_id:
        limited_edit_message_text(text=text, inline_id=inline_id, reply_markup=reply_markup, parse_mode=parse_mode)
        return
    if getattr(call, "message", None):
        limited_edit_message_text(
            text=text,
            chat_id=call.message.chat.id,
            msg_id=call.message.message_id,
            reply_markup=reply_markup,
            parse_mode=parse_mode
        )
        return

# INLINE MENU
def inline_article(title: str, desc: str, text: str, kb: InlineKeyboardMarkup) -> InlineQueryResultArticle:
    return InlineQueryResultArticle(
        id=str(uuid.uuid4()),
        title=title,
        description=desc,
        input_message_content=InputTextMessageContent(text, parse_mode="HTML"),
        reply_markup=kb
    )

@bot.inline_handler(func=lambda q: True)
def on_inline(q: InlineQuery):
    uid = q.from_user.id
    username = getattr(q.from_user, "username", None)
    upsert_user(uid, username) 

    query_text = (q.query or "").strip()

    results = []
    u = get_user(uid)
    if not is_registered(uid) or (u and u[2] is None):
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("Открыть конверт?", url=f"https://t.me/{BOT_USERNAME}?start=contract"))
        results.append(inline_article(
            "Добро пожаловать",
            "",
            "Вам прислал письмо анонимный доброжелатель",
            kb
            ))
        bot.answer_inline_query(q.id, results, cache_time=0)
        return

    stake_cents = None
    m = re.search(r"\b(\d+(?:[.,]\d+)?)\b", query_text)
    if m:
        stake_cents = money_to_cents(m.group(1))

    u_me = get_user(uid)
    is_demon_me = bool(u_me and int(u_me[7] or 0) == 1)
    
    qt_low = (query_text or "").lower()
    life_flag = is_demon_me and any(w in qt_low for w in ["жизн", "life"])
    
    # если демон пишет только "жизнь" без числа — дефолт 1000$
    if life_flag and stake_cents is None:
        stake_cents = 1000 * 100

    # Начать игру  
    if stake_cents is None:
        text = "Не думай, что всё так просто. Сделай ставку, введи сумму"
        results.append(inline_article(
            "Начать игру",
            "Сделай свою ставку",
            text,
            None
        ))
    elif stake_cents <= 0:
        text = "Мы не работаем в долг. Сделай ставку, введи сумму"
        results.append(inline_article(
            "Начать игру",
            "Сделай свою ставку",
            text,
            None
        ))
    else:
        kb = InlineKeyboardMarkup()
        if life_flag:
            kb.add(InlineKeyboardButton(
                "Слот автомат / Рулетка",
                callback_data=cb_pack(f"game:start:roulette:life:{stake_cents}", uid)
            ))
            kb.add(InlineKeyboardButton(
                "Марафон рулетка",
                callback_data=cb_pack(f"game:start:cross:life:{stake_cents}", uid)
            ))
        else:
            kb.add(InlineKeyboardButton(
                "Слот автомат / Рулетка",
                callback_data=cb_pack(f"game:start:roulette:{stake_cents}", uid)
            ))
            kb.add(InlineKeyboardButton(
                "Марафон рулетка",
                callback_data=cb_pack(f"game:start:cross:{stake_cents}", uid)
            ))
        if life_flag:
            game_text = (
                "<b><u>⟢♣♦ Игры ♥♠⟣</u></b>\n\n"
                "Текущая ставка: <b>ҖนՅዙ৮</b>\n"
                f"Расчётная ставка: <b>{cents_to_money_str(stake_cents)}</b>$\n"
                "Выберите игру:"
            )
        else:
            game_text = (
                "<b><u>⟢♣♦ Игры ♥♠⟣</u></b>\n\n"
                f"Текущая ставка: <b>{cents_to_money_str(stake_cents)}</b>$\n"
                "Выберите игру:"
            )
        results.append(inline_article(
            "Начать игру",
            "Выбери игру",
            game_text,
            kb
        ))

    # Работа
    u = get_user(uid)
    if not u or not u[2]:
        results.append(inline_article(
            "Работа",
            "Выбрать вакансию и выйти в смену",
            "Вас ожидают.",
            None
        ))
    else:
        sh = get_current_shift(uid)
        if sh and now_ts() < int(sh[3]):
            job_key = sh[1]
            jobs = load_jobs()
            job = jobs.get(job_key)
            job_title = job.title if job else job_key
            left = int(sh[3]) - now_ts()
            text = (
                f"Имя: <b>{html_escape(u[2])}</b>" + (f" (@{html_escape(u[1])})" if u[1] else "") +
                f"\n\nРаботает по вакансии <b>{html_escape(job_title)}</b>\n"
                f"Вернётся через: <b>{_format_duration(left)}</b>"
            )
            results.append(inline_article(
                "Работа",
                "Текущая смена",
                text,
                None
            ))
        else:
            jobs = load_jobs()
            if not jobs:
                results.append(inline_article(
                    "Работа",
                    "Выбрать вакансию и выйти в смену",
                    "Файл jobs.txt пуст или сломан.",
                    None
                ))
            else:
                rows = db_all("SELECT job_key, shifts FROM work_stats WHERE user_id=?", (uid,))
                if not rows:
                    position = "Безработный"
                    seniority_days = 0
                else:
                    rows2 = [(r[0], int(r[1] or 0)) for r in rows]
                    mx = max(s for _, s in rows2)
                    best = [jk for jk, s in rows2 if s == mx and mx > 0]
                    if len(best) != 1:
                        position = "Разнорабочий"
                    else:
                        jk = best[0]
                        job = jobs.get(jk)
                        _, days, _ = get_work_stats(uid, jk)
                        position = _rank_for_days(job, days) if job else "Работник"
                    seniority_days = sum(get_work_stats(uid, r[0])[1] for r in rows2)

                text = (
                    f"Имя: <b>{html_escape(u[2])}</b>" + (f" (@{html_escape(u[1])})" if u[1] else "") +
                    f"\nСтаж: <b>{seniority_days} дней</b>\n"
                    f"Должность: <b>{html_escape(position)}</b>\n\n"
                    "Выбери сегодняшнюю вакансию:"
                )

                kb = InlineKeyboardMarkup()
                for jk, job in jobs.items():
                    kb.add(InlineKeyboardButton(job.title, callback_data=cb_pack(f"work:pick:{jk}", uid)))

                results.append(inline_article(
                    "Работа",
                    "Выбрать вакансию и выйти в смену",
                    text,
                    kb
                ))

    # Профиль
    u = get_user(uid)
    if not u or not u[2]:
        results.append(inline_article(
            "Профиль",
            "Основная сводка по вашей деятельности",
            "Вас ожидают.",
            None
        ))
    else:
        uid2, uname, short_name, created_ts, contract_ts, bal, gift, demon = u
        cur.execute("SELECT user_id FROM users WHERE demon=0")
        uids = [r[0] for r in cur.fetchall()]
        uids.sort(key=lambda x: top_value_cents(x), reverse=True)
        place = (uids.index(uid2) + 1) if (demon == 0 and uid2 in uids) else "-"

        status = compute_status(uid)

        text = (
            f"Имя пользователя: <i>{html_escape(short_name)}</i>\n"
            f"Дата подписания контракта: <b>{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(contract_ts or created_ts or now_ts()))}</b>\n"
            f"Статус: <b>{html_escape(status)}</b>\n"
            f"Капитал: <b>{cents_to_money_str(int(bal or 0))}</b>$\n"
            f"Место в топе: <b>{place}</b>"
        )

        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("Статистика по играм", callback_data=cb_pack("profile:games", uid)))
        if has_work_history(uid):
            kb.add(InlineKeyboardButton("Трудовая книга", callback_data=cb_pack("profile:workbook", uid)))
        if owns_slaves(uid):
            kb.add(InlineKeyboardButton("Список рабов", callback_data=cb_pack("profile:slaves", uid)))

        results.append(inline_article(
            "Профиль",
            "Основная сводка по вашей деятельности",
            text,
            kb
        ))

    # Статистика
    cur.execute("SELECT user_id FROM users WHERE demon=0")
    all_uids = [r[0] for r in cur.fetchall()]
    all_uids.sort(key=lambda u: top_value_cents(u), reverse=True)

    header = "📄<b><u>Статистика</u></b>\n\n"
    lines = []
    top15 = all_uids[:15]
    for i2, uid_top in enumerate(top15, start=1):
        lines.append(format_user_line(uid_top, i2, uid))

    if uid in all_uids:
        my_place = all_uids.index(uid) + 1
        if my_place > 15:
            lines.append("…")
            lines.append(format_user_line(uid, my_place, uid))

    text = header + "\n".join(lines if lines else ["Пусто"])
    results.append(inline_article(
        "Статистика",
        "Топ 15 игроков с наибольшим доходом",
        text,
        None
    ))

    bot.answer_inline_query(q.id, results, cache_time=0)

# /start
@bot.message_handler(commands=["start"])
def cmd_start(message):
    uid = message.from_user.id
    username = getattr(message.from_user, "username", None)
    upsert_user(uid, username)

    parts = message.text.split(maxsplit=1)
    payload = parts[1].strip() if len(parts) > 1 else ""

    if payload != "contract":
        return

    u = get_user(uid)
    if u and (u[2] is not None) and (u[4] is not None):
        return

    text = "Из конверта выглядывает строка для вашей росписи. Оставить подпись?"
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Выкинуть подозрительное письмо", callback_data=cb_pack("reg:throw", uid)))
    kb.add(InlineKeyboardButton("Подписать", callback_data=cb_pack("reg:sign", uid)))
    sent = bot.send_message(message.chat.id, text, reply_markup=kb)
    set_reg_state(uid, "await_name", sent.message_id)

# REGISTRATION callbacks
@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("reg:"))
def on_reg_callbacks(call: CallbackQuery):
    base, owner = cb_unpack(call.data)
    clicker = call.from_user.id

    if owner is not None and clicker != owner:
        bot.answer_callback_query(call.id, "Вы не можете нажать на эту кнопку", show_alert=True)
        return

    action = base.split(":", 1)[1] if ":" in base else ""

    if action == "throw":
        stage, msg_id = get_reg_state(clicker)
        u = get_user(clicker)
        contract_ts = int((u[4] if u else 0) or 0)
    
        if contract_ts > 0:
            try:
                set_reg_state(clicker, None, None)
            except Exception:
                pass
            try:
                if getattr(call, "message", None):
                    bot.delete_message(call.message.chat.id, call.message.message_id)
            except Exception:
                pass
            bot.answer_callback_query(call.id, "Письмо выброшено.")
            return
    
        wipe_user(clicker)
        try:
            if getattr(call, "message", None):
                bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception:
            pass
        bot.answer_callback_query(call.id, "Данные удалены.")
        return

    if action == "sign":
        stage, msg_id = get_reg_state(clicker)
        if getattr(call, "message", None):
            new_text = "Из конверта выглядывает строка для вашей росписи. Оставить подпись?\n(введите короткое имя)"
            try:
                limited_edit_message_text(text=new_text, chat_id=call.message.chat.id, msg_id=call.message.message_id, reply_markup=None, parse_mode=None)
                set_reg_state(clicker, "await_name", call.message.message_id)
            except Exception:
                pass
        bot.answer_callback_query(call.id)
        return

# Name capture 
@bot.message_handler(func=lambda m: m.chat.type == "private" and m.text and not m.text.startswith("/"))
def on_private_text(message):
    uid = message.from_user.id
    username = getattr(message.from_user, "username", None)
    upsert_user(uid, username)

    stage, msg_id = get_reg_state(uid)
    if stage != "await_name" or not msg_id:
        return

    txt = (message.text or "").strip()
    if not re.fullmatch(r"[^\s]{1,24}", txt):
        return

    set_short_name(uid, txt)
    u = get_user(uid)
    contract = load_contract_text()
    rendered = safe_format(
        contract,
        name=html_escape(txt),
        username=html_escape(u[1] or ""),
        date=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now_ts())),
        user_id=str(uid),
    )

    try:
        limited_edit_message_text(text=rendered, chat_id=message.chat.id, msg_id=msg_id, parse_mode="HTML", reply_markup=None)
    except Exception:
        bot.send_message(message.chat.id, rendered, parse_mode="HTML")

    gift = 1000 * 100
    set_contract_signed(uid, gift)

    bot.send_message(message.chat.id, "<i>В конверте также лежал чек на сумму <b>1000$</b>. Подпись:</i> Дополнительная финансовая поддержка придёт позже. Куратор.", parse_mode="HTML")

    set_reg_state(uid, None, None)

# STATS / PROFILE / WORK / GAME callbacks
def format_user_line(uid: int, place: int, highlight_uid: int) -> str:
    cur.execute("SELECT short_name, username FROM users WHERE user_id=?", (uid,))
    row = cur.fetchone()
    name = row[0] or "Без имени"
    uname = row[1] or ""
    val = top_value_cents(uid)
    money = cents_to_money_str(val)
    name_html = f"<b>{html_escape(name)}</b>"
    if uid == highlight_uid:
        name_html = f"<b><u>{html_escape(name)}</u></b>"
    uname_part = f" (@{html_escape(uname)})" if uname else ""
    return f"{place}. {name_html}{uname_part} - <b>{money}</b>$"

@bot.callback_query_handler(func=lambda c: c.data and (c.data.startswith("stats:") or c.data.startswith("profile:") or c.data.startswith("work:") or c.data.startswith("game:")))
def on_main_callbacks(call: CallbackQuery):
    base, owner = cb_unpack(call.data)
    clicker = call.from_user.id

    if owner is not None and clicker != owner:
        bot.answer_callback_query(call.id, "Вы не можете нажать на эту кнопку", show_alert=True)
        return

    group_key = compute_group_key_from_callback(call)

    parts = base.split(":")
    kind = parts[0]

    #STATS TOP
    if kind == "stats" and parts[1] == "top":
        cur.execute("SELECT user_id FROM users WHERE demon=0")
        all_uids = [r[0] for r in cur.fetchall()]
        all_uids.sort(key=lambda u: top_value_cents(u), reverse=True)

        header = "📄<b><u>Статистика</u></b>\n\n"
        lines = []
        top15 = all_uids[:15]
        for i, uid in enumerate(top15, start=1):
            lines.append(format_user_line(uid, i, clicker))

        if clicker in all_uids:
            my_place = all_uids.index(clicker) + 1
            if my_place > 15:
                lines.append("…")
                lines.append(format_user_line(clicker, my_place, clicker))

        text = header + "\n".join(lines if lines else ["Пусто"])
        edit_inline_or_message(call, text, reply_markup=None, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        return

    # PROFILE
    if kind == "profile" and parts[1] == "open":
        u = get_user(clicker)
        if not u or not u[2]:
            edit_inline_or_message(call, "Вам пришло одно особенное письмо. Рекомендуем вам его проверить.", None, "HTML")
            bot.answer_callback_query(call.id)
            return

        uid, uname, short_name, created_ts, contract_ts, bal, gift, demon = u
        cur.execute("SELECT user_id FROM users WHERE demon=0")
        uids = [r[0] for r in cur.fetchall()]
        uids.sort(key=lambda x: top_value_cents(x), reverse=True)
        place = (uids.index(uid) + 1) if (demon == 0 and uid in uids) else "-"

        status = compute_status(uid)

        text = (
            f"Имя пользователя: <i>{html_escape(short_name)}</i>\n"
            f"Дата подписания контракта: <b>{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(contract_ts or created_ts or now_ts()))}</b>\n"
            f"Статус: <b>{html_escape(status)}</b>\n"
            f"Капитал: <b>{cents_to_money_str(int(bal or 0))}</b>$\n"
            f"Место в топе: <b>{place}</b>"
        )

        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("Статистика по играм", callback_data=cb_pack("profile:games", clicker)))
        if has_work_history(clicker):
            kb.add(InlineKeyboardButton("Трудовая книга", callback_data=cb_pack("profile:workbook", clicker)))
        if owns_slaves(clicker):
            kb.add(InlineKeyboardButton("Список рабов", callback_data=cb_pack("profile:slaves", clicker)))
        if is_slave(clicker):
            kb.add(InlineKeyboardButton("Статус раба", callback_data=cb_pack("profile:slave_status", clicker)))

        edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        return

    if kind == "profile" and parts[1] == "games":
        st = get_game_stats(clicker)
        games_total, wins, losses, max_win, max_lose = st
        pct_w = (wins / games_total * 100.0) if games_total > 0 else 0.0
        pct_l = (losses / games_total * 100.0) if games_total > 0 else 0.0
        text = (
            f"Общее число игр: <b>{games_total}</b>\n"
            f"Часто играет: <i>{html_escape(get_favorite_game_title(clicker))}</i>\n"
            f"Победы: <b>{wins}</b> /<b>{pct_w:.1f}%</b>\n"
            f"Поражения: <b>{losses}</b> /<b>{pct_l:.1f}%</b>\n"
            f"Максимальная выигранная сумма: <b>{cents_to_money_str(max_win)}</b>$\n"
            f"Максимальная проигранная сумма: <b>{cents_to_money_str(max_lose)}</b>$"
        )
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("Назад в профиль", callback_data=cb_pack("profile:open", clicker)))
        edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        return

    if kind == "profile" and parts[1] == "workbook":
        u = get_user(clicker)
        jobs = load_jobs()

        cur.execute("SELECT job_key, shifts, days, earned_cents FROM work_stats WHERE user_id=? ORDER BY shifts DESC", (clicker,))
        rows = cur.fetchall()
        
        if not rows:
            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("Назад в профиль", callback_data=cb_pack("profile:open", clicker)))
            edit_inline_or_message(call, "Ты ещё ни разу не выходил на работу.", reply_markup=kb, parse_mode="HTML")
            bot.answer_callback_query(call.id)
            return
        
        total_days = sum(int(r[2] or 0) for r in rows)
        total_earned = sum(int(r[3] or 0) for r in rows)
        
        mx = max(int(r[1] or 0) for r in rows)
        best = [r for r in rows if int(r[1] or 0) == mx and mx > 0]
        if len(best) != 1:
            pos = "Разнорабочий"
        else:
            jk = best[0][0]
            job = jobs.get(jk)
            pos = _rank_for_days(job, int(best[0][2] or 0)) if job else "Работник"
            
        lines = []
        for jk, shifts, days, earned in rows:
            job = jobs.get(jk)
            title = job.title if job else jk
            lines.append(f"<i>{html_escape(title)}</i> - <b>{int(shifts or 0)}</b>")
            
        text = (
            f"Должность: <i>{html_escape(pos)}</i>\n"
            f"Заработанно: <b>{cents_to_money_str(total_earned)}</b>$\n"
            f"Общий стаж: <b>{total_days}</b> дней\n\n"
            "Работы:\n" + "\n".join(lines)
        )
        
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("Назад в профиль", callback_data=cb_pack("profile:open", clicker)))
        edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        return

    if kind == "profile" and parts[1] == "slaves":
        cur.execute("""
            SELECT slave_id, COALESCE(earned_cents,0), COALESCE(share_bp,0), COALESCE(acquired_ts,0)
            FROM slavery
            WHERE owner_id=?
            ORDER BY COALESCE(earned_cents,0) DESC
        """, (clicker,))
        rows = cur.fetchall()

        if not rows:
            text = "Список вашего второстепенного дохода\n\nПусто"
            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("Назад в профиль", callback_data=cb_pack("profile:open", clicker)))
            edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
            bot.answer_callback_query(call.id)
            return

        lines = ["Список вашего второстепенного дохода"]
        top = rows[:20]
        for i, (slave_id, earned_cents, share_bp, acquired_ts) in enumerate(top, 1):
            slave_id = int(slave_id)
            earned_cents = int(earned_cents or 0)
            last24 = slave_profit_last24h(slave_id, clicker)

            cur.execute("SELECT short_name, username FROM users WHERE user_id=?", (slave_id,))
            r = cur.fetchone() or (None, None)
            sname = r[0] or "Без имени"
            sun = r[1] or ""

            uname_part = f" (@{html_escape(sun)})" if sun else ""
            lines.append(
                f"{i}|<b>{html_escape(sname)}</b>{uname_part} "
                f"<b>{cents_to_money_str(earned_cents)}</b>$ + "
                f"<u><b>{cents_to_money_str(last24)}</b></u>$"
            )

        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("Назад в профиль", callback_data=cb_pack("profile:open", clicker)))
        for (slave_id, _earned, _bp, _acq) in top:
            slave_id = int(slave_id)
            cur.execute("SELECT short_name FROM users WHERE user_id=?", (slave_id,))
            sname = (cur.fetchone() or ("Без имени",))[0] or "Без имени"
            btn_text = sname
            if len(btn_text) > 18:
                btn_text = btn_text[:18] + "…"
            kb.add(InlineKeyboardButton(btn_text, callback_data=cb_pack(f"profile:slavecard:{slave_id}", clicker)))
        
        edit_inline_or_message(call, "\n".join(lines), reply_markup=kb, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        return
    
    if kind == "profile" and parts[1] == "slavecard":
            try:
                slave_id = int(parts[2])
            except Exception:
                bot.answer_callback_query(call.id, "Ошибка.", show_alert=True)
                return
    
            cur.execute("""
                SELECT COALESCE(earned_cents,0), COALESCE(share_bp,0), COALESCE(acquired_ts,0)
                FROM slavery
                WHERE slave_id=? AND owner_id=?
            """, (slave_id, clicker))
            row = cur.fetchone()
            if not row:
                kb = InlineKeyboardMarkup()
                kb.add(InlineKeyboardButton("Назад в профиль", callback_data=cb_pack("profile:open", clicker)))
                edit_inline_or_message(call, "Вы не владеете этим рабом.", reply_markup=kb, parse_mode="HTML")
                bot.answer_callback_query(call.id)
                return
    
            earned_cents, share_bp, acquired_ts = int(row[0] or 0), int(row[1] or 0), int(row[2] or 0)
            last24 = slave_profit_last24h(slave_id, clicker)
    
            cur.execute("SELECT short_name, username FROM users WHERE user_id=?", (slave_id,))
            r = cur.fetchone() or ("Без имени", "")
            sname = r[0] or "Без имени"
            sun = r[1] or ""
            uname_part = f" (@{html_escape(sun)})" if sun else ""
    
            _ensure_slave_meta_row(slave_id)
            cur.execute("SELECT COALESCE(buyout_cents,0) FROM slave_meta WHERE slave_id=?", (slave_id,))
            buyout_cents = int((cur.fetchone() or (0,))[0] or 0)
    
            ts_txt = "-"
            if acquired_ts and int(acquired_ts) > 0:
                ts_txt = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(acquired_ts)))
    
            text = (
                f"<b>{html_escape(sname)}</b>{uname_part} <i>{html_escape(ts_txt)}</i>\n"
                f"Цена раба: <b>{cents_to_money_str(buyout_cents)}</b>$\n"
                f"<b>{cents_to_money_str(earned_cents)}</b>$ + <u><b>{cents_to_money_str(last24)}</b></u>$"
            )
    
            cur.execute("""
                SELECT owner_id FROM slavery
                WHERE slave_id=? AND owner_id<>?
                ORDER BY owner_id
            """, (slave_id, clicker))
            other = [int(x[0]) for x in cur.fetchall()]
    
            if other:
                text += "\n\nВладельцы:\n"
                for oid in other[:2]:
                    cur.execute("SELECT short_name, username FROM users WHERE user_id=?", (oid,))
                    rr = cur.fetchone() or ("Без имени", "")
                    oname = rr[0] or "Без имени"
                    oun = rr[1] or ""
                    ou_part = f" (@{html_escape(oun)})" if oun else ""
                    text += f"{html_escape(oname)}{ou_part}\n"
                text += "Для полноправного владения рабом, выкупите его командой /buy"
    
            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("Назад к списку рабов", callback_data=cb_pack("profile:slaves", clicker)))
            kb.add(InlineKeyboardButton("Назад в профиль", callback_data=cb_pack("profile:open", clicker)))
            edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
            bot.answer_callback_query(call.id)
            return

    if kind == "profile" and parts[1] == "slave_status":
        uid = clicker
        if not is_slave(uid):
            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("Назад в профиль", callback_data=cb_pack("profile:open", clicker)))
            edit_inline_or_message(call, "У вас нет статуса раба.", reply_markup=kb, parse_mode="HTML")
            bot.answer_callback_query(call.id)
            return

        _ensure_slave_meta_row(uid)
        cur.execute("SELECT COALESCE(buyout_cents,0), COALESCE(strikes,0), COALESCE(life_uses,0) FROM slave_meta WHERE slave_id=?", (uid,))
        buyout_cents, strikes, life_uses = (cur.fetchone() or (0, 0, 0))
        buyout_cents = int(buyout_cents or 0)
        strikes = int(strikes or 0)
        life_uses = int(life_uses or 0)
        rem = get_life_remaining(uid)

        owners = get_slave_owners(uid)
        lines = []
        lines.append("Статус: <b>Раб</b>")
        if owners:
            lines.append("\nВладельцы:")
            for i, (oid, bp) in enumerate(owners, 1):
                ou = get_user(int(oid))
                oname = (ou[2] if ou and ou[2] else "Игрок")
                oun = (ou[1] if ou and ou[1] else "")
                tag = f" (@{html_escape(oun)})" if oun else ""
                pct = (int(bp or 0) / 100.0)
                lines.append(f"{i}) <b>{html_escape(oname)}</b>{tag} - <b>{pct:.1f}%</b>")
        else:
            lines.append("\nВладельцы: <b>-</b>")

        lines.append("")
        if buyout_cents > 0:
            lines.append(f"Сумма выкупа: <b>{cents_to_money_str(buyout_cents)}</b>$")
        else:
            lines.append("Сумма выкупа: <b>-</b>")

        lines.append(f"Проигрышей жизни: <b>{strikes}</b>/3")
        lines.append(f"Шансов поставить жизнь: <b><u>{rem}</u></b>")
        lines.append(f"Чтобы попробовать выкупить свою свободу - команда /buyout")

        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("Назад в профиль", callback_data=cb_pack("profile:open", clicker)))
        edit_inline_or_message(call, "\n".join(lines), reply_markup=kb, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        return

    # WORK
    if kind == "work" and parts[1] == "open":
        u = get_user(clicker)
        if not u or not u[2]:
            bot.answer_callback_query(call.id)
            return
        sh = get_current_shift(clicker)
        if sh and now_ts() < int(sh[3]):
            job_key = sh[1]
            jobs = load_jobs()
            job = jobs.get(job_key)
            job_title = job.title if job else job_key
            left = int(sh[3]) - now_ts()
            text = (
                f"Имя: <b>{html_escape(u[2])}</b>" + (f" (@{html_escape(u[1])})" if u[1] else "") +
                f"\n\nРаботает по вакансии <b>{html_escape(job_title)}</b>\n"
                f"Вернётся через: <b>{_format_duration(left)}</b>"
                )
            edit_inline_or_message(call, text, reply_markup=None, parse_mode="HTML")
            bot.answer_callback_query(call.id)
            return
        
        jobs = load_jobs()
        if not jobs:
            edit_inline_or_message(call, "Файл jobs.txt пустой или сломан.", None, "HTML")
            bot.answer_callback_query(call.id)
            return
        
        cur.execute("SELECT job_key, shifts FROM work_stats WHERE user_id=?", (clicker,))
        rows = cur.fetchall()
        if not rows:
            position = "Безработный"
            seniority_days = 0
        else:
            rows2 = [(r[0], int(r[1] or 0)) for r in rows]
            mx = max(s for _, s in rows2)
            best = [jk for jk, s in rows2 if s == mx and mx > 0]
            if len(best) != 1:
                position = "Разнорабочий"
            else:
                jk = best[0]
                job = jobs.get(jk)
                _, days, _ = get_work_stats(clicker, jk)
                position = _rank_for_days(job, days) if job else "Работник"
            seniority_days = sum(get_work_stats(clicker, r[0])[1] for r in rows2)
            
        text = (
            f"Имя: <b>{html_escape(u[2])}</b>" + (f" (@{html_escape(u[1])})" if u[1] else "") +
            f"\nСтаж: <b>{seniority_days} дней</b>\n"
            f"Должность: <b>{html_escape(position)}</b>\n\n"
            "Выбери сегодняшнюю вакансию:"
        )
        
        kb = InlineKeyboardMarkup()
        for jk, job in jobs.items():
            kb.add(InlineKeyboardButton(job.title, callback_data=cb_pack(f"work:pick:{jk}", clicker)))
            
        edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        return
    
    if kind == "work" and parts[1] == "pick":
        u = get_user(clicker)
        jobs = load_jobs()
        jk = parts[2] if len(parts) > 2 else ""
        job = jobs.get(jk)
        if not job:
            bot.answer_callback_query(call.id, "Вакансия не найдена.", show_alert=True)
            return
        
        sh = get_current_shift(clicker)
        if sh and now_ts() < int(sh[3]):
            bot.answer_callback_query(call.id, "Ты уже на смене.", show_alert=True)
            return
        
        shifts, days, earned = get_work_stats(clicker, jk)
        salary_full = _salary_with_seniority(job, days)

        text = (
            f"Название деятельности: <b>{html_escape(job.title)}</b>\n"
            f"Зарплата: <b>{cents_to_money_str(salary_full)}</b>$\n"
            f"Продолжительность рабочего дня: <b>{job.hours}</b> ч\n"
            "Подтверждая свой выбор, вы автоматически отказываетесь от финансовой поддержки куратора.\n"
            "Интересует?"
        )

        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("Выйти в смену", callback_data=cb_pack(f"work:go:{jk}", clicker)))
        kb.add(InlineKeyboardButton("Вернуться к выбору вакансий", callback_data=cb_pack("work:open", clicker)))
        
        edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        return
    
    if kind == "work" and parts[1] == "go":
        jk = parts[2] if len(parts) > 2 else ""
        jobs = load_jobs()
        job = jobs.get(jk)
        if not job:
            bot.answer_callback_query(call.id, "Вакансия не найдена.", show_alert=True)
            return
        
        sh = get_current_shift(clicker)
        if sh and now_ts() < int(sh[3]):
            bot.answer_callback_query(call.id, "Ты уже на смене.", show_alert=True)
            return
        
        ends_ts, salary_full = start_shift(clicker, jk)
        text = (
            f"Ты вышел в смену по вакансии <b>{html_escape(job.title)}</b>\n"
            f"Вернёшься через: <b>{_format_duration(ends_ts - now_ts())}</b>\n\n"
            "Мы уведомим вас, когда смена закончится."
            )
        edit_inline_or_message(call, text, reply_markup=None, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        return

    #  GAME START
    if kind == "game" and parts[1] == "start":
        if not group_key:
            bot.answer_callback_query(call.id, "Не могу определить чат/группу для игры.", show_alert=True)
            return

        game_key = parts[2] if len(parts) > 2 else "roulette"
        
        stake_kind = "money"
        life_demon_id = 0
        
        if len(parts) > 3 and parts[3] == "life":
            stake_kind = "life_demon"
            life_demon_id = clicker
            stake_raw = parts[4] if len(parts) > 4 else "0"
        else:
            stake_raw = parts[3] if len(parts) > 3 else "none"
        
        if game_key.isdigit():
            stake_raw = game_key
            game_key = "roulette"

        if game_key.isdigit():
            stake_raw = game_key
            game_key = "roulette"
        u = get_user(clicker)
        if not u or not u[2]:
            edit_inline_or_message(call, "Вы ещё не готовы.", None, "HTML")
            bot.answer_callback_query(call.id)
            return

        if stake_raw == "none":
            edit_inline_or_message(call, "Не думай, что всё так просто. Сделай ставку, введи сумму", None, "HTML")
            bot.answer_callback_query(call.id)
            return

        stake_cents = int(stake_raw)
        if stake_cents <= 0:
            edit_inline_or_message(call, "Мы не работаем в долг. Сделай ставку, введи сумму", None, "HTML")
            bot.answer_callback_query(call.id)
            return

        bal_cents = int(u[5] or 0)
        is_demon = (int(u[7] or 0) == 1)
        
        # обычные игроки не могут ставить больше баланса
        if (not is_demon) and stake_cents > bal_cents:
            edit_inline_or_message(call, "Не думай, что всё так просто. Сделай ставку, введи реальную сумму", None, "HTML")
            bot.answer_callback_query(call.id)
            return
        
        # режим life доступен только демону
        if stake_kind == "life_demon" and (not is_demon):
            edit_inline_or_message(call, "Эта ставка доступна только демонам.", None, "HTML")
            bot.answer_callback_query(call.id)
            return

        game_id = uuid.uuid4().hex[:16]
        reg_ends = now_ts() + 30
        origin_chat_id = None
        origin_message_id = None
        origin_inline_id = None
        if getattr(call, "message", None) and getattr(call.message, "chat", None):
            origin_chat_id = call.message.chat.id
            origin_message_id = call.message.message_id
        else:
            origin_inline_id = getattr(call, "inline_message_id", None)
        cur.execute("""
        INSERT INTO games (game_id, group_key, creator_id, state, stake_cents, created_ts, reg_ends_ts,
                    origin_chat_id, origin_message_id, origin_inline_id, game_type, cross_round,
                    stake_kind, life_demon_id, demon_settled)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (game_id, group_key, clicker, "lobby", stake_cents, now_ts(), reg_ends,
              origin_chat_id, origin_message_id, origin_inline_id, game_key, 1,
              stake_kind, int(life_demon_id), 0))
        cur.execute("INSERT INTO game_players (game_id, user_id, status) VALUES (?,?,?)", (game_id, clicker, "ready"))
        conn.commit()

        schedule_lobby_end(game_id)

        text, kb = render_lobby(game_id)
        edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        return

    # JOIN / EXTEND / CANCEL / CONTINUE 
    if kind == "game" and parts[1] in ("join", "extend", "cancel", "continue"):
        if len(parts) < 3:
            bot.answer_callback_query(call.id, "Bad request.", show_alert=True)
            return
        game_id = parts[2]
        if parts[1] == "join":
            handle_join(call, game_id)
            return
        if parts[1] == "extend":
            handle_extend(call, game_id)
            return
        if parts[1] == "cancel":
            handle_cancel(call, game_id)
            return
        if parts[1] == "continue":
            handle_continue(call, game_id)
            return

    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("buy:"))
def on_buy_callbacks(call: CallbackQuery):
    base, owner = cb_unpack(call.data)
    clicker = call.from_user.id

    if owner is not None and owner != 0 and clicker != owner:
        bot.answer_callback_query(call.id, "Вы не можете нажать на эту кнопку", show_alert=True)
        return

    parts = base.split(":")
    if len(parts) < 3:
        bot.answer_callback_query(call.id)
        return

    action = parts[1]
    offer_id = parts[2]

    buyer_bal = get_balance_cents(buyer_id)
    if buyer_bal < 0 or buyer_bal < price_cents:
        try:
            bot.answer_callback_query(call.id, "Сделка сорвалась: у покупателя недостаточно средств.", show_alert=True)
        except Exception:
            pass
        try:
            bot.send_message(buyer_id, "Сделка /buy сорвалась: у вас недостаточно средств на оплату.")
        except Exception:
            pass
        return

    cur.execute("SELECT slave_id, buyer_id, price_cents, active FROM buy_offers WHERE offer_id=?", (offer_id,))
    off = cur.fetchone()
    if not off:
        bot.answer_callback_query(call.id, "Оффер не найден.", show_alert=True)
        return

    slave_id, buyer_id, price_cents, active = int(off[0]), int(off[1]), int(off[2]), int(off[3] or 0)
    if active != 1:
        bot.answer_callback_query(call.id, "Оффер уже закрыт.", show_alert=True)
        return

    cur.execute("SELECT status FROM buy_offer_resp WHERE offer_id=? AND owner_id=?", (offer_id, clicker))
    r = cur.fetchone()
    if not r:
        bot.answer_callback_query(call.id, "Это предложение не для тебя.", show_alert=True)
        return
    if int(r[0] or 0) != 0:
        bot.answer_callback_query(call.id, "Ты уже ответил на предложение.", show_alert=True)
        return

    if action == "dec":
        cur.execute("UPDATE buy_offer_resp SET status=-1 WHERE offer_id=? AND owner_id=?", (offer_id, clicker))
        conn.commit()
        try:
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
        except Exception:
            pass
        bot.answer_callback_query(call.id, "Отказ отправлен.")
        try:
            bot.send_message(buyer_id, f"Владелец @{call.from_user.username or clicker} отказался продавать долю своего раба.")
        except Exception:
            pass

    elif action == "acc":
        cur.execute("SELECT share_bp FROM slavery WHERE slave_id=? AND owner_id=?", (slave_id, clicker))
        sr = cur.fetchone()
        if not sr:
            cur.execute("UPDATE buy_offer_resp SET status=-1 WHERE offer_id=? AND owner_id=?", (offer_id, clicker))
            conn.commit()
            bot.answer_callback_query(call.id, "У тебя уже нет доли за владение рабом.", show_alert=True)
            return
        seller_bp = int(sr[0] or 0)

        cur.execute("SELECT balance_cents FROM users WHERE user_id=?", (buyer_id,))
        br = cur.fetchone()
        buyer_bal = int(br[0] or 0) if br else 0
        if buyer_bal < price_cents or buyer_bal < 0:
            cur.execute("UPDATE buy_offer_resp SET status=-1 WHERE offer_id=? AND owner_id=?", (offer_id, clicker))
            conn.commit()
            bot.answer_callback_query(call.id, "У покупателя не хватает средств.", show_alert=True)
            return

        add_balance(buyer_id, -price_cents)
        add_balance(clicker, price_cents)

        cur.execute("DELETE FROM slavery WHERE slave_id=? AND owner_id=?", (slave_id, clicker))
        cur.execute("SELECT share_bp FROM slavery WHERE slave_id=? AND owner_id=?", (slave_id, buyer_id))
        br2 = cur.fetchone()
        if br2:
            new_bp = min(10000, int(br2[0] or 0) + seller_bp)
            cur.execute("UPDATE slavery SET share_bp=? WHERE slave_id=? AND owner_id=?", (new_bp, slave_id, buyer_id))
        else:
            cur.execute("INSERT OR IGNORE INTO slavery (slave_id, owner_id, share_bp, earned_cents) VALUES (?,?,?,0)", (slave_id, buyer_id, seller_bp))
        conn.commit()

        cur.execute("UPDATE buy_offer_resp SET status=1 WHERE offer_id=? AND owner_id=?", (offer_id, clicker))
        conn.commit()

        try:
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
        except Exception:
            pass

        bot.answer_callback_query(call.id, "Сделка состоялась.")
        try:
            bot.send_message(buyer_id, f"Владелец @{call.from_user.username or clicker} согласился и продал долю своего раба за {cents_to_money_str(price_cents)}$.")
        except Exception:
            pass

    cur.execute("SELECT COUNT(1) FROM buy_offer_resp WHERE offer_id=? AND status=0", (offer_id,))
    pending = int(cur.fetchone()[0] or 0)
    if pending == 0:
        cur.execute("UPDATE buy_offers SET active=0 WHERE offer_id=?", (offer_id,))
        conn.commit()
        cur.execute("SELECT COUNT(1) FROM buy_offer_resp WHERE offer_id=? AND status=1", (offer_id,))
        acc = int(cur.fetchone()[0] or 0)
        cur.execute("SELECT COUNT(1) FROM buy_offer_resp WHERE offer_id=? AND status=-1", (offer_id,))
        dec = int(cur.fetchone()[0] or 0)

        cur.execute("SELECT owner_id, share_bp FROM slavery WHERE slave_id=? ORDER BY share_bp DESC", (slave_id,))
        owners = cur.fetchall()
        owners_text = []
        for oid, bp in owners:
            cur.execute("SELECT short_name, username FROM users WHERE user_id=?", (int(oid),))
            ur = cur.fetchone() or (None, None)
            nm = ur[0] or "Без имени"
            un = ur[1] or ""
            owners_text.append(f"<b>{html_escape(nm)}</b>" + (f" (@{html_escape(un)})" if un else "") + f" - <b>{(int(bp or 0)/100):.1f}%</b>")

        try:
            bot.send_message(
                buyer_id,
                "Ваше предложение о выкупе рассмотрено.\n"
                "Краткая сводка:\n"
                f"Согласились: <b>{acc}</b>\n"
                f"Отказались: <b>{dec}</b>\n\n"
                "Текущие владельцы:\n" + ("\n".join(owners_text) if owners_text else "-"),
                parse_mode="HTML",
            )
        except Exception:
            pass

# Game lobby rendering & handlers
def render_lobby(game_id: str) -> Tuple[str, InlineKeyboardMarkup]:
    cur.execute("""
    SELECT creator_id, stake_cents, reg_ends_ts, reg_extended,
           COALESCE(game_type,'roulette'),
           COALESCE(stake_kind,'money'),
           COALESCE(life_demon_id,0)
    FROM games WHERE game_id=?
    """, (game_id,))
    row = cur.fetchone()
    creator_id, stake_cents, reg_ends_ts, reg_extended, game_type, stake_kind, life_demon_id = row
    cur.execute("SELECT user_id, status FROM game_players WHERE game_id=? ORDER BY rowid", (game_id,))
    players = cur.fetchall()

    lines = []
    for uid, status in players:
        cur.execute("SELECT short_name, username FROM users WHERE user_id=?", (uid,))
        u = cur.fetchone()
        if not u or not u[0]:
            name = "<b>Аноним</b>"
            tail = "в ожидании подтверждения"
        else:
            name = f"<b>{html_escape(u[0])}</b>"
            tail = "готов"
        uname = f" (@{html_escape(u[1])})" if (u and u[1]) else ""
        lines.append(f"• {name}{uname} - {tail}")

    left = max(0, int(reg_ends_ts) - now_ts())
    game_title = "⟢♣♦ Рулетка ♥♠⟣" if game_type != "cross" else "⟢♣♦ Марафон рулетка ♥♠⟣"
    stake_line = f"Текущая ставка: <b>{cents_to_money_str(int(stake_cents))}</b>$"
    if stake_kind == "life_demon":
        stake_line = (
            "Текущая ставка: <b>ҖนՅዙ৮</b>\n"
            f"Расчётная ставка: <b>{cents_to_money_str(int(stake_cents))}</b>$"
        )
    
    text = (
        f"Игра выбрана: <b>{game_title}</b>\n"
        f"{stake_line}\n\n"
        "Игроки, учавствующие в игре:\n"
        + "\n".join(lines if lines else ["• (пусто)"])
        + f"\n\nВремя регистрации: {left} секунд"
    )

    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Присоединиться к игре", callback_data=f"game:join:{game_id}"))
    if reg_extended == 0:
        kb.add(InlineKeyboardButton("Продлить на 30 сек", callback_data=cb_pack(f"game:extend:{game_id}", creator_id)))
    kb.add(InlineKeyboardButton("Отменить игру", callback_data=cb_pack(f"game:cancel:{game_id}", creator_id)))
    if len(players) >= 2:
        kb.add(InlineKeyboardButton("Продолжить", callback_data=cb_pack(f"game:continue:{game_id}", creator_id)))
    return text, kb

def schedule_lobby_end(game_id: str, delay: float = 0.5):
    def _fire():
        try:
            end_lobby_if_needed(game_id)
        except Exception:
            pass
    t = threading.Timer(delay, _fire)
    t.daemon = True
    t.start()

def end_lobby_if_needed(game_id: str):
    cur.execute("SELECT state, reg_ends_ts, creator_id, COALESCE(game_type,'roulette') FROM games WHERE game_id=?", (game_id,))
    row = cur.fetchone()
    if not row:
        return
    state, reg_ends_ts, creator_id, game_type = row
    if state != "lobby":
        return

    left = int(reg_ends_ts) - now_ts()
    if left > 0:
        schedule_lobby_end(game_id, delay=left + 0.5)
        return

    cur.execute("SELECT COUNT(*) FROM game_players WHERE game_id=? AND user_id<>?", (game_id, creator_id))
    others = int(cur.fetchone()[0])

    if others == 0:
        cur.execute("UPDATE games SET state='cancelled' WHERE game_id=?", (game_id,))
        conn.commit()
        edit_game_message(game_id, "Регистрация завершена. Никто не присоединился.\nИгра отменена", reply_markup=None)
        return

    if game_type == "cross":
        cur.execute("SELECT stake_cents FROM games WHERE game_id=?", (game_id,))
        stake_cents = int((cur.fetchone() or (0,))[0] or 0)
        r = 1
        rfmt = cross_format_for_round(r)
        cur.execute("UPDATE games SET state='playing', roulette_format=?, cross_round=?, turn_index=0 WHERE game_id=?",
                    (rfmt, r, game_id))
        conn.commit()

        u = get_user(int(creator_id))
        cname = u[2] if u and u[2] else "Игрок"
        stake_now, add = cross_stake_for_round(stake_cents, r)
        title = "1×3" if rfmt == "1x3" else ("3×3" if rfmt == "3x3" else "3×5")
        text = (
            "Выбор сохранён.\n"
            f"Раунд: <b>{r}</b>\n"
            f"Режим {title}\n"
            f"Ставка <b>{cents_to_money_str(stake_now)}</b>$"
            "Приятной игры."
        )
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton(f"Ход {cname}", callback_data=cb_pack(f"turn:begin:{game_id}", int(creator_id))))
        edit_game_message(game_id, text, reply_markup=kb, parse_mode="HTML")
        return

    # Обычная рулетка
    cur.execute("UPDATE games SET state='choose_format' WHERE game_id=?", (game_id,))
    conn.commit()
    text = (
        "Выберите формат рулетки:\n"
        "Режим ¨Кросс¨ 3 слота (Формат 1×3)\n"
        "Режим ¨Классика¨ 9 слотов (Формат 3×3)\n"
        "Режим 𖤐ĐĒʋÍ£𖤐 15 слотов (Формат 3×5)"
        )
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Кросс 1×3", callback_data=cb_pack(f"rfmt:set:{game_id}:1x3", creator_id)))
    kb.add(InlineKeyboardButton("Классика 3×3", callback_data=cb_pack(f"rfmt:set:{game_id}:3x3", creator_id)))
    kb.add(InlineKeyboardButton("ĐĒʋÍ£ 3×5", callback_data=cb_pack(f"rfmt:set:{game_id}:3x5", creator_id)))
    edit_game_message(game_id, text, reply_markup=kb, parse_mode="HTML")

def build_totals_block(game_id: str, creator_id: int) -> str:
    cur.execute("""
        SELECT gp.user_id,
               COALESCE(gr.delta_cents, 0) AS delta
        FROM game_players gp
        LEFT JOIN game_results gr
          ON gr.game_id = gp.game_id AND gr.user_id = gp.user_id
        WHERE gp.game_id=?
    """, (game_id,))
    rows = cur.fetchall()
    rows.sort(key=lambda r: int(r[1] or 0), reverse=True)

    lines = ["⟢♣♦ Итоги игры ♥♠⟣"]
    for i, (uid, delta) in enumerate(rows, start=1):
        u = get_user(uid)
        name = u[2] if u and u[2] else "Игрок"
        name_html = f"<b>{html_escape(name)}</b>"
        if uid == creator_id:
            name_html = f"<b><u>{html_escape(name)}</u></b>"
        lines.append(f"{i}. {name_html} - <b>{cents_to_money_str(int(delta))}</b>$")

    lines.append("")
    lines.append("Хотите отыграться?")
    return "\n".join(lines)

def render_game_totals(game_id: str, creator_id: int) -> Tuple[str, InlineKeyboardMarkup]:
    text = build_totals_block(game_id, creator_id)

    cur.execute("SELECT COUNT(*) FROM rematch_votes WHERE game_id=? AND vote='yes'", (game_id,))
    yes_n = int(cur.fetchone()[0])
    cur.execute("SELECT COUNT(*) FROM rematch_votes WHERE game_id=? AND vote='no'", (game_id,))
    no_n = int(cur.fetchone()[0])

    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton(f"Да {yes_n}", callback_data=f"rematch:vote:{game_id}:yes"))
    kb.add(InlineKeyboardButton(f"Нет {no_n}", callback_data=f"rematch:vote:{game_id}:no"))
    return text, kb

def start_rematch_from_votes(call: CallbackQuery, old_game_id: str, yes_set: set):
    cur.execute("SELECT group_key, creator_id, stake_cents, roulette_format, COALESCE(game_type,'roulette') FROM games WHERE game_id=?", (old_game_id,))
    old = cur.fetchone()
    if not old:
        return
    group_key, old_creator, stake_cents, rfmt, game_type = old
    old_creator = int(old_creator)
    if game_type == "cross":
        rfmt = cross_format_for_round(1)

    cur.execute("SELECT user_id FROM game_players WHERE game_id=? ORDER BY rowid", (old_game_id,))
    old_order = [int(r[0]) for r in cur.fetchall()]
    yes_order = [u for u in old_order if u in yes_set]

    if len(yes_order) < 2:
        end_text = "Игра завершена. Недостаточно игроков для продолжения (нужно минимум 2 «Да»)."
        edit_inline_or_message(call, end_text, reply_markup=None, parse_mode="HTML")
        return

    new_creator = old_creator if old_creator in yes_set else yes_order[0]

    new_order = [new_creator] + [u for u in yes_order if u != new_creator]

    new_game_id = uuid.uuid4().hex[:16]

    origin_chat_id = None
    origin_message_id = None
    origin_inline_id = None
    if getattr(call, "message", None) and getattr(call.message, "chat", None):
        origin_chat_id = call.message.chat.id
        origin_message_id = call.message.message_id
    else:
        origin_inline_id = getattr(call, "inline_message_id", None)
        
    pending_life = []
    excluded_no_stake = []
    filtered_order = []
    for uid in new_order:
        u = get_user(uid)
        demon_flag = int(u[7] or 0) if u else 0
        bal = int(u[5] or 0) if u else 0
        if demon_flag == 0 and bal <= 0:
            rem = get_life_remaining(uid)
            if rem > 0:
                pending_life.append(uid)
                filtered_order.append(uid)
            else:
                excluded_no_stake.append(uid)
        else:
            filtered_order.append(uid)

    new_order = filtered_order
    if len(new_order) < 2:
        names = []
        for puid in excluded_no_stake:
            uu = get_user(puid)
            names.append(f"<b>{html_escape(uu[2] if uu and uu[2] else 'Игрок')}</b>")
        extra = ""
        if names:
            extra = "\n\nПокидают эту игру:\n" + "\n".join(names)
        end_text = "Игра завершена. Недостаточно игроков для продолжения." + extra
        edit_inline_or_message(call, end_text, reply_markup=None, parse_mode="HTML")
        return

    new_state = "life_wait" if pending_life else "playing"

    cur.execute("""
        INSERT INTO games (game_id, group_key, creator_id, state, stake_cents, created_ts, reg_ends_ts, roulette_format,
                           origin_chat_id, origin_message_id, origin_inline_id, turn_index, game_type, cross_round)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (new_game_id, group_key, new_creator, new_state, int(stake_cents), now_ts(), now_ts(), rfmt,
          origin_chat_id, origin_message_id, origin_inline_id, 0, game_type, 1))

    for uid in new_order:
        u = get_user(uid)
        if u and u[2]:
            st = "need_life" if uid in pending_life else "ready"
            cur.execute("INSERT OR IGNORE INTO game_players (game_id, user_id, status) VALUES (?,?,?)", (new_game_id, uid, st))
            if st == "need_life":
               cur.execute("INSERT OR IGNORE INTO life_wait (game_id, user_id, stake_cents) VALUES (?,?,?)", (new_game_id, uid, int(stake_cents))) 

    conn.commit()
    
    if pending_life:
        names = []
        for puid in pending_life:
            uu = get_user(puid)
            names.append(f"<b>{html_escape(uu[2] if uu and uu[2] else 'Игрок')}</b>")
            try:
                rem = get_life_remaining(puid)
                bot.send_message(
                    puid,
                    "Сожалеем, но у вас недостаточно средств для продолжения игры. Однако, найдено одно решение. У вас ещё есть один актив, подлежащий монетизации.\n"
                    "👹҈ В҈а҈ш҈е҈й҈ с҈т҈а҈в҈к҈о҈й҈ с҈т҈а҈н҈е҈т҈ в҈а҈ш҈а҈ ж҈и҈з҈н҈ь҈\n"
                    f"У вас ещё <u><b>{rem}</b></u> шанса на это. Воспользуйтесь этой возможностью рационально.",
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup().add(
                        InlineKeyboardButton("Пожать руку куратору", callback_data=cb_pack(f"life:accept:{new_game_id}", puid))
                    )
                )
            except Exception:
                pass
            
        ex_lines = []
        for puid in excluded_no_stake:
            uu = get_user(puid)
            ex_lines.append(f"<b>{html_escape(uu[2] if uu and uu[2] else 'Игрок')}</b>")
        excluded_part = ("\n\nИсключены из голосования:\n" + "\n".join(ex_lines)) if ex_lines else ""

        wait_text = (
            "Следующим участникам из списка поступило специальное предложение. Ожидайте.\n" +
            "\n".join(names) +
            excluded_part
            )
        edit_inline_or_message(call, wait_text, reply_markup=None, parse_mode="HTML")
        return

    first_uid = new_order[0]
    first_u = get_user(first_uid)
    first_name = first_u[2] if first_u and first_u[2] else "Игрок"

    text = (
        "Выбор сохранён.\n"
        f"Ставка <b>{cents_to_money_str(int(stake_cents))}</b>\n"
        "Приятной игры."
    )
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton(f"Ход {first_name}", callback_data=cb_pack(f"turn:begin:{new_game_id}", first_uid)))

    edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")

def edit_game_message(game_id: str, text: str, reply_markup=None, parse_mode="HTML"):
    row = db_one("SELECT origin_chat_id, origin_message_id, origin_inline_id FROM games WHERE game_id=?", (game_id,))
    if not row:
        return
    chat_id, msg_id, inline_id = row
    if inline_id:
        limited_edit_message_text(text=text, inline_id=inline_id, reply_markup=reply_markup, parse_mode=parse_mode)
    elif chat_id and msg_id:
        limited_edit_message_text(text=text, chat_id=chat_id, msg_id=msg_id, reply_markup=reply_markup, parse_mode=parse_mode)

def handle_join(call: CallbackQuery, game_id: str):
    uid = call.from_user.id
    cur.execute("SELECT state, creator_id FROM games WHERE game_id=?", (game_id,))
    row = cur.fetchone()
    if not row:
        bot.answer_callback_query(call.id, "Игра не найдена.", show_alert=True)
        return
    state, creator_id = row
    if state != "lobby":
        bot.answer_callback_query(call.id, "Регистрация на игру уже закрыта.", show_alert=True)
        return
    if uid == creator_id:
        bot.answer_callback_query(call.id, "Создатель приглашения уже в игре.", show_alert=True)
        return
    
    # life-mode rules
    r = db_one("SELECT COALESCE(stake_kind,'money'), COALESCE(life_demon_id,0) FROM games WHERE game_id=?", (game_id,))
    stake_kind = (r[0] if r else "money") or "money"
    life_demon_id = int((r[1] if r else 0) or 0)
    
    if stake_kind == "life_demon":
        # максимум 2 игрока: демон + 1 оппонент
        cnt = db_one("SELECT COUNT(*) FROM game_players WHERE game_id=?", (game_id,))
        if cnt and int(cnt[0]) >= 2:
            bot.answer_callback_query(call.id, "В этой игре на жизнь может быть только один оппонент.", show_alert=True)
            return
    
        # demon vs demon: запрещено если ни у кого нет рабов
        u_creator = get_user(int(life_demon_id))
        u_joiner = get_user(uid)
        if u_creator and u_joiner and int(u_creator[7] or 0) == 1 and int(u_joiner[7] or 0) == 1:
            a = db_one("SELECT COUNT(*) FROM slavery WHERE owner_id=?", (int(life_demon_id),))
            b = db_one("SELECT COUNT(*) FROM slavery WHERE owner_id=?", (int(uid),))
            if int((a[0] if a else 0) or 0) == 0 and int((b[0] if b else 0) or 0) == 0:
                bot.answer_callback_query(call.id, "Демоны не могут играть друг с другом без ставки на душу.", show_alert=True)
                return

    cur.execute("SELECT 1 FROM game_players WHERE game_id=? AND user_id=?", (game_id, uid))
    if cur.fetchone():
        bot.answer_callback_query(call.id, "Ты уже в списке участников.", show_alert=True)
        return

    u = get_user(uid)
    if not u or not u[2]:
        try:
            bot.send_message(uid, "Куратор позволяет вам принять приглашение в игру.\n Однако, вам необходимо принять приглашение. Пропишите @casino_rpg_bot, Вас ожидает награда.")
        except Exception:
            pass
        cur.execute("INSERT OR IGNORE INTO game_players (game_id, user_id, status) VALUES (?,?,?)", (game_id, uid, "anon_pending"))
        conn.commit()
        text, kb = render_lobby(game_id)
        edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        return

    cur.execute("INSERT INTO game_players (game_id, user_id, status) VALUES (?,?,?)", (game_id, uid, "ready"))
    conn.commit()
    text, kb = render_lobby(game_id)
    edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
    bot.answer_callback_query(call.id)

def handle_extend(call: CallbackQuery, game_id: str):
    cur.execute("SELECT state, reg_extended, reg_ends_ts FROM games WHERE game_id=?", (game_id,))
    row = cur.fetchone()
    if not row:
        bot.answer_callback_query(call.id, "Игра не найдена.", show_alert=True)
        return
    state, reg_extended, reg_ends_ts = row
    if state != "lobby":
        bot.answer_callback_query(call.id, "Поздно продлевать. Время вышло.", show_alert=True)
        return
    if int(reg_extended) == 1:
        bot.answer_callback_query(call.id, "Ожидание игроков уже продлено.", show_alert=True)
        return

    cur.execute("UPDATE games SET reg_extended=1, reg_ends_ts=? WHERE game_id=?", (int(reg_ends_ts) + 30, game_id))
    conn.commit()
    text, kb = render_lobby(game_id)
    edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
    bot.answer_callback_query(call.id)

def handle_cancel(call: CallbackQuery, game_id: str):
    cur.execute("SELECT state, creator_id, stake_cents FROM games WHERE game_id=?", (game_id,))
    row = cur.fetchone()
    if not row:
        bot.answer_callback_query(call.id, "Игра не найдена.", show_alert=True)
        return
    state, creator_id, stake_cents = row
    if state not in ("lobby", "choose_format", "playing"):
        bot.answer_callback_query(call.id, "Нельзя отменить.", show_alert=True)
        return

    comp = int(int(stake_cents) * 0.10)
    if get_user(creator_id) and int(get_user(creator_id)[7] or 0) == 0:
        add_balance(creator_id, -comp)

    cur.execute("SELECT user_id FROM game_players WHERE game_id=? AND user_id<>?", (game_id, creator_id))
    others = [r[0] for r in cur.fetchall()]
    for uid in others:
        u = get_user(uid)
        if u and u[2]:
            add_balance(uid, comp)

    cur.execute("UPDATE games SET state='cancelled' WHERE game_id=?", (game_id,))
    conn.commit()

    creator_name = get_user(creator_id)[2] if get_user(creator_id) else "Инициатор"
    text = (
        f"Игра была отменена инициатором. Приносим свои извинения за доставленные неудобства."
        f"Конпенсация участникам игры произведина со счёта <b>{html_escape(creator_name)}</b>: <b>{cents_to_money_str(comp)}</b>$"
    )
    edit_inline_or_message(call, text, reply_markup=None, parse_mode="HTML")
    bot.answer_callback_query(call.id)

def handle_continue(call: CallbackQuery, game_id: str):
    cur.execute("SELECT state, creator_id, COALESCE(game_type,'roulette') FROM games WHERE game_id=?", (game_id,))
    row = cur.fetchone()
    if not row:
        bot.answer_callback_query(call.id, "Игра не найдена.", show_alert=True)
        return
    state, creator_id, game_type = row
    if state != "lobby":
        bot.answer_callback_query(call.id, "Уже поздно.", show_alert=True)
        return
    cur.execute("SELECT COUNT(*) FROM game_players WHERE game_id=?", (game_id,))
    cnt = int(cur.fetchone()[0])
    if cnt < 2:
        bot.answer_callback_query(call.id, "Для игры нужен хотя бы один участник.", show_alert=True)
        return
    
    r = db_one("SELECT COALESCE(stake_kind,'money'), COALESCE(life_demon_id,0) FROM games WHERE game_id=?", (game_id,))
    stake_kind = (r[0] if r else "money") or "money"
    life_demon_id = int((r[1] if r else 0) or 0)
    
    if stake_kind == "life_demon":
        cnt = db_one("SELECT COUNT(*) FROM game_players WHERE game_id=?", (game_id,))
        if not cnt or int(cnt[0]) != 2:
            bot.answer_callback_query(call.id, "Игра на жизнь возможна только 1×1 (демон и один оппонент).", show_alert=True)
            return

    if game_type == "cross":
        cur.execute("SELECT stake_cents FROM games WHERE game_id=?", (game_id,))
        stake_cents = int((cur.fetchone() or (0,))[0] or 0)
        r = 1
        rfmt = cross_format_for_round(r)
        cur.execute("UPDATE games SET state='playing', roulette_format=?, cross_round=?, turn_index=0 WHERE game_id=?",
                    (rfmt, r, game_id))
        conn.commit()

        u = get_user(int(creator_id))
        cname = u[2] if u and u[2] else "Игрок"
        stake_now, add = cross_stake_for_round(stake_cents, r)
        title = "1×3" if rfmt == "1x3" else ("3×3" if rfmt == "3x3" else "3×5")
        text = (
            "Выбор сохранён.\n"
            f"Раунд: <b>{r}</b>\n"
            f"Режим {title}\n"
            f"Ставка <b>{cents_to_money_str(stake_now)}</b>$"
            "Приятной игры."
        )
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton(f"Ход {cname}", callback_data=cb_pack(f"turn:begin:{game_id}", int(creator_id))))
        edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        return

    cur.execute("UPDATE games SET state='choose_format' WHERE game_id=?", (game_id,))
    conn.commit()

    text = (
        "Выберите формат рулетки:\n"
        "Режим ¨Кросс¨ 3 слота (Формат 1×3)\n"
        "Режим ¨Классика¨ 9 слотов (Формат 3×3)\n"
        "Режим 𖤐ĐĒʋÍ£𖤐 15 слотов (Формат 3×5)"
    )
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("1×3", callback_data=cb_pack(f"rfmt:set:{game_id}:1x3", creator_id)))
    kb.add(InlineKeyboardButton("3×3", callback_data=cb_pack(f"rfmt:set:{game_id}:3x3", creator_id)))
    kb.add(InlineKeyboardButton("3×5", callback_data=cb_pack(f"rfmt:set:{game_id}:3x5", creator_id)))
    edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("rematch:vote:"))
def on_rematch_vote(call: CallbackQuery):
    parts = call.data.split(":")
    if len(parts) != 4:
        bot.answer_callback_query(call.id)
        return
    _, _, game_id, vote = parts
    uid = call.from_user.id

    if vote not in ("yes", "no"):
        bot.answer_callback_query(call.id)
        return

    cur.execute("SELECT 1 FROM game_players WHERE game_id=? AND user_id=? LIMIT 1", (game_id, uid))
    if not cur.fetchone():
        bot.answer_callback_query(call.id, "Голосовать могут только участники игры.", show_alert=True)
        return

    cur.execute("""
        INSERT INTO rematch_votes (game_id, user_id, vote)
        VALUES (?,?,?)
        ON CONFLICT(game_id, user_id) DO UPDATE SET vote=excluded.vote
    """, (game_id, uid, vote))
    conn.commit()
    cur.execute("SELECT creator_id FROM games WHERE game_id=?", (game_id,))
    row = cur.fetchone()
    if not row:
        bot.answer_callback_query(call.id)
        return
    creator_id = int(row[0])
    text, kb = render_game_totals(game_id, creator_id)
    cur.execute("SELECT COUNT(*) FROM game_players WHERE game_id=?", (game_id,))
    players_n = int(cur.fetchone()[0])

    cur.execute("SELECT COUNT(*) FROM rematch_votes WHERE game_id=?", (game_id,))
    votes_n = int(cur.fetchone()[0])

    if votes_n < players_n:
        edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        return

    cur.execute("SELECT user_id FROM rematch_votes WHERE game_id=? AND vote='yes'", (game_id,))
    yes_uids = {int(r[0]) for r in cur.fetchall()}

    cur.execute("UPDATE games SET state='finished' WHERE game_id=?", (game_id,))
    conn.commit()

    if len(yes_uids) < 2:
        end_text = text + "\n\nИгра завершена. Недостаточно игроков для продолжения игры (нужно минимум 2 «Да»)."
        edit_inline_or_message(call, end_text, reply_markup=None, parse_mode="HTML")
    else:
        start_rematch_from_votes(call, game_id, yes_uids)

    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("rfmt:set:"))
def on_rfmt(call: CallbackQuery):
    base, owner = cb_unpack(call.data)
    clicker = call.from_user.id
    if owner is not None and clicker != owner:
        bot.answer_callback_query(call.id, "Вы не можете нажать на эту кнопку", show_alert=True)
        return

    _, _, game_id, fmt = base.split(":")
    cur.execute("SELECT state, creator_id, stake_cents, COALESCE(game_type,'roulette') FROM games WHERE game_id=?", (game_id,))
    row = cur.fetchone()
    if not row:
        bot.answer_callback_query(call.id, "Игра не найдена.", show_alert=True)
        return
    state, creator_id, stake_cents, game_type = row
    if game_type == "cross":
        bot.answer_callback_query(call.id, "В марафоне рулетки формат не выбирается.", show_alert=True)
        return
    if state != "choose_format":
        bot.answer_callback_query(call.id, "Формат уже выбран.", show_alert=True)
        return

    cur.execute("UPDATE games SET roulette_format=?, state='playing', turn_index=0 WHERE game_id=?", (fmt, game_id))
    conn.commit()

    creator_name = get_user(creator_id)[2] if get_user(creator_id) else "Игрок"
    text = f"Выбор сохранён.\nСтавка <b>{cents_to_money_str(int(stake_cents))}</b>\nПриятной игры."
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton(f"Ход {creator_name}", callback_data=cb_pack(f"turn:begin:{game_id}", creator_id)))
    edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("turn:begin:"))
def on_turn_begin(call: CallbackQuery):
    base, owner = cb_unpack(call.data)
    clicker = call.from_user.id

    if owner is not None and clicker != owner:
        bot.answer_callback_query(call.id, "Сейчас не твой ход.", show_alert=True)
        return

    parts = base.split(":")
    if len(parts) != 3:
        bot.answer_callback_query(call.id, "Bad request.", show_alert=True)
        return
    game_id = parts[2]
    uid = owner

    cur.execute("SELECT state, roulette_format, stake_cents, turn_index, COALESCE(game_type,'roulette'), COALESCE(cross_round,1) FROM games WHERE game_id=?", (game_id,))
    row = cur.fetchone()
    if not row:
        bot.answer_callback_query(call.id, "Игра не найдена.", show_alert=True)
        return
    state, rfmt, stake_cents, turn_index, game_type, cross_round = row
    if state != "playing":
        bot.answer_callback_query(call.id, "Вы не можете сейчас ходить.", show_alert=True)
        return

    cur.execute("SELECT user_id FROM game_players WHERE game_id=? ORDER BY rowid", (game_id,))
    order = [r[0] for r in cur.fetchall()]
    if not order:
        bot.answer_callback_query(call.id, "Нет игроков.", show_alert=True)
        return

    current_uid = order[int(turn_index) % len(order)]
    if uid != current_uid:
        bot.answer_callback_query(call.id, "Сейчас ход другого игрока.", show_alert=True)
        return

    if rfmt not in ("1x3", "3x3", "3x5"):
        bot.answer_callback_query(call.id, "Неизвестный формат.", show_alert=True)
        return
    
    empty_grid = empty_grid_text(rfmt)
    title = "1×3" if rfmt == "1x3" else ("3×3" if rfmt == "3x3" else "3×5")
    player = get_user(uid)
    pname = player[2] if player and player[2] else "Игрок"

    stake_now = int(stake_cents)
    add_cents = 0
    round_line = ""
    header = "⟢♣♦ Рулетка ♥♠⟣"
    if game_type == "cross":
        header = "⟢♣♦ Марафон рулетка ♥♠⟣"
        stake_now, add_cents = cross_stake_for_round(int(stake_cents), int(cross_round))
        round_line = f"Раунд: <b>{int(cross_round)}</b>\n"

    stake_line = f"Ставка: <b>{cents_to_money_str(int(stake_now))}</b>$"
    text = (
        (f"<b>{header}</b>\n" + round_line + f"<b>Режим {title}</b>\n\n")
        + f"{empty_grid}\n\n"
        + f"Ход: <u>{html_escape(pname)}</u>\n"
        + stake_line
    )

    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Нажать рычаг", callback_data=cb_pack(f"spin:pull:{game_id}", uid)))

    inline_id = getattr(call, "inline_message_id", None)
    if inline_id:
        cur.execute("""
        INSERT OR REPLACE INTO spins (game_id, user_id, stage, msg_chat_id, msg_id, inline_id, grid_text, started_ts)
        VALUES (?,?,?,?,?,?,?,?)
        """, (game_id, uid, "ready", None, None, inline_id, empty_grid, now_ts()))
    else:
        cur.execute("""
        INSERT OR REPLACE INTO spins (game_id, user_id, stage, msg_chat_id, msg_id, inline_id, grid_text, started_ts)
        VALUES (?,?,?,?,?,?,?,?)
        """, (game_id, uid, "ready", call.message.chat.id, call.message.message_id, None, empty_grid, now_ts()))
    conn.commit()

    edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("spin:pull:"))
def on_spin_pull(call: CallbackQuery):
    base, owner = cb_unpack(call.data)
    clicker = call.from_user.id

    if owner is not None and clicker != owner:
        bot.answer_callback_query(call.id, "Сейчас не твой ход.", show_alert=True)
        return

    _, _, game_id = base.split(":", 2)
    uid = owner

    srow = db_one("SELECT stage, msg_chat_id, msg_id, inline_id FROM spins WHERE game_id=? AND user_id=?", (game_id, uid))
    if not srow:
        bot.answer_callback_query(call.id, "Этот ход не активен.", show_alert=True)
        return
    stage, msg_chat_id, msg_id, inline_id = srow
    if stage != "ready":
        bot.answer_callback_query(call.id, "Рулетка уже крутится. Бот прогружает её. Подождите.", show_alert=True)
        return
    
    def _edit(text: str, kb=None):
        if inline_id:
            limited_edit_message_text(text=text, inline_id=inline_id, reply_markup=kb, parse_mode="HTML")
        else:
            limited_edit_message_text(text=text, chat_id=msg_chat_id, msg_id=msg_id, reply_markup=kb, parse_mode="HTML")


    def run_spin():
        grow = db_one("SELECT roulette_format, stake_cents, turn_index, COALESCE(game_type,'roulette'), COALESCE(cross_round,1) FROM games WHERE game_id=?", (game_id,))
        if not grow:
            bot.answer_callback_query(call.id, "Игра не найдена.", show_alert=True)
            return
        rfmt, stake_cents, turn_index, game_type, cross_round = grow
    
        stake_now = int(stake_cents)
        add_cents = 0
        if game_type == "cross":
            stake_now, add_cents = cross_stake_for_round(int(stake_cents), int(cross_round))
        
        rr = db_one("SELECT creator_id FROM games WHERE game_id=?", (game_id,))
        creator_id = int((rr[0] if rr else 0) or 0)
    
        strow = db_one("SELECT status FROM game_players WHERE game_id=? AND user_id=?", (game_id, uid))
        pstatus = (strow[0] if strow else "") or ""
    
        title = "1×3" if rfmt == "1x3" else ("3×3" if rfmt == "3x3" else "3×5")
        def make_rand_state():
            ww = roulette_weights_for(uid, rfmt, game_id)
            if rfmt == "1x3":
                st = [weighted_pick(ww) for _ in range(3)]
                return apply_fake_clover_to_state(uid, rfmt, st, game_id)
            if rfmt == "3x3":
                st = [[weighted_pick(ww) for _ in range(3)] for __ in range(3)]
                return apply_fake_clover_to_state(uid, rfmt, st, game_id)
            st = [[weighted_pick(ww) for _ in range(5)] for __ in range(3)]
            return apply_fake_clover_to_state(uid, rfmt, st, game_id)
        def render_state(state):
            if rfmt == "1x3":
                return render_1x3(state)
            if rfmt == "3x3":
                return render_3x3(state)
            return render_3x5(state)
        def calc_delta_state(state):
            if rfmt == "1x3":
                return calc_delta_1x3(state, int(stake_now))
            if rfmt == "3x3":
                return calc_delta_3x3(state, int(stake_now))
            return calc_delta_3x5(state, int(stake_now))
        player = get_user(uid)
        pname = player[2] if player and player[2] else "Игрок"
        steps = 6 if rfmt != "1x3" else 5
        sleep_s = 0.9 if rfmt == "3x5" else 0.7 
    
        for _ in range(steps):
            st = make_rand_state()
            grid_txt = render_state(st)
            
            header = "⟢♣♦ Рулетка ♥♠⟣" if game_type != "cross" else "⟢♣♦ Марафон рулетка ♥♠⟣"
            round_line = f"Раунд: <b>{int(cross_round)}</b>\n" if game_type == "cross" else ""
            stake_line = f"Ставка: <b>{cents_to_money_str(int(stake_now))}</b>$"
            text = (
                (f"<b>{header}</b>\n" + round_line + f"<b>Режим {title}</b>\n\n")
                + f"{grid_txt}\n\n"
                + f"Ход: <u>{html_escape(pname)}</u>\n"
                + stake_line
            )
            _edit(text, kb=None)
            time.sleep(sleep_s)
        
        final_state = make_rand_state()
        final_grid = render_state(final_state)
        delta = int(calc_delta_state(final_state))
        raw_delta = delta
        
        debt_mult = debt_mult_from_skulls(final_state, rfmt)
        if debt_mult > 0:
            strow2 = db_one("SELECT status FROM game_players WHERE game_id=? AND user_id=?", (game_id, uid))
            pstatus2 = (strow2[0] if strow2 else "") or ""
            player2 = get_user(uid)
            is_demon2 = (player2 and int(player2[7] or 0) == 1)
            
            if (not is_demon2) and (pstatus2 != "life"):
                bal_now = get_balance_cents(uid)
                debt_cents = int(debt_mult) * int(stake_now)
                predicted = bal_now + int(delta)
                target = -debt_cents
                final_balance = min(predicted, bal_now, target)
                delta = int(final_balance - bal_now)
                if final_balance < 0:
                    set_slave_buyout(uid, abs(int(final_balance)) * 100)

        active = shop_get_active_for_game(uid, game_id)
        active_for_display = dict(active)
        boosts_line = render_active_boosts(active_for_display)

        pepper_on = active.get("devil_pepper", 0) > 0
        if pepper_on and pepper_triggers_demon(final_state, rfmt):
            rr_pep = db_one("SELECT user_id FROM users WHERE demon=1 LIMIT 1")
            if rr_pep:
                demon_id = int(rr_pep[0])
                slavery_add_owner(uid, demon_id, 2000)

        insured = active.get("insurance", 0) > 0
        if insured and delta < 0:
            set_slave_buyout(uid, abs(raw_delta) * 100)
            delta = 0
            rr = db_one("SELECT user_id FROM users WHERE demon=1 LIMIT 1")
            if rr:
                demon_id = int(rr[0])
                slavery_add_owner(uid, demon_id, 2000)
                try:
                    cur.execute("UPDATE slavery SET share_bp=? WHERE slave_id=? AND owner_id=?", (2000, int(uid), int(demon_id)))
                    conn.commit()
                except Exception:
                    pass
    
        if pepper_on:
            delta = int(delta) * 2

        u = get_user(uid)
        is_demon = (u and int(u[7] or 0) == 1)
        if not is_demon:
            if delta > 0:
                kept = apply_slave_cut(uid, delta, reason="roulette")
                add_balance(uid, kept)
            else:
                add_balance(uid, delta)
    
        if game_type == "cross":
            db_exec("""
            INSERT INTO game_results (game_id, user_id, delta_cents, finished)
            VALUES (?,?,?,1)
            ON CONFLICT(game_id, user_id) DO UPDATE SET
                delta_cents = COALESCE(game_results.delta_cents, 0) + excluded.delta_cents,
                finished = 1
            """, (game_id, uid, int(delta)), commit=True)
        else:
            db_exec("""
            INSERT INTO game_results (game_id, user_id, delta_cents, finished)
            VALUES (?,?,?,1)
            ON CONFLICT(game_id, user_id) DO UPDATE SET delta_cents=excluded.delta_cents, finished=1
            """, (game_id, uid, int(delta)), commit=True)
    
        if (game_type != "cross") or (int(cross_round) >= 9):
            shop_tick_after_game(uid, game_id)
    
        if game_type != "cross":
            db_exec("INSERT OR IGNORE INTO game_stats (user_id) VALUES (?)", (uid,), commit=True)
            if delta >= 0:
                db_exec(
                    "UPDATE game_stats SET games_total=games_total+1, wins=wins+1, max_win_cents=MAX(max_win_cents, ?) WHERE user_id=?",
                    (int(delta), uid), commit=True
                )
            else:
                db_exec(
                    "UPDATE game_stats SET games_total=games_total+1, losses=losses+1, max_lose_cents=MAX(max_lose_cents, ?) WHERE user_id=?",
                    (int(abs(delta)), uid), commit=True
                )
            bump_game_type_stat(uid, game_type)
        elif int(cross_round) >= 9:
            rr_tot = db_one("SELECT delta_cents FROM game_results WHERE game_id=? AND user_id=?", (game_id, uid))
            tot = int((rr_tot[0] if rr_tot else 0) or 0)
            db_exec("INSERT OR IGNORE INTO game_stats (user_id) VALUES (?)", (uid,), commit=True)
            if tot >= 0:
                db_exec(
                    "UPDATE game_stats SET games_total=games_total+1, wins=wins+1, max_win_cents=MAX(max_win_cents, ?) WHERE user_id=?",
                    (int(tot), uid), commit=True
                )
            else:
                db_exec(
                    "UPDATE game_stats SET games_total=games_total+1, losses=losses+1, max_lose_cents=MAX(max_lose_cents, ?) WHERE user_id=?",
                    (int(abs(tot)), uid), commit=True
                )
            bump_game_type_stat(uid, game_type)

        order = [r[0] for r in db_all("SELECT user_id FROM game_players WHERE game_id=? ORDER BY rowid", (game_id,))]
        if not order:
            return
        
        if (not is_demon) and (pstatus == "life") and (delta < 0) and creator_id:
            set_slave_buyout(uid, abs(delta) * 100)
            owner_id = pick_life_owner(game_id, int(uid), int(creator_id) if creator_id else None)
            if owner_id and int(owner_id) != int(uid):
                db_exec("INSERT OR IGNORE INTO slave_meta (slave_id) VALUES (?)", (int(uid),), commit=True)
                db_exec("UPDATE slave_meta SET strikes=strikes+1 WHERE slave_id=?", (int(uid),), commit=True)
                existed = db_one("SELECT 1 FROM slavery WHERE slave_id=? AND owner_id=?", (int(uid), int(owner_id)))
                db_exec(
                    "INSERT OR REPLACE INTO slavery (slave_id, owner_id, share_bp) VALUES (?,?,?)",
                    (int(uid), int(owner_id), 2000), commit=True
                )

                if not existed:
                    ou = get_user(int(owner_id))
                    oname = (ou[2] if ou and ou[2] else "Игрок")
                    oun = (ou[1] if ou and ou[1] else "")
                    o_tag = f" (@{html_escape(oun)})" if oun else ""
                    notify_safe(uid, f"Ты проиграл свою свободу. С этого момента ты личная собственность: <b>{html_escape(oname)}</b>{o_tag}")
    
        current_pos = int(turn_index) % len(order)
        is_round_last = (current_pos == len(order) - 1)
    
        header = "⟢♣♦ Рулетка ♥♠⟣" if game_type != "cross" else "⟢♣♦ Марафон рулетка ♥♠⟣"
        round_line = f"Раунд: <b>{int(cross_round)}</b>\n" if game_type == "cross" else ""
        result_line = f"Результат <u>{html_escape(pname)}</u>: <b>{cents_to_money_str(delta)}</b>$"
    
        strow = db_one("SELECT status FROM game_players WHERE game_id=? AND user_id=?", (game_id, uid))
        pstatus = (strow[0] if strow else "") or ""
        if pstatus == "life":
            stake_line = "Ваша ставка: <b>Ӂนℨℍ৮</b>"
        else:
            stake_line = f"Ваша ставка: <b>{cents_to_money_str(int(stake_now))}</b>$"
            if game_type == "cross":
                stake_line += f" + <b>{cents_to_money_str(int(add_cents))}</b>$"
    
        if game_type == "cross" and is_round_last and int(cross_round) < 9:
            next_round = int(cross_round) + 1
            next_fmt = cross_format_for_round(next_round)
            db_exec("UPDATE games SET cross_round=?, roulette_format=?, turn_index=0 WHERE game_id=?",
                        (next_round, next_fmt, game_id), commit=True)
    
            next_uid = order[0]
            next_user = get_user(next_uid)
            next_name = next_user[2] if next_user and next_user[2] else "Игрок"
            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton(f"Ход {next_name}", callback_data=cb_pack(f"turn:begin:{game_id}", next_uid)))
    
            final_text = (
                (f"<b>{header}</b>\n" + round_line + f"<b>Режим {title}</b>\n\n")
                + f"{final_grid}\n\n"
                + f"{result_line}\n"
                + f"{stake_line}\n\n"
                + f"{boosts_line}\n\n"
                + f"Следующий раунд: <b>{next_round}</b>"
            )
            _edit(final_text, kb=kb)
    
        elif is_round_last:
            db_exec("UPDATE games SET state='finished' WHERE game_id=?", (game_id,), commit=True)

            apply_demon_life_settlement(game_id)

            update_demon_streak_after_game(game_id)
    
            emancipate_slaves_after_game(game_id)
    
            rr2 = db_one("SELECT creator_id FROM games WHERE game_id=?", (game_id,))
            creator_id2 = int((rr2[0] if rr2 else 0) or 0)
            totals_text, totals_kb = render_game_totals(game_id, creator_id2)
    
            final_text = (
                (f"<b>{header}</b>\n" + round_line + f"<b>Режим {title}</b>\n\n")
                + f"{final_grid}\n\n"
                + f"{result_line}\n"
                + f"{stake_line}\n\n"
                + f"{boosts_line}\n\n"
                + f"{totals_text}"
            )
            _edit(final_text, kb=totals_kb)
    
        else:
            next_index = current_pos + 1
            next_uid = order[next_index]
            next_user = get_user(next_uid)
            next_name = next_user[2] if next_user and next_user[2] else "Игрок"
    
            db_exec("UPDATE games SET turn_index=? WHERE game_id=?", (next_index, game_id), commit=True)
    
            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton(
                f"Ход {next_name}",
                callback_data=cb_pack(f"turn:begin:{game_id}", next_uid)
            ))
    
            text = (
                (f"<b>{header}</b>\n" + round_line + f"<b>Режим {title}</b>\n\n")
                + f"{final_grid}\n\n"
                + f"{result_line}\n"
                + f"{stake_line}\n\n" + f"{boosts_line}"
            )
            _edit(text, kb=kb)
        db_exec("UPDATE spins SET stage='done' WHERE game_id=? AND user_id=?", (game_id, uid), commit=True)
    
    threading.Thread(target=run_spin, daemon=True).start()
    bot.answer_callback_query(call.id)
    return
@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("life:accept:"))
def on_life_accept(call: CallbackQuery):
    base, owner = cb_unpack(call.data)
    clicker = call.from_user.id

    if owner is not None and clicker != owner:
        bot.answer_callback_query(call.id, "Вы не можете нажать на эту кнопку", show_alert=True)
        return

    _, _, game_id = base.split(":", 2)

    cur.execute("SELECT state, stake_cents, creator_id FROM games WHERE game_id=?", (game_id,))
    g = cur.fetchone()
    if not g:
        bot.answer_callback_query(call.id, "Игра не найдена.", show_alert=True)
        return

    state, stake_cents, creator_id = g
    stake_cents = int(stake_cents or 0)
    creator_id = int(creator_id or 0)

    cur.execute("SELECT 1 FROM life_wait WHERE game_id=? AND user_id=?", (game_id, clicker))
    if not cur.fetchone():
        bot.answer_callback_query(call.id, "Нет ожидающего подтверждения.", show_alert=True)
        return

    cur.execute("INSERT OR IGNORE INTO slave_meta (slave_id) VALUES (?)", (clicker,))
    conn.commit()
    cur.execute("SELECT COALESCE(life_uses,0) FROM slave_meta WHERE slave_id=?", (clicker,))
    life_uses = int((cur.fetchone() or (0,))[0] or 0)
    if life_uses >= MAX_LIFE_STAKES:
        bot.answer_callback_query(call.id, "Лимит шансов поставить жизнь исчерпан.", show_alert=True)
        return
    u = get_user(clicker)
    bal = int(u[5] or 0) if u else 0
    if bal < 0:
        add_balance(clicker, -bal) 
    add_balance(clicker, stake_cents)

    cur.execute("SELECT status FROM game_players WHERE game_id=? AND user_id=?", (game_id, clicker))
    st = (cur.fetchone() or ("",))[0]
    if st != "need_life":
        bot.answer_callback_query(call.id, "Предложение уже неактуально.", show_alert=True)
        return

    inc_life_uses(clicker)
    try:
        if getattr(call, 'message', None) and call.message.chat and call.message.chat.type == 'private':
            bot.delete_message(call.message.chat.id, call.message.message_id)
    except Exception:
        pass

    cur.execute("UPDATE game_players SET status='life' WHERE game_id=? AND user_id=?", (game_id, clicker))
    cur.execute("DELETE FROM life_wait WHERE game_id=? AND user_id=?", (game_id, clicker))
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM life_wait WHERE game_id=?", (game_id,))
    pending = int(cur.fetchone()[0] or 0)
    if pending == 0:
        cur.execute("UPDATE games SET state='playing' WHERE game_id=?", (game_id,))
        conn.commit()

        cur.execute("SELECT user_id FROM game_players WHERE game_id=? ORDER BY rowid", (game_id,))
        order = [int(r[0]) for r in cur.fetchall()]
        if len(order) >= 2:
            first_uid = order[0]
            fu = get_user(first_uid)
            first_name = fu[2] if fu and fu[2] else "Игрок"

            text = (
                "Выбор сохранён.\n"
                f"Ставка <b>{cents_to_money_str(stake_cents)}</b>\n"
                "Приятной игры."
            )
            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton(f"Ход {first_name}", callback_data=cb_pack(f"turn:begin:{game_id}", first_uid)))
            edit_game_message(game_id, text, reply_markup=kb, parse_mode="HTML")

    try:
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
    except Exception:
        pass

    bot.answer_callback_query(call.id, "С вами приятно иметь дело.")

def is_slave(uid: int) -> bool:
    cur.execute("SELECT 1 FROM slavery WHERE slave_id=? LIMIT 1", (uid,))
    return cur.fetchone() is not None

def owns_slaves(uid: int) -> bool:
    cur.execute("SELECT 1 FROM slavery WHERE owner_id=? LIMIT 1", (uid,))
    return cur.fetchone() is not None

def get_game_stats(uid: int) -> Tuple[int,int,int,int,int]:
    cur.execute("INSERT OR IGNORE INTO game_stats (user_id) VALUES (?)", (uid,))
    conn.commit()
    cur.execute("SELECT games_total, wins, losses, max_win_cents, max_lose_cents FROM game_stats WHERE user_id=?", (uid,))
    row = cur.fetchone()
    return tuple(int(x or 0) for x in row)

def slavery_add_owner(slave_id: int, owner_id: int, share_bp: int = 2000) -> bool:
    """
    Добавляет владельца раба (без self-ownership) и проставляет acquired_ts (если есть колонка).
    Возвращает True, если связь (slave->owner) была создана впервые.
    """
    try:
        slave_id = int(slave_id)
        owner_id = int(owner_id)
        share_bp = int(share_bp)
    except Exception:
        return False

    if slave_id <= 0 or owner_id <= 0 or slave_id == owner_id:
        return False

    existed = False
    try:
        cur.execute("SELECT 1 FROM slavery WHERE slave_id=? AND owner_id=? LIMIT 1", (slave_id, owner_id))
        existed = cur.fetchone() is not None
    except Exception:
        existed = False

    ts = now_ts()

    try:
        cur.execute(
            "INSERT OR IGNORE INTO slavery (slave_id, owner_id, share_bp, acquired_ts) VALUES (?,?,?,?)",
            (slave_id, owner_id, share_bp, ts)
        )
        inserted = (cur.rowcount or 0) > 0
        cur.execute(
            "UPDATE slavery SET acquired_ts=? WHERE slave_id=? AND owner_id=? AND (acquired_ts IS NULL OR acquired_ts=0)",
            (ts, slave_id, owner_id)
        )
        conn.commit()
        return inserted and (not existed)
    except Exception:
        pass

    try:
        cur.execute(
            "INSERT OR IGNORE INTO slavery (slave_id, owner_id, share_bp) VALUES (?,?,?)",
            (slave_id, owner_id, share_bp)
        )
        inserted = (cur.rowcount or 0) > 0
        conn.commit()
    except Exception:
        return False

    return inserted and (not existed)

def slave_profit_last24h(slave_id: int, owner_id: int) -> int:
    """Сумма выплат от раба владельцу за последние 24 часа."""
    try:
        ts0 = now_ts() - 24*3600
        cur.execute(
            "SELECT COALESCE(SUM(amount_cents),0) FROM slave_earn_log WHERE slave_id=? AND owner_id=? AND ts>=?",
            (int(slave_id), int(owner_id), int(ts0))
        )
        return int((cur.fetchone() or (0,))[0] or 0)
    except Exception:
        return 0

def apply_slave_cut(slave_id: int, income_cents: int, reason: str = "") -> int:
    """
    Если пользователь раб - удерживаем доли share_bp и раздаём владельцам.
    Возвращает income_cents ПОСЛЕ удержания.

    Также логируем выплаты для "за последние 24 часа" (slave_earn_log).
    """
    if income_cents <= 0:
        return income_cents

    cur.execute("SELECT owner_id, share_bp FROM slavery WHERE slave_id=?", (slave_id,))
    rows = cur.fetchall()
    if not rows:
        return income_cents

    total_bp = sum(int(r[1] or 0) for r in rows)
    if total_bp <= 0:
        return income_cents

    total_bp = min(total_bp, 10000)

    kept = income_cents
    ts = now_ts()

    for owner_id, bp in rows:
        owner_id = int(owner_id or 0)
        bp = int(bp or 0)
        if owner_id <= 0 or bp <= 0:
            continue

        part = int((income_cents * bp) // 10000)
        if part <= 0:
            continue

        add_balance(owner_id, part)

        try:
            cur.execute(
                "UPDATE slavery SET earned_cents = COALESCE(earned_cents, 0) + ? WHERE slave_id=? AND owner_id=?",
                (part, int(slave_id), owner_id)
            )
        except Exception:
            pass
        try:
            cur.execute(
                "INSERT INTO slave_earn_log (slave_id, owner_id, ts, amount_cents) VALUES (?,?,?,?)",
                (int(slave_id), owner_id, ts, part)
            )
        except Exception:
            pass

        kept -= part
    try:
        conn.commit()
    except Exception:
        pass

    return kept

def set_slave_buyout(slave_id: int, buyout_cents: int):
    """Сумма выкупа раба (в центах)."""
    buyout_cents = int(buyout_cents or 0)
    if buyout_cents < 0:
        buyout_cents = -buyout_cents
    cur.execute("INSERT OR IGNORE INTO slave_meta (slave_id) VALUES (?)", (int(slave_id),))
    cur.execute("UPDATE slave_meta SET buyout_cents=? WHERE slave_id=?", (buyout_cents, int(slave_id)))
    conn.commit()

def clear_slave_buyout(slave_id: int):
    cur.execute("INSERT OR IGNORE INTO slave_meta (slave_id) VALUES (?)", (int(slave_id),))
    cur.execute("UPDATE slave_meta SET buyout_cents=0 WHERE slave_id=?", (int(slave_id),))
    conn.commit()


def _ensure_slave_meta_row(uid: int):
    cur.execute("INSERT OR IGNORE INTO slave_meta (slave_id) VALUES (?)", (int(uid),))
    conn.commit()

def get_life_uses(uid: int) -> int:
    _ensure_slave_meta_row(uid)
    cur.execute("SELECT life_uses FROM slave_meta WHERE slave_id=?", (int(uid),))
    return int((cur.fetchone() or (0,))[0] or 0)

def get_life_remaining(uid: int) -> int:
    used = get_life_uses(uid)
    rem = MAX_LIFE_STAKES - used
    return rem if rem > 0 else 0

def inc_life_uses(uid: int):
    _ensure_slave_meta_row(uid)
    cur.execute("UPDATE slave_meta SET life_uses=COALESCE(life_uses,0)+1 WHERE slave_id=?", (int(uid),))
    conn.commit()

def get_slave_owners(slave_id: int):
    cur.execute("SELECT owner_id, share_bp FROM slavery WHERE slave_id=? ORDER BY share_bp DESC", (int(slave_id),))
    return [(int(o), int(bp or 0)) for (o, bp) in cur.fetchall()]

def notify_safe(uid: int, text: str):
    try:
        bot.send_message(int(uid), text, parse_mode="HTML")
    except Exception:
        pass

def remove_owner_from_slave(slave_id: int, owner_id: int) -> bool:
    cur.execute("SELECT 1 FROM slavery WHERE slave_id=? AND owner_id=?", (int(slave_id), int(owner_id)))
    existed = cur.fetchone() is not None
    if existed:
        cur.execute("DELETE FROM slavery WHERE slave_id=? AND owner_id=?", (int(slave_id), int(owner_id)))
        conn.commit()
    return existed

def free_slave_fully(slave_id: int, reason: str):
    """Полное освобождение: удаляем все доли владельцев + обнуляем buyout."""
    owners = get_slave_owners(slave_id)
    cur.execute("DELETE FROM slavery WHERE slave_id=?", (int(slave_id),))
    conn.commit()
    clear_slave_buyout(slave_id)

    su = get_user(slave_id)
    sname = (su[2] if su and su[2] else "Игрок")
    sun = (su[1] if su and su[1] else "")
    stag = f" (@{html_escape(sun)})" if sun else ""
    s_line = f"<b>{html_escape(sname)}</b>{stag}"

    for oid, _bp in owners:
        notify_safe(oid, f"ℹРаб {s_line} освободился. Причина: <b>{html_escape(reason)}</b>")

    if owners:
        notify_safe(slave_id, f"Ты освобождён от статуса раба. Причина: <b>{html_escape(reason)}</b>")

def emancipate_slaves_after_game(game_id: str):
    """
    Освобождение после игры:
    - Если раб в этой игре обыграл демона -> полное освобождение
    - Если раб обыграл одного/нескольких владельцев, которые участвовали -> удаляем их долю
      (если владельцев больше не осталось -> полное освобождение)
    """
    cur.execute("""
        SELECT gp.user_id, COALESCE(gr.delta_cents, 0) AS delta
        FROM game_players gp
        LEFT JOIN game_results gr
          ON gr.game_id = gp.game_id AND gr.user_id = gp.user_id
        WHERE gp.game_id=?
    """, (game_id,))
    rows = [(int(uid), int(delta or 0)) for (uid, delta) in cur.fetchall()]
    if not rows:
        return

    deltas = {uid: delta for uid, delta in rows}
    participants = list(deltas.keys())
    if not participants:
        return

    qmarks = ",".join(["?"] * len(participants))
    cur.execute(f"SELECT user_id FROM users WHERE demon=1 AND user_id IN ({qmarks})", tuple(participants))
    demons = {int(r[0]) for r in cur.fetchall()}

    for uid in participants:
        if not is_slave(uid):
            continue

        my_delta = deltas.get(uid, 0)

        demon_beaten = any(my_delta > deltas.get(did, 0) for did in demons)
        if demon_beaten:
            free_slave_fully(uid, "победа над демоном в игре")
            continue

        owners = get_slave_owners(uid)
        removed = []
        for owner_id, _bp in owners:
            if owner_id in deltas and my_delta > deltas.get(owner_id, 0):
                if remove_owner_from_slave(uid, owner_id):
                    removed.append(owner_id)

        if removed:
            su = get_user(uid)
            sname = (su[2] if su and su[2] else "Игрок")
            sun = (su[1] if su and su[1] else "")
            s_line = f"<b>{html_escape(sname)}</b>" + (f" (@{html_escape(sun)})" if sun else "")

            for oid in removed:
                notify_safe(oid, f"Ты потерял права на раба {s_line}: он обыграл тебя в игре.")

            if not is_slave(uid):
                free_slave_fully(uid, "победа над владельцем в игре")

def apply_demon_life_settlement(game_id: str):
    g = db_one("SELECT COALESCE(stake_kind,'money'), COALESCE(life_demon_id,0), COALESCE(demon_settled,0) FROM games WHERE game_id=?", (game_id,))
    if not g:
        return
    stake_kind, life_demon_id, demon_settled = (g[0] or "money"), int(g[1] or 0), int(g[2] or 0)
    if stake_kind != "life_demon" or demon_settled == 1:
        return

    db_exec("UPDATE games SET demon_settled=1 WHERE game_id=?", (game_id,), commit=True)

    rows = db_all("""
        SELECT gp.user_id, COALESCE(gr.delta_cents,0) AS delta
        FROM game_players gp
        LEFT JOIN game_results gr ON gr.game_id=gp.game_id AND gr.user_id=gp.user_id
        WHERE gp.game_id=?
    """, (game_id,))
    if not rows or len(rows) < 2:
        return

    rows.sort(key=lambda r: int(r[1] or 0), reverse=True)
    winner_id = int(rows[0][0])
    loser_id  = int(rows[-1][0])

    w = get_user(winner_id)
    l = get_user(loser_id)
    w_is_demon = bool(w and int(w[7] or 0) == 1)
    l_is_demon = bool(l and int(l[7] or 0) == 1)

    # демон проиграл обычному: перевод 50% капитала + письмо
    if l_is_demon and (not w_is_demon):
        demon_bal = get_balance_cents(loser_id)
        payout = demon_bal // 2
        if payout > 0:
            add_balance(loser_id, -payout)

            kept = apply_slave_cut(winner_id, payout, reason="demon_pay")
            add_balance(winner_id, kept)

            try:
                ensure_daily_mail_row(winner_id)
                _send_mail_prompt(winner_id, "demon_pay", kept)
            except Exception:
                pass
        return

    # демон победил обычного: забирает душу, цена выкупа = 25×капитал демона (в центах)
    if w_is_demon and (not l_is_demon):
        inserted = slavery_add_owner(loser_id, winner_id, 2000)
        demon_bal = get_balance_cents(winner_id)
        set_slave_buyout(loser_id, int(demon_bal) * 25)

        if inserted:
            try:
                un = l[3] if l else ""
                uname = f" (@{un})" if un else ""
                bot.send_message(
                    loser_id,
                    f"Ты проиграл свою свободу. С этого момента ты личная собственность: <b>{html_escape(w[2] or 'Демон')}</b>{uname}",
                    parse_mode="HTML"
                )
            except Exception:
                pass
        return

    # демон победил демона: победителю отправляем список рабов проигравшего (команда /get)
    if w_is_demon and l_is_demon:
        slaves = db_all("SELECT slave_id FROM slavery WHERE owner_id=? ORDER BY slave_id", (loser_id,))
        if not slaves:
            return

        for (sid,) in slaves:
            db_exec(
                "INSERT OR IGNORE INTO demon_loot (winner_id, loser_id, slave_id, ts, taken) VALUES (?,?,?,?,0)",
                (winner_id, loser_id, int(sid), now_ts()),
                commit=False
            )
        db_exec("SELECT 1", (), commit=True)

        lines = ["⟢♣♦ Добыча демона ♥♠⟣", "", "Выигранные рабы:"]
        for (sid,) in slaves[:30]:
            ru = get_user(int(sid))
            nm = (ru[2] if ru and ru[2] else "Без имени")
            un = (ru[3] if ru and ru[3] else "")
            uname = f" (@{un})" if un else ""
            lines.append(f"• {nm}{uname}")

        lines.append("")
        lines.append("Забрать раба: /get @username")
        try:
            bot.send_message(winner_id, "\n".join(lines))
        except Exception:
            pass

# DEV COMMANDS
@bot.message_handler(commands=["devil"])
def cmd_devil(message):
    if message.from_user.id != OWNER_ID:
        return
    parts = message.text.split()
    target = message.from_user.id
    if len(parts) >= 2 and parts[1].startswith("@"):
        uname = parts[1][1:]
        cur.execute("SELECT user_id FROM users WHERE username=?", (uname,))
        r = cur.fetchone()
        if r:
            target = int(r[0])
    upsert_user(target, None)
    cur.execute("UPDATE users SET demon=1 WHERE user_id=?", (target,))
    conn.commit()
    bot.reply_to(message, "Статус ¨Демон¨ установлен.")

def _work_daemon():
    while True:
        try:
            cur.execute("SELECT user_id FROM work_shift WHERE ends_ts <= ?", (now_ts(),))
            uids = [int(r[0]) for r in cur.fetchall()]
            for uid in uids:
                finish_shift(uid)
        except Exception:
            pass
        time.sleep(2)

threading.Thread(target=_work_daemon, daemon=True).start()
threading.Thread(target=_mail_daemon, daemon=True).start()

@bot.message_handler(commands=["human"])
def cmd_human(message):
    if message.from_user.id != OWNER_ID:
        return
    parts = message.text.split()
    target = message.from_user.id
    if len(parts) >= 2 and parts[1].startswith("@"):
        uname = parts[1][1:]
        cur.execute("SELECT user_id FROM users WHERE username=?", (uname,))
        r = cur.fetchone()
        if r:
            target = int(r[0])
    cur.execute("SELECT demo_gift_cents FROM users WHERE user_id=?", (target,))
    r = cur.fetchone()
    gift = int(r[0] or 0) if r else 0
    cur.execute("UPDATE users SET demon=0, balance_cents=? WHERE user_id=?", (gift, target))
    conn.commit()
    bot.reply_to(message, "Статус ¨Демон¨ снят, профиль откатан.")

@bot.message_handler(commands=["get"])
def cmd_get(message):
    if message.chat.type != "private":
        return

    demon_id = message.from_user.id
    upsert_user(demon_id, getattr(message.from_user, "username", None))
    u = get_user(demon_id)
    if not u or int(u[7] or 0) != 1:
        bot.reply_to(message, "Эта команда доступна только демонам.")
        return

    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].startswith("@"):
        bot.reply_to(message, "Использование: /get @username")
        return

    target_un = parts[1][1:]
    rr = db_one("SELECT user_id, short_name, username FROM users WHERE username=?", (target_un,))
    if not rr:
        bot.reply_to(message, "Пользователь не найден в базе.")
        return

    slave_id = int(rr[0])

    loot = db_one(
        "SELECT loser_id, taken FROM demon_loot WHERE winner_id=? AND slave_id=?",
        (demon_id, slave_id)
    )
    if not loot:
        bot.reply_to(message, "Нет прав на этого раба.")
        return

    loser_id, taken = int(loot[0] or 0), int(loot[1] or 0)
    if taken == 1:
        bot.reply_to(message, "Этот раб уже был забран.")
        return

    db_exec("DELETE FROM slavery WHERE slave_id=? AND owner_id=?", (slave_id, loser_id), commit=True)
    slavery_add_owner(slave_id, demon_id, 2000)

    db_exec("UPDATE demon_loot SET taken=1 WHERE winner_id=? AND slave_id=?", (demon_id, slave_id), commit=True)

    bot.reply_to(message, "Готово. Раб передан тебе.")

@bot.message_handler(commands=["finance"])
def cmd_finance(message):
    if message.from_user.id != OWNER_ID:
        return
    if message.chat.type != "private":
        return

    raw = message.text or ""
    lines = raw.split("\n")
    head = (lines[0] or "").strip()
    comment = "\n".join(lines[1:]).strip()

    parts = head.split()
    if len(parts) < 3 or not parts[1].startswith("@"):
        bot.reply_to(message, "Использование: /finance @username сумма\n<комментарий (необязательно)>")
        return

    uname = parts[1][1:]
    amt = money_to_cents(parts[2])
    if amt is None:
        bot.reply_to(message, "Неверная сумма.")
        return

    r = db_one("SELECT user_id FROM users WHERE username=?", (uname,))
    if not r:
        bot.reply_to(message, "Пользователь не найден в базе.")
        return

    uid = int(r[0])
    add_balance(uid, amt)

    try:
        ensure_daily_mail_row(uid)
        payload = base64.urlsafe_b64encode((comment or "").encode("utf-8")).decode("ascii")
        _send_mail_prompt(uid, f"owner_finance|{payload}", int(amt))
    except Exception:
        pass

    bot.reply_to(message, f"Начислено {cents_to_money_str(amt)}$ пользователю @{uname}")

@bot.message_handler(commands=["reg"])
def cmd_reg(message):
    if message.from_user.id != OWNER_ID:
        return
    if message.chat.type != "private":
        return

    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 3:
        bot.reply_to(message, "Использование: /reg user_id Имя")
        return

    try:
        uid = int(parts[1])
    except Exception:
        bot.reply_to(message, "user_id должен быть числом.")
        return

    name = parts[2].strip()
    if not name or " " in name:
        bot.reply_to(message, "Имя должно быть одним словом.")
        return

    fetched_username = None
    try:
        ch = bot.get_chat(uid)
        fetched_username = getattr(ch, "username", None)
    except Exception:
        fetched_username = None

    upsert_user(uid, fetched_username)
    set_short_name(uid, name)

    try:
        set_reg_state(uid, None, None)
    except Exception:
        pass

    u = get_user(uid)
    contract_ts = int((u[4] if u else 0) or 0)
    if contract_ts <= 0:
        gift = 1000 * 100
        set_contract_signed(uid, gift)  
        contract_note = "контракт подписан, выдано 1000$"
    else:
        contract_note = "контракт уже был подписан (без доп. начислений)"

    u2 = get_user(uid)
    uname = (u2[1] if u2 else None) or ""
    uname_text = f"@{uname}" if uname else "(username неизвестен)"

    bot.reply_to(message, f"Готово: user_id={uid}, имя={name}, {uname_text}; {contract_note}")

    try:
        bot.send_message(
            uid,
            f"Вы зарегистрированы администратором. Ваше имя: <b>{html_escape(name)}</b>",
            parse_mode="HTML"
        )
    except Exception:
        pass

@bot.message_handler(commands=["work"])
def cmd_work(message):
    if message.from_user.id != OWNER_ID:
        return
    if message.chat.type != "private":
        return

    parts = message.text.split()
    if len(parts) < 2 or not parts[1].startswith("@"):
        bot.reply_to(message, "Использование: /work @username")
        return
    uname = parts[1][1:].strip()

    r = db_one("SELECT user_id FROM users WHERE username=?", (uname,))
    if not r:
        bot.reply_to(message, "Пользователь не найден в базе.")
        return
    uid = int(r[0])

    u = get_user(uid)
    if not u or not u[2]:
        bot.reply_to(message, "У пользователя нет анкеты (не введено имя).")
        return

    cur_shift = db_one(
        "SELECT user_id, job_key, started_ts, ends_ts, salary_full_cents, success_pct FROM work_shift WHERE user_id=?",
        (uid,)
    )
    if cur_shift:
        ends_ts = int(cur_shift[3] or 0)
        bot.reply_to(message, f"Пользователь уже работает. Вернётся через {_format_duration(max(0, ends_ts - now_ts()))}.")
        return

    jobs = load_jobs()
    if not jobs:
        bot.reply_to(message, "Список вакансий пуст (файл работ не загружен).")
        return

    job_key = list(jobs.keys())[0]
    shifts, days, earned = get_work_stats(uid, job_key)
    salary_full = _salary_with_seniority(jobs[job_key], days)
    ends_ts = now_ts() + int(jobs[job_key].hours) * 3600

    db_exec("""
    INSERT INTO work_shift (user_id, job_key, started_ts, ends_ts, salary_full_cents, success_pct)
    VALUES (?,?,?,?,?,?)
    ON CONFLICT(user_id) DO UPDATE SET
      job_key=excluded.job_key,
      started_ts=excluded.started_ts,
      ends_ts=excluded.ends_ts,
      salary_full_cents=excluded.salary_full_cents,
      success_pct=excluded.success_pct
    """, (uid, job_key, now_ts(), ends_ts, int(salary_full), int(jobs[job_key].success_pct)), commit=True)

    bot.reply_to(message, f"Пользователь @{uname} отправлен на работу: {jobs[job_key].title} (до {time.strftime('%H:%M:%S', time.localtime(ends_ts))})")

    try:
        bot.send_message(uid, f"Вас отправили на работу: <b>{html_escape(jobs[job_key].title)}</b>\nВернётесь через {_format_duration(ends_ts - now_ts())}.", parse_mode="HTML")
    except Exception:
        pass

@bot.message_handler(commands=["profile"])
def cmd_profile(message):
    if message.chat.type != "private":
        return
    uid = message.from_user.id
    username = getattr(message.from_user, "username", None)
    upsert_user(uid, username)

    u = get_user(uid)
    if not u or not u[2]:
        return

    cur.execute("SELECT user_id FROM users WHERE demon=0")
    uids = [r[0] for r in cur.fetchall()]
    uids.sort(key=lambda x: top_value_cents(x), reverse=True)

    place = (uids.index(uid) + 1) if (u[7] == 0 and uid in uids) else "-"
    status = compute_status(uid)

    text = (
        f"Имя пользователя: <i>{html_escape(u[2])}</i>\n"
        f"Дата подписания контракта: <b>{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(u[4] or u[3] or now_ts()))}</b>\n"
        f"Статус: <b>{html_escape(status)}</b>\n"
        f"Капитал: <b>{cents_to_money_str(int(u[5] or 0))}</b>$\n"
        f"Место в топе: <b>{place}</b>"
    )
    bot.send_message(message.chat.id, text, parse_mode="HTML")

@bot.message_handler(commands=["buyout"])
def cmd_buyout(message):
    if message.chat.type != "private":
        return

    uid = message.from_user.id
    username = getattr(message.from_user, "username", None)
    upsert_user(uid, username)

    if not is_slave(uid):
        bot.send_message(message.chat.id, "Ты не раб.")
        return

    cur.execute("INSERT OR IGNORE INTO slave_meta (slave_id) VALUES (?)", (uid,))
    conn.commit()
    cur.execute("SELECT buyout_cents FROM slave_meta WHERE slave_id=?", (uid,))
    buyout_cents = int((cur.fetchone() or (0,))[0] or 0)

    if buyout_cents <= 0:
        bot.send_message(message.chat.id, "Сумма выкупа не назначена.")
        return

    u = get_user(uid)
    bal = int(u[5] or 0) if u else 0
    if bal < buyout_cents:
        bot.send_message(
            message.chat.id,
            f"Недостаточно средств. Необходимо <b>{cents_to_money_str(buyout_cents)}</b>$",
            parse_mode="HTML"
        )
        return

    owners = get_slave_owners(uid)
    if not owners:
        free_slave_fully(uid, "самовыкуп (владельцы не найдены)")
        bot.send_message(message.chat.id, "Ты освобождён.", parse_mode="HTML")
        return

    total_bp = sum(bp for _oid, bp in owners) or 0
    if total_bp <= 0:
        total_bp = 10000

    add_balance(uid, -buyout_cents)

    distributed = 0
    for i, (oid, bp) in enumerate(owners):
        part = int((buyout_cents * bp) // total_bp) if bp > 0 else 0
        if i == 0:
            part += (buyout_cents - sum(int((buyout_cents * b) // total_bp) for _o, b in owners))
        if part > 0:
            add_balance(oid, part)
            distributed += part
            notify_safe(oid, f"Раб выкупил себя. Сумма, которую он оставил вам за свою свободу: <b>{cents_to_money_str(part)}</b>$",)

    free_slave_fully(uid, "самовыкуп")

    bot.send_message(
        message.chat.id,
        f"Ты успешно выкупил свою свободу за <b>{cents_to_money_str(buyout_cents)}</b>$.",
        parse_mode="HTML"
    )

@bot.message_handler(commands=["buy"])
def cmd_buy(message):
    if message.chat.type != "private":
        return

    buyer_id = message.from_user.id
    buyer_username = getattr(message.from_user, "username", None)
    upsert_user(buyer_id, buyer_username)

    parts = (message.text or "").split()
    if len(parts) < 3 or not parts[1].startswith("@"):
        bot.reply_to(message, "Использование: /buy @username цена")
        return

    slave_un = parts[1][1:]
    price_cents = money_to_cents(parts[2])
    if price_cents is None or price_cents <= 0:
        bot.reply_to(message, "Неверная цена.")
        return

    cur.execute("SELECT user_id, short_name, username FROM users WHERE username=?", (slave_un,))
    rr = cur.fetchone()
    if not rr:
        bot.reply_to(message, "Пользователь не найден в базе.")
        return

    slave_id = int(rr[0])
    slave_name = rr[1] or "Без имени"

    cur.execute("SELECT 1 FROM slavery WHERE slave_id=? AND owner_id=? LIMIT 1", (slave_id, buyer_id))
    if cur.fetchone() is None:
        bot.reply_to(message, "Ты не являешься владельцем этого раба.")
        return

    cur.execute("SELECT owner_id FROM slavery WHERE slave_id=? AND owner_id<>?", (slave_id, buyer_id))
    other_owners = [int(r[0]) for r in cur.fetchall()]
    if not other_owners:
        bot.reply_to(message, "Ты уже единственный владелец.")
        return

    cur.execute("SELECT balance_cents FROM users WHERE user_id=?", (buyer_id,))
    bal = cur.fetchone()
    buyer_bal = int(bal[0] or 0) if bal else 0

    buyer_bal = get_balance_cents(buyer_id)
    if buyer_bal < 0:
        bot.reply_to(message, "Сделка невозможна: у вас минусовой баланс.")
        return
    
    if price_cents <= 0:
        bot.reply_to(message, "Цена должна быть больше нуля.")
        return
    
    if buyer_bal < price_cents:
        bot.reply_to(message, "Сделка невозможна: недостаточно средств.")
        return

    worst_cost = price_cents * len(other_owners)
    if buyer_bal < 0 or buyer_bal < worst_cost:
        bot.reply_to(message, f"Недостаточно средств. Необходимо минимум {cents_to_money_str(worst_cost)}$")
        return

    offer_id = uuid.uuid4().hex[:8]
    cur.execute(
        "INSERT INTO buy_offers (offer_id, slave_id, buyer_id, price_cents, created_ts, active) VALUES (?,?,?,?,?,1)",
        (offer_id, slave_id, buyer_id, price_cents, now_ts()),
    )
    for oid in other_owners:
        cur.execute("INSERT OR IGNORE INTO buy_offer_resp (offer_id, owner_id, status) VALUES (?,?,0)", (offer_id, oid))
    conn.commit()

    buyer_u = get_user(buyer_id)
    buyer_name = (buyer_u[2] if buyer_u and buyer_u[2] else "Игрок")
    buyer_un = (buyer_u[1] if buyer_u and buyer_u[1] else None)
    buyer_tag = f"@{buyer_un}" if buyer_un else html_escape(buyer_name)

    text = (
        f"Предложение о выкупе раба:\n\n"
        f"Раб: <b>{html_escape(slave_name)}</b> (@{html_escape(slave_un)})\n"
        f"Покупатель: <b>{html_escape(buyer_tag)}</b>\n"
        f"Цена за твою долю: <b>{cents_to_money_str(price_cents)}</b>$\n\n"
        f"Согласиться?"
    )

    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Согласиться", callback_data=cb_pack(f"buy:acc:{offer_id}", 0)))
    kb.add(InlineKeyboardButton("Отказаться", callback_data=cb_pack(f"buy:dec:{offer_id}", 0)))

    sent_count = 0
    for oid in other_owners:
        try:
            kb2 = InlineKeyboardMarkup()
            kb2.add(InlineKeyboardButton("Согласиться", callback_data=cb_pack(f"buy:acc:{offer_id}", oid)))
            kb2.add(InlineKeyboardButton("Отказаться", callback_data=cb_pack(f"buy:dec:{offer_id}", oid)))
            bot.send_message(oid, text, parse_mode="HTML", reply_markup=kb2)
            sent_count += 1
        except Exception:
            pass

    bot.reply_to(message, f"Оффер отправлен на рассмотрение владельцам: {sent_count}/{len(other_owners)}")

@bot.message_handler(commands=["shop"])
def cmd_shop(message):
    if message.chat.type != "private":
        return
    uid = message.from_user.id
    username = getattr(message.from_user, "username", None)
    upsert_user(uid, username)

    u = get_user(uid)
    if not u or not u[2]:
        return

    text = shop_menu_text(uid)
    kb = shop_menu_kb(uid)
    bot.send_message(message.chat.id, text, parse_mode="HTML", reply_markup=kb)

def integrity_ok(c: sqlite3.Connection) -> bool:
    try:
        r = c.execute("PRAGMA integrity_check;").fetchone()
        return bool(r and r[0] == "ok")
    except Exception:
        return False

# RUN
print(f"Contest bot started as @{BOT_USERNAME}")
bot.infinity_polling(skip_pending=True)