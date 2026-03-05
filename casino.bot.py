import os
import re
import time
import uuid
import base64
import sqlite3
import shutil
import random
import threading
from dataclasses import dataclass
from html import escape as html_escape
from typing import Optional, List, Tuple, Dict
from telebot import TeleBot
from telebot.handler_backends import ContinueHandling
from telebot.types import (
    InlineQuery,
    InlineQueryResultArticle,
    InputTextMessageContent,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    InputMediaPhoto,
)
import io
import sys
import traceback

# CONFIG
OWNER_ID = int(os.environ.get("OWNER_ID", "7739179390"))
MAX_LIFE_STAKES = 5  # сколько раз можно поставить жизнь
PHOTO_FILE_ID = os.environ.get("ZERO_PHOTO_FILE_ID", "AgACAgIAAxkBAAPBaZ38U4pV3G6c4JFrmMMD1-aU0nUAAiwUaxsXG_BIB9b_4FJoVCoBAAMCAAN3AAM6BA")  # file_id для фото в Зеро-рулетке

INLINE_THUMB_START_URL = os.environ.get("INLINE_THUMB_START_URL", "")
INLINE_THUMB_BAN_URL = os.environ.get("INLINE_THUMB_BAN_URL", "")
INLINE_THUMB_GAME_URL = os.environ.get("INLINE_THUMB_GAME_URL", "")
INLINE_THUMB_PROFILE_URL = os.environ.get("INLINE_THUMB_PROFILE_URL", "")
INLINE_THUMB_STATS_URL = os.environ.get("INLINE_THUMB_STATS_URL", "")
INLINE_THUMB_WORK_URL = os.environ.get("INLINE_THUMB_WORK_URL", "")
INLINE_THUMB_CREDIT_URL = os.environ.get("INLINE_THUMB_CREDIT_URL", "")

_INLINE_THUMB_URL_CACHE: dict[str, str] = {}

def load_bot_token() -> str:
    """
    Приоритет:
      1) переменные окружения (сервер)
      2) локальный конфиг config_local.py (ноутбук/тест)
    """
    token = (os.environ.get("BOT_TOKEN") or os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
    if token:
        return token

    try:
        import config_local  # type: ignore файл рядом с ботом
        token = str(getattr(config_local, "BOT_TOKEN", "") or "").strip()
        if token:
            return token
    except Exception:
        pass

    raise RuntimeError(
        "BOT_TOKEN не задан. Укажи переменную окружения BOT_TOKEN "
        "или создай файл config_local.py рядом с ботом с BOT_TOKEN = '...'."
    )

BOT_TOKEN = load_bot_token()

bot = TeleBot(BOT_TOKEN, threaded=True, num_threads=8)

# MAINTENANCE GATES
_SLEEP_CHAT_LAST_TS: Dict[int, int] = {}
_FORCE_SLEEPING: bool = False
_FORCE_SLEEP_MODE: str = ""  # "error"|"update"

def _sleep_chat_cooldown_ok(chat_id: int, sec: int = 300) -> bool:
    chat_id = int(chat_id or 0)
    now = int(time.time())
    prev = int(_SLEEP_CHAT_LAST_TS.get(chat_id, 0) or 0)
    if (now - prev) < int(sec):
        return False
    _SLEEP_CHAT_LAST_TS[chat_id] = now
    return True

@bot.message_handler(
    func=lambda m: True,
    content_types=[
        "text", "photo", "audio", "document", "animation", "game",
        "video", "voice", "video_note", "location", "contact",
        "sticker", "venue", "dice", "new_chat_members",
        "left_chat_member", "pinned_message"
    ]
)
def _maintenance_gate_message(message):
    try:
        _touch_group_from_message(message)
    except Exception:
        pass

    try:
        sleeping = bool(_FORCE_SLEEPING)
        if not sleeping:
            sleeping, _mode, _reason, _last_err = get_bot_sleep_state()

        if not sleeping:
            return ContinueHandling()

        uid = int(getattr(getattr(message, "from_user", None), "id", 0) or 0)
        if is_bot_admin(uid):
            return ContinueHandling()

        txt = (getattr(message, "text", "") or "").strip()

        if txt.startswith("/report"):
            return ContinueHandling()
        if getattr(getattr(message, "chat", None), "type", "") == "private":
            try:
                rs = db_one("SELECT 1 FROM report_state WHERE user_id=? LIMIT 1", (uid,))
                if rs:
                    return ContinueHandling()
            except Exception:
                pass

        chat = getattr(message, "chat", None)
        chat_id = int(getattr(chat, "id", 0) or 0)
        chat_type = str(getattr(chat, "type", "") or "")

        if chat_type in ("group", "supergroup"):
            return

        if not txt.startswith("/"):
            return

        if chat_id and _sleep_chat_cooldown_ok(chat_id, sec=300):
            try:
                bot.send_message(chat_id, build_sleep_notice_text(), parse_mode="HTML")
            except Exception:
                pass
        return

    except Exception:
        return ContinueHandling()

@bot.callback_query_handler(func=lambda c: True)
def _maintenance_gate_callback(call: CallbackQuery):
    try:
        _touch_group_from_callback(call)
    except Exception:
        pass

    sleeping = bool(_FORCE_SLEEPING)
    if not sleeping:
        try:
            sleeping, _mode, _reason, _last_err = get_bot_sleep_state()
        except Exception:
            sleeping = False

    if not sleeping:
        return ContinueHandling()

    try:
        data = str(getattr(call, "data", "") or "")
    except Exception:
        data = ""

    allow = (
        data.startswith("report:")
        or data.startswith("profile:commands")
        or data.startswith("profile:open")
    )
    if allow:
        return ContinueHandling()

    try:
        bot.answer_callback_query(call.id, "Бот временно отключён (технические работы).", show_alert=True)
    except Exception:
        pass
    return

# Коды ошибок 
_ERROR_REPORT_LAST: dict[str, int] = {}
ERROR_REPORT_COOLDOWN_SEC = 60

def send_error_report(context: str, exc: Exception | None = None) -> None:
    try:
        now = int(time.time())
        prev = int(_ERROR_REPORT_LAST.get(context, 0) or 0)
        if (now - prev) < ERROR_REPORT_COOLDOWN_SEC:
            return
        _ERROR_REPORT_LAST[context] = now
        try:
            remember_last_error(context)
        except Exception:
            pass       

        if exc is None:
            tb = traceback.format_exc()
            if not tb or tb.strip() == "NoneType: None":
                tb = "".join(traceback.format_stack(limit=40))
        else:
            tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))

        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now))
        payload = f"[{ts}] {context}\n\n{tb}".encode("utf-8", errors="replace")

        bio = io.BytesIO(payload)
        bio.name = f"bot_error_{now}.txt"

        bot.send_document(
            OWNER_ID,
            bio,
            caption=f"Ошибка бота: {context}"
        )
    except Exception:
        try:
            print("send_error_report failed")
        except Exception:
            pass

def _thread_excepthook(args):
    try:
        text = "".join(traceback.format_exception(args.exc_type, args.exc_value, args.exc_traceback))
        bio = io.BytesIO(text.encode("utf-8", errors="replace"))
        bio.name = f"thread_error_{int(time.time())}.txt"
        bot.send_document(OWNER_ID, bio, caption=f"Поток: {getattr(args.thread, 'name', 'thread')}")
    except Exception:
        pass

threading.excepthook = _thread_excepthook

def _sys_excepthook(exc_type, exc_value, exc_tb):
    try:
        text = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
        bio = io.BytesIO(text.encode("utf-8", errors="replace"))
        bio.name = f"fatal_error_{int(time.time())}.txt"
        bot.send_document(OWNER_ID, bio, caption="Необработанная ошибка")
    except Exception:
        pass
    sys.__excepthook__(exc_type, exc_value, exc_tb)

sys.excepthook = _sys_excepthook

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

# Защита бота от падения
def init_bot_identity():
    try:
        me = bot.get_me()
        return me, (getattr(me, "username", "") or "").strip()
    except Exception as e:
        try:
            print("get_me failed:", repr(e))
        except Exception:
            pass

        try:
            send_error_report("startup:get_me", e)
        except Exception:
            pass

        return None, ""

ME, BOT_USERNAME = init_bot_identity()

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))

#  DATA DIR + DB PATH 
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

OLD_DB_PATH = os.path.join(SCRIPT_DIR, "contest_bot.db")      # старая база (если была рядом со скриптом)
DB_PATH = os.path.join(DATA_DIR, "contest_bot.db")            # новая база в папке data/

# Авто-перенос базы при первом запуске после патча
if os.path.exists(OLD_DB_PATH) and (not os.path.exists(DB_PATH)):
    try:
        _c = sqlite3.connect(OLD_DB_PATH, check_same_thread=False)
        _c.execute("PRAGMA journal_mode=WAL;")
        try:
            _c.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        except Exception:
            pass
        _c.close()
    except Exception:
        pass

    for ext in ("", "-wal", "-shm"):
        src = OLD_DB_PATH + ext
        dst = DB_PATH + ext
        if os.path.exists(src) and (not os.path.exists(dst)):
            try:
                shutil.move(src, dst)
            except Exception:
                try:
                    shutil.copy2(src, dst)
                except Exception:
                    pass

#проверка 1
print("DB_PATH =", DB_PATH)
print("DB exists =", os.path.exists(DB_PATH), "size =", os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else None)
print("WAL exists =", os.path.exists(DB_PATH + "-wal"), "size =", os.path.getsize(DB_PATH + "-wal") if os.path.exists(DB_PATH + "-wal") else None)
print("SHM exists =", os.path.exists(DB_PATH + "-shm"), "size =", os.path.getsize(DB_PATH + "-shm") if os.path.exists(DB_PATH + "-shm") else None)

CONTRACT_PATH = os.path.join(SCRIPT_DIR, "contract.txt")
JOBS_PATH = os.path.join(SCRIPT_DIR, "jobs.txt")
PREFIX_LEN = 12

# DB
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
conn.execute("PRAGMA journal_mode=WAL;")
conn.execute("PRAGMA synchronous=NORMAL;")
conn.execute("PRAGMA busy_timeout=8000;")
conn.execute("PRAGMA wal_autocheckpoint=2000;")  # ~8MB при page_size=4096
try: #проверка 2
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
except Exception as e:
    print("wal_checkpoint failed:", e)

cur = conn.cursor()
#проверка 3
print("DB absolute:", cur.execute("PRAGMA database_list;").fetchall())
print("journal_mode:", cur.execute("PRAGMA journal_mode;").fetchone())
print("wal_autocheckpoint:", cur.execute("PRAGMA wal_autocheckpoint;").fetchone())

DB_LOCK = threading.RLock()
with DB_LOCK:
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
    except Exception:
        pass

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
            rc = c.rowcount
            lid = c.lastrowid
            if commit:
                conn.commit()
            return rc, lid
        finally:
            try:
                c.close()
            except:
                pass

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
CREATE TABLE IF NOT EXISTS user_settings (
  user_id INTEGER PRIMARY KEY,
  pm_notify INTEGER NOT NULL DEFAULT 1,
  auto_delete_pm INTEGER NOT NULL DEFAULT 1,
  settings_msg_id INTEGER NOT NULL DEFAULT 0
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS pm_bot_messages (
  chat_id INTEGER NOT NULL,
  message_id INTEGER NOT NULL,
  created_ts INTEGER NOT NULL,
  delete_after_ts INTEGER NOT NULL,
  deleted INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (chat_id, message_id)
)
""")

cur.execute("""
CREATE INDEX IF NOT EXISTS idx_pm_bot_messages_gc
ON pm_bot_messages(deleted, delete_after_ts)
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
CREATE TABLE IF NOT EXISTS enslave_risk (
  user_id INTEGER PRIMARY KEY,
  chance_pct INTEGER NOT NULL DEFAULT 10
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
  share_bp INTEGER DEFAULT 6000,  -- доля в базисных пунктах (1000=10%)
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
try:
    cur.execute("""
        UPDATE slavery
        SET share_bp=6000
        WHERE share_bp=2000
          AND slave_id IN (
              SELECT slave_id
              FROM slavery
              GROUP BY slave_id
              HAVING COUNT(*)=1
          )
    """)
    conn.commit()
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

try:  # ensure slave_meta has life_uses column
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
CREATE TABLE IF NOT EXISTS buyrab_offers (
  offer_id TEXT PRIMARY KEY,
  tx_no INTEGER,
  slave_id INTEGER,
  buyer_id INTEGER,
  total_cents INTEGER,
  hold_cents INTEGER DEFAULT 0,
  created_ts INTEGER,
  state INTEGER DEFAULT 0        -- 0 draft, 1 pending, 2 done, -1 cancelled
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS buyrab_offer_resp (
  offer_id TEXT,
  owner_id INTEGER,
  pay_cents INTEGER DEFAULT 0,
  status INTEGER DEFAULT 0,      -- 0 pending, 1 accepted, -1 declined
  PRIMARY KEY (offer_id, owner_id)
)
""")

# WORK
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

# SHOP
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
CREATE TABLE IF NOT EXISTS shop_used (
    user_id INTEGER NOT NULL,
    game_id TEXT NOT NULL,
    item_key TEXT NOT NULL,
    used_ts INTEGER NOT NULL,
    PRIMARY KEY (user_id, game_id, item_key)
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS shop_cooldowns (
    user_id INTEGER PRIMARY KEY,
    next_protect_ts INTEGER NOT NULL DEFAULT 0
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS shop_item_cooldowns (
    user_id INTEGER NOT NULL,
    item_key TEXT NOT NULL,
    next_ts INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (user_id, item_key)
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS shop_catalog (
    user_id INTEGER PRIMARY KEY,
    cycle_start_ts INTEGER NOT NULL,
    keys_csv TEXT NOT NULL
)
""")

# GAMES
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
CREATE TABLE IF NOT EXISTS turn_orders (
  game_id TEXT PRIMARY KEY,
  order_csv TEXT NOT NULL,
  round INTEGER NOT NULL DEFAULT 0,
  updated_ts INTEGER NOT NULL DEFAULT 0
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
CREATE TABLE IF NOT EXISTS zero_bets (
  game_id TEXT,
  user_id INTEGER,
  slot INTEGER,        -- 0..4 по порядку выбора
  code TEXT,           -- N1..N36 | Z | E|O|R|B
  PRIMARY KEY (game_id, user_id, slot)
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS zero_lock (
  game_id TEXT,
  user_id INTEGER,
  locked INTEGER DEFAULT 0,
  PRIMARY KEY (game_id, user_id)
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS zero_state (
  game_id TEXT PRIMARY KEY,
  stage TEXT DEFAULT 'betting',     -- betting|reveal|done
  revealed INTEGER DEFAULT 0,       -- 0..5
  gen_csv TEXT DEFAULT '',
  gen_ts INTEGER DEFAULT 0
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS zero_outcomes (
  game_id TEXT,
  user_id INTEGER,
  combo TEXT DEFAULT '',
  mult REAL DEFAULT 1.0,
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

# ПРОЧЕЕ 
cur.execute("""
CREATE TABLE IF NOT EXISTS credit_loans (
  user_id INTEGER PRIMARY KEY,
  contract_code INTEGER NOT NULL,
  principal_cents INTEGER NOT NULL,
  term_days INTEGER NOT NULL,
  rate_pct INTEGER NOT NULL,
  created_ts INTEGER NOT NULL,
  status TEXT DEFAULT 'active'
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS transfers (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  from_id INTEGER NOT NULL,
  to_id INTEGER NOT NULL,
  amount_cents INTEGER NOT NULL,
  fee_cents INTEGER DEFAULT 0,
  ts INTEGER NOT NULL,
  comment TEXT,
  chat_id INTEGER DEFAULT 0,
  msg_id INTEGER DEFAULT 0
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS known_group_chats (
  chat_id INTEGER PRIMARY KEY,
  title TEXT DEFAULT '',
  added_ts INTEGER NOT NULL DEFAULT 0,
  last_seen_ts INTEGER NOT NULL DEFAULT 0
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS transfer_blocks (
  user_id INTEGER PRIMARY KEY,
  until_ts INTEGER NOT NULL,
  reason TEXT,
  created_ts INTEGER NOT NULL,
  first_notice_ts INTEGER NOT NULL DEFAULT 0
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS transfer_block_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  action TEXT NOT NULL,             -- 'block'|'manual_unblock'
  user_id INTEGER NOT NULL,
  until_ts INTEGER NOT NULL,
  reason TEXT,
  created_ts INTEGER NOT NULL,
  from_id INTEGER DEFAULT 0,
  to_id INTEGER DEFAULT 0,
  amount_cents INTEGER DEFAULT 0,
  c0 INTEGER DEFAULT 0,
  c100k INTEGER DEFAULT 0,
  c1m INTEGER DEFAULT 0,
  chat_id INTEGER DEFAULT 0,
  msg_id INTEGER DEFAULT 0
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS user_custom_status (
  user_id INTEGER NOT NULL,
  status TEXT NOT NULL,
  added_ts INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (user_id, status)
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS bans (
  user_id INTEGER PRIMARY KEY,
  banned INTEGER NOT NULL DEFAULT 1,
  ts INTEGER NOT NULL,
  until_ts INTEGER NOT NULL DEFAULT 0,
  by_id INTEGER DEFAULT 0,
  reason TEXT
)
""")
try:
    cur.execute("ALTER TABLE bans ADD COLUMN until_ts INTEGER NOT NULL DEFAULT 0")
except Exception:
    pass

cur.execute("""
CREATE TABLE IF NOT EXISTS report_state (
  user_id INTEGER PRIMARY KEY,
  category TEXT NOT NULL,
  stage TEXT NOT NULL,
  created_ts INTEGER NOT NULL
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS pm_trade_state (
  user_id INTEGER PRIMARY KEY,
  action TEXT NOT NULL,
  payload TEXT NOT NULL DEFAULT '',
  stage TEXT NOT NULL DEFAULT 'ready',
  created_ts INTEGER NOT NULL DEFAULT 0
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS bot_admins (
  user_id INTEGER PRIMARY KEY,
  added_ts INTEGER NOT NULL DEFAULT 0,
  added_by INTEGER NOT NULL DEFAULT 0
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS bot_state (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL DEFAULT '',
  updated_ts INTEGER NOT NULL DEFAULT 0
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

def ensure_credit_columns():
    cols = [
        ("next_due_ts", "INTEGER DEFAULT 0"),
        ("end_ts", "INTEGER DEFAULT 0"),
        ("payment_cents", "INTEGER DEFAULT 0"),
        ("remaining_cents", "INTEGER DEFAULT 0"),
        ("postponed_cents", "INTEGER DEFAULT 0"),
        ("last_notice_ts", "INTEGER DEFAULT 0"),
        ("notice_msg_id", "INTEGER DEFAULT 0"),
    ]
    for name, typ in cols:
        try:
            conn.execute(f"ALTER TABLE credit_loans ADD COLUMN {name} {typ}")
        except sqlite3.OperationalError:
            pass
    conn.commit()

def ensure_transfer_columns():
    for sql in [
        "ALTER TABLE transfers ADD COLUMN fee_cents INTEGER DEFAULT 0",
        "ALTER TABLE transfer_blocks ADD COLUMN first_notice_ts INTEGER NOT NULL DEFAULT 0",
    ]:
        try:
            conn.execute(sql)
        except sqlite3.OperationalError:
            pass

    conn.execute("""
    CREATE TABLE IF NOT EXISTS transfer_blocks (
      user_id INTEGER PRIMARY KEY,
      until_ts INTEGER NOT NULL,
      reason TEXT,
      created_ts INTEGER NOT NULL,
      first_notice_ts INTEGER NOT NULL DEFAULT 0
    )
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS transfer_block_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      action TEXT NOT NULL,             -- 'block'|'manual_block'|'manual_unblock'
      user_id INTEGER NOT NULL,
      until_ts INTEGER NOT NULL,
      reason TEXT,
      created_ts INTEGER NOT NULL,
      from_id INTEGER DEFAULT 0,
      to_id INTEGER DEFAULT 0,
      amount_cents INTEGER DEFAULT 0,
      c0 INTEGER DEFAULT 0,
      c100k INTEGER DEFAULT 0,
      c1m INTEGER DEFAULT 0,
      chat_id INTEGER DEFAULT 0,
      msg_id INTEGER DEFAULT 0
    )
    """)
    conn.commit()

ensure_transfer_columns()

def ensure_shop_cooldowns():
    conn.execute("""
    CREATE TABLE IF NOT EXISTS shop_cooldowns (
      user_id INTEGER PRIMARY KEY,
      next_protect_ts INTEGER NOT NULL DEFAULT 0
    )
    """)
    conn.commit()

ensure_shop_cooldowns()

# Runtime DB: безопасный "cur" 
# До этого места "cur" был реальным sqlite3.Cursor и использовался для миграций/DDL.
# Дальше в рантайме он НЕ должен быть реальным курсором, иначе при потоках ловим:
# sqlite3.ProgrammingError: Recursive use of cursors not allowed.
try:
    cur.close()
except Exception:
    pass

class CurProxy:
    """
    Совместимый заменитель sqlite cursor:
    - cur.execute(sql, params)
    - cur.fetchone()
    - cur.fetchall()
    - cur.rowcount
    При этом:
    - SELECT/PRAGMA/WITH/EXPLAIN -> буферизуем результаты через db_all
    - INSERT/UPDATE/DELETE/...   -> выполняем через db_exec(commit=True) и выставляем rowcount
    """
    def __init__(self):
        self._local = threading.local()
        self.rowcount = 0

    def _set_rows(self, rows):
        self._local.rows = rows or []
        self._local.idx = 0

    def execute(self, sql, params=()):
        s = (sql or "").lstrip().upper()

        is_read = (
            s.startswith("SELECT")
            or s.startswith("PRAGMA")
            or s.startswith("WITH")
            or s.startswith("EXPLAIN")
        )

        if is_read:
            rows = db_all(sql, params)
            self.rowcount = len(rows)
            self._set_rows(rows)
            return self

        rc, _ = db_exec(sql, params, commit=True)
        self.rowcount = int(rc or 0)
        self._set_rows([])
        return self

    def fetchone(self):
        rows = getattr(self._local, "rows", [])
        idx = getattr(self._local, "idx", 0)
        if idx >= len(rows):
            return None
        self._local.idx = idx + 1
        return rows[idx]

    def fetchall(self):
        rows = getattr(self._local, "rows", [])
        idx = getattr(self._local, "idx", 0)
        if idx <= 0:
            return rows
        return rows[idx:]

# безопасный cur прокси для всего рантайма
cur = CurProxy()

# Helpers
def now_ts() -> int:
    return int(time.time())

PM_AUTO_DELETE_SEC = 48 * 3600 # таймер авто-удаления

def ensure_user_settings(uid: int):
    db_exec(
        "INSERT OR IGNORE INTO user_settings (user_id, pm_notify, auto_delete_pm, settings_msg_id) VALUES (?,?,?,?)",
        (int(uid), 1, 1, 0),
        commit=True
    )

def _user_settings_row(uid: int) -> Tuple[int, int, int]:
    row = db_one(
        "SELECT pm_notify, auto_delete_pm, settings_msg_id FROM user_settings WHERE user_id=?",
        (int(uid),)
    )
    if not row:
        return (1, 1, 0)
    return (int(row[0] or 0), int(row[1] or 0), int(row[2] or 0))

def user_pm_notifications_enabled(uid: int) -> bool:
    row = db_one("SELECT pm_notify FROM user_settings WHERE user_id=?", (int(uid),))
    if not row:
        return True
    return bool(int(row[0] or 0))

def user_auto_delete_pm_enabled(uid: int) -> bool:
    row = db_one("SELECT auto_delete_pm FROM user_settings WHERE user_id=?", (int(uid),))
    if not row:
        return True
    return bool(int(row[0] or 0))

def set_user_pm_notify(uid: int, enabled: bool):
    ensure_user_settings(uid)
    db_exec(
        "UPDATE user_settings SET pm_notify=? WHERE user_id=?",
        (1 if enabled else 0, int(uid)),
        commit=True
    )

def set_user_auto_delete_pm(uid: int, enabled: bool):
    ensure_user_settings(uid)
    db_exec(
        "UPDATE user_settings SET auto_delete_pm=? WHERE user_id=?",
        (1 if enabled else 0, int(uid)),
        commit=True
    )

def get_settings_msg_id(uid: int) -> int:
    row = db_one("SELECT settings_msg_id FROM user_settings WHERE user_id=?", (int(uid),))
    return int((row[0] if row else 0) or 0)

def set_settings_msg_id(uid: int, msg_id: int):
    ensure_user_settings(uid)
    db_exec(
        "UPDATE user_settings SET settings_msg_id=? WHERE user_id=?",
        (int(msg_id or 0), int(uid)),
        commit=True
    )

def _settings_onoff(v: bool) -> str:
    return "✅" if v else "❌"

def _settings_menu_text(uid: int) -> str:
    pm_notify, auto_del, _msg_id = _user_settings_row(uid)
    return (
        "⚙️ Настройки бота\n\n"
        f"Уведомления в ЛС: {_settings_onoff(bool(pm_notify))}\n"
        f"Автоудаление сообщений: {_settings_onoff(bool(auto_del))}"
    )

def _settings_menu_kb(uid: int) -> InlineKeyboardMarkup:
    pm_notify, auto_del, _msg_id = _user_settings_row(uid)

    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton(
        f"Уведомления в ЛС: {'✅' if pm_notify else '❌'}",
        callback_data=cb_pack("settings:toggle:pmnotify", uid)
    ))
    kb.add(InlineKeyboardButton(
        f"Автоудаление 48ч: {'✅' if auto_del else '❌'}",
        callback_data=cb_pack("settings:toggle:autodel", uid)
    ))
    return kb

def show_settings_menu(chat_id: int, uid: int, prefer_edit: bool = True):
    uid = int(uid)
    chat_id = int(chat_id)
    ensure_user_settings(uid)

    if chat_id != uid:
        text = "Настройки доступны в личных сообщениях бота."
        if BOT_USERNAME:
            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton(
                "Открыть настройки",
                url=f"https://t.me/{BOT_USERNAME}?start=settings"
            ))
            bot.send_message(chat_id, text, reply_markup=kb)
        else:
            bot.send_message(chat_id, text)
        return

    msg_id = get_settings_msg_id(uid)
    text = _settings_menu_text(uid)
    kb = _settings_menu_kb(uid)

    if prefer_edit and msg_id:
        try:
            bot.edit_message_text(text, chat_id=chat_id, message_id=msg_id, reply_markup=kb)
            return
        except Exception:
            pass

    sent = bot.send_message(chat_id, text, reply_markup=kb)
    set_settings_msg_id(uid, int(sent.message_id))

def _track_private_bot_message(sent_msg):
    try:
        if not sent_msg:
            return
        chat = getattr(sent_msg, "chat", None)
        if not chat:
            return
        if str(getattr(chat, "type", "") or "") != "private":
            return

        chat_id = int(getattr(chat, "id", 0) or 0)
        msg_id = int(getattr(sent_msg, "message_id", 0) or 0)
        if chat_id <= 0 or msg_id <= 0:
            return

        db_exec(
            "INSERT OR REPLACE INTO pm_bot_messages (chat_id, message_id, created_ts, delete_after_ts, deleted) VALUES (?,?,?,?,0)",
            (chat_id, msg_id, now_ts(), now_ts() + PM_AUTO_DELETE_SEC),
            commit=True
        )
    except Exception:
        pass

def _delete_tracked_pm_message(chat_id: int, msg_id: int) -> bool:
    try:
        bot.delete_message(int(chat_id), int(msg_id))
        db_exec(
            "UPDATE pm_bot_messages SET deleted=1 WHERE chat_id=? AND message_id=?",
            (int(chat_id), int(msg_id)),
            commit=True
        )
        return True
    except Exception:
        return False

def _pm_autodelete_daemon():
    while True:
        try:
            rows = db_all(
                "SELECT chat_id, message_id FROM pm_bot_messages "
                "WHERE deleted=0 AND delete_after_ts<=? "
                "ORDER BY delete_after_ts ASC LIMIT 200",
                (now_ts(),)
            ) or []

            for chat_id, msg_id in rows:
                chat_id = int(chat_id)
                msg_id = int(msg_id)

                if not user_auto_delete_pm_enabled(chat_id):
                    continue

                _delete_tracked_pm_message(chat_id, msg_id)
                time.sleep(0.03)
        except Exception:
            send_error_report("_pm_autodelete_daemon")

        time.sleep(30)

# логируем новые исходящие ЛС-сообщения бота
_ORIG_SEND_MESSAGE = bot.send_message
_ORIG_SEND_PHOTO = bot.send_photo
_ORIG_SEND_VIDEO = bot.send_video

def _tracked_send_message(*args, **kwargs):
    sent = _ORIG_SEND_MESSAGE(*args, **kwargs)
    _track_private_bot_message(sent)
    return sent

def _tracked_send_photo(*args, **kwargs):
    sent = _ORIG_SEND_PHOTO(*args, **kwargs)
    _track_private_bot_message(sent)
    return sent

def _tracked_send_video(*args, **kwargs):
    sent = _ORIG_SEND_VIDEO(*args, **kwargs)
    _track_private_bot_message(sent)
    return sent

bot.send_message = _tracked_send_message
bot.send_photo = _tracked_send_photo
bot.send_video = _tracked_send_video

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

def cents_to_money_str(cents: Optional[int]) -> str:
    cents = int(cents or 0)
    sign = "-" if cents < 0 else ""
    cents = abs(cents)
    return f"{sign}{cents//100}.{cents%100:02d}"

def safe_format(template: str, **kwargs) -> str:
    class DD(dict):
        def __missing__(self, key):
            return "{" + key + "}"
    return template.format_map(DD(**kwargs))

def remember_group_chat(chat_id: int, title: str = "") -> None:
    chat_id = int(chat_id or 0)
    if chat_id >= 0:
        return

    db_exec(
        "INSERT INTO known_group_chats (chat_id, title, added_ts, last_seen_ts) VALUES (?,?,?,?) "
        "ON CONFLICT(chat_id) DO UPDATE SET "
        "title=CASE WHEN excluded.title<>'' THEN excluded.title ELSE known_group_chats.title END, "
        "last_seen_ts=excluded.last_seen_ts",
        (chat_id, str(title or "")[:200], now_ts(), now_ts()),
        commit=True
    )

def forget_group_chat(chat_id: int) -> None:
    db_exec("DELETE FROM known_group_chats WHERE chat_id=?", (int(chat_id),), commit=True)

# BOT ADMINS + MAINTENANCE MODE

def is_owner(uid: int) -> bool:
    return int(uid) == int(OWNER_ID)

def is_bot_admin(uid: int) -> bool:
    uid = int(uid or 0)
    if uid == int(OWNER_ID):
        return True
    try:
        r = db_one("SELECT 1 FROM bot_admins WHERE user_id=? LIMIT 1", (uid,))
        return bool(r)
    except Exception:
        return False

def set_bot_admin(uid: int, enabled: bool, by_id: int = 0) -> None:
    uid = int(uid or 0)
    by_id = int(by_id or 0)
    if uid <= 0:
        return

    if enabled:
        db_exec(
            "INSERT OR IGNORE INTO bot_admins (user_id, added_ts, added_by) VALUES (?,?,?)",
            (uid, now_ts(), by_id),
            commit=True
        )
        # видимый статус
        try:
            add_custom_status(uid, "Бот-админ")
        except Exception:
            pass
    else:
        db_exec("DELETE FROM bot_admins WHERE user_id=?", (uid,), commit=True)
        try:
            db_exec(
                "DELETE FROM user_custom_status WHERE user_id=? AND status=?",
                (uid, "Бот-админ"),
                commit=True
            )
        except Exception:
            pass

def bot_state_get(key: str, default: str = "") -> str:
    key = str(key or "").strip()
    if not key:
        return default
    try:
        r = db_one("SELECT value FROM bot_state WHERE key=?", (key,))
        if not r:
            return default
        return str(r[0] or "")
    except Exception:
        return default

def bot_state_set(key: str, value: str) -> None:
    key = str(key or "").strip()
    if not key:
        return
    db_exec(
        "INSERT INTO bot_state (key, value, updated_ts) VALUES (?,?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_ts=excluded.updated_ts",
        (key, str(value or ""), now_ts()),
        commit=True
    )

_SLEEP_CACHE: dict = {"ts": 0, "sleeping": False, "mode": "", "reason": "", "last_err": ""}

def get_last_error_code() -> str:
    try:
        ctx = (bot_state_get("last_error_ctx", "") or "").strip()
        if not ctx:
            return ""
        seq = (bot_state_get("error_seq", "") or "").strip()
        ts = (bot_state_get("last_error_ts", "") or "").strip()

        pref = f"E{seq}: " if seq.isdigit() else ""
        ttxt = ""
        if ts.isdigit():
            try:
                ttxt = _fmt_ts(int(ts))
            except Exception:
                ttxt = ""

        if ttxt:
            return f"{pref}{ctx} ({ttxt})"
        return f"{pref}{ctx}"
    except Exception:
        return ""

def remember_last_error(context: str) -> None:
    context = (context or "").strip()
    if not context:
        return
    try:
        seq = int(bot_state_get("error_seq", "0") or 0) + 1
    except Exception:
        seq = 1
    try:
        bot_state_set("error_seq", str(seq))
        bot_state_set("last_error_ctx", context[:200])
        bot_state_set("last_error_ts", str(now_ts()))
        _SLEEP_CACHE["ts"] = 0
    except Exception:
        pass

def get_bot_sleep_state() -> Tuple[bool, str, str, str]:
    now = now_ts()
    try:
        if (now - int(_SLEEP_CACHE.get("ts", 0) or 0)) < 2:
            return (
                bool(_SLEEP_CACHE.get("sleeping")),
                str(_SLEEP_CACHE.get("mode", "") or ""),
                str(_SLEEP_CACHE.get("reason", "") or ""),
                str(_SLEEP_CACHE.get("last_err", "") or ""),
            )
    except Exception:
        pass

    sleeping = (bot_state_get("sleeping", "0") == "1")
    mode = bot_state_get("sleep_mode", "")
    reason = bot_state_get("sleep_reason", "")
    last_err = get_last_error_code()

    _SLEEP_CACHE.update({
        "ts": now,
        "sleeping": sleeping,
        "mode": mode,
        "reason": reason,
        "last_err": last_err
    })
    return sleeping, mode, reason, last_err

def set_bot_sleep(mode: str, reason: str = "") -> None:
    bot_state_set("sleeping", "1")
    bot_state_set("sleep_mode", (mode or "").strip().lower())
    bot_state_set("sleep_reason", (reason or "").strip())
    _SLEEP_CACHE["ts"] = 0

def clear_bot_sleep() -> None:
    bot_state_set("sleeping", "0")
    bot_state_set("sleep_mode", "")
    bot_state_set("sleep_reason", "")
    _SLEEP_CACHE["ts"] = 0

def build_sleep_notice_text() -> str:
    sleeping, mode, reason, last_err = get_bot_sleep_state()
    if not sleeping:
        return ""

    mode = (mode or "").strip().lower()
    reason = (reason or "").strip()

    if mode == "update":
        return (
            "🛠️🤖 Внимание. Проводятся сложные технические работы обновления версии.\n"
            "Дабы не омрачить ваш опыт пользования ботом, на время технических работ все функции бота будут отключены до непосредственного выхода обновления.\n"
            "Благодарю за понимание. Администратор."
        )

    err_txt = (last_err or "").strip() or "-"
    base = (
        "<b>⚠️Внимание!</b>\n"
        "В связи с технической ошибкой и/или неисправностью, бот переходит в аварийный режим.\n"
        "<u>Все функции бота (игры, статистика, транзакции) будут ОТКЛЮЧЕНЫ до их частичного или полного урегулирования.</u>\n"
        "Благодарю за понимание. Администратор."
    )
    return base + "\n" + f"<code>{html_escape(err_txt)}</code>"

def bot_status_human() -> str:
    sleeping, mode, _reason, last_err = get_bot_sleep_state()
    if not sleeping:
        return "ON ✅"
    mode = (mode or "").strip().lower()
    if mode == "update":
        return "OFF 🛠️ (update)"
    # error
    if last_err:
        return f"OFF ⚠️ (error) — {last_err}"
    return "OFF ⚠️ (error)"

_SLEEP_NOTICE_LAST: Dict[Tuple[int, int], int] = {}

def _sleep_notice_cooldown_ok(chat_id: int, user_id: int, sec: int = 30) -> bool:
    chat_id = int(chat_id or 0)
    user_id = int(user_id or 0)
    key = (chat_id, user_id)
    now = now_ts()
    prev = int(_SLEEP_NOTICE_LAST.get(key, 0) or 0)
    if (now - prev) < int(sec):
        return False
    _SLEEP_NOTICE_LAST[key] = now
    return True

def _allow_message_during_sleep(message) -> bool:
    try:
        uid = int(getattr(getattr(message, "from_user", None), "id", 0) or 0)
    except Exception:
        uid = 0

    if is_bot_admin(uid):
        return True

    try:
        txt = str(getattr(message, "text", "") or "")
    except Exception:
        txt = ""

    txt = txt.strip()
    if txt.startswith("/report"):
        return True

    return False

def _allow_callback_during_sleep(call: CallbackQuery) -> bool:
    try:
        uid = int(getattr(getattr(call, "from_user", None), "id", 0) or 0)
    except Exception:
        uid = 0

    if is_bot_admin(uid):
        return True

    try:
        data = str(getattr(call, "data", "") or "")
    except Exception:
        data = ""

    if data.startswith("report:"):
        return True

    return False

def _parse_retry_after(exc: Exception) -> float:
    s = str(exc)
    m = re.search(r"retry after (\d+(?:\.\d+)?)", s, re.IGNORECASE)
    if m:
        try:
            return float(m.group(1))
        except Exception:
            return 0.0
    return 0.0

def send_message_with_retry(chat_id: int, text: str, parse_mode: str = "HTML") -> bool:
    try:
        bot.send_message(int(chat_id), text, parse_mode=parse_mode)
        return True
    except Exception as e:
        ra = _parse_retry_after(e)
        if ra and ra > 0:
            time.sleep(ra + 0.2)
            try:
                bot.send_message(int(chat_id), text, parse_mode=parse_mode)
                return True
            except Exception:
                return False
        return False

def broadcast_notice(body: str, respect_pm_settings: bool = True) -> Dict[str, int]:
    rows = db_all("SELECT user_id FROM users WHERE COALESCE(contract_ts,0) > 0 ORDER BY user_id", ()) or []
    uids = [int(r[0]) for r in rows if r and int(r[0] or 0) > 0]
    if not uids:
        return {"groups_sent": 0, "groups_failed": 0, "pm_sent": 0, "pm_failed": 0, "pm_skipped": 0, "covered": 0, "groups_checked": 0}

    group_sent = 0
    group_failed = 0
    group_checked = 0
    covered_uids: set[int] = set()

    bot_me_id = 0
    try:
        if ME:
            bot_me_id = int(getattr(ME, "id", 0) or 0)
    except Exception:
        bot_me_id = 0
    if bot_me_id <= 0:
        try:
            me = bot.get_me()
            bot_me_id = int(getattr(me, "id", 0) or 0)
        except Exception:
            bot_me_id = 0

    group_ids = get_known_broadcast_group_ids()
    for chat_id in group_ids:
        group_checked += 1
        try:
            if not bot_is_present_in_group(int(chat_id), bot_me_id=bot_me_id):
                continue
        except Exception:
            pass

        sent_to_group = send_message_with_retry(int(chat_id), body, parse_mode="HTML")
        if sent_to_group:
            group_sent += 1
            try:
                remember_group_chat(int(chat_id))
            except Exception:
                pass
        else:
            group_failed += 1
            time.sleep(0.05)
            continue

        for uid in uids:
            if uid in covered_uids:
                continue
            try:
                mem = bot.get_chat_member(int(chat_id), int(uid))
                st = str(getattr(mem, "status", "") or "")
                if st and st not in ("left", "kicked"):
                    covered_uids.add(int(uid))
            except Exception:
                pass
            time.sleep(0.02)

        time.sleep(0.05)

    pm_sent = 0
    pm_failed = 0
    pm_skipped = 0

    for uid in uids:
        if uid in covered_uids:
            continue
        if respect_pm_settings and (not user_pm_notifications_enabled(int(uid))):
            pm_skipped += 1
            continue
        if send_message_with_retry(int(uid), body, parse_mode="HTML"):
            pm_sent += 1
        else:
            pm_failed += 1
        time.sleep(0.03)

    return {
        "groups_sent": group_sent,
        "groups_failed": group_failed,
        "groups_checked": group_checked,
        "covered": len(covered_uids),
        "pm_sent": pm_sent,
        "pm_failed": pm_failed,
        "pm_skipped": pm_skipped,
    }

def _touch_group_from_message(message) -> None:
    try:
        chat = getattr(message, "chat", None)
        if chat and getattr(chat, "type", "") in ("group", "supergroup"):
            remember_group_chat(int(chat.id), getattr(chat, "title", "") or "")
    except Exception:
        pass

def _touch_group_from_callback(call: CallbackQuery) -> None:
    try:
        msg = getattr(call, "message", None)
        chat = getattr(msg, "chat", None) if msg else None
        if chat and getattr(chat, "type", "") in ("group", "supergroup"):
            remember_group_chat(int(chat.id), getattr(chat, "title", "") or "")
    except Exception:
        pass

@bot.message_handler(
    func=lambda m: True,
    content_types=[
        "text", "photo", "audio", "document", "animation", "game",
        "video", "voice", "video_note", "location", "contact",
        "sticker", "venue", "dice", "new_chat_members",
        "left_chat_member", "pinned_message"
    ]
)
def _track_any_group_message(message):
    _touch_group_from_message(message)
    return ContinueHandling()

@bot.callback_query_handler(func=lambda c: True)
def _track_any_group_callback(call: CallbackQuery):
    _touch_group_from_callback(call)
    return ContinueHandling()

def get_known_broadcast_group_ids() -> List[int]:
    """
    Источники:
    1) known_group_chats — чаты, где бот уже видел команды/сообщения
    2) games.origin_chat_id
    3) transfers.chat_id
    """
    out: List[int] = []
    seen = set()

    queries = [
        "SELECT chat_id FROM known_group_chats WHERE chat_id < 0 ORDER BY last_seen_ts DESC, added_ts DESC",
        "SELECT DISTINCT COALESCE(origin_chat_id,0) FROM games WHERE COALESCE(origin_chat_id,0) < 0",
        "SELECT DISTINCT COALESCE(chat_id,0) FROM transfers WHERE COALESCE(chat_id,0) < 0",
    ]

    for sql in queries:
        try:
            rows = db_all(sql, ())
        except Exception:
            rows = []

        for r in rows or []:
            try:
                chat_id = int((r[0] if r else 0) or 0)
            except Exception:
                chat_id = 0

            if chat_id < 0 and chat_id not in seen:
                seen.add(chat_id)
                out.append(chat_id)

    return out

def bot_is_present_in_group(chat_id: int, bot_me_id: int = 0) -> bool:
    chat_id = int(chat_id or 0)
    if chat_id >= 0:
        return False

    try:
        if int(bot_me_id or 0) <= 0:
            me = bot.get_me()
            bot_me_id = int(getattr(me, "id", 0) or 0)

        me_member = bot.get_chat_member(chat_id, int(bot_me_id))
        me_status = str(getattr(me_member, "status", "") or "")

        if me_status in ("left", "kicked"):
            forget_group_chat(chat_id)
            return False

        remember_group_chat(chat_id)
        return True
    except Exception:
        return True

# Moderation helpers
def parse_duration_to_seconds(token: str) -> Optional[int]:
    """
    Примеры: 30m, 24h, 7d, 2w
    Возвращает:
      - int секунд
      - 0 для перманентного бана (perm/0/forever)
      - None если токен не похож на длительность
    """
    t = (token or "").strip().lower()
    if not t:
        return None
    if t in ("perm", "permanent", "forever", "inf", "infty", "infinite", "0"):
        return 0
    m = re.fullmatch(r"(\d+)\s*([smhdw])", t)
    if not m:
        return None
    n = int(m.group(1))
    unit = m.group(2)
    if n <= 0:
        return 0
    mul = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 7 * 86400}[unit]
    return int(n * mul)

def _fmt_ts(ts: int) -> str:
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(ts)))
    except Exception:
        return str(ts)

def get_ban_info(uid: int) -> Tuple[bool, int, str]:
    """
    Возвращает (banned, until_ts, reason).
    until_ts=0 => перманентный бан.
    Если бан истёк — автоматически снимает.
    """
    r = db_one(
        "SELECT COALESCE(banned,0), COALESCE(until_ts,0), COALESCE(reason,'') FROM bans WHERE user_id=? LIMIT 1",
        (int(uid),)
    )
    if not r:
        return False, 0, ""

    banned = int(r[0] or 0)
    until_ts = int(r[1] or 0)
    reason = str(r[2] or "")

    if banned != 1:
        return False, 0, reason

    if until_ts > 0 and now_ts() >= until_ts:
        try:
            db_exec("UPDATE bans SET banned=0, until_ts=0 WHERE user_id=?", (int(uid),), commit=True)
        except Exception:
            pass
        return False, 0, reason

    return True, until_ts, reason

def is_banned(uid: int) -> bool:
    return get_ban_info(uid)[0]
def ban_user(uid: int, by_id: int = 0, reason: str = "", *, duration_sec: int = 0) -> int:
    """
    Банит пользователя.
    duration_sec:
      - 0 => перманентно
      - >0 => временно, до now + duration_sec
    Возвращает until_ts (0 если перманентно).
    """
    until_ts = 0
    if int(duration_sec or 0) > 0:
        until_ts = now_ts() + int(duration_sec)

    db_exec(
        "INSERT INTO bans (user_id, banned, ts, until_ts, by_id, reason) VALUES (?,?,?,?,?,?) "
        "ON CONFLICT(user_id) DO UPDATE SET banned=1, ts=excluded.ts, until_ts=excluded.until_ts, by_id=excluded.by_id, reason=excluded.reason",
        (int(uid), 1, now_ts(), int(until_ts), int(by_id or 0), (reason or "")[:500]),
        commit=True
    )
    return int(until_ts)

def unban_user(uid: int, by_id: int = 0, reason: str = "") -> None:
    db_exec(
        "INSERT INTO bans (user_id, banned, ts, until_ts, by_id, reason) VALUES (?,?,?,?,?,?) "
        "ON CONFLICT(user_id) DO UPDATE SET banned=0, ts=excluded.ts, until_ts=0, by_id=excluded.by_id, reason=excluded.reason",
        (int(uid), 0, now_ts(), 0, int(by_id or 0), (reason or "")[:500]),
        commit=True
    )

def report_set_state(uid: int, category: str, stage: str) -> None:
    db_exec(
        "INSERT INTO report_state (user_id, category, stage, created_ts) VALUES (?,?,?,?) "
        "ON CONFLICT(user_id) DO UPDATE SET category=excluded.category, stage=excluded.stage, created_ts=excluded.created_ts",
        (int(uid), str(category), str(stage), now_ts()),
        commit=True
    )

def report_get_state(uid: int) -> Tuple[Optional[str], Optional[str]]:
    r = db_one("SELECT stage, category FROM report_state WHERE user_id=?", (int(uid),))
    if not r:
        return None, None
    return (r[0], r[1])

def report_clear_state(uid: int) -> None:
    db_exec("DELETE FROM report_state WHERE user_id=?", (int(uid),), commit=True)

def trade_state_set(uid: int, action: str, payload: str = "", stage: str = "ready") -> None:
    db_exec(
        "INSERT INTO pm_trade_state (user_id, action, payload, stage, created_ts) VALUES (?,?,?,?,?) "
        "ON CONFLICT(user_id) DO UPDATE SET action=excluded.action, payload=excluded.payload, "
        "stage=excluded.stage, created_ts=excluded.created_ts",
        (int(uid), str(action or ""), str(payload or ""), str(stage or "ready"), now_ts()),
        commit=True
    )

def trade_state_get(uid: int) -> Tuple[Optional[str], Optional[str], str]:
    r = db_one(
        "SELECT action, stage, payload FROM pm_trade_state WHERE user_id=?",
        (int(uid),)
    )
    if not r:
        return None, None, ""
    return (r[0], r[1], str(r[2] or ""))

def trade_state_clear(uid: int) -> None:
    db_exec("DELETE FROM pm_trade_state WHERE user_id=?", (int(uid),), commit=True)

def _trade_pack_payload(target_un: str = "", amount_raw: str = "") -> str:
    return f"{(target_un or '').strip()}|{(amount_raw or '').strip()}"

def _trade_unpack_payload(payload: str) -> Tuple[str, str]:
    s = str(payload or "")
    if "|" in s:
        a, b = s.split("|", 1)
        return a.strip(), b.strip()
    return s.strip(), ""

def _make_private_stub_message(uid: int, username: Optional[str], text: str):
    class _Obj:
        pass

    msg = _Obj()
    chat = _Obj()
    user = _Obj()

    chat.id = int(uid)
    chat.type = "private"

    user.id = int(uid)
    user.username = username

    msg.chat = chat
    msg.from_user = user
    msg.text = text
    msg.message_id = 0
    msg.reply_to_message = None

    return msg

def _begin_trade_pm_flow(uid: int, username: Optional[str], action: str, payload: str) -> None:
    action = str(action or "").strip()
    target_un, amount_raw = _trade_unpack_payload(payload)

    bot.send_message(int(uid), "Продолжаем оформление сделки в личных сообщениях.")

    if action == "buyout":
        trade_state_clear(uid)
        cmd_buyout(_make_private_stub_message(uid, username, "/buyout"))
        return

    if action not in ("buyrab", "rebuy"):
        trade_state_clear(uid)
        return

    if not target_un:
        trade_state_clear(uid)
        bot.send_message(int(uid), "Заявка устарела. Запустите сделку заново.")
        return

    if amount_raw:
        trade_state_clear(uid)
        if action == "buyrab":
            cmd_buyrab(_make_private_stub_message(uid, username, f"/buyrab @{target_un} {amount_raw}"))
        else:
            cmd_buy(_make_private_stub_message(uid, username, f"/rebuy @{target_un} {amount_raw}"))
        return

    trade_state_set(uid, action, _trade_pack_payload(target_un, ""), stage="await_amount")
    bot.send_message(
        int(uid),
        f"Укажите цену для сделки с @{html_escape(target_un)}.\n"
        "Поддерживаемые форматы <code>15000</code> или <code>15000.50</code>",
        parse_mode="HTML"
    )

# Credit helpers
CREDIT_INTERVAL_SEC = 2 * 24 * 3600
CREDIT_NOTICE_GRACE_SEC = 12 * 3600
# Credit limits
CREDIT_MIN_DOLLARS = 1_000
CREDIT_BASE_MAX_DOLLARS = 1_000_000
CREDIT_MAX_STEP_WINS = 10
CREDIT_MAX_STEP_DOLLARS = 1_000_000

def credit_limits_cents(uid: int) -> Tuple[int, int, int]:
    """
    Возвращает (min_cents, max_cents, wins).
    max растёт на +1_000_000$ за каждые 10 побед.
    """
    try:
        _games_total, wins, *_ = get_game_stats(int(uid))
    except Exception:
        wins = 0
    wins = int(wins or 0)

    step = wins // CREDIT_MAX_STEP_WINS
    max_dollars = CREDIT_BASE_MAX_DOLLARS + step * CREDIT_MAX_STEP_DOLLARS

    return CREDIT_MIN_DOLLARS * 100, int(max_dollars) * 100, wins

def credit_amount_ok(uid: int, sum_cents: int) -> Tuple[bool, str]:
    min_c, max_c, wins = credit_limits_cents(uid)
    sum_cents = int(sum_cents or 0)

    if sum_cents < min_c or sum_cents > max_c:
        msg = (
            f"Превышен лимит суммы кредита\n минимум {cents_to_money_str(min_c)}$, максимум {cents_to_money_str(max_c)}$.\n"
            f"Примечание: Лимит растёт каждые {CREDIT_MAX_STEP_WINS} побед в играх (+{CREDIT_MAX_STEP_DOLLARS}$ к максимуму), ваше колличество побед на данный момент: {wins}."
        )
        return False, msg

    return True, ""

def credit_total_payable_cents(principal_cents: int, rate_pct: int) -> int:
    """principal + interest, округление вверх до цента."""
    principal_cents = int(principal_cents)
    rate_pct = int(rate_pct)
    return (principal_cents * (100 + rate_pct) + 99) // 100

def credit_payments_count(term_days: int) -> int:
    term_days = int(term_days)
    return max(1, term_days // 2)

def credit_payment_cents(total_payable_cents: int, payments_count: int) -> int:
    payments_count = max(1, int(payments_count))
    return (int(total_payable_cents) + payments_count - 1) // payments_count

def credit_get_active(uid: int):
    return db_one(
        """SELECT contract_code, principal_cents, term_days, rate_pct, created_ts, status,
                  next_due_ts, end_ts, payment_cents, remaining_cents, postponed_cents,
                  last_notice_ts, notice_msg_id
           FROM credit_loans
           WHERE user_id=? AND status='active'
        """,
        (int(uid),)
    )

def credit_has_active(uid: int) -> bool:
    return credit_get_active(uid) is not None

def credit_due_amount_cents(loan_row) -> int:
    """Текущая сумма к списанию (с учетом переносов), не больше остатка."""
    remaining = int(loan_row[9] or 0)
    payment = int(loan_row[8] or 0)
    postponed = int(loan_row[10] or 0)
    due = payment + postponed
    if due <= 0:
        due = payment
    if remaining <= 0:
        return 0
    return min(remaining, max(0, due))

def credit_format_contract(uid: int, loan_row, *, as_active_view: bool = True) -> str:
    code, principal, term_days, rate, created_ts, _st, next_due_ts, end_ts, payment_c, remaining_c, postponed_c, *_ = loan_row
    me = get_user(uid)
    me_name = me[2] if me and me[2] else "—"
    total = credit_total_payable_cents(int(principal), int(rate))
    pay_cnt = credit_payments_count(int(term_days))
    pay_each = int(payment_c or credit_payment_cents(total, pay_cnt))
    if remaining_c is None or int(remaining_c) <= 0:
        remaining_c = total

    now = now_ts()
    rem_sec = max(0, int(end_ts or 0) - now)
    rem_days = (rem_sec + 86399) // 86400
    next_sec = max(0, int(next_due_ts or 0) - now)

    due = min(int(remaining_c), max(0, pay_each + int(postponed_c or 0)))

    text = (
        f"Договор о предоставлении услуг кредитования № {int(code):07d}\n"
        f"Вы: <u>{html_escape(me_name)}</u>\n"
        f"Сумма кредита: <b>{cents_to_money_str(int(principal))}</b>$\n"
        + (f"Оставшийся срок: <b>{int(rem_days)}</b> дней\n" if as_active_view else f"Срок: <b>{int(term_days)}</b> дней\n") +
        f"Ставка: <b>{int(rate)}</b>%\n"
        f"Сумма выплаты: <b><u>{cents_to_money_str(int(due))}</u></b>$\n"
        f"Выплата по кредиту будет производиться каждые 2 дня\n"
        + (f"Следующее списание с вашего счета через <u>{_format_duration(next_sec)}</u>\n" if as_active_view else "") +
        f"Остаток долга: <b>{cents_to_money_str(int(remaining_c))}</b>$"
    )
    return text

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

def render_contract_for_user(uid: int) -> Optional[str]:
    u = get_user(int(uid))
    if not u or not u[2]:
        return None

    signed_ts = int(u[4] or u[3] or now_ts())

    rendered = safe_format(
        load_contract_text(),
        name=html_escape(u[2] or ""),
        username=html_escape(u[1] or ""),
        date=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(signed_ts)),
        user_id=str(int(uid)),
    )

    lines = rendered.splitlines()
    if not lines:
        return rendered

    head = lines[0]
    tail = "\n".join(lines[1:]).strip()

    if not tail:
        return head

    return f"{head}\n\n<blockquote expandable>{tail}</blockquote>"

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

def resolve_user_id_ref(ref: str) -> Optional[int]:
    """
    Разрешает пользователя из ссылки вида:
      - @username
      - числовой user_id
    Возвращает user_id если пользователь есть в базе, иначе None.
    """
    ref = (ref or "").strip()
    if not ref:
        return None

    if ref.startswith("@"):
        uname = ref[1:].strip()
        if not uname:
            return None
        r = db_one("SELECT user_id FROM users WHERE username=? COLLATE NOCASE", (uname,))
        return int(r[0]) if r else None

    if ref.isdigit():
        uid = int(ref)
        r = db_one("SELECT 1 FROM users WHERE user_id=?", (uid,))
        return uid if r else None

    return None

def get_custom_statuses(uid: int) -> List[str]:
    rows = db_all(
        "SELECT status FROM user_custom_status WHERE user_id=? ORDER BY added_ts, status",
        (int(uid),)
    )
    out: List[str] = []
    for r in rows or []:
        s = str((r[0] if r else "") or "").strip()
        if s:
            out.append(s)
    return out

def add_custom_status(uid: int, status: str) -> bool:
    status = str(status or "").strip()
    if not status:
        return False
    # небольшой лимит, чтобы не ломать вёрстку
    if len(status) > 64:
        status = status[:64]
    db_exec(
        "INSERT OR IGNORE INTO user_custom_status (user_id, status, added_ts) VALUES (?,?,?)",
        (int(uid), status, now_ts()),
        commit=True
    )
    return True

# PAY
PAY_FRAUD_WINDOW_SEC = 24 * 3600
PAY_FRAUD_BLOCK_SEC = 24 * 3600

PAY_FRAUD_BLOCK_TEXT = (
    "Наши операторы обнаружили подозрительные переводы средств. Дабы уберечь ваши средства, мы временно блокируем любые переводы с вашего счёта и на ваш счёт на день. Благодарим вас за понимание.\n"
    "Сотрудник КО НПАО \"G®️eed\""
)

TRANSFER_BLOCK_LOG_PATH = os.path.join(DATA_DIR, "transfer_blocks.log")

def get_transfer_block(uid: int) -> Tuple[int, int, str]:
    """Возвращает (until_ts, first_notice_ts, reason) для активной блокировки, либо (0,0,'')."""
    r = db_one(
        "SELECT COALESCE(until_ts,0), COALESCE(first_notice_ts,0), COALESCE(reason,'') FROM transfer_blocks WHERE user_id=?",
        (int(uid),)
    )
    if not r:
        return 0, 0, ""
    return int(r[0] or 0), int(r[1] or 0), str(r[2] or "")

def mark_transfer_block_notified(uid: int):
    """Отмечает, что пользователю уже отправили первое уведомление о блокировке."""
    db_exec(
        "UPDATE transfer_blocks SET first_notice_ts=? WHERE user_id=? AND COALESCE(first_notice_ts,0)=0",
        (now_ts(), int(uid)),
        commit=True
    )

def log_transfer_block_file(action: str, user_id: int, until_ts: int, reason: str, *, extra: str = ""):
    """Простое логирование в файл data/transfer_blocks.log (не критично, если упадёт)."""
    try:
        line = (
            f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(now_ts()))}\t"
            f"{action}\tuser={int(user_id)}\tuntil={int(until_ts)}\treason={reason or ''}\t{extra}\n"
        )
        with open(TRANSFER_BLOCK_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass

def calc_pay_fee_cents(amount_cents: int) -> int:
    """Комиссия за /pay.

    Пороговые значения заданы в долларах (храним в центах):
    - >100_000$ -> 1%
    - >500_000$ -> 5%
    - >1_000_000$ -> 10% +0.1% за каждый полный следующий 1_000_000$, максимум 30%

    Возвращает fee в центах.
    """
    amount_cents = int(amount_cents or 0)
    if amount_cents <= 0:
        return 0

    TH_100K = 100_000 * 100
    TH_500K = 500_000 * 100
    TH_1M = 1_000_000 * 100

    bp = 0  # basis points (1% = 100bp)
    if amount_cents > TH_1M:
        extra_full_millions = (amount_cents - TH_1M) // TH_1M
        bp = 1000 + 10 * int(extra_full_millions)  # 10% + 0.1% * N
    elif amount_cents > TH_500K:
        bp = 500  # 5%
    elif amount_cents > TH_100K:
        bp = 100  # 1%

    bp = min(int(bp), 3000)  # max 30%
    if bp <= 0:
        return 0

    # округляем вверх до цента
    return int((amount_cents * bp + 9999) // 10000)

def transfer_balance(
    from_uid: int,
    to_uid: int,
    amount_cents: int,
    *,
    comment: str = "",
    chat_id: int = 0,
    msg_id: int = 0,
) -> Tuple[bool, str, int, int, int]:
    """
    Атомарный перевод денег между пользователями.

    Комиссия:
      - списывается с отправителя дополнительно (получатель получает ровно amount_cents)

    Анти-фрод:
      - если отправитель делает переводы одному и тому же получателю:
          3 раза > 1_000_000$
          5 раз > 100_000$
          10 раз > 0$
        то отправитель блокируется на 24 часа (переводы с его счёта и на его счёт).

    Возвращает: (ok, reason, sender_balance, receiver_balance, transfer_id)
      reason:
        - ok
        - bad_amount
        - self
        - insufficient
        - blocked_sender
        - blocked_receiver
        - error:...
    """
    from_uid = int(from_uid)
    to_uid = int(to_uid)
    amount_cents = int(amount_cents)

    if amount_cents <= 0:
        return False, "bad_amount", get_balance_cents(from_uid), get_balance_cents(to_uid), 0
    if from_uid == to_uid:
        return False, "self", get_balance_cents(from_uid), get_balance_cents(to_uid), 0

    with DB_LOCK:
        c = conn.cursor()
        try:
            c.execute("BEGIN")
            ts = now_ts()

            c.execute("INSERT OR IGNORE INTO users (user_id, created_ts) VALUES (?,?)", (from_uid, ts))
            c.execute("INSERT OR IGNORE INTO users (user_id, created_ts) VALUES (?,?)", (to_uid, ts))

            c.execute("SELECT COALESCE(balance_cents,0) FROM users WHERE user_id=?", (from_uid,))
            sbal = int((c.fetchone() or [0])[0] or 0)
            c.execute("SELECT COALESCE(balance_cents,0) FROM users WHERE user_id=?", (to_uid,))
            rbal = int((c.fetchone() or [0])[0] or 0)

            def _check_block(uid: int) -> int:
                c.execute("SELECT until_ts FROM transfer_blocks WHERE user_id=?", (int(uid),))
                rr = c.fetchone()
                if not rr:
                    return 0
                until_ts = int((rr[0] if rr else 0) or 0)
                if until_ts <= ts:
                    c.execute("DELETE FROM transfer_blocks WHERE user_id=?", (int(uid),))
                    return 0
                return until_ts

            from_until = _check_block(from_uid)
            to_until = _check_block(to_uid)

            if from_until > 0:
                conn.commit()
                return False, "blocked_sender", sbal, rbal, 0
            if to_until > 0:
                conn.commit()
                return False, "blocked_receiver", sbal, rbal, 0

            # анти-фрод: считаем переводы за окно времени
            ts0 = ts - int(PAY_FRAUD_WINDOW_SEC)
            TH_100K = 100_000 * 100
            TH_1M = 1_000_000 * 100

            c.execute(
                """
                SELECT
                  COALESCE(SUM(CASE WHEN amount_cents > 0 THEN 1 ELSE 0 END),0) AS c0,
                  COALESCE(SUM(CASE WHEN amount_cents > ? THEN 1 ELSE 0 END),0) AS c100k,
                  COALESCE(SUM(CASE WHEN amount_cents > ? THEN 1 ELSE 0 END),0) AS c1m
                FROM transfers
                WHERE from_id=? AND to_id=? AND ts>=?
                """,
                (TH_100K, TH_1M, from_uid, to_uid, ts0)
            )
            row = c.fetchone() or (0, 0, 0)
            c0 = int((row[0] if len(row) > 0 else 0) or 0)
            c100k = int((row[1] if len(row) > 1 else 0) or 0)
            c1m = int((row[2] if len(row) > 2 else 0) or 0)

            c0_new = c0 + 1
            c100k_new = c100k + (1 if amount_cents > TH_100K else 0)
            c1m_new = c1m + (1 if amount_cents > TH_1M else 0)

            if c1m_new >= 3 or c100k_new >= 5 or c0_new >= 10:
                until = ts + int(PAY_FRAUD_BLOCK_SEC)
                c.execute(
                    "INSERT OR REPLACE INTO transfer_blocks (user_id, until_ts, reason, created_ts, first_notice_ts) VALUES (?,?,?,?,0)",
                    (from_uid, until, "suspicious", ts)
                )
                
                c.execute(
                    "INSERT INTO transfer_block_log (action, user_id, until_ts, reason, created_ts, from_id, to_id, amount_cents, c0, c100k, c1m, chat_id, msg_id) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        "block",
                        from_uid,
                        int(until),
                        "suspicious",
                        int(ts),
                        from_uid,
                        to_uid,
                        int(amount_cents),
                        int(c0_new),
                        int(c100k_new),
                        int(c1m_new),
                        int(chat_id or 0),
                        int(msg_id or 0),
                    )
                )
                
                conn.commit()
                log_transfer_block_file(
                    "block",
                    from_uid,
                    int(until),
                    "suspicious",
                    extra=f"to={to_uid}\tamount={amount_cents}\tc0={c0_new}\tc100k={c100k_new}\tc1m={c1m_new}",
                )
                return False, "blocked_sender", sbal, rbal, 0

            fee_cents = calc_pay_fee_cents(amount_cents)
            total_debit = amount_cents + fee_cents

            if sbal < total_debit:
                conn.rollback()
                return False, "insufficient", sbal, rbal, 0

            c.execute(
                "UPDATE users SET balance_cents = COALESCE(balance_cents,0) - ? WHERE user_id=?",
                (total_debit, from_uid)
            )
            c.execute(
                "UPDATE users SET balance_cents = COALESCE(balance_cents,0) + ? WHERE user_id=?",
                (amount_cents, to_uid)
            )

            c.execute(
                "INSERT INTO transfers (from_id, to_id, amount_cents, fee_cents, ts, comment, chat_id, msg_id) VALUES (?,?,?,?,?,?,?,?)",
                (from_uid, to_uid, amount_cents, int(fee_cents), ts, (comment or "")[:500], int(chat_id or 0), int(msg_id or 0))
            )
            transfer_id = int(c.lastrowid or 0)

            c.execute("SELECT COALESCE(balance_cents,0) FROM users WHERE user_id=?", (from_uid,))
            sbal2 = int((c.fetchone() or [0])[0] or 0)
            c.execute("SELECT COALESCE(balance_cents,0) FROM users WHERE user_id=?", (to_uid,))
            rbal2 = int((c.fetchone() or [0])[0] or 0)

            conn.commit()
            return True, "ok", sbal2, rbal2, transfer_id

        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            return False, f"error:{e}", get_balance_cents(from_uid), get_balance_cents(to_uid), 0
        finally:
            try:
                c.close()
            except Exception:
                pass

def set_contract_signed(uid: int, gift_cents: int):
    db_exec("""
    UPDATE users
    SET contract_ts=?, demo_gift_cents=?, balance_cents=COALESCE(balance_cents,0)+?
    WHERE user_id=?
    """, (now_ts(), int(gift_cents), int(gift_cents), int(uid)), commit=True)
    ensure_daily_mail_row(int(uid))

# Daily mail
MAIL_INTRO_DELAY_SEC = 2 * 3600
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
        if gt == "zero":
            return "Зеро-рулетка"
        if gt == "roulette":
            return "Рулетка"
        # fallback
        return gt
    except Exception:
        return "—"

def is_registered(uid: int) -> bool:
    row = db_one("SELECT contract_ts, short_name FROM users WHERE user_id=?", (uid,))
    if not row:
        return False
    contract_ts = int((row[0] if isinstance(row, (tuple, list)) else row["contract_ts"]) or 0)
    short_name = (row[1] if isinstance(row, (tuple, list)) else row["short_name"])
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
                return f"{html_escape(comment)}"
    
    if kind.startswith("credit_default|"):
        try:
            demon_id = int(kind.split("|", 1)[1])
        except Exception:
            demon_id = 0
        d = get_user(demon_id) if demon_id else None
        dname = (d[2] if d and d[2] else "Демон")
        return (
            "Долги всегда нужно возвращать. К сожалению, вы так и не усвоили этот урок. "
            f"В наказание вашей выплатой становится ваша жизнь. С этого момента вы принадлежите <b>{html_escape(dname)}</b>.\n"
            "Куратор."
        )

    if kind.startswith("asset_slave|"):
        try:
            demon_id = int(kind.split("|", 1)[1])
        except Exception:
            demon_id = 0
        d = get_user(demon_id) if demon_id else None
        dname = (d[2] if d and d[2] else "Демон")
        return (
            "За всё необходимо платить по счетам. Черёд вашего попечителя получить свою долю от ваших побед.\n\n"
            f"<i>К письму прилагался отчет о вашем текущем положении. Демон <b>{html_escape(dname)}</b> стал держателем вашего \"основного актива\".</i>\n"
            "Всё же стоило читать условия страховки и соц.пакета..."
        )

    if kind == "demon_pay":
        return (
            "Демоны всегда держат обещания. В этот раз удача на твоей стороне."
        )

    if kind == "intro":
        body = "Ваш доброжелатель очень рад вашему вниманию и, в качестве поощрения будет раз в день высылать вам подарок."
    else:
        body = "Финансовая помощь от анонимного доброжелателя."

    return (
        f"{html_escape(body)}\n"
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
            cur.execute("SELECT user_id, next_ts, intro_sent, stopped, pending_amt_cents, pending_msg_id FROM daily_mail")
            rows = cur.fetchall()
            for (uid, next_ts, intro_sent, stopped, pending_amt, pending_msg_id) in rows:
                uid = int(uid)
                if int(stopped or 0) == 1:
                    continue

                if not is_registered(uid):
                    continue

                if has_work_history(uid):
                    stop_daily_mail(uid)
                    continue

                if int(pending_msg_id or 0) != 0:
                    continue

                if now < int(next_ts or 0):
                    continue

                if int(intro_sent or 0) == 0:
                    kind = "intro"
                    amt = 40000
                    cur.execute("UPDATE daily_mail SET next_ts=?, intro_sent=1 WHERE user_id=?", (now + MAIL_PERIOD_SEC, uid))
                    conn.commit()
                    try:
                        if user_pm_notifications_enabled(uid):
                            _send_mail_prompt(uid, kind, amt)
                        else:
                            add_balance(uid, amt)
                    except Exception:
                        pass
        except Exception:
            send_error_report("_mail_daemon")
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

    uid = int(uid)
    bal = int(u[5] or 0)
    demon = int(u[7] or 0)

    def _dedup_keep_order(items: List[str]) -> List[str]:
        seen = set()
        out: List[str] = []
        for it in items:
            it = str(it or "").strip()
            if not it or it in seen:
                continue
            seen.add(it)
            out.append(it)
        return out

    statuses: List[str] = []

    # админ / владелец бота
    if uid == int(OWNER_ID):
        statuses.append("Бот-админ")

    # кастомные статусы
    try:
        statuses.extend(get_custom_statuses(uid))
    except Exception:
        pass

    # демон
    if demon == 1:
        statuses = _dedup_keep_order(statuses)
        return "ĐĒʋÍ£" + (", " + ", ".join(statuses) if statuses else "")

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
        r = db_one("SELECT wins, losses, games_total FROM game_stats WHERE user_id=?", (uid,))
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
            r = db_one("SELECT COALESCE(MIN(acquired_ts),0) FROM slavery WHERE slave_id=?", (uid,))
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

    # Ломаный рот этой рулетки
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
        """, (uid, -1_000_000 * 100))
        if r:
            statuses.append("Ломаный рот этой рулетки")
    except Exception:
        pass

    statuses = _dedup_keep_order(statuses)
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

# SHOP: CATALOG
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
        "desc": "Шанс 7⃣ и 💀 +20% на 3 игры. Алкоголизм страшная вещь, особено от алкоголя из самых глубин ада.",
    },
    "devil_pepper": {
        "title": "🌶️ Перец дьявола",
        "price_cents": 666_00,
        "max_qty": 2,
        "duration_games": 1,
        "desc": "Могущество ада в каждом укусе! Увеличивает итоговый результат в рулетке в два раза. Однако при чрезмерно критическ💀м пр💀игрыше, вас ждет незавидная судьба... «Всё или ничего!»",
    },
    "insurance": {
        "title": "📜 Страхование капитала",
        "price_cents": 1300_00,
        "max_qty": 1,
        "duration_games": 1,
        "desc": "Защита Ваших денежных средств в случае непредвиденных затрат. Полностью сохраняет Ваши финансы от проигрыша. Соглашаясь с условиями оформления Вы полностью осознаете все сопутствующие риски. в̶п̶л̶о̶т̶ь̶ д̶о̶ п̶о̶т̶е̶р̶и̶ п̶р̶а̶в̶а̶ н̶а̶ ж̶и̶з̶н̶ь̶. ",
    },
    "paket": {
        "title": "📑 Пакет соц.поддержки",
        "price_cents": 1600_00,
        "max_qty": 1,
        "duration_games": 1,
        "desc": "Заверено нотариусом! Несколько важных бумаг в одном пакете: страхование капитала, социальный пакет, денежная компенсация! С ним вернется полная стоимость Вашего проигрыша! Соглашаясь с условиями оформления Вы полностью осознаете все сопутствующие риски. в̶п̶л̶о̶т̶ь̶ д̶о̶ п̶о̶т̶е̶р̶и̶ п̶р̶а̶в̶а̶ н̶а̶ ж̶и̶з̶н̶ь̶. ",
    },
        "lucky_chip": {
        "title": "🉐 Удачная фишка",
        "price_cents": 777_00,
        "max_qty": 3,
        "duration_games": 2,
        "desc": "С шансом 25% хотя бы две ваши ставки из прогноза будут выигрышными. Фишка с именной меткой. Интересно, чья она?",
    },
    "black_chip": {
        "title": "⚫ Черная фишка",
        "price_cents": 400_00,
        "max_qty": 4,
        "duration_games": 1,
        "desc": "Увеличивает шанс Чёрного на 5%. Складывается до 3 стаков (+15%), затем уходит на откат 1 час.",
    },
    "red_chip": {
        "title": "🔴 Красная фишка",
        "price_cents": 400_00,
        "max_qty": 4,
        "duration_games": 1,
        "desc": "Увеличивает шанс Красного на 5%. Складывается до 3 стаков (+15%), затем уходит на откат 1 час.",
    },
}

# SHOP: какие предметы работают в какой игре
SHOP_ZERO_ONLY_ITEMS = {"lucky_chip", "black_chip", "red_chip"}
SHOP_ROULETTE_ONLY_ITEMS = {"magnet", "fake_clover", "wine", "devil_pepper"}

def shop_allowed_items_for_game_type(game_type: str) -> set:
    game_type = (game_type or "roulette").strip().lower()
    if game_type == "zero":
        return {"insurance", "paket"} | set(SHOP_ZERO_ONLY_ITEMS)
    return set(SHOP_ITEMS.keys()) - set(SHOP_ZERO_ONLY_ITEMS)

# SHOP: cooldown
SHOP_ITEM_COOLDOWN_SEC = {
    "insurance": 4 * 3600,
    "paket": 8 * 3600,
    "black_chip": 1 * 3600,
    "red_chip": 1 * 3600,
}

SHOP_STACKABLE_ZERO_ITEMS = {"black_chip", "red_chip"}
SHOP_STACK_MAX = 3 # стак фишек

def shop_get_item_next_ts(uid: int, key: str) -> int:
    r = db_one(
        "SELECT COALESCE(next_ts,0) FROM shop_item_cooldowns WHERE user_id=? AND item_key=?",
        (int(uid), str(key))
    )
    return int((r[0] if r else 0) or 0)

def shop_set_item_next_ts(uid: int, key: str, next_ts: int) -> None:
    db_exec(
        "INSERT INTO shop_item_cooldowns (user_id, item_key, next_ts) VALUES (?,?,?) "
        "ON CONFLICT(user_id,item_key) DO UPDATE SET next_ts=excluded.next_ts",
        (int(uid), str(key), int(next_ts)),
        commit=True
    )

def shop_item_cooldown_left(uid: int, key: str) -> int:
    left = shop_get_item_next_ts(uid, key) - now_ts()
    return max(0, int(left))

def shop_item_cooldown_text(uid: int, key: str) -> str:
    left = shop_item_cooldown_left(uid, key)
    if left <= 0:
        return ""
    nxt = shop_get_item_next_ts(uid, key)
    nxt_txt = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(nxt)))
    return f"Следующая активация через <b>{_format_duration(left)}</b> (<b>{nxt_txt}</b>)"

# SHOP: dynamic pricing (balance-based)
SHOP_PRICE_STEP_CENTS = 5000_00  # each full $ on balance increases price
SHOP_PRICE_STEP_ADD_PCT = 100     # +% of base price per step

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

    if is_slave(uid) and "paket" in keys:
        keys = [k for k in keys if k != "paket"]

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

    if is_slave(uid):
        keys = [k for k in keys if k != "paket"]

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

def _boost_emoji_for_item(item_key: str) -> str:
    """
    Возвращает эмодзи для предмета магазина.
    Порядок:
      1) SHOP_ITEMS[item_key]['emoji'] если есть
      2) пытаемся взять первый "токен" из title, если он выглядит как эмодзи
      3) иначе пусто
    """
    item = SHOP_ITEMS.get(item_key, {}) or {}
    e = (item.get("emoji") or "").strip()
    if e:
        return e

    title = (item.get("title") or "").strip()
    if not title:
        return ""

    first = title.split()[0]
    if first and not first[0].isalnum():
        return first
    return ""

def render_active_boosts_line(player_name: str, active: dict) -> str:
    """
    Новый формат:
    Усиления {имя игрока}:
    🌶️ 🍀 🧲
    Если усилений нет — возвращаем пустую строку (строка вообще не показывается).
    """
    if not active:
        return ""

    icons: list[str] = []
    for k, v in active.items():
        try:
            if int(v or 0) <= 0:
                continue
        except Exception:
            continue

        ic = _boost_emoji_for_item(str(k))
        if ic:
            icons.append(ic)

    if not icons:
        return ""

    pname = (player_name or "").strip() or "Игрок"
    return f"Усиления {pname}:\n" + " ".join(icons)

def render_zero_boosts_inline(active: dict) -> str:
    """
    Для Зеро-рулетки:
    'Усиления: 🉐 🔴 ⚫ 📜'
    Показываем только если есть активные эффекты (remaining_games > 0).
    """
    if not active:
        return ""

    preferred_order = ["lucky_chip", "red_chip", "black_chip", "insurance", "paket"]

    icons: list[str] = []

    def _push(key: str):
        try:
            if int(active.get(key, 0) or 0) <= 0:
                return
        except Exception:
            return
        ic = _boost_emoji_for_item(key)
        if ic:
            icons.append(ic)

    for k in preferred_order:
        _push(k)

    for k, v in (active or {}).items():
        if k in preferred_order:
            continue
        try:
            if int(v or 0) <= 0:
                continue
        except Exception:
            continue
        ic = _boost_emoji_for_item(str(k))
        if ic:
            icons.append(ic)

    if not icons:
        return ""

    return "Усиления: " + " ".join(icons)

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

def shop_bind_players_for_game(game_id: str):
    """
    Привязывает активные эффекты к этой игре всем игрокам, у кого есть активки.
    Делать это нужно в момент, когда игра переходит в playing.
    """
    try:
        rows = db_all("SELECT DISTINCT user_id FROM game_players WHERE game_id=?", (game_id,))
        for (uid,) in rows:
            uid = int(uid)
            if shop_get_active(uid):  # есть активные эффекты
                shop_bind_to_game(uid, game_id)
    except Exception:
        pass

SHOP_BIND_STALE_SEC = 20 * 60  # 20 минут: старые лобби считаем зависшими для привязки активок

def shop_get_earliest_active_game(uid: int) -> str | None:
    """
    Возвращает самую раннюю активную игру пользователя для привязки усилений,
    но игнорирует "зависшие" лобби (старые lobby), которые часто остаются в БД и блокируют привязку.
    """
    rows = db_all(
        """SELECT g.game_id, g.state, g.created_ts
             FROM games g
             JOIN game_players gp ON gp.game_id=g.game_id
            WHERE gp.user_id=?
              AND g.state NOT IN ('finished','cancelled')
            ORDER BY g.created_ts ASC""",
        (uid,)
    )
    if not rows:
        return None

    now = int(time.time())
    for game_id, state, created_ts in rows:
        try:
            created_ts = int(created_ts or 0)
        except Exception:
            created_ts = 0

        if state == "lobby" and created_ts and (now - created_ts) > SHOP_BIND_STALE_SEC:
            continue

        return str(game_id)

    return None

def shop_get_active_for_game(uid: int, game_id: str) -> dict:
    """
    Активные эффекты магазина, применяемые ТОЛЬКО к привязанной игре.

    Дополнительно:
    - фильтруем предметы по типу игры (в Зеро не работают клевер/магнит/вино/перец; в рулетке не работают зеро-фишки)
    - если привязка указывает на "зависшее" старое lobby — очищаем её и даём привязаться к текущей игре.
    """
    active = shop_get_active(uid)
    if not active:
        return {}

    gt = db_one("SELECT COALESCE(game_type,'roulette') FROM games WHERE game_id=?", (str(game_id),))
    game_type = (gt[0] if gt else "roulette") or "roulette"
    allowed = shop_allowed_items_for_game_type(str(game_type))

    def _filter(d: dict) -> dict:
        out = {}
        for k, v in (d or {}).items():
            try:
                vv = int(v or 0)
            except Exception:
                vv = 0
            if vv > 0 and k in allowed:
                out[k] = vv
        return out

    now = int(time.time())
    bound = shop_get_bound_game(uid)

    if bound:
        row = db_one("SELECT state, created_ts FROM games WHERE game_id=?", (bound,))
        if not row:
            shop_clear_bind(uid)
            bound = None
        else:
            state, created_ts = row[0], row[1]
            try:
                created_ts = int(created_ts or 0)
            except Exception:
                created_ts = 0

            if state in ("finished", "cancelled"):
                shop_clear_bind(uid)
                bound = None

            elif state == "lobby" and created_ts and (now - created_ts) > SHOP_BIND_STALE_SEC:
                shop_clear_bind(uid)
                bound = None

    if bound:
        return _filter(active) if bound == game_id else {}

    earliest = shop_get_earliest_active_game(uid)
    if earliest and earliest == game_id:
        shop_bind_to_game(uid, game_id)
        return _filter(active)

    return {}

def shop_buy(uid: int, key: str) -> tuple[bool, str]:
    if key not in SHOP_ITEMS:
        return False, "Товар не найден."

    if key == "paket" and is_slave(uid):
        return False, "Этот товар недоступен для рабов."

    item = SHOP_ITEMS[key]
    have = shop_get_qty(uid, key)
    if have >= item["max_qty"]:
        return False, "У тебя уже максимальное количество этого предмета."

    u = get_user(uid)
    if not u or not u[2]:
        return False, "Нет анкеты."

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

    if key == "paket" and is_slave(uid):
        return False, "Этот товар недоступен для рабов."

    left = shop_item_cooldown_left(uid, key)
    if left > 0:
        nxt = shop_get_item_next_ts(uid, key)
        nxt_txt = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(nxt)))
        return False, f"Следующая активация через <b>{_format_duration(left)}</b> (<b>{nxt_txt}</b>)."

    item = SHOP_ITEMS[key]
    have = int(shop_get_qty(uid, key) or 0)
    if have <= 0:
        return False, "У тебя нет этого предмета."

    active = shop_get_active(uid)
    active_now = int(active.get(key, 0) or 0)

    if key in SHOP_STACKABLE_ZERO_ITEMS:
        if active_now >= SHOP_STACK_MAX:
            cd = int(SHOP_ITEM_COOLDOWN_SEC.get(key, 0) or 0)
            if cd > 0 and shop_item_cooldown_left(uid, key) <= 0:
                shop_set_item_next_ts(uid, key, now_ts() + cd)
            return False, "Достигнут максимальный стак этого эффекта."

        free_slots = max(0, SHOP_STACK_MAX - active_now)
        to_consume = min(have, free_slots, SHOP_STACK_MAX)

        if to_consume <= 0:
            return False, "Нечего активировать."

        new_stack = active_now + to_consume

        shop_set_qty(uid, key, have - to_consume)
        shop_set_active(uid, key, new_stack)

        if new_stack >= SHOP_STACK_MAX:
            cd = int(SHOP_ITEM_COOLDOWN_SEC.get(key, 0) or 0)
            if cd > 0:
                shop_set_item_next_ts(uid, key, now_ts() + cd)

        return True, (
            f"Активировано. Списано: {to_consume}. "
            f"Текущий стак: {new_stack}/{SHOP_STACK_MAX}."
        )

    if key in active and active_now > 0:
        return False, "Этот эффект уже активен."

    shop_set_qty(uid, key, have - 1)
    shop_set_active(uid, key, int(item["duration_games"]))

    if key in ("insurance", "paket"):
        cd = int(SHOP_ITEM_COOLDOWN_SEC.get(key, 0) or 0)
        if cd > 0:
            shop_set_item_next_ts(uid, key, now_ts() + cd)

    return True, f"Активировано на {item['duration_games']} игр."

def shop_mark_used(uid: int, game_id: str, item_key: str):
    db_exec(
        "INSERT OR REPLACE INTO shop_used (user_id, game_id, item_key, used_ts) VALUES (?,?,?,?)",
        (int(uid), str(game_id), str(item_key), int(time.time())),
        commit=True
    )

def shop_is_used(uid: int, game_id: str, item_key: str) -> bool:
    r = db_one(
        "SELECT 1 FROM shop_used WHERE user_id=? AND game_id=? AND item_key=?",
        (int(uid), str(game_id), str(item_key))
    )
    return bool(r)

def shop_clear_used(uid: int, game_id: str):
    db_exec(
        "DELETE FROM shop_used WHERE user_id=? AND game_id=?",
        (int(uid), str(game_id)),
        commit=True
    )

def shop_tick_after_game(uid: int, game_id: str):
    """
    Списываем 1 'игру' со всех активных эффектов пользователя ТОЛЬКО если они были привязаны к этой игре.

    Дополнительно: списываем только те предметы, которые реально работают в данном типе игры.
    Для insurance/paket списываем только если эффект реально сработал в этой игре.
    """
    bound = shop_get_bound_game(uid)
    if not bound or bound != game_id:
        return

    gt = db_one("SELECT COALESCE(game_type,'roulette') FROM games WHERE game_id=?", (str(game_id),))
    game_type = (gt[0] if gt else "roulette") or "roulette"
    allowed = shop_allowed_items_for_game_type(str(game_type))

    active = shop_get_active(uid)
    if not active:
        shop_clear_bind(uid)
        return

    for k, rem in list(active.items()):
        if k not in allowed:
            continue

        if k in ("insurance", "paket"):
            if not shop_is_used(uid, game_id, k):
                continue
            shop_set_active(uid, k, int(rem) - 1)
            continue

        if k in SHOP_STACKABLE_ZERO_ITEMS:
            shop_set_active(uid, k, 0)
            continue

        shop_set_active(uid, k, int(rem) - 1)

    shop_clear_bind(uid)
    shop_clear_used(uid, game_id)

def shop_menu_text(uid: int) -> str:
    u = get_user(uid)
    bal = int(u[5] or 0) if u else 0
    price_steps = shop_price_steps_for_balance(bal)
    price_markup = price_steps * SHOP_PRICE_STEP_ADD_PCT

    active = shop_get_active(uid)

    act_lines = []
    for k, rem in active.items():
        title = SHOP_ITEMS.get(k, {}).get("title", k)
        if k in SHOP_STACKABLE_ZERO_ITEMS:
            act_lines.append(f"• {html_escape(title)} - стак <b>{rem}</b>/<b>{SHOP_STACK_MAX}</b>")
        else:
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
    kb = InlineKeyboardMarkup(row_width=3)
    btns: list[InlineKeyboardButton] = []
    for key in get_shop_catalog(uid):
        item = SHOP_ITEMS[key]
        have = shop_get_qty(uid, key)
        btns.append(InlineKeyboardButton(
            f"{item['title']} ×{have}",
            callback_data=cb_pack(f"shop:item:{key}", uid)
        ))

    if btns:
        kb.add(*btns)
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

    cooldown_line = ""
    cd_txt = shop_item_cooldown_text(uid, key)
    if cd_txt:
        cooldown_line = cd_txt + "\n"

    warn = ""
    if key == "paket" and is_slave(uid):
        warn = "\n<b>Недоступно для рабов.</b>\n"

    return (
        f"{html_escape(item['title'])}\n\n"
        f"{html_escape(item['desc'])}\n"
        f"{warn}\n"
        f"Цена: <b>{cents_to_money_str(int(price))}</b>$\n"
        f"{markup_line}"
        f"Количество: <b>{have}</b> из <b>{item['max_qty']}</b>\n"
        f"{cooldown_line}"
        f"{f'Активный стак: <b>{rem}</b> из <b>{SHOP_STACK_MAX}</b>' if key in SHOP_STACKABLE_ZERO_ITEMS else f'Активен: <b>{rem}</b> игр'}"
    )

def shop_item_kb(uid: int, key: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()

    if key == "paket" and is_slave(uid):
        kb.add(InlineKeyboardButton("Назад", callback_data=cb_pack("shop:open", uid)))
        return kb

    kb.add(InlineKeyboardButton("Купить", callback_data=cb_pack(f"shop:buy:{key}", uid)))

    qty = int(shop_get_qty(uid, key) or 0)
    active_now = int(shop_get_active(uid).get(key, 0) or 0)
    can_activate = False

    if qty > 0:
        # Для красной/чёрной фишек разрешаем дожимать стак до 3
        if key in SHOP_STACKABLE_ZERO_ITEMS:
            can_activate = (active_now < SHOP_STACK_MAX)
            if can_activate and shop_item_cooldown_left(uid, key) > 0:
                can_activate = False
        else:
            can_activate = (active_now <= 0)
            if can_activate and key in ("insurance", "paket"):
                if shop_item_cooldown_left(uid, key) > 0:
                    can_activate = False

    if can_activate:
        kb.add(InlineKeyboardButton("Активировать", callback_data=cb_pack(f"shop:act:{key}", uid)))

    kb.add(InlineKeyboardButton("Назад", callback_data=cb_pack("shop:open", uid)))
    return kb
# Шанс раба для Страховки и Пакета
SLAVE_RISK_BASE_PCT = 15 #начальный %
SLAVE_RISK_STEP_PCT = 10 #добавочный % после каждого использования

def slave_risk_get_pct(uid: int) -> int:
    row = db_one("SELECT chance_pct FROM enslave_risk WHERE user_id=?", (int(uid),))
    if not row:
        slave_risk_reset(uid)
        return SLAVE_RISK_BASE_PCT
    try:
        ch = int(row[0] or SLAVE_RISK_BASE_PCT)
    except Exception:
        ch = SLAVE_RISK_BASE_PCT
    return max(0, min(100, ch))

def slave_risk_set_pct(uid: int, pct: int) -> None:
    pct = max(0, min(100, int(pct)))
    db_exec(
        "INSERT INTO enslave_risk (user_id, chance_pct) VALUES (?,?) "
        "ON CONFLICT(user_id) DO UPDATE SET chance_pct=excluded.chance_pct",
        (int(uid), int(pct)),
        commit=True
    )

def slave_risk_reset(uid: int) -> None:
    slave_risk_set_pct(uid, SLAVE_RISK_BASE_PCT)

def slave_risk_bump(uid: int) -> None:
    slave_risk_set_pct(uid, min(100, slave_risk_get_pct(uid) + SLAVE_RISK_STEP_PCT))

def maybe_make_slave_by_shop_trigger(uid: int, protected_amount_cents: int, game_id: str) -> Optional[int]:
    """
    Вызывается только когда сработало 'страхование' или 'алая фишка'
    (т.е. был проигрыш и предмет реально отработал).
    Возвращает demon_id если рабство назначено, иначе None.
    """
    uid = int(uid)
    if uid <= 0:
        return None

    
    if is_slave(uid): # Пока пользователь раб, шанс не накапливается и не срабатывает
        return None

    chance = slave_risk_get_pct(uid)
    roll = random.randint(1, 100)

    if roll <= chance:
        rr = db_one("SELECT user_id FROM users WHERE demon=1 ORDER BY RANDOM() LIMIT 1")
        if rr:
            demon_id = int(rr[0] or 0)
            if demon_id > 0 and demon_id != uid:
                db_exec("DELETE FROM slavery WHERE slave_id=?", (uid,), commit=True)
                slavery_add_owner(uid, demon_id, 6000)
                try:
                    set_slave_buyout(uid, int(abs(int(protected_amount_cents))) * 25) # назначение цены рабу
                except Exception:
                    pass

                try:
                    ensure_daily_mail_row(uid)
                    _send_mail_prompt(uid, f"asset_slave|{demon_id}", 0)
                except Exception:
                    pass

                slave_risk_reset(uid)
                return demon_id

        slave_risk_bump(uid)
        return None

    slave_risk_bump(uid)
    return None

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
    mult = 1.0 + 0.1 * max(0, thresholds - 1)
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
# 1x3 шансы на 💀 👹 7⃣ , стандартные 🍒🍀🍋
R_WEIGHTS_1x3 = [
    (0, 2),   # 💀
    (5, 1),   # 👹
    (4, 4),   # 7⃣
    (1, 6),   # 🍒
    (2, 6),   # 🍀
    (3, 6),   # 🍋
]
# 3x3 такие же шансы, как 1x3
R_WEIGHTS_3x3 = R_WEIGHTS_1x3
# 3x5 шансы
R_WEIGHTS_3x5 = [
    (5, 2),   # 👹
    (4, 5),   # 7⃣
    (1, 8),   # 🍒
    (2, 8),   # 🍀
    (3, 8),   # 🍋
    (0, 4),   # 💀
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
    
    def is_std(x): return x in (1, 2, 3)
    if is_std(codes[0]) and is_std(codes[1]) and codes[0] == codes[1]:
        return 0
    if is_std(codes[1]) and is_std(codes[2]) and codes[1] == codes[2]:
        return 0
    
    std_adjacent = ((is_std(codes[0]) and is_std(codes[1])) or (is_std(codes[1]) and is_std(codes[2])))
    sevens = codes.count(4)
    demons = codes.count(5)
    if std_adjacent:
        if sevens == 1 and demons == 0:
            return 0
        if demons == 1 and sevens == 0:
            return 0

    if sevens > 0:
        if sevens == 1:
            return +1 * stake
        if sevens == 2:
            return +2 * stake
        return +3 * stake

    if demons > 0:
        if demons == 1:
            return +1 * stake
        if demons == 2:
            return +4 * stake
        return +5 * stake

    if codes[0] == codes[1] == codes[2]:
        return int(round(1 * stake))
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
    
    if is_banned(clicker):
        bot.answer_callback_query(call.id, "Вам нечего здесь делать.", show_alert=True)
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

# Credit callback
@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("credit:"))
def on_credit(call: CallbackQuery):
    base, owner = cb_unpack(call.data)
    uid = call.from_user.id

    if owner is not None and uid != owner:
        bot.answer_callback_query(call.id, "Эта кнопка не для вас.", show_alert=True)
        return
    if not is_registered(uid):
        bot.answer_callback_query(call.id, "Сначала подпишите контракт в ЛС бота.", show_alert=True)
        return

    parts = base.split(":")
    action = parts[1] if len(parts) > 1 else ""

    # PAY NOW 
    if action == "pay":
        loan = credit_get_active(uid)
        if not loan:
            bot.answer_callback_query(call.id, "У вас нет активного кредита.", show_alert=True)
            return

        due = credit_due_amount_cents(loan)
        if due <= 0:
            bot.answer_callback_query(call.id, "Платеж не требуется.", show_alert=True)
            return

        bal = get_balance_cents(uid)
        if bal < due:
            bot.answer_callback_query(call.id, "Недостаточно средств для выплаты.", show_alert=True)
            return

        add_balance(uid, -due)

        remaining = int(loan[9] or 0)
        postponed = int(loan[10] or 0)
        next_due = int(loan[6] or 0)

        new_remaining = max(0, remaining - due)
        new_postponed = 0
        new_next_due = next_due + CREDIT_INTERVAL_SEC

        if new_remaining <= 0:
            db_exec(
                "UPDATE credit_loans SET status='closed', remaining_cents=0, postponed_cents=0 WHERE user_id=?",
                (uid,),
                commit=True
            )
            text = "Кредит полностью погашен.\nСпасибо за сотрудничество."
            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("Назад в профиль", callback_data=cb_pack("profile:open", uid)))
            edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
            bot.answer_callback_query(call.id)
            return

        db_exec(
            "UPDATE credit_loans SET remaining_cents=?, postponed_cents=?, next_due_ts=? WHERE user_id=? AND status='active'",
            (new_remaining, new_postponed, new_next_due, uid),
            commit=True
        )

        loan2 = credit_get_active(uid)
        text = credit_format_contract(uid, loan2, as_active_view=True)
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("Внести выплату сразу", callback_data=cb_pack("credit:pay", uid)))
        kb.add(InlineKeyboardButton("Перенести выплату", callback_data=cb_pack("credit:skip", uid)))
        kb.add(InlineKeyboardButton("Внести всю сумму долга досрочно", callback_data=cb_pack("credit:payfull", uid)))
        kb.add(InlineKeyboardButton("Назад в профиль", callback_data=cb_pack("profile:open", uid)))
        edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        return

    #  SKIP / POSTPONE 
    if action == "skip":
        loan = credit_get_active(uid)
        if not loan:
            bot.answer_callback_query(call.id, "У вас нет активного кредита.", show_alert=True)
            return

        due = credit_due_amount_cents(loan)
        if due <= 0:
            bot.answer_callback_query(call.id, "Платеж не требуется.", show_alert=True)
            return

        remaining = int(loan[9] or 0)
        postponed = int(loan[10] or 0)
        next_due = int(loan[6] or 0)

        new_postponed = postponed + due
        new_next_due = next_due + CREDIT_INTERVAL_SEC

        db_exec(
            "UPDATE credit_loans SET postponed_cents=?, next_due_ts=? WHERE user_id=? AND status='active'",
            (new_postponed, new_next_due, uid),
            commit=True
        )

        loan2 = credit_get_active(uid)
        text = credit_format_contract(uid, loan2, as_active_view=True)
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("Внести выплату сразу", callback_data=cb_pack("credit:pay", uid)))
        kb.add(InlineKeyboardButton("Перенести выплату", callback_data=cb_pack("credit:skip", uid)))
        kb.add(InlineKeyboardButton("Внести всю сумму долга досрочно", callback_data=cb_pack("credit:payfull", uid)))
        kb.add(InlineKeyboardButton("Назад в профиль", callback_data=cb_pack("profile:open", uid)))
        edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        return

    # PAY FULL
    if action == "payfull":
        loan = credit_get_active(uid)
        if not loan:
            bot.answer_callback_query(call.id, "У вас нет активного кредита.", show_alert=True)
            return

        principal = int(loan[1] or 0)
        rate = int(loan[3] or 0)
        remaining = int(loan[9] or 0)

        # Досрочное погашение: остаток + "процентную ставку" (штраф 1 раз от тела кредита)
        penalty = (principal * rate + 99) // 100
        need = remaining + penalty

        bal = get_balance_cents(uid)
        if bal < need:
            bot.answer_callback_query(call.id, "Недостаточно средств для досрочного погашения.", show_alert=True)
            return

        add_balance(uid, -need)
        db_exec(
            "UPDATE credit_loans SET status='closed', remaining_cents=0, postponed_cents=0 WHERE user_id=?",
            (uid,),
            commit=True
        )

        text = "Кредит досрочно погашен.\nСпасибо за сотрудничество."
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("Назад в профиль", callback_data=cb_pack("profile:open", uid)))
        edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        return

    #  TERM 
    if action == "term":
        sum_cents = int(parts[2])
        term_days = int(parts[3])

        ok, msg = credit_amount_ok(uid, sum_cents)
        if not ok:
            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("Назад в профиль", callback_data=cb_pack("profile:open", uid)))
            edit_inline_or_message(call, f"<b>Ошибка:</b> {html_escape(msg)}", reply_markup=kb, parse_mode="HTML")
            bot.answer_callback_query(call.id)
            return

        rates = []
        if term_days == 30:
            rates = [15, 20]
        elif term_days == 60:
            rates = [20, 25]
        elif term_days == 90:
            rates = [25, 35]

        kb = InlineKeyboardMarkup()
        for r in rates:
            kb.add(InlineKeyboardButton(f"{r}%", callback_data=cb_pack(f"credit:rate:{sum_cents}:{term_days}:{r}", uid)))
        kb.add(InlineKeyboardButton("Отказаться", callback_data=cb_pack(f"credit:cancel:{sum_cents}", uid)))

        text = (
            "<i><u>Кредитная организация НПАО \"G®️eed\"</u></i>\n"
            "Номер 7660006213 ОГРН 132066630021\n"
            "Предоставление частных кредитных услуг на комфортные сроки под приятные процентные ставки.\n"
            f"Желаемая сумма: <b>{cents_to_money_str(sum_cents)}</b>$\n\n"
            "Выберите процентную ставку:"
        )
        edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        return

    # RATE -> SHOW CONTRACT
    if action == "rate":
        sum_cents = int(parts[2])
        term_days = int(parts[3])
        rate = int(parts[4])

        ok, msg = credit_amount_ok(uid, sum_cents)
        if not ok:
            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("Назад в профиль", callback_data=cb_pack("profile:open", uid)))
            edit_inline_or_message(call, f"<b>Ошибка:</b> {html_escape(msg)}", reply_markup=kb, parse_mode="HTML")
            bot.answer_callback_query(call.id)
            return

        code_num = random.randint(1000000, 9999999)
        me = get_user(uid)
        me_name = me[2] if me and me[2] else "—"

        total = credit_total_payable_cents(sum_cents, rate)
        pay_cnt = credit_payments_count(term_days)
        pay_each = credit_payment_cents(total, pay_cnt)

        text = (
            f"Договор о предоставлении услуг кредитования № {code_num:07d}\n"
            f"Вы: <u>{html_escape(me_name)}</u>\n"
            f"Сумма кредита: <b>{cents_to_money_str(sum_cents)}</b>$\n"
            f"Срок: <b>{term_days}</b> дней\n"
            f"Ставка: <b>{rate}</b>%\n"
            f"Сумма выплаты: <b><u>{cents_to_money_str(pay_each)}</u></b>$\n"
            "Выплата по кредиту будет производиться каждые 2 дня\n"
            "Интересует?"
        )

        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("Подписать договор", callback_data=cb_pack(f"credit:sign:{sum_cents}:{term_days}:{rate}:{code_num}", uid)))
        kb.add(InlineKeyboardButton("Отказаться", callback_data=cb_pack(f"credit:cancel:{sum_cents}", uid)))

        edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        return

    # SIGN
    if action == "sign":
        sum_cents = int(parts[2])
        term_days = int(parts[3])
        rate = int(parts[4])
        code_num = int(parts[5])

        ok, msg = credit_amount_ok(uid, sum_cents)
        if not ok:
            bot.answer_callback_query(call.id, msg, show_alert=True)
            return

        if credit_has_active(uid):
            bot.answer_callback_query(call.id, "У вас уже есть активный кредит.", show_alert=True)
            return

        now = now_ts()
        total = credit_total_payable_cents(sum_cents, rate)
        pay_cnt = credit_payments_count(term_days)
        pay_each = credit_payment_cents(total, pay_cnt)

        next_due = now + CREDIT_INTERVAL_SEC
        end_ts = now + int(term_days) * 24 * 3600

        db_exec(
            """
            INSERT OR REPLACE INTO credit_loans
            (user_id, contract_code, principal_cents, term_days, rate_pct, created_ts, status,
             next_due_ts, end_ts, payment_cents, remaining_cents, postponed_cents, last_notice_ts, notice_msg_id)
            VALUES (?,?,?,?,?,?, 'active', ?,?,?,?,?, 0, 0)
            """,
            (uid, code_num, sum_cents, term_days, rate, now,
             next_due, end_ts, pay_each, total, 0),
            commit=True
        )

        # выдаём кредит
        add_balance(uid, sum_cents)

        edit_inline_or_message(
            call,
            "С вами приятно иметь дело!\n"
            "Мы будем уведомлять вас о наступающем списании выплаты, благодарим вас за использование наших услуг.",
            reply_markup=None,
            parse_mode="HTML"
        )
        bot.answer_callback_query(call.id)
        return

    # CANCEL
    if action == "cancel":
        sum_cents = int(parts[2]) if len(parts) > 2 else 0
        if sum_cents <= 0:
            ok, msg = credit_amount_ok(uid, sum_cents)
            if not ok:
                kb = InlineKeyboardMarkup()
                kb.add(InlineKeyboardButton("Назад в профиль", callback_data=cb_pack("profile:open", uid)))
                edit_inline_or_message(call, f"<b>Ошибка:</b> {html_escape(msg)}", reply_markup=kb, parse_mode="HTML")
                bot.answer_callback_query(call.id)
                return    
            edit_inline_or_message(call, "Вы не указали сумму. Повторите свой запрос.", reply_markup=None, parse_mode="HTML")
            bot.answer_callback_query(call.id)
            return

        text = (
            "<i><u>Кредитная организация НПАО \"G®️eed\"</u></i>\n"
            "Номер 7660006213 ОГРН 132066630021\n"
            "Предоставление частных кредитных услуг на комфортные сроки под приятные процентные ставки.\n"
            f"Желаемая сумма: <b>{cents_to_money_str(sum_cents)}</b>$\n\n"
            "Выберите срок погашения кредита:"
        )
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("30 дней", callback_data=cb_pack(f"credit:term:{sum_cents}:30", uid)))
        kb.add(InlineKeyboardButton("60 дней", callback_data=cb_pack(f"credit:term:{sum_cents}:60", uid)))
        kb.add(InlineKeyboardButton("90 дней", callback_data=cb_pack(f"credit:term:{sum_cents}:90", uid)))
        edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
        bot.answer_callback_query(call.id)
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
        bot.answer_callback_query(call.id, "Вы не можете нажать на эту кнопку", show_alert=True)
        return

    row = db_one("SELECT pending_amt_cents, pending_msg_id, pending_kind FROM daily_mail WHERE user_id=?", (uid,))
    if not row:
        bot.answer_callback_query(call.id, "Письмо не найдено.", show_alert=True)
        return

    amt_cents = int(row[0] or 0)
    msg_id = int(row[1] or 0)
    kind = row[2] or ""

    if msg_id == 0:
        bot.answer_callback_query(call.id, "Письмо уже открыто.", show_alert=True)
        return

    try:
        if call.message and call.message.message_id != msg_id:
            bot.answer_callback_query(call.id, "Это письмо уже неактуально.", show_alert=True)
            return
    except Exception:
        pass

    letter = _mail_letter_text(kind, amt_cents)
    text = f"<i>Текст письма:</i>\n{letter}"
    if amt_cents > 0:
        text += f"\n<i>К письму прилагался чек на</i> <b>{cents_to_money_str(amt_cents)}</b>$"

    rc, _ = db_exec(
        """
        UPDATE daily_mail
           SET pending_amt_cents=0,
               pending_msg_id=0,
               pending_kind=''
         WHERE user_id=?
           AND pending_msg_id=?
           AND pending_amt_cents=?
           AND pending_kind=?
        """,
        (uid, msg_id, amt_cents, kind),
        commit=True
    )

    if int(rc or 0) == 0:
        bot.answer_callback_query(call.id, "Письмо уже было открыто.", show_alert=True)
        return

    if amt_cents > 0:
        add_balance(uid, amt_cents)

    try:
        bot.edit_message_text(text, chat_id=uid, message_id=msg_id, parse_mode="HTML")
    except Exception:
        try:
            bot.send_message(uid, text, parse_mode="HTML")
        except Exception:
            pass

    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("settings:toggle:"))
def on_settings_toggle(call: CallbackQuery):
    base, owner = cb_unpack(call.data)
    uid = call.from_user.id

    if owner is not None and owner != uid:
        bot.answer_callback_query(call.id, "Вы не можете нажать на эту кнопку", show_alert=True)
        return

    ensure_user_settings(uid)

    what = base.split(":", 2)[2] if ":" in base else ""

    if what == "pmnotify":
        set_user_pm_notify(uid, not user_pm_notifications_enabled(uid))
    elif what == "autodel":
        set_user_auto_delete_pm(uid, not user_auto_delete_pm_enabled(uid))
    else:
        bot.answer_callback_query(call.id, "Неизвестная настройка.", show_alert=True)
        return

    try:
        if getattr(call, "message", None):
            set_settings_msg_id(uid, int(call.message.message_id))
            show_settings_menu(call.message.chat.id, uid, prefer_edit=True)
        bot.answer_callback_query(call.id)
    except Exception:
        bot.answer_callback_query(call.id, "Не удалось обновить настройки.", show_alert=True)

@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("dealpm:"))
def on_dealpm_callbacks(call: CallbackQuery):
    base, owner = cb_unpack(call.data)
    uid = call.from_user.id

    if owner is not None and owner != uid:
        bot.answer_callback_query(call.id, "Вы не можете нажать на эту кнопку", show_alert=True)
        return

    parts = (base or "").split(":")
    if len(parts) < 2 or parts[1] != "open":
        bot.answer_callback_query(call.id)
        return

    action, _stage, payload = trade_state_get(uid)
    if not action:
        bot.answer_callback_query(call.id, "Заявка не найдена или устарела.", show_alert=True)
        return

    try:
        _begin_trade_pm_flow(uid, getattr(call.from_user, "username", None), action, payload)
    except Exception:
        open_hint = f"@{BOT_USERNAME}" if BOT_USERNAME else "ботом"
        bot.answer_callback_query(
            call.id,
            f"Первым делом запустите {open_hint}",
            show_alert=True
        )
        return

    bot.answer_callback_query(call.id, "Проверьте личные сообщения.")

@bot.message_handler(content_types=["new_chat_members"])
def on_bot_added_to_group(message):
    if message.chat.type not in ("group", "supergroup"):
        return

    try:
        me_id = int(getattr(ME, "id", 0) or 0)
    except Exception:
        me_id = 0

    for u in (getattr(message, "new_chat_members", None) or []):
        try:
            uid = int(getattr(u, "id", 0) or 0)
        except Exception:
            uid = 0

        if me_id > 0 and uid == me_id:
            remember_group_chat(message.chat.id, getattr(message.chat, "title", "") or "")
            break

@bot.message_handler(content_types=["left_chat_member"])
def on_bot_left_group(message):
    if message.chat.type not in ("group", "supergroup"):
        return

    try:
        me_id = int(getattr(ME, "id", 0) or 0)
    except Exception:
        me_id = 0

    left_user = getattr(message, "left_chat_member", None)
    left_id = int(getattr(left_user, "id", 0) or 0) if left_user else 0

    if me_id > 0 and left_id == me_id:
        forget_group_chat(message.chat.id)

def compute_group_key_from_callback(call: CallbackQuery, prefix_len=PREFIX_LEN) -> Optional[str]:
    if getattr(call, "message", None) and getattr(call.message, "chat", None):
        try:
            if getattr(call.message.chat, "type", "") in ("group", "supergroup"):
                remember_group_chat(int(call.message.chat.id), getattr(call.message.chat, "title", "") or "")
        except Exception:
            pass
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

def zero_media_enabled() -> bool:
    fid = (PHOTO_FILE_ID or "").strip()
    return bool(fid) and fid != "PASTE_YOUR_FILE_ID_HERE"

def edit_zero_message(
    call: CallbackQuery,
    text: str,
    reply_markup=None,
    parse_mode: Optional[str] = None,
    force_media: bool = False
):
    """
    Для Зеро-рулетки:
    - если PHOTO_FILE_ID задан => сообщение становится фото+caption (force_media=True),
      далее обновляем caption.
    - если PHOTO_FILE_ID не задан => обычное edit_message_text.
    """
    if not zero_media_enabled():
        edit_inline_or_message(call, text, reply_markup=reply_markup, parse_mode=parse_mode)
        return

    inline_id = getattr(call, "inline_message_id", None)

    if force_media:
        try:
            media = InputMediaPhoto(media=PHOTO_FILE_ID, caption=text, parse_mode=parse_mode)
            if inline_id:
                bot.edit_message_media(inline_message_id=inline_id, media=media, reply_markup=reply_markup)
                return
            if getattr(call, "message", None):
                bot.edit_message_media(
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    media=media,
                    reply_markup=reply_markup
                )
                return
        except Exception:
            pass

        edit_inline_or_message(call, text, reply_markup=reply_markup, parse_mode=parse_mode)
        return

    try:
        if inline_id:
            bot.edit_message_caption(
                inline_message_id=inline_id,
                caption=text,
                parse_mode=parse_mode,
                reply_markup=reply_markup
            )
            return
        if getattr(call, "message", None):
            bot.edit_message_caption(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                caption=text,
                parse_mode=parse_mode,
                reply_markup=reply_markup
            )
            return
    except Exception:
        pass

    edit_inline_or_message(call, text, reply_markup=reply_markup, parse_mode=parse_mode)

# INLINE MENU
def inline_article(title: str, desc: str, text: str, kb, thumb_key: str = "") -> InlineQueryResultArticle:
    tu = get_inline_thumb_url(thumb_key)

    base_kwargs = dict(
        id=str(uuid.uuid4()),
        title=title,
        description=desc,
        input_message_content=InputTextMessageContent(text, parse_mode="HTML"),
        reply_markup=kb
    )

    if tu:
        try:
            return InlineQueryResultArticle(**base_kwargs, thumbnail_url=tu)
        except TypeError:
            try:
                return InlineQueryResultArticle(**base_kwargs, thumb_url=tu)
            except TypeError:
                res = InlineQueryResultArticle(**base_kwargs)
                try:
                    res.thumb_url = tu
                except Exception:
                    pass
                try:
                    res.thumbnail_url = tu
                except Exception:
                    pass
                return res

    return InlineQueryResultArticle(**base_kwargs)

_INLINE_THUMB_VARNAMES = { # inline thumbs: key -> config var name
    "start": "INLINE_THUMB_START_URL",
    "ban":"INLINE_THUMB_BAN_URL",
    "game": "INLINE_THUMB_GAME_URL",
    "profile": "INLINE_THUMB_PROFILE_URL",
    "stats": "INLINE_THUMB_STATS_URL",
    "work": "INLINE_THUMB_WORK_URL",
    "credit": "INLINE_THUMB_CREDIT_URL",
}

def _normalize_github_url(url: str) -> str:
    """
    Поддерживаем:
    1) https://raw.githubusercontent.com/user/repo/branch/path.png
    2) https://github.com/user/repo/blob/branch/path.png  -> raw
    3) ...?raw=true -> игнорируем query, чтобы raw не ломался
    """
    url = (url or "").strip()
    if not url:
        return ""

    # убираем query (?raw=true и т.п.), иначе в raw получится битый путь
    url = url.split("?", 1)[0].strip()

    if "raw.githubusercontent.com/" in url:
        return url

    m = re.match(r"^https?://github\.com/([^/]+)/([^/]+)/blob/([^/]+)/(.*)$", url)
    if m:
        user, repo, branch, path = m.group(1), m.group(2), m.group(3), m.group(4)
        return f"https://raw.githubusercontent.com/{user}/{repo}/{branch}/{path}"

    return url

def _cfg_get(name: str) -> str:
    """env -> config_local.py -> ''"""
    v = (os.environ.get(name) or "").strip()
    if v:
        return v
    try:
        import config_local  # type: ignore
        v = str(getattr(config_local, name, "") or "").strip()
        return v
    except Exception:
        return ""

def get_inline_thumb_url(key: str) -> str:
    key = (key or "").strip().lower()
    if not key:
        return ""

    if key in _INLINE_THUMB_URL_CACHE:
        return _INLINE_THUMB_URL_CACHE.get(key, "") or ""

    varname = _INLINE_THUMB_VARNAMES.get(key, "")
    if not varname:
        _INLINE_THUMB_URL_CACHE[key] = ""
        return ""

    url = _cfg_get(varname)
    url = _normalize_github_url(url)

    if not (url.startswith("http://") or url.startswith("https://")):
        url = ""

    _INLINE_THUMB_URL_CACHE[key] = url
    return url

@bot.inline_handler(func=lambda q: True)
def on_inline(q: InlineQuery):
    uid = q.from_user.id
    username = getattr(q.from_user, "username", None)
    upsert_user(uid, username)
    sleeping = bool(_FORCE_SLEEPING)
    if not sleeping:
        try:
            sleeping, _mode, _reason, _last_err = get_bot_sleep_state()
        except Exception:
            sleeping = False

    if sleeping:
        u = get_user(uid)

        if u and u[2]:
            uid2, uname, short_name, created_ts, contract_ts, bal, gift, demon = u
            status = compute_status(uid)

            try:
                cur.execute("SELECT user_id FROM users WHERE demon=0")
                uids = [r[0] for r in cur.fetchall()]
                uids.sort(key=lambda x: top_value_cents(x), reverse=True)
                place = (uids.index(uid2) + 1) if (int(demon or 0) == 0 and uid2 in uids) else "-"
            except Exception:
                place = "-"

            text = (
                f"Имя пользователя: <i>{html_escape(short_name)}</i>\n"
                f"Дата подписания контракта: <b>{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(contract_ts or created_ts or now_ts()))}</b>\n"
                f"Статус: <b>{html_escape(status)}</b>\n"
                f"Капитал: <b>{cents_to_money_str(int(bal or 0))}</b>$\n"
                f"Место в топе: <b>{place}</b>\n\n"
                "<b>⛔ Бот временно отключён. Игры и остальные функции недоступны.</b>"
            )
        else:
            text = "<b>⛔ Бот временно отключён.</b>\nПрофиль недоступен (нет анкеты)."

        kb = None
        if is_bot_admin(uid):
            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("Команды", callback_data=cb_pack("profile:commands", uid)))

        results = [inline_article(
            "Профиль",
            "Основная сводка по вашей деятельности в боте",
            text,
            kb,
            thumb_key="profile"
        )]

        bot.answer_inline_query(q.id, results, cache_time=0, is_personal=True)
        return

    # Бан (inline)
    banned, until_ts, reason = get_ban_info(uid)
    if banned:
        txt = "Ваш аккаунт заблокирован администратором."
        if until_ts and int(until_ts) > 0:
            txt += f"\nДо: <b>{html_escape(_fmt_ts(int(until_ts)))}</b>."
        if reason:
            txt += f"\nПричина: <i>{html_escape(reason)}. Куратор вами разочарован.</i>"
        txt += "\n\nЕсли вы не согласны с решением — /report → Апелляция."
    
        results = []
        results.append(inline_article(
            "Конец",
            "Ваша история подошла к концу...",
            txt,
            None,
            thumb_key="ban"
        ))
        bot.answer_inline_query(q.id, results, cache_time=0)
        return

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
            kb,
            thumb_key="start"
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
            None,
            thumb_key="game"
        ))
    elif stake_cents <= 0:
        text = "Мы не работаем в долг. Сделай ставку, введи сумму"
        results.append(inline_article(
            "Начать игру",
            "Сделай свою ставку",
            text,
            None,
            thumb_key="game"
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
            kb.add(InlineKeyboardButton(
                "Зеро-рулетка",
                callback_data=cb_pack(f"game:start:zero:life:{stake_cents}", uid)
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
            kb.add(InlineKeyboardButton(
                "Зеро-рулетка",
                callback_data=cb_pack(f"game:start:zero:{stake_cents}", uid)
            ))
        if life_flag:
            game_text = (
                "<b><u>⟢♣♦ Игры ♥♠⟣</u></b>\n\n"
                f"Текущая ставка: <b>{cents_to_money_str(stake_cents)}</b>$\n"
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
            kb,
            thumb_key="game"
        ))

    # Работа
    u = get_user(uid)
    if not u or not u[2]:
        results.append(inline_article(
            "Работа",
            "Выбрать вакансию и выйти в смену",
            "Вас ожидают.",
            None,
            thumb_key="work"
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
                f"Вернётся через <b>{_format_duration(left)}</b>"
            )
            results.append(inline_article(
                "Работа",
                "Текущая смена",
                text,
                None,
                thumb_key="work"
            ))
        else:
            jobs = load_jobs()
            if not jobs:
                results.append(inline_article(
                    "Работа",
                    "Выбрать вакансию и выйти в смену",
                    "Файл jobs.txt пуст или сломан.",
                    None,
                    thumb_key="work"
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
                job_buttons = []
                for jk, job in jobs.items():
                    job_buttons.append(
                        InlineKeyboardButton(
                            job.title,
                            callback_data=cb_pack(f"work:pick:{jk}", uid)
                        )
                    )
                for i in range(0, len(job_buttons), 2):
                    kb.row(*job_buttons[i:i + 2])
                results.append(inline_article(
                    "Работа",
                    "Выбрать вакансию и выйти в смену",
                    text,
                    kb,
                    thumb_key="work"
                ))

    # Профиль
    u = get_user(uid)
    if not u or not u[2]:
        results.append(inline_article(
            "Профиль",
            "Основная сводка по вашей деятельности в боте",
            "Вас ожидают.",
            None,
            thumb_key="profile"
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
        kb.add(InlineKeyboardButton("Контракт", callback_data=cb_pack("profile:contract", uid)))
        if is_bot_admin(uid):
            kb.add(InlineKeyboardButton("Команды", callback_data=cb_pack("profile:commands", uid)))
        if credit_has_active(uid):
            kb.add(InlineKeyboardButton("Договор по кредиту", callback_data=cb_pack("profile:credit", uid)))
        if has_work_history(uid):
            kb.add(InlineKeyboardButton("Трудовая книга", callback_data=cb_pack("profile:workbook", uid)))
        if owns_slaves(uid):
            kb.add(InlineKeyboardButton("Список рабов", callback_data=cb_pack("profile:slaves", uid)))
        if is_slave(uid):
            kb.add(InlineKeyboardButton("Статус раба", callback_data=cb_pack("profile:slave_status", uid)))

        results.append(inline_article(
            "Профиль",
            "Основная сводка по вашей деятельности в боте",
            text,
            kb,
            thumb_key="profile"
        ))

    # Кредит
    try:
        loan = credit_get_active(uid)
        if loan:
            text = credit_format_contract(uid, loan, as_active_view=True)
    
            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("Внести выплату сразу", callback_data=cb_pack("credit:pay", uid)))
            kb.add(InlineKeyboardButton("Внести всю сумму долга досрочно", callback_data=cb_pack("credit:payfull", uid)))
        
        else:
            sum_cents = int(stake_cents or 0)
            min_c, max_c, wins = credit_limits_cents(uid)

            if sum_cents <= 0:
                text = (
                    "Вы указали недостоверную сумму, согласно лимиту.\n"
                    f"Лимит кредита: <b>{cents_to_money_str(min_c)}</b>$ — <b>{cents_to_money_str(max_c)}</b>$.\n"
                    "Повторите свой запрос с учетом лимита.\n\n"
                    "Примечание: за каждые 10 побед мы предоставляем повышенные условия по лимиту."
                )
                kb = InlineKeyboardMarkup()
            else:
                ok, msg = credit_amount_ok(uid, sum_cents)
                if not ok:
                    text = (
                        "<i><u>Кредитная организация НПАО \"G®️eed\"</u></i>\n"
                        "Номер 7660006213 ОГРН 132066630021\n"
                        "Предоставление частных кредитных услуг на комфортные сроки под приятные процентные ставки.\n"
                        f"Запрошено: <b>{cents_to_money_str(sum_cents)}</b>$\n\n"
                        f"{html_escape(msg)}"
                    )
                    kb = InlineKeyboardMarkup()
                else:
                    text = (
                        "<i><u>Кредитная организация НПАО \"G®️eed\"</u></i>\n"
                        "Номер 7660006213 ОГРН 132066630021\n"
                        "Предоставление частных кредитных услуг на комфортные сроки под приятные процентные ставки.\n"
                        f"Желаемая сумма: <b>{cents_to_money_str(sum_cents)}</b>$\n\n"
                        "Выберите срок погашения кредита:"
                    )
                    kb = InlineKeyboardMarkup()
                    kb.add(InlineKeyboardButton("30 дней", callback_data=cb_pack(f"credit:term:{sum_cents}:30", uid)))
                    kb.add(InlineKeyboardButton("60 дней", callback_data=cb_pack(f"credit:term:{sum_cents}:60", uid)))
                    kb.add(InlineKeyboardButton("90 дней", callback_data=cb_pack(f"credit:term:{sum_cents}:90", uid)))
    
        results.append(inline_article(
            "Кредит",
            "Оформить кредит",
            text,
            kb,
            thumb_key="credit"
        ))
    except Exception:
        pass

    # Статистика
    cur.execute("SELECT user_id FROM users WHERE demon=0")
    all_uids = [r[0] for r in cur.fetchall()]
    all_uids.sort(key=lambda u: top_value_cents(u), reverse=True)
    
    header = "📄<b><u>Статистика</u>\nПо количеству денежного трафика</b>\n\n"
    lines = []
    topn = all_uids[:STATS_TOP_LIMIT]
    for i2, uid_top in enumerate(topn, start=1):
        lines.append(format_user_line(uid_top, i2, uid))
    
    if uid in all_uids:
        my_place = all_uids.index(uid) + 1
        if my_place > STATS_TOP_LIMIT:
            lines.append("…")
            lines.append(format_user_line(uid, my_place, uid))
    
    text = header + "\n".join(lines if lines else ["Пусто"])
    
    kb = stats_kb(uid, "money")
    results.append(inline_article(
        "Статистика",
        f"Топ {STATS_TOP_LIMIT} по трафику / рабовладельцам",
        text,
        kb,
        thumb_key="stats"
    ))

    bot.answer_inline_query(q.id, results, cache_time=0)

# /start
@bot.message_handler(commands=["start"])
def cmd_start(message):
    uid = message.from_user.id
    username = getattr(message.from_user, "username", None)
    upsert_user(uid, username)
    if is_banned(uid):
        bot.send_message(message.chat.id, "Очередная рекламная брошура. Вы выкинули письмо...\n\n\n🚫Ваш аккаунт был заблокирован администратором. Если не согласны с решением алминистратора, отправте апелляцию /report")
        return

    parts = message.text.split(maxsplit=1)
    payload = parts[1].strip() if len(parts) > 1 else ""

    if payload.startswith("confirm_"):
        try:
            _, game_id, target_uid = payload.split("_", 2)
            target_uid = int(target_uid)
        except Exception:
            return

        if target_uid != uid:
            bot.send_message(message.chat.id, "Это подтверждение не для вас.")
            return

        r = db_one("SELECT 1 FROM game_players WHERE game_id=? AND user_id=?", (game_id, uid))
        if not r:
            bot.send_message(message.chat.id, "Вы не находитесь в лобби этой игры.")
            return

        payload = "contract"

    if payload == "settings":
        show_settings_menu(message.chat.id, uid, prefer_edit=True)
        return

    if payload != "contract":
        return

    u = get_user(uid)
    if u and u[2] and u[4]:
        try:
            refresh_lobbies_for_user(uid)
        except Exception:
            pass
        return

    text = "Из конверта выглядывает строка для вашей росписи. Оставить подпись?"
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Выкинуть подозрительное письмо", callback_data=cb_pack("reg:throw", uid)))
    kb.add(InlineKeyboardButton("Подписать", callback_data=cb_pack("reg:sign", uid)))
    sent = bot.send_message(message.chat.id, text, reply_markup=kb)
    set_reg_state(uid, "await_name", sent.message_id)

@bot.message_handler(commands=["settings"])
def cmd_settings(message):
    uid = message.from_user.id
    username = getattr(message.from_user, "username", None)
    upsert_user(uid, username)
    show_settings_menu(message.chat.id, uid, prefer_edit=True)

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
        bot.answer_callback_query(call.id, "Время ещё не пришло...")
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
@bot.message_handler(func=lambda m: (
    m.chat.type == "private"
    and m.text
    and not m.text.startswith("/")
    and db_one(
        "SELECT 1 FROM report_state WHERE user_id=? AND stage='await_content' LIMIT 1",
        (int(m.from_user.id),),
    ) is None
))
def on_private_text(message):
    uid = message.from_user.id
    username = getattr(message.from_user, "username", None)
    upsert_user(uid, username)

    t_action, t_stage, t_payload = trade_state_get(uid)
    if t_action and t_stage == "await_amount":
        target_un, _old_amount = _trade_unpack_payload(t_payload)

        if not target_un:
            trade_state_clear(uid)
            bot.reply_to(message, "Заявка устарела. Запустите сделку заново.")
            return

        raw_sum = (message.text or "").replace("$", "").strip()
        cents = money_to_cents(raw_sum)
        if cents is None or cents <= 0:
            bot.reply_to(message, "Неверный формат суммы. Поддерживаемые форматы 15000 или 15000.50")
            return

        trade_state_clear(uid)

        if t_action == "buyrab":
            cmd_buyrab(_make_private_stub_message(uid, username, f"/buyrab @{target_un} {raw_sum}"))
            return

        if t_action == "rebuy":
            cmd_buy(_make_private_stub_message(uid, username, f"/rebuy @{target_un} {raw_sum}"))
            return

        trade_state_clear(uid)
        bot.reply_to(message, "Заявка устарела. Запустите сделку заново.")
        return   

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
    try:
        refresh_lobbies_for_user(uid)
    except Exception:
        pass

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

STATS_TOP_LIMIT = 20

def stats_kb(uid: int, active: str) -> InlineKeyboardMarkup:
    active = (active or "money").strip()
    kb = InlineKeyboardMarkup()
    a1 = "Денежный трафик" if active == "money" else "Денежный трафик"
    a2 = "Рабовладельцы" if active == "owners" else "Рабовладельцы"
    kb.row(
        InlineKeyboardButton(a1, callback_data=cb_pack("stats:top", uid)),
        InlineKeyboardButton(a2, callback_data=cb_pack("stats:owners", uid)),
    )
    return kb

def format_owner_line(owner_id: int, place: int, highlight_uid: int, slaves_cnt: int, earned_cents: int) -> str:
    r = db_one("SELECT short_name, username FROM users WHERE user_id=?", (int(owner_id),))
    name = (r[0] if r else None) or "Без имени"
    uname = (r[1] if r else "") or ""

    name_html = f"<b>{html_escape(name)}</b>"
    if int(owner_id) == int(highlight_uid):
        name_html = f"<b><u>{html_escape(name)}</u></b>"

    uname_part = f" (@{html_escape(uname)})" if uname else ""
    return (
        f"{place}. {name_html}{uname_part} — рабов: <b>{int(slaves_cnt)}</b> | "
        f"доход: <b>{cents_to_money_str(int(earned_cents))}</b>$"
    )

def get_slave_owner_ranking() -> list[tuple[int, int, int]]:
    """
    Возвращает список (owner_id, slaves_cnt, earned_cents),
    сортировка: slaves_cnt desc, earned_cents desc.
    """
    rows = db_all("""
        SELECT owner_id,
               COUNT(DISTINCT slave_id) AS slaves_cnt,
               COALESCE(SUM(COALESCE(earned_cents,0)),0) AS earned_cents
        FROM slavery
        GROUP BY owner_id
        HAVING slaves_cnt > 0
    """, ())
    out = [(int(r[0]), int(r[1] or 0), int(r[2] or 0)) for r in (rows or [])]
    out.sort(key=lambda x: (x[1], x[2]), reverse=True)
    return out

@bot.callback_query_handler(func=lambda c: c.data and (c.data.startswith("stats:") or c.data.startswith("profile:") or c.data.startswith("work:") or c.data.startswith("game:")))
def on_main_callbacks(call: CallbackQuery):
    base, owner = cb_unpack(call.data)
    clicker = call.from_user.id

    if owner is not None and clicker != owner:
        bot.answer_callback_query(call.id, "Вы не можете нажать на эту кнопку", show_alert=True)
        return

    if is_banned(clicker):
        bot.answer_callback_query(call.id, "Ваш аккаунт заблокирован. Вы больше не принадлежите этому миру.", show_alert=True)
        return

    group_key = compute_group_key_from_callback(call)

    parts = base.split(":")
    kind = parts[0]

    #STATS TOP
    if kind == "stats" and parts[1] == "top":
        cur.execute("SELECT user_id FROM users WHERE demon=0")
        all_uids = [r[0] for r in cur.fetchall()]
        all_uids.sort(key=lambda u: top_value_cents(u), reverse=True)
    
        header = "📄<b><u>Статистика</u>\nПо количеству денежного трафика</b>\n\n"
        lines = []
        topn = all_uids[:STATS_TOP_LIMIT]
        for i, uid in enumerate(topn, start=1):
            lines.append(format_user_line(uid, i, clicker))
    
        if clicker in all_uids:
            my_place = all_uids.index(clicker) + 1
            if my_place > STATS_TOP_LIMIT:
                lines.append("…")
                lines.append(format_user_line(clicker, my_place, clicker))
    
        text = header + "\n".join(lines if lines else ["Пусто"])
        kb = stats_kb(clicker, "money")
        edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        return
    
    if kind == "stats" and parts[1] == "owners":
        ranking = get_slave_owner_ranking()
    
        header = "📄<b><u>Статистика</u>\nРабовладельцы</b>\n\n"
        helpline = "\n\nДля более детальной информации по пользователю /rabs"
        lines = []
    
        topn = ranking[:STATS_TOP_LIMIT]
        for i, (oid, scnt, earned) in enumerate(topn, start=1):
            lines.append(format_owner_line(oid, i, clicker, scnt, earned))
    
        my_place = None
        for i, (oid, _sc, _er) in enumerate(ranking, start=1):
            if int(oid) == int(clicker):
                my_place = i
                break
    
        if my_place is not None and my_place > STATS_TOP_LIMIT:
            oid, scnt, earned = next((x for x in ranking if int(x[0]) == int(clicker)), (clicker, 0, 0))
            lines.append("…")
            lines.append(format_owner_line(oid, int(my_place), clicker, scnt, earned))
    
        text = header + "\n".join(lines if lines else ["Пусто"]) + helpline
        kb = stats_kb(clicker, "owners")
        edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        return

    # PROFILE

    if kind == "profile" and parts[1] == "openview":
        try:
            target_id = int(parts[2])
        except Exception:
            bot.answer_callback_query(call.id, "Ошибка.", show_alert=True)
            return

        if is_banned(target_id):
            edit_inline_or_message(call, "У пользователя нет больше профиля.", reply_markup=None, parse_mode="HTML")
            bot.answer_callback_query(call.id)
            return

        text = build_profile_summary_text(target_id)
        if not text:
            edit_inline_or_message(call, "У пользователя нет профиля.", reply_markup=None, parse_mode="HTML")
            bot.answer_callback_query(call.id)
            return

        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton(
            "Статистика по играм",
            callback_data=cb_pack(f"profile:gamesview:{target_id}", clicker)
        ))

        edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        return

    if kind == "profile" and parts[1] == "gamesview":
        try:
            target_id = int(parts[2])
        except Exception:
            bot.answer_callback_query(call.id, "Ошибка.", show_alert=True)
            return

        st = get_game_stats(target_id)
        games_total, wins, losses, max_win, max_lose = st
        pct_w = (wins / games_total * 100.0) if games_total > 0 else 0.0
        pct_l = (losses / games_total * 100.0) if games_total > 0 else 0.0

        text = (
            f"Общее число игр: <b>{games_total}</b>\n"
            f"Часто играет: <i>{html_escape(get_favorite_game_title(target_id))}</i>\n"
            f"Победы: <b>{wins}</b> /<b>{pct_w:.1f}%</b>\n"
            f"Поражения: <b>{losses}</b> /<b>{pct_l:.1f}%</b>\n"
            f"Максимальная выигранная сумма: <b>{cents_to_money_str(max_win)}</b>$\n"
            f"Максимальная проигранная сумма: <b>{cents_to_money_str(max_lose)}</b>$"
        )

        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton(
            "Назад к профилю",
            callback_data=cb_pack(f"profile:openview:{target_id}", clicker)
        ))

        edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        return
    
    if kind == "profile" and parts[1] == "contract":
        text = render_contract_for_user(clicker)
        if not text:
            edit_inline_or_message(call, "Контракт ещё не подписан.", reply_markup=None, parse_mode="HTML")
            bot.answer_callback_query(call.id)
            return

        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("Назад в профиль", callback_data=cb_pack("profile:open", clicker)))

        edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        return

    if kind == "profile" and parts[1] == "open":
        text = build_profile_summary_text(clicker)
        if not text:
            edit_inline_or_message(call, "Вам пришло одно особенное письмо. Рекомендуем вам его проверить.", None, "HTML")
            bot.answer_callback_query(call.id)
            return
    
        kb = build_profile_open_kb(clicker)
        edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        return
    
    if kind == "profile" and parts[1] == "commands":
        if not is_bot_admin(clicker):
            bot.answer_callback_query(call.id, "Недостаточно прав.", show_alert=True)
            return

        lines = [
            "Список команд модерирования",
            "",
            f"Состояние бота: <b>{html_escape(bot_status_human())}</b> ☚",            
            "☛ рассылка /remessage",
            "☛ чаты /chatlist",
            "Статусы ☚",
            "☛ кастомный /addstatus",
            "☛ демон /devil",
            "☛ человек /human",
            "☛ удалить раба /delrab",
            "Регистрация ☚",
            "☛ перерегистрация юзера /reg",
            "☛ удаление юзера /del",
            "☛ бан юзера /ban",
            "☛ разбан юзера /unban",
            "Редактирование ☚",
            "ㅤфинансы ☚",
            "ㅤ☛ выдать /finance",
            "ㅤ☛ забрать /take",
            "ㅤ☛ разблокировка /udblockcash",
            "ㅤ☛ блокировка /blockcash",
            "☛ работа /work",
            "☛ чистка чатов /clearpm",
        ]

        if clicker == OWNER_ID:
            lines += [
                "",
                "Владелец ☚",
                "☛ добавить админа /add_admin",
                "☛ убрать админа /remove_admin",
                "☛ список админов /admins",
                "☛ база данных /db",
                "☛ включить бота /bot_on",
                "☛ выключить бота /bot_off {error|update}",
            ]

        text = "\n".join(lines)

        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("Назад в профиль", callback_data=cb_pack("profile:open", clicker)))

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
    
    if kind == "profile" and parts[1] == "credit":
        loan = credit_get_active(clicker)
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("Назад в профиль", callback_data=cb_pack("profile:open", clicker)))

        if not loan:
            edit_inline_or_message(call, "У вас нет активного кредита.", reply_markup=kb, parse_mode="HTML")
            bot.answer_callback_query(call.id)
            return

        text = credit_format_contract(clicker, loan, as_active_view=True)

        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("Внести выплату сразу", callback_data=cb_pack("credit:pay", clicker)))
        kb.add(InlineKeyboardButton("Перенести выплату", callback_data=cb_pack("credit:skip", clicker)))
        kb.add(InlineKeyboardButton("Внести всю сумму долга досрочно", callback_data=cb_pack("credit:payfull", clicker)))
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

        lines = ["Список вашего второстепенного дохода\nИмя|Общий доход|За последнее время|Последнее зачисление"]
        top = rows[:20]
        for i, (slave_id, earned_cents, share_bp, acquired_ts) in enumerate(top, 1):
            slave_id = int(slave_id)
            earned_cents = int(earned_cents or 0)
            lasth = slave_profit_lasth(slave_id, clicker)
            lastp = int(slave_last_credit(slave_id, clicker) or 0)


            cur.execute("SELECT short_name, username FROM users WHERE user_id=?", (slave_id,))
            r = cur.fetchone() or (None, None)
            sname = r[0] or "Без имени"
            sun = r[1] or ""

            uname_part = f" (@{html_escape(sun)})" if sun else ""
            lines.append(
                f"{i}|<b>{html_escape(sname)}</b>{uname_part} "
                f"<u><b>{cents_to_money_str(earned_cents)}</b>$</u>"
                f"(<b>{cents_to_money_str(lasth)}</b>$) "
                f"+ <b>{cents_to_money_str(lastp)}</b>$"
            )

        kb = InlineKeyboardMarkup()

        slave_buttons = []
        for (slave_id, _earned, _bp, _acq) in top:
            slave_id = int(slave_id)
            cur.execute("SELECT short_name FROM users WHERE user_id=?", (slave_id,))
            sname = (cur.fetchone() or ("Без имени",))[0] or "Без имени"

            btn_text = sname
            if len(btn_text) > 18:
                btn_text = btn_text[:18] + "…"

            slave_buttons.append(
                InlineKeyboardButton(
                    btn_text,
                    callback_data=cb_pack(f"profile:slavecard:{slave_id}", clicker)
                )
            )

        for i in range(0, len(slave_buttons), 3):
            kb.row(*slave_buttons[i:i + 3])

        kb.row(InlineKeyboardButton("Назад в профиль", callback_data=cb_pack("profile:open", clicker)))

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
            lasth = slave_profit_lasth(slave_id, clicker)
            lastp = int(slave_last_credit(slave_id, clicker) or 0)
    
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
                "Общий доход|За последнее время|Последнее начисление\n"
                f"<u><b>{cents_to_money_str(earned_cents)}</b>$</u>"
                f"(<b>{cents_to_money_str(lasth)}</b>$) "
                f"+ <b>{cents_to_money_str(lastp)}</b>$"
            )

            owners_all = get_slave_owners(slave_id)
            other = [(oid, bp) for (oid, bp) in owners_all if int(oid) != int(clicker)]
            
            if other:
                total_bp = sum(int(bp or 0) for (_oid, bp) in owners_all) or 0
                pay_map = {}
                if buyout_cents > 0 and total_bp > 0 and owners_all:
                    allocated = 0
                    for i, (oid, bp) in enumerate(owners_all):
                        part = (buyout_cents * int(bp or 0)) // total_bp
                        pay_map[int(oid)] = int(part)
                        allocated += int(part)
                    pay_map[int(owners_all[0][0])] = pay_map.get(int(owners_all[0][0]), 0) + (buyout_cents - allocated)
            
                text += "\n\nВладельцы:\n"
                for oid, _bp in other:
                    cur.execute("SELECT short_name, username FROM users WHERE user_id=?", (int(oid),))
                    rr = cur.fetchone() or ("Без имени", "")
                    oname = rr[0] or "Без имени"
                    oun = rr[1] or ""
                    ou_part = f" (@{html_escape(oun)})" if oun else ""
                    price = int(pay_map.get(int(oid), 0) or 0)
                    text += (
                        f"{html_escape(oname)}{ou_part} | Сумма выкупа его доли: "
                        f"<b>{cents_to_money_str(price)}</b>$\n"
                    )
                text += "Для полноправного владения рабом, выкупите его командой /rebuy"
    
            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("Освободить раба", callback_data=cb_pack(f"profile:slavefreeask:{slave_id}", clicker)))
            kb.add(InlineKeyboardButton("Назад к списку рабов", callback_data=cb_pack("profile:slaves", clicker)))
            kb.add(InlineKeyboardButton("Назад в профиль", callback_data=cb_pack("profile:open", clicker)))
            edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
            bot.answer_callback_query(call.id)
            return

    if kind == "profile" and parts[1] == "slavefreeask":
        try:
            slave_id = int(parts[2])
        except Exception:
            bot.answer_callback_query(call.id, "Ошибка.", show_alert=True)
            return

        row = db_one(
            "SELECT 1 FROM slavery WHERE slave_id=? AND owner_id=? LIMIT 1",
            (slave_id, clicker)
        )
        if not row:
            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("Назад к списку рабов", callback_data=cb_pack("profile:slaves", clicker)))
            edit_inline_or_message(call, "Вы больше не владеете этим рабом.", reply_markup=kb, parse_mode="HTML")
            bot.answer_callback_query(call.id)
            return

        _ensure_slave_meta_row(slave_id)
        meta = db_one(
            "SELECT COALESCE(buyout_cents,0) FROM slave_meta WHERE slave_id=?",
            (slave_id,)
        )
        buyout_cents = int((meta[0] if meta else 0) or 0)
        reward_cents = max(0, buyout_cents // 10)

        text = (
            "Вы точно уверены, что хотите отпустить раба?\n"
            "Вы потеряете довольно солидную часть дохода, если решитесь его освободить.\n\n"
            f"Компенсация за освобождение: <b>{cents_to_money_str(reward_cents)}</b>$"
        )

        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("Да, я уверен", callback_data=cb_pack(f"profile:slavefreeyes:{slave_id}", clicker)))
        kb.add(InlineKeyboardButton("Я ещё подумаю...", callback_data=cb_pack(f"profile:slavecard:{slave_id}", clicker)))

        edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        return

    if kind == "profile" and parts[1] == "slavefreeyes":
        try:
            slave_id = int(parts[2])
        except Exception:
            bot.answer_callback_query(call.id, "Ошибка.", show_alert=True)
            return

        ok, reward_cents = owner_free_slave_with_reward(clicker, slave_id)
        if not ok:
            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("Назад к списку рабов", callback_data=cb_pack("profile:slaves", clicker)))
            edit_inline_or_message(call, "Освобождение не выполнено: вы больше не владеете этим рабом.", reply_markup=kb, parse_mode="HTML")
            bot.answer_callback_query(call.id)
            return

        text = (
            "Раб освобождён.\n"
            f"Вам начислено: <b>{cents_to_money_str(reward_cents)}</b>$"
        )

        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("Вернуться к списку рабов", callback_data=cb_pack("profile:slaves", clicker)))
        kb.add(InlineKeyboardButton("Назад в профиль", callback_data=cb_pack("profile:open", clicker)))

        edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        return

    if kind == "profile" and parts[1] == "rabslist":
        try:
            owner_id = int(parts[2])
        except Exception:
            bot.answer_callback_query(call.id, "Ошибка.", show_alert=True)
            return

        text, kb = build_rabs_list_text_kb(owner_id, clicker)
        edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        return

    if kind == "profile" and parts[1] == "rabsview":
        try:
            owner_id = int(parts[2])
            slave_id = int(parts[3])
        except Exception:
            bot.answer_callback_query(call.id, "Ошибка.", show_alert=True)
            return

        cur.execute("""
            SELECT COALESCE(earned_cents,0), COALESCE(share_bp,0), COALESCE(acquired_ts,0)
            FROM slavery
            WHERE slave_id=? AND owner_id=?
        """, (slave_id, owner_id))
        row = cur.fetchone()
        if not row:
            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("Назад к списку рабов", callback_data=cb_pack(f"profile:rabslist:{owner_id}", clicker)))
            edit_inline_or_message(call, "Этот раб больше не принадлежит выбранному владельцу.", reply_markup=kb, parse_mode="HTML")
            bot.answer_callback_query(call.id)
            return

        earned_cents, share_bp, acquired_ts = int(row[0] or 0), int(row[1] or 0), int(row[2] or 0)
        lasth = int(slave_profit_lasth(slave_id, owner_id) or 0)
        lastp = int(slave_last_credit(slave_id, owner_id) or 0)

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
            "Общий доход|За последнее время|Последнее начисление\n"
            f"<u><b>{cents_to_money_str(earned_cents)}</b>$</u>"
            f"(<b>{cents_to_money_str(lasth)}</b>$) "
            f"+ <b>{cents_to_money_str(lastp)}</b>$"
        )

        owners_all = get_slave_owners(slave_id)
        other = [(oid, bp) for (oid, bp) in owners_all if int(oid) != int(owner_id)]

        if other:
            total_bp = sum(int(bp or 0) for (_oid, bp) in owners_all) or 0
            pay_map = {}
            if buyout_cents > 0 and total_bp > 0 and owners_all:
                allocated = 0
                for i, (oid, bp) in enumerate(owners_all):
                    part = (buyout_cents * int(bp or 0)) // total_bp
                    pay_map[int(oid)] = int(part)
                    allocated += int(part)
                pay_map[int(owners_all[0][0])] = pay_map.get(int(owners_all[0][0]), 0) + (buyout_cents - allocated)

            text += "\n\nВладельцы:\n"
            for oid, _bp in other:
                cur.execute("SELECT short_name, username FROM users WHERE user_id=?", (int(oid),))
                rr = cur.fetchone() or ("Без имени", "")
                oname = rr[0] or "Без имени"
                oun = rr[1] or ""
                ou_part = f" (@{html_escape(oun)})" if oun else ""
                price = int(pay_map.get(int(oid), 0) or 0)
                text += (
                    f"{html_escape(oname)}{ou_part} | Сумма выкупа его доли: "
                    f"<b>{cents_to_money_str(price)}</b>$\n"
                )
            text += "Для полноправного владения рабом, выкупите его командой /rebuy"

        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("Назад к списку рабов", callback_data=cb_pack(f"profile:rabslist:{owner_id}", clicker)))

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
                pct = (int(bp or 0) / 100.0)
                last = slave_last_credit(uid, int(oid))
                if last is None:
                    last_part = "<b>-</b>"
                else:
                    last_part = f"<b>{cents_to_money_str(last)}</b>$"
                
                lines.append(
                    f"{i}) <b>{html_escape(oname)}</b>{tag} - <b>{pct:.1f}%</b> | "
                    f"Последнее зачисление {last_part}"
                )
        else:
            lines.append("\nВладельцы: <b>-</b>")

        lines.append("")
        if buyout_cents > 0:
            lines.append(f"Сумма выкупа: <b>{cents_to_money_str(buyout_cents)}</b>$")
        else:
            lines.append("Сумма выкупа: <b>-</b>")

        lines.append(f"Проигрышей жизни: <b>{strikes}</b>")
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
                f"Вернётся через <b>{_format_duration(left)}</b>"
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

        job_buttons = []
        for jk, job in jobs.items():
            job_buttons.append(
                InlineKeyboardButton(
                    job.title,
                    callback_data=cb_pack(f"work:pick:{jk}", clicker)
                )
            )

        for i in range(0, len(job_buttons), 2):
            kb.row(*job_buttons[i:i + 2])
            
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
            f"Вернёшься через <b>{_format_duration(ends_ts - now_ts())}</b>\n\n"
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
        if game_key == "zero" and getattr(getattr(call, "message", None), "chat", None) and call.message.chat.type == "private":
            bot.answer_callback_query(call.id, "Зеро-рулетка доступна только в групповых чатах.", show_alert=True)
            return
        
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

    cur.execute("SELECT slave_id, buyer_id, price_cents, active FROM buy_offers WHERE offer_id=?", (offer_id,))
    off = cur.fetchone()
    if not off:
        bot.answer_callback_query(call.id, "Оффер не найден.", show_alert=True)
        return

    slave_id, buyer_id, price_cents, active = int(off[0]), int(off[1]), int(off[2]), int(off[3] or 0)
    if active != 1:
        bot.answer_callback_query(call.id, "Оффер уже закрыт.", show_alert=True)
        return

    buyer_bal = get_balance_cents(buyer_id)
    if buyer_bal < 0 or buyer_bal < price_cents:
        try:
            bot.answer_callback_query(call.id, "Сделка сорвалась: у покупателя недостаточно средств.", show_alert=True)
        except Exception:
            pass
        try:
            bot.send_message(buyer_id, "Сделка сорвалась: у вас недостаточно средств на оплату.")
        except Exception:
            pass
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

# BUYRAB offers покупка раба
def _buyrab_finalize_if_ready(offer_id: str):
    """
    Если все владельцы дали ответ, закрывает сделку:
    - возвращает остаток hold покупателю
    - помечает state=2
    Возвращает dict с результатом или None, если сделка ещё не готова.
    """
    offer_id = str(offer_id or "")
    if not offer_id:
        return None

    with DB_LOCK:
        c = conn.cursor()
        try:
            c.execute("BEGIN")
            c.execute(
                "SELECT tx_no, slave_id, buyer_id, total_cents, hold_cents, state "
                "FROM buyrab_offers WHERE offer_id=?",
                (offer_id,),
            )
            off = c.fetchone()
            if not off:
                conn.rollback()
                return None

            tx_no = int(off[0] or 0)
            slave_id = int(off[1] or 0)
            buyer_id = int(off[2] or 0)
            total_cents = int(off[3] or 0)
            hold_cents = int(off[4] or 0)
            state = int(off[5] or 0)

            if state != 1:
                conn.rollback()
                return None

            c.execute(
                "SELECT owner_id, pay_cents, status FROM buyrab_offer_resp WHERE offer_id=?",
                (offer_id,),
            )
            rows = c.fetchall() or []
            if not rows:
                conn.rollback()
                return None

            pending = sum(1 for (_oid, _pay, st) in rows if int(st or 0) == 0)
            if pending > 0:
                conn.rollback()
                return None

            accepted = [(int(oid), int(pay or 0)) for (oid, pay, st) in rows if int(st or 0) == 1]
            declined = [(int(oid), int(pay or 0)) for (oid, pay, st) in rows if int(st or 0) == -1]
            owners_count = len(rows)

            refund = max(0, hold_cents)
            if refund > 0 and buyer_id > 0:
                c.execute(
                    "UPDATE users SET balance_cents=COALESCE(balance_cents,0)+? WHERE user_id=?",
                    (refund, buyer_id),
                )

            c.execute(
                "UPDATE buyrab_offers SET hold_cents=0, state=2 WHERE offer_id=?",
                (offer_id,),
            )
            conn.commit()

            spent = max(0, total_cents - refund)

            return {
                "offer_id": offer_id,
                "tx_no": tx_no,
                "slave_id": slave_id,
                "buyer_id": buyer_id,
                "total_cents": total_cents,
                "spent_cents": spent,
                "refund_cents": refund,
                "owners_count": owners_count,
                "accepted": accepted,
                "declined": declined,
            }

        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            return None
        finally:
            try:
                c.close()
            except Exception:
                pass

@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("buyrab:"))
def on_buyrab_callbacks(call: CallbackQuery):
    base, owner = cb_unpack(call.data)
    clicker = call.from_user.id

    if owner is not None and owner != 0 and clicker != owner:
        bot.answer_callback_query(call.id, "Вы не можете нажать на эту кнопку", show_alert=True)
        return

    parts = (base or "").split(":")
    if len(parts) < 3:
        bot.answer_callback_query(call.id)
        return

    action = parts[1]
    offer_id = parts[2]

    if action not in ("send", "cancel", "acc", "dec"):
        bot.answer_callback_query(call.id)
        return

    if action in ("send", "cancel"):
        with DB_LOCK:
            c = conn.cursor()
            try:
                c.execute("BEGIN")
                c.execute(
                    "SELECT tx_no, slave_id, buyer_id, total_cents, state FROM buyrab_offers WHERE offer_id=?",
                    (offer_id,),
                )
                off = c.fetchone()
                if not off:
                    conn.rollback()
                    bot.answer_callback_query(call.id, "Сделка не найдена.", show_alert=True)
                    return

                tx_no = int(off[0] or 0)
                slave_id = int(off[1] or 0)
                buyer_id = int(off[2] or 0)
                total_cents = int(off[3] or 0)
                state = int(off[4] or 0)

                if clicker != buyer_id:
                    conn.rollback()
                    bot.answer_callback_query(call.id, "Это не ваша сделка.", show_alert=True)
                    return

                if action == "cancel":
                    if state != 0:
                        conn.rollback()
                        bot.answer_callback_query(call.id, "Сделка уже отправлена или закрыта.", show_alert=True)
                        return
                    c.execute("UPDATE buyrab_offers SET state=-1 WHERE offer_id=?", (offer_id,))
                    c.execute("DELETE FROM buyrab_offer_resp WHERE offer_id=?", (offer_id,))
                    conn.commit()

                    try:
                        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
                    except Exception:
                        pass
                    try:
                        bot.edit_message_text("Сделка отменена.", call.message.chat.id, call.message.message_id)
                    except Exception:
                        pass

                    bot.answer_callback_query(call.id, "Отменено.")
                    return

                if state != 0:
                    conn.rollback()
                    bot.answer_callback_query(call.id, "Сделка уже отправлена или закрыта.", show_alert=True)
                    return

                c.execute("SELECT balance_cents FROM users WHERE user_id=?", (buyer_id,))
                br = c.fetchone()
                bal = int(br[0] or 0) if br else 0
                if bal < total_cents or bal < 0:
                    conn.rollback()
                    bot.answer_callback_query(call.id, "Недостаточно средств для оформления сделки.", show_alert=True)
                    return

                c.execute(
                    "UPDATE users SET balance_cents=COALESCE(balance_cents,0)-? WHERE user_id=?",
                    (total_cents, buyer_id),
                )
                c.execute(
                    "UPDATE buyrab_offers SET hold_cents=?, state=1 WHERE offer_id=?",
                    (total_cents, offer_id),
                )

                c.execute("SELECT owner_id, pay_cents FROM buyrab_offer_resp WHERE offer_id=?", (offer_id,))
                owner_rows = [(int(r[0]), int(r[1] or 0)) for r in (c.fetchall() or [])]

                c.execute("SELECT short_name, username FROM users WHERE user_id=?", (slave_id,))
                sr = c.fetchone() or (None, None)
                slave_name = sr[0] or "Без имени"
                slave_un = sr[1] or ""

                c.execute("SELECT short_name, username FROM users WHERE user_id=?", (buyer_id,))
                ur = c.fetchone() or (None, None)
                buyer_name = ur[0] or "Покупатель"
                buyer_un = ur[1] or ""

                conn.commit()

            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass
                bot.answer_callback_query(call.id, f"Ошибка: {e}", show_alert=True)
                return
            finally:
                try:
                    c.close()
                except Exception:
                    pass

        buyer_un_part = f" (@{html_escape(buyer_un)})" if buyer_un else ""
        slave_un_part = f" (@{html_escape(slave_un)})" if slave_un else ""

        failed = []
        sent = 0
        for oid, pay_cents in owner_rows:
            kb = InlineKeyboardMarkup()
            kb.row(
                InlineKeyboardButton("Согласиться", callback_data=cb_pack(f"buyrab:acc:{offer_id}", oid)),
                InlineKeyboardButton("Отказаться", callback_data=cb_pack(f"buyrab:dec:{offer_id}", oid)),
            )
            msg = (
                f"Сделка купле-продажи раба №{tx_no}\n\n"
                f"Объект сделки: раб <b>{html_escape(slave_name)}</b>{slave_un_part}\n"
                f"Покупатель: <b>{html_escape(buyer_name)}</b>{buyer_un_part}\n"
                f"Сумма к получению: <b>{cents_to_money_str(pay_cents)}</b>$\n"
                f"Согласны на сделку?"
            )
            try:
                bot.send_message(oid, msg, parse_mode="HTML", reply_markup=kb)
                sent += 1
            except Exception:
                failed.append(oid)

        if failed:
            with DB_LOCK:
                c = conn.cursor()
                try:
                    c.execute("BEGIN")
                    for oid in failed:
                        c.execute(
                            "UPDATE buyrab_offer_resp SET status=-1 WHERE offer_id=? AND owner_id=? AND status=0",
                            (offer_id, int(oid)),
                        )
                    conn.commit()
                except Exception:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                finally:
                    try:
                        c.close()
                    except Exception:
                        pass

        try:
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
        except Exception:
            pass
        try:
            bot.edit_message_text("Предложение отправлено владельцам.", call.message.chat.id, call.message.message_id)
        except Exception:
            pass

        bot.answer_callback_query(call.id, "Отправлено.")

        fin = _buyrab_finalize_if_ready(offer_id)
        if fin:
            _send_buyrab_final(fin)

        return

    if action in ("acc", "dec"):
        with DB_LOCK:
            c = conn.cursor()
            try:
                c.execute("BEGIN")
                c.execute(
                    "SELECT tx_no, slave_id, buyer_id, total_cents, hold_cents, state "
                    "FROM buyrab_offers WHERE offer_id=?",
                    (offer_id,),
                )
                off = c.fetchone()
                if not off:
                    conn.rollback()
                    bot.answer_callback_query(call.id, "Сделка не найдена.", show_alert=True)
                    return

                tx_no = int(off[0] or 0)
                slave_id = int(off[1] or 0)
                buyer_id = int(off[2] or 0)
                hold_cents = int(off[4] or 0)
                state = int(off[5] or 0)

                if state != 1:
                    conn.rollback()
                    bot.answer_callback_query(call.id, "Сделка уже закрыта.", show_alert=True)
                    return

                c.execute(
                    "SELECT pay_cents, status FROM buyrab_offer_resp WHERE offer_id=? AND owner_id=?",
                    (offer_id, clicker),
                )
                rr = c.fetchone()
                if not rr:
                    conn.rollback()
                    bot.answer_callback_query(call.id, "Это предложение не для вас.", show_alert=True)
                    return
                pay_cents = int(rr[0] or 0)
                st = int(rr[1] or 0)
                if st != 0:
                    conn.rollback()
                    bot.answer_callback_query(call.id, "Вы уже ответили.", show_alert=True)
                    return

                if action == "dec":
                    c.execute(
                        "UPDATE buyrab_offer_resp SET status=-1 WHERE offer_id=? AND owner_id=?",
                        (offer_id, clicker),
                    )
                    conn.commit()

                else:
                    c.execute("SELECT share_bp FROM slavery WHERE slave_id=? AND owner_id=?", (slave_id, clicker))
                    sr = c.fetchone()
                    if not sr:
                        c.execute(
                            "UPDATE buyrab_offer_resp SET status=-1 WHERE offer_id=? AND owner_id=?",
                            (offer_id, clicker),
                        )
                        conn.commit()
                        bot.answer_callback_query(call.id, "У вас уже нет доли владения этим рабом.", show_alert=True)
                        return
                    seller_bp = int(sr[0] or 0)

                    if pay_cents <= 0:
                        c.execute(
                            "UPDATE buyrab_offer_resp SET status=-1 WHERE offer_id=? AND owner_id=?",
                            (offer_id, clicker),
                        )
                        conn.commit()
                        bot.answer_callback_query(call.id, "Некорректная сумма сделки.", show_alert=True)
                        return

                    if int(hold_cents or 0) < pay_cents:
                        c.execute(
                            "UPDATE buyrab_offer_resp SET status=-1 WHERE offer_id=? AND owner_id=?",
                            (offer_id, clicker),
                        )
                        conn.commit()
                        bot.answer_callback_query(call.id, "У покупателя не хватает зарезервированных средств.", show_alert=True)
                        return

                    c.execute(
                        "UPDATE users SET balance_cents=COALESCE(balance_cents,0)+? WHERE user_id=?",
                        (pay_cents, clicker),
                    )
                    c.execute(
                        "UPDATE buyrab_offers SET hold_cents=COALESCE(hold_cents,0)-? WHERE offer_id=?",
                        (pay_cents, offer_id),
                    )

                    c.execute("DELETE FROM slavery WHERE slave_id=? AND owner_id=?", (slave_id, clicker))
                    c.execute("SELECT share_bp FROM slavery WHERE slave_id=? AND owner_id=?", (slave_id, buyer_id))
                    br2 = c.fetchone()
                    if br2:
                        new_bp = min(10000, int(br2[0] or 0) + seller_bp)
                        c.execute(
                            "UPDATE slavery SET share_bp=? WHERE slave_id=? AND owner_id=?",
                            (new_bp, slave_id, buyer_id),
                        )
                    else:
                        c.execute(
                            "INSERT OR IGNORE INTO slavery (slave_id, owner_id, share_bp, earned_cents, acquired_ts) "
                            "VALUES (?,?,?,?,?)",
                            (slave_id, buyer_id, seller_bp, 0, now_ts()),
                        )

                    c.execute(
                        "UPDATE buyrab_offer_resp SET status=1 WHERE offer_id=? AND owner_id=?",
                        (offer_id, clicker),
                    )
                    conn.commit()

            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass
                bot.answer_callback_query(call.id, f"Ошибка: {e}", show_alert=True)
                return
            finally:
                try:
                    c.close()
                except Exception:
                    pass

        try:
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
        except Exception:
            pass

        bot.answer_callback_query(call.id, "Ответ учтён.")

        fin = _buyrab_finalize_if_ready(offer_id)
        if fin:
            _send_buyrab_final(fin)

        return

def _send_buyrab_final(fin: dict):
    """
    Рассылает итог покупателю (и кратко уведомляет раба).
    fin — результат из _buyrab_finalize_if_ready.
    """
    if not fin:
        return

    buyer_id = int(fin.get("buyer_id") or 0)
    slave_id = int(fin.get("slave_id") or 0)
    owners_count = int(fin.get("owners_count") or 0)
    accepted = fin.get("accepted") or []
    declined = fin.get("declined") or []
    spent = int(fin.get("spent_cents") or 0)
    refund = int(fin.get("refund_cents") or 0)

    def _disp(uid: int) -> str:
        u = get_user(uid)
        nm = (u[2] if u and u[2] else "Без имени")
        un = (u[1] if u and u[1] else "")
        return f"{html_escape(nm)}" + (f" (@{html_escape(un)})" if un else "")

    if accepted:
        sellers = [_disp(int(oid)) for (oid, _pay) in accepted]
        sellers_txt = ", ".join(sellers)

        if len(sellers) == 1:
            sellers_line = f"Продавец {sellers_txt}"
        else:
            sellers_line = f"Продавцы: {sellers_txt}"

        txt = (
            "Сделка прошла успешно!\n"
            f"{sellers_line}\n"
            f"Потраченная сумма: <b>{cents_to_money_str(spent)}</b>$"
        )
        if refund > 0 and declined:
            txt += f"\nСумма к возврату: <b>{cents_to_money_str(refund)}</b>$"

        try:
            bot.send_message(buyer_id, txt, parse_mode="HTML")
        except Exception:
            pass

        # уведомим раба, если купили хотя бы долю
        try:
            bot.send_message(
                slave_id,
                "Часть твоих прав владения перешла другому пользователю. Проверь текущих владельцев в профиле.",
                parse_mode="HTML",
            )
        except Exception:
            pass

    else:
        if owners_count > 1:
            fail_txt = "Сделка сорвалась, ни один из владельцев этого \"товара\" не захотел отдавать свою часть."
        else:
            fail_txt = "Сделка сорвалась, владелец этого \"товара\" отказался от вашего предложения."
        try:
            bot.send_message(buyer_id, fail_txt)
        except Exception:
            pass

# Game lobby rendering & handlers
def render_lobby(game_id: str) -> Tuple[str, InlineKeyboardMarkup]:
    row = db_one(
        """
        SELECT creator_id, stake_cents, reg_ends_ts, reg_extended,
               COALESCE(game_type,'roulette'),
               COALESCE(stake_kind,'money'),
               COALESCE(life_demon_id,0)
        FROM games WHERE game_id=?
        """,
        (game_id,),
    )
    if not row:
        return "Игра не найдена.", InlineKeyboardMarkup()

    creator_id, stake_cents, reg_ends_ts, reg_extended, game_type, stake_kind, life_demon_id = row
    players = db_all("SELECT user_id, status FROM game_players WHERE game_id=? ORDER BY rowid", (game_id,))

    lines = []
    pending_uids = []
    for uid, status in players:
        u = get_user(int(uid))
        if not u or not u[2]:
            pending_uids.append(int(uid))
            name = "<b>Аноним</b>"
            tail = "в ожидании подтверждения"
        else:
            name = f"<b>{html_escape(u[2])}</b>"
            tail = "готов"
        uname = f" (@{html_escape(u[1])})" if (u and u[1]) else ""
        lines.append(f"• {name}{uname} - {tail}")

    left = max(0, int(reg_ends_ts) - now_ts())
    if game_type == "cross":
        game_title = "⟢♣♦ Марафон рулетка ♥♠⟣"
    elif game_type == "zero":
        game_title = "⟢♣♦ Зеро-рулетка ♥♠⟣"
    else:
        game_title = "⟢♣♦ Рулетка ♥♠⟣"
    
    stake_line = f"Текущая ставка: <b>{cents_to_money_str(int(stake_cents))}</b>$"
    if stake_kind == "life_demon":
        stake_line = (
            f"Текущая ставка: <b>{cents_to_money_str(int(stake_cents))}</b>$"
        )
    
    max_players = 2 if stake_kind == "life_demon" else (4 if game_type == "zero" else (5 if game_type in ("roulette", "cross") else len(players)))
    text = (
        f"Игра выбрана: <b>{game_title}</b>\n"
        f"{stake_line}\n\n"
        f"Участники {len(players)}/{int(max_players)}:\n"
        + "\n".join(lines if lines else ["• (пусто)"])
        + f"\n\nВремя регистрации: {left} секунд"
    )
    kb = InlineKeyboardMarkup()
    if len(players) < int(max_players):
        kb.add(InlineKeyboardButton("Присоединиться к игре", callback_data=f"game:join:{game_id}"))

    for puid in pending_uids:
        kb.add(
            InlineKeyboardButton(
                "Подтвердить",
                url=f"https://t.me/{BOT_USERNAME}?start=confirm_{game_id}_{puid}",
            )
        )

    if reg_extended == 0:
        kb.add(InlineKeyboardButton("Продлить на 30 сек", callback_data=cb_pack(f"game:extend:{game_id}", int(creator_id))))
    kb.add(InlineKeyboardButton("Отменить игру", callback_data=cb_pack(f"game:cancel:{game_id}", int(creator_id))))
    if len(players) >= 2:
        kb.add(InlineKeyboardButton("Продолжить", callback_data=cb_pack(f"game:continue:{game_id}", int(creator_id))))
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
    row = db_one(
        "SELECT state, reg_ends_ts, creator_id, COALESCE(game_type,'roulette'), stake_cents FROM games WHERE game_id=?",
        (game_id,),
    )
    if not row:
        return
    state, reg_ends_ts, creator_id, game_type, stake_cents = row
    creator_id = int(creator_id)

    if state != "lobby":
        return

    left = int(reg_ends_ts) - now_ts()
    if left > 0:
        schedule_lobby_end(game_id, delay=left + 0.5)
        return

    others = db_all("SELECT user_id FROM game_players WHERE game_id=? AND user_id<>?", (game_id, creator_id))
    for (uid,) in others:
        u = get_user(int(uid))
        if not u or not u[2]:
            db_exec("DELETE FROM game_players WHERE game_id=? AND user_id=?", (game_id, int(uid)), commit=True)

    cnt = db_one("SELECT COUNT(*) FROM game_players WHERE game_id=? AND user_id<>?", (game_id, creator_id))
    others_n = int((cnt[0] if cnt else 0) or 0)

    if others_n == 0:
        db_exec("UPDATE games SET state='cancelled' WHERE game_id=?", (game_id,), commit=True)
        edit_game_message(game_id, "Регистрация завершена. Никто не присоединился.\nИгра отменена", reply_markup=None)
        return

    if game_type == "cross":
        r = 1
        rfmt = cross_format_for_round(r)
        db_exec(
            "UPDATE games SET state='playing', roulette_format=?, cross_round=?, turn_index=0 WHERE game_id=?",
            (rfmt, r, game_id),
            commit=True,
        )
        shop_bind_players_for_game(game_id)

        order = turn_order_get(game_id)
        first_uid = int(order[0]) if order else int(creator_id)

        u = get_user(first_uid)
        cname = u[2] if u and u[2] else "Игрок"
        stake_now, add = cross_stake_for_round(int(stake_cents or 0), r)
        title = "1×3" if rfmt == "1x3" else ("3×3" if rfmt == "3x3" else "3×5")
        text = (
            "Выбор сохранён.\n"
            f"Раунд: <b>{r}</b>\n"
            f"Режим {title}\n"
            f"Ставка <b>{cents_to_money_str(stake_now)}</b>$\n"
            "Приятной игры."
        )
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton(f"Ход {cname}", callback_data=cb_pack(f"turn:begin:{game_id}", first_uid)))
        edit_game_message(game_id, text, reply_markup=kb, parse_mode="HTML")
        return
    
    if game_type == "zero":
        db_exec(
            "UPDATE games SET state='playing', turn_index=0 WHERE game_id=?",
            (game_id,),
            commit=True,
        )
        shop_bind_players_for_game(game_id)
        try:
            zero_init_game(game_id)
        except Exception:
            pass
    
        order = turn_order_get(game_id)
        first_uid = order[0] if order else int(creator_id)
        fu = get_user(first_uid)
        first_name = fu[2] if fu and fu[2] else "Игрок"
    
        text = (
            "Выбор сохранён.\n"
            f"⛂⛁ Цена фишки: <b>{cents_to_money_str(int(stake_cents))}</b>$\n"
            "Приятной игры."
        )
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton(f"Ход {first_name}", callback_data=cb_pack(f"zero:begin:{game_id}", first_uid)))
        edit_game_message(game_id, text, reply_markup=kb, parse_mode="HTML")
        return

    db_exec("UPDATE games SET state='choose_format' WHERE game_id=?", (game_id,), commit=True)
    text = (
        "Выберите формат рулетки:\n"
        "Режим ¨Кросс¨ 3 слота (Формат 1×3)\n"
        "Режим ¨Классика¨ 9 слотов (Формат 3×3)\n"
        "Режим 𖤐ĐĒʋÍ£𖤐 15 слотов (Формат 3×5)"
    )
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Кросс 1×3", callback_data=cb_pack(f"rfmt:set:{game_id}:1x3", int(creator_id))))
    kb.add(InlineKeyboardButton("Классика 3×3", callback_data=cb_pack(f"rfmt:set:{game_id}:3x3", int(creator_id))))
    kb.add(InlineKeyboardButton("ĐĒʋÍ£ 3×5", callback_data=cb_pack(f"rfmt:set:{game_id}:3x5", int(creator_id))))
    edit_game_message(game_id, text, reply_markup=kb, parse_mode="HTML")

# TURN ORDER (случайный порядок ходов)
def game_players_list(game_id: str) -> list:
    rows = db_all("SELECT user_id FROM game_players WHERE game_id=? ORDER BY rowid", (str(game_id),))
    return [int(r[0]) for r in rows]

def turn_order_get(game_id: str) -> list:
    """
    Возвращает фиксированный (но случайно сгенерированный) порядок игроков для игры.
    Для марафона (cross) порядок пересоздаётся каждый раунд.
    """
    players = game_players_list(game_id)
    if not players:
        return []

    rr = db_one("SELECT COALESCE(game_type,'roulette'), COALESCE(cross_round,1) FROM games WHERE game_id=?", (str(game_id),))
    game_type = (rr[0] if rr else "roulette") or "roulette"
    cross_round = int((rr[1] if rr else 1) or 1)
    desired_round = cross_round if str(game_type) == "cross" else 0

    row = db_one("SELECT order_csv, COALESCE(round,0) FROM turn_orders WHERE game_id=?", (str(game_id),))
    if row:
        csv = (row[0] or "").strip()
        stored_round = int((row[1] if row else 0) or 0)
        order = []
        if csv:
            for s in csv.split(","):
                s = s.strip()
                if not s:
                    continue
                try:
                    order.append(int(s))
                except Exception:
                    pass

        if stored_round == desired_round and len(order) == len(players) and set(order) == set(players):
            return order

    # Перегенерация
    order = list(players)
    random.shuffle(order)
    db_exec(
        "INSERT INTO turn_orders (game_id, order_csv, round, updated_ts) VALUES (?,?,?,?) "
        "ON CONFLICT(game_id) DO UPDATE SET order_csv=excluded.order_csv, round=excluded.round, updated_ts=excluded.updated_ts",
        (str(game_id), ",".join(str(x) for x in order), int(desired_round), int(now_ts())),
        commit=True
    )
    return order

def turn_order_reset(game_id: str):
    db_exec("DELETE FROM turn_orders WHERE game_id=?", (str(game_id),), commit=True)

# ZERO-ROULETTE
ZERO_RULES_URL = "https://teletype.in/@vers_octava/zero_roulete_gude" # ссылка
ZERO_EMPTY = "ㅤㅤ"
ZERO_RED = { # Цвета чисел
    1,3,5,7,9,12,14,16,18,19,21,23,25,27,30,32,34,36
}

def zero_color(num: int) -> str:
    """R/B/W (W = зеро)."""
    num = int(num)
    if num == 0:
        return "W"
    return "R" if num in ZERO_RED else "B"

def zero_num_label(num: int) -> str:
    num = int(num)
    if num == 0:
        return "Зеро⚪"
    return f"{num}{'🔴' if zero_color(num) == 'R' else '⚫'}"

def zero_code_is_num(code: str) -> bool:
    return bool(code) and (code == "Z" or code.startswith("N"))

def zero_code_to_num(code: str) -> int:
    if code == "Z":
        return 0
    return int(code[1:])

def zero_code_label(code: str) -> str:
    if not code:
        return ""
    if code == "E":
        return "Чётное"
    if code == "O":
        return "Нечётное"
    if code == "R":
        return "Красное"
    if code == "B":
        return "Чёрное"
    if code == "Z":
        return "Зеро⚪"
    if code.startswith("N"):
        try:
            return zero_num_label(int(code[1:]))
        except Exception:
            return code
    return code

ZERO_SEQ_TIER = [33,16,24,5,10,23,8,30,11,36,13,27]
ZERO_SEQ_ORPHELINS = [9,31,14,20,1,6,34,17]
ZERO_SEQ_VOISINS = [22,18,29,7,28,19,4,21,2,25]
ZERO_SEQ_ZERO_SPIEL = [12,35,3,26,0,32,15]

def zero_seq_match(nums: list, seq: list) -> bool:
    """nums == contiguous subseq of seq OR reverse(seq)."""
    n = len(nums)
    if n <= 0:
        return False
    for base in (seq, list(reversed(seq))):
        for i in range(0, len(base) - n + 1):
            if base[i:i+n] == nums:
                return True
    return False

def zero_get_order(game_id: str) -> list:
    return turn_order_get(game_id)

def zero_get_turn_uid(game_id: str) -> int:
    row = db_one("SELECT COALESCE(turn_index,0) FROM games WHERE game_id=?", (str(game_id),))
    turn_index = int((row[0] if row else 0) or 0)
    order = turn_order_get(game_id)
    if not order:
        return 0
    return int(order[turn_index % len(order)])

def zero_get_picks(game_id: str, uid: int) -> list:
    rows = db_all(
        "SELECT slot, code FROM zero_bets WHERE game_id=? AND user_id=? ORDER BY slot",
        (game_id, int(uid))
    )
    codes = []
    for _slot, code in rows:
        codes.append((code or "").strip())
    return codes

def zero_clear_picks(game_id: str, uid: int):
    db_exec("DELETE FROM zero_bets WHERE game_id=? AND user_id=?", (game_id, int(uid)), commit=True)

def zero_set_locked(game_id: str, uid: int, locked: bool):
    db_exec(
        "INSERT INTO zero_lock (game_id, user_id, locked) VALUES (?,?,?) "
        "ON CONFLICT(game_id,user_id) DO UPDATE SET locked=excluded.locked",
        (game_id, int(uid), 1 if locked else 0),
        commit=True
    )

def zero_is_locked(game_id: str, uid: int) -> bool:
    row = db_one("SELECT COALESCE(locked,0) FROM zero_lock WHERE game_id=? AND user_id=?", (game_id, int(uid)))
    return bool(int((row[0] if row else 0) or 0))

def zero_all_locked(game_id: str) -> bool:
    order = zero_get_order(game_id)
    if not order:
        return False
    for uid in order:
        if not zero_is_locked(game_id, uid):
            return False
    return True

def zero_ensure_initialized(game_id: str):
    row = db_one("SELECT 1 FROM zero_state WHERE game_id=?", (game_id,))
    if not row:
        zero_init_game(game_id)

def zero_init_game(game_id: str):
    """Сброс выбора для новой игры/реванша."""
    order = zero_get_order(game_id)

    db_exec("DELETE FROM zero_bets WHERE game_id=?", (game_id,), commit=True)
    db_exec("DELETE FROM zero_lock WHERE game_id=?", (game_id,), commit=True)
    db_exec("DELETE FROM zero_outcomes WHERE game_id=?", (game_id,), commit=True)

    db_exec(
        "INSERT INTO zero_state (game_id, stage, revealed, gen_csv, gen_ts) VALUES (?,?,?,?,?) "
        "ON CONFLICT(game_id) DO UPDATE SET stage=excluded.stage, revealed=0, gen_csv='', gen_ts=0",
        (game_id, "betting", 0, "", 0),
        commit=True
    )

    for uid in order:
        db_exec(
            "INSERT INTO zero_lock (game_id, user_id, locked) VALUES (?,?,0)",
            (game_id, int(uid)),
            commit=True
        )

def zero_add_pick(game_id: str, uid: int, code: str) -> Tuple[bool, str]:
    code = (code or "").strip()
    if not code:
        return False, "Пустой выбор."

    picks = zero_get_picks(game_id, uid)
    if len(picks) >= 5:
        return False, "Максимум 5."

    if code in ("E", "O"):
        if any(c in ("E", "O") for c in picks):
            return False, "Можно выбрать только одно: Чётное/Нечётное."
    if code in ("R", "B"):
        if any(c in ("R", "B") for c in picks):
            return False, "Можно выбрать только одно: Красное/Чёрное."

    if code.startswith("N"):
        try:
            n = int(code[1:])
            if n < 1 or n > 36:
                return False, "Неверное число."
        except Exception:
            return False, "Неверное число."

    slot = len(picks)
    db_exec(
        "INSERT OR REPLACE INTO zero_bets (game_id, user_id, slot, code) VALUES (?,?,?,?)",
        (game_id, int(uid), int(slot), code),
        commit=True
    )
    return True, ""

def zero_parse_gen(game_id: str) -> list:
    row = db_one("SELECT COALESCE(gen_csv,'') FROM zero_state WHERE game_id=?", (game_id,))
    csv = (row[0] if row else "") or ""
    out = []
    for part in csv.split(","):
        part = part.strip()
        if part == "":
            continue
        try:
            out.append(int(part))
        except Exception:
            pass
    return out

def zero_format_cells(codes: list, fill_to: int = 5) -> str:
    parts = []
    for i in range(fill_to):
        if i < len(codes) and codes[i]:
            parts.append(f"[{zero_code_label(codes[i])}]")
        else:
            parts.append(f"[{ZERO_EMPTY}]")
    return "".join(parts)

def zero_format_gen_row(gen_nums: list, revealed: int) -> str:
    parts = []
    for i in range(5):
        if i < revealed and i < len(gen_nums):
            parts.append(f"[{zero_num_label(gen_nums[i])}]")
        else:
            parts.append(f"[{ZERO_EMPTY}]")
    return "".join(parts)

def zero_render_screen(game_id: str) -> Tuple[str, Optional[InlineKeyboardMarkup]]:
    g = db_one(
        "SELECT stake_cents, COALESCE(stake_kind,'money'), COALESCE(turn_index,0), COALESCE(state,'') "
        "FROM games WHERE game_id=?",
        (game_id,)
    )
    if not g:
        return "Игра не найдена.", None
    stake_cents, stake_kind, turn_index, gstate = int(g[0] or 0), (g[1] or "money"), int(g[2] or 0), (g[3] or "")

    z = db_one("SELECT COALESCE(stage,'betting'), COALESCE(revealed,0) FROM zero_state WHERE game_id=?", (game_id,))
    stage = (z[0] if z else "betting") or "betting"
    revealed = int((z[1] if z else 0) or 0)

    order = zero_get_order(game_id)
    if not order:
        return "Нет игроков.", None

    active_uid = int(order[int(turn_index) % len(order)])

    lines = ["<b>⟢♣♦ Зеро-рулетка ♥♠⟣</b>", ""]
    if ZERO_RULES_URL:
        lines.append(f'<a href="{html_escape(ZERO_RULES_URL)}">☙ Правила игры ❧</a>')
    else:
        lines.append("☙ Правила игры ❧")
    lines.append(f"⛂⛁ Цена фишки: <b>{cents_to_money_str(stake_cents)}</b>$")
    lines.append("Игроки:")

    for uid in order:
        u = get_user(uid)
        name = u[2] if u and u[2] else "Игрок"
        uname = u[1] if u and u[1] else ""
        name_html = f"<b>{html_escape(name)}</b>"
        if stage == "betting" and uid == active_uid:
            name_html = f"<b>Ход <u>{html_escape(name)}</u></b>"
        tail = f" (@{html_escape(uname)})" if uname else ""
        picks = zero_get_picks(game_id, uid)
    
        lines.append(f"{name_html}{tail}")
    
        if stage == "betting" and uid == active_uid:
            a = shop_get_active_for_game(int(uid), str(game_id))
            allowed = shop_allowed_items_for_game_type("zero")
            a = {k: v for k, v in (a or {}).items() if k in allowed}
            bl = render_zero_boosts_inline(a)
            if bl:
                lines.append(bl)
    
        lines.append(zero_format_cells(picks, 5))

    if stage == "reveal":
        gen_nums = zero_parse_gen(game_id)
        lines.append("")
        lines.append("Выпавшие на рулетке значения:")
        lines.append(zero_format_gen_row(gen_nums, revealed))
    else:
        lines.append("")
        lines.append("Сделайте свою ставку:")

    kb = None
    if gstate == "playing" and stage == "betting":
        if not zero_is_locked(game_id, active_uid):
            kb = zero_build_keyboard(game_id, active_uid)

    return "\n".join(lines), kb

def zero_build_keyboard(game_id: str, active_uid: int) -> InlineKeyboardMarkup:
    picks = zero_get_picks(game_id, active_uid)
    kb = InlineKeyboardMarkup(row_width=8)

    def add_row(nums):
        row = []
        for n in nums:
            if n == 0:
                txt = "⚪ℤ𝕖𝕣𝕠"
                code = "Z"
            else:
                txt = zero_num_label(n)
                code = f"N{n}"
            row.append(InlineKeyboardButton(txt, callback_data=cb_pack(f"zero:pick:{game_id}:{code}", active_uid)))
        kb.row(*row)

    add_row(list(range(1, 9)))
    add_row(list(range(9, 17)))
    add_row(list(range(17, 25)))
    add_row(list(range(25, 33)))
    add_row([33, 34, 35, 36, 0])

    kb.row(
        InlineKeyboardButton("𝔼𝕍𝔼ℕ", callback_data=cb_pack(f"zero:pick:{game_id}:E", active_uid)),
        InlineKeyboardButton("𝕆𝔻𝔻", callback_data=cb_pack(f"zero:pick:{game_id}:O", active_uid)),
    )
    kb.row(
        InlineKeyboardButton("🟥🟥", callback_data=cb_pack(f"zero:pick:{game_id}:R", active_uid)),
        InlineKeyboardButton("⬛⬛", callback_data=cb_pack(f"zero:pick:{game_id}:B", active_uid)),
    )

    if len(picks) > 0:
        if len(picks) >= 5:
            kb.row(
                InlineKeyboardButton("Ставка", callback_data=cb_pack(f"zero:lock:{game_id}", active_uid)),
                InlineKeyboardButton("Отменить выбор", callback_data=cb_pack(f"zero:cancel:{game_id}", active_uid)),
            )
        else:
            kb.row(
                InlineKeyboardButton("Отменить выбор", callback_data=cb_pack(f"zero:cancel:{game_id}", active_uid))
            )

    return kb

def zero_compute_combo(picks: list, gen_nums: list) -> Tuple[str, int]:
    slots = []
    for code in picks[:5]:
        if zero_code_is_num(code):
            n = zero_code_to_num(code)
            slots.append(n if n in gen_nums else None)
        else:
            slots.append(None)

    best = ("", 1)

    def consider(name: str, mult: int):
        nonlocal best
        mult = int(mult)
        if mult > best[1]:
            best = (name, mult)

    i = 0
    while i < len(slots):
        if slots[i] is None:
            i += 1
            continue
        j = i
        seg = []
        while j < len(slots) and slots[j] is not None:
            seg.append(int(slots[j]))
            j += 1

        L = len(seg)

        if L >= 3:
            ok = True
            for k in range(1, L):
                if abs(seg[k] - seg[k-1]) != 1:
                    ok = False
                    break
            if ok:
                consider("Strit", 2 if L == 3 else (3 if L == 4 else 5))

            ok = True
            if any(x == 0 for x in seg):
                ok = False
            else:
                c0 = zero_color(seg[0])
                for k in range(1, L):
                    if abs(seg[k] - seg[k-1]) != 2 or zero_color(seg[k]) != c0:
                        ok = False
                        break
            if ok:
                consider("Flash", 2 if L == 3 else (3 if L == 4 else 5))

        if L >= 4:
            if zero_seq_match(seg, ZERO_SEQ_TIER):
                consider("Tier", 2 if L == 4 else 3)
            if zero_seq_match(seg, ZERO_SEQ_ORPHELINS):
                consider("Orphelins", 2 if L == 4 else 3)
            if zero_seq_match(seg, ZERO_SEQ_VOISINS):
                consider("Voisins Du Zero", 2 if L == 4 else 3)
            if zero_seq_match(seg, ZERO_SEQ_ZERO_SPIEL):
                if seg and seg[0] == 0:
                    consider("Zero Spiel", 3 if L == 4 else 5)
                else:
                    consider("Zero Spiel", 2 if L == 4 else 3)

        i = j

    return best[0], best[1]

def zero_compute_delta(picks: list, gen_nums: list, stake_cents: int) -> Tuple[int, str, int]:
    stake_cents = int(stake_cents or 0)

    nums_only = [zero_code_to_num(c) for c in picks if zero_code_is_num(c)]
    special_zero = (len(nums_only) > 0 and set(nums_only) == {0})

    if special_zero:
        if 0 in gen_nums:
            return stake_cents * 10, "", 1
        return -stake_cents * 5, "", 1

    delta = 0
    even_cnt = sum(1 for n in gen_nums if n != 0 and (n % 2 == 0))
    odd_cnt = sum(1 for n in gen_nums if n != 0 and (n % 2 == 1))
    red_cnt = sum(1 for n in gen_nums if zero_color(n) == "R")
    black_cnt = sum(1 for n in gen_nums if zero_color(n) == "B")

    for code in picks[:5]:
        if zero_code_is_num(code):
            n = zero_code_to_num(code)
            delta += (stake_cents if n in gen_nums else -stake_cents)
            continue

        if code in ("E", "O") and even_cnt != odd_cnt:
            if code == "E":
                if even_cnt > odd_cnt:
                    delta += (stake_cents * even_cnt + 1) // 2
                else:
                    delta -= (stake_cents * odd_cnt * 3 + 1) // 2
            else:
                if odd_cnt > even_cnt:
                    delta += (stake_cents * odd_cnt + 1) // 2
                else:
                    delta -= (stake_cents * even_cnt * 3 + 1) // 2
            continue

        if code in ("R", "B") and red_cnt != black_cnt:
            if code == "R":
                if red_cnt > black_cnt:
                    delta += (stake_cents * red_cnt + 1) // 2
                else:
                    delta -= (stake_cents * black_cnt * 3 + 1) // 2
            else:
                if black_cnt > red_cnt:
                    delta += (stake_cents * black_cnt + 1) // 2
                else:
                    delta -= (stake_cents * red_cnt * 3 + 1) // 2
            continue

    combo_name, mult = zero_compute_combo(picks, gen_nums)
    if mult > 1:
        delta = int(delta) * int(mult)

    return int(delta), combo_name, int(mult)

# ZERO-ROULETTE: генерация с учётом зеро-фишек
ZERO_LUCKY_PROC_PCT = 25        # шанс срабатывания удачной фишки
ZERO_COLOR_BONUS_PCT = 5        # +% к весам (за каждую активную фишку)

def _weighted_sample_unique(pool: list, weight_fn, k: int) -> list:
    pool = list(pool)
    out = []
    for _ in range(int(k)):
        if not pool:
            break
        weights = []
        for x in pool:
            try:
                w = float(weight_fn(x))
            except Exception:
                w = 0.0
            if w <= 0:
                w = 0.0
            weights.append(w)
        total = sum(weights)
        if total <= 0:
            idx = random.randrange(len(pool))
        else:
            r = random.random() * total
            s = 0.0
            idx = 0
            for i, w in enumerate(weights):
                s += w
                if r <= s:
                    idx = i
                    break
        out.append(pool.pop(idx))
    return out

def _zero_picks_nums(picks: list) -> list:
    nums = []
    for code in (picks or []):
        if zero_code_is_num(code):
            try:
                nums.append(int(zero_code_to_num(code)))
            except Exception:
                pass
    return nums

def zero_generate_numbers(game_id: str) -> list:
    """Генерация 5 уникальных чисел 0..36 с учётом зеро-фишек."""
    stake_row = db_one("SELECT stake_cents FROM games WHERE game_id=?", (str(game_id),))
    stake_cents = int((stake_row[0] if stake_row else 0) or 0)

    order = turn_order_get(game_id)
    if not order:
        return random.sample(list(range(0, 37)), 5)

    # Считаем активные фишки игроков (влияют на ОБЩУЮ генерацию)
    red_cnt = 0
    black_cnt = 0
    lucky_users = []
    for uid in order:
        a = shop_get_active_for_game(int(uid), str(game_id))
        red_cnt += max(0, int(a.get("red_chip", 0) or 0))
        black_cnt += max(0, int(a.get("black_chip", 0) or 0))
        if a.get("lucky_chip", 0) > 0:
            lucky_users.append(int(uid))

    red_mul = 1.0 + (ZERO_COLOR_BONUS_PCT / 100.0) * red_cnt
    black_mul = 1.0 + (ZERO_COLOR_BONUS_PCT / 100.0) * black_cnt

    def w(n: int) -> float:
        n = int(n)
        if n == 0:
            return 1.0
        return red_mul if zero_color(n) == "R" else black_mul

    gen = _weighted_sample_unique(list(range(0, 37)), w, 5)

    # Удачная фишка: с шансом 25% обеспечиваем минимум 2 удачных ставки (по числам)
    if lucky_users:
        random.shuffle(lucky_users)
        gen_set = set(gen)

        for uid in lucky_users:
            if random.randint(1, 100) > ZERO_LUCKY_PROC_PCT:
                continue

            picks = zero_get_picks(game_id, uid)
            nums = _zero_picks_nums(picks)
            if not nums:
                maybe_make_slave_by_shop_trigger(uid, max(0, stake_cents * 2), game_id)
                continue

            hits = sum(1 for n in nums if n in gen_set)
            if hits < 2:
                from collections import Counter
                cnt = Counter(nums)

                cand = [n for n in cnt.keys() if n not in gen_set]
                cand.sort(key=lambda n: cnt[n], reverse=True)

                to_put = []
                for n in cand:
                    if hits >= 2:
                        break
                    to_put.append(int(n))
                    hits += int(cnt[n])

                for n in to_put:
                    if n in gen_set:
                        continue
                    idx = random.randrange(len(gen))
                    for _ in range(10):
                        if gen[idx] != n and gen[idx] not in to_put:
                            break
                        idx = random.randrange(len(gen))
                    gen_set.discard(gen[idx])
                    gen[idx] = int(n)
                    gen_set.add(int(n))

            maybe_make_slave_by_shop_trigger(uid, max(0, stake_cents * 2), game_id)

    return gen

def zero_start_reveal(game_id: str):
    gen = zero_generate_numbers(game_id)
    csv = ",".join(str(x) for x in gen)

    db_exec(
        "UPDATE zero_state SET stage='reveal', revealed=0, gen_csv=?, gen_ts=? WHERE game_id=?",
        (csv, now_ts(), game_id),
        commit=True
    )

    text, _kb = zero_render_screen(game_id)
    edit_game_message(game_id, text, reply_markup=None, parse_mode="HTML")

    zero_schedule_reveal(game_id, 1)

def zero_schedule_reveal(game_id: str, revealed: int):
    def _tick():
        try:
            z = db_one("SELECT COALESCE(stage,'') FROM zero_state WHERE game_id=?", (game_id,))
            if not z or (z[0] or "") != "reveal":
                return

            db_exec(
                "UPDATE zero_state SET revealed=? WHERE game_id=?",
                (int(revealed), game_id),
                commit=True
            )

            text, _kb = zero_render_screen(game_id)
            edit_game_message(game_id, text, reply_markup=None, parse_mode="HTML")

            if int(revealed) < 5:
                zero_schedule_reveal(game_id, int(revealed) + 1)
            else:
                zero_finish_game(game_id)
        except Exception:
            pass

    t = threading.Timer(2.0, _tick)
    t.daemon = True
    t.start()

def zero_finish_game(game_id: str):
    g = db_one("SELECT stake_cents, COALESCE(game_type,'roulette'), COALESCE(state,'') FROM games WHERE game_id=?", (game_id,))
    if not g:
        return
    stake_cents = int(g[0] or 0)
    game_type = (g[1] or "roulette")
    if game_type != "zero":
        return

    gen_nums = zero_parse_gen(game_id)
    order = zero_get_order(game_id)
    if not order:
        return

    for uid in order:
        picks = zero_get_picks(game_id, uid)
        delta, combo_name, mult = zero_compute_delta(picks, gen_nums, stake_cents)

        active = shop_get_active_for_game(uid, game_id)

        insured = (active.get("insurance", 0) > 0) or (active.get("paket", 0) > 0)
        if insured and int(delta) < 0:
            protected_amt = abs(int(delta))
            if active.get("paket", 0) > 0:
                shop_mark_used(uid, game_id, "paket")
                delta = protected_amt
            else:
                shop_mark_used(uid, game_id, "insurance")
                delta = 0
            maybe_make_slave_by_shop_trigger(uid, protected_amt, game_id)

        u = get_user(uid)
        is_demon = (u and int(u[7] or 0) == 1)
        if not is_demon:
            if delta > 0:
                kept = apply_slave_cut(uid, int(delta), reason="zero")
                add_balance(uid, kept)
            else:
                add_balance(uid, int(delta))

        db_exec(
            "INSERT INTO game_results (game_id, user_id, delta_cents, finished) "
            "VALUES (?,?,?,1) "
            "ON CONFLICT(game_id,user_id) DO UPDATE SET delta_cents=excluded.delta_cents, finished=1",
            (game_id, int(uid), int(delta)),
            commit=True
        )

        db_exec(
            "INSERT INTO zero_outcomes (game_id, user_id, combo, mult) VALUES (?,?,?,?) "
            "ON CONFLICT(game_id,user_id) DO UPDATE SET combo=excluded.combo, mult=excluded.mult",
            (game_id, int(uid), combo_name or "", float(mult)),
            commit=True
        )

        db_exec("INSERT OR IGNORE INTO game_stats (user_id) VALUES (?)", (int(uid),), commit=True)
        if int(delta) >= 0:
            db_exec(
                "UPDATE game_stats SET games_total=games_total+1, wins=wins+1, max_win_cents=MAX(max_win_cents, ?) WHERE user_id=?",
                (int(delta), int(uid)),
                commit=True
            )
        else:
            db_exec(
                "UPDATE game_stats SET games_total=games_total+1, losses=losses+1, max_lose_cents=MAX(max_lose_cents, ?) WHERE user_id=?",
                (int(abs(int(delta))), int(uid)),
                commit=True
            )
        bump_game_type_stat(int(uid), "zero")

    db_exec("UPDATE games SET state='finished' WHERE game_id=?", (game_id,), commit=True)

    try:
        for pid in set(order):
            shop_tick_after_game(int(pid), game_id)
    except Exception:
        pass

    apply_demon_life_settlement(game_id)
    update_demon_streak_after_game(game_id)
    emancipate_slaves_after_game(game_id)

    creator_row = db_one("SELECT creator_id FROM games WHERE game_id=?", (game_id,))
    creator_id = int((creator_row[0] if creator_row else 0) or 0)

    totals_text, totals_kb = render_game_totals(game_id, creator_id)
    edit_game_message(game_id, totals_text, reply_markup=totals_kb, parse_mode="HTML")

@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("zero:"))
def on_zero_callbacks(call: CallbackQuery):
    base, owner = cb_unpack(call.data)
    clicker = call.from_user.id

    if owner is not None and clicker != owner:
        bot.answer_callback_query(call.id, "Сейчас не твой ход.", show_alert=True)
        return

    parts = base.split(":")
    if len(parts) < 3:
        bot.answer_callback_query(call.id)
        return

    action = parts[1]
    game_id = parts[2]

    g = db_one("SELECT COALESCE(state,''), COALESCE(game_type,'roulette') FROM games WHERE game_id=?", (game_id,))
    if not g:
        bot.answer_callback_query(call.id, "Игра не найдена.", show_alert=True)
        return
    state, game_type = (g[0] or ""), (g[1] or "roulette")
    if game_type != "zero":
        bot.answer_callback_query(call.id, "Это не зеро-рулетка.", show_alert=True)
        return
    if state != "playing":
        bot.answer_callback_query(call.id, "Сейчас нельзя ходить.", show_alert=True)
        return

    zero_ensure_initialized(game_id)
    current_uid = zero_get_turn_uid(game_id)
    if int(clicker) != int(current_uid):
        bot.answer_callback_query(call.id, "Сейчас ход другого игрока.", show_alert=True)
        return
    if zero_is_locked(game_id, clicker):
        bot.answer_callback_query(call.id, "Вы уже сохранили ставку.", show_alert=True)
        return

    if action == "begin":
        text, kb = zero_render_screen(game_id)
        edit_zero_message(call, text, reply_markup=kb, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        return

    if action == "cancel":
        zero_clear_picks(game_id, clicker)
        text, kb = zero_render_screen(game_id)
        edit_zero_message(call, text, reply_markup=kb, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        return

    if action == "pick":
        if len(parts) < 4:
            bot.answer_callback_query(call.id)
            return
        code = parts[3]
        ok, err = zero_add_pick(game_id, clicker, code)
        if not ok:
            bot.answer_callback_query(call.id, err, show_alert=True)
            return
        text, kb = zero_render_screen(game_id)
        edit_zero_message(call, text, reply_markup=kb, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        return

    if action == "lock":
        picks = zero_get_picks(game_id, clicker)
        if len(picks) < 5:
            bot.answer_callback_query(call.id, "Нужно заполнить все 5 ячеек.", show_alert=True)
            return

        zero_set_locked(game_id, clicker, True)

        if zero_all_locked(game_id):
            bot.answer_callback_query(call.id)
            try:
                zero_start_reveal(game_id)
            except Exception:
                pass
            return

        row = db_one("SELECT COALESCE(turn_index,0) FROM games WHERE game_id=?", (game_id,))
        turn_index = int((row[0] if row else 0) or 0)
        order = zero_get_order(game_id)
        next_index = (turn_index + 1) % len(order) if order else 0
        db_exec("UPDATE games SET turn_index=? WHERE game_id=?", (int(next_index), game_id), commit=True)

        text, kb = zero_render_screen(game_id)
        edit_zero_message(call, text, reply_markup=kb, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        return

    bot.answer_callback_query(call.id)

# Итоги игры
def build_totals_block(game_id: str, creator_id: int) -> str:
    gt = db_one("SELECT COALESCE(game_type,'roulette') FROM games WHERE game_id=?", (game_id,))
    game_type = (gt[0] if gt else "roulette") or "roulette"

    if game_type == "zero":
        gen_nums = zero_parse_gen(game_id)
        gen_row = zero_format_gen_row(gen_nums, 5) if gen_nums else ""

        order = zero_get_order(game_id)

        rows = db_all("""
            SELECT gp.user_id, COALESCE(gr.delta_cents, 0) AS delta
            FROM game_players gp
            LEFT JOIN game_results gr
              ON gr.game_id = gp.game_id AND gr.user_id = gp.user_id
            WHERE gp.game_id=?
        """, (game_id,))
        delta_map = {int(uid): int(delta or 0) for (uid, delta) in rows}

        lines = ["<b>⟢♣♦ Зеро-рулетка ♥♠⟣</b>"]
        if gen_row:
            lines.append("")
            lines.append("Выпавшие на рулетке значения:")
            lines.append(gen_row)

        lines.append("")
        lines.append("⟢♣♦ Итоги игры ♥♠⟣")

        for i, uid in enumerate(order, start=1):
            u = get_user(uid)
            name = u[2] if u and u[2] else "Игрок"
            uname = u[1] if u and u[1] else ""
            tail = f" (@{html_escape(uname)})" if uname else ""
            lines.append(f"{i}. <b>{html_escape(name)}</b>{tail}")

            picks = zero_get_picks(game_id, uid)
            cells = zero_format_cells(picks, 5)

            oc = db_one(
                "SELECT COALESCE(combo,''), COALESCE(mult,1.0) FROM zero_outcomes WHERE game_id=? AND user_id=?",
                (game_id, int(uid))
            )
            combo = (oc[0] if oc else "") or ""
            mult = float((oc[1] if oc else 1.0) or 1.0)
            combo_part = ""
            if combo and mult > 1.01:
                combo_part = f" | {html_escape(combo)} ×{int(round(mult))}"

            delta = delta_map.get(int(uid), 0)
            lines.append(f"{cells}{combo_part} | <b>{cents_to_money_str(int(delta))}</b>$")

        lines.append("")
        lines.append("Хотите отыграться?")
        return "\n".join(lines)

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
    desired_round = 1 if str(game_type) == "cross" else 0
    db_exec(
        "INSERT INTO turn_orders (game_id, order_csv, round, updated_ts) VALUES (?,?,?,?) "
        "ON CONFLICT(game_id) DO UPDATE SET order_csv=excluded.order_csv, round=excluded.round, updated_ts=excluded.updated_ts",
        (str(new_game_id), ",".join(str(x) for x in new_order), int(desired_round), int(now_ts())),
        commit=True
    )
    shop_bind_players_for_game(new_game_id)    
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
    shop_bind_players_for_game(new_game_id)
    
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

    order = turn_order_get(new_game_id)
    first_uid = int(order[0]) if order else int(new_order[0])    
    first_u = get_user(first_uid)
    first_name = first_u[2] if first_u and first_u[2] else "Игрок"

    text = (
        "Выбор сохранён.\n"
        f"Ставка <b>{cents_to_money_str(int(stake_cents))}</b>\n"
        "Приятной игры."
    )
    kb = InlineKeyboardMarkup()
    cb = f"zero:begin:{new_game_id}" if game_type == "zero" else f"turn:begin:{new_game_id}"
    kb.add(InlineKeyboardButton(f"Ход {first_name}", callback_data=cb_pack(cb, first_uid)))

    if game_type == "zero":
        edit_zero_message(call, text, reply_markup=kb, parse_mode="HTML", force_media=True)
    else:
        edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")


def edit_game_message(game_id: str, text: str, reply_markup=None, parse_mode="HTML"):
    row = db_one("SELECT origin_chat_id, origin_message_id, origin_inline_id FROM games WHERE game_id=?", (game_id,))
    if not row:
        return
    chat_id, msg_id, inline_id = row

    g = db_one("SELECT COALESCE(game_type,'roulette'), COALESCE(state,'') FROM games WHERE game_id=?", (game_id,))
    game_type = (g[0] if g else "roulette") or "roulette"
    state = (g[1] if g else "") or ""

    if game_type == "zero" and state != "lobby" and zero_media_enabled():
        try:
            if inline_id:
                bot.edit_message_caption(
                    inline_message_id=inline_id,
                    caption=text,
                    parse_mode=parse_mode,
                    reply_markup=reply_markup
                )
                return
            if chat_id and msg_id:
                bot.edit_message_caption(
                    chat_id=chat_id,
                    message_id=msg_id,
                    caption=text,
                    parse_mode=parse_mode,
                    reply_markup=reply_markup
                )
                return
        except Exception:
            pass

    if inline_id:
        limited_edit_message_text(text=text, inline_id=inline_id, reply_markup=reply_markup, parse_mode=parse_mode)
    elif chat_id and msg_id:
        limited_edit_message_text(text=text, chat_id=chat_id, msg_id=msg_id, reply_markup=reply_markup, parse_mode=parse_mode)

def refresh_lobbies_for_user(uid: int):
    """После регистрации обновляет все лобби, где пользователь находится как 'Аноним'."""
    rows = db_all(
        """
        SELECT gp.game_id
        FROM game_players gp
        JOIN games g ON g.game_id = gp.game_id
        WHERE gp.user_id=? AND g.state='lobby'
        """,
        (int(uid),),
    )
    for (game_id,) in rows:
        db_exec(
            "UPDATE game_players SET status='ready' WHERE game_id=? AND user_id=?",
            (game_id, int(uid)),
            commit=True,
        )
        text, kb = render_lobby(game_id)
        edit_game_message(game_id, text, reply_markup=kb, parse_mode="HTML")

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

    r = db_one(
        "SELECT COALESCE(game_type,'roulette'), COALESCE(stake_kind,'money'), COALESCE(life_demon_id,0) "
        "FROM games WHERE game_id=?",
        (game_id,),
    )
    game_type = (r[0] if r else "roulette") or "roulette"
    stake_kind = (r[1] if r else "money") or "money"
    life_demon_id = int((r[2] if r else 0) or 0)
    
    if stake_kind == "life_demon":
        # максимум 2 игрока: демон + 1 оппонент
        cnt = db_one("SELECT COUNT(*) FROM game_players WHERE game_id=?", (game_id,))
        if cnt and int(cnt[0]) >= 2:
            bot.answer_callback_query(call.id, "Эта игра приватная", show_alert=True)
            return
    
        # demon vs demon: запрещено если ни у кого нет рабов
        u_creator = get_user(int(life_demon_id))
        u_joiner = get_user(uid)
        if u_creator and u_joiner and int(u_creator[7] or 0) == 1 and int(u_joiner[7] or 0) == 1:
            a = db_one("SELECT COUNT(*) FROM slavery WHERE owner_id=?", (int(life_demon_id),))
            b = db_one("SELECT COUNT(*) FROM slavery WHERE owner_id=?", (int(uid),))
            if int((a[0] if a else 0) or 0) == 0 and int((b[0] if b else 0) or 0) == 0:
                bot.answer_callback_query(call.id, "Демоны не могут играть друг с другом без соответствующей ставки", show_alert=True)
                return

    cur.execute("SELECT 1 FROM game_players WHERE game_id=? AND user_id=?", (game_id, uid))
    if cur.fetchone():
        bot.answer_callback_query(call.id, "Ты уже в списке участников.", show_alert=True)
        return
    
    if stake_kind != "life_demon":
        max_players = 4 if game_type == "zero" else (5 if game_type in ("roulette", "cross") else 0)
        if max_players:
            cnt = db_one("SELECT COUNT(*) FROM game_players WHERE game_id=?", (game_id,))
            if int((cnt[0] if cnt else 0) or 0) >= int(max_players):
                bot.answer_callback_query(call.id, "Лобби заполнено.", show_alert=True)
                return

    u = get_user(uid)
    if not u or not u[2]:
        try:
            bot.send_message(uid, "Куратор позволяет вам принять приглашение в игру.\n Однако, вам необходимо принять приглашение. Пропишите @casino_rpg_bot, Вас ожидает награда.")
        except Exception:
            pass
        db_exec(
            "INSERT OR IGNORE INTO game_players (game_id, user_id, status) VALUES (?,?,?)",
            (game_id, uid, "pending"),
            commit=True,
        )
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


        order = turn_order_get(game_id)
        first_uid = int(order[0]) if order else int(creator_id)

        u = get_user(first_uid)
        cname = u[2] if u and u[2] else "Игрок"
        stake_now, add = cross_stake_for_round(int(stake_cents or 0), r)
        title = "1×3" if rfmt == "1x3" else ("3×3" if rfmt == "3x3" else "3×5")
        text = (
            "Выбор сохранён.\n"
            f"Раунд: <b>{r}</b>\n"
            f"Режим {title}\n"
            f"Ставка <b>{cents_to_money_str(stake_now)}</b>$\n"
            "Приятной игры."
        )
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton(f"Ход {cname}", callback_data=cb_pack(f"turn:begin:{game_id}", first_uid)))
        edit_inline_or_message(call, text, reply_markup=kb, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        return
    
    if game_type == "zero":
        cur.execute("SELECT stake_cents FROM games WHERE game_id=?", (game_id,))
        stake_cents = int((cur.fetchone() or (0,))[0] or 0)
    
        cur.execute("UPDATE games SET state='playing', turn_index=0 WHERE game_id=?", (game_id,))
        conn.commit()
        shop_bind_players_for_game(game_id)
        try:
            zero_init_game(game_id)
        except Exception:
            pass
    
        order = turn_order_get(game_id)
        first_uid = order[0] if order else int(creator_id)
        fu = get_user(first_uid)
        first_name = fu[2] if fu and fu[2] else "Игрок"
    
        text = (
            "Выбор сохранён.\n"
            f"⛂⛁ Цена фишки: <b>{cents_to_money_str(int(stake_cents))}</b>$\n"
            "Приятной игры."
        )
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton(f"Ход {first_name}", callback_data=cb_pack(f"zero:begin:{game_id}", first_uid)))
        edit_zero_message(call, text, reply_markup=kb, parse_mode="HTML", force_media=True)
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
    shop_bind_players_for_game(game_id)

    order = turn_order_get(game_id)
    first_uid = int(order[0]) if order else int(creator_id)
    fu = get_user(first_uid)
    first_name = fu[2] if fu and fu[2] else "Игрок"

    text = f"Выбор сохранён.\nСтавка <b>{cents_to_money_str(int(stake_cents))}</b>\nПриятной игры."
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton(f"Ход {first_name}", callback_data=cb_pack(f"turn:begin:{game_id}", first_uid)))

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

    order = turn_order_get(game_id)
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

    srow = db_one("SELECT COALESCE (stage, 'ready'), msg_chat_id, msg_id, inline_id FROM spins WHERE game_id=? AND user_id=?", (game_id, uid))
    if not srow:
        bot.answer_callback_query(call.id, "Этот ход не активен.", show_alert=True)
        return
    stage, msg_chat_id, msg_id, inline_id = srow
    if stage != "ready":
        bot.answer_callback_query(call.id, "Рулетка уже крутится. Бот прогружает её. Подождите.", show_alert=True)
        return
    
    db_exec("UPDATE spins SET stage='spinning' WHERE game_id=? AND user_id=?", (game_id, uid), commit=True)
    
    def _edit(text: str, kb=None):
        if inline_id:
            limited_edit_message_text(text=text, inline_id=inline_id, reply_markup=kb, parse_mode="HTML")
        else:
            limited_edit_message_text(text=text, chat_id=msg_chat_id, msg_id=msg_id, reply_markup=kb, parse_mode="HTML")

    def run_spin():
        try:
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
                    
            # Сначала узнаём активные усиления (чтобы страховка могла отключить негативные эффекты)
            active = shop_get_active_for_game(uid, game_id)
            print("DEBUG boosts:", uid, game_id, active)
            pepper_on = active.get("devil_pepper", 0) > 0
            active_for_display = dict(active)
            boosts_line = render_active_boosts_line(pname, active_for_display)
            boosts_block = (boosts_line + "\n\n") if boosts_line else ""

            if pepper_on: delta = int(delta) * 2

            # Применение страховки или пакета
            insured = (active.get("insurance", 0) > 0) or (active.get("paket", 0) > 0)
            insurance_triggered = False
            chip_triggered = False

            if insured and int(delta) < 0:
                protected_amt = abs(int(delta))

                # Приоритет: пакет превращает минус в плюс
                if active.get("paket", 0) > 0:
                    chip_triggered = True
                    shop_mark_used(uid, game_id, "paket")
                    delta = protected_amt
                else:
                    insurance_triggered = True
                    shop_mark_used(uid, game_id, "insurance")
                    delta = 0

                # Общий шанс рабства
                maybe_make_slave_by_shop_trigger(uid, protected_amt, game_id)
            
            # Для отображения усилений в тексте результата
            active_for_display = dict(active)
            boosts_line = render_active_boosts_line(pname, active_for_display)
            boosts_block = (boosts_line + "\n\n") if boosts_line else ""

            # Негативные "черепные долги" применяем только если НЕТ страховки
            if not insured:
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
                            set_slave_buyout(uid, abs(int(final_balance)) * 100) # назначение цены рабу
                    
            # Дьявольский перец
            if pepper_on and pepper_triggers_demon(final_state, rfmt):
                rr_pep = db_one("SELECT user_id FROM users WHERE demon=1 ORDER BY RANDOM() LIMIT 1")
                if rr_pep:
                    demon_id = int(rr_pep[0])
                    slavery_add_owner(uid, demon_id, 6000)
            
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

            order = turn_order_get(game_id)
            if not order:
                return
                    
            if (not is_demon) and (pstatus == "life") and (delta < 0) and creator_id:
                set_slave_buyout(uid, abs(delta) * 100) # назначение цены рабу
                owner_id = pick_life_owner(game_id, int(uid), int(creator_id) if creator_id else None)
                if owner_id and int(owner_id) != int(uid):
                    db_exec("INSERT OR IGNORE INTO slave_meta (slave_id) VALUES (?)", (int(uid),), commit=True)
                    db_exec("UPDATE slave_meta SET strikes=strikes+1 WHERE slave_id=?", (int(uid),), commit=True)
                    existed = db_one("SELECT 1 FROM slavery WHERE slave_id=? AND owner_id=?", (int(uid), int(owner_id)))
                    db_exec(
                        "INSERT OR REPLACE INTO slavery (slave_id, owner_id, share_bp) VALUES (?,?,?)",
                        (int(uid), int(owner_id), 6000), commit=True
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
                stake_line = "Ставка: <b>1000$</b>"
            else:
                stake_line = f"Ставка: <b>{cents_to_money_str(int(stake_now))}</b>$"
                if game_type == "cross":
                    stake_line += f" + <b>{cents_to_money_str(int(add_cents))}</b>$"
                
            if game_type == "cross" and is_round_last and int(cross_round) < 9:
                next_round = int(cross_round) + 1
                next_fmt = cross_format_for_round(next_round)
                db_exec("UPDATE games SET cross_round=?, roulette_format=?, turn_index=0 WHERE game_id=?",
                                    (next_round, next_fmt, game_id), commit=True)
                
                order = turn_order_get(game_id)
                next_uid = int(order[0]) if order else int(uid)

                next_user = get_user(next_uid)
                next_name = next_user[2] if next_user and next_user[2] else "Игрок"
                kb = InlineKeyboardMarkup()
                kb.add(InlineKeyboardButton(f"Ход {next_name}", callback_data=cb_pack(f"turn:begin:{game_id}", next_uid)))
                
                final_text = (
                    (f"<b>{header}</b>\n" + round_line + f"<b>Режим {title}</b>\n\n")
                    + f"{final_grid}\n\n"
                    + f"{result_line}\n"
                    + f"{stake_line}\n\n"
                    + boosts_block
                    + f"Следующий раунд: <b>{next_round}</b>"
                )
                _edit(final_text, kb=kb)
                
            elif is_round_last:
                db_exec("UPDATE games SET state='finished' WHERE game_id=?", (game_id,), commit=True)
                try:
                    for pid in set(order):
                        shop_tick_after_game(int(pid), game_id)
                except Exception:
                    pass
            
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
                    + boosts_block
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
                    + f"{stake_line}\n\n"
                    + boosts_block
                )
                _edit(text, kb=kb)
        
        except Exception as e:
            try:
                print("run_spin crashed:", repr(e))
            except Exception:
                pass
            send_error_report(f"run_spin game_id={game_id} uid={uid}", e)
        finally:
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

    cur.execute("SELECT state, stake_cents, creator_id, COALESCE(game_type,'roulette') FROM games WHERE game_id=?", (game_id,))
    g = cur.fetchone()
    if not g:
        bot.answer_callback_query(call.id, "Игра не найдена.", show_alert=True)
        return

    state, stake_cents, creator_id, game_type = g
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

        order = turn_order_get(game_id)
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
            cb = f"zero:begin:{game_id}" if game_type == "zero" else f"turn:begin:{game_id}"
            kb.add(InlineKeyboardButton(f"Ход {first_name}", callback_data=cb_pack(cb, first_uid)))
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

def build_profile_summary_text(view_uid: int) -> Optional[str]:
    u = get_user(int(view_uid))
    if not u or not u[2]:
        return None

    cur.execute("SELECT user_id FROM users WHERE demon=0")
    uids = [int(r[0]) for r in cur.fetchall()]
    uids.sort(key=lambda x: top_value_cents(x), reverse=True)

    place = (uids.index(int(view_uid)) + 1) if (int(u[7] or 0) == 0 and int(view_uid) in uids) else "-"
    status = compute_status(int(view_uid))

    base = (
        f"Имя пользователя: <i>{html_escape(u[2])}</i>\n"
        f"Дата подписания контракта: <b>{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(u[4] or u[3] or now_ts()))}</b>\n"
        f"Статус: <b>{html_escape(status)}</b>\n"
        f"Капитал: <b>{cents_to_money_str(int(u[5] or 0))}</b>$\n"
        f"Место в топе: <b>{place}</b>"
    )
    if int(view_uid) == int(OWNER_ID):
        base += f"\n\nСостояние бота: <b>{html_escape(bot_status_human())}</b>"

    sleeping = bool(_FORCE_SLEEPING)
    if not sleeping:
        try:
            sleeping, _mode, _reason, _last_err = get_bot_sleep_state()
        except Exception:
            sleeping = False
    if sleeping:
        base += "\n\n<b>⛔ Бот временно отключён. Игры и остальные функции недоступны.</b>"

    return base    

def build_profile_open_kb(uid: int) -> InlineKeyboardMarkup:
    uid = int(uid)

    sleeping = bool(_FORCE_SLEEPING)
    if not sleeping:
        try:
            sleeping, _mode, _reason, _last_err = get_bot_sleep_state()
        except Exception:
            sleeping = False

    kb = InlineKeyboardMarkup()

    if sleeping:
        if is_bot_admin(uid):
            kb.add(InlineKeyboardButton("Команды", callback_data=cb_pack("profile:commands", uid)))
        return kb

    kb.add(InlineKeyboardButton("Статистика по играм", callback_data=cb_pack("profile:games", uid)))
    kb.add(InlineKeyboardButton("Контракт", callback_data=cb_pack("profile:contract", uid)))
    if is_bot_admin(uid):
        kb.add(InlineKeyboardButton("Команды", callback_data=cb_pack("profile:commands", uid)))
    if credit_has_active(uid):
        kb.add(InlineKeyboardButton("Договор по кредиту", callback_data=cb_pack("profile:credit", uid)))
    if has_work_history(uid):
        kb.add(InlineKeyboardButton("Трудовая книга", callback_data=cb_pack("profile:workbook", uid)))
    if owns_slaves(uid):
        kb.add(InlineKeyboardButton("Список рабов", callback_data=cb_pack("profile:slaves", uid)))
    if is_slave(uid):
        kb.add(InlineKeyboardButton("Статус раба", callback_data=cb_pack("profile:slave_status", uid)))

    return kb

def slavery_add_owner(slave_id: int, owner_id: int, share_bp: int = 6000) -> bool:
    """
    Потокобезопасная версия: только db_one/db_exec/db_all.
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

    existed = db_one(
        "SELECT 1 FROM slavery WHERE slave_id=? AND owner_id=? LIMIT 1",
        (slave_id, owner_id)
    ) is not None

    ts = now_ts()

    rc, _ = db_exec(
        "INSERT OR IGNORE INTO slavery (slave_id, owner_id, share_bp, acquired_ts) VALUES (?,?,?,?)",
        (slave_id, owner_id, share_bp, ts),
        commit=True
    )
    db_exec(
        "UPDATE slavery SET acquired_ts=? WHERE slave_id=? AND owner_id=? AND (acquired_ts IS NULL OR acquired_ts=0)",
        (ts, slave_id, owner_id),
        commit=True
    )

    inserted = (rc or 0) > 0
    return inserted and (not existed)

def slave_profit_lasth(slave_id: int, owner_id: int) -> int:
    """Сумма выплат от раба владельцу за последние часы."""
    ts0 = now_ts() - 4 * 3600 # время последней выплаты
    row = db_one(
        "SELECT COALESCE(SUM(amount_cents),0) FROM slave_earn_log WHERE slave_id=? AND owner_id=? AND ts>=?",
        (int(slave_id), int(owner_id), int(ts0))
    )
    return int((row[0] if row else 0) or 0)

def slave_last_credit(slave_id: int, owner_id: int) -> Optional[int]:
    """
    Последнее зачисление (в центах), которое этот раб перечислил конкретному владельцу.
    Если начислений не было — None.
    """
    row = db_one(
        "SELECT amount_cents FROM slave_earn_log "
        "WHERE slave_id=? AND owner_id=? "
        "ORDER BY ts DESC LIMIT 1",
        (int(slave_id), int(owner_id))
    )
    if not row:
        return None
    return int(row[0] or 0)

def apply_slave_cut(slave_id: int, income_cents: int, reason: str = "") -> int:
    """
    Потокобезопасная версия.
    Если пользователь раб — удерживаем доли share_bp и раздаём владельцам.
    Возвращает income_cents ПОСЛЕ удержания.
    """
    income_cents = int(income_cents or 0)
    if income_cents <= 0:
        return income_cents

    owners = db_all(
        "SELECT owner_id, share_bp FROM slavery WHERE slave_id=? ORDER BY share_bp DESC",
        (int(slave_id),)
    )
    if not owners:
        return income_cents

    kept = income_cents
    ts = now_ts()

    for owner_id, share_bp in owners:
        owner_id = int(owner_id or 0)
        share_bp = int(share_bp or 0)
        if owner_id <= 0 or share_bp <= 0:
            continue

        part = int(income_cents * share_bp / 10000)
        if part <= 0:
            continue

        kept -= part

        add_balance(owner_id, part)

        db_exec(
            "INSERT INTO slave_earn_log (slave_id, owner_id, ts, amount_cents) VALUES (?,?,?,?)",
            (int(slave_id), int(owner_id), int(ts), int(part)),
            commit=True
        )
        db_exec(
            "UPDATE slavery SET earned_cents=COALESCE(earned_cents,0)+? WHERE slave_id=? AND owner_id=?",
            (int(part), int(slave_id), int(owner_id)),
            commit=True
        )
    
    return kept

def set_slave_buyout(slave_id: int, buyout_cents: int):
    """Сумма выкупа раба (в центах)."""
    buyout_cents = int(buyout_cents or 0)
    if buyout_cents < 0:
        buyout_cents = -buyout_cents
    db_exec("INSERT OR IGNORE INTO slave_meta (slave_id) VALUES (?)", (int(slave_id),), commit=True)
    db_exec(
        "UPDATE slave_meta SET buyout_cents=? WHERE slave_id=?",
        (int(buyout_cents), int(slave_id)),
        commit=True
    )

def clear_slave_buyout(slave_id: int):
    db_exec("INSERT OR IGNORE INTO slave_meta (slave_id) VALUES (?)", (int(slave_id),), commit=True)
    db_exec(
        "UPDATE slave_meta SET buyout_cents=0 WHERE slave_id=?",
        (int(slave_id),),
        commit=True
    )

def _ensure_slave_meta_row(uid: int):
    db_exec("INSERT OR IGNORE INTO slave_meta (slave_id) VALUES (?)", (int(uid),), commit=True)

def get_life_uses(uid: int) -> int:
    _ensure_slave_meta_row(uid)
    row = db_one("SELECT life_uses FROM slave_meta WHERE slave_id=?", (int(uid),))
    return int((row[0] if row else 0) or 0)

def get_life_remaining(uid: int) -> int:
    used = get_life_uses(uid)
    rem = MAX_LIFE_STAKES - used
    return rem if rem > 0 else 0

def inc_life_uses(uid: int):
    _ensure_slave_meta_row(uid)
    db_exec(
        "UPDATE slave_meta SET life_uses=COALESCE(life_uses,0)+1 WHERE slave_id=?",
        (int(uid),),
        commit=True
    )

def get_slave_owners(slave_id: int):
    rows = db_all(
        "SELECT owner_id, share_bp FROM slavery WHERE slave_id=? ORDER BY share_bp DESC",
        (int(slave_id),)
    )
    return [(int(o), int(bp or 0)) for (o, bp) in rows]

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
        notify_safe(oid, f"ℹРаб {s_line} освободился. {html_escape(reason)}")

    if owners:
        notify_safe(slave_id, f"Ты освобождён от статуса раба. {html_escape(reason)}")
    try:
        slave_risk_reset(slave_id)
    except Exception:
        pass

def owner_free_slave_with_reward(owner_id: int, slave_id: int) -> Tuple[bool, int]:
    """
    Добровольное освобождение раба владельцем:
    - полностью снимает с пользователя статус раба
    - начисляет инициатору 10% от текущей суммы выкупа
    Возвращает (ok, reward_cents)
    """
    owner_id = int(owner_id)
    slave_id = int(slave_id)

    row = db_one(
        "SELECT 1 FROM slavery WHERE slave_id=? AND owner_id=? LIMIT 1",
        (slave_id, owner_id)
    )
    if not row:
        return False, 0

    _ensure_slave_meta_row(slave_id)
    meta = db_one(
        "SELECT COALESCE(buyout_cents,0) FROM slave_meta WHERE slave_id=?",
        (slave_id,)
    )
    buyout_cents = int((meta[0] if meta else 0) or 0)
    reward_cents = max(0, buyout_cents // 10)

    if reward_cents > 0:
        add_balance(owner_id, reward_cents)

    free_slave_fully(slave_id, "Владелец добровольно освободил раба.")
    return True, reward_cents


def build_rabs_list_text_kb(owner_id: int, viewer_id: int) -> tuple[str, Optional[InlineKeyboardMarkup]]:
    owner_id = int(owner_id)
    viewer_id = int(viewer_id)

    rr = db_one(
        "SELECT short_name, username FROM users WHERE user_id=?",
        (owner_id,)
    )
    owner_name = (rr[0] if rr else None) or "Без имени"
    owner_username = (rr[1] if rr else "") or ""

    rows = db_all("""
        SELECT slave_id, COALESCE(earned_cents,0), COALESCE(share_bp,0), COALESCE(acquired_ts,0)
        FROM slavery
        WHERE owner_id=?
        ORDER BY COALESCE(earned_cents,0) DESC
    """, (owner_id,)) or []

    head_owner_un = f" (@{html_escape(owner_username)})" if owner_username else ""
    intro = f"Список рабов пользователя <b>{html_escape(owner_name)}</b>{head_owner_un}"
    intro2 = "\n\nЧтобы приобрести раба, используйте /buyrab"

    if not rows:
        return intro + "\nПусто", None

    lines = [intro, "", "Имя|Общий доход|За последнее время|Последнее зачисление"]
    top = rows[:20]

    kb = InlineKeyboardMarkup()
    slave_buttons = []

    for i, (slave_id, earned_cents, _share_bp, _acquired_ts) in enumerate(top, 1):
        slave_id = int(slave_id)
        earned_cents = int(earned_cents or 0)
        lasth = int(slave_profit_lasth(slave_id, owner_id) or 0)
        lastp = int(slave_last_credit(slave_id, owner_id) or 0)

        sr = db_one("SELECT short_name, username FROM users WHERE user_id=?", (slave_id,))
        sname = (sr[0] if sr else None) or "Без имени"
        sun = (sr[1] if sr else "") or ""

        uname_part = f" (@{html_escape(sun)})" if sun else ""
        lines.append(
            f"{i}|<b>{html_escape(sname)}</b>{uname_part} "
            f"<u><b>{cents_to_money_str(earned_cents)}</b>$</u>"
            f"(<b>{cents_to_money_str(lasth)}</b>$) "
            f"+ <b>{cents_to_money_str(lastp)}</b>$"
        )

        btn_text = sname
        if len(btn_text) > 18:
            btn_text = btn_text[:18] + "…"

        slave_buttons.append(
            InlineKeyboardButton(
                btn_text,
                callback_data=cb_pack(f"profile:rabsview:{owner_id}:{slave_id}", viewer_id)
            )
        )

    for i in range(0, len(slave_buttons), 3):
        kb.row(*slave_buttons[i:i + 3])

    return "\n".join(lines) + intro2, kb

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

    # демон проиграл обычному: перевод % капитала + письмо
    if l_is_demon and (not w_is_demon):
        demon_bal = get_balance_cents(loser_id)
        payout = demon_bal // 4 # % капитала
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

    # демон победил обычного: забирает душy 
    if w_is_demon and (not l_is_demon):
        inserted = slavery_add_owner(loser_id, winner_id, 6000)
        demon_bal = get_balance_cents(winner_id)
        set_slave_buyout(loser_id, int(demon_bal) * 25) # цена выкупа

        if inserted:
            try:
                un = l[3] if l else ""
                uname = f" (@{un})" if un else ""
                bot.send_message(
                    loser_id,
                    f"Ты проиграл свою свободу. С этого момента ты личная собственность <b>{html_escape(w[2] or 'Демон')}</b>{uname}",
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

        lines = ["⟢♣♦ Добыча демона ♥♠⟣", "", "Выбери свою награду:"]
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
    if message.chat.type != "private":
        return
    if not is_bot_admin(message.from_user.id):
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
    bot.reply_to(message, "Статус \"Демон\" установлен.")

def _work_daemon():
    while True:
        try:
            cur.execute("SELECT user_id FROM work_shift WHERE ends_ts <= ?", (now_ts(),))
            uids = [int(r[0]) for r in cur.fetchall()]
            for uid in uids:
                finish_shift(uid)
        except Exception:
            send_error_report("_work_daemon")
        time.sleep(2)

# Димоны
threading.Thread(target=_work_daemon, daemon=True).start()
threading.Thread(target=_mail_daemon, daemon=True).start()
threading.Thread(target=_pm_autodelete_daemon, daemon=True).start()

@bot.message_handler(commands=["human"])
def cmd_human(message):
    if message.chat.type != "private":
        return
    if not is_bot_admin(message.from_user.id):
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
    bot.reply_to(message, "Статус \"Демон\" снят, профиль откатан.")

@bot.message_handler(commands=["finance"])
def cmd_finance(message):
    if not is_bot_admin(message.from_user.id):
        return
    if message.chat.type != "private":
        return

    raw = message.text or ""
    lines = raw.split("\n")
    head = (lines[0] or "").strip()
    comment = "\n".join(lines[1:]).strip()
    parts = head.split()

    mode = None
    uname = ""
    amt_token = ""

    if len(parts) >= 3 and parts[1].startswith("@"):
        mode = "single"
        uname = parts[1][1:]
        amt_token = parts[2]
    elif len(parts) >= 2:
        mode = "all"
        amt_token = parts[1]
    else:
        bot.reply_to(
            message,
            "Использование:\n"
            "/finance @username сумма\n"
            "или\n"
            "/finance сумма\n"
            "<комментарий (необязательно)>"
        )
        return

    amt = money_to_cents(amt_token)
    if amt is None:
        bot.reply_to(message, "Неверная сумма.")
        return

    payload = base64.urlsafe_b64encode((comment or "").encode("utf-8")).decode("ascii")

    if mode == "single":
        r = db_one("SELECT user_id FROM users WHERE username=?", (uname,))
        if not r:
            bot.reply_to(message, "Пользователь не найден в базе.")
            return

        uid = int(r[0])

        try:
            if user_pm_notifications_enabled(uid):
                ensure_daily_mail_row(uid)
                _send_mail_prompt(uid, f"owner_finance|{payload}", int(amt))
                bot.reply_to(
                    message,
                    f"Письмо отправлено пользователю @{uname} с суммой в размере {cents_to_money_str(amt)}$"
                )
            else:
                add_balance(uid, int(amt))
                bot.reply_to(
                    message,
                    f"Пользователю @{uname} сразу зачислено {cents_to_money_str(amt)}$ "
                    f"(уведомления в ЛС отключены)."
                )
        except Exception:
            bot.reply_to(message, "Не удалось выполнить перевод.")
        return

    rows = db_all(
        "SELECT user_id FROM users WHERE COALESCE(contract_ts,0) > 0 ORDER BY user_id"
    )
    if not rows:
        bot.reply_to(message, "В базе нет зарегистрированных пользователей для рассылки.")
        return

    mailed = 0
    instant = 0
    failed = 0

    for (uid,) in rows:
        uid = int(uid)
        try:
            if user_pm_notifications_enabled(uid):
                ensure_daily_mail_row(uid)
                _send_mail_prompt(uid, f"owner_finance|{payload}", int(amt))
                mailed += 1
            else:
                add_balance(uid, int(amt))
                instant += 1
        except Exception:
            failed += 1

    bot.reply_to(
        message,
        "Массовая рассылка завершена.\n"
        f"Писем отправлено: {mailed}\n"
        f"Сразу зачислено без ЛС: {instant}\n"
        f"Ошибок: {failed}\n"
        f"Сумма каждому: {cents_to_money_str(amt)}$"
    )

@bot.message_handler(commands=["take"])
def cmd_take(message):
    if not is_bot_admin(message.from_user.id):
        return
    if message.chat.type != "private":
        return

    parts = (message.text or "").split()
    if len(parts) < 3 or not parts[1].startswith("@"):
        bot.reply_to(message, "Использование: /take @username сумма")
        return

    uname = parts[1][1:]
    amt = money_to_cents(parts[2])
    if amt is None:
        bot.reply_to(message, "Неверная сумма.")
        return
    if amt < 0:
        amt = -amt

    r = db_one("SELECT user_id FROM users WHERE username=?", (uname,))
    if not r:
        bot.reply_to(message, "Пользователь не найден в базе.")
        return

    uid = int(r[0])
    add_balance(uid, -amt)

    bot.reply_to(message, f"Списано {cents_to_money_str(amt)}$ у пользователя @{uname}")

@bot.message_handler(commands=["addstatus"])
def cmd_addstatus(message):
    if not is_bot_admin(message.from_user.id):
        return
    if message.chat.type != "private":
        return

    raw = (message.text or "").strip()
    parts = raw.split(maxsplit=2)
    if len(parts) < 3:
        bot.reply_to(message, "Использование: /addstatus @username текст_статуса")
        return

    target = parts[1].strip()
    status_txt = parts[2].strip()

    if not target.startswith("@"):
        bot.reply_to(message, "Использование: /addstatus @username текст_статуса")
        return

    uname = target[1:].strip()
    rr = db_one("SELECT user_id FROM users WHERE username=? COLLATE NOCASE", (uname,))
    if not rr:
        bot.reply_to(message, "Пользователь не найден в базе.")
        return

    uid = int(rr[0])
    if add_custom_status(uid, status_txt):
        bot.reply_to(message, f"Готово. Пользователю @{uname} добавлен статус: {status_txt}")
    else:
        bot.reply_to(message, "Пустой статус не добавлен.")

@bot.message_handler(commands=["reg"])
def cmd_reg(message):
    if not is_bot_admin(message.from_user.id):
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
            f"В вашем почтовом ящике лежало письмо контракта с заполненой строкой имени: <b>{html_escape(name)}</b>",
            parse_mode="HTML"
        )
    except Exception:
        pass

@bot.message_handler(commands=["work"])
def cmd_work(message):
    if not is_bot_admin(message.from_user.id):
        return
    if message.chat.type != "private":
        return

    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 2 or not parts[1].startswith("@"):
        bot.reply_to(message, "Использование: /work @username [вакансия]")
        return

    uname = parts[1][1:].strip()
    job_query = parts[2].strip() if len(parts) >= 3 else ""

    r = db_one("SELECT user_id FROM users WHERE username=? COLLATE NOCASE", (uname,))
    if not r:
        bot.reply_to(message, "Пользователь не найден в базе.")
        return
    uid = int(r[0])

    u = get_user(uid)
    if not u or not u[2]:
        bot.reply_to(message, "У пользователя нет анкеты (не введено имя).")
        return

    cur_shift = get_current_shift(uid)
    if cur_shift:
        ends_ts0 = int(cur_shift[3] or 0)
        if ends_ts0 > now_ts():
            bot.reply_to(message, f"Пользователь уже работает. Вернётся через {_format_duration(max(0, ends_ts0 - now_ts()))}.")
            return
        db_exec("DELETE FROM work_shift WHERE user_id=?", (uid,), commit=True)

    jobs = load_jobs()
    if not jobs:
        bot.reply_to(message, "Список вакансий пуст (файл работ не загружен).")
        return

    job_key: Optional[str] = None
    if job_query:
        q = job_query.strip().lower()
        qn = _normalize_job_key(job_query)

        if q in jobs:
            job_key = q
        elif qn in jobs:
            job_key = qn
        else:
            for jk, jb in jobs.items():
                title_l = (jb.title or "").lower()
                if q in title_l or qn == _normalize_job_key(jb.title):
                    job_key = jk
                    break

        if not job_key:
            lst = "\n".join([f"• {j.title} (ключ: {k})" for k, j in jobs.items()])
            bot.reply_to(message, "Вакансия не найдена. Доступные вакансии:\n" + lst)
            return
    else:
        job_key = list(jobs.keys())[0]

    ends_ts, _salary_full = start_shift(uid, job_key)

    bot.reply_to(message, f"Пользователь @{uname} отправлен на работу: {jobs[job_key].title} (до {time.strftime('%H:%M:%S', time.localtime(ends_ts))})")

    try:
        bot.send_message(
            uid,
            f"Вас \"добровольно\" отправили на работу по вокансии <b>{html_escape(jobs[job_key].title)}</b>\nВернётесь через {_format_duration(max(0, ends_ts - now_ts()))}.",
            parse_mode="HTML"
        )
    except Exception:
        pass

@bot.message_handler(commands=["delrab"])
def cmd_delstat(message):
    if not is_bot_admin(message.from_user.id):
        return

    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].startswith("@"):
        bot.reply_to(message, "Использование: /delrab @username")
        return

    uname = parts[1][1:].strip()
    rr = db_one("SELECT user_id FROM users WHERE username=? COLLATE NOCASE", (uname,))
    if not rr:
        bot.reply_to(message, "Пользователь не найден в базе.")
        return

    target_id = int(rr[0])
    if not is_slave(target_id):
        bot.reply_to(message, "У пользователя нет статуса раба.")
        return

    free_slave_fully(target_id, "Администратор снял статус раба")
    bot.reply_to(message, f"Готово. Статус раба снят с @{uname}.")

@bot.message_handler(commands=["blockcash"])
def cmd_blockcash(message):
    if not is_bot_admin(message.from_user.id):
        return

    raw = (message.text or "").strip()
    parts = raw.split(maxsplit=2)

    target_id: Optional[int] = None
    dur_spec = ""

    if len(parts) >= 2 and (parts[1].startswith("@") or parts[1].isdigit()):
        target_id = resolve_user_id_ref(parts[1].strip())
        dur_spec = parts[2].strip() if len(parts) >= 3 else ""
    elif message.reply_to_message and len(parts) >= 2:
        target_user = message.reply_to_message.from_user
        target_id = int(target_user.id)
        upsert_user(target_id, getattr(target_user, "username", None))
        dur_spec = parts[1].strip()

    if not target_id or not dur_spec:
        bot.reply_to(message, "Использование: /blockcash @username|user_id 24h\nили ответом на сообщение: /blockcash 24h")
        return

    m = re.fullmatch(r"(?i)\s*(\d+)\s*([smhd])\s*", dur_spec)
    if not m:
        bot.reply_to(message, "Неверная длительность. Примеры: 30m, 6h, 24h, 2d")
        return

    n = int(m.group(1))
    unit = m.group(2).lower()
    mult = {"s": 1, "m": 60, "h": 3600, "d": 86400}.get(unit, 0)
    sec = n * mult
    if sec <= 0:
        bot.reply_to(message, "Неверная длительность.")
        return

    
    sec = min(sec, 30 * 86400) # максимум 30 дней

    ts = now_ts()
    until_ts = ts + int(sec)

    with DB_LOCK:
        c = conn.cursor()
        try:
            c.execute("BEGIN")
            c.execute(
                "INSERT OR REPLACE INTO transfer_blocks (user_id, until_ts, reason, created_ts, first_notice_ts) VALUES (?,?,?,?,?)",
                (int(target_id), int(until_ts), "manual", int(ts), int(ts))
            )
            c.execute(
                "INSERT INTO transfer_block_log (action, user_id, until_ts, reason, created_ts, chat_id, msg_id) VALUES (?,?,?,?,?,?,?)",
                ("manual_block", int(target_id), int(until_ts), "manual", int(ts), int(message.chat.id), int(message.message_id))
            )
            conn.commit()
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            bot.reply_to(message, f"Ошибка блокировки: {e}")
            return
        finally:
            try:
                c.close()
            except Exception:
                pass

    log_transfer_block_file("manual_block", int(target_id), int(until_ts), "manual", extra=f"by={message.from_user.id}")

    until_txt = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(until_ts)))
    left = max(0, int(until_ts) - now_ts())

    tu = get_user(int(target_id))
    tname = (tu[2] if tu and tu[2] else "Пользователь")
    tun = (tu[1] if tu and tu[1] else "") or ""
    tun_part = f" (@{html_escape(tun)})" if tun else ""

    bot.reply_to(
        message,
        f"Готово. Переводы заблокированы для <b>{html_escape(tname)}</b>{tun_part} до <b>{until_txt}</b> (через {_format_duration(left)}).",
        parse_mode="HTML"
    )

    try:
        bot.send_message(
            int(target_id),
            f"Ваш счёт был принудительно заморожен. Блокировка переводов истечёт через <b>{_format_duration(left)}</b> (Дата разблокировки <b>{until_txt}</b>). Сотрудник КО НПАО \"G®️eed\"",
            parse_mode="HTML"
        )
    except Exception:
        pass

@bot.message_handler(commands=["udblockcash"])
def cmd_udblockcash(message):
    if not is_bot_admin(message.from_user.id):
        return

    parts = (message.text or "").split(maxsplit=1)

    target_id: Optional[int] = None
    if len(parts) >= 2 and parts[1].strip():
        target_id = resolve_user_id_ref(parts[1].strip())
    elif message.reply_to_message:
        target_user = message.reply_to_message.from_user
        target_id = int(target_user.id)
        upsert_user(target_id, getattr(target_user, "username", None))
    else:
        bot.reply_to(message, "Использование: /udblockcash @username|user_id (или ответом на сообщение)")
        return

    if not target_id:
        bot.reply_to(message, "Пользователь не найден в базе. Используйте @username или user_id.")
        return

    until_ts = 0
    reason = ""
    with DB_LOCK:
        c = conn.cursor()
        try:
            c.execute("BEGIN")
            c.execute("SELECT until_ts, reason FROM transfer_blocks WHERE user_id=?", (int(target_id),))
            rr = c.fetchone()
            if not rr:
                conn.commit()
                bot.reply_to(message, "У пользователя нет активной блокировки переводов.")
                return

            until_ts = int((rr[0] if rr else 0) or 0)
            reason = str((rr[1] if rr else "") or "")

            c.execute("DELETE FROM transfer_blocks WHERE user_id=?", (int(target_id),))

            c.execute(
                "INSERT INTO transfer_block_log (action, user_id, until_ts, reason, created_ts, chat_id, msg_id) VALUES (?,?,?,?,?,?,?)",
                ("manual_unblock", int(target_id), int(until_ts), (reason or "")[:200], now_ts(), int(message.chat.id), int(message.message_id))
            )

            conn.commit()
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            bot.reply_to(message, f"Ошибка разблокировки: {e}")
            return
        finally:
            try:
                c.close()
            except Exception:
                pass

    log_transfer_block_file("manual_unblock", int(target_id), int(until_ts), reason, extra=f"by={message.from_user.id}")

    tu = get_user(int(target_id))
    tname = (tu[2] if tu and tu[2] else "Пользователь")
    tun = (tu[1] if tu and tu[1] else "") or ""
    tun_part = f" (@{html_escape(tun)})" if tun else ""

    bot.reply_to(message, f"Готово. Блокировка переводов снята с <b>{html_escape(tname)}</b>{tun_part}.", parse_mode="HTML")

    try:
        bot.send_message(int(target_id), "С вашего счёта снята блокировка переводов средств. Благодарим вас за оидание. Ваш НПАО \"G®️eed\"", parse_mode="HTML")
    except Exception:
        pass

@bot.message_handler(commands=["remessage"])
def cmd_remessage(message):
    if not is_bot_admin(message.from_user.id):
        return
    if message.chat.type != "private":
        return

    raw = message.text or ""
    if "\n" not in raw:
        bot.reply_to(
            message,
            "Использование:\n"
            "/remessage\n"
            "<текст рассылки с HTML-разметкой>"
        )
        return

    _cmd, body = raw.split("\n", 1)
    body = (body or "").strip("\n")
    if not body.strip():
        bot.reply_to(message, "Текст рассылки пуст.")
        return

    rows = db_all("SELECT user_id FROM users WHERE COALESCE(contract_ts,0) > 0", ())
    uids = [int(r[0]) for r in (rows or []) if r and str(r[0]).isdigit()]

    if not uids:
        bot.reply_to(message, "Нет зарегистрированных пользователей для рассылки.")
        return

    def _parse_retry_after(exc: Exception) -> float:
        s = str(exc)
        m = re.search(r"retry after (\d+(?:\.\d+)?)", s, re.IGNORECASE)
        if m:
            try:
                return float(m.group(1))
            except Exception:
                return 0.0
        return 0.0

    def _send_with_retry(chat_id: int, text: str) -> bool:
        try:
            bot.send_message(int(chat_id), text, parse_mode="HTML")
            return True
        except Exception as e:
            ra = _parse_retry_after(e)
            if ra and ra > 0:
                time.sleep(ra + 0.2)
                try:
                    bot.send_message(int(chat_id), text, parse_mode="HTML")
                    return True
                except Exception:
                    return False
            return False

    group_sent = 0
    group_failed = 0
    group_checked = 0

    covered_uids = set()

    bot_me_id = 0
    try:
        if ME:
            bot_me_id = int(getattr(ME, "id", 0) or 0)
    except Exception:
        bot_me_id = 0

    if bot_me_id <= 0:
        try:
            me = bot.get_me()
            bot_me_id = int(getattr(me, "id", 0) or 0)
        except Exception:
            bot_me_id = 0

    group_ids = get_known_broadcast_group_ids()

    for chat_id in group_ids:
        group_checked += 1

        if not bot_is_present_in_group(int(chat_id), bot_me_id=bot_me_id):
            continue

        sent_to_group = _send_with_retry(int(chat_id), body)
        if sent_to_group:
            group_sent += 1
            remember_group_chat(int(chat_id))
        else:
            group_failed += 1
            time.sleep(0.05)
            continue

        for uid in uids:
            if uid in covered_uids:
                continue

            try:
                mem = bot.get_chat_member(int(chat_id), int(uid))
                st = str(getattr(mem, "status", "") or "")
                if st and st not in ("left", "kicked"):
                    covered_uids.add(int(uid))
            except Exception:
                pass

            time.sleep(0.02)

        time.sleep(0.05)
    
    sent = 0
    failed = 0
    skipped_no_pm = 0

    for uid in uids:
        if uid in covered_uids:
            continue

        if not user_pm_notifications_enabled(int(uid)):
            skipped_no_pm += 1
            continue

        if _send_with_retry(int(uid), body):
            sent += 1
        else:
            failed += 1

        time.sleep(0.03)

    bot.reply_to(
        message,
        "Рассылка завершена.\n"
        f"Групповых отправок: {group_sent}\n"
        f"Ошибок по группам: {group_failed}\n"
        f"Проверено групп: {group_checked}\n"
        f"Покрыто через группы: {len(covered_uids)}\n"
        f"Личных отправок: {sent}\n"
        f"Пропущено из-за настроек ЛС: {skipped_no_pm}\n"
        f"Ошибок в ЛС: {failed}"
    )

@bot.message_handler(commands=["chatlist"])
def cmd_chatlist(message):
    if not is_bot_admin(message.from_user.id):
        return
    if message.chat.type != "private":
        return

    rows = db_all(
        "SELECT chat_id, COALESCE(title,''), COALESCE(last_seen_ts,0) "
        "FROM known_group_chats "
        "WHERE chat_id < 0 "
        "ORDER BY last_seen_ts DESC, added_ts DESC, chat_id ASC",
        ()
    ) or []

    if not rows:
        bot.reply_to(message, "Список известных групп пуст.")
        return

    try:
        if ME:
            bot_me_id = int(getattr(ME, "id", 0) or 0)
        else:
            me = bot.get_me()
            bot_me_id = int(getattr(me, "id", 0) or 0)
    except Exception:
        bot_me_id = 0

    lines = ["Известные групповые чаты:"]

    for chat_id, saved_title, last_seen_ts in rows:
        chat_id = int(chat_id or 0)
        title = str(saved_title or "").strip()
        present = True

        try:
            present = bot_is_present_in_group(chat_id, bot_me_id=bot_me_id)
        except Exception:
            present = True

        if not present:
            status = "не найден"
        else:
            status = "ok"

        try:
            ch = bot.get_chat(chat_id)
            fresh_title = str(getattr(ch, "title", "") or "").strip()
            if fresh_title:
                title = fresh_title
                remember_group_chat(chat_id, fresh_title)
        except Exception:
            pass

        if not title:
            title = "(без названия)"

        seen_txt = "-"
        try:
            if int(last_seen_ts or 0) > 0:
                seen_txt = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(last_seen_ts)))
        except Exception:
            seen_txt = str(last_seen_ts)

        lines.append(
            f"• <b>{html_escape(title)}</b>\n"
            f"ID: <code>{chat_id}</code>\n"
            f"Статус: <i>{status}</i>\n"
            f"Последняя активность: <i>{html_escape(seen_txt)}</i>"
        )

    bot.send_message(message.chat.id, "\n\n".join(lines), parse_mode="HTML")

@bot.message_handler(commands=["clearpm"])
def cmd_clearpm(message):
    if not is_bot_admin(message.from_user.id):
        return
    if message.chat.type != "private":
        return

    rows = db_all(
        "SELECT chat_id, message_id FROM pm_bot_messages WHERE deleted=0 ORDER BY created_ts ASC",
        ()
    ) or []

    if not rows:
        bot.reply_to(message, "Нет отслеживаемых ЛС-сообщений для очистки.")
        return

    ok = 0
    fail = 0

    for chat_id, msg_id in rows:
        if _delete_tracked_pm_message(int(chat_id), int(msg_id)):
            ok += 1
        else:
            fail += 1
        time.sleep(0.03)

    bot.reply_to(
        message,
        f"Очистка ЛС завершена.\nУдалено: {ok}\nНе удалось удалить: {fail}"
    )

@bot.message_handler(commands=["del"])
def cmd_del(message):
    if not is_bot_admin(message.from_user.id):
        return

    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].startswith("@"):
        bot.reply_to(message, "Использование: /del @username")
        return

    uname = parts[1][1:].strip()
    rr = db_one("SELECT user_id FROM users WHERE username=? COLLATE NOCASE", (uname,))
    if not rr:
        bot.reply_to(message, "Пользователь не найден в базе.")
        return

    target_id = int(rr[0])
    if target_id == OWNER_ID:
        bot.reply_to(message, "Нельзя удалить владельца бота.")
        return

    with DB_LOCK:
        c = conn.cursor()
        try:
            c.execute("BEGIN")

            c.execute("SELECT DISTINCT slave_id FROM slavery WHERE owner_id=?", (target_id,))
            affected_slaves = [int(r[0]) for r in c.fetchall()]

            c.execute("DELETE FROM reg_state WHERE user_id=?", (target_id,))
            c.execute("DELETE FROM daily_mail WHERE user_id=?", (target_id,))
            c.execute("DELETE FROM game_stats WHERE user_id=?", (target_id,))
            c.execute("DELETE FROM game_type_stats WHERE user_id=?", (target_id,))

            c.execute("DELETE FROM slavery WHERE slave_id=? OR owner_id=?", (target_id, target_id))
            c.execute("DELETE FROM slave_earn_log WHERE slave_id=? OR owner_id=?", (target_id, target_id))
            c.execute("DELETE FROM slave_meta WHERE slave_id=?", (target_id,))

            c.execute("DELETE FROM demon_loot WHERE winner_id=? OR loser_id=? OR slave_id=?",
                      (target_id, target_id, target_id))

            c.execute("DELETE FROM buy_offer_resp WHERE owner_id=?", (target_id,))
            c.execute("DELETE FROM buy_offers WHERE buyer_id=? OR slave_id=?", (target_id, target_id))
            c.execute(
                "DELETE FROM buyrab_offer_resp WHERE offer_id IN (SELECT offer_id FROM buyrab_offers WHERE buyer_id=? OR slave_id=?)",
                (target_id, target_id)
            )
            c.execute("DELETE FROM buyrab_offer_resp WHERE owner_id=?", (target_id,))
            c.execute("DELETE FROM buyrab_offers WHERE buyer_id=? OR slave_id=?", (target_id, target_id))

            c.execute("DELETE FROM work_stats WHERE user_id=?", (target_id,))
            c.execute("DELETE FROM work_shift WHERE user_id=?", (target_id,))
            c.execute("DELETE FROM work_history WHERE user_id=?", (target_id,))

            c.execute("DELETE FROM shop_inv WHERE user_id=?", (target_id,))
            c.execute("DELETE FROM shop_active WHERE user_id=?", (target_id,))
            c.execute("DELETE FROM shop_bind WHERE user_id=?", (target_id,))
            c.execute("DELETE FROM shop_used WHERE user_id=?", (target_id,))

            c.execute("DELETE FROM continue_tokens WHERE user_id=?", (target_id,))
            c.execute("DELETE FROM spins WHERE user_id=?", (target_id,))
            c.execute("DELETE FROM rematch_votes WHERE user_id=?", (target_id,))
            c.execute("DELETE FROM life_wait WHERE user_id=?", (target_id,))
            c.execute("DELETE FROM demon_streak WHERE user_id=?", (target_id,))
            c.execute("DELETE FROM credit_loans WHERE user_id=?", (target_id,))

            c.execute("DELETE FROM user_custom_status WHERE user_id=?", (target_id,))
            c.execute("DELETE FROM transfers WHERE from_id=? OR to_id=?", (target_id, target_id))

            c.execute("DELETE FROM game_players WHERE user_id=?", (target_id,))
            c.execute("DELETE FROM game_results WHERE user_id=?", (target_id,))

            c.execute("DELETE FROM users WHERE user_id=?", (target_id,))

            for sid in affected_slaves:
                c.execute("SELECT 1 FROM slavery WHERE slave_id=? LIMIT 1", (sid,))
                still_slave = c.fetchone() is not None
                if not still_slave:
                    c.execute("INSERT OR IGNORE INTO slave_meta (slave_id) VALUES (?)", (sid,))
                    c.execute("UPDATE slave_meta SET buyout_cents=0 WHERE slave_id=?", (sid,))

            conn.commit()
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            bot.reply_to(message, f"Ошибка удаления: {e}")
            return
        finally:
            try:
                c.close()
            except Exception:
                pass

    bot.reply_to(message, f"Готово. Пользователь @{uname} полностью удалён из базы.")

@bot.message_handler(commands=["ban"])
def cmd_ban(message):
    if not is_bot_admin(message.from_user.id):
        return

    raw = message.text or ""
    lines = raw.splitlines()
    first_line = (lines[0] if lines else "").strip()
    reason_nl = "\n".join(lines[1:]).strip()

    parts = first_line.split(maxsplit=3)
    if len(parts) < 2 or not parts[1].startswith("@"):
        bot.reply_to(message, "Использование:\n/ban @username [24h|7d|perm]\n<причина с новой строки>")
        return

    uname = parts[1][1:].strip()
    tok = parts[2].strip() if len(parts) >= 3 else ""
    tail_same_line = parts[3].strip() if len(parts) >= 4 else ""

    duration_sec = 0
    extra_reason = ""

    if tok:
        dur = parse_duration_to_seconds(tok)
        if dur is None:
            duration_sec = 0
            extra_reason = " ".join([tok, tail_same_line]).strip()
        else:
            duration_sec = int(dur or 0)
            extra_reason = tail_same_line

    reason = reason_nl if reason_nl else extra_reason
    reason = (reason or "").strip()

    rr = db_one("SELECT user_id FROM users WHERE username=? COLLATE NOCASE", (uname,))
    if not rr:
        bot.reply_to(message, "Пользователь не найден в базе.")
        return

    target_id = int(rr[0])
    if target_id == OWNER_ID:
        bot.reply_to(message, "Нельзя заблокировать владельца бота.")
        return

    until_ts = ban_user(target_id, by_id=message.from_user.id, reason=reason, duration_sec=duration_sec)

    try:
        msg = "Ваш аккаунт заблокирован администратором"
        if until_ts and int(until_ts) > 0:
            msg += f" до <b>{html_escape(_fmt_ts(int(until_ts)))}</b>."
        else:
            msg += "."

        if reason:
            msg += f"\nПричина: <i>{html_escape(reason)}</i>"

        msg += "\nЕсли вы не согласны с решением — отправьте апелляцию через /report."
        bot.send_message(target_id, msg, parse_mode="HTML")
    except Exception:
        pass

    if until_ts and int(until_ts) > 0:
        out = f"Пользователь @{uname} заблокирован до {_fmt_ts(int(until_ts))}"
    else:
        out = f"Пользователь @{uname} перманентно заблокирован"
    if reason:
        out += f"\nПричина:\n{reason}"
    bot.reply_to(message, out)

@bot.message_handler(commands=["unban"])
def cmd_unban(message):
    if not is_bot_admin(message.from_user.id):
        return

    txt = (message.text or "").strip()
    m = re.match(r"^/unban\s+@([A-Za-z0-9_]+)(?:\s+([\s\S]+))?$", txt)
    if not m:
        bot.reply_to(message, "Использование: /unban @username [причина]")
        return

    uname = (m.group(1) or "").strip()
    reason = (m.group(2) or "").strip()

    rr = db_one("SELECT user_id FROM users WHERE username=? COLLATE NOCASE", (uname,))
    if not rr:
        bot.reply_to(message, "Пользователь не найден в базе.")
        return

    target_id = int(rr[0])
    if target_id == OWNER_ID:
        bot.reply_to(message, "Владелец бота не может быть забанен.")
        return

    unban_user(target_id, by_id=message.from_user.id, reason=reason)

    try:
        bot.send_message(target_id, "Ваш аккаунт разблокирован администратором.")
    except Exception:
        pass

    bot.reply_to(message, f"Пользователь @{uname} разблокирован.")

# OWNER COMMANDS
@bot.message_handler(commands=["add_admin"])
def cmd_add_admin(message):
    if message.from_user.id != OWNER_ID:
        return

    target_id: Optional[int] = None
    target_uname: Optional[str] = None

    if message.reply_to_message:
        u = getattr(message.reply_to_message, "from_user", None)
        if u:
            target_id = int(getattr(u, "id", 0) or 0)
            target_uname = getattr(u, "username", None)
    else:
        parts = (message.text or "").split(maxsplit=1)
        if len(parts) >= 2:
            ref = parts[1].strip()
            if ref.startswith("@"):
                rr = db_one("SELECT user_id FROM users WHERE username=? COLLATE NOCASE", (ref[1:],))
                target_id = int(rr[0]) if rr else None
            elif ref.isdigit():
                target_id = int(ref)

    if not target_id:
        bot.reply_to(message, "Использование: /add_admin @username\nили ответом на сообщение.")
        return

    if int(target_id) == int(OWNER_ID):
        bot.reply_to(message, "Владелец уже имеет все права.")
        return

    upsert_user(int(target_id), target_uname)
    set_bot_admin(int(target_id), True, by_id=int(OWNER_ID))

    bot.reply_to(message, "Готово. Администратор назначен.")
    try:
        bot.send_message(int(target_id), "Вам выдан статус администратора бота.")
    except Exception:
        pass

@bot.message_handler(commands=["remove_admin"])
def cmd_remove_admin(message):
    if message.from_user.id != OWNER_ID:
        return

    target_id: Optional[int] = None
    target_uname: Optional[str] = None

    if message.reply_to_message:
        u = getattr(message.reply_to_message, "from_user", None)
        if u:
            target_id = int(getattr(u, "id", 0) or 0)
            target_uname = getattr(u, "username", None)
    else:
        parts = (message.text or "").split(maxsplit=1)
        if len(parts) >= 2:
            ref = parts[1].strip()
            if ref.startswith("@"):
                rr = db_one("SELECT user_id FROM users WHERE username=? COLLATE NOCASE", (ref[1:],))
                target_id = int(rr[0]) if rr else None
            elif ref.isdigit():
                target_id = int(ref)

    if not target_id:
        bot.reply_to(message, "Использование: /remove_admin @username\nили ответом на сообщение.")
        return

    if int(target_id) == int(OWNER_ID):
        bot.reply_to(message, "Нельзя снять права с владельца.")
        return

    upsert_user(int(target_id), target_uname)
    set_bot_admin(int(target_id), False, by_id=int(OWNER_ID))

    bot.reply_to(message, "Готово. Администратор разжалован.")
    try:
        bot.send_message(int(target_id), "Ваш статус администратора бота снят.")
    except Exception:
        pass

@bot.message_handler(commands=["db"])
def cmd_db(message):
    if message.from_user.id != OWNER_ID:
        return
    if message.chat.type != "private":
        bot.reply_to(message, "Команда /db доступна только в личных сообщениях с ботом.")
        return

    try:
        with open(DB_PATH, "rb") as f:
            bot.send_document(message.chat.id, f, caption="База данных бота")
    except Exception as e:
        bot.reply_to(message, f"Не удалось отправить базу данных: {e}")

@bot.message_handler(commands=["bot_off"])
def cmd_bot_off(message):
    if message.from_user.id != OWNER_ID:
        return
    parts = (message.text or "").split(maxsplit=1)
    mode = (parts[1].strip().lower() if len(parts) >= 2 else "")
    if mode not in ("error", "update"):
        bot.reply_to(message, "Использование: /bot_off error\nили\n/bot_off update")
        return

    global _FORCE_SLEEPING, _FORCE_SLEEP_MODE
    _FORCE_SLEEPING = True
    _FORCE_SLEEP_MODE = mode

    set_bot_sleep(mode, reason="")

    body = build_sleep_notice_text()
    stats = broadcast_notice(body, respect_pm_settings=True)

    bot.reply_to(
        message,
        "Готово. Бот переведён в спящий режим.\n"
        f"Групп: отправлено {stats.get('groups_sent',0)}, ошибок {stats.get('groups_failed',0)}\n"
        f"ЛС: отправлено {stats.get('pm_sent',0)}, пропущено {stats.get('pm_skipped',0)}, ошибок {stats.get('pm_failed',0)}"
    )

@bot.message_handler(commands=["bot_on"])
def cmd_bot_on(message):
    if message.from_user.id != OWNER_ID:
        return

    global _FORCE_SLEEPING, _FORCE_SLEEP_MODE
    _FORCE_SLEEPING = False
    _FORCE_SLEEP_MODE = ""

    clear_bot_sleep()

    amt = 111111  # компенсация 1111,11$
    comment = "Компенсация на время технических работ. Администратор"
    payload = base64.urlsafe_b64encode(comment.encode("utf-8")).decode("ascii")

    rows = db_all("SELECT user_id FROM users WHERE COALESCE(contract_ts,0) > 0 ORDER BY user_id", ()) or []
    mailed = 0
    instant = 0
    failed = 0

    for (uid,) in rows:
        uid = int(uid)
        try:
            if user_pm_notifications_enabled(uid):
                ensure_daily_mail_row(uid)
                _send_mail_prompt(uid, f"owner_finance|{payload}", int(amt))
                mailed += 1
            else:
                add_balance(uid, int(amt))
                instant += 1
        except Exception:
            failed += 1
        time.sleep(0.02)

    notify = "✅ Работа бота восстановлена! Благодарю Вас за ожидание. Администратор"
    nstats = broadcast_notice(notify, respect_pm_settings=True)

    bot.reply_to(
        message,
        "✅ Бот включён.\n"
        f"Компенсация: писем {mailed}, сразу зачислено {instant}, ошибок {failed}.\n"
        f"Уведомление: групп {nstats.get('groups_sent',0)}, ЛС {nstats.get('pm_sent',0)}, "
        f"пропущено {nstats.get('pm_skipped',0)}."
    )

@bot.message_handler(commands=["admins"])
def cmd_admins(message):
    # можно разрешить бот-админам смотреть список, но без OWNER строк
    if not is_bot_admin(message.from_user.id):
        return
    if message.chat.type != "private":
        return

    rows = db_all("""
        SELECT a.user_id,
               COALESCE(u.short_name,'') AS name,
               COALESCE(u.username,'')   AS uname,
               COALESCE(a.added_ts,0)    AS added_ts,
               COALESCE(a.added_by,0)    AS added_by
        FROM bot_admins a
        LEFT JOIN users u ON u.user_id=a.user_id
        ORDER BY COALESCE(a.added_ts,0) DESC, a.user_id ASC
    """, ()) or []

    lines = []
    lines.append("👮 Администраторы бота")
    lines.append(f"Состояние бота: <b>{html_escape(bot_status_human())}</b>")
    lines.append("")

    owner_u = db_one("SELECT COALESCE(short_name,''), COALESCE(username,'') FROM users WHERE user_id=?", (int(OWNER_ID),))
    owner_name = (owner_u[0] if owner_u else "") or "Владелец"
    owner_un = (owner_u[1] if owner_u else "") or ""
    owner_un_part = f" (@{html_escape(owner_un)})" if owner_un else ""
    lines.append(f"• <b>{html_escape(owner_name)}</b>{owner_un_part} — <code>{int(OWNER_ID)}</code> (OWNER)")

    if not rows:
        lines.append("")
        lines.append("Список бот-админов пуст.")
        bot.send_message(message.chat.id, "\n".join(lines), parse_mode="HTML")
        return

    lines.append("")
    lines.append("Бот-админы:")

    for uid, name, uname, added_ts, added_by in rows:
        uid = int(uid or 0)
        if uid == int(OWNER_ID):
            continue

        nm = (name or "").strip() or "Без имени"
        un = (uname or "").strip()
        un_part = f" (@{html_escape(un)})" if un else ""

        ts_txt = "-"
        try:
            if int(added_ts or 0) > 0:
                ts_txt = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(added_ts)))
        except Exception:
            ts_txt = "-"

        by_txt = ""
        if message.from_user.id == OWNER_ID and int(added_by or 0) > 0:
            by_txt = f", by <code>{int(added_by)}</code>"

        lines.append(f"• <b>{html_escape(nm)}</b>{un_part} — <code>{uid}</code> (с {html_escape(ts_txt)}{by_txt})")

    bot.send_message(message.chat.id, "\n".join(lines), parse_mode="HTML")

# DIFFERENT COMMANDS
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
    slavery_add_owner(slave_id, demon_id, 6000)

    db_exec("UPDATE demon_loot SET taken=1 WHERE winner_id=? AND slave_id=?", (demon_id, slave_id), commit=True)

    bot.reply_to(message, "Готово. Раб передан тебе.")

@bot.message_handler(commands=["profile"])
def cmd_profile(message):
    uid = message.from_user.id
    username = getattr(message.from_user, "username", None)
    upsert_user(uid, username)

    if is_banned(uid):
        if message.chat.type == "private":
            bot.reply_to(message, "У вас нет больше профиля.")
        return

    if message.reply_to_message:
        target_user = message.reply_to_message.from_user
        target_id = int(getattr(target_user, "id", 0) or 0)

        if target_id <= 0:
            bot.reply_to(message, "Не удалось определить пользователя.")
            return

        if int(target_id) != int(uid):
            try:
                upsert_user(target_id, getattr(target_user, "username", None))
            except Exception:
                pass

        if is_banned(target_id):
            bot.reply_to(message, "У пользователя нет больше профиля.")
            return

        text = build_profile_summary_text(target_id)
        if not text:
            bot.reply_to(message, "У пользователя нет профиля.")
            return

        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton(
            "Статистика по играм",
            callback_data=cb_pack(f"profile:gamesview:{target_id}", uid)
        ))

        bot.send_message(message.chat.id, text, parse_mode="HTML", reply_markup=kb)
        return

    raw = (message.text or "").strip()
    parts = raw.split(maxsplit=1)

    if len(parts) >= 2 and parts[1].strip():
        target_ref = parts[1].strip()

        if not target_ref.startswith("@"):
            bot.reply_to(message, "Использование: /profile, /profile @username или ответом на сообщение.")
            return

        target_un = target_ref[1:].strip()
        rr = db_one("SELECT user_id FROM users WHERE username=? COLLATE NOCASE", (target_un,))
        if not rr:
            bot.reply_to(message, "Пользователь не найден в базе.")
            return

        target_id = int(rr[0])

        if is_banned(target_id):
            bot.reply_to(message, "У пользователя нет больше профиля.")
            return

        text = build_profile_summary_text(target_id)
        if not text:
            bot.reply_to(message, "У пользователя нет профиля.")
            return

        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton(
            "Статистика по играм",
            callback_data=cb_pack(f"profile:gamesview:{target_id}", uid)
        ))

        bot.send_message(message.chat.id, text, parse_mode="HTML", reply_markup=kb)
        return

    text = build_profile_summary_text(uid)
    if not text:
        return

    kb = build_profile_open_kb(uid)
    bot.send_message(message.chat.id, text, parse_mode="HTML", reply_markup=kb)

REPORT_CATS = {
    "bug": "Ошибка бота",
    "user": "Жалоба на пользователя",
    "appeal": "Апелляция",
    "other": "Другое",
}

@bot.message_handler(commands=["report"])
def cmd_report(message):
    if message.chat.type != "private":
        bot.reply_to(message, "Команда /report доступна только в личных сообщениях.")
        return

    uid = message.from_user.id
    upsert_user(uid, getattr(message.from_user, "username", None))

    banned_now = is_banned(uid)

    if (not banned_now) and (not is_registered(uid)):
        return

    report_clear_state(uid)

    kb = InlineKeyboardMarkup()

    if banned_now:
        kb.add(InlineKeyboardButton("Апелляция", callback_data=cb_pack("report:cat:appeal", uid)))
    else:
        kb.add(InlineKeyboardButton("Ошибка бота", callback_data=cb_pack("report:cat:bug", uid)))
        kb.add(InlineKeyboardButton("Жалоба на пользователя", callback_data=cb_pack("report:cat:user", uid)))
        kb.add(InlineKeyboardButton("Апелляция", callback_data=cb_pack("report:cat:appeal", uid)))
        kb.add(InlineKeyboardButton("Другое", callback_data=cb_pack("report:cat:other", uid)))

    bot.send_message(message.chat.id, "Выберите категорию запроса:", reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("report:"))
def on_report_callbacks(call: CallbackQuery):
    base, owner = cb_unpack(call.data)
    clicker = call.from_user.id

    if owner is not None and clicker != owner:
        bot.answer_callback_query(call.id, "Вы не можете нажать на эту кнопку", show_alert=True)
        return

    parts = base.split(":")
    if len(parts) < 3 or parts[1] != "cat":
        bot.answer_callback_query(call.id)
        return

    cat = parts[2].strip()
    if cat not in REPORT_CATS:
        bot.answer_callback_query(call.id, "Неизвестная категория.", show_alert=True)
        return

    banned_now = is_banned(clicker)
    if banned_now and cat != "appeal":
        bot.answer_callback_query(call.id, "Вам доступна только апелляция.", show_alert=True)
        return

    report_set_state(clicker, cat, "await_content")

    if cat == "user":
        text = (
            "Отправьте одним сообщением:\n"
            "1-я строка @username нарушителя\n"
            "со 2-й строки описание (обязательно)\n\n"
            "Можно прикрепить фото или видео к этому сообщению."
        )
    elif cat == "appeal":
        if banned_now:
            text = (
                "Отправьте описание апелляции одним сообщением.\n\n"
                "Можно прикрепить фото или видео к этому сообщению."
            )
        else:
            text = (
                "Отправьте одним сообщением:\n"
                "1-я строка @username (по кому рассматривается апелляция)\n"
                "со 2-й строки описание (обязательно)\n\n"
                "Можно прикрепить фото или видео к этому сообщению."
            )
    else:
        text = (
            "Отправьте описание проблемы одним сообщением.\n\n"
            "Можно прикрепить фото или видео к этому сообщению."
        )

    edit_inline_or_message(call, text, reply_markup=None, parse_mode=None)
    bot.answer_callback_query(call.id)

@bot.message_handler(
        content_types=["text", "photo", "video"], 
        func=lambda m: (m.chat.type == "private" and report_get_state(m.from_user.id)[0] == "await_content")
)
def on_report_content(message):
    uid = message.from_user.id
    stage, cat = report_get_state(uid)
    if stage != "await_content" or not cat:
        return

    upsert_user(uid, getattr(message.from_user, "username", None))

    banned_now = is_banned(uid)

    if banned_now and cat != "appeal":
        report_clear_state(uid)
        try:
            r = db_one("SELECT COALESCE(until_ts,0) FROM bans WHERE user_id=? LIMIT 1", (int(uid),))
            until_ts = int((r[0] if r else 0) or 0)
        except Exception:
            until_ts = 0

        if until_ts > 0:
            bot.reply_to(
                message,
                f"Ваш аккаунт заблокирован администратором до <b>{html_escape(_fmt_ts(int(until_ts)))}</b>.",
                parse_mode="HTML"
            )
        else:
            bot.reply_to(message, "Ваш аккаунт заблокирован администратором. Используйте /report для подачи апелляции.")
        return

    raw = ""
    if message.content_type == "text":
        raw = (message.text or "").strip()
    else:
        raw = (message.caption or "").strip()

    if raw.startswith("/"):
        bot.reply_to(message, "Заполните форму одним сообщением (текст + опционально фото/видео).")
        return

    if not raw:
        bot.reply_to(message, "Пустое сообщение. Пришлите текст описания (и, при желании, фото/видео).")
        return

    target_un = ""        # для категории "Жалоба на пользователя"
    appeal_to_un = ""     # для категории "Апелляция"
    desc = ""

    if cat == "user":
        lines = raw.splitlines()
        if not lines or not lines[0].strip().startswith("@"):
            bot.reply_to(message, "Формат неверный. Первая строка должна быть @username")
            return
        target_un = lines[0].strip()
        desc = "\n".join(lines[1:]).strip()
        if not desc:
            bot.reply_to(message, "Добавьте описание проблемы со второй строки. Оно необходимо для выявления проблемы.")
            return

    elif cat == "appeal" and (not banned_now):
        lines = raw.splitlines()
        if not lines or not lines[0].strip().startswith("@"):
            bot.reply_to(message, "Формат неверный. Первая строка должна быть @username")
            return
        appeal_to_un = lines[0].strip()
        desc = "\n".join(lines[1:]).strip()
        if not desc:
            bot.reply_to(message, "Добавьте описание проблемы со второй строки. Оно необходимо для выявления проблемы.")
            return

    else:
        desc = raw.strip()

    from_name = message.from_user.first_name or "Пользователь"
    from_un = getattr(message.from_user, "username", None) or ""
    from_line = f"{html_escape(from_name)}" + (f" (@{html_escape(from_un)})" if from_un else "")
    ts_txt = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now_ts()))
    cat_title = REPORT_CATS.get(cat, cat)

    admin_text = f"Репорт {ts_txt}\nОт {from_line}\nКатегория {cat_title}\n"

    # на кого жалоба
    if cat == "user":
        tu = target_un.lstrip("@").strip()
        rr = db_one("SELECT short_name, username FROM users WHERE username=? COLLATE NOCASE", (tu,))
        if rr:
            tname = rr[0] or "Пользователь"
            tun = rr[1] or tu
            admin_text += f"На {html_escape(tname)} (@{html_escape(tun)})\n"
        else:
            admin_text += f"На @{html_escape(tu)}\n"

    # кому рассматривать апелляцию
    if cat == "appeal" and appeal_to_un:
        tu = appeal_to_un.lstrip("@").strip()
        rr = db_one("SELECT short_name, username FROM users WHERE username=? COLLATE NOCASE", (tu,))
        if rr:
            tname = rr[0] or "Пользователь"
            tun = rr[1] or tu
            admin_text += f"Для {html_escape(tname)} (@{html_escape(tun)})\n"
        else:
            admin_text += f"Для @{html_escape(tu)}\n"

    admin_text += "Описание проблемы:\n"
    admin_text += f"<i>{html_escape(desc)}</i>"

    if cat == "user":
        admin_text += "\nБыстрые команды:\n/ban  /del"

    # медиа
    media_type = None
    media_file_id = None
    try:
        if message.content_type == "photo" and message.photo:
            media_type = "photo"
            media_file_id = message.photo[-1].file_id
        elif message.content_type == "video" and message.video:
            media_type = "video"
            media_file_id = message.video.file_id
    except Exception:
        media_type = None
        media_file_id = None

    try:
        if media_type and media_file_id:
            if len(admin_text) <= 900:
                if media_type == "photo":
                    bot.send_photo(OWNER_ID, media_file_id, caption=admin_text, parse_mode="HTML")
                else:
                    bot.send_video(OWNER_ID, media_file_id, caption=admin_text, parse_mode="HTML")
            else:
                bot.send_message(OWNER_ID, admin_text, parse_mode="HTML")
                if media_type == "photo":
                    bot.send_photo(OWNER_ID, media_file_id)
                else:
                    bot.send_video(OWNER_ID, media_file_id)
        else:
            bot.send_message(OWNER_ID, admin_text, parse_mode="HTML")
    except Exception:
        bot.reply_to(message, "Не удалось отправить репорт. Попробуйте позже.")
        return

    report_clear_state(uid)
    bot.reply_to(message, "Репорт отправлен администратору на рассмотрение. Благодарим вас за поддержку проекта!")

@bot.message_handler(commands=["pay"])
def cmd_pay(message):
    sender_id = int(message.from_user.id)
    sender_un = getattr(message.from_user, "username", None)
    upsert_user(sender_id, sender_un)

    if is_banned(sender_id):
        bot.reply_to(message, "Вам нечего переводить пользователю.")
        return

    if not is_registered(sender_id):
        return

    raw = (message.text or "").strip()
    parts = raw.split(maxsplit=3)

    target_id: Optional[int] = None
    amount_str: Optional[str] = None
    comment = ""

    if target_id and is_banned(int(target_id)):
        bot.reply_to(message, "Переводы этому пользователю больше недоступны. Приносим свои извинения.\nСотрудник КО НПАО \"G®️eed\"")
        return

    if message.reply_to_message and len(parts) >= 2 and not parts[1].startswith("@"):
        target_user = message.reply_to_message.from_user
        target_id = int(target_user.id)
        upsert_user(target_id, getattr(target_user, "username", None))
        amount_str = parts[1]
        comment = " ".join(parts[2:]).strip() if len(parts) >= 3 else ""
    else:
        if len(parts) < 3:
            bot.reply_to(
                message,
                "Приветсвуем вас в системе быстрых переводов средств КО НПАО \"G®️®eed\"\n"
                "Чтобы воспользоваться услугой, введите: /pay @username сумма [комментарий]\n"
                "Поддержка перевода по NFS! Достаточно ответить на чужое сообщение и ввести: /pay сумма [комментарий]"
            )
            return

        target_ref = parts[1].strip()
        if not target_ref.startswith("@"):
            bot.reply_to(message, "Использование: /pay @username сумма [комментарий]")
            return

        target_un = target_ref[1:].strip()
        rr = db_one("SELECT user_id FROM users WHERE username=? COLLATE NOCASE", (target_un,))
        if not rr:
            bot.reply_to(message, "Пользователь не найден в базе данных нашей организации :(")
            return

        target_id = int(rr[0])
        amount_str = parts[2]
        comment = parts[3] if len(parts) >= 4 else ""

    amt = money_to_cents(amount_str or "")
    if amt is None or int(amt) <= 0:
        bot.reply_to(message, "Неверная сумма.")
        return

    fee = calc_pay_fee_cents(int(amt))

    ok, reason, sbal, rbal, _tid = transfer_balance(
        sender_id, int(target_id), int(amt),
        comment=comment,
        chat_id=int(message.chat.id),
        msg_id=int(message.message_id)
    )

    if not ok:
        if reason == "insufficient":
            if fee > 0:
                bot.reply_to(
                    message,
                    f"Недостаточно средств на перевод.\n"
                    f"К переводу: {cents_to_money_str(int(amt))}$\n"
                    f"Комиссия: {cents_to_money_str(int(fee))}$\n"
                    f"К списанию: {cents_to_money_str(int(amt) + int(fee))}$\n"
                    f"Ваш баланс: {cents_to_money_str(int(sbal))}$"
                )
            else:
                bot.reply_to(message, f"Недостаточно средств на вашем балансе: {cents_to_money_str(int(sbal))}$")
            return
        if reason == "self":
            return
        
        if reason == "blocked_sender":
            until_ts, first_notice_ts, b_reason = get_transfer_block(sender_id)
        
            if int(first_notice_ts or 0) <= 0 and (b_reason or "") == "suspicious":
                bot.reply_to(message, PAY_FRAUD_BLOCK_TEXT)
                mark_transfer_block_notified(sender_id)
            else:
                if int(until_ts or 0) > 0:
                    until_txt = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(until_ts)))
                    left = max(0, int(until_ts) - now_ts())
                    bot.reply_to(
                        message,
                        f"Ваш счёт временно недоступен. Примерное время ожидания через <b>{_format_duration(left)}</b> (Дата разблокировки <b>{until_txt}</b>). Благодарим вас за понимание. Ваш НПАО \"G®️eed\"",
                        parse_mode="HTML"
                    )
                else:
                    bot.reply_to(message, "Переводы временно заблокированы. Попробуйте позже.")
            return

        if reason == "blocked_receiver":
            bot.reply_to(message, "Переводы на счёт этого пользователя временно недоступны.")
            return

        bot.reply_to(message, "Не удалось выполнить перевод. Попробуйте позже.")
        return

    su = get_user(sender_id)
    sname = (su[2] if su and su[2] else None) or (su[1] if su and su[1] else None) or (sender_un or str(sender_id))
    sun = (su[1] if su and su[1] else "") or ""
    sun_part = f" (@{html_escape(sun)})" if sun else ""

    tu = get_user(int(target_id))
    tname = (tu[2] if tu and tu[2] else None) or (tu[1] if tu and tu[1] else None) or str(target_id)
    tun = (tu[1] if tu and tu[1] else "") or ""
    tun_part = f" (@{html_escape(tun)})" if tun else ""

    fee_lines = ""
    if fee > 0:
        fee_lines = (
            f"\nКомиссия за перевод <b>{cents_to_money_str(int(fee))}</b>$"
            f"\nИтоговое списание: <b>{cents_to_money_str(int(amt) + int(fee))}</b>$"
        )

    comment_clean = (comment or "").strip()
    if len(comment_clean) > 240:
        comment_clean = comment_clean[:240] + "…"
    comment_line = f"\nКомментарий к переводу: <i>{html_escape(comment_clean)}</i>" if comment_clean else ""
    
    bot.reply_to(
        message,
        f"Перевод выполнен: <b>{cents_to_money_str(int(amt))}</b>$ → <b>{html_escape(tname)}</b>{tun_part}"
        f"{fee_lines}\n"
        f"Ваш баланс: <b>{cents_to_money_str(int(sbal))}</b>$\n"
        "Благодарим за пользование услугами перевода КО НПАО \"G®️eed\"",
        parse_mode="HTML"
    )

    try:
        bot.send_message(
            int(target_id),
            f"Вам перевели <b>{cents_to_money_str(int(amt))}</b>$ от <b>{html_escape(sname)}</b>{sun_part}.\n"
            f"Ваш баланс: <b>{cents_to_money_str(int(rbal))}</b>$\n"
            f"{comment_line}\n"
            "Ваш НПАО \"G®️eed\"",
            parse_mode="HTML"
        )
    except Exception:
        pass

@bot.message_handler(commands=["rabs"])
def cmd_rabs(message):
    viewer_id = message.from_user.id
    upsert_user(viewer_id, getattr(message.from_user, "username", None))

    if message.chat.type in ("group", "supergroup"):
        remember_group_chat(message.chat.id, getattr(message.chat, "title", "") or "")

    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].startswith("@"):
        bot.reply_to(message, "Использование: /rabs @username")
        return

    owner_un = parts[1][1:].strip()
    rr = db_one(
        "SELECT user_id FROM users WHERE username=? COLLATE NOCASE",
        (owner_un,)
    )
    if not rr:
        bot.reply_to(message, "Пользователь не найден.")
        return

    owner_id = int(rr[0])
    text, kb = build_rabs_list_text_kb(owner_id, viewer_id)
    bot.send_message(message.chat.id, text, reply_markup=kb, parse_mode="HTML")

@bot.message_handler(commands=["buyrab"])
def cmd_buyrab(message):
    buyer_id = message.from_user.id
    buyer_un = getattr(message.from_user, "username", None)
    upsert_user(buyer_id, buyer_un)

    if message.chat.type in ("group", "supergroup"):
        remember_group_chat(message.chat.id, getattr(message.chat, "title", "") or "")

        parts = (message.text or "").split(maxsplit=2)
        if len(parts) < 2 or not parts[1].startswith("@"):
            bot.reply_to(message, "Использование: /buyrab @username [сумма]")
            return

        target_uname = parts[1][1:].strip()
        amount_raw = ""

        if len(parts) >= 3 and parts[2].strip():
            amount_raw = parts[2].replace("$", "").strip()
            chk = money_to_cents(amount_raw)
            if chk is None or chk <= 0:
                bot.reply_to(message, "Неверный формат суммы. Поддерживаемые форматы 15000 или 15000.50")
                return

        rr = db_one(
            "SELECT user_id, short_name, username FROM users WHERE username=? COLLATE NOCASE",
            (target_uname,),
        )
        if not rr:
            bot.reply_to(message, "Пользователь не найден.")
            return

        slave_id = int(rr[0])
        actual_un = (rr[2] or target_uname or "").strip()

        if slave_id == buyer_id:
            bot.reply_to(message, "Насколько не была бы ценна ваша душа, поверьте, вам не хватит средств, чтобы выкупить её.")
            return

        if not is_slave(slave_id):
            bot.reply_to(message, "Этот пользователь не является рабом.")
            return

        if db_one("SELECT 1 FROM slavery WHERE slave_id=? AND owner_id=? LIMIT 1", (slave_id, buyer_id)):
            bot.reply_to(message, "Вы уже являетесь владельцем этого раба. Для выкупа доли с владения раба используйте /rebuy.")
            return

        if db_one(
            "SELECT 1 FROM buyrab_offers WHERE slave_id=? AND buyer_id=? AND state IN (0,1) LIMIT 1",
            (slave_id, buyer_id),
        ):
            bot.reply_to(message, "У вас уже есть активная сделка на этого раба.")
            return

        trade_state_set(buyer_id, "buyrab", _trade_pack_payload(actual_un, amount_raw), stage="ready")

        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("Продолжить", callback_data=cb_pack("dealpm:open", buyer_id)))

        bot.reply_to(
            message,
            "Продолжить оформление сделки можно в личных сообщениях бота.",
            reply_markup=kb
        )
        return

    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 2 or not parts[1].startswith("@"):
        bot.reply_to(message, "Использование: /buyrab @username [сумма]")
        return

    target_uname = parts[1][1:].strip()
    custom_total = None
    if len(parts) >= 3 and parts[2].strip():
        raw = parts[2].replace("$", "").strip()
        custom_total = money_to_cents(raw)
        if custom_total is None or custom_total <= 0:
            bot.reply_to(message, "Неверный формат суммы. Поддерживаемые форматы 15000 или 15000.50")
            return

    rr = db_one(
        "SELECT user_id, short_name, username FROM users WHERE username=? COLLATE NOCASE",
        (target_uname,),
    )
    if not rr:
        bot.reply_to(message, "Пользователь не найден.")
        return

    slave_id = int(rr[0])
    slave_name = rr[1] or "Без имени"
    slave_username = rr[2] or ""
    slave_un_part = f" (@{html_escape(slave_username)})" if slave_username else ""

    if slave_id == buyer_id:
        bot.reply_to(message, "Насколько не была бы ценна ваша душа, поверьте, вам не хватит средств, чтобы выкупить её.")
        return

    if not is_slave(slave_id):
        bot.reply_to(message, "Этот пользователь не является рабом.")
        return

    if db_one("SELECT 1 FROM slavery WHERE slave_id=? AND owner_id=? LIMIT 1", (slave_id, buyer_id)):
        bot.reply_to(message, "Вы уже являетесь владельцем этого раба. Для выкупа доли с владения раба используйте /rebuy.")
        return

    db_exec("INSERT OR IGNORE INTO slave_meta (slave_id) VALUES (?)", (slave_id,), commit=True)
    row = db_one("SELECT buyout_cents FROM slave_meta WHERE slave_id=?", (slave_id,))
    buyout_cents = int((row or (0,))[0] or 0)
    if buyout_cents <= 0:
        bot.reply_to(message, "У этого раба не назначена цена выкупа.")
        return

    owners = get_slave_owners(slave_id)
    if not owners:
        bot.reply_to(message, "У этого раба нет владельцев.")
        return

    if db_one(
        "SELECT 1 FROM buyrab_offers WHERE slave_id=? AND buyer_id=? AND state IN (0,1) LIMIT 1",
        (slave_id, buyer_id),
    ):
        bot.reply_to(message, "У вас уже есть активная сделка на этого раба. Дождитесь ответа владельцев или отмените прошлую сделку.")
        return

    total_cents = int(custom_total if custom_total is not None else buyout_cents)

    if total_cents <= 0:
        bot.reply_to(message, "Некорректная сумма сделки.")
        return

    buyer_bal = get_balance_cents(buyer_id)
    if buyer_bal < total_cents or buyer_bal < 0:
        bot.reply_to(
            message,
            f"Недостаточно средств для приобретения. Необходимая сумма: {cents_to_money_str(total_cents)}$\nВаш балансе: {cents_to_money_str(buyer_bal)}$.",
        )
        return

    total_bp = sum(int(bp or 0) for (_oid, bp) in owners) or 0
    if total_bp <= 0:
        bot.reply_to(message, "Некорректные доли владельцев у раба.")
        return

    pay_parts = []
    allocated = 0
    for i, (oid, bp) in enumerate(owners):
        part = (total_cents * int(bp or 0)) // total_bp
        pay_parts.append([int(oid), int(part)])
        allocated += int(part)
    rem = total_cents - allocated
    if pay_parts:
        pay_parts[0][1] += rem

    offer_id = uuid.uuid4().hex
    tx_no = random.randint(10000, 99999)

    with DB_LOCK:
        c = conn.cursor()
        try:
            c.execute("BEGIN")
            c.execute(
                "INSERT INTO buyrab_offers (offer_id, tx_no, slave_id, buyer_id, total_cents, hold_cents, created_ts, state) "
                "VALUES (?,?,?,?,?,?,?,0)",
                (offer_id, tx_no, slave_id, buyer_id, total_cents, 0, now_ts()),
            )
            for oid, pay_cents in pay_parts:
                c.execute(
                    "INSERT OR REPLACE INTO buyrab_offer_resp (offer_id, owner_id, pay_cents, status) VALUES (?,?,?,0)",
                    (offer_id, int(oid), int(pay_cents)),
                )
            conn.commit()
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            bot.reply_to(message, f"Ошибка создания сделки: {e}")
            return
        finally:
            try:
                c.close()
            except Exception:
                pass

    owners_disp = []
    for oid, _bp in owners:
        u = get_user(int(oid))
        nm = (u[2] if u and u[2] else "Без имени")
        un = (u[1] if u and u[1] else "")
        owners_disp.append(f"{html_escape(nm)}" + (f" (@{html_escape(un)})" if un else ""))
    owners_line = ", ".join(owners_disp) if owners_disp else "-"

    explain = "каждый владелец получит свою долю, равную цене выкупа его доли."
    if custom_total is not None and custom_total != buyout_cents:
        explain = "сумма будет распределена между владельцами пропорционально их долям владения."

    txt = (
        "Проверьте данные, перед приобретением \"товара\":\n"
        f"Имя раба: <b>{html_escape(slave_name)}</b>{slave_un_part}\n"
        f"Владельцы: {owners_line}\n"
        f"Цена к оплате: <b>{cents_to_money_str(total_cents)}</b>$\n"
        f"{explain}"
    )

    kb = InlineKeyboardMarkup()
    kb.row(
        InlineKeyboardButton("Согласиться и отправить", callback_data=cb_pack(f"buyrab:send:{offer_id}", buyer_id)),
        InlineKeyboardButton("Отказаться", callback_data=cb_pack(f"buyrab:cancel:{offer_id}", buyer_id)),
    )

    bot.send_message(message.chat.id, txt, parse_mode="HTML", reply_markup=kb)

@bot.message_handler(commands=["buyout"])
def cmd_buyout(message):
    if message.chat.type in ("group", "supergroup"):
        remember_group_chat(message.chat.id, getattr(message.chat, "title", "") or "")

        if not is_slave(message.from_user.id):
            bot.reply_to(message, "Ты не раб.")
            return

        trade_state_set(message.from_user.id, "buyout", "", stage="ready")

        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("Продолжить", callback_data=cb_pack("dealpm:open", message.from_user.id)))

        bot.reply_to(
            message,
            "Продолжить оформление выкупа можно в личных сообщениях бота.",
            reply_markup=kb
        )
        return

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
        bot.send_message(message.chat.id, "Ты свободен.", parse_mode="HTML")
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
            notify_safe(oid, f"Раб выкупил себя. Сумма, которую он оставил вам за свою свободу <b>{cents_to_money_str(part)}</b>$",)

    free_slave_fully(uid, "самовыкуп")

    bot.send_message(
        message.chat.id,
        f"Ты успешно выкупил свою свободу за <b>{cents_to_money_str(buyout_cents)}</b>$.",
        parse_mode="HTML"
    )

@bot.message_handler(commands=["rebuy"])
def cmd_buy(message):
    if message.chat.type in ("group", "supergroup"):
        remember_group_chat(message.chat.id, getattr(message.chat, "title", "") or "")

        buyer_id = message.from_user.id
        buyer_username = getattr(message.from_user, "username", None)
        upsert_user(buyer_id, buyer_username)

        parts = (message.text or "").split(maxsplit=2)
        if len(parts) < 2 or not parts[1].startswith("@"):
            bot.reply_to(message, "Использование: /rebuy @username [цена]")
            return

        slave_un = parts[1][1:].strip()
        amount_raw = ""

        if len(parts) >= 3 and parts[2].strip():
            amount_raw = parts[2].replace("$", "").strip()
            chk = money_to_cents(amount_raw)
            if chk is None or chk <= 0:
                bot.reply_to(message, "Неверная цена.")
                return

        rr = db_one(
            "SELECT user_id, username FROM users WHERE username=? COLLATE NOCASE",
            (slave_un,)
        )
        if not rr:
            bot.reply_to(message, "Пользователь не найден в базе.")
            return

        slave_id = int(rr[0])
        actual_un = (rr[1] or slave_un or "").strip()

        if db_one("SELECT 1 FROM slavery WHERE slave_id=? AND owner_id=? LIMIT 1", (slave_id, buyer_id)) is None:
            bot.reply_to(message, "Ты не являешься владельцем этого раба.")
            return

        other_owners = db_all(
            "SELECT owner_id FROM slavery WHERE slave_id=? AND owner_id<>?",
            (slave_id, buyer_id)
        )
        if not other_owners:
            bot.reply_to(message, "Ты уже единственный владелец.")
            return

        trade_state_set(buyer_id, "rebuy", _trade_pack_payload(actual_un, amount_raw), stage="ready")

        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("Продолжить", callback_data=cb_pack("dealpm:open", buyer_id)))

        bot.reply_to(
            message,
            "Продолжить оформление сделки можно в личных сообщениях бота.",
            reply_markup=kb
        )
        return

    if message.chat.type != "private":
        return
    
    buyer_id = message.from_user.id
    buyer_username = getattr(message.from_user, "username", None)
    upsert_user(buyer_id, buyer_username)

    parts = (message.text or "").split()
    if len(parts) < 3 or not parts[1].startswith("@"):
        bot.reply_to(message, "Использование: /rebuy @username цена")
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
    rand = random.randint(1000000, 9999999)

    text = (
        f"Предложение о выкупе раба №{rand}\n\n"
        f"Раб: <b>{html_escape(slave_name)}</b> (@{html_escape(slave_un)})\n"
        f"Покупатель: <b>{html_escape(buyer_tag)}</b>\n"
        f"Предлагаемая цена выкупа: <b>{cents_to_money_str(price_cents)}</b>$\n\n"
        f"Согласны на сделку?"
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

def _checkpoint_daemon(): #checkpoint
    while True:
        time.sleep(1800)  # раз в 30 минут (600 = раз в 10 минут)
        with DB_LOCK:
            try:
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            except Exception:
                pass

threading.Thread(target=_checkpoint_daemon, daemon=True).start()

# RUN
print(f"Contest bot started as @{BOT_USERNAME}")
while True:
    try:
        bot.infinity_polling(skip_pending=True, timeout=10, long_polling_timeout=20)
    except Exception as e:
        try:
            print("polling crashed:", repr(e))
        except Exception:
            pass

        try:
            send_error_report("infinity_polling", e)
        except Exception:
            pass
        time.sleep(5)