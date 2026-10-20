# -*- coding: utf-8 -*-
"""
TaskExchange — ربات تبادل تسک تلگرام (عضویت / ری‌اکشن / بازدید)
Stack (مثل نمونه‌ی AI BotMaker): Flask + MongoDB(pymongo) + وب‌هوک، تک‌فایل و بدون مینی‌اپ

مدل کار:
- کاربر با انجام تسک «کوین» می‌گیره و با کوین، تسک خودش رو ثبت می‌کنه (هزینه‌ی ثبت > پاداش؛ اقتصاد تورم نمی‌کنه)
- عضویتِ تأییدشده، بازدید پس از رعایت زمان، و ری‌اکشنِ قابل‌تأیید پاداش فوری می‌گیرند؛ موارد نیازمند بررسی/تأیید شمارش طبق مسیر مخصوص خودشان مدیریت می‌شوند
- عضویت  : getChatMember (ربات باید ادمین کانال/گروه باشه) + چک دوباره‌ی آخر دوره‌ی معلق (لفت = برگشت کوین + اخطار)
- ری‌اکشن : گروه/سوپرگروه با ربات ادمین → تأیید فردی با message_reaction
            کانال → تأیید تجمیعی با message_reaction_count؛ هر واحد افزایش شمارش حداکثر یک ادعا را تأیید می‌کند.
            مهلت تسک کانال ۲ یا ۵ دقیقه است، پاداش پس از دریافت افزایش معتبر فوری پرداخت می‌شود، و آپدیت ممکن است چند دقیقه تأخیر داشته باشد.
            شمارش تجمیعی هویت کاربر را ثابت نمی‌کند؛ این روش تعداد افزایش‌ها را می‌سنجد، نه اینکه چه کسی واکنش داده است.
- بازدید  : فقط تأیید زمانی (از طریق Bot API قابل راستی‌آزمایی نیست) → پاداش کم

نیازمندی‌ها: flask pymongo requests gunicorn Pillow   (Pillow برای تشخیص عکس تکراریِ دوباره‌آپلودشده لازمه؛ mongomock اختیاری برای تست)
اجرا: gunicorn taskbot:app
"""
import os, re, io, json, time, hmac, hashlib, base64, random, logging, threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta

import requests
from flask import Flask, request
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from bson import ObjectId
from bson.errors import InvalidId

# ───────────────────────── تنظیمات (Environment) ─────────────────────────
BOT_TOKEN   = os.environ["BOT_TOKEN"]
BASE_URL    = os.environ["BASE_URL"].rstrip("/")                 # مثلا https://yourapp.onrender.com
MONGO_URI   = os.environ.get("MONGO_URI", "").strip()           # خالی = حافظه‌ی موقت (ذخیره نمی‌شه)
SECRET_KEY  = os.environ.get("SECRET_KEY", BOT_TOKEN)
ADMINS      = {int(x) for x in re.findall(r"\d+", os.environ.get("ADMIN_IDS", ""))}   # مثلا 12345,67890
TASKS_CHANNEL = os.environ.get("TASKS_CHANNEL", "").strip()  # مثلا @MyTasksChannel؛ ربات باید ادمین کانال باشد
TASKS_CHANNEL_URL = os.environ.get("TASKS_CHANNEL_URL", "").strip()  # اختیاری؛ لینک عمومی کانال فهرست تسک‌ها

# مدل ویژن (اختیاری؛ هر API سازگار با OpenAI). اگه تنظیم نشه، اسکرین‌شات‌ها می‌رن برای بازبینی صاحب کمپین
AI_BASE_URL = re.sub(r"/chat/completions/?$", "", os.environ.get("AI_BASE_URL", "").strip().rstrip("/"))
AI_API_KEY  = os.environ.get("AI_API_KEY", "")
# AI_MODEL می‌تونه چند مدل باشه (با ویرگول، ; یا خط جدید جدا کن). ترتیب = اولویت. هر آیتم یکی از این شکل‌هاست:
#   model                       ← با AI_BASE_URL و AI_API_KEY بالا
#   model|https://base/v1       ← همون کلید، سرویس دیگه
#   model|https://base/v1|key   ← سرویس و کلید جدا
AI_ENDPOINTS = []
for _it in re.split(r"[,;\n]+", os.environ.get("AI_MODEL", "")):
    _p = [x.strip() for x in _it.split("|")]
    if _p[0]:
        _b = re.sub(r"/chat/completions/?$", "", (_p[1] if len(_p) > 1 and _p[1] else AI_BASE_URL).rstrip("/"))
        _k = _p[2] if len(_p) > 2 and _p[2] else AI_API_KEY
        _lab = _p[0] if _b == AI_BASE_URL else f"{_p[0]}@{_b.split('//')[-1].split('/')[0]}"
        if not any(x["label"] == _lab for x in AI_ENDPOINTS):          # مدل تکراری در AI_MODEL نادیده گرفته می‌شه
            AI_ENDPOINTS.append({"label": _lab, "model": _p[0], "base": _b, "key": _k})
AI_MODEL    = AI_ENDPOINTS[0]["model"] if AI_ENDPOINTS else ""
AI_ON       = any(e["base"] and e["key"] for e in AI_ENDPOINTS)
AI_BUDGET   = int(os.environ.get("AI_BUDGET", "60"))                # سقف کل ثانیه‌ی یک درخواست (همه‌ی مدل‌ها روی هم)

START_POINTS = int(os.environ.get("START_POINTS", "20"))        # هدیه‌ی شروع
HOLD_HOURS   = float(os.environ.get("HOLD_HOURS", "24"))       # مدت معلق ماندن پاداش
DAILY_TASKS  = int(os.environ.get("DAILY_TASKS", "60"))        # سقف تسک روزانه‌ی هر کاربر
DAILY_AI     = int(os.environ.get("DAILY_AI", "12"))           # سقف بررسی اسکرین‌شات در روز برای هر کاربر
STRIKE_LIMIT = int(os.environ.get("STRIKE_LIMIT", "3"))        # بعد از این تعداد اخطار، انجام تسک بسته می‌شه
MAX_QTY      = int(os.environ.get("MAX_QTY", "500"))
MAX_ACTIVE   = int(os.environ.get("MAX_ACTIVE", "10"))         # سقف کمپین فعال هر کاربر
SHOT_WINDOW  = 600                                              # مهلت ارسال اسکرین‌شات بعد از زدن «انجام دادم» (ثانیه)
REACTION_UPDATE_GRACE = int(os.environ.get("REACTION_UPDATE_GRACE", "180"))  # فرصت اضافه برای تأخیر آپدیت شمارش تلگرام
REACTION_BASELINE_MAX_AGE = int(os.environ.get("REACTION_BASELINE_MAX_AGE", "900"))  # حداکثر سن شمارش پایه (ثانیه)
ADMIN_MONITOR_INTERVAL = int(os.environ.get("ADMIN_MONITOR_INTERVAL", "60"))  # فاصله بررسی ادمین کمپین‌ها
CONF_MIN     = float(os.environ.get("CONF_MIN", "0.3"))        # حداقل اطمینان مدل برای قبول عکس
VERSION      = "v40-coin-clean-copy-completed-button-view-5s"
AI_TIMEOUT   = int(os.environ.get("AI_TIMEOUT", "25"))              # حداکثر ثانیه‌ی هر تماس با مدل
CLOCK_BUDGET = int(os.environ.get("CLOCK_BUDGET", "15"))            # سقف ثانیه‌ی خوندن ساعت
CLOCK_REQUIRED = True                                           # ساعت خونده نشد = رد (بدون ساعتِ تازه هیچ عکسی قبول نمی‌شه)
try:
    import PIL.Image  # noqa
    PIL_OK = True
except Exception:
    PIL_OK = False
PIXEL_CHECK  = os.environ.get("PIXEL_CHECK", "1") != "0"        # بررسی پیکسلی چیپ پررنگ ری‌اکشن (قبل از مدل)؛ =0 خاموش
AI_DEBUG     = os.environ.get("AI_DEBUG", "") == "1"           # =1: جواب خام مدل برای هر رد شدن به ادمین‌ها فرستاده می‌شه
TIME_TOL     = int(os.environ.get("TIME_TOL", "5"))             # چند دقیقه قبل از زدن «انجام دادم» هنوز قبوله (دقیقه)
TZ_NAME      = os.environ.get("TZ_NAME", "Asia/Tehran")        # برای مقایسه‌ی ساعت داخل اسکرین‌شات

# نوع تسک: پاداش انجام‌دهنده / هزینه‌ی ثبت برای صاحب کمپین (هر واحد) / حداقل صبر (ثانیه)
TYPES = {
    "join":  {"title": "عضویت",   "reward": 3, "cost": 4, "wait": 0},
    "react": {"title": "ری‌اکشن", "reward": 2, "cost": 3, "wait": 10},
    "view":  {"title": "بازدید",  "reward": 1, "cost": 2, "wait": 5},
}

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("taskbot")

app = Flask(__name__)
if MONGO_URI:
    db = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000).get_database("taskbot")
else:
    import mongomock
    db = mongomock.MongoClient().get_database("taskbot")
    log.warning("MONGO_URI تنظیم نشده؛ از حافظه‌ی موقت استفاده می‌شه و با هر ری‌استارت همه‌چی پاک می‌شه")
db.completions.create_index([("uid", 1), ("cid", 1)], unique=True)     # هر کاربر فقط یک‌بار برای هر کمپین
db.completions.create_index([("status", 1), ("due", 1)])
db.campaigns.create_index([("status", 1), ("remaining", 1)])
db.campaigns.create_index("owner")
db.reaction_watch.create_index([("chat_id", 1), ("mid", 1)])
db.reaction_credits.create_index([("post_key", 1), ("event_at", 1)])
db.completions.create_index([("status", 1), ("claim_expires", 1)])

HOOK_SECRET = hashlib.sha256(("hook" + SECRET_KEY).encode()).hexdigest()[:32]
_me = {}
_last_admin_check = 0.0


# ───────────────────────── ابزارهای تلگرام ─────────────────────────
def tg(method, **data):
    try:
        r = requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/{method}", json=data, timeout=20)
        return r.json()
    except Exception as e:
        log.warning("tg %s failed: %s", method, e)
        return {"ok": False, "description": str(e)}


def bot_id():
    if not _me.get("id"):
        r = tg("getMe")
        if r.get("ok"):
            _me.update(id=r["result"]["id"], username=r["result"].get("username", ""))
    return _me.get("id")


def btn(text, data=None, url=None, style=None):
    """ساخت دکمه با رنگ‌بندی معنایی؛ دکمه‌های کمکی ساده می‌مانند تا رابط شلوغ نشود."""
    label = str(text)
    if style is None:
        # رنگ‌ها برای اقدام‌های مهم استفاده می‌شوند؛ گزینه‌های اطلاعاتی/بازگشت ساده‌اند.
        if any(k in label for k in ("انصراف", "لغو", "حذف", "رد درخواست", "توقف")):
            style = "danger"
        elif any(k in label for k in ("انجام دادم", "ثبت و کسر", "تأیید", "تایید", "بله، لغو کن", "دریافت پاداش", "ساخت کمپین", "ثبت درخواست")):
            style = "success"
        elif any(k in label for k in ("شروع کسب کوین", "تسک بعدی", "انجام تسک", "ادامه", "مشاهده تسک", "ویترین تسک", "بازگشت به کانال", "رفتن به پست مقصد", "تسک‌های کانال", "۲ دقیقه", "۵ دقیقه")):
            style = "primary"
    out = {"text": text, "url": url} if url else {"text": text, "callback_data": data}
    if style in ("primary", "success", "danger"):
        out["style"] = style
    return out


_HTML_TAG_RE = re.compile(r"</?(?:b|strong|i|em|u|ins|s|strike|del|code|pre|blockquote)>|<a\s+href=(?:\"[^\"]*\"|'[^']*')\s*>|</a>", re.I)

def telegram_html(text):
    """HTML امن برای Bot API: متن معمولی escape می‌شود و فقط تگ‌های مجاز تلگرام حفظ می‌شوند."""
    import html
    raw = str(text or "")
    out, pos = [], 0
    for m in _HTML_TAG_RE.finditer(raw):
        out.append(html.escape(raw[pos:m.start()], quote=False))
        out.append(m.group(0))
        pos = m.end()
    out.append(html.escape(raw[pos:], quote=False))
    return "".join(out)


def send(chat, text, kb=None):
    p = {"chat_id": chat, "text": telegram_html(str(text)[:4000]), "parse_mode": "HTML", "disable_web_page_preview": True}
    if kb:
        p["reply_markup"] = {"inline_keyboard": kb}
    return tg("sendMessage", **p)


def view(chat, mid, text, kb=None):
    """پیام قبلی را ویرایش می‌کند تا منوها مرتب بمانند؛ در غیر این صورت پیام تازه می‌فرستد."""
    safe_text = telegram_html(str(text)[:4000])
    if mid:
        r = tg("editMessageText", chat_id=chat, message_id=mid, text=safe_text, parse_mode="HTML",
               disable_web_page_preview=True, reply_markup={"inline_keyboard": kb or []})
        if r.get("ok") or "not modified" in str(r.get("description", "")):
            return
    p = {"chat_id": chat, "text": safe_text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if kb:
        p["reply_markup"] = {"inline_keyboard": kb}
    return tg("sendMessage", **p)


def ans(cq, text=None, alert=False):
    p = {"callback_query_id": cq["id"]}
    if text:
        p.update(text=text[:200], show_alert=alert)
    tg("answerCallbackQuery", **p)


def member_ok(r):
    return r.get("status") in ("creator", "administrator", "member") or (r.get("status") == "restricted" and r.get("is_member"))


def bot_admin_status(chat_id):
    """True/False برای پاسخ معتبر تلگرام؛ None برای خطای موقت API."""
    r = tg("getChatMember", chat_id=chat_id, user_id=bot_id())
    if not r.get("ok") or not isinstance(r.get("result"), dict):
        return None
    return r["result"].get("status") in ("administrator", "creator")


def bot_is_admin(chat_id):
    return bot_admin_status(chat_id) is True


def campaign_needs_admin(c):
    # مسیر فعلی شمارش ری‌اکشن کانال عمداً از منطق جدید جدا نگه داشته می‌شود.
    if not c:
        return False
    # کمپین‌های قدیمی که پیش از افزودن این فیلد ساخته شده‌اند هم پوشش داده می‌شوند.
    required = c.get("admin_required")
    if required is None:
        required = c.get("type") == "join" or (c.get("type") == "react" and c.get("mode") == "auto")
    return bool(required and (c.get("type") == "join" or
                              (c.get("type") == "react" and c.get("mode") == "auto")))


def monitor_campaign_admins(now=None):
    """پایش دوره‌ای ادمین برای عضویت و ری‌اکشن گروه؛ خطای API وضعیت را تغییر نمی‌دهد."""
    global _last_admin_check
    now = time.time() if now is None else now
    if now - _last_admin_check < max(15, ADMIN_MONITOR_INTERVAL):
        return
    _last_admin_check = now
    query = {"status": "active", "$and": [
        {"$or": [{"admin_required": True}, {"admin_required": {"$exists": False}}]},
        {"$or": [{"type": "join"}, {"type": "react", "mode": "auto"}]}
    ]}
    for c in db.campaigns.find(query).sort([("admin_checked_at", 1)]).limit(100):
        state = bot_admin_status(c.get("chat_id"))
        if state is None:
            continue
        paused = bool(c.get("admin_paused"))
        if state and paused:
            db.campaigns.update_one({"_id": c["_id"], "status": "active"},
                {"$set": {"admin_paused": False, "admin_checked_at": now, "admin_restored_at": now},
                 "$unset": {"admin_lost_at": ""}})
            refresh_campaign_listing(c["_id"])
            send(c.get("owner"), f"✅ دسترسی ربات در «{c.get('title') or c.get('uname') or 'کمپین'}» برگشت؛ کمپین دوباره فعال است.")
        elif not state and not paused:
            db.campaigns.update_one({"_id": c["_id"], "status": "active"},
                {"$set": {"admin_paused": True, "admin_checked_at": now, "admin_lost_at": now}})
            refresh_campaign_listing(c["_id"])
            send(c.get("owner"), f"⛔ ربات دسترسی ادمین مقصد «{c.get('title') or c.get('uname') or 'کمپین'}» را از دست داده است. کمپین موقتاً متوقف شد و پس از بازیابی دسترسی ادامه می‌یابد.")
        else:
            db.campaigns.update_one({"_id": c["_id"], "status": "active"}, {"$set": {"admin_checked_at": now}})


def resolve_chat(uname):
    r = tg("getChat", chat_id="@" + uname)
    if r.get("ok") and r["result"].get("type") in ("channel", "group", "supergroup"):
        return r["result"]
    return None


# ───────────────────────── دیتابیس: کاربر، کوین، شمارنده ─────────────────────────
def day():
    return time.strftime("%Y-%m-%d", time.gmtime())


def ledger(uid, delta, kind, ref=""):
    db.ledger.insert_one({"uid": uid, "d": delta, "k": kind, "ref": str(ref), "t": time.time()})


def ensure_user(u):
    uid, name = u["id"], (u.get("first_name") or "")[:40]
    doc = db.users.find_one({"_id": uid})
    if not doc:
        try:
            db.users.insert_one({"_id": uid, "name": name, "username": u.get("username"), "points": START_POINTS,
                                 "hold": 0, "earned": 0, "strikes": 0, "skip": [],
                                 "joined": time.time()})
            if START_POINTS:
                ledger(uid, START_POINTS, "bonus")
        except DuplicateKeyError:
            pass
        doc = db.users.find_one({"_id": uid})
    elif doc.get("name") != name or doc.get("username") != u.get("username"):
        db.users.update_one({"_id": uid}, {"$set": {"name": name, "username": u.get("username")}})
    return doc


def add_points(uid, delta, kind, ref=""):
    db.users.update_one({"_id": uid}, {"$inc": {"points": delta}})
    ledger(uid, delta, kind, ref)


def spend(uid, amount, kind, ref=""):
    """کسر اتمی؛ فقط اگه موجودی کافی باشه"""
    r = db.users.find_one_and_update({"_id": uid, "points": {"$gte": amount}}, {"$inc": {"points": -amount}})
    if r:
        ledger(uid, -amount, kind, ref)
    return bool(r)


def counter_take(key, limit):
    try:
        db.counters.find_one_and_update({"_id": key, "n": {"$lt": limit}}, {"$inc": {"n": 1}}, upsert=True)
        return True
    except DuplicateKeyError:
        return False


def counter_get(key):
    d = db.counters.find_one({"_id": key})
    return d["n"] if d else 0


# ───────────────────────── متن‌ها و منوها ─────────────────────────
HOME_KB = [[btn("🏠 منوی اصلی", "home")]]


def tasks_channel_url():
    if TASKS_CHANNEL_URL:
        return TASKS_CHANNEL_URL
    name = TASKS_CHANNEL.lstrip("@")
    if name and not name.startswith("-") and re.fullmatch(r"[A-Za-z0-9_]{5,32}", name):
        return f"https://t.me/{name}"
    return ""


def bot_deep_link(cid):
    username = _me.get("username") or ""
    return f"https://t.me/{username}?start=task_{cid}" if username else ""


def task_listing_url(c):
    """لینک مستقیم همان پست تسک در کانال ویترین، با fallback به خود کانال."""
    mid = c.get("listing_message_id")
    name = TASKS_CHANNEL.lstrip("@")
    if mid and name and not name.startswith("-") and re.fullmatch(r"[A-Za-z0-9_]{5,32}", name):
        return f"https://t.me/{name}/{mid}"
    return tasks_channel_url()


def main_kb():
    rows = [[btn("🎯 شروع کسب کوین", "earn")],
            [btn("➕ ساخت کمپین", "new"), btn("📊 کمپین‌های من", "my")]]
    ch_url = tasks_channel_url()
    if ch_url:
        rows.append([btn("📣 ویترین تسک‌ها", url=ch_url)])
    rows += [[btn("📡 پایش ری‌اکشن‌ها", "rwatch")],
             [btn("👤 حساب کاربری", "acc"), btn("📘 راهنمای استفاده", "help")]]
    return rows


def menu_text(u):
    name = u.get("name") or "دوست من"
    hold = f"\n⏳ <b>پاداش‌های در انتظار:</b> {u['hold']} کوین" if u.get("hold") else ""
    channel_line = "\n📣 فهرست عمومی تسک‌ها از دکمهٔ <b>ویترین تسک‌ها</b> در دسترس است." if tasks_channel_url() else ""
    return (f"🤖 <b>به تاسک‌پلاس خوش اومدی، {name}!</b>\n\n"
            f"اینجا می‌تونی با انجام تسک‌های دیگران کوین بگیری و با کوینت برای کانال یا پست خودت کمپین بسازی.\n\n"
            f"💰 <b>موجودی قابل‌استفاده:</b> {u.get('points', 0)} کوین{hold}"
            f"{channel_line}\n\n<b>از کجا شروع کنیم؟</b> یکی از گزینه‌های پایین رو انتخاب کن؛ من قدم‌به‌قدم راهنماییت می‌کنم.")


def help_text():
    t = TYPES
    return (
        "📘 <b>راهنمای سریع تاسک‌پلاس</b>\n\n"
        "<b>🎯 کسب کوین</b>\nتسکی رو از فهرست انتخاب کن، دستورالعملش رو انجام بده و دکمهٔ «انجام دادم» رو بزن. نتیجه رو همین‌جا بهت اعلام می‌کنم.\n\n"
        f"<b>{t['join']['title']}</b> — پاداش {t['join']['reward']} کوین؛ عضویت با ربات بررسی می‌شه و در صورت تأیید، پاداش فوری واریز می‌شه.\n"
        f"<b>{t['react']['title']}</b> — پاداش {t['react']['reward']} کوین؛ در گروه، در صورت پشتیبانی تأیید فردی انجام می‌شه؛ در کانال، افزایش شمارش کل بررسی می‌شه.\n"
        f"<b>{t['view']['title']}</b> — پاداش {t['view']['reward']} کوین؛ زمان لازم رو رعایت کن.\n\n"
        "<b>➕ ساخت کمپین</b>\nنوع تسک، لینک مقصد و تعداد موردنظرت رو وارد کن. پیش از ثبت، هزینه و روش تأیید رو بهت نشون می‌دم. برای تأیید عضویت، ربات باید ادمین کانال یا گروه مقصد باشه.\n\n"
        "<b>📡 ری‌اکشن کانال؛ شفاف و مهم</b>\nدر این روش، افزایش شمارش کلی ری‌اکشن‌ها بررسی می‌شه؛ این شمارش هویت شخصی واکنش‌دهنده رو مشخص نمی‌کنه. تلگرام ممکنه آپدیت شمارش رو با تأخیر بفرسته، پس پرداخت فوری فقط زمانی انجام می‌شه که افزایش معتبر دریافت و به تسک اختصاص داده بشه.\n\n"
        f"<b>🛡️ رعایت قوانین</b>\nهر کاربر برای هر کمپین فقط یک بار می‌تونه درخواست ثبت کنه. تخلف یا برداشتن عضویت/ری‌اکشن در تسک‌های مشمول بازبینی ممکنه باعث برگشت کوین و اخطار بشه. سقف اخطار: {STRIKE_LIMIT}.\n\n"
        "برای دیدن تسک‌های موجود، «🎯 شروع کسب کوین» رو بزن؛ برای بررسی وضعیت کمپین‌های خودت، «📊 کمپین‌های من» رو انتخاب کن.")


def acc_text(u):
    n = db.campaigns.count_documents({"owner": u["_id"], "status": "active"})
    return ("👤 <b>حساب کاربری تو</b>\n\n"
            f"💰 <b>موجودی قابل‌استفاده:</b> {u.get('points', 0)} کوین\n"
            f"⏳ <b>پاداش در انتظار:</b> {u.get('hold', 0)} کوین\n"
            f"🏆 <b>مجموع کوین کسب‌شده:</b> {u.get('earned', 0)}\n"
            f"🛡️ <b>اخطارها:</b> {u.get('strikes', 0)} از {STRIKE_LIMIT}\n"
            f"📋 <b>کمپین‌های فعال تو:</b> {n} از {MAX_ACTIVE}\n\n"
            "هر زمان خواستی می‌تونی از منوی اصلی تسک انجام بدی یا کمپین تازه بسازی.")


def link_of(c):
    return f"https://t.me/{c['uname']}" + (f"/{c['mid']}" if c.get("mid") else "")


def task_text(c):
    t = TYPES[c["type"]]
    head = (f"{t['title']}  ·  <b>پاداش: {t['reward']} کوین</b>\n"
            f"📌 <b>مقصد:</b> {c.get('title') or c['uname']}\n\n")
    if c["type"] == "join":
        how = ("<b>مراحل انجام</b>\n۱. دکمهٔ «باز کردن مقصد» رو بزن.\n۲. عضو کانال یا گروه شو.\n۳. برگرد و «انجام دادم» رو انتخاب کن.\n\n"
               "⚡ عضویت خودکار بررسی می‌شه و در صورت تأیید، پاداش همون لحظه واریز می‌شه.")
    elif c["type"] == "view":
        how = ("<b>مراحل انجام</b>\n۱. مقصد رو باز کن و پست رو ببین.\n"
               f"۲. حداقل {t['wait']} ثانیه صبر کن.\n۳. به ربات برگرد و «انجام دادم» رو بزن.\n\n"
               "ℹ️ توجه: بازدید به‌تنهایی از طریق Bot API قابل اثبات قطعی نیست؛ این تسک بر اساس زمان ثبت‌شده مدیریت می‌شه.")
    elif c.get("mode") == "auto":
        how = ("<b>مراحل انجام</b>\n۱. پست مقصد رو باز کن.\n۲. ری‌اکشن خواسته‌شده رو ثبت کن.\n۳. به ربات برگرد و «انجام دادم» رو بزن.\n\n"
               "⚡ ری‌اکشن در صورت پشتیبانی تلگرام به‌صورت خودکار بررسی می‌شه و پس از تأیید، پاداش فوری واریز می‌شه.")
    elif c.get("mode") == "count":
        how = ("<b>مراحل انجام</b>\n۱. پست مقصد رو باز کن و ری‌اکشن بزن.\n"
               f"۲. حداکثر تا {c.get('window_min', 5)} دقیقه به ربات برگرد و «انجام دادم» رو انتخاب کن.\n\n"
               "📡 <b>نحوهٔ بررسی:</b> افزایش شمارش کل ری‌اکشن‌های پست بررسی می‌شه؛ این روش هویت فردی واکنش‌دهنده رو مشخص نمی‌کنه. اگر افزایش معتبر در بازهٔ تسک دریافت و به درخواستت اختصاص داده بشه، پاداش فوری واریز می‌شه.\n"
               "⏱ ممکنه خود تلگرام آپدیت شمارش رو با تأخیر ارسال کنه؛ تا پایان بررسی، پیام وضعیت رو نگه دار.")
    else:
        how = ("<b>مراحل انجام</b>\n۱. پست مقصد رو باز کن و ری‌اکشن بزن.\n"
               "۲. اسکرین‌شاتی بگیر که نام کانال، پست و ری‌اکشن انتخاب‌شده مشخص باشه.\n"
               f"۳. به ربات برگرد، «انجام دادم» رو بزن و عکس رو ارسال کن.\n\n⏱ مهلت ارسال عکس: {SHOT_WINDOW // 60} دقیقه.")
    return head + how


# ───────────────────────── تسک‌ها: انتخاب، ثبت انجام، تأیید نهایی ─────────────────────────
def post_key(chat_id, message_id):
    return f"{chat_id}:{message_id}"


def fresh_reaction_snapshot(chat_id, message_id, max_age=REACTION_BASELINE_MAX_AGE):
    w = db.reaction_watch.find_one({"_id": post_key(chat_id, message_id)})
    if not w or w.get("total") is None or not w.get("updated_at"):
        return None
    if time.time() - float(w["updated_at"]) > max_age:
        return None
    return w


def next_task(uid, skip):
    done = [x["cid"] for x in db.completions.find({"uid": uid}, {"cid": 1})]
    ids = done + [ObjectId(s) for s in skip if ObjectId.is_valid(s)]
    candidates = db.campaigns.find({"status": "active", "remaining": {"$gt": 0}, "owner": {"$ne": uid},
                                    "_id": {"$nin": ids}}, sort=[("created", 1)])
    for c in candidates:
        if c.get("admin_paused") and campaign_needs_admin(c):
            continue
        if c.get("type") == "react" and c.get("mode") == "count":
            if not fresh_reaction_snapshot(c.get("chat_id"), c.get("mid")):
                continue
        return c
    return None


def show_task(uid, chat, mid=None):
    """مسیر کسب کوین فقط از کانال است؛ فهرست و لینک پست مقصد داخل ربات نمایش داده نمی‌شود."""
    ch_url = tasks_channel_url()
    if not ch_url:
        return view(chat, mid, "⚠️ کانال تسک‌ها هنوز تنظیم نشده است. لطفاً بعداً دوباره تلاش کن.", HOME_KB)
    return view(chat, mid,
                "📣 <b>تسک‌ها در کانال انجام می‌شوند.</b>\n\n"
                "تسک را در کانال کامل کن؛ سپس در همان پست روی «🎁 دریافت پاداش» بزن تا درخواستت ثبت شود.",
                [[btn("📣 مشاهده تسک‌های کانال", url=ch_url, style="primary")], [btn("🏠 منوی اصلی", "home")]])


def campaign_listing_text(c):
    t = TYPES.get(c.get("type"), {"title": "📌 تسک"})
    remaining = max(0, int(c.get("remaining", 0)))
    target = c.get("title") or c.get("uname") or "تسک جدید"
    if c.get("status") == "cancelled":
        state = "🔴 لغو شده"
    elif c.get("admin_paused") and campaign_needs_admin(c):
        state = "⛔ توقف موقت: دسترسی ربات"
    elif c.get("status") == "active" and remaining:
        state = "🟢 فعال"
    else:
        state = "⚪ تکمیل ظرفیت"

    if c.get("type") == "react" and c.get("mode") == "count":
        instruction = f"ری‌اکشن درخواستی را روی پست بزن و تا {int(c.get('window_min', 5))} دقیقه درخواست پاداش را ثبت کن."
    elif c.get("type") == "join":
        instruction = "از طریق پست مقصد عضو کانال شو؛ سپس درخواست پاداش را ثبت کن."
    elif c.get("type") == "view":
        instruction = "پست مقصد را ببین و زمان تعیین‌شده را رعایت کن؛ سپس درخواست پاداش را ثبت کن."
    else:
        instruction = "دستور تسک را در پست مقصد کامل کن؛ سپس درخواست پاداش را ثبت کن."

    return (f"<b>{t['title']}</b>\n"
            f"📌 {target}\n\n"
            f"🎁 <b>پاداش:</b> {c.get('reward', 0)} کوین  ·  👥 <b>ظرفیت:</b> {remaining} نفر\n"
            f"📍 <b>وضعیت:</b> {state}\n\n"
            f"📝 {instruction}\n\n"
            "پس از انجام کار، از همین پست «🎁 دریافت پاداش» را بزن.")

def campaign_listing_keyboard(c):
    # تسک تمام‌شده در کانال دکمهٔ مقصد/پاداش ندارد؛ فقط نشانگر قرمز می‌ماند.
    rows = []
    available = (c.get("status") == "active" and int(c.get("remaining", 0)) > 0
                 and not (c.get("admin_paused") and campaign_needs_admin(c)))
    if not available:
        return [[btn("🔴 تکمیل شده", "noop", style="danger")]]
    target_link = link_of(c)
    if target_link:
        rows.append([btn("رفتن به پست مقصد", url=target_link, style="primary")])
    deep = bot_deep_link(c.get("_id"))
    if deep:
        rows.append([btn("دریافت پاداش", url=deep, style="success")])
    return rows


def publish_campaign(c):
    """انتشار تسک در کانال عمومی؛ تسک بدون انتشار موفق در کانال قابل انجام نیست."""
    if not TASKS_CHANNEL:
        log.warning("TASKS_CHANNEL تنظیم نشده؛ کمپین %s در کانال منتشر نشد", c.get("_id"))
        return False
    r = tg("sendMessage", chat_id=TASKS_CHANNEL, text=telegram_html(campaign_listing_text(c)), parse_mode="HTML",
           disable_web_page_preview=True, reply_markup={"inline_keyboard": campaign_listing_keyboard(c)})
    if not r.get("ok"):
        log.error("publish_campaign failed for %s: %s", c.get("_id"), r.get("description"))
        return False
    result = r.get("result", {})
    db.campaigns.update_one({"_id": c["_id"]}, {"$set": {"listing_chat_id": result.get("chat", {}).get("id"),
                                                              "listing_message_id": result.get("message_id"),
                                                              "listed_at": time.time()}})
    return True


def refresh_campaign_listing(cid):
    c = db.campaigns.find_one({"_id": cid})
    if not c or not c.get("listing_chat_id") or not c.get("listing_message_id"):
        return
    tg("editMessageText", chat_id=c["listing_chat_id"], message_id=c["listing_message_id"],
       text=telegram_html(campaign_listing_text(c)), parse_mode="HTML", disable_web_page_preview=True,
       reply_markup={"inline_keyboard": campaign_listing_keyboard(c)})


def grant(uid, c, status, extra=None):
    """گرفتن یک ظرفیت از کمپین + ثبت انجام. خروجی: (کد، comp)  کد: ok | full | dup | limit"""
    if not counter_take(f"d:{uid}:{day()}", DAILY_TASKS):
        return "limit", None
    r = db.campaigns.find_one_and_update({"_id": c["_id"], "status": "active", "remaining": {"$gt": 0}},
                                         {"$inc": {"remaining": -1}})
    if not r:
        db.counters.update_one({"_id": f"d:{uid}:{day()}"}, {"$inc": {"n": -1}})
        return "full", None
    comp = {"uid": uid, "cid": c["_id"], "owner": c["owner"], "type": c["type"], "reward": c["reward"],
            "status": status, "t": time.time(), "due": time.time() + HOLD_HOURS * 3600}
    if extra:
        comp.update(extra)
    try:
        comp["_id"] = db.completions.insert_one(comp).inserted_id
    except DuplicateKeyError:
        db.campaigns.update_one({"_id": c["_id"]}, {"$inc": {"remaining": 1}})
        db.counters.update_one({"_id": f"d:{uid}:{day()}"}, {"$inc": {"n": -1}})
        return "dup", None
    if status == "pending":
        db.users.update_one({"_id": uid}, {"$inc": {"hold": c["reward"]}})
    refresh_campaign_listing(c["_id"])
    if r["remaining"] == 1 and status != "count_pending":
        send(c["owner"], f"🎉 کمپین «{c.get('title') or c['uname']}» ({TYPES[c['type']]['title']}) تکمیل شد!\n"
                         f"تأیید نهایی طی {HOLD_HOURS:g} ساعت انجام می‌شه و اگه کسی تخلف کنه ظرفیتش دوباره باز می‌شه.")
    return "ok", comp


def claim_text(code, c):
    return {"limit": f"✋ سقف {DAILY_TASKS} تسک امروز پر شده.",
            "full": "این تسک همین الان تکمیل شد 😅 برو سراغ بعدی.",
            "dup": "این تسک رو قبلاً انجام دادی."}[code]


def after_ok(cq, chat, mid, c, comp=None):
    """تسک بازدید بعد از گذشت زمان لازم فوراً پاداش می‌گیرد؛ بازدید واقعی قابل اثبات قطعی نیست."""
    ans(cq, "🎉 پاداش واریز شد")
    if comp:
        moved = db.completions.update_one(
            {"_id": comp["_id"], "status": "pending"},
            {"$set": {"status": "settling", "verified_at": time.time(), "due": time.time(), "lock": time.time()}}
        )
        if moved.modified_count:
            pay(comp, notify=False)
    return view(chat, mid,
         f"🎉 <b>تسک ثبت شد و پاداش پرداخت شد!</b>\n\n"
         f"💰 {c['reward']} کوین به موجودی‌ات اضافه شد.",
         [[btn("🎯 تسک بعدی", "earn", style="success")], [btn("👤 حساب کاربری", "acc", style="primary")]])


def on_claim(cq, uid, chat, mid, cid_s):
    try:
        cid = ObjectId(cid_s)
    except InvalidId:
        return ans(cq, "نامعتبر", True)
    c = db.campaigns.find_one({"_id": cid})
    u = db.users.find_one({"_id": uid}) or {}
    if not c or c["status"] != "active" or c["remaining"] <= 0:
        ans(cq, "این تسک دیگه ظرفیت نداره", True)
        return show_task(uid, chat, mid)
    if campaign_needs_admin(c):
        if c.get("admin_paused"):
            return ans(cq, "این کمپین موقتاً متوقف شده؛ دسترسی ربات در حال بازیابی است.", True)
        admin_status = bot_admin_status(c.get("chat_id"))
        if admin_status is None:
            return ans(cq, "بررسی دسترسی مقصد موقتاً ممکن نیست؛ کمی بعد دوباره امتحان کن.", True)
        if not admin_status:
            db.campaigns.update_one({"_id": c["_id"], "status": "active"}, {"$set": {"admin_paused": True, "admin_lost_at": time.time()}})
            refresh_campaign_listing(c["_id"])
            send(c.get("owner"), f"⛔ کمپین «{c.get('title') or c.get('uname') or 'کمپین'}» به‌دلیل ازدست‌رفتن دسترسی ادمین متوقف شد.")
            return ans(cq, "کمپین موقتاً متوقف است؛ لطفاً بعداً دوباره امتحان کن.", True)
    if u.get("strikes", 0) >= STRIKE_LIMIT or c["owner"] == uid:
        return ans(cq, "امکان انجام این تسک نیست", True)
    if db.completions.find_one({"uid": uid, "cid": cid}):
        ans(cq, "این تسک رو قبلاً انجام دادی", True)
        return show_task(uid, chat, mid)
    ses = db.sessions.find_one({"_id": f"{uid}:{cid}"})
    if not ses:
        ans(cq, "اول تسک رو از منو باز کن", True)
        return show_task(uid, chat, mid)
    wait = 0 if (c.get("type") == "react" and c.get("mode") == "count") else TYPES[c["type"]]["wait"] - (time.time() - ses["t"])
    if wait > 0:
        return ans(cq, f"⏱ هنوز {int(wait) + 1} ثانیه صبر کن و دوباره بزن", True)

    if c["type"] == "join":
        r = tg("getChatMember", chat_id=c["chat_id"], user_id=uid)
        if not r.get("ok"):
            return ans(cq, "الان امکان بررسی نیست؛ یه تسک دیگه رو امتحان کن.", True)
        if not member_ok(r["result"]):
            return ans(cq, "هنوز عضو نشدی! اول عضو شو، بعد دکمه رو بزن.", True)
    elif c["type"] == "react" and c.get("mode") == "auto":
        d = db.reacts.find_one({"_id": f"{c['chat_id']}:{c['mid']}:{uid}"})
        if not (d and d.get("on")):
            return ans(cq, "ری‌اکشنی از تو ثبت نشده. روی پست ری‌اکشن بزن، چند ثانیه صبر کن و دوباره بزن.", True)
    elif c["type"] == "react" and c.get("mode") == "count":
        now = time.time()
        window_min = int(c.get("window_min", 5))
        started = float(ses.get("task_started", float(ses.get("t", now)) - window_min * 60))
        deadline = float(ses.get("task_deadline", now))
        if now > deadline:
            return ans(cq, f"مهلت {window_min} دقیقه‌ای این تسک تمام شده؛ از کانال دوباره شروع کن.", True)
        if ses.get("baseline_total") is None or not ses.get("post_key"):
            return ans(cq, "شمارش پایه در دسترس نیست؛ تسک را دوباره باز کن.", True)
        extra = {"post_key": ses["post_key"], "baseline_total": int(ses["baseline_total"]),
                 "baseline_at": float(ses.get("baseline_at", 0)), "task_started": started,
                 "task_deadline": deadline, "claim_expires": deadline,
                 "day_key": f"d:{uid}:{day()}"}
        code, comp = grant(uid, c, "count_pending", extra=extra)
        if code != "ok":
            ans(cq, claim_text(code, c), True)
            return show_task(uid, chat, mid)
        ans(cq, "در انتظار تأیید شمارش")
        if consume_credit_for_claim(comp):
            return view(chat, mid, "✅ افزایش شمارش تأیید شد؛ پاداش همین حالا پرداخت شد.",
                        [[btn("🎯 تسک بعدی", "earn")], [btn("🏠 منو", "home")]])
        return view(chat, mid, f"🕓 <b>درخواستت ثبت شد و در حال بررسی است.</b>\n\n"
                               f"🎁 پاداش: {c['reward']} کوین\n"
                               f"⏱ مهلت انجام: {window_min} دقیقه\n\n"
                               f"منتظر آپدیت شمارش تلگرام هستیم؛ ممکن است چند دقیقه طول بکشد (حداکثر فرصت اضافه: {REACTION_UPDATE_GRACE // 60} دقیقه). "
                               "اگر افزایش معتبر شمارش در بازهٔ تسک ثبت شود، پاداش به‌صورت خودکار و فوری واریز می‌شود و پیام تأیید می‌گیری.",
                    [[btn("↩️ بازگشت به انجام مأموریت", url=link_of(c), style="primary")], [btn("🏠 منوی اصلی", "home")]])
    elif c["type"] == "react":
        db.states.replace_one({"_id": uid}, {"_id": uid, "step": "shot", "cid": str(cid), "t": time.time()}, upsert=True)
        ans(cq)
        return view(chat, mid, "📸 حالا اسکرین‌شات پست رو همین‌جا بفرست.\n"
                               "باید اسم کانال، پست و ری‌اکشن انتخاب‌شده‌ی خودت توش دیده بشه.\n"
                               f"مهلت: {SHOT_WINDOW // 60} دقیقه.", [[btn("❌ انصراف", "home")]])

    # عضویت و ری‌اکشنِ قابل‌تأیید با Bot API همان لحظه بررسی شده‌اند؛
    # برای این دو نوع تسک پاداش را بی‌دلیل ۲۴ ساعت معلق نکن.
    instant = (c.get("type") in ("join", "view") or
               (c.get("type") == "react" and c.get("mode") == "auto"))
    code, comp = grant(uid, c, "pending")
    if code != "ok":
        ans(cq, claim_text(code, c), True)
        return show_task(uid, chat, mid)
    if instant:
        moved = db.completions.update_one(
            {"_id": comp["_id"], "status": "pending"},
            {"$set": {"status": "settling", "lock": time.time()}}
        )
        if moved.modified_count:
            pay(comp, notify=False)
        ans(cq, "✅ بررسی شد و پاداش واریز شد")
        return view(chat, mid,
                    f"<b>تسک تکمیل شد و پاداش پرداخت شد.</b>\n\n{c['reward']} کوین به موجودی‌ات اضافه شد.",
                    [[btn("🎯 تسک بعدی", "earn", style="success")],
                     [btn("👤 حساب کاربری", "acc", style="primary")]])
    after_ok(cq, chat, mid, c, comp)


# ───────────────────────── اسکرین‌شات: ضدتکرار + مدل ویژن ─────────────────────────
def fetch_file(file_id):
    r = tg("getFile", file_id=file_id)
    if not r.get("ok"):
        return None
    try:
        x = requests.get(f"https://api.telegram.org/file/bot{BOT_TOKEN}/{r['result']['file_path']}", timeout=25)
        return x.content if x.status_code == 200 and len(x.content) < 8_000_000 else None
    except Exception:
        return None


def img_hash(b):
    """هش میانگین ۱۶×۱۶ (۲۵۶ بیتی)؛ برای تشخیص عکس تکراریِ ریسایز/فشرده‌شده. بدون Pillow غیرفعاله"""
    try:
        from PIL import Image
        px = list(Image.open(io.BytesIO(b)).convert("L").resize((16, 16)).getdata())
        avg = sum(px) / len(px)
        return "%064x" % int("".join("1" if p > avg else "0" for p in px), 2)
    except Exception:
        return None


def ham(a, b):
    return bin(int(a, 16) ^ int(b, 16)).count("1")


def time_ok(s, since=None):
    """ساعت گوشی در عکس باید داخل پنجره‌ی «از ‹TIME_TOL› دقیقه قبل از زدن «انجام دادم» تا الان» باشه (آینده فقط ۲ دقیقه).
    since = زمان زدن «انجام دادم» (epoch). ساعت ۱۲ و ۲۴ ساعته هر دو پوشش داده می‌شن. ناخونا = False"""
    m = re.match(r"^\s*(\d{1,2}):(\d{2})", str(s or ""))
    if not m:
        return False
    try:
        from zoneinfo import ZoneInfo
        ref = datetime.now(ZoneInfo(TZ_NAME))
    except Exception:
        ref = datetime.now(timezone(timedelta(hours=3, minutes=30)))
    past = TIME_TOL + (max(0.0, (time.time() - since) / 60) if since else 0)
    mod = 1440 if int(m[1]) >= 13 else 720
    d = (ref.hour * 60 + ref.minute - int(m[1]) * 60 - int(m[2])) % mod
    return d <= past or d >= mod - 2


def now_str():
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo(TZ_NAME)).strftime("%H:%M")
    except Exception:
        return datetime.now(timezone(timedelta(hours=3, minutes=30))).strftime("%H:%M")


_mlock = threading.Lock()
_morder = [e["label"] for e in AI_ENDPOINTS]
_mstat = {e["label"]: {"fails": 0, "until": 0.0, "err": "", "ms": None} for e in AI_ENDPOINTS}
_mby = {e["label"]: e for e in AI_ENDPOINTS}


def _model_list():
    """مدل‌های آماده به ترتیب اولویت؛ مدل‌هایی که تازه خراب شدن (در استراحت) آخر صف می‌رن و فقط اگه بقیه جواب ندادن امتحان می‌شن"""
    now = time.time()
    with _mlock:
        ready = [m for m in _morder if _mstat[m]["until"] <= now]
        cool = sorted((m for m in _morder if _mstat[m]["until"] > now), key=lambda m: _mstat[m]["until"])
    return ready + cool


def _mark_fail(m, err, hard=False):
    with _mlock:
        st = _mstat[m]
        st["fails"] += 1
        st["err"] = err[:160]
        st["until"] = time.time() + (600 if hard else min(30 * 2 ** (st["fails"] - 1), 600))
        if m in _morder:
            _morder.remove(m)
            _morder.append(m)                      # مدل خراب می‌ره آخر صف
    log.warning("AI model %s failed (%s) -> moved to end of queue", m, err[:120])


def _mark_ok(m, ms):
    with _mlock:
        _mstat[m].update(fails=0, until=0.0, err="", ms=ms)


def _json_ok(txt):
    m = re.search(r"\{.*\}", txt or "", re.S)
    try:
        json.loads(m.group(0))
        return True
    except Exception:
        return False


def _ai_call(e, msgs, timeout):
    """یک تماس با یک مدل. خروجی: (متن، خطا، hard) ؛ hard = خطای دائمی (کلید/نام مدل غلط) → استراحت طولانی"""
    url = e["base"] + "/chat/completions"
    base = {"model": e["model"], "messages": msgs}
    err, hard = "", False
    # بعضی مدل‌ها temperature/max_tokens رو قبول نمی‌کنن؛ روی خطای ۴۰۰ با پارامترهای ساده‌تر دوباره امتحان می‌شه
    for body in (dict(base, temperature=0, max_tokens=600), dict(base, max_completion_tokens=600), base):
        try:
            r = requests.post(url, headers={"Authorization": "Bearer " + e["key"]}, json=body, timeout=timeout)
        except Exception as ex:
            return None, f"اتصال/تایم‌اوت: {str(ex)[:120]}", False
        try:
            j = r.json()
        except Exception:
            return None, f"HTTP {r.status_code} پاسخ غیر JSON: {getattr(r, 'text', '')[:120]}", r.status_code in (401, 403, 404)
        if r.status_code == 200 and j.get("choices"):
            c = (j["choices"][0].get("message") or {}).get("content")
            if isinstance(c, list):
                c = " ".join(p.get("text", "") for p in c if isinstance(p, dict))
            if c and str(c).strip():
                if _json_ok(str(c)):
                    return str(c), "", False
                return None, f"پاسخ JSON نبود: {str(c)[:100]}", False
            err = f"پاسخ خالی: {str(j)[:150]}"
        else:
            err = f"HTTP {r.status_code}: {str(j)[:200]}"
            hard = r.status_code in (401, 403, 404)
        if r.status_code != 400:
            break
    return None, err, hard


def ai_request(prompt, img, mime="image/jpeg", only=None, budget=None):
    """تماس با مدل‌ها به ترتیب اولویت؛ هر مدلی خطا/تایم‌اوت/پاسخ غیر JSON بده، می‌ره آخر صف و فوری مدل بعدی امتحان می‌شه.
    خروجی: (متن، خطا). only = فقط همین مدل (برای /aitest)"""
    imgs = img if isinstance(img, list) else [img]
    msgs = [{"role": "user", "content": [{"type": "text", "text": prompt}] + [
        {"type": "image_url", "image_url": {"url": f"data:{mime};base64," + base64.b64encode(x).decode()}} for x in imgs]}]
    deadline, errs = time.time() + (budget or AI_BUDGET), []
    for m in ([only] if only else _model_list()):
        remain = deadline - time.time()
        if remain < 3:
            errs.append("سقف زمانی تموم شد")
            break
        t0 = time.time()
        txt, err, hard = _ai_call(_mby[m], msgs, min(AI_TIMEOUT, remain))
        if txt:
            _mark_ok(m, int((time.time() - t0) * 1000))
            return txt, ""
        _mark_fail(m, err, hard)
        errs.append(f"{m}: {err}")
    return None, " | ".join(errs)[:900] or "هیچ مدلی تنظیم نشده"


_ai_alert = {"t": 0}


def ai_alert(err):
    log.warning("ai error: %s", err)
    if time.time() - _ai_alert["t"] > 600:
        _ai_alert["t"] = time.time()
        for a in ADMINS:
            send(a, "⚠️ خطای مدل ویژن (دستور /aitest برای تست دستی):\n" + err[:800])


def small_jpeg(b, maxw=900):
    """کوچک‌کردن عکس قبل از ارسال به مدل (سریع‌تر و پایدارتر)؛ تشخیص پیکسلی روی عکس اصلی انجام می‌شه"""
    try:
        from PIL import Image
        im = Image.open(io.BytesIO(b)).convert("RGB")
        if im.width > maxw:
            im = im.resize((maxw, int(im.height * maxw / im.width)), Image.LANCZOS)
        out = io.BytesIO()
        im.save(out, "JPEG", quality=88)
        return out.getvalue()
    except Exception:
        return b


def ai_retry(fn, *a):
    """یک بار تکرار روی خطای موقت (قطعی شبکه، پاسخ خالی، JSON خراب)"""
    r, err = fn(*a)
    if r is None and "تنظیم نشده" not in (err or ""):
        time.sleep(1.5)
        r, err = fn(*a)
    return r, err


def parse_json(txt):
    m = re.search(r"\{.*\}", txt or "", re.S)
    try:
        return json.loads(m.group(0)), ""
    except Exception:
        return None, f"پاسخ مدل JSON نبود: {(txt or '')[:300]}"


def ref_info(c):
    """توضیح پست هدف برای پرامپت + عکس مرجع. خروجی: (لیست توضیح، لیست عکس‌های اضافه، خطا)"""
    desc, extra = [], []
    if c.get("hint"):
        desc.append(f'the post text starts with: "{c["hint"]}"')
    if c.get("ref_photo"):
        ref = fetch_file(c["ref_photo"])
        if not ref:
            return desc, extra, "نتونستم عکس مرجع پست رو بگیرم"
        extra.append(small_jpeg(ref, 600))
        desc.append("the reference image is the original post media")
    return desc, extra, ""


def ai_check(img, c):
    """بررسی کلی و ساده: اسکرین‌شات تلگرامه و کانال درسته؟ (پست و ساعت اینجا نیست). خروجی: (دیکشنری یا None، خطا)"""
    if not AI_ON:
        return None, "AI_BASE_URL / AI_API_KEY / AI_MODEL تنظیم نشده"
    prompt = (
        "This is a user-submitted Telegram screenshot. Light and dark themes are both valid.\n"
        f'Expected channel/group title: "{c.get("title") or ""}" (@{c["uname"]}).\n'
        "Answer ONLY with one JSON object, no markdown:\n"
        '{"is_telegram_screenshot": true|false, "channel_match": true|false|null, "confidence": 0.0-1.0}\n'
        "- is_telegram_screenshot: a genuine Telegram app screen, not an obviously edited/fake image or a meme.\n"
        "- channel_match: the channel/group name shown in the top header matches the expected title (same name, small "
        "differences in emoji/decoration are fine); false if a clearly different name; null if no name is visible.\n"
        "- confidence: how readable the screenshot is (never lowered by the theme).")
    txt, err = ai_request(prompt, [img])
    if not txt:
        return None, err
    return parse_json(txt)


def status_crop(b, mode=0):
    """برش نوار وضعیت بالای عکس و بزرگ‌کردنش تا مدل فقط ساعت رو بخونه. سه برش متفاوت برای خوندن مستقل"""
    from PIL import Image
    im = Image.open(io.BytesIO(b)).convert("RGB")
    W, H = im.size
    fw, fh, k = ((1.0, 0.055, 1.5), (0.5, 0.055, 2.0), (0.6, 0.07, 2.5))[mode]
    im = im.crop((0, 0, int(W * fw), max(40, int(H * fh))))
    im = im.resize((int(im.width * k), int(im.height * k)), Image.LANCZOS)
    out = io.BytesIO()
    im.save(out, "JPEG", quality=92)
    return out.getvalue()


def ai_clock(b, mode=0):
    """خوندن ساعت نوار وضعیت گوشی از برش بالای عکس. خروجی: (رشته‌ی H:MM یا None، خطا)"""
    prompt = (
        "This is the top strip of a phone screenshot. Read the phone clock from the system status bar "
        "(the digits next to the small status icons). Ignore any faded or blurred text behind it. "
        'Answer ONLY with JSON: {"clock": "HH:MM"|null}. Use null unless you can read all the digits clearly; never guess.')
    try:
        txt, err = ai_request(prompt, [status_crop(b, mode)], budget=CLOCK_BUDGET)
    except Exception as e:
        return None, str(e)
    if not txt:
        return None, err
    d, err = parse_json(txt)
    if not isinstance(d, dict):
        return None, err
    m = re.match(r"^\s*(\d{1,2}):(\d{2})\s*$", str(d.get("clock") or ""))
    return (f"{int(m[1])}:{m[2]}" if m else None), ""


def find_chips(b):
    """بررسی پیکسلی بدون وابستگی به مدل: ری‌اکشنِ زده‌شده‌ی کاربر در تلگرام یه چیپ قرصی‌شکلِ پر و پررنگ (رنگ اکسنت) می‌شه؛
    چیپ‌های نزده در همه‌ی تم‌ها کم‌رنگ/شفافن. خروجی: لیست جعبه‌ها (x0,y0,x1,y1 به‌صورت کسری از ابعاد عکس)،
    لیست خالی = هیچ چیپ پررنگی نیست = ری‌اکشن نخورده؛ None = بررسی ممکن نشد (Pillow نیست یا خطا)"""
    try:
        from PIL import Image
        im = Image.open(io.BytesIO(b)).convert("RGB")
        W = 360
        im = im.resize((W, max(1, int(im.height * W / im.width))), Image.BILINEAR)
        hsv = im.convert("HSV")
        S, V = hsv.getdata(band=1), hsv.getdata(band=2)
        w, h = im.size
        mask = bytearray(w * h)
        for i in range(w * h):
            if S[i] > 130 and V[i] > 130:
                mask[i] = 1
        out = []
        for i0 in range(w * h):
            if mask[i0] != 1:
                continue
            st, mask[i0] = [i0], 2
            n, x0, x1, y0, y1 = 0, w, 0, h, 0
            while st:
                i = st.pop()
                x, y = i % w, i // w
                n += 1
                x0, x1, y0, y1 = min(x0, x), max(x1, x), min(y0, y), max(y1, y)
                for j in (i - 1 if x else -1, i + 1 if x < w - 1 else -1, i - w, i + w if y < h - 1 else -1):
                    if 0 <= j < w * h and mask[j] == 1:
                        mask[j] = 2
                        st.append(j)
            bw, bh = x1 - x0 + 1, y1 - y0 + 1
            if 0.045 * w <= bh <= 0.11 * w and 1.4 <= bw / bh <= 4.5 and n / (bw * bh) >= 0.55 and n >= 0.012 * w * w / 4:
                out.append((x0 / w, y0 / h, (x1 + 1) / w, (y1 + 1) / h))
        out.sort(key=lambda t: t[1])
        return out[:6]
    except Exception as e:
        log.warning("pixel check failed: %s", e)
        return None


def mark_chips(b, boxes):
    """روی هر چیپ پررنگِ پیداشده یه کادر قرمز و شماره می‌کشه تا مدل فقط بگه هر شماره مال کدوم پسته (سؤال ساده‌تر از مختصات)"""
    from PIL import Image, ImageDraw, ImageFont
    im = Image.open(io.BytesIO(small_jpeg(b))).convert("RGB")
    W, H = im.size
    d = ImageDraw.Draw(im)
    try:
        font = ImageFont.load_default(size=max(18, W // 28))
    except TypeError:
        font = ImageFont.load_default()
    r = max(14, W // 40)
    for k, (x0, y0, x1, y1) in enumerate(boxes, 1):
        X0, Y0, X1, Y1 = x0 * W, y0 * H, x1 * W, y1 * H
        d.rectangle([X0 - 3, Y0 - 3, X1 + 3, Y1 + 3], outline=(255, 0, 0), width=4)
        cx, cy = X0 - r, max(r, Y0 - r)
        cx = max(r, cx)
        d.ellipse([cx - r, cy - r, cx + r, cy + r], fill=(255, 0, 0))
        d.text((cx, cy), str(k), fill=(255, 255, 255), font=font, anchor="mm")
    out = io.BytesIO()
    im.save(out, "JPEG", quality=88)
    return out.getvalue()


def ai_owner_check(img, boxes, c):
    """هر چیپ پررنگِ شماره‌دار مال همون پست تسکه یا پست دیگه؟ خروجی: (دیکشنری یا None، خطا)"""
    desc, extra, err = ref_info(c)
    if err:
        return None, err
    prompt = (
        "The FIRST image is a Telegram screenshot. Some reaction chips are marked with a red box and a red numbered tag. "
        f'Target post: {"; ".join(desc) or "unknown"}. If a second image is attached it is that post\'s original media.\n'
        "For EACH numbered chip decide whether it sits in the SAME message bubble as the target post (the target's own reaction "
        "row, right above that bubble's eye/views counter and time) or belongs to a DIFFERENT post (an older/newer bubble above "
        "or below, or a different message). A chip directly above the target's top edge or under the next bubble is a different post.\n"
        'Answer ONLY one JSON object, no markdown: {"tags": [{"n": 1, "own_post": true|false}]}\n'
        "If you are not sure about a tag, own_post=false.")
    txt, err = ai_request(prompt, [mark_chips(img, boxes)] + extra)
    if not txt:
        return None, err
    return parse_json(txt)


def ai_reaction_check(img, c):
    """جایگزین وقتی تشخیص پیکسلی ممکن نیست: فقط مدل (کم‌اعتمادتر). خروجی: (دیکشنری یا None، خطا)"""
    desc, extra, err = ref_info(c)
    if err:
        return None, err
    prompt = (
        "The FIRST image is a Telegram screenshot (any theme). "
        f'Target post: {"; ".join(desc) or "unknown"}.\n'
        "Look ONLY at the reaction chips inside the SAME message bubble as the target post (right above its views/time footer); "
        "ignore other posts. Compare those chips with each other: unselected ones share one calm tinted look, the user's own "
        "selected chip is a strong solid saturated accent fill. If all look the same, nobody reacted.\n"
        'Answer ONLY one JSON object: {"selected_emoji": "<emoji>"|null}. null if unsure, if all look alike or if no chips.')
    txt, err = ai_request(prompt, [small_jpeg(img)] + extra)
    if not txt:
        return None, err
    return parse_json(txt)


def judge(v, has_hint=True):
    """تصمیم روی بررسی کلی ساده: اسکرین‌شات تلگرام + کانال (نامطمئن = رد)"""
    if not isinstance(v, dict):
        return "reject", "بررسی عکس الان ممکن نشد؛ چند لحظه بعد دوباره امتحان کن"
    try:
        conf = float(v.get("confidence") or 0)
    except (TypeError, ValueError):
        conf = 0
    if conf < CONF_MIN:
        return "reject", "عکس واضح نیست"
    if v.get("is_telegram_screenshot") is not True:
        return "reject", "این اسکرین‌شات تلگرام نیست"
    if v.get("channel_match") is not True:
        return "reject", "اسم کانال بالای عکس دیده نمی‌شه"
    return "approve", ""


def _mins(t):
    m = re.match(r"^(\d{1,2}):(\d{2})", t)
    return int(m[1]) * 60 + int(m[2])


def clock_decide(reads, since=None):
    """سه خوندن مستقل از ساعت. قبول فقط با شواهد مثبت: خوندن‌های همخوان با الان باید بیشتر از ناهمخوان‌ها باشن.
    هیچ خوندنی/مساوی = رد (بسته). خروجی: None (قبول) یا متن دلیل رد"""
    reads = [x for x in reads if x]
    good = [t for t in reads if time_ok(t, since)]
    bad = [t for t in reads if not time_ok(t, since)]
    if len(good) > len(bad):
        return None
    if not reads:
        return "ساعت بالای گوشی تو عکس خونده نشد؛ اسکرین‌شات کامل با نوار بالای گوشی بگیر"
    if good:
        return "ساعت عکس واضح خونده نشد؛ اسکرین‌شات تازه و کامل بگیر"
    return f"ساعت عکس ({bad[0]}) با الان ({now_str()}) نمی‌خونه؛ اسکرین‌شات تازه بگیر"


def _safe(fn, *a):
    try:
        return fn(*a)
    except Exception as e:
        log.exception("%s", getattr(fn, "__name__", "fn"))
        return None, f"{getattr(fn, '__name__', 'fn')}: {e}"


def ai_verdict(img, c, since=None):
    """۱) چک پیکسلی چیپ پررنگ  ۲) به‌موازات: مدل کلی، مالکیت چیپ، دو خوندن ساعت  ۳) تصمیم.
    همه‌ی چک‌ها بسته‌اند: اگه ابزاری کار نکنه (مثلاً Pillow) هیچ‌چیز تأیید نمی‌شه. خروجی: (حکم، دلیل، خطا)"""
    if PIXEL_CHECK and not PIL_OK:
        return None, "", "Pillow نصب نیست ولی PIXEL_CHECK روشنه؛ Pillow رو به requirements.txt اضافه کن"
    boxes = find_chips(img) if PIXEL_CHECK else None
    if PIXEL_CHECK and boxes is None:
        return None, "", "چک پیکسلی خطا داد (لاگ سرور رو ببین)"
    if boxes is not None and not boxes:
        return "reject", "ری‌اکشنی نزدی (یا تو عکس دیده نمی‌شه)؛ اول ری‌اکشن بزن بعد اسکرین‌شات بگیر", ""
    small = small_jpeg(img)
    with ThreadPoolExecutor(max_workers=5) as ex:
        f_gen = ex.submit(_safe, ai_retry, ai_check, small, c)
        f_own = (ex.submit(_safe, ai_retry, ai_owner_check, img, boxes, c) if boxes
                 else ex.submit(_safe, ai_retry, ai_reaction_check, img, c))
        f_c0 = ex.submit(_safe, ai_clock, img, 0)
        f_c1 = ex.submit(_safe, ai_clock, img, 1)
        f_c2 = ex.submit(_safe, ai_clock, img, 2)
        (v, e1), (r, e2), (t0, _), (t1, _), (t2, _) = f_gen.result(), f_own.result(), f_c0.result(), f_c1.result(), f_c2.result()
    if v is None or r is None:
        return None, "", e1 or e2 or "پاسخ مدل نیومد"
    problems = []                                       # همه‌ی مشکل‌ها با هم گزارش می‌شن، نه فقط اولینش
    verdict, why = judge(v, True)
    low_conf = verdict != "approve" and "واضح نیست" in why
    if verdict != "approve":
        problems.append(why)
    if not low_conf:
        if boxes:
            own = [t for t in (r.get("tags") or []) if isinstance(t, dict) and t.get("own_post") is True]
            if not own:
                problems.append("ری‌اکشنت مالِ پستِ تسک نیست؛ روی خودِ پست تسک ری‌اکشن بزن")
        elif not str(r.get("selected_emoji") or "").strip():
            problems.append("ری‌اکشنت زیر پستِ تسک دیده نمی‌شه")
        why2 = clock_decide([t0, t1, t2], since)
        if why2:
            problems.append(why2)
    if problems:
        why = problems[0] if len(problems) == 1 else "\n".join("• " + x for x in problems)
        log.info("ai reject: %s | %s | %s | clocks=%s | %s", problems, json.dumps(v, ensure_ascii=False)[:400], json.dumps(r, ensure_ascii=False)[:300], [t0, t1, t2], boxes)
        if AI_DEBUG:
            for a_ in ADMINS:
                send(a_, "🔎 رد شد: " + why + "\n" + json.dumps(v, ensure_ascii=False)[:600] + "\n" + json.dumps(r, ensure_ascii=False)[:400] + f"\nclocks={[t0, t1, t2]}")
        return "reject", why, ""
    return "approve", "", ""


def verify_shot(uid, chat, cid_s, ph, since=None):
    """در ترد جدا اجرا می‌شه (تماس با مدل ممکنه کند باشه)"""
    fid = ph["file_unique_id"]
    try:
        c = db.campaigns.find_one({"_id": ObjectId(cid_s)})
        if not c or c["status"] != "active" or c["remaining"] <= 0:
            return send(chat, "این تسک دیگه ظرفیت نداره.", [[btn("🎯 تسک بعدی", "earn")]])
        RETRY = "❌ این عکس قبلاً رد شده. باید واقعاً ری‌اکشن بزنی و اسکرین‌شات تازه بگیری."
        try:
            db.shots.insert_one({"_id": fid, "uid": uid, "cid": cid_s, "t": time.time()})
        except DuplicateKeyError:
            old = db.shots.find_one({"_id": fid}) or {}
            return send(chat, RETRY if old.get("rej") and old.get("uid") == uid else
                        "❌ این عکس قبلاً ارسال شده. اسکرین‌شات تازه‌ی خودت رو بفرست.", [[btn("🏠 منو", "home")]])
        img = fetch_file(ph["file_id"])
        if not img:
            db.shots.delete_one({"_id": fid})
            return send(chat, "⚠️ نتونستم عکس رو بگیرم. دوباره «انجام دادم» رو بزن و عکس رو بفرست.", [[btn("🎯 ادامه", "earn")]])
        h = img_hash(img)
        if h:
            # فقط با عکس‌های «قبول‌شده/در حال بررسی» مقایسه می‌شه. عکس رد‌شده شبیه‌یابی نمی‌شه چون اسکرین‌شات تازه‌ی همون
            # صفحه (با ری‌اکشن عوض‌شده) هش تقریباً یکسان داره و بی‌گناه رد می‌شد؛ عکس رد‌شده‌ی عین‌به‌عین با file_unique_id گرفته می‌شه
            for o in db.shots.find({"cid": cid_s, "ah": {"$exists": True}, "rej": {"$ne": True}, "uid": {"$ne": uid}}):
                if ham(h, o["ah"]) <= 6:
                    db.shots.delete_one({"_id": fid})
                    return send(chat, "❌ این اسکرین‌شات شبیه عکس قبلیِ یه کاربر دیگه‌ست. عکس تازه‌ی خودت رو بفرست.", [[btn("🏠 منو", "home")]])
            db.shots.update_one({"_id": fid}, {"$set": {"ah": h}})
        verdict, why, err = ai_verdict(img, c, since)
        if verdict is None:                             # مشکل از سرویس مدله، نه از عکس کاربر
            db.shots.delete_one({"_id": fid})
            db.counters.update_one({"_id": f"ai:{uid}:{day()}"}, {"$inc": {"n": -1}})
            ai_alert(err)
            return send(chat, "⚠️ سرویس بررسی عکس الان در دسترس نیست (مشکل از عکس تو نیست). "
                              "چند دقیقه دیگه دوباره «انجام دادم» رو بزن.", [[btn("🎯 تسک‌ها", "earn")]])
        if verdict == "reject":                         # عکس رد‌شده نگه داشته می‌شه تا دوباره ارسالش فایده نداشته باشه
            db.shots.update_one({"_id": fid}, {"$set": {"rej": True}})
            return send(chat, f"❌ {why}\nاسکرین‌شات تازه بگیر و دوباره «انجام دادم» رو بزن.",
                        [[btn("🎯 تسک‌ها", "earn")]])
        code, comp = grant(uid, c, "pending")
        if code != "ok":
            return send(chat, claim_text(code, c), [[btn("🎯 تسک بعدی", "earn")]])
        send(chat, f"✅ تأیید شد!\n{c['reward']} کوین {HOLD_HOURS:g} ساعت معلق می‌مونه و بعد قطعی می‌شه.",
             [[btn("🎯 تسک بعدی", "earn")]])
    except Exception:
        log.exception("verify_shot")
        db.shots.delete_one({"_id": fid})
        send(chat, "⚠️ خطایی رخ داد؛ دوباره امتحان کن.", [[btn("🏠 منو", "home")]])


# ───────────────────────── تأیید تجمیعی ری‌اکشن کانال ─────────────────────────
def accept_count_claim(comp):
    """یک واحد افزایش شمارش به یک تسک در بازه‌ی زمانی‌اش اختصاص می‌دهد و پاداش را فوری می‌پردازد."""
    c = db.campaigns.find_one({"_id": comp["cid"]}) or {}
    if c.get("status") != "active":
        fail_count_claim(comp, "کمپین پیش از تأیید متوقف شد؛ ظرفیت به صاحب کمپین برگشت داده شد.")
        return False
    r = db.completions.find_one_and_update({"_id": comp["_id"], "status": "count_pending"},
                                           {"$set": {"status": "settling", "verified_at": time.time(), "due": time.time(), "lock": time.time()}})
    if not r:
        return False
    db.users.update_one({"_id": r["uid"]}, {"$inc": {"hold": r["reward"]}})
    pay(r)
    c = db.campaigns.find_one({"_id": r["cid"]}) or {}
    if c.get("status") == "active" and c.get("remaining", 1) == 0 and db.completions.count_documents({"cid": r["cid"], "status": "count_pending"}) == 0:
        send(c["owner"], f"🎉 کمپین «{c.get('title') or c.get('uname')}» (❤️ ری‌اکشن) تکمیل شد؛ افزایش شمارش تأیید شد.")
    return True


def fail_count_claim(comp, reason):
    """ادعای بدون افزایش شمارش را بی‌پاداش می‌بندد و ظرفیت را امن برمی‌گرداند."""
    r = db.completions.find_one_and_update({"_id": comp["_id"], "status": "count_pending"},
                                           {"$set": {"status": "failed", "failed_at": time.time()}})
    if not r:
        return False
    c = db.campaigns.find_one({"_id": r["cid"]}) or {}
    if c.get("status") == "active":
        db.campaigns.update_one({"_id": r["cid"], "status": "active"}, {"$inc": {"remaining": 1}})
        refresh_campaign_listing(r["cid"])
    else:
        if c.get("cost"):
            add_points(r["owner"], c["cost"], "refund", r["cid"])
    if r.get("day_key"):
        db.counters.update_one({"_id": r["day_key"]}, {"$inc": {"n": -1}})
    send(r["uid"], "⌛ " + reason + " پاداشی پرداخت نشد؛ می‌توانی دوباره تلاش کنی.")
    # اجازه‌ی تلاش دوباره برای همان کمپین را می‌دهیم؛ رویداد شکست برای دیباگ در لاگ می‌ماند.
    db.reaction_claim_logs.insert_one({"uid": r["uid"], "cid": r["cid"], "post_key": r.get("post_key"),
                                       "status": "failed", "reason": reason, "t": time.time()})
    db.completions.delete_one({"_id": r["_id"], "status": "failed"})
    return True


def consume_credit_for_claim(comp):
    """یک credit از افزایش شمارش مصرف می‌کند؛ هر افزایش فقط به تعداد واحدهای واقعی‌اش تسک را تأیید می‌کند."""
    c = db.campaigns.find_one({"_id": comp["cid"]}) or {}
    if c.get("status") != "active":
        fail_count_claim(comp, "کمپین متوقف شده است.")
        return False
    # Claims start after the user returns from the channel, so a per-user baseline
    # captured at that point would already include their reaction. Use unused
    # aggregate credits from the eligible time window instead. This remains a
    # count-based estimate and cannot prove the identity of the reactor.
    query = {"post_key": comp.get("post_key"),
             "event_at": {"$gte": float(comp.get("task_started", 0)), "$lte": float(comp.get("task_deadline", 0))},
             "delta": {"$gt": 0}}
    for credit in db.reaction_credits.find(query).sort([("event_at", 1)]):
        while True:
            current = db.reaction_credits.find_one({"_id": credit["_id"]})
            if not current or int(current.get("used", 0)) >= int(current.get("delta", 0)):
                break
            used = int(current.get("used", 0))
            updated = db.reaction_credits.update_one({"_id": current["_id"], "used": used,
                                                      "delta": {"$gte": used + 1}}, {"$inc": {"used": 1}})
            if updated.modified_count:
                if accept_count_claim(comp):
                    return True
                db.reaction_credits.update_one({"_id": current["_id"], "used": used + 1}, {"$inc": {"used": -1}})
                return False
            # another webhook worker consumed this unit; reread the credit
    return False


def invalidate_count_credits(key, decrease):
    """اگر شمارش پایین آمد، واحدهای افزایشِ هنوز مصرف‌نشده را با کاهش خنثی می‌کند."""
    remaining = int(decrease)
    if remaining <= 0:
        return
    for credit in db.reaction_credits.find({"post_key": key}).sort([("event_at", -1)]):
        used, total_credit = int(credit.get("used", 0)), int(credit.get("delta", 0))
        available = max(0, total_credit - used)
        cut = min(available, remaining)
        if cut:
            db.reaction_credits.update_one({"_id": credit["_id"], "delta": total_credit, "used": used},
                                           {"$set": {"delta": total_credit - cut}})
            remaining -= cut
        if remaining <= 0:
            break


def process_count_claims(key, event_at, old_total, new_total, delta):
    if delta <= 0:
        invalidate_count_credits(key, -delta)
        return
    claims = db.completions.find({"status": "count_pending", "post_key": key,
                                  "task_started": {"$lte": event_at},
                                  "task_deadline": {"$gte": event_at}}, sort=[("task_started", 1)])
    for comp in claims:
        if not consume_credit_for_claim(comp):
            # Credit may be consumed by another eligible claim; try next claim only while there are credits.
            credit_query = {"post_key": key,
                            "event_at": {"$gte": float(comp.get("task_started", 0)), "$lte": float(comp.get("task_deadline", 0))}}
            available = any(int(x.get("used", 0)) < int(x.get("delta", 0)) for x in db.reaction_credits.find(credit_query))
            if not available:
                break


def expire_count_claims(now):
    for comp in db.completions.find({"status": "count_pending", "claim_expires": {"$lte": now}}).limit(200):
        fail_count_claim(comp, "در مهلت تعیین‌شده افزایش قابل‌تأییدی در شمارش دریافت نشد.")
    # حفظ اندازه‌ی دیتابیس: creditها فقط تا یک روز برای بررسی و جلوگیری از دوباره‌مصرف‌شدن نگه داشته می‌شوند.
    db.reaction_credits.delete_many({"received_at": {"$lt": now - 86400}})
    db.reaction_events.delete_many({"received_at": {"$lt": now - 14 * 86400}})


def reaction_watch_text(uid, all_admin=False):
    query = {} if all_admin else {"$or": [{"owner": uid}, {"owners": uid}]}
    watches = list(db.reaction_watch.find(query, sort=[("updated_at", -1)], limit=15))
    if not watches:
        return "📡 هنوز پستی برای پایش ثبت نشده. هنگام ثبت تسک ری‌اکشن، کانال به این فهرست اضافه می‌شود."
    lines = ["📡 پایش شمارش ری‌اکشن‌ها", "آپدیت‌ها فقط هنگام تغییر شمارش می‌آیند و تلگرام ممکن است آن‌ها را چند دقیقه گروه‌بندی/با تأخیر ارسال کند.", ""]
    for w in watches:
        total = w.get("total")
        age = max(0, int(time.time() - w.get("updated_at", time.time()))) if w.get("updated_at") else None
        age_text = f"{age // 60}دقیقه {age % 60}ثانیه پیش" if age is not None else "هنوز آپدیتی نرسیده"
        delay = max(0, int(w.get("updated_at", time.time()) - w.get("event_at", w.get("updated_at", time.time())))) if w.get("updated_at") else None
        delay_text = f"تأخیر دریافت حدود {delay} ثانیه" if delay is not None else "تأخیر نامشخص"
        title = w.get("title") or w.get("uname") or str(w.get("chat_id"))
        lines.append(f"• {title} — پست {w.get('mid', '?')}")
        lines.append(f"  شمارش فعلی: {total if total is not None else 'نامشخص'} | آخرین آپدیت: {age_text} | {delay_text}")
        if w.get("reactions"):
            lines.append("  " + "، ".join(f"{x['label']} {x['count']}" for x in w["reactions"][:5]))
        camp_query = {"type": "react", "mode": "count", "chat_id": w.get("chat_id"), "mid": w.get("mid"), "status": "active"}
        campaigns = list(db.campaigns.find(camp_query, {"target": 1, "remaining": 1, "base_count": 1, "window_min": 1}))
        if campaigns:
            c = campaigns[-1]
            base = c.get("base_count")
            delta = int(total) - int(base) if total is not None and base is not None else None
            lines.append(f"  کمپین فعال: {c.get('target', 0)} سفارش | باقی‌مانده {c.get('remaining', 0)} | مهلت {c.get('window_min', 5)} دقیقه" +
                         (f" | تغییر از شروع کمپین: {delta:+d}" if delta is not None else ""))
        lines.append("")
    return "\n".join(lines)[:3900]


# ───────────────────────── برگشت / پرداخت / بازبینی ─────────────────────────
def undo(comp, had_hold, strike, why):
    """لغو یک انجام: کوین معلق حذف، ظرفیت کمپین دوباره باز (یا اگه کمپین لغو شده، هزینه به صاحب برمی‌گرده)"""
    inc = {"hold": -comp["reward"]} if had_hold else {}
    if strike:
        inc["strikes"] = 1
    if inc:
        db.users.update_one({"_id": comp["uid"]}, {"$inc": inc})
    r = db.campaigns.update_one({"_id": comp["cid"], "status": "active"}, {"$inc": {"remaining": 1}})
    if r.matched_count:
        refresh_campaign_listing(comp["cid"])
    if not r.matched_count:
        c = db.campaigns.find_one({"_id": comp["cid"]}) or {}
        if c.get("cost"):
            add_points(comp["owner"], c["cost"], "refund", comp["cid"])
    send(comp["uid"], f"⚠️ {why}" + (f"\nیک اخطار ثبت شد ({STRIKE_LIMIT} اخطار = بسته شدن انجام تسک)." if strike else ""))


def pay(comp, notify=True):
    r = db.completions.find_one_and_update({"_id": comp["_id"], "status": "settling"},
                                           {"$set": {"status": "paid", "paid": time.time()}})
    if not r:
        return
    db.users.update_one({"_id": comp["uid"]}, {"$inc": {"hold": -comp["reward"], "points": comp["reward"], "earned": comp["reward"]}})
    ledger(comp["uid"], comp["reward"], "task", comp["cid"])
    c = db.campaigns.find_one({"_id": comp["cid"]}) or {}
    if notify:
        kb = [[btn("🎯 انجام تسک بعدی", "earn", style="success")], [btn("👤 حساب کاربری", "acc", style="primary")]]
        send(comp["uid"], f"🎉 <b>تسکت تأیید شد!</b>\n\n💰 پاداش {comp['reward']} کوین با موفقیت به موجودی‌ات اضافه شد. ممنون از مشارکتت ❤️", kb)


def settle(comp):
    """پایان دوره‌ی معلق: چک دوباره، بعد پرداخت یا برگشت"""
    c = db.campaigns.find_one({"_id": comp["cid"]}) or {}
    ok = True
    if campaign_needs_admin(c):
        if c.get("admin_paused"):
            raise RuntimeError("campaign paused: bot admin access missing")
        admin_status = bot_admin_status(c.get("chat_id"))
        if admin_status is None:
            raise RuntimeError("temporary API error while checking bot admin status")
        if not admin_status:
            db.campaigns.update_one({"_id": c["_id"], "status": "active"}, {"$set": {"admin_paused": True, "admin_lost_at": time.time()}})
            refresh_campaign_listing(c["_id"])
            send(c.get("owner"), f"⛔ کمپین «{c.get('title') or c.get('uname') or 'کمپین'}» به‌دلیل ازدست‌رفتن دسترسی ادمین متوقف شد.")
            raise RuntimeError("campaign paused: bot admin access missing")
    if comp["type"] == "join":
        r = tg("getChatMember", chat_id=c.get("chat_id"), user_id=comp["uid"])
        if not r.get("ok"):
            raise RuntimeError("temporary API error while checking user membership")
        if not member_ok(r["result"]):
            ok = False
    elif comp["type"] == "react" and c.get("mode") == "auto":
        d = db.reacts.find_one({"_id": f"{c.get('chat_id')}:{c.get('mid')}:{comp['uid']}"})
        ok = bool(d and d.get("on"))
    if ok:
        return pay(comp)
    r = db.completions.find_one_and_update({"_id": comp["_id"], "status": "settling"}, {"$set": {"status": "reversed"}})
    if r:
        undo(comp, True, True, "از کانال خارج شدی یا ری‌اکشن رو برداشتی؛ کوین این تسک برگشت خورد.")


_last = {"clean": 0}


def tick():
    t = time.time()
    monitor_campaign_admins(t)
    expire_count_claims(t)
    db.completions.update_many({"status": "settling", "lock": {"$lt": t - 600}}, {"$set": {"status": "pending"}})
    while True:
        comp = db.completions.find_one_and_update({"status": "pending", "due": {"$lte": t}},
                                                  {"$set": {"status": "settling", "lock": t}})
        if not comp:
            break
        try:
            settle(comp)
        except Exception:
            log.exception("settle")
            db.completions.update_one({"_id": comp["_id"], "status": "settling"},
                                      {"$set": {"status": "pending", "due": time.time() + 60}})
            break
    if t - _last["clean"] > 3600:
        _last["clean"] = t
        db.reacts.delete_many({"t": {"$lt": t - 40 * 86400}})
        db.sessions.delete_many({"t": {"$lt": t - 7 * 86400}})


def worker():
    while True:
        try:
            tick()
        except Exception:
            log.exception("tick")
        time.sleep(30)


# ───────────────────────── ثبت تسک جدید (چندمرحله‌ای) ─────────────────────────
POST_RE = re.compile(r"(?:https?://)?(?:t\.me|telegram\.me)/([A-Za-z][A-Za-z0-9_]{3,31})/(\d+)", re.I)
CHAN_RE = re.compile(r"^(?:https?://)?(?:(?:t\.me|telegram\.me)/)?@?([A-Za-z][A-Za-z0-9_]{3,31})/?$", re.I)
FA = str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789")


def set_state(uid, step, d):
    db.states.replace_one({"_id": uid}, {"_id": uid, "step": step, "d": d, "t": time.time()}, upsert=True)


def start_new(cq, uid, chat, mid):
    ans(cq)
    t = TYPES
    view(chat, mid, "🧩 <b>ساخت کمپین جدید</b>\n\nاول نوع فعالیتی رو انتخاب کن که می‌خوای برای کانال یا پستت سفارش بدی. هزینهٔ هر واحد قبل از ثبت نهایی بهت نمایش داده می‌شه.\n\n"
         + "\n".join(f"{v['title']} — <b>{v['cost']} کوین</b> برای هر واحد" for v in t.values()),
         [[btn(f"{v['title']} · {v['cost']} کوین", f"nt:{k}") for k, v in t.items()], [btn("↩️ بازگشت به منو", "home")]])


def pick_type(cq, uid, chat, mid, typ):
    if typ not in TYPES:
        return ans(cq)
    if db.campaigns.count_documents({"owner": uid, "status": "active"}) >= MAX_ACTIVE:
        return ans(cq, f"حداکثر {MAX_ACTIVE} تسک فعال می‌تونی داشته باشی.", True)
    ans(cq)
    set_state(uid, "link", {"type": typ})
    if typ == "join":
        txt = ("👥 <b>مقصد کمپین عضویت</b>\n\nیوزرنیم یا لینک عمومی کانال/گروه رو بفرست؛ مثلاً <code>@mychannel</code>.\n\n"
               f"🛡️ برای اینکه بتونم عضویت رو بررسی کنم، ابتدا ربات <code>@{_me.get('username', '')}</code> رو در مقصد ادمین کن.")
    else:
        txt = ("🔗 <b>انتخاب پست مقصد</b>\n\nلینک پست رو بفرست؛ یا اگر لینک نداره، خودِ پیام رو از کانال برای ربات فوروارد کن.\n\n"
               "فورواردِ پست کانال معمولاً شناسهٔ دقیق پست رو مشخص می‌کنه. در گروه‌ها تلگرام ممکنه شناسهٔ پیام اصلی رو در اطلاعات فوروارد پنهان کنه؛ در این حالت برای ری‌اکشن خودکار باید لینک پست یا یوزرنیم گروه + شمارهٔ پیام رو وارد کنی.")
    view(chat, mid, txt, [[btn("❌ انصراف", "home")]])


def ask_qty(chat, d, c_cost):
    u = db.users.find_one({"_id": chat}) or {}
    mx = min(MAX_QTY, u.get("points", 0) // c_cost)
    if mx < 1:
        return send(chat, f"موجودی‌ات ({u.get('points', 0)}) برای ثبت حتی یک واحد ({c_cost} کوین) کافی نیست. اول چند تسک انجام بده.",
                    [[btn("🎯 انجام تسک", "earn")]])
    send(chat, f"🔢 چند واحد می‌خوای؟ (۱ تا {mx}) — فقط عدد بفرست.", [[btn("❌ انصراف", "home")]])
    return True


def on_state_text(uid, chat, text, st):
    if time.time() - st.get("t", 0) > 3600:
        db.states.delete_one({"_id": uid})
        return send(chat, "⌛ این مرحله منقضی شد. از منو دوباره شروع کن.", main_kb())
    step, d = st["step"], st.get("d", {})
    if step == "shot":
        return send(chat, "📸 لطفاً اسکرین‌شات رو به‌صورت «عکس» بفرست.", [[btn("❌ انصراف", "home")]])
    typ = d.get("type")
    cost = TYPES[typ]["cost"] if typ else 0
    if step == "link":
        if typ == "join":
            m = CHAN_RE.match(text.strip())
            uname, mid_ = (m[1], None) if m else (None, None)
        else:
            m = POST_RE.search(text)
            uname, mid_ = (m[1], int(m[2])) if m else (None, None)
        if not uname:
            return send(chat, "❌ لینک معتبر نیست. " + ("فقط @یوزرنیم یا t.me/یوزرنیم." if typ == "join"
                                                       else "فرمت درست: https://t.me/یوزرنیم/شماره‌ی‌پست"))
        ch = resolve_chat(uname)
        if not ch:
            return send(chat, "❌ این کانال/گروه رو پیدا نکردم. باید عمومی (با یوزرنیم) باشه.")
        d.update(uname=ch.get("username") or uname, chat_id=ch["id"], title=(ch.get("title") or uname)[:60], mid=mid_, mode=None, ref_text="", ref_photo=None)
        if typ == "join" and not bot_is_admin(ch["id"]):
            return send(chat, f"❌ ربات هنوز ادمین «{d['title']}» نیست. اول @{_me.get('username', '')} رو ادمین کن و دوباره لینک بفرست.")
        if typ == "react":
            if ch["type"] in ("group", "supergroup") and bot_is_admin(ch["id"]):
                d["mode"] = "auto"
            elif ch["type"] == "channel":
                if not bot_is_admin(ch["id"]):
                    return send(chat, f"❌ برای تأیید شمارشی، ربات باید ادمین کانال «{d['title']}» باشد. بعد از ادمین کردن، لینک پست را دوباره بفرست.")
                d["mode"] = "count"
                key = post_key(d["chat_id"], d["mid"])
                db.reaction_watch.update_one({"_id": key}, {"$set": {"chat_id": d["chat_id"], "mid": d["mid"],
                    "uname": d["uname"], "title": d["title"], "owner": uid, "registered_at": time.time()},
                    "$addToSet": {"owners": uid}}, upsert=True)
                snap = fresh_reaction_snapshot(d["chat_id"], d["mid"])
                if not snap:
                    db.reaction_watch.update_one({"_id": key}, {"$set": {"calibrating_owner": uid, "calibrating_at": time.time()}})
                    set_state(uid, "count_baseline", d)
                    return send(chat, "📡 برای شروع پایش باید یک آپدیت واقعی از شمارش این پست دریافت کنیم.\n\n"
                                      "حالا در خود کانال یک ری‌اکشن به همین پست اضافه/حذف کن (یا از یک ادمین بخواه این کار را انجام دهد)، سپس ۱ تا چند دقیقه صبر کن. "
                                      "وقتی تلگرام شمارش را بفرستد، خودکار به مرحله‌ی انتخاب مهلت می‌رویم.\n\n"
                                      "توجه: تلگرام شمارش را از طریق API به‌صورت فوری قابل‌خواندن نمی‌کند؛ آپدیت‌ها ممکن است چند دقیقه تأخیر داشته باشند.",
                                [[btn("❌ انصراف", "home")]])
                set_state(uid, "window", d)
                return send(chat, f"📡 <b>پایش پست آماده است</b>\n\nآخرین شمارش دریافت‌شده: <b>{snap.get('total', 0)} ری‌اکشن</b>.\n\nحالا مهلتی رو انتخاب کن که کاربر برای انجام هر تسک در اختیار داشته باشه. توجه کن که تلگرام ممکنه خودِ آپدیت شمارش رو دیرتر ارسال کنه.",
                            [[btn("⏱️ ۲ دقیقه", "rw:2"), btn("⏱️ ۵ دقیقه", "rw:5")], [btn("❌ لغو کمپین", "home")]])
            else:
                if ch["type"] in ("group", "supergroup"):
                    return send(chat, f"❌ برای اعتبارسنجی دقیق ری‌اکشن گروه، ربات باید در «{d['title']}» ادمین باشد. پس از ادمین‌کردن، لینک پست را دوباره بفرست.")
                if not AI_ON:
                    return send(chat, "❌ برای گروه، ربات باید ادمین باشد تا ری‌اکشن فردی را ببیند؛ برای کانال، ربات باید ادمین باشد تا شمارش تجمیعی را پایش کند.")
                d["mode"] = "shot"
                set_state(uid, "ref", d)
                return send(chat, "🔎 برای اینکه ربات پست دقیق رو تشخیص بده (نه هر پست دیگه‌ی کانال)، همون پست رو از کانال برای من فوروارد کن.\n\n"
                                  "اگه کانال فوروارد رو بسته، یه جمله‌ی مشخص و منحصربه‌فرد از متن پست رو تایپ کن.",
                            [[btn("❌ انصراف", "home")]])
        set_state(uid, "qty", d)
        return ask_qty(uid, d, cost)
    if step == "count_baseline":
        return send(chat, "⏳ هنوز آپدیت شمارش این پست نرسیده. یک ری‌اکشن روی همان پست اضافه/حذف کن و کمی صبر کن.", [[btn("❌ انصراف", "home")]])
    if step == "window":
        return send(chat, "مهلت را با دکمه‌های ۲ یا ۵ دقیقه انتخاب کن.", [[btn("⏱ ۲ دقیقه", "rw:2"), btn("⏱ ۵ دقیقه", "rw:5")], [btn("❌ انصراف", "home")]])
    if step == "qty":
        try:
            q = int(text.translate(FA).strip())
        except ValueError:
            return send(chat, "فقط یه عدد بفرست.")
        u = db.users.find_one({"_id": uid}) or {}
        mx = min(MAX_QTY, u.get("points", 0) // cost)
        if not 1 <= q <= mx:
            return send(chat, f"عدد باید بین ۱ تا {mx} باشه.")
        d["qty"] = q
        set_state(uid, "confirm", d)
        how = {"join": "تأیید خودکار با عضویت‌سنجی + چک دوباره‌ی آخر دوره‌ی معلق",
               "view": "تأیید زمانی (بازدید قابل راستی‌آزمایی دقیق نیست)",
               "react": ("تأیید خودکار فردی (گروه)" if d.get("mode") == "auto" else
                         f"تأیید شمارشی کانال؛ مهلت {d.get('window_min', 5)} دقیقه + پرداخت فوری" if d.get("mode") == "count" else
                         "اسکرین‌شات + بررسی خودکار هوش مصنوعی + ضدتکرار")}[typ]
        admin_note = ("\n👑 ادمین اجباری ربات: فعال تا پایان کمپین؛ در صورت قطع دسترسی، کمپین موقتاً متوقف می‌شود."
                      if typ == "join" or (typ == "react" and d.get("mode") == "auto") else "")
        return send(chat, f"📝 خلاصه‌ی تسک\n\n{TYPES[typ]['title']}\n📌 {d['title']}\n🔢 تعداد: {q}\n💰 هزینه: {q * cost} کوین\n"
                          f"🛡 تأیید: {how}{admin_note}\n\nثبت بشه؟",
                    [[btn(f"✅ ثبت و کسر {q * cost} کوین", "ok")], [btn("❌ انصراف", "home")]])
    send(chat, "از منو ادامه بده 👇", main_kb())


def on_ref(uid, chat, msg, st):
    """مرجع پست: پیام فورواردشده (متن + عکس واقعی پست) یا متن تایپ‌شده. فوروارد حتماً باید همون پستِ لینک‌شده باشه"""
    if time.time() - st.get("t", 0) > 3600:
        db.states.delete_one({"_id": uid})
        return send(chat, "⌛ این مرحله منقضی شد. از منو دوباره شروع کن.", main_kb())
    d = st["d"]
    text = (msg.get("text") or msg.get("caption") or "").strip()
    fo = msg.get("forward_origin")
    if fo:
        if fo.get("type") != "channel" or (fo.get("chat") or {}).get("id") != d["chat_id"] or fo.get("message_id") != d.get("mid"):
            return send(chat, "❌ این پیام همون پستی که لینکش رو دادی نیست. دقیقاً همون پست رو از کانال فوروارد کن.")
        photo = (msg.get("photo") or [{}])[-1].get("file_id")
        if not photo:
            for k in ("video", "animation", "document"):
                photo = ((msg.get(k) or {}).get("thumbnail") or {}).get("file_id")
                if photo:
                    break
        if not text and not photo:
            return send(chat, "❌ این پست متن یا تصویر قابل تشخیص نداره (مثلاً فقط استیکر/نظرسنجیه)؛ ری‌اکشن اون‌طور پست‌ها قابل تأیید نیست.")
        d["ref_photo"] = photo
    elif len(text) < 4:
        return send(chat, "پست رو فوروارد کن یا یه جمله‌ی مشخص (حداقل ۴ حرف) از متن پست بفرست.")
    d["ref_text"] = text[:300]
    set_state(uid, "qty", d)
    return ask_qty(uid, d, TYPES[d["type"]]["cost"])


def confirm_new(cq, uid, chat, mid):
    st = db.states.find_one({"_id": uid})
    if not st or st.get("step") != "confirm":
        ans(cq, "این مرحله منقضی شده", True)
        return view(chat, mid, "از منو دوباره شروع کن.", main_kb())
    d, typ = st["d"], st["d"]["type"]
    t = TYPES[typ]
    snap = None
    if typ == "react" and d.get("mode") == "count":
        snap = fresh_reaction_snapshot(d.get("chat_id"), d.get("mid"))
        if not snap:
            ans(cq, "شمارش پایه قدیمی یا ناموجود است؛ دوباره لینک پست را بفرست تا پایش تازه شود.", True)
            db.states.delete_one({"_id": uid})
            return view(chat, mid, "📡 شمارش تازه لازم است. از ثبت تسک ری‌اکشن دوباره شروع کن.", main_kb())
    if db.campaigns.count_documents({"owner": uid, "status": "active"}) >= MAX_ACTIVE:
        return ans(cq, f"حداکثر {MAX_ACTIVE} تسک فعال می‌تونی داشته باشی.", True)
    # کنترل نهایی دسترسی درست پیش از کسر هزینه؛ خطای API به‌معنای تأیید تلقی نمی‌شود.
    needs_admin = typ == "join" or (typ == "react" and d.get("mode") == "auto")
    if needs_admin:
        admin_status = bot_admin_status(d.get("chat_id"))
        if admin_status is None:
            return ans(cq, "بررسی ادمین مقصد موقتاً ممکن نیست؛ هزینه‌ای کسر نشد. دوباره تلاش کن.", True)
        if not admin_status:
            return ans(cq, "ربات باید هنگام ثبت و تا پایان کمپین ادمین مقصد باشد؛ هزینه‌ای کسر نشد.", True)
    if not spend(uid, d["qty"] * t["cost"], "campaign", d["uname"]):
        return ans(cq, "موجودی کافی نیست", True)
    db.states.delete_one({"_id": uid})
    admin_required = bool(typ == "join" or (typ == "react" and d.get("mode") == "auto"))
    campaign = {"owner": uid, "type": typ, "chat_id": d["chat_id"], "uname": d["uname"], "title": d["title"],
                "mid": d.get("mid"), "mode": d.get("mode"), "hint": d.get("ref_text", ""), "ref_photo": d.get("ref_photo"), "target": d["qty"],
                "remaining": d["qty"], "cost": t["cost"], "reward": t["reward"], "status": "active", "created": time.time(),
                "admin_required": admin_required, "admin_paused": False,
                "admin_checked_at": time.time() if admin_required else None}
    if typ == "react" and d.get("mode") == "count":
        campaign.update(window_min=int(d.get("window_min", 5)), base_count=int(snap.get("total", 0)),
                        base_updated_at=float(snap.get("updated_at", time.time())), post_key=post_key(d["chat_id"], d["mid"]))
    campaign["_id"] = db.campaigns.insert_one(campaign).inserted_id
    listed = publish_campaign(campaign)
    if not listed:
        db.campaigns.update_one({"_id": campaign["_id"], "status": "active"}, {"$set": {"status": "cancelled"}})
        add_points(uid, d["qty"] * t["cost"], "refund", campaign["_id"])
        ans(cq, "ویترین در دسترس نیست", True)
        return view(chat, mid,
                    "⚠️ <b>کمپین منتشر نشد و هزینه برگشت خورد.</b>\n\n"
                    "برای اینکه همهٔ کمپین‌ها در کانال ویترین نمایش داده بشن، مدیر ربات باید متغیر <code>TASKS_CHANNEL</code> رو تنظیم کنه، ربات رو در کانال ادمین کنه و دسترسی ارسال/ویرایش پیام بده.\n\n"
                    "بعد از آماده شدن ویترین، می‌تونی کمپین رو دوباره ثبت کنی؛ کوین هزینه‌شده به حسابت برگشته.",
                    [[btn("📊 کمپین‌های من", "my")], [btn("🏠 منوی اصلی", "home")]])
    ans(cq, "کمپین منتشر شد")
    msg = ("🚀 <b>کمپینت با موفقیت منتشر شد!</b>\n\n"
           "تسک در کانال ویترین قرار گرفت و کاربران از همون‌جا وارد ربات می‌شن تا دستورالعمل رو ببینن و انجام تسک رو ثبت کنن.\n\n"
           "🔔 پس از تأیید هر انجام، نتیجه و وضعیت واریز پاداش به کاربر داخل ربات اعلام می‌شه. وضعیت کمپینت رو هم می‌تونی از بخش «کمپین‌های من» پیگیری کنی.")
    kb = [[btn("📣 مشاهدهٔ تسک در ویترین", url=tasks_channel_url())], [btn("📊 کمپین‌های من", "my")], [btn("🏠 منوی اصلی", "home")]]
    view(chat, mid, msg, kb)


def my_campaigns(cq, uid, chat, mid):
    ans(cq)
    render_my(uid, chat, mid)


def render_my(uid, chat, mid):
    cs = list(db.campaigns.find({"owner": uid}, sort=[("created", -1)], limit=10))
    if not cs:
        return view(chat, mid, "هنوز تسکی ثبت نکردی.", [[btn("➕ ثبت تسک جدید", "new")], [btn("🏠 منو", "home")]])
    lines, kb = [], []
    for i, c in enumerate(cs, 1):
        if c.get("type") == "react" and c.get("mode") == "count":
            paid_count = db.completions.count_documents({"cid": c["_id"], "status": "paid"})
            pending_count = db.completions.count_documents({"cid": c["_id"], "status": "count_pending"})
            st = "⛔ لغو شده" if c["status"] != "active" else ("🟡 در انتظار آپدیت" if pending_count else ("✅ تکمیل" if c["remaining"] == 0 else "🟢 فعال"))
            progress = f"{paid_count}/{c['target']} تأیید | در انتظار {pending_count}"
        else:
            st = "⛔ لغو شده" if c["status"] != "active" else ("✅ تکمیل" if c["remaining"] == 0 else "🟢 فعال")
            progress = f"{c['target'] - c['remaining']}/{c['target']}"
        lines.append(f"{i}. {TYPES[c['type']]['title']} {c['title']} — {progress} {st}")
        if c["status"] == "active" and c["remaining"] > 0:
            kb.append([btn(f"❌ لغو شماره {i}", f"x:{c['_id']}")])
    view(chat, mid, "📋 تسک‌های من\n\n" + "\n".join(lines), kb + [[btn("🏠 منو", "home")]])


def cancel_campaign(cq, uid, chat, mid, cid_s, sure):
    try:
        cid = ObjectId(cid_s)
    except InvalidId:
        return ans(cq)
    if not sure:
        ans(cq)
        return view(chat, mid, "مطمئنی؟ ظرفیت باقی‌مانده لغو و هزینه‌اش برمی‌گرده.",
                    [[btn("✅ بله، لغو کن", f"xy:{cid_s}")], [btn("↩️ برگشت", "my")]])
    r = db.campaigns.find_one_and_update({"_id": cid, "owner": uid, "status": "active"}, {"$set": {"status": "cancelled"}})
    if not r:
        ans(cq, "این تسک فعال نیست", True)
    else:
        refund = r["remaining"] * r["cost"]
        if refund:
            add_points(uid, refund, "refund", cid)
        refresh_campaign_listing(cid)
        ans(cq, f"لغو شد؛ {refund} کوین برگشت", True)
    render_my(uid, chat, mid)


# ───────────────────────── دستورات ادمین ─────────────────────────
def admin_cmd(uid, chat, cmd, args):
    if cmd == "reactwatch":
        return send(chat, reaction_watch_text(uid, all_admin=True))
    if cmd == "stats":
        by = {s: db.completions.count_documents({"status": s}) for s in ("pending", "paid", "reversed")}
        return send(chat, f"📊 کاربران: {db.users.count_documents({})}\nکمپین فعال: {db.campaigns.count_documents({'status': 'active'})}\n"
                          + "\n".join(f"{k}: {v}" for k, v in by.items()))
    if cmd == "aitest":
        png = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==")
        res = {}

        def _t(m):
            t0 = time.time()
            txt, err = ai_request('Reply with only this JSON: {"ok": true}', png, "image/png", only=m)
            res[m] = (bool(txt), round(time.time() - t0, 1), (txt or err)[:90])
        ths = [threading.Thread(target=_t, args=(e["label"],)) for e in AI_ENDPOINTS]
        [t.start() for t in ths]
        [t.join(AI_TIMEOUT + 10) for t in ths]
        lines = [(f"✅ {m} — {res[m][1]}s" if res[m][0] else f"❌ {m} — {res[m][1]}s\n    {res[m][2]}") for m in [e["label"] for e in AI_ENDPOINTS] if m in res]
        return send(chat, "تست تک‌تک مدل‌ها:\n" + ("\n".join(lines) or "هیچ مدلی تنظیم نشده")
                    + "\n\nترتیب فعلی اولویت:\n" + "\n".join(f"{i}. {m}" for i, m in enumerate(_model_list(), 1))
                    + f"\n\nنسخه: {VERSION}\nPillow: {'✅' if PIL_OK else '❌ نصب نیست'}\nPIXEL_CHECK: {PIXEL_CHECK}  CLOCK_REQUIRED: {CLOCK_REQUIRED}"
                    + f"\nTIME_TOL: {TIME_TOL}  TZ: {TZ_NAME}  ساعت سرور: {now_str()}  AI_TIMEOUT: {AI_TIMEOUT}  AI_BUDGET: {AI_BUDGET}")
    if cmd == "give" and len(args) == 2 and args[1].lstrip("-").isdigit():
        add_points(int(args[0]), int(args[1]), "admin")
        return send(chat, "انجام شد.")
    if cmd == "unban" and args:
        db.users.update_one({"_id": int(args[0])}, {"$set": {"strikes": 0}})
        return send(chat, "اخطارها صفر شد.")
    if cmd == "stop" and args:
        try:
            r = db.campaigns.find_one_and_update({"_id": ObjectId(args[0]), "status": "active"}, {"$set": {"status": "cancelled"}})
        except InvalidId:
            r = None
        if r:
            add_points(r["owner"], r["remaining"] * r["cost"], "refund", r["_id"])
        return send(chat, "کمپین متوقف شد و باقی‌مانده برگشت خورد." if r else "کمپین فعالی پیدا نشد.")
    send(chat, "دستورها: /stats  /reactwatch  /aitest  /give <uid> <n>  /unban <uid>  /stop <campaign_id>")


# ───────────────────────── مسیریابی آپدیت‌ها ─────────────────────────
def open_linked_task(uid, chat, cid_s):
    try:
        cid = ObjectId(cid_s)
    except (InvalidId, TypeError):
        return send(chat, "🔎 این لینک تسک معتبر نیست یا ناقصه. از ویترین، یک تسک فعال رو انتخاب کن.", main_kb())
    c = db.campaigns.find_one({"_id": cid})
    if not c or c.get("status") != "active" or int(c.get("remaining", 0)) <= 0:
        return send(chat, "⏳ این تسک دیگه ظرفیت نداره. از ویترین یک تسک دیگه انتخاب کن.", main_kb())
    if campaign_needs_admin(c):
        if c.get("admin_paused"):
            return send(chat, "⛔ این کمپین موقتاً متوقف شده؛ دسترسی ربات در حال بازیابی است.",
                        [[btn("📣 بازگشت به ویترین", url=tasks_channel_url(), style="primary")]])
        admin_status = bot_admin_status(c.get("chat_id"))
        if admin_status is None:
            return send(chat, "⚠️ بررسی دسترسی مقصد موقتاً ممکن نیست. کمی بعد دوباره امتحان کن.", main_kb())
        if not admin_status:
            db.campaigns.update_one({"_id": c["_id"], "status": "active"}, {"$set": {"admin_paused": True, "admin_lost_at": time.time()}})
            refresh_campaign_listing(c["_id"])
            send(c.get("owner"), f"⛔ کمپین «{c.get('title') or c.get('uname') or 'کمپین'}» به‌دلیل ازدست‌رفتن دسترسی ادمین متوقف شد.")
            return send(chat, "⛔ این کمپین موقتاً متوقف است؛ لطفاً بعداً دوباره امتحان کن.", main_kb())
    u = db.users.find_one({"_id": uid}) or {}
    if c.get("owner") == uid:
        return send(chat, "ℹ️ این کمپین متعلق به خودته؛ نمی‌تونی تسک خودت رو انجام بدی.", main_kb())
    if db.completions.find_one({"uid": uid, "cid": cid}):
        return send(chat, "✅ این تسک قبلاً توسط حساب تو ثبت شده. برای تسک‌های دیگه به ویترین برگرد.",
                    [[btn("📣 بازگشت به ویترین", url=tasks_channel_url())], [btn("🏠 منوی اصلی", "home")]])
    if u.get("strikes", 0) >= STRIKE_LIMIT:
        return send(chat, "⛔ فعلاً امکان انجام تسک برات غیرفعاله؛ تعداد اخطارها به حد مجاز رسیده.", main_kb())
    if c.get("type") == "react" and c.get("mode") == "count":
        snap = fresh_reaction_snapshot(c.get("chat_id"), c.get("mid"))
        if not snap:
            return send(chat, "📡 شمارش این پست فعلاً تازه نیست؛ این تسک موقتاً در دسترس نیست. کمی بعد دوباره از ویترین امتحان کن.", main_kb())
        now = time.time()
        window_min = int(c.get("window_min", 5))
        ses = {"t": now, "task_started": now - window_min * 60,
               "task_deadline": now + REACTION_UPDATE_GRACE,
               "baseline_total": int(c.get("base_count", snap.get("total", 0))),
               "baseline_at": float(c.get("base_updated_at", snap.get("updated_at", now))),
               "post_key": post_key(c["chat_id"], c["mid"])}
    else:
        ses = {"t": time.time()}
    db.sessions.update_one({"_id": f"{uid}:{cid}"}, {"$set": ses}, upsert=True)
    return send(chat, "<b>ثبت انجام تسک</b>\n\n"
                "اگر کار را در کانال کامل کرده‌ای" +
                (" و ری‌اکشن خواسته‌شده را زده‌ای" if c.get("type") == "react" else "") +
                "، درخواستت را ثبت کن.\n\n"
                "درخواست پس از بررسی تأیید یا رد می‌شود و نتیجه همین‌جا اعلام خواهد شد.",
                [[btn("✅ انجام دادم", f"c:{cid}", style="success")],
                 [btn("↩️ بازگشت به انجام مأموریت", url=link_of(c), style="primary")]])


def on_message(msg):
    if msg["chat"]["type"] != "private" or "from" not in msg:
        return
    uid, chat = msg["from"]["id"], msg["chat"]["id"]
    u = ensure_user(msg["from"])
    text = (msg.get("text") or "").strip()
    if text.startswith("/"):
        parts = text[1:].split()
        cmd, args = parts[0].split("@")[0].lower(), parts[1:]
        if cmd in ("start", "menu"):
            db.states.delete_one({"_id": uid})
            if cmd == "start" and args and args[0].startswith("task_"):
                return open_linked_task(uid, chat, args[0][5:])
            return send(chat, menu_text(u), main_kb())
        if cmd == "help":
            return send(chat, help_text(), HOME_KB)
        if uid in ADMINS:
            return admin_cmd(uid, chat, cmd, args)
        return send(chat, menu_text(u), main_kb())
    st = db.states.find_one({"_id": uid})
    if st and st.get("step") == "shot" and msg.get("photo"):
        db.states.delete_one({"_id": uid})
        if time.time() - st.get("t", 0) > SHOT_WINDOW:
            return send(chat, "⌛ مهلت ارسال اسکرین‌شات تموم شده. دوباره «انجام دادم» رو بزن.", [[btn("🎯 تسک‌ها", "earn")]])
        if not counter_take(f"ai:{uid}:{day()}", DAILY_AI):
            return send(chat, f"✋ سقف {DAILY_AI} بررسی اسکرین‌شات امروز پر شده. فردا دوباره امتحان کن.", HOME_KB)
        send(chat, "⏳ در حال بررسی اسکرین‌شات…")
        threading.Thread(target=verify_shot, args=(uid, chat, st["cid"], msg["photo"][-1], st.get("t")), daemon=True).start()
        return
    if st and st.get("step") == "ref":
        return on_ref(uid, chat, msg, st)
    if st and st.get("step") == "link" and msg.get("forward_origin"):
        origin = msg.get("forward_origin") or {}
        if origin.get("type") == "channel":
            source = origin.get("chat") or {}
            source_username = source.get("username")
            source_mid = origin.get("message_id")
            if source_username and source_mid:
                # لینک داخلی از متادیتای فوروارد ساخته می‌شود؛ کاربر لازم نیست لینک پست را کپی کند.
                return on_state_text(uid, chat, f"https://t.me/{source_username}/{source_mid}", st)
            return send(chat, "❌ فوروارد دریافت شد، اما تلگرام شناسهٔ عمومی پست را در اختیار ربات نگذاشت. لینک پست را بفرست یا برای مقصد عمومی، @یوزرنیم و شمارهٔ پیام را وارد کن.", [[btn("❌ انصراف", "home")]])
        if origin.get("type") == "chat":
            return send(chat, "⚠️ فورواردِ پیام گروه دریافت شد، اما تلگرام شناسهٔ پیام اصلی گروه را در اطلاعات فوروارد نمی‌دهد؛ برای اینکه ری‌اکشن دقیقاً روی همان پیام بررسی شود، لینک پیام یا یوزرنیم گروه و شمارهٔ پیام لازم است.", [[btn("❌ انصراف", "home")]])
        return send(chat, "⚠️ فوروارد دریافت شد، ولی اطلاعات مبدأ برای شناسایی دقیق پست کافی نیست. لینک پست را بفرست؛ برای گروه‌های خصوصی، تلگرام ممکن است شناسهٔ پیام اصلی را پنهان کند.", [[btn("❌ انصراف", "home")]])
    if st and text:
        return on_state_text(uid, chat, text, st)
    if st:
        return send(chat, "لطفاً طبق مرحله‌ی قبل جواب بده.", [[btn("❌ انصراف", "home")]])
    send(chat, menu_text(u), main_kb())


def on_callback(cq):
    uid, data = cq["from"]["id"], cq.get("data", "")
    m = cq.get("message") or {}
    chat, mid = (m.get("chat") or {}).get("id"), m.get("message_id")
    u = ensure_user(cq["from"])
    if not chat:
        return ans(cq)
    if data == "home":
        ans(cq)
        db.states.delete_one({"_id": uid})
        return view(chat, mid, menu_text(u), main_kb())
    if data == "help":
        ans(cq)
        return view(chat, mid, help_text(), HOME_KB)
    if data == "acc":
        ans(cq)
        return view(chat, mid, acc_text(u), HOME_KB)
    if data == "earn":
        ans(cq)
        ch_url = tasks_channel_url()
        if ch_url:
            return view(chat, mid, "📣 <b>تسک‌ها فقط در کانال انجام می‌شن</b>\n\nاز کانال، پست موردنظرت رو باز کن و تسک رو همون‌جا انجام بده. بعد روی «🎁 دریافت پاداش» بزن تا درخواستت در ربات ثبت بشه.",
                        [[btn("📣 مشاهده تسک‌های کانال", url=ch_url, style="primary")], [btn("🏠 منوی اصلی", "home")]])
        return view(chat, mid, "⚠️ کانال تسک‌ها هنوز تنظیم نشده. لطفاً بعداً دوباره تلاش کن.", HOME_KB)
    if data.startswith("nx:"):
        ans(cq)
        db.users.update_one({"_id": uid}, {"$push": {"skip": {"$each": [data[3:]], "$slice": -30}}})
        return show_task(uid, chat, mid)
    if data.startswith("c:"):
        return on_claim(cq, uid, chat, mid, data[2:])
    if data == "new":
        return start_new(cq, uid, chat, mid)
    if data.startswith("nt:"):
        return pick_type(cq, uid, chat, mid, data[3:])
    if data == "ok":
        return confirm_new(cq, uid, chat, mid)
    if data == "my":
        return my_campaigns(cq, uid, chat, mid)
    if data == "rwatch":
        ans(cq)
        return view(chat, mid, reaction_watch_text(uid), [[btn("🔄 بروزرسانی", "rwatch")], [btn("🏠 منو", "home")]])
    if data.startswith("rw:"):
        return choose_reaction_window(cq, uid, chat, mid, data[3:])
    if data.startswith("x:"):
        return cancel_campaign(cq, uid, chat, mid, data[2:], False)
    if data.startswith("xy:"):
        return cancel_campaign(cq, uid, chat, mid, data[3:], True)
    ans(cq)


def choose_reaction_window(cq, uid, chat, mid, value):
    st = db.states.find_one({"_id": uid})
    if not st or st.get("step") != "window":
        ans(cq, "این مرحله منقضی شده؛ ثبت تسک را دوباره شروع کن.", True)
        return view(chat, mid, "از منو دوباره شروع کن.", main_kb())
    try:
        window_min = int(value)
    except (TypeError, ValueError):
        return ans(cq, "مهلت نامعتبر است.", True)
    if window_min not in (2, 5):
        return ans(cq, "فقط ۲ یا ۵ دقیقه قابل انتخاب است.", True)
    d = st.get("d", {})
    snap = fresh_reaction_snapshot(d.get("chat_id"), d.get("mid"))
    if not snap:
        ans(cq, "شمارش قدیمی شده؛ یک تغییر ری‌اکشن ایجاد کن تا دوباره به‌روز شود.", True)
        return view(chat, mid, "📡 برای ادامه، یک ری‌اکشن به همان پست اضافه/حذف کن و منتظر آپدیت بمان.",
                    [[btn("❌ انصراف", "home")]])
    d["window_min"] = window_min
    set_state(uid, "qty", d)
    ans(cq, f"مهلت {window_min} دقیقه انتخاب شد")
    return ask_qty(uid, d, TYPES[d["type"]]["cost"])


def on_reaction_count(mr, update_id=None):
    """Snapshot شمارش تجمیعی کانال را ذخیره و هر واحد افزایش را حداکثر به یک ادعای تسک اختصاص می‌دهد."""
    chat = mr.get("chat") or {}
    chat_id, mid = chat.get("id"), mr.get("message_id")
    if chat_id is None or mid is None:
        return
    key = post_key(chat_id, mid)
    received_at = time.time()
    event_at = float(mr.get("date") or received_at)
    old = db.reaction_watch.find_one({"_id": key})
    if not old:
        # فقط پست‌هایی را ذخیره می‌کنیم که کاربر برایشان پایش/کمپین ساخته؛ از ذخیره‌ی همه‌ی کانال‌ها جلوگیری می‌شود.
        watched_campaign = db.campaigns.count_documents({"chat_id": chat_id, "mid": int(mid), "type": "react", "mode": "count"})
        if not watched_campaign:
            return
    if update_id is not None:
        try:
            db.reaction_events.insert_one({"_id": int(update_id), "post_key": key, "received_at": received_at})
        except DuplicateKeyError:
            return
    if old and update_id is not None and int(old.get("last_update_id", -1)) >= int(update_id):
        return
    if old and old.get("event_at") and event_at < float(old["event_at"]):
        return

    reactions, total = [], 0
    for item in (mr.get("reactions") or []):
        typ = item.get("type") or {}
        kind = typ.get("type")
        if kind == "emoji":
            label = str(typ.get("emoji") or "🙂")
        elif kind == "custom_emoji":
            label = "ایموجی اختصاصی"
        elif kind == "paid":
            label = "⭐ ری‌اکشن پولی"
        else:
            label = str(kind or "ری‌اکشن")
        count = max(0, int(item.get("total_count", 0)))
        total += count
        reactions.append({"label": label[:40], "count": count, "type": typ})
    reactions.sort(key=lambda x: x["count"], reverse=True)
    old_total = int(old["total"]) if old and old.get("total") is not None else None
    set_fields = {"chat_id": chat_id, "mid": int(mid), "total": total, "reactions": reactions,
                  "event_at": event_at, "updated_at": received_at}
    if update_id is not None:
        set_fields["last_update_id"] = int(update_id)
    if chat.get("username"):
        set_fields["uname"] = chat["username"]
    if chat.get("title"):
        set_fields["title"] = chat["title"][:100]
    db.reaction_watch.update_one({"_id": key}, {"$set": set_fields,
        "$setOnInsert": {"created_at": received_at}}, upsert=True)

    if old_total is not None and total != old_total:
        delta = total - old_total
        if delta > 0:
            credit_id = int(update_id) if update_id is not None else f"{key}:{int(event_at)}:{random.randint(1000,9999)}"
            try:
                db.reaction_credits.insert_one({"_id": credit_id, "post_key": key, "old_total": old_total,
                    "new_total": total, "delta": delta, "used": 0, "event_at": event_at, "received_at": received_at})
            except DuplicateKeyError:
                pass
            process_count_claims(key, event_at, old_total, total, delta)
        else:
            invalidate_count_credits(key, -delta)

    # اگر صاحب کمپین منتظر اولین snapshot بود، خودکار به انتخاب مهلت هدایتش می‌کنیم.
    latest = db.reaction_watch.find_one({"_id": key}) or {}
    owner = latest.get("calibrating_owner")
    if owner:
        st = db.states.find_one({"_id": int(owner)})
        if st and st.get("step") == "count_baseline":
            d = st.get("d", {})
            db.reaction_watch.update_one({"_id": key, "calibrating_owner": owner},
                                         {"$unset": {"calibrating_owner": "", "calibrating_at": ""}})
            set_state(int(owner), "window", d)
            send(int(owner), f"✅ شمارش دریافت شد: {total} ری‌اکشن.\nآخرین آپدیت ثبت شد؛ مهلت انجام هر تسک را انتخاب کن:",
                 [[btn("⏱ ۲ دقیقه", "rw:2"), btn("⏱ ۵ دقیقه", "rw:5")], [btn("❌ انصراف", "home")]])


def on_reaction(mr):
    """ری‌اکشن‌های گروه‌هایی که ربات ادمینشونه (کاربر ناشناس/کانال‌ها این آپدیت رو ندارن)"""
    u = mr.get("user")
    if not u:
        return
    db.reacts.update_one({"_id": f"{mr['chat']['id']}:{mr['message_id']}:{u['id']}"},
                         {"$set": {"on": bool(mr.get("new_reaction")), "t": time.time()}}, upsert=True)


@app.post("/hook")
def hook():
    if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != HOOK_SECRET:
        return "forbidden", 403
    upd = request.get_json(silent=True) or {}
    try:
        if "callback_query" in upd:
            on_callback(upd["callback_query"])
        elif "message_reaction" in upd:
            on_reaction(upd["message_reaction"])
        elif "message_reaction_count" in upd:
            on_reaction_count(upd["message_reaction_count"], upd.get("update_id"))
        elif "message" in upd:
            on_message(upd["message"])
    except Exception:
        log.exception("update error")
    return "ok"


@app.get("/")
def index():
    return "ok"


def setup():
    bot_id()
    r = tg("setWebhook", url=f"{BASE_URL}/hook", secret_token=HOOK_SECRET,
           allowed_updates=["message", "callback_query", "message_reaction", "message_reaction_count"])
    tg("setMyCommands", commands=[{"command": "start", "description": "شروع و منوی اصلی"},
                                   {"command": "help", "description": "راهنمای استفاده"},
                                   {"command": "reactwatch", "description": "پایش شمارش ری‌اکشن‌ها (ادمین)"}])
    log.info("webhook: %s", r)
    if TASKS_CHANNEL:
        log.info("TASKS_CHANNEL configured: %s", TASKS_CHANNEL)
    else:
        log.warning("TASKS_CHANNEL تنظیم نشده؛ انتشار خودکار کمپین‌ها در کانال ویترین غیرفعال است")
    threading.Thread(target=worker, daemon=True).start()


setup()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
