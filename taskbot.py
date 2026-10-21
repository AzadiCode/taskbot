# -*- coding: utf-8 -*-
"""
TaskExchange — ربات تبادل تسک تلگرام (عضویت / ری‌اکشن / بازدید)
Stack: Flask + MongoDB(pymongo) + وب‌هوک، تک‌فایل و بدون مینی‌اپ

مدل کار:
- کاربر با انجام تسک «کوین» می‌گیره و با کوین، تسک خودش رو ثبت می‌کنه (هزینه‌ی ثبت > پاداش؛ اقتصاد تورم نمی‌کنه)
- عضویتِ تأییدشده، بازدید پس از رعایت زمان، و ری‌اکشنِ قابل‌تأیید پاداش می‌گیرند؛ ری‌اکشن کانال از مسیر شمارش و ری‌اکشن گروه از مسیر رویداد فردی تأیید می‌شود
- عضویت  : getChatMember (ربات باید ادمین کانال/گروه باشه) + چک دوباره‌ی آخر دوره‌ی معلق (لفت = برگشت کوین + اخطار)
- ری‌اکشن : گروه/سوپرگروه با ربات ادمین → تأیید فردی با message_reaction
            کانال → تأیید تجمیعی با message_reaction_count؛ هر واحد افزایش شمارش حداکثر یک ادعا را تأیید می‌کند.
            مهلت تسک کانال ۲ یا ۵ دقیقه است، پاداش پس از دریافت افزایش معتبر فوری پرداخت می‌شود، و آپدیت ممکن است چند دقیقه تأخیر داشته باشد.
            شمارش تجمیعی هویت کاربر را ثابت نمی‌کند؛ این روش تعداد افزایش‌ها را می‌سنجد، نه اینکه چه کسی واکنش داده است.
- بازدید  : فقط تأیید زمانی (از طریق Bot API قابل راستی‌آزمایی نیست) → پاداش کم

- دعوت    : پاداش دو‌مرحله‌ای (ورود دوست + فعال‌شدن دوست)؛ لینک اختصاصی start=ref_<uid>

نیازمندی‌ها: flask pymongo requests gunicorn؛ mongomock فقط برای اجرای آزمایشی بدون MongoDB
اجرا: gunicorn taskbot:app
"""
import os, re, time, hmac, hashlib, random, logging, threading

import requests
from urllib.parse import quote
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

START_POINTS = int(os.environ.get("START_POINTS", "20"))        # هدیه‌ی شروع
HOLD_HOURS   = float(os.environ.get("HOLD_HOURS", "24"))       # مدت معلق ماندن پاداش
DAILY_TASKS  = int(os.environ.get("DAILY_TASKS", "60"))        # سقف تسک روزانه‌ی هر کاربر
STRIKE_LIMIT = int(os.environ.get("STRIKE_LIMIT", "3"))        # بعد از این تعداد اخطار، انجام تسک بسته می‌شه
REF_JOIN_BONUS   = int(os.environ.get("REF_JOIN_BONUS", "2"))      # پاداش کوچکِ ورود دوستِ دعوت‌شده
REF_ACTIVE_BONUS = int(os.environ.get("REF_ACTIVE_BONUS", "8"))    # پاداش شیرینِ فعال‌شدن دوست (باید از پاداش ورود بیشتر باشه)
REF_ACTIVE_TASKS = int(os.environ.get("REF_ACTIVE_TASKS", "3"))    # تعداد تسک عضویت/ری‌اکشنِ تأییدشده برای «فعال» حساب شدن
REF_DAILY_CAP    = int(os.environ.get("REF_DAILY_CAP", "20"))      # سقف روزانهٔ پاداش ورود برای هر دعوت‌کننده (ضد سوءاستفاده)
MAX_QTY      = int(os.environ.get("MAX_QTY", "500"))
MAX_ACTIVE   = int(os.environ.get("MAX_ACTIVE", "10"))         # سقف کمپین فعال هر کاربر
REACTION_UPDATE_GRACE = int(os.environ.get("REACTION_UPDATE_GRACE", "180"))  # فرصت اضافه برای تأخیر آپدیت شمارش تلگرام
REACTION_BASELINE_MAX_AGE = int(os.environ.get("REACTION_BASELINE_MAX_AGE", "900"))  # حداکثر سن شمارش پایه (ثانیه)
ADMIN_MONITOR_INTERVAL = int(os.environ.get("ADMIN_MONITOR_INTERVAL", "60"))  # فاصله بررسی ادمین کمپین‌ها
VERSION      = "v48-ui-refresh-referral"

# نوع تسک: پاداش انجام‌دهنده / هزینه‌ی ثبت برای صاحب کمپین (هر واحد) / حداقل صبر (ثانیه)
TYPES = {
    "join":  {"title": "👥 عضویت",   "reward": 3, "cost": 4, "wait": 0},
    "react": {"title": "❤️ ری‌اکشن", "reward": 2, "cost": 3, "wait": 10},
    "view":  {"title": "👁 بازدید",  "reward": 1, "cost": 2, "wait": 5},
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
db.users.create_index("referred_by")
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
        elif any(k in label for k in ("کسب کوین", "تسک بعدی", "انجام تسک", "ادامه", "مشاهده تسک", "ویترین تسک", "بازگشت به کانال", "رفتن به پست مقصد", "تسک‌های کانال", "۲ دقیقه", "۵ دقیقه")):
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


def ans(cq, text=None, alert=False, url=None):
    p = {"callback_query_id": cq["id"]}
    if text:
        p.update(text=text[:200], show_alert=alert)
    if url:
        p["url"] = url
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
    return [[btn("🎯 کسب کوین", "earn")],
            [btn("➕ ساخت کمپین", "new", style="plain"), btn("📊 کمپین‌های من", "my")],
            [btn("🎁 دعوت دوستان", "ref", style="success")],
            [btn("👤 حساب من", "acc"), btn("💡 راهنما", "help")]]


def menu_text(u):
    name = u.get("name") or "دوست من"
    hold = f"\n⏳ در انتظار تأیید: <b>{u['hold']}</b> کوین" if u.get("hold") else ""
    return (f"👋 سلام {name}، به <b>تاسک‌پلاس</b> خوش اومدی!\n\n"
            f"💰 موجودی: <b>{u.get('points', 0)}</b> کوین{hold}\n\n"
            "تسک انجام بده، کوین بگیر و با کوین برای کانال خودت کمپین بساز.\n"
            "دوستانت رو هم دعوت کن تا از هر کدوم پاداش بگیری 🎁")


def help_text():
    t = TYPES
    return (
        "💡 <b>راهنمای تاسک‌پلاس</b>\n\n"
        "<b>🎯 کسب کوین</b>\n"
        "تسک رو در کانال ویترین انجام بده، زیر همون پست «🎁 دریافت پاداش» رو بزن و اینجا «انجام دادم» رو تأیید کن.\n"
        f"• {t['join']['title']} — {t['join']['reward']} کوین؛ عضویت بررسی و پاداش فوری واریز می‌شه.\n"
        f"• {t['react']['title']} — {t['react']['reward']} کوین؛ در گروه تأیید فردی، در کانال بر اساس افزایش شمارش.\n"
        f"• {t['view']['title']} — {t['view']['reward']} کوین؛ بعد از رعایت زمان لازم.\n\n"
        "<b>➕ ساخت کمپین</b>\n"
        "نوع تسک، لینک مقصد و تعداد رو بده؛ هزینه پیش از ثبت نشون داده می‌شه. برای عضویت و ری‌اکشن گروه، ربات باید ادمین مقصد باشه.\n\n"
        "<b>🎁 دعوت دوستان</b>\n"
        f"با ورود هر دوست {REF_JOIN_BONUS} کوین می‌گیری و وقتی فعال بشه ({REF_ACTIVE_TASKS} تسک عضویت/ری‌اکشن) {REF_ACTIVE_BONUS} کوین دیگه.\n\n"
        "<b>📡 نکتهٔ ری‌اکشن کانال</b>\n"
        "تأیید بر اساس افزایش شمارش کل انجام می‌شه و هویت واکنش‌دهنده رو مشخص نمی‌کنه؛ تلگرام ممکنه آپدیت رو چند دقیقه دیر بفرسته.\n\n"
        f"<b>🛡️ قوانین</b>\nهر کمپین برای هر نفر فقط یک بار. خروج از کانال یا برداشتن ری‌اکشن یعنی برگشت کوین و اخطار (سقف {STRIKE_LIMIT} اخطار).")


def acc_text(u):
    n = db.campaigns.count_documents({"owner": u["_id"], "status": "active"})
    hold = f"\n⏳ در انتظار تأیید: <b>{u['hold']}</b> کوین" if u.get("hold") else ""
    return ("👤 <b>حساب من</b>\n\n"
            f"💰 موجودی: <b>{u.get('points', 0)}</b> کوین{hold}\n"
            f"🏆 مجموع کسب‌شده: {u.get('earned', 0)} کوین\n"
            f"👥 دعوت‌ها: {u.get('ref_count', 0)} نفر ({u.get('ref_active_count', 0)} فعال)\n"
            f"📋 کمپین فعال: {n} از {MAX_ACTIVE}\n"
            f"🛡️ اخطار: {u.get('strikes', 0)} از {STRIKE_LIMIT}")


def acc_kb():
    return [[btn("🎁 دعوت دوستان", "ref")], [btn("🏠 منوی اصلی", "home")]]


def link_of(c):
    return f"https://t.me/{c['uname']}" + (f"/{c['mid']}" if c.get("mid") else "")


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


def show_task(uid, chat, mid=None):
    """مسیر کسب کوین فقط از کانال ویترین است."""
    ch_url = tasks_channel_url()
    if not ch_url:
        return view(chat, mid, "⚠️ کانال تسک‌ها هنوز تنظیم نشده؛ کمی بعد دوباره سر بزن.", HOME_KB)
    return view(chat, mid,
                "🎯 <b>کسب کوین</b>\n\n"
                "تسک‌ها در کانال ویترین منتشر می‌شن:\n"
                "۱. یه تسک انتخاب کن و انجامش بده\n"
                "۲. زیر همون پست «🎁 دریافت پاداش» رو بزن\n"
                "۳. اینجا «انجام دادم» رو تأیید کن\n\n"
                "پاداش بعد از بررسی خودکار واریز می‌شه.",
                [[btn("📣 رفتن به ویترین", url=ch_url, style="primary")], [btn("🏠 منوی اصلی", "home")]])


def campaign_listing_text(c):
    t = TYPES.get(c.get("type"), {"title": "📌 تسک"})
    remaining = max(0, int(c.get("remaining", 0)))
    target = c.get("title") or c.get("uname") or "تسک جدید"
    if c.get("status") == "cancelled":
        state = "🔴 لغو شده"
    elif c.get("type") == "react" and c.get("mode") not in ("auto", "count"):
        state = "⚪ روش تأیید قدیمی غیرفعال"
    elif c.get("admin_paused") and campaign_needs_admin(c):
        state = "⛔ توقف موقت: دسترسی ربات"
    elif c.get("status") == "active" and remaining:
        state = "🟢 فعال"
    else:
        state = "⚪ ظرفیت تکمیل شد"

    if c.get("type") == "react" and c.get("mode") == "count":
        instruction = f"ری‌اکشن درخواستی رو روی پست بزن و تا {int(c.get('window_min', 5))} دقیقه بعدش «🎁 دریافت پاداش» رو بزن."
    elif c.get("type") == "join":
        instruction = "عضو مقصد شو، بعد «🎁 دریافت پاداش» رو بزن."
    elif c.get("type") == "view":
        instruction = "پست مقصد رو ببین و چند ثانیه صبر کن، بعد «🎁 دریافت پاداش» رو بزن."
    else:
        instruction = "دستور تسک رو در مقصد انجام بده، بعد «🎁 دریافت پاداش» رو بزن."

    return (f"<b>{t['title']}</b>  ·  📌 {target}\n\n"
            f"🎁 پاداش: <b>{c.get('reward', 0)}</b> کوین\n"
            f"👥 ظرفیت باقی‌مانده: <b>{remaining}</b> نفر\n"
            f"📍 {state}\n\n"
            f"📝 {instruction}")


def campaign_listing_keyboard(c):
    # کمپین‌های قدیمیِ وابسته به اسکرین‌شات/هوش مصنوعی دیگر قابل انجام نیستند.
    if c.get("type") == "react" and c.get("mode") not in ("auto", "count"):
        return [[btn("⚪ روش قدیمی غیرفعال", "noop")]]
    available = (c.get("status") == "active" and int(c.get("remaining", 0)) > 0
                 and not (c.get("admin_paused") and campaign_needs_admin(c)))
    if not available:
        return [[btn("🔴 ظرفیت تکمیل شد", "noop", style="danger")]]
    rows = []
    target_link = link_of(c)
    if target_link:
        rows.append(btn("🔗 رفتن به مقصد", url=target_link, style="primary"))
    # دکمهٔ پاداش callback است تا تأخیر اجباری قبل از ورود به صفحهٔ دریافت پاداش اعمال شود.
    if bot_deep_link(c.get("_id")):
        rows.append(btn("🎁 دریافت پاداش", f"rp:{c['_id']}", style="success"))
    return [rows] if rows else []


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
    listed_at = time.time()
    db.campaigns.update_one({"_id": c["_id"]}, {"$set": {"listing_chat_id": result.get("chat", {}).get("id"),
                                                              "listing_message_id": result.get("message_id"),
                                                              "listed_at": listed_at}})
    return True


def refresh_campaign_listing(cid):
    c = db.campaigns.find_one({"_id": cid})
    if not c or not c.get("listing_chat_id") or not c.get("listing_message_id"):
        return
    r = tg("editMessageText", chat_id=c["listing_chat_id"], message_id=c["listing_message_id"],
           text=telegram_html(campaign_listing_text(c)), parse_mode="HTML", disable_web_page_preview=True,
           reply_markup={"inline_keyboard": campaign_listing_keyboard(c)})
    if not r.get("ok") and "message is not modified" not in str(r.get("description", "")).lower():
        log.warning("refresh_campaign_listing failed cid=%s: %s", cid, r.get("description"))


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
    if c.get("type") == "react" and c.get("mode") not in ("auto", "count"):
        return ans(cq, "روش تأیید قدیمی حذف شده؛ این کمپین قابل انجام نیست.", True)
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
        return view(chat, mid,
                    f"🕓 <b>درخواستت ثبت شد</b>\n\n"
                    f"🎁 پاداش: {c['reward']} کوین\n"
                    f"⏱ مهلت: {window_min} دقیقه\n\n"
                    f"منتظر آپدیت شمارش تلگرام هستیم؛ ممکنه تا {REACTION_UPDATE_GRACE // 60} دقیقه دیرتر برسه. "
                    "اگه افزایش معتبری ثبت بشه، پاداش خودکار واریز می‌شه و بهت خبر می‌دم.",
                    [[btn("↩️ بازگشت به مقصد", url=link_of(c), style="primary")], [btn("🏠 منوی اصلی", "home")]])
    # عضویت و ری‌اکشنِ گروه همان لحظه بررسی شده‌اند؛ پاداششان را معلق نگه نمی‌داریم.
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
                    f"✅ <b>تسک تأیید شد!</b>\n\n💰 {c['reward']} کوین به موجودیت اضافه شد.",
                    [[btn("🎯 تسک بعدی", "earn")], [btn("👤 حساب من", "acc")]])
    after_ok(cq, chat, mid, c, comp)


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


# ───────────────────────── دعوت دوستان (پاداش دو‌مرحله‌ای) ─────────────────────────
def ref_link(uid):
    username = _me.get("username") or ""
    return f"https://t.me/{username}?start=ref_{uid}" if username else ""


def _plain(s, n=18):
    return re.sub(r"[<>&]", "", str(s or ""))[:n].strip()


def ref_register(uid, ref_s, name=""):
    """ثبت دعوت برای کاربر کاملاً جدید + پاداش کوچک ورود به دعوت‌کننده (با سقف روزانه)."""
    try:
        ref = int(ref_s)
    except (TypeError, ValueError):
        return
    if ref == uid or not db.users.find_one({"_id": ref}, {"_id": 1}):
        return
    got = db.users.update_one({"_id": uid, "referred_by": {"$exists": False}},
                              {"$set": {"referred_by": ref, "ref_tasks": 0}})
    if not got.modified_count:
        return
    db.users.update_one({"_id": ref}, {"$inc": {"ref_count": 1}})
    if not REF_JOIN_BONUS or not counter_take(f"refd:{ref}:{day()}", REF_DAILY_CAP):
        return
    add_points(ref, REF_JOIN_BONUS, "ref_join", uid)
    db.users.update_one({"_id": ref}, {"$inc": {"ref_earned": REF_JOIN_BONUS}})
    send(ref, f"🎉 <b>یه دوست جدید اومد!</b>\n\n"
              f"{_plain(name) or 'یه نفر'} با لینک تو وارد شد.\n"
              f"💰 +{REF_JOIN_BONUS} کوین\n\n"
              f"وقتی {REF_ACTIVE_TASKS} تسک انجام بده، {REF_ACTIVE_BONUS} کوین دیگه هم می‌گیری 🎁",
         [[btn("🎁 دعوت دوستان", "ref")]])


def ref_on_task_paid(uid, typ):
    """هر تسک عضویت/ری‌اکشنِ پرداخت‌شدهٔ کاربرِ دعوت‌شده یک قدم به فعال‌شدنش نزدیک می‌کند.
    (بازدید حساب نمی‌شه چون قابل راستی‌آزمایی نیست.)"""
    if typ not in ("join", "react"):
        return
    r = db.users.update_one({"_id": uid, "referred_by": {"$exists": True}, "ref_active": {"$ne": True}},
                            {"$inc": {"ref_tasks": 1}})
    if not r.modified_count:
        return
    u = db.users.find_one({"_id": uid}) or {}
    if int(u.get("ref_tasks", 0)) < REF_ACTIVE_TASKS:
        return
    won = db.users.update_one({"_id": uid, "ref_active": {"$ne": True}},
                              {"$set": {"ref_active": True, "ref_active_at": time.time()}})
    if not won.modified_count:      # کارگر دیگری همزمان فعال‌سازی را ثبت کرده؛ پاداش دوبار پرداخت نمی‌شه
        return
    ref = u.get("referred_by")
    if not ref or not REF_ACTIVE_BONUS:
        return
    db.users.update_one({"_id": ref}, {"$inc": {"ref_active_count": 1, "ref_earned": REF_ACTIVE_BONUS}})
    add_points(ref, REF_ACTIVE_BONUS, "ref_active", uid)
    send(ref, f"🔥 <b>دوستت فعال شد!</b>\n\n"
              f"{_plain(u.get('name')) or 'دوستت'} {REF_ACTIVE_TASKS} تسک انجام داد.\n"
              f"💰 +{REF_ACTIVE_BONUS} کوین",
         [[btn("🎁 دعوت دوستان", "ref")]])


def ref_text(u):
    uid = u["_id"]
    link = ref_link(uid)
    total, active = int(u.get("ref_count", 0)), int(u.get("ref_active_count", 0))
    out = ("🎁 <b>دعوت دوستان</b>\n\n"
           "از هر دوستی که دعوت کنی دو بار پاداش می‌گیری:\n"
           f"1️⃣ وقتی با لینکت وارد ربات بشه: <b>{REF_JOIN_BONUS}</b> کوین\n"
           f"2️⃣ وقتی فعال بشه ({REF_ACTIVE_TASKS} تسک عضویت یا ری‌اکشن): <b>{REF_ACTIVE_BONUS}</b> کوین\n\n"
           f"💡 هر دوست فعال در مجموع <b>{REF_JOIN_BONUS + REF_ACTIVE_BONUS}</b> کوین برات داره.\n\n"
           f"👥 دعوت‌شده: {total}  ·  ✅ فعال: {active}\n"
           f"💰 درآمد دعوت: <b>{int(u.get('ref_earned', 0))}</b> کوین")
    friends = list(db.users.find({"referred_by": uid}, sort=[("joined", -1)], limit=5))
    if friends:
        rows = []
        for f in friends:
            st = "✅ فعال" if f.get("ref_active") else f"⏳ {min(int(f.get('ref_tasks', 0)), REF_ACTIVE_TASKS)}/{REF_ACTIVE_TASKS}"
            rows.append(f"• {_plain(f.get('name')) or 'کاربر'} — {st}")
        out += "\n\n<b>آخرین دعوت‌ها</b>\n" + "\n".join(rows)
    if counter_get(f"refd:{uid}:{day()}") >= REF_DAILY_CAP:
        out += "\n\n⏸ سقف پاداش ورود برای امروز پر شده؛ از فردا دوباره فعال می‌شه."
    if link:
        out += f"\n\n🔗 <b>لینک اختصاصی تو</b>\n<code>{link}</code>"
    else:
        out += "\n\n⚠️ لینک دعوت موقتاً در دسترس نیست؛ کمی بعد دوباره امتحان کن."
    return out


def ref_kb(uid):
    link = ref_link(uid)
    rows = []
    if link:
        share = "با تاسک‌پلاس تسک انجام بده، کوین بگیر و کانالت رو رشد بده 🚀"
        rows.append([btn("📤 اشتراک‌گذاری لینک", url=f"https://t.me/share/url?url={quote(link, safe='')}&text={quote(share)}",
                         style="primary")])
    rows.append([btn("🏠 منوی اصلی", "home")])
    return rows


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
    if notify:
        send(comp["uid"], f"✅ <b>تسکت تأیید شد!</b>\n\n💰 {comp['reward']} کوین به موجودیت اضافه شد.",
             [[btn("🎯 تسک بعدی", "earn"), btn("👤 حساب من", "acc")]])
    try:
        ref_on_task_paid(comp["uid"], comp.get("type"))
    except Exception:
        log.exception("ref_on_task_paid")


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
    rows = [[btn(f"{v['title']} · {v['cost']} کوین", f"nt:{k}")] for k, v in TYPES.items()]
    view(chat, mid, "➕ <b>ساخت کمپین</b>\n\nچه فعالیتی رو می‌خوای برای کانال یا پستت سفارش بدی؟\nهزینهٔ هر واحد روی دکمه‌ها نوشته شده.",
         rows + [[btn("🏠 منوی اصلی", "home")]])


def pick_type(cq, uid, chat, mid, typ):
    if typ not in TYPES:
        return ans(cq)
    if db.campaigns.count_documents({"owner": uid, "status": "active"}) >= MAX_ACTIVE:
        return ans(cq, f"حداکثر {MAX_ACTIVE} تسک فعال می‌تونی داشته باشی.", True)
    ans(cq)
    set_state(uid, "link", {"type": typ})
    if typ == "join":
        txt = ("👥 <b>مقصد عضویت</b>\n\nیوزرنیم یا لینک عمومی کانال/گروه رو بفرست؛ مثلاً <code>@mychannel</code>.\n\n"
               f"🛡️ اول ربات <code>@{_me.get('username', '')}</code> رو در مقصد ادمین کن تا بتونم عضویت‌ها رو بررسی کنم.")
    else:
        txt = ("🔗 <b>پست مقصد</b>\n\nلینک پست رو بفرست (مثلاً <code>https://t.me/channel/123</code>) یا خود پست کانال رو برای من فوروارد کن.\n\n"
               "ℹ️ پیام‌های گروه با فوروارد قابل شناسایی نیستن؛ برای گروه حتماً لینک پیام رو بفرست.")
    view(chat, mid, txt, [[btn("❌ انصراف", "home")]])


def ask_qty(chat, d, c_cost):
    u = db.users.find_one({"_id": chat}) or {}
    mx = min(MAX_QTY, u.get("points", 0) // c_cost)
    if mx < 1:
        return send(chat, f"💰 موجودیت ({u.get('points', 0)} کوین) برای ثبت حتی یک واحد ({c_cost} کوین) کافی نیست.\n"
                          "اول چند تسک انجام بده یا با دعوت دوستان کوین بگیر.",
                    [[btn("🎯 کسب کوین", "earn")], [btn("🎁 دعوت دوستان", "ref")]])
    send(chat, f"🔢 چند واحد می‌خوای؟ (۱ تا {mx})\nفقط عدد بفرست.", [[btn("❌ انصراف", "home")]])
    return True


def on_state_text(uid, chat, text, st):
    if time.time() - st.get("t", 0) > 3600:
        db.states.delete_one({"_id": uid})
        return send(chat, "⌛ این مرحله منقضی شد. از منو دوباره شروع کن.", main_kb())
    step, d = st["step"], st.get("d", {})
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
        d.update(uname=ch.get("username") or uname, chat_id=ch["id"], title=(ch.get("title") or uname)[:60], mid=mid_, mode=None)
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
                return send(chat, "❌ برای ساخت تسک ری‌اکشن، مقصد باید کانالی باشد که ربات در آن ادمین است، یا گروهی که ربات در آن ادمین است و تأیید فردی ری‌اکشن را پشتیبانی می‌کند.")
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
        how = {"join": "عضویت‌سنجی خودکار + بازبینی پایان دورهٔ معلق",
               "view": "تأیید زمانی (بازدید قابل راستی‌آزمایی دقیق نیست)",
               "react": ("تأیید فردی ری‌اکشن (گروه)" if d.get("mode") == "auto" else
                         f"تأیید شمارشی کانال · مهلت {d.get('window_min', 5)} دقیقه")}[typ]
        admin_note = ("\n👑 ربات باید تا پایان کمپین ادمین مقصد بمونه؛ در غیر این صورت کمپین موقتاً متوقف می‌شه."
                      if typ == "join" or (typ == "react" and d.get("mode") == "auto") else "")
        return send(chat, f"📝 <b>خلاصهٔ کمپین</b>\n\n{TYPES[typ]['title']}\n📌 {d['title']}\n🔢 تعداد: {q}\n"
                          f"💰 هزینه: <b>{q * cost}</b> کوین\n🛡 تأیید: {how}{admin_note}\n\nثبت بشه؟",
                    [[btn(f"✅ ثبت و کسر {q * cost} کوین", "ok")], [btn("❌ انصراف", "home")]])
    send(chat, "👇 از منو ادامه بده.", main_kb())


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
                "mid": d.get("mid"), "mode": d.get("mode"), "target": d["qty"],
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
    msg = ("🚀 <b>کمپینت منتشر شد!</b>\n\n"
           "تسک در کانال ویترین قرار گرفت. بعد از تأیید هر انجام، پاداش خودکار واریز می‌شه و پیشرفت کمپین رو از «📊 کمپین‌های من» می‌بینی.")
    kb = []
    if tasks_channel_url():
        kb.append([btn("📣 دیدن در ویترین", url=tasks_channel_url())])
    kb += [[btn("📊 کمپین‌های من", "my")], [btn("🏠 منوی اصلی", "home")]]
    view(chat, mid, msg, kb)


def my_campaigns(cq, uid, chat, mid):
    ans(cq)
    render_my(uid, chat, mid)


def render_my(uid, chat, mid):
    cs = list(db.campaigns.find({"owner": uid}, sort=[("created", -1)], limit=10))
    if not cs:
        return view(chat, mid, "📊 <b>کمپین‌های من</b>\n\nهنوز کمپینی نساختی.",
                    [[btn("➕ ساخت کمپین", "new")], [btn("🏠 منوی اصلی", "home")]])
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
    if db.reaction_watch.count_documents({"$or": [{"owner": uid}, {"owners": uid}]}):
        kb.append([btn("📡 پایش ری‌اکشن‌ها", "rwatch")])
    view(chat, mid, "📊 <b>کمپین‌های من</b>\n\n" + "\n".join(lines), kb + [[btn("🏠 منوی اصلی", "home")]])


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
        refs = db.users.count_documents({"referred_by": {"$exists": True}})
        refs_active = db.users.count_documents({"ref_active": True})
        return send(chat, f"📊 کاربران: {db.users.count_documents({})}\nکمپین فعال: {db.campaigns.count_documents({'status': 'active'})}\n"
                          + "\n".join(f"{k}: {v}" for k, v in by.items())
                          + f"\nدعوت‌شده‌ها: {refs} (فعال: {refs_active})")
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
    send(chat, "دستورها: /stats  /reactwatch  /give <uid> <n>  /unban <uid>  /stop <campaign_id>")


# ───────────────────────── مسیریابی آپدیت‌ها ─────────────────────────
def open_linked_task(uid, chat, cid_s):
    try:
        cid = ObjectId(cid_s)
    except (InvalidId, TypeError):
        return send(chat, "🔎 این لینک تسک معتبر نیست یا ناقصه. از ویترین، یک تسک فعال رو انتخاب کن.", main_kb())
    c = db.campaigns.find_one({"_id": cid})
    if not c or c.get("status") != "active" or int(c.get("remaining", 0)) <= 0:
        return send(chat, "⏳ این تسک دیگه ظرفیت نداره. از ویترین یک تسک دیگه انتخاب کن.", main_kb())
    if c.get("type") == "react" and c.get("mode") not in ("auto", "count"):
        return send(chat, "⚪ این کمپین از روش تأیید قدیمی استفاده می‌کرد و دیگر قابل انجام نیست. لطفاً یک تسک دیگر انتخاب کن.", main_kb())
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
    return send(chat, "🎁 <b>دریافت پاداش</b>\n\n"
                "اگه تسک رو در کانال انجام دادی" +
                (" و ری‌اکشن رو زدی" if c.get("type") == "react" else "") +
                "، «انجام دادم» رو بزن. بعد از بررسی، نتیجه همین‌جا اعلام می‌شه.",
                [[btn("✅ انجام دادم", f"c:{cid}", style="success")],
                 [btn("↩️ بازگشت به مقصد", url=link_of(c), style="primary")]])


def on_message(msg):
    if msg["chat"]["type"] != "private" or "from" not in msg:
        return
    uid, chat = msg["from"]["id"], msg["chat"]["id"]
    existed = db.users.find_one({"_id": uid}, {"_id": 1}) is not None
    u = ensure_user(msg["from"])
    text = (msg.get("text") or "").strip()
    if text.startswith("/"):
        parts = text[1:].split()
        cmd, args = parts[0].split("@")[0].lower(), parts[1:]
        if cmd in ("start", "menu"):
            db.states.delete_one({"_id": uid})
            if cmd == "start" and args and args[0].startswith("task_"):
                return open_linked_task(uid, chat, args[0][5:])
            if cmd == "start" and args and args[0].startswith("ref_") and not existed:
                ref_register(uid, args[0][4:], msg["from"].get("first_name"))
            return send(chat, menu_text(u), main_kb())
        if cmd == "help":
            return send(chat, help_text(), HOME_KB)
        if cmd in ("invite", "ref"):
            return send(chat, ref_text(u), ref_kb(uid))
        if uid in ADMINS:
            return admin_cmd(uid, chat, cmd, args)
        return send(chat, menu_text(u), main_kb())
    st = db.states.find_one({"_id": uid})
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
        return view(chat, mid, acc_text(u), acc_kb())
    if data == "earn":
        ans(cq)
        return show_task(uid, chat, mid)
    if data == "ref":
        ans(cq)
        return view(chat, mid, ref_text(u), ref_kb(uid))
    if data.startswith("rp:"):
        cid_s = data[3:]
        try:
            cid = ObjectId(cid_s)
        except (InvalidId, TypeError):
            return ans(cq, "شناسهٔ این تسک معتبر نیست؛ لطفاً از پست تازهٔ کانال استفاده کن.", True)
        c = db.campaigns.find_one({"_id": cid})
        if not c or c.get("status") != "active" or int(c.get("remaining", 0)) <= 0:
            return ans(cq, "این تسک دیگر فعال نیست.", True)
        deep = bot_deep_link(cid)
        if not deep:
            return ans(cq, "لینک ورود به صفحهٔ دریافت پاداش در دسترس نیست.", True)
        # تأخیر برای هر کاربر و هر تسک جداگانه است و فقط روی دکمهٔ پاداش اعمال می‌شود.
        click_id = f"reward:{cid}:{uid}"
        now = time.time()
        click = db.campaign_clicks.find_one({"_id": click_id})
        if not click:
            db.campaign_clicks.update_one(
                {"_id": click_id}, {"$set": {"first_click_at": now}}, upsert=True
            )
            return ans(cq, "برای ثبت بازدید، ۵ ثانیه صبر کن و مطمئن شو تسک رو دیدی؛ بعد دوباره روی «🎁 دریافت پاداش» بزن.", True)
        first_at = float(click.get("first_click_at", now))
        remaining_wait = 5 - (now - first_at)
        if remaining_wait > 0:
            return ans(cq, f"هنوز {int(remaining_wait + 0.999)} ثانیه صبر کن؛ بعد دوباره روی «🎁 دریافت پاداش» بزن.", True)
        # لینک deep-link معتبر تلگرام از طریق answerCallbackQuery باز می‌شود؛ مرورگر واسطه‌ای نداریم.
        return ans(cq, url=deep)
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
        return view(chat, mid, reaction_watch_text(uid), [[btn("🔄 به‌روزرسانی", "rwatch")], [btn("↩️ کمپین‌های من", "my")]])
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
    tg("setMyCommands", commands=[{"command": "start", "description": "منوی اصلی"},
                                   {"command": "invite", "description": "دعوت دوستان و پاداش"},
                                   {"command": "help", "description": "راهنما"}])
    log.info("webhook: %s", r)
    if TASKS_CHANNEL:
        log.info("TASKS_CHANNEL configured: %s", TASKS_CHANNEL)
    else:
        log.warning("TASKS_CHANNEL تنظیم نشده؛ انتشار خودکار کمپین‌ها در کانال ویترین غیرفعال است")
    threading.Thread(target=worker, daemon=True).start()


setup()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
