# -*- coding: utf-8 -*-
"""
TaskExchange — ربات تبادل تسک تلگرام (عضویت / ری‌اکشن / بازدید)
Stack (مثل نمونه‌ی AI BotMaker): Flask + MongoDB(pymongo) + وب‌هوک، تک‌فایل و بدون مینی‌اپ

مدل کار:
- کاربر با انجام تسک «امتیاز» می‌گیره و با امتیاز، تسک خودش رو ثبت می‌کنه (هزینه‌ی ثبت > پاداش؛ اقتصاد تورم نمی‌کنه)
- امتیاز بعد از انجام تسک HOLD_HOURS ساعت «معلق» می‌مونه، بعد دوباره چک می‌شه و قطعی می‌شه
- عضویت  : getChatMember (ربات باید ادمین کانال/گروه باشه) + چک دوباره‌ی آخر دوره‌ی معلق (لفت = برگشت امتیاز + اخطار)
- ری‌اکشن : گروه با ربات ادمین → تأیید خودکار با message_reaction (برداشتن ری‌اکشن هم دیده می‌شه)
            کانال (ری‌اکشن‌ها ناشناس‌اند) → اسکرین‌شات + موتور محلی (چک پیکسلی چیپ + OCR ساعت/اسم کانال + تطبیق تصویر پست) + ضدتکرار
            (بدون هیچ API: VISION_MODE=local | auto = محلی + کمک مدل فقط در حالت «نامطمئن» | ai = رفتار قبلی)
            (file_unique_id و هش تصویری). تصمیم کاملاً خودکاره: تأیید یا رد؛ نامطمئن = درخواست عکس واضح‌تر
- بازدید  : فقط تأیید زمانی (از طریق Bot API قابل راستی‌آزمایی نیست) → پاداش کم

نیازمندی‌ها: flask pymongo requests gunicorn Pillow rapidocr-onnxruntime   (Pillow برای تشخیص عکس تکراریِ دوباره‌آپلودشده لازمه؛ mongomock اختیاری برای تست)
اجرا: gunicorn app:app
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
CONF_MIN     = float(os.environ.get("CONF_MIN", "0.3"))        # حداقل اطمینان مدل برای قبول عکس
VERSION      = "v32-local"
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
    "join":  {"title": "👥 عضویت",   "reward": 3, "cost": 4, "wait": 0},
    "react": {"title": "❤️ ری‌اکشن", "reward": 2, "cost": 3, "wait": 10},
    "view":  {"title": "👁 بازدید",  "reward": 1, "cost": 2, "wait": 15},
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

HOOK_SECRET = hashlib.sha256(("hook" + SECRET_KEY).encode()).hexdigest()[:32]
_me = {}


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


def btn(text, data=None, url=None):
    return {"text": text, "url": url} if url else {"text": text, "callback_data": data}


def send(chat, text, kb=None):
    p = {"chat_id": chat, "text": text[:4000], "disable_web_page_preview": True}
    if kb:
        p["reply_markup"] = {"inline_keyboard": kb}
    return tg("sendMessage", **p)


def view(chat, mid, text, kb=None):
    """اگه پیام قبلی هست ویرایشش می‌کنه (منوها تمیز بمونن)، وگرنه پیام جدید می‌فرسته"""
    if mid:
        r = tg("editMessageText", chat_id=chat, message_id=mid, text=text[:4000],
               disable_web_page_preview=True, reply_markup={"inline_keyboard": kb or []})
        if r.get("ok") or "not modified" in str(r.get("description", "")):
            return
    send(chat, text, kb)


def ans(cq, text=None, alert=False):
    p = {"callback_query_id": cq["id"]}
    if text:
        p.update(text=text[:200], show_alert=alert)
    tg("answerCallbackQuery", **p)


def member_ok(r):
    return r.get("status") in ("creator", "administrator", "member") or (r.get("status") == "restricted" and r.get("is_member"))


def bot_is_admin(chat_id):
    r = tg("getChatMember", chat_id=chat_id, user_id=bot_id())
    return bool(r.get("ok") and r["result"].get("status") in ("administrator", "creator"))


def resolve_chat(uname):
    r = tg("getChat", chat_id="@" + uname)
    if r.get("ok") and r["result"].get("type") in ("channel", "group", "supergroup"):
        return r["result"]
    return None


# ───────────────────────── دیتابیس: کاربر، امتیاز، شمارنده ─────────────────────────
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


def main_kb():
    return [[btn("🎯 انجام تسک و کسب امتیاز", "earn")],
            [btn("➕ ثبت تسک جدید", "new"), btn("📋 تسک‌های من", "my")],
            [btn("👤 حساب من", "acc"), btn("❓ راهنما", "help")]]


def menu_text(u):
    return (f"سلام {u.get('name') or ''} 👋\n\n💰 موجودی: {u.get('points', 0)} امتیاز"
            + (f"  (⏳ معلق: {u['hold']})" if u.get("hold") else "")
            + "\n\nبا انجام تسک‌های بقیه امتیاز بگیر و با امتیاز، عضو/ری‌اکشن/بازدید برای کانال و پست خودت بگیر.")


def help_text():
    t = TYPES
    return (
        "❓ راهنما\n\n"
        "🎯 انجام تسک: تسک رو انجام بده و «انجام دادم» رو بزن؛ امتیاز بعد از بازبینی نهایی قطعی می‌شه.\n"
        f"• {t['join']['title']}: {t['join']['reward']} امتیاز — اتوماتیک تأیید می‌شه\n"
        f"• {t['react']['title']}: {t['react']['reward']} امتیاز — در گروه اتوماتیک، در کانال با اسکرین‌شات\n"
        f"• {t['view']['title']}: {t['view']['reward']} امتیاز — با صبر کردن چند ثانیه\n\n"
        f"⏳ پاداش {HOLD_HOURS:g} ساعت معلق می‌مونه. اگه در این مدت از کانال لفت بدی یا ری‌اکشن رو برداری، امتیاز برمی‌گرده و "
        f"اخطار می‌گیری ({STRIKE_LIMIT} اخطار = بسته شدن انجام تسک).\n\n"
        "➕ ثبت تسک: هزینه‌ی هر واحد "
        f"عضویت {t['join']['cost']}، ری‌اکشن {t['react']['cost']}، بازدید {t['view']['cost']} امتیازه. "
        "ربات باید در کانال/گروه شما ادمین باشه (برای عضویت الزامیه). فقط کانال/گروه عمومی (با یوزرنیم) پشتیبانی می‌شه.\n\n"
        "📸 ری‌اکشن در کانال: بعد از ری‌اکشن، اسکرین‌شات بگیر (اسم کانال، پست و ری‌اکشن انتخاب‌شده‌ات دیده بشه) و برای ربات بفرست."
    )


def acc_text(u):
    n = db.campaigns.count_documents({"owner": u["_id"], "status": "active"})
    return (f"👤 حساب من\n\n💰 موجودی قابل خرج: {u.get('points', 0)}\n⏳ معلق: {u.get('hold', 0)}\n"
            f"🏆 مجموع کسب‌شده: {u.get('earned', 0)}\n⚠️ اخطار: {u.get('strikes', 0)}/{STRIKE_LIMIT}\n"
            f"📋 تسک فعال: {n}")


def link_of(c):
    return f"https://t.me/{c['uname']}" + (f"/{c['mid']}" if c.get("mid") else "")


def task_text(c):
    t = TYPES[c["type"]]
    head = f"{t['title']}  •  پاداش: {t['reward']} امتیاز\n📌 {c.get('title') or c['uname']}\n\n"
    if c["type"] == "join":
        how = (f"۱) «باز کردن» رو بزن و عضو شو\n۲) برگرد و «انجام دادم» رو بزن\n\n"
               f"⚠️ پاداش {HOLD_HOURS:g} ساعت معلق می‌مونه؛ لفت بدی برمی‌گرده و اخطار می‌گیری.")
    elif c["type"] == "view":
        how = f"۱) «باز کردن» رو بزن و پست رو ببین\n۲) حداقل {t['wait']} ثانیه صبر کن و «انجام دادم» رو بزن"
    elif c.get("mode") == "auto":
        how = ("۱) «باز کردن» رو بزن و روی پست ری‌اکشن بزن\n۲) «انجام دادم» رو بزن\n\n"
               f"⚠️ ری‌اکشن رو تا {HOLD_HOURS:g} ساعت برندار؛ برداری امتیاز برمی‌گرده و اخطار می‌گیری.")
    else:
        how = ("۱) «باز کردن» رو بزن و روی پست ری‌اکشن بزن\n۲) از پست اسکرین‌شات بگیر (اسم کانال، پست و ری‌اکشن انتخاب‌شده‌ات دیده بشه)\n"
               "۳) «انجام دادم» رو بزن و عکس رو بفرست")
    return head + how


# ───────────────────────── تسک‌ها: انتخاب، ثبت انجام، تأیید نهایی ─────────────────────────
def next_task(uid, skip):
    done = [x["cid"] for x in db.completions.find({"uid": uid}, {"cid": 1})]
    ids = done + [ObjectId(s) for s in skip if ObjectId.is_valid(s)]
    return db.campaigns.find_one({"status": "active", "remaining": {"$gt": 0}, "owner": {"$ne": uid},
                                  "_id": {"$nin": ids}}, sort=[("created", 1)])


def show_task(uid, chat, mid=None):
    u = db.users.find_one({"_id": uid}) or {}
    if u.get("strikes", 0) >= STRIKE_LIMIT:
        return view(chat, mid, "⛔ به‌خاطر اخطارهای مکرر، انجام تسک برات بسته شده. می‌تونی همچنان تسک ثبت کنی.", HOME_KB)
    if counter_get(f"d:{uid}:{day()}") >= DAILY_TASKS:
        return view(chat, mid, f"✋ به سقف {DAILY_TASKS} تسک امروز رسیدی. فردا دوباره بیا!", HOME_KB)
    c = next_task(uid, u.get("skip", []))
    if not c and u.get("skip"):
        db.users.update_one({"_id": uid}, {"$set": {"skip": []}})
        c = next_task(uid, [])
    if not c:
        return view(chat, mid, "فعلاً تسک جدیدی نیست 🙂 چند دقیقه‌ی دیگه دوباره سر بزن.",
                    [[btn("🔄 تلاش دوباره", "earn")], [btn("🏠 منوی اصلی", "home")]])
    db.sessions.update_one({"_id": f"{uid}:{c['_id']}"}, {"$set": {"t": time.time()}}, upsert=True)
    view(chat, mid, task_text(c), [[btn("🔗 باز کردن", url=link_of(c))],
                                   [btn("✅ انجام دادم", f"c:{c['_id']}")],
                                   [btn("⏭ بعدی", f"nx:{c['_id']}"), btn("🏠 منو", "home")]])


def grant(uid, c, status):
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
    try:
        comp["_id"] = db.completions.insert_one(comp).inserted_id
    except DuplicateKeyError:
        db.campaigns.update_one({"_id": c["_id"]}, {"$inc": {"remaining": 1}})
        db.counters.update_one({"_id": f"d:{uid}:{day()}"}, {"$inc": {"n": -1}})
        return "dup", None
    if status == "pending":
        db.users.update_one({"_id": uid}, {"$inc": {"hold": c["reward"]}})
    if r["remaining"] == 1:
        send(c["owner"], f"🎉 کمپین «{c.get('title') or c['uname']}» ({TYPES[c['type']]['title']}) تکمیل شد!\n"
                         f"تأیید نهایی طی {HOLD_HOURS:g} ساعت انجام می‌شه و اگه کسی تخلف کنه ظرفیتش دوباره باز می‌شه.")
    return "ok", comp


def claim_text(code, c):
    return {"limit": f"✋ سقف {DAILY_TASKS} تسک امروز پر شده.",
            "full": "این تسک همین الان تکمیل شد 😅 برو سراغ بعدی.",
            "dup": "این تسک رو قبلاً انجام دادی."}[code]


def after_ok(cq, chat, mid, c):
    ans(cq, "✅ ثبت شد")
    view(chat, mid, f"✅ ثبت شد!\n{c['reward']} امتیاز {HOLD_HOURS:g} ساعت معلق می‌مونه و بعد از بازبینی نهایی به موجودی‌ات اضافه می‌شه.",
         [[btn("🎯 تسک بعدی", "earn")], [btn("🏠 منو", "home")]])


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
    if u.get("strikes", 0) >= STRIKE_LIMIT or c["owner"] == uid:
        return ans(cq, "امکان انجام این تسک نیست", True)
    if db.completions.find_one({"uid": uid, "cid": cid}):
        ans(cq, "این تسک رو قبلاً انجام دادی", True)
        return show_task(uid, chat, mid)
    ses = db.sessions.find_one({"_id": f"{uid}:{cid}"})
    if not ses:
        ans(cq, "اول تسک رو از منو باز کن", True)
        return show_task(uid, chat, mid)
    wait = TYPES[c["type"]]["wait"] - (time.time() - ses["t"])
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
    elif c["type"] == "react":
        db.states.replace_one({"_id": uid}, {"_id": uid, "step": "shot", "cid": str(cid), "t": time.time()}, upsert=True)
        ans(cq)
        return view(chat, mid, "📸 حالا اسکرین‌شات پست رو همین‌جا بفرست.\n"
                               "باید اسم کانال، پست و ری‌اکشن انتخاب‌شده‌ی خودت توش دیده بشه.\n"
                               f"مهلت: {SHOT_WINDOW // 60} دقیقه.", [[btn("❌ انصراف", "home")]])

    code, comp = grant(uid, c, "pending")
    if code != "ok":
        ans(cq, claim_text(code, c), True)
        return show_task(uid, chat, mid)
    after_ok(cq, chat, mid, c)


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


# ───────────────────────── موتور محلی: بدون API و بدون مدل آنلاین ─────────────────────────
# چک پیکسلی چیپ + OCR محلی (ONNX روی همین سرور) + تطبیق تصویر پست (ORB) + منطق موقعیت (فوتر پست).
# هیچ تماس شبکه‌ای نداره، پس قطع و وصل و سقف رایگان و کند شدن سرویس‌دهنده معنی نداره.
# VISION_MODE: auto (پیش‌فرض: اول محلی؛ فقط اگه «نامطمئن» بود و AI تنظیم شده باشه از مدل کمک می‌گیره) | local | ai (رفتار قبلی)
VISION_MODE = os.environ.get("VISION_MODE", "auto").strip().lower()
OCR_MAXW    = int(os.environ.get("OCR_MAXW", "1000"))           # عرض عکس برای OCR (کوچک‌تر = سریع‌تر و کم‌مصرف‌تر)
try:
    import importlib.util as _iu
    LOCAL_ON = _iu.find_spec("rapidocr_onnxruntime") is not None and _iu.find_spec("cv2") is not None
except Exception:
    LOCAL_ON = False
VISION_ON = AI_ON or LOCAL_ON

_ocr = {"eng": None, "err": "", "lock": threading.Lock()}
_FA_NORM = str.maketrans({"ي": "ی", "ك": "ک", "ۀ": "ه", "ة": "ه", "أ": "ا", "إ": "ا", "ؤ": "و"})


def _ocr_engine():
    with _ocr["lock"]:
        if _ocr["eng"] is None and not _ocr["err"]:
            try:
                from rapidocr_onnxruntime import RapidOCR
                _ocr["eng"] = RapidOCR()
            except Exception as e:
                _ocr["err"] = f"OCR محلی بالا نیومد: {e}"[:200]
                log.warning(_ocr["err"])
        return _ocr["eng"]


def ocr_read(b, maxw=None):
    """OCR محلی. خروجی: لیست (متن، اطمینان، (x0,y0,x1,y1) کسری از ابعاد عکس) یا None اگه موتور در دسترس نیست"""
    eng = _ocr_engine()
    if eng is None:
        return None
    try:
        from PIL import Image
        im = Image.open(io.BytesIO(b)).convert("RGB")
        if maxw and im.width > maxw:
            im = im.resize((maxw, int(im.height * maxw / im.width)), Image.LANCZOS)
        w, h = im.size
        buf = io.BytesIO()
        im.save(buf, "JPEG", quality=92)
        with _ocr["lock"]:                                   # یک موتور مشترک؛ درخواست‌ها پشت سر هم اجرا می‌شن
            res, _ = eng(buf.getvalue())
        out = []
        for box, txt, conf in (res or []):
            xs, ys = [p[0] for p in box], [p[1] for p in box]
            out.append((str(txt), float(conf), (min(xs) / w, min(ys) / h, max(xs) / w, max(ys) / h)))
        return out
    except Exception as e:
        log.warning("local ocr failed: %s", e)
        return None


def norm_txt(s):
    s = re.sub(r"[\u064b-\u065f\u0670]", "", str(s or "")).translate(FA).translate(_FA_NORM).lower()
    return re.sub(r"[^0-9a-z\u0600-\u06ff]+", "", s)       # ایموجی، فاصله، نیم‌فاصله و علامت‌ها حذف می‌شن


def latin_len(s):
    return len(re.findall(r"[a-z0-9]", norm_txt(s)))


def fuzzy_in(needle, hay):
    """شباهت (۰ تا ۱) نزدیک‌ترین تکه‌ی hay به needle؛ تحمل خطای OCR"""
    from difflib import SequenceMatcher
    n, hy = norm_txt(needle), norm_txt(hay)
    if len(n) < 3 or not hy:
        return 0.0
    if n in hy:
        return 1.0
    best = 0.0
    for L in {len(n), max(3, len(n) - 2), len(n) + 2}:
        for i in range(0, max(1, len(hy) - L + 1)):
            best = max(best, SequenceMatcher(None, n, hy[i:i + L]).ratio())
    return best


_CLK = re.compile(r"(?<!\d)(\d{1,2})\s*[:：]\s*(\d{2})(?!\d)")
_FOOT = re.compile(r"^(?:\S{1,8}\s+)?(?:edited\s+)?\d{1,2}\s*:\s*\d{2}(?:\s?[ap]m)?$", re.I)      # فوتر حباب: «1.2K 14:20» یا «edited 14:20»


def _clock_of(txt):
    m = _CLK.search(str(txt).translate(FA))
    return f"{int(m[1])}:{m[2]}" if m and int(m[1]) <= 23 and int(m[2]) <= 59 else None


def local_clock_reads(img, items, since=None):
    """ساعت نوار وضعیت: یک‌بار از OCR کل عکس (فقط نوار بالا)، یک‌بار از برش بزرگ‌شده‌ی نوار بالا"""
    reads = [t for t in (_clock_of(x[0]) for x in items if x[2][3] <= 0.07 and x[1] >= 0.5) if t]
    if any(time_ok(t, since) for t in reads):
        return reads                                         # خوندنِ اول قبوله؛ OCR دوم لازم نیست (سریع‌تر)
    try:
        crop = ocr_read(status_crop(img, 2)) or []
        reads += [t for t in (_clock_of(x[0]) for x in crop if x[1] >= 0.5) if t]
    except Exception as e:
        log.warning("clock crop failed: %s", e)
    return reads


def locate_ref(shot, ref):
    """پیدا کردن عکس مرجع پست داخل اسکرین‌شات با ORB + هوموگرافی (مستقل از زبان، تم و اندازه). خروجی: (x0,y0,x1,y1) کسری یا None"""
    try:
        import cv2
        import numpy as np
        s = cv2.imdecode(np.frombuffer(shot, np.uint8), cv2.IMREAD_GRAYSCALE)
        r = cv2.imdecode(np.frombuffer(ref, np.uint8), cv2.IMREAD_GRAYSCALE)
        if s is None or r is None:
            return None
        if s.shape[1] > 900:
            s = cv2.resize(s, (900, int(s.shape[0] * 900 / s.shape[1])))
        if r.shape[1] > 600:
            r = cv2.resize(r, (600, int(r.shape[0] * 600 / r.shape[1])))
        orb = cv2.ORB_create(2000)
        ks, ds = orb.detectAndCompute(s, None)
        kr, dr = orb.detectAndCompute(r, None)
        if ds is None or dr is None or len(kr) < 25 or len(ks) < 25:
            return None
        mt = cv2.BFMatcher(cv2.NORM_HAMMING).knnMatch(dr, ds, k=2)
        good = [m[0] for m in mt if len(m) == 2 and m[0].distance < 0.75 * m[1].distance]
        if len(good) < 14:
            return None
        src = np.float32([kr[g.queryIdx].pt for g in good]).reshape(-1, 1, 2)
        dst = np.float32([ks[g.trainIdx].pt for g in good]).reshape(-1, 1, 2)
        M, mask = cv2.findHomography(src, dst, cv2.RANSAC, 4.0)
        if M is None or int(mask.sum()) < 12:
            return None
        h, w = r.shape
        pts = cv2.perspectiveTransform(np.float32([[0, 0], [w, 0], [w, h], [0, h]]).reshape(-1, 1, 2), M).reshape(-1, 2)
        Hs, Ws = s.shape
        (x0, y0), (x1, y1) = pts.min(0), pts.max(0)
        box = (max(0.0, x0 / Ws), max(0.0, y0 / Hs), min(1.0, x1 / Ws), min(1.0, y1 / Hs))
        return box if (box[2] - box[0]) >= 0.2 and (box[3] - box[1]) >= 0.04 else None
    except Exception as e:
        log.warning("locate_ref failed: %s", e)
        return None


def locate_hint(items, hint):
    """پیدا کردن خط‌های متن پست (فقط لاتین/عدد؛ OCR پیش‌فرض فارسی رو مطمئن نمی‌خونه). خروجی: (x0,y0,x1,y1) یا None"""
    from difflib import SequenceMatcher
    h = norm_txt(hint)
    if latin_len(hint) < 6:
        return None
    hit = []
    for t, conf, bb in items:
        n = norm_txt(t)
        if len(n) < 6 or conf < 0.6:
            continue
        mm = SequenceMatcher(None, n, h).find_longest_match(0, len(n), 0, len(h))
        if mm.size >= max(6, 0.7 * len(n)):
            hit.append(bb)
    if not hit:
        return None
    return (min(b[0] for b in hit), min(b[1] for b in hit), max(b[2] for b in hit), max(b[3] for b in hit))


def own_chips(boxes, anchor, items, max_gap=0.35):
    """چیپ‌هایی که توی حباب همون پست‌اند: زیر پست، بالای اولین فوروتر (ساعت/بازدید) بعد از پست، و نه خیلی دور"""
    ax0, ay0, ax1, ay1 = anchor
    foot = [x[2][1] for x in items if x[2][1] >= ay1 - 0.01 and len(str(x[0])) <= 16 and _FOOT.match(str(x[0]).translate(FA).strip()) and x[1] >= 0.5]
    foot_y = min(foot) if foot else None
    if foot_y is None:
        max_gap = min(max_gap, 0.25)                         # فوتر دیده نشد → فقط چیپ‌های خیلی نزدیک
    out = []
    for b in boxes:
        if b[1] < ay1 - 0.04 or b[1] - ay1 > max_gap:
            continue
        if foot_y is not None and b[3] > foot_y + 0.01:      # چیپ زیرِ فوتر پست = مال حباب بعدیه
            continue
        out.append(b)
    return out


def local_verdict(img, c, since=None):
    """خروجی: (approve | reject | unsure | None، دلیل، خطا). reject فقط با شواهد منفی روشن؛ نداشتن شواهد کافی = unsure"""
    if not PIL_OK:
        return None, "", "Pillow نصب نیست"
    boxes = find_chips(img)
    if boxes is None:
        return None, "", "چک پیکسلی خطا داد (لاگ سرور رو ببین)"
    if not boxes:
        return "reject", "ری‌اکشنی نزدی (یا تو عکس دیده نمی‌شه)؛ اول ری‌اکشن بزن بعد اسکرین‌شات بگیر", ""
    items = ocr_read(img, OCR_MAXW)
    if items is None:
        return None, "", _ocr["err"] or "OCR محلی در دسترس نیست (rapidocr-onnxruntime نصب نیست؟)"
    bad, unsure = [], []
    # ۱) ساعت گوشی
    reads = local_clock_reads(img, items, since)
    why = clock_decide(reads, since)
    if why:
        (bad if reads and not any(time_ok(t, since) for t in reads) else unsure).append(why)
    # ۲) اسم کانال (فقط وقتی عنوان لاتین/عدد داره؛ عنوان فارسی با تطبیق تصویر پست پوشش داده می‌شه)
    title = c.get("title") or ""
    if latin_len(title) >= 3:
        hdr = " ".join(x[0] for x in items if 0.03 <= x[2][1] <= 0.22 and x[1] >= 0.4)
        sc = fuzzy_in(norm_txt(title)[:24], hdr)
        if sc < 0.5:
            bad.append("اسم کانال بالای عکس دیده نمی‌شه")
        elif sc < 0.72:
            unsure.append("اسم کانال بالای عکس واضح نیست")
    # ۳) پیدا کردن پست تسک + مالکیت ری‌اکشن
    anchor = None
    if c.get("ref_photo"):
        ref = fetch_file(c["ref_photo"])
        if ref:
            anchor = locate_ref(img, ref)
    if anchor is None and c.get("hint"):
        anchor = locate_hint(items, c["hint"])
    if anchor is None:
        unsure.append("پستِ تسک توی عکس پیدا نشد؛ اسکرین‌شات رو طوری بگیر که خودِ پست و ری‌اکشنش کامل دیده بشه")
    elif not own_chips(boxes, anchor, items):
        bad.append("ری‌اکشنت مالِ پستِ تسک نیست؛ روی خودِ پست تسک ری‌اکشن بزن")
    log.info("local: bad=%s unsure=%s clocks=%s anchor=%s boxes=%s", bad, unsure, reads, anchor, boxes)
    if bad:
        return "reject", "\n".join("• " + x for x in bad) if len(bad) > 1 else bad[0], ""
    if unsure:
        return "unsure", "\n".join("• " + x for x in unsure) if len(unsure) > 1 else unsure[0], ""
    return "approve", "", ""


def vision_verdict(img, c, since=None):
    """مسیر یکپارچه: محلی اول؛ فقط در حالت auto و فقط وقتی محلی «نامطمئن» بود، مدل (اگه تنظیم شده) کمک می‌کنه.
    خروجی مثل ai_verdict: (حکم، دلیل، خطا)؛ حکم None = مشکل سرویس/ابزار (نه عکس کاربر)"""
    if VISION_MODE == "ai" or not LOCAL_ON:
        return ai_verdict(img, c, since)
    v, why, err = local_verdict(img, c, since)
    if AI_DEBUG and v in ("reject", "unsure"):
        for a_ in ADMINS:
            send(a_, f"🔎 محلی: {v}\n{why}")
    if v in ("approve", "reject"):
        return v, why, ""
    if VISION_MODE == "auto" and AI_ON:
        v2 = ai_verdict(img, c, since)
        if v2[0] is not None:
            return v2
    if v == "unsure":
        return "reject", why, ""
    return None, "", err


def local_selftest():
    if not LOCAL_ON:
        return "❌ نصب نیست (rapidocr-onnxruntime رو به requirements.txt اضافه کن)"
    try:
        from PIL import Image, ImageDraw, ImageFont
        im = Image.new("RGB", (900, 140), (20, 20, 20))
        try:
            f = ImageFont.load_default(size=70)
        except TypeError:
            f = ImageFont.load_default()
        ImageDraw.Draw(im).text((30, 25), "12:34", fill=(255, 255, 255), font=f)
        b = io.BytesIO()
        im.save(b, "PNG")
        t0 = time.time()
        items = ocr_read(b.getvalue())
        txt = " ".join(x[0] for x in items or [])
        return f"{'✅' if '12:34' in txt else '⚠️'} OCR محلی ({time.time() - t0:.1f}s) خوند: {txt[:30]!r}"
    except Exception as e:
        return f"❌ {e}"[:150]


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
        verdict, why, err = vision_verdict(img, c, since)
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
        send(chat, f"✅ تأیید شد!\n{c['reward']} امتیاز {HOLD_HOURS:g} ساعت معلق می‌مونه و بعد قطعی می‌شه.",
             [[btn("🎯 تسک بعدی", "earn")]])
    except Exception:
        log.exception("verify_shot")
        db.shots.delete_one({"_id": fid})
        send(chat, "⚠️ خطایی رخ داد؛ دوباره امتحان کن.", [[btn("🏠 منو", "home")]])


# ───────────────────────── برگشت / پرداخت / بازبینی ─────────────────────────
def undo(comp, had_hold, strike, why):
    """لغو یک انجام: امتیاز معلق حذف، ظرفیت کمپین دوباره باز (یا اگه کمپین لغو شده، هزینه به صاحب برمی‌گرده)"""
    inc = {"hold": -comp["reward"]} if had_hold else {}
    if strike:
        inc["strikes"] = 1
    if inc:
        db.users.update_one({"_id": comp["uid"]}, {"$inc": inc})
    r = db.campaigns.update_one({"_id": comp["cid"], "status": "active"}, {"$inc": {"remaining": 1}})
    if not r.matched_count:
        c = db.campaigns.find_one({"_id": comp["cid"]}) or {}
        if c.get("cost"):
            add_points(comp["owner"], c["cost"], "refund", comp["cid"])
    send(comp["uid"], f"⚠️ {why}" + (f"\nیک اخطار ثبت شد ({STRIKE_LIMIT} اخطار = بسته شدن انجام تسک)." if strike else ""))


def pay(comp):
    r = db.completions.find_one_and_update({"_id": comp["_id"], "status": "settling"},
                                           {"$set": {"status": "paid", "paid": time.time()}})
    if not r:
        return
    db.users.update_one({"_id": comp["uid"]}, {"$inc": {"hold": -comp["reward"], "points": comp["reward"], "earned": comp["reward"]}})
    ledger(comp["uid"], comp["reward"], "task", comp["cid"])
    send(comp["uid"], f"💰 {comp['reward']} امتیاز قطعی شد و به موجودی‌ات اضافه شد.")


def settle(comp):
    """پایان دوره‌ی معلق: چک دوباره، بعد پرداخت یا برگشت"""
    c = db.campaigns.find_one({"_id": comp["cid"]}) or {}
    ok = True
    if comp["type"] == "join":
        r = tg("getChatMember", chat_id=c.get("chat_id"), user_id=comp["uid"])
        if r.get("ok") and not member_ok(r["result"]):                 # خطای API = نمی‌شه بررسی کرد → به نفع کاربر
            ok = False
    elif comp["type"] == "react" and c.get("mode") == "auto":
        d = db.reacts.find_one({"_id": f"{c.get('chat_id')}:{c.get('mid')}:{comp['uid']}"})
        ok = bool(d and d.get("on"))
    if ok:
        return pay(comp)
    r = db.completions.find_one_and_update({"_id": comp["_id"], "status": "settling"}, {"$set": {"status": "reversed"}})
    if r:
        undo(comp, True, True, "از کانال خارج شدی یا ری‌اکشن رو برداشتی؛ امتیاز این تسک برگشت خورد.")


_last = {"clean": 0}


def tick():
    t = time.time()
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
            db.completions.update_one({"_id": comp["_id"], "status": "settling"}, {"$set": {"status": "pending"}})
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
    view(chat, mid, "➕ ثبت تسک جدید — نوعش رو انتخاب کن:\n\n"
         + "\n".join(f"{v['title']}: هزینه‌ی هر واحد {v['cost']} امتیاز" for v in t.values()),
         [[btn(f"{v['title']} ({v['cost']})", f"nt:{k}") for k, v in t.items()], [btn("🏠 منوی اصلی", "home")]])


def pick_type(cq, uid, chat, mid, typ):
    if typ not in TYPES:
        return ans(cq)
    if db.campaigns.count_documents({"owner": uid, "status": "active"}) >= MAX_ACTIVE:
        return ans(cq, f"حداکثر {MAX_ACTIVE} تسک فعال می‌تونی داشته باشی.", True)
    ans(cq)
    set_state(uid, "link", {"type": typ})
    if typ == "join":
        txt = ("👥 یوزرنیم یا لینک کانال/گروه عمومیت رو بفرست (مثلاً @mychannel).\n\n"
               f"⚠️ اول ربات @{_me.get('username', '')} رو ادمین اون کانال/گروه کن؛ بدون اون عضویت قابل تأیید نیست.")
    else:
        txt = ("🔗 لینک پست رو بفرست (مثلاً https://t.me/mychannel/123).\n"
               "فقط پست‌های کانال/گروه عمومی (با یوزرنیم) پشتیبانی می‌شه.")
    view(chat, mid, txt, [[btn("❌ انصراف", "home")]])


def ask_qty(chat, d, c_cost):
    u = db.users.find_one({"_id": chat}) or {}
    mx = min(MAX_QTY, u.get("points", 0) // c_cost)
    if mx < 1:
        return send(chat, f"موجودی‌ات ({u.get('points', 0)}) برای ثبت حتی یک واحد ({c_cost} امتیاز) کافی نیست. اول چند تسک انجام بده.",
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
            else:
                if not VISION_ON:
                    return send(chat, "❌ ری‌اکشن برای کانال‌ها فعلاً فعال نیست. ری‌اکشن فقط برای گروهی که ربات ادمینشه (تأیید خودکار) ممکنه.")
                d["mode"] = "shot"
                set_state(uid, "ref", d)
                return send(chat, "🔎 برای اینکه ربات پست دقیق رو تشخیص بده (نه هر پست دیگه‌ی کانال)، همون پست رو از کانال برای من فوروارد کن.\n\n"
                                  "اگه کانال فوروارد رو بسته، یه جمله‌ی مشخص و منحصربه‌فرد از متن پست رو تایپ کن.",
                            [[btn("❌ انصراف", "home")]])
        set_state(uid, "qty", d)
        return ask_qty(uid, d, cost)
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
               "react": "تأیید خودکار (گروه)" if d.get("mode") == "auto" else "اسکرین‌شات + بررسی خودکار (پیکسل + OCR) + ضدتکرار"}[typ]
        return send(chat, f"📝 خلاصه‌ی تسک\n\n{TYPES[typ]['title']}\n📌 {d['title']}\n🔢 تعداد: {q}\n💰 هزینه: {q * cost} امتیاز\n"
                          f"🛡 تأیید: {how}\n\nثبت بشه؟",
                    [[btn(f"✅ ثبت و کسر {q * cost} امتیاز", "ok")], [btn("❌ انصراف", "home")]])
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
    if db.campaigns.count_documents({"owner": uid, "status": "active"}) >= MAX_ACTIVE:
        return ans(cq, f"حداکثر {MAX_ACTIVE} تسک فعال می‌تونی داشته باشی.", True)
    if not spend(uid, d["qty"] * t["cost"], "campaign", d["uname"]):
        return ans(cq, "موجودی کافی نیست", True)
    db.states.delete_one({"_id": uid})
    db.campaigns.insert_one({"owner": uid, "type": typ, "chat_id": d["chat_id"], "uname": d["uname"], "title": d["title"],
                             "mid": d.get("mid"), "mode": d.get("mode"), "hint": d.get("ref_text", ""), "ref_photo": d.get("ref_photo"), "target": d["qty"],
                             "remaining": d["qty"], "cost": t["cost"], "reward": t["reward"], "status": "active",
                             "created": time.time()})
    ans(cq, "✅ ثبت شد")
    view(chat, mid, f"✅ تسک ثبت شد و در لیست کاربران قرار گرفت!\nهر زمان تکمیل شد خبرت می‌کنم.",
         [[btn("📋 تسک‌های من", "my")], [btn("🏠 منو", "home")]])


def my_campaigns(cq, uid, chat, mid):
    ans(cq)
    render_my(uid, chat, mid)


def render_my(uid, chat, mid):
    cs = list(db.campaigns.find({"owner": uid}, sort=[("created", -1)], limit=10))
    if not cs:
        return view(chat, mid, "هنوز تسکی ثبت نکردی.", [[btn("➕ ثبت تسک جدید", "new")], [btn("🏠 منو", "home")]])
    lines, kb = [], []
    for i, c in enumerate(cs, 1):
        st = "⛔ لغو شده" if c["status"] != "active" else ("✅ تکمیل" if c["remaining"] == 0 else "🟢 فعال")
        lines.append(f"{i}. {TYPES[c['type']]['title']} {c['title']} — {c['target'] - c['remaining']}/{c['target']} {st}")
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
        ans(cq, f"لغو شد؛ {refund} امتیاز برگشت", True)
    render_my(uid, chat, mid)


# ───────────────────────── دستورات ادمین ─────────────────────────
def admin_cmd(uid, chat, cmd, args):
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
                    + f"\n\nموتور محلی (VISION_MODE={VISION_MODE}): {local_selftest()}"
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
    send(chat, "دستورها: /stats  /aitest  /give <uid> <n>  /unban <uid>  /stop <campaign_id>")


# ───────────────────────── مسیریابی آپدیت‌ها ─────────────────────────
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
        return show_task(uid, chat, mid)
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
    if data.startswith("x:"):
        return cancel_campaign(cq, uid, chat, mid, data[2:], False)
    if data.startswith("xy:"):
        return cancel_campaign(cq, uid, chat, mid, data[3:], True)
    ans(cq)


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
    if LOCAL_ON and VISION_MODE != "ai":
        threading.Thread(target=_ocr_engine, daemon=True).start()      # گرم‌کردن موتور OCR تا اولین اسکرین‌شات کند نشه
    r = tg("setWebhook", url=f"{BASE_URL}/hook", secret_token=HOOK_SECRET,
           allowed_updates=["message", "callback_query", "message_reaction"])
    tg("setMyCommands", commands=[{"command": "start", "description": "منوی اصلی"}, {"command": "help", "description": "راهنما"}])
    log.info("webhook: %s", r)
    threading.Thread(target=worker, daemon=True).start()


setup()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
