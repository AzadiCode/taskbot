# -*- coding: utf-8 -*-
"""
TaskExchange — ربات تبادل تسک تلگرام (عضویت / ری‌اکشن / بازدید)
Stack (مثل نمونه‌ی AI BotMaker): Flask + MongoDB(pymongo) + وب‌هوک، تک‌فایل و بدون مینی‌اپ

مدل کار:
- کاربر با انجام تسک «امتیاز» می‌گیره و با امتیاز، تسک خودش رو ثبت می‌کنه (هزینه‌ی ثبت > پاداش؛ اقتصاد تورم نمی‌کنه)
- امتیاز بعد از انجام تسک HOLD_HOURS ساعت «معلق» می‌مونه، بعد دوباره چک می‌شه و قطعی می‌شه
- عضویت  : getChatMember (ربات باید ادمین کانال/گروه باشه) + چک دوباره‌ی آخر دوره‌ی معلق (لفت = برگشت امتیاز + اخطار)
- ری‌اکشن : گروه با ربات ادمین → تأیید خودکار با message_reaction (برداشتن ری‌اکشن هم دیده می‌شه)
            کانال (ری‌اکشن‌ها ناشناس‌اند) → اسکرین‌شات + مدل ویژن (کانال/پست/هایلایت/ساعت) + ضدتکرار
            (file_unique_id و هش تصویری). تصمیم کاملاً خودکاره: تأیید یا رد؛ نامطمئن = درخواست عکس واضح‌تر
- بازدید  : فقط تأیید زمانی (از طریق Bot API قابل راستی‌آزمایی نیست) → پاداش کم

نیازمندی‌ها: flask pymongo requests gunicorn   (اختیاری: Pillow برای هش تصویری، mongomock برای تست بدون دیتابیس)
اجرا: gunicorn app:app
"""
import os, re, io, json, time, hmac, hashlib, base64, random, logging, threading
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
AI_MODEL    = os.environ.get("AI_MODEL", "")
AI_ON       = bool(AI_BASE_URL and AI_API_KEY and AI_MODEL)

START_POINTS = int(os.environ.get("START_POINTS", "5"))        # هدیه‌ی شروع
HOLD_HOURS   = float(os.environ.get("HOLD_HOURS", "24"))       # مدت معلق ماندن پاداش
DAILY_TASKS  = int(os.environ.get("DAILY_TASKS", "60"))        # سقف تسک روزانه‌ی هر کاربر
DAILY_AI     = int(os.environ.get("DAILY_AI", "12"))           # سقف بررسی اسکرین‌شات در روز برای هر کاربر
STRIKE_LIMIT = int(os.environ.get("STRIKE_LIMIT", "3"))        # بعد از این تعداد اخطار، انجام تسک بسته می‌شه
MAX_QTY      = int(os.environ.get("MAX_QTY", "500"))
MAX_ACTIVE   = int(os.environ.get("MAX_ACTIVE", "10"))         # سقف کمپین فعال هر کاربر
SHOT_WINDOW  = 600                                              # مهلت ارسال اسکرین‌شات بعد از زدن «انجام دادم» (ثانیه)
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


def time_ok(s):
    """ساعت نوار وضعیت گوشی با الان مقایسه می‌شه (خوانده نشده = مشکلی نیست). ساعت ۱۲ و ۲۴ ساعته هر دو پوشش داده می‌شن"""
    m = re.match(r"^\s*(\d{1,2}):(\d{2})", str(s or ""))
    if not m:
        return True
    try:
        from zoneinfo import ZoneInfo
        ref = datetime.now(ZoneInfo(TZ_NAME))
    except Exception:
        ref = datetime.now(timezone(timedelta(hours=3, minutes=30)))
    d = abs(int(m[1]) * 60 + int(m[2]) - (ref.hour * 60 + ref.minute)) % 720
    return min(d, 720 - d) <= 12


def ai_request(prompt, img, mime="image/jpeg"):
    """تماس با API سازگار با OpenAI. خروجی: (متن، خطا). خطا شامل کد HTTP و پاسخ سرور است تا علت واقعی معلوم بشه"""
    url = AI_BASE_URL + "/chat/completions"
    msgs = [{"role": "user", "content": [
        {"type": "text", "text": prompt},
        {"type": "image_url", "image_url": {"url": f"data:{mime};base64," + base64.b64encode(img).decode()}}]}]
    base = {"model": AI_MODEL, "messages": msgs}
    err = ""
    # بعضی مدل‌ها temperature/max_tokens رو قبول نمی‌کنن؛ روی خطای ۴۰۰ با پارامترهای ساده‌تر دوباره امتحان می‌شه
    for body in (dict(base, temperature=0, max_tokens=500), dict(base, max_completion_tokens=500), base):
        try:
            r = requests.post(url, headers={"Authorization": "Bearer " + AI_API_KEY}, json=body, timeout=60)
        except Exception as e:
            return None, f"اتصال به {url} ناموفق: {e}"
        try:
            j = r.json()
        except Exception:
            return None, f"HTTP {r.status_code} پاسخ غیر JSON: {getattr(r, 'text', '')[:200]}"
        if r.status_code == 200 and j.get("choices"):
            c = (j["choices"][0].get("message") or {}).get("content")
            if isinstance(c, list):
                c = " ".join(p.get("text", "") for p in c if isinstance(p, dict))
            if c and str(c).strip():
                return str(c), ""
            err = f"پاسخ خالی از مدل: {str(j)[:300]}"
        else:
            err = f"HTTP {r.status_code}: {str(j)[:300]}"
        if r.status_code != 400:
            break
    return None, err


_ai_alert = {"t": 0}


def ai_alert(err):
    log.warning("ai error: %s", err)
    if time.time() - _ai_alert["t"] > 600:
        _ai_alert["t"] = time.time()
        for a in ADMINS:
            send(a, "⚠️ خطای مدل ویژن (دستور /aitest برای تست دستی):\n" + err[:800])


def ai_check(img, c):
    """خروجی: (دیکشنری نتیجه یا None، متن خطا)"""
    if not AI_ON:
        return None, "AI_BASE_URL / AI_API_KEY / AI_MODEL تنظیم نشده"
    hint = (f'Expected post text/keyword: "{c["hint"]}".' if c.get("hint") else "No post text hint was provided.")
    prompt = (
        'You verify a user-submitted Telegram screenshot for a "react to this post" task.\n'
        f'Expected channel/group title: "{c.get("title") or ""}" (@{c["uname"]}). {hint}\n'
        "Answer ONLY with one JSON object, no markdown:\n"
        '{"is_telegram_screenshot": bool, "channel_match": true|false|null, "post_match": true|false|null, '
        '"reaction_highlighted": true|false|null, "status_bar_time": "HH:MM"|null, "confidence": 0..1}\n'
        "- is_telegram_screenshot: a genuine Telegram app screen, not edited/fake-looking, not a meme.\n"
        "- channel_match: the visible channel/group name or username matches the expected one; null if no name is visible.\n"
        "- post_match: the visible post matches the expected text; null if no hint or not readable.\n"
        "- reaction_highlighted: under the post one reaction chip is visibly highlighted/selected (the user's own reaction). "
        "false if reactions are visible but none is selected; null if no reactions are visible.\n"
        "- status_bar_time: the phone clock in the status bar if readable, else null.")
    txt, err = ai_request(prompt, img)
    if not txt:
        return None, err
    m = re.search(r"\{.*\}", txt, re.S)
    try:
        return json.loads(m.group(0)), ""
    except Exception:
        return None, f"پاسخ مدل JSON نبود: {txt[:300]}"


def judge(v, has_hint):
    """تصمیم کاملاً خودکار: approve یا reject (نامطمئن = رد با درخواست عکس واضح‌تر؛ کاربر می‌تونه دوباره بفرسته)"""
    if not isinstance(v, dict):
        return "reject", "بررسی عکس الان ممکن نشد؛ چند لحظه بعد دوباره امتحان کن"
    try:
        conf = float(v.get("confidence") or 0)
    except (TypeError, ValueError):
        conf = 0
    if conf < 0.6:
        return "reject", "عکس واضح نیست؛ اسکرین‌شات کامل‌تر و واضح‌تر بفرست"
    if v.get("is_telegram_screenshot") is not True:
        return "reject", "این تصویر اسکرین‌شات معتبر تلگرام نیست"
    if v.get("channel_match") is not True:
        return "reject", "اسم کانال داخل عکس دیده نمی‌شه یا با تسک نمی‌خونه"
    if has_hint and v.get("post_match") is not True:
        return "reject", "پست مورد نظر داخل عکس دیده نمی‌شه"
    if v.get("post_match") is False:
        return "reject", "پست داخل عکس با تسک نمی‌خونه"
    if v.get("reaction_highlighted") is not True:
        return "reject", "ری‌اکشن انتخاب‌شده‌ی تو داخل عکس دیده نمی‌شه"
    if not time_ok(v.get("status_bar_time")):
        return "reject", "ساعت داخل عکس با الان نمی‌خونه؛ اسکرین‌شات تازه بفرست"
    return "approve", ""


def verify_shot(uid, chat, cid_s, ph):
    """در ترد جدا اجرا می‌شه (تماس با مدل ممکنه کند باشه)"""
    fid = ph["file_unique_id"]
    try:
        c = db.campaigns.find_one({"_id": ObjectId(cid_s)})
        if not c or c["status"] != "active" or c["remaining"] <= 0:
            return send(chat, "این تسک دیگه ظرفیت نداره.", [[btn("🎯 تسک بعدی", "earn")]])
        try:
            db.shots.insert_one({"_id": fid, "uid": uid, "cid": cid_s, "t": time.time()})
        except DuplicateKeyError:
            return send(chat, "❌ این عکس قبلاً ارسال شده. اسکرین‌شات تازه‌ی خودت رو بفرست.", [[btn("🏠 منو", "home")]])
        img = fetch_file(ph["file_id"])
        if not img:
            db.shots.delete_one({"_id": fid})
            return send(chat, "⚠️ نتونستم عکس رو بگیرم. دوباره «انجام دادم» رو بزن و عکس رو بفرست.", [[btn("🎯 ادامه", "earn")]])
        h = img_hash(img)
        if h:
            for s in db.shots.find({"cid": cid_s, "ah": {"$exists": True}}):
                if ham(h, s["ah"]) <= 6:
                    db.shots.delete_one({"_id": fid})
                    return send(chat, "❌ این اسکرین‌شات شبیه عکس قبلیِ یه کاربر دیگه‌ست. عکس تازه‌ی خودت رو بفرست.",
                                [[btn("🏠 منو", "home")]])
            db.shots.update_one({"_id": fid}, {"$set": {"ah": h}})
        v, err = ai_check(img, c)
        if v is None:                                   # مشکل از سرویس مدله، نه از عکس کاربر
            db.shots.delete_one({"_id": fid})
            db.counters.update_one({"_id": f"ai:{uid}:{day()}"}, {"$inc": {"n": -1}})
            ai_alert(err)
            return send(chat, "⚠️ سرویس بررسی عکس الان در دسترس نیست (مشکل از عکس تو نیست). "
                              "چند دقیقه دیگه دوباره «انجام دادم» رو بزن.", [[btn("🎯 تسک‌ها", "earn")]])
        verdict, why = judge(v, bool(c.get("hint")))
        if verdict == "reject":
            db.shots.delete_one({"_id": fid})
            return send(chat, f"❌ {why}.\nاگه تسک رو درست انجام دادی، از همین تسک دوباره «انجام دادم» رو بزن و عکس بهتری بفرست.",
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
        d.update(uname=ch.get("username") or uname, chat_id=ch["id"], title=(ch.get("title") or uname)[:60], mid=mid_, mode=None, hint="")
        if typ == "join" and not bot_is_admin(ch["id"]):
            return send(chat, f"❌ ربات هنوز ادمین «{d['title']}» نیست. اول @{_me.get('username', '')} رو ادمین کن و دوباره لینک بفرست.")
        if typ == "react":
            if ch["type"] in ("group", "supergroup") and bot_is_admin(ch["id"]):
                d["mode"] = "auto"
            else:
                if not AI_ON:
                    return send(chat, "❌ ری‌اکشن برای کانال‌ها فعلاً فعال نیست. ری‌اکشن فقط برای گروهی که ربات ادمینشه (تأیید خودکار) ممکنه.")
                d["mode"] = "shot"
                set_state(uid, "hint", d)
                return send(chat, "✍️ (اختیاری، برای دقت بیشتر) یه کلمه یا جمله‌ی کوتاه از متن پست بفرست تا هوش مصنوعی توی اسکرین‌شات‌ها "
                                  "بتونه پست درست رو تشخیص بده. نمی‌خوای؟ بفرست: -", [[btn("❌ انصراف", "home")]])
        set_state(uid, "qty", d)
        return ask_qty(uid, d, cost)
    if step == "hint":
        d["hint"] = "" if text.strip() in ("-", "—") else text.strip()[:100]
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
               "react": "تأیید خودکار (گروه)" if d.get("mode") == "auto" else "اسکرین‌شات + بررسی خودکار هوش مصنوعی + ضدتکرار"}[typ]
        return send(chat, f"📝 خلاصه‌ی تسک\n\n{TYPES[typ]['title']}\n📌 {d['title']}\n🔢 تعداد: {q}\n💰 هزینه: {q * cost} امتیاز\n"
                          f"🛡 تأیید: {how}\n\nثبت بشه؟",
                    [[btn(f"✅ ثبت و کسر {q * cost} امتیاز", "ok")], [btn("❌ انصراف", "home")]])
    send(chat, "از منو ادامه بده 👇", main_kb())


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
                             "mid": d.get("mid"), "mode": d.get("mode"), "hint": d.get("hint", ""), "target": d["qty"],
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
        txt, err = ai_request('Reply with only this JSON: {"ok": true}', png, "image/png")
        return send(chat, (f"✅ مدل جواب داد:\n{txt[:500]}" if txt else f"❌ خطا:\n{err}")
                    + f"\n\nBASE: {AI_BASE_URL or '-'}\nMODEL: {AI_MODEL or '-'}")
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
        threading.Thread(target=verify_shot, args=(uid, chat, st["cid"], msg["photo"][-1]), daemon=True).start()
        return
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
    r = tg("setWebhook", url=f"{BASE_URL}/hook", secret_token=HOOK_SECRET,
           allowed_updates=["message", "callback_query", "message_reaction"])
    tg("setMyCommands", commands=[{"command": "start", "description": "منوی اصلی"}, {"command": "help", "description": "راهنما"}])
    log.info("webhook: %s", r)
    threading.Thread(target=worker, daemon=True).start()


setup()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
