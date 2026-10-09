# -*- coding: utf-8 -*-
"""
TaskBot — ربات تبادل تسک (فالو / لایک / کامنت) با سیستم امتیاز
Stack (مثل AI BotMaker): Flask + MongoDB(pymongo) + وب‌هوک تک‌فایلی

Environment:
  BOT_TOKEN    توکن ربات
  BASE_URL     مثلا https://yourapp.onrender.com
  MONGO_URI    خالی = حافظه‌ی موقت (با ری‌استارت پاک می‌شه)
  ADMIN_IDS    آیدی عددی ادمین‌ها با کاما (اختیاری)
  SECRET_KEY   اختیاری (پیش‌فرض: BOT_TOKEN)
  START_POINTS امتیاز اولیه‌ی کاربر جدید (پیش‌فرض ۱۰)

نحوه‌ی سنجش:
  فالو   → getChatMember (دقیق)            + بررسی ترک کانال تا ۳ روز با /cron
  کامنت  → ثبت پیام‌های گروه گفتگوی کانال (دقیق)
  لایک   → ری‌اکشن روی کپی پست داخل گروه گفتگو (دقیق)؛ اگه ممکن نبود: حالت نیمه‌مطمئن (پاداش نصف)
  همه‌ی تسک‌ها: مینی‌اپ ردیاب کلیک (داخل تلگرام) + تأخیر رندوم، بعد دکمه‌ی «بررسی» ظاهر می‌شه
"""
import os, re, json, time, hmac, hashlib, random, secrets, logging
from urllib.parse import parse_qsl
from datetime import datetime, timezone, timedelta

import requests
from flask import Flask, request, jsonify, abort, Response
from pymongo import MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError
from bson import ObjectId
from bson.errors import InvalidId

# ───────────────────────── تنظیمات ─────────────────────────
BOT_TOKEN    = os.environ["BOT_TOKEN"]
BASE_URL     = os.environ["BASE_URL"].rstrip("/")
MONGO_URI    = os.environ.get("MONGO_URI", "").strip()
SECRET_KEY   = os.environ.get("SECRET_KEY", BOT_TOKEN)
ADMIN_IDS    = {int(x) for x in os.environ.get("ADMIN_IDS", "").replace(" ", "").split(",") if x.isdigit()}
START_POINTS = int(os.environ.get("START_POINTS", "100"))

MIN_WAIT, MAX_WAIT = 15, 45        # تأخیر رندوم قبل از فعال شدن دکمه‌ی بررسی (ثانیه)
MIN_SLOTS, MAX_SLOTS = 5, 1000     # تعداد نفرات هر تسک
MIN_COMMENT_LEN = 4                # حداقل طول کامنت معتبر
LEAVE_DAYS = 3                     # تا چند روز بعد از فالو، ترک کانال جریمه داره
SOFT_FACTOR = 0.5                  # ضریب پاداش لایکِ غیرقابل‌سنجش

TYPES = {  # reward = پاداش انجام‌دهنده | cost = هزینه‌ی هر نفر برای ثبت‌کننده
    "follow":  {"emoji": "👥", "name": "فالو (عضویت)",   "reward": 3, "cost": 5},
    "like":    {"emoji": "❤️", "name": "لایک (ری‌اکشن)", "reward": 2, "cost": 3},
    "comment": {"emoji": "💬", "name": "کامنت",          "reward": 4, "cost": 6},
}

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("taskbot")

app = Flask(__name__)
if MONGO_URI:
    db = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000).get_database("taskbot")
else:
    import mongomock
    db = mongomock.MongoClient().get_database("taskbot")
    log.warning("MONGO_URI تنظیم نشده؛ از حافظه‌ی موقت استفاده می‌شه")

db.claims.create_index("tok", unique=True)
db.tasks.create_index([("active", 1), ("remaining", 1)])
db.tasks.create_index("owner")
db.completions.create_index("user")

BOT_ID = int(BOT_TOKEN.split(":")[0])
SECRET = hashlib.sha256(("hook" + SECRET_KEY).encode()).hexdigest()[:32]
CRON_SECRET = hashlib.sha256(("cron" + SECRET_KEY).encode()).hexdigest()[:24]

RE_CH = re.compile(r"^(?:https?://)?(?:t\.me/|@)?([A-Za-z][A-Za-z0-9_]{3,31})/?$")
RE_POST = re.compile(r"(?:https?://)?t\.me/([A-Za-z][A-Za-z0-9_]{3,31})/(\d+)")
_DIG = str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789")


# ───────────────────────── ابزارها ─────────────────────────
def tg(method, **data):
    try:
        r = requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/{method}", json=data, timeout=15)
        return r.json()
    except Exception as e:
        log.warning("tg %s failed: %s", method, e)
        return {"ok": False, "description": str(e)}


def now():
    return datetime.now(timezone.utc).replace(tzinfo=None)   # UTC بدون tzinfo (مثل چیزی که pymongo برمی‌گردونه)


def get_task(tid):
    try:
        return db.tasks.find_one({"_id": ObjectId(tid)})
    except (InvalidId, TypeError):
        return None


def get_user(u):
    return db.users.find_one_and_update(
        {"_id": u["id"]},
        {"$setOnInsert": {"points": START_POINTS, "created": now()},
         "$set": {"name": u.get("first_name", ""), "username": u.get("username")}},
        upsert=True, return_document=ReturnDocument.AFTER)


def reward_of(t):
    if t["type"] == "like" and not t.get("verified"):
        return max(1, int(t["reward"] * SOFT_FACTOR))
    return t["reward"]


MENU_ONLY = {"inline_keyboard": [[{"text": "🏠 منوی اصلی", "callback_data": "m"}]]}


def main_kb():
    return {"inline_keyboard": [
        [{"text": "📋 انجام تسک و کسب امتیاز", "callback_data": "t"}],
        [{"text": "➕ ثبت تسک جدید", "callback_data": "n"}, {"text": "📊 تسک‌های من", "callback_data": "my"}],
        [{"text": "💰 امتیاز من", "callback_data": "me"}]]}


def menu_text(points):
    return (f"🏠 منوی اصلی\n💰 امتیاز تو: {points}\n\n"
            "با انجام تسک‌ها (فالو، لایک، کامنت) امتیاز بگیر و با امتیازت تسک خودت رو ثبت کن.")


def show(chat_id, text, kb=None, edit=None):
    kb = kb or MENU_ONLY
    if edit:
        r = tg("editMessageText", chat_id=chat_id, message_id=edit["message_id"], text=text,
               reply_markup=kb, disable_web_page_preview=True)
        if r.get("ok") or "not modified" in str(r.get("description", "")):
            return
    tg("sendMessage", chat_id=chat_id, text=text, reply_markup=kb, disable_web_page_preview=True)


def say(chat_id, text):
    tg("sendMessage", chat_id=chat_id, text=text, disable_web_page_preview=True)


# ───────────────────────── تسک‌ها: نمایش و لینک ─────────────────────────
def target_url(t):
    if t["type"] == "follow":
        return f"https://t.me/{t['target']}"
    if t["type"] == "comment":
        return f"https://t.me/{t['target']}/{t['post']}?comment=1"
    if t.get("verified"):   # لایک دقیق: کپی پست داخل گروه گفتگو
        if t.get("g_user"):
            return f"https://t.me/{t['g_user']}/{t['gmsg']}"
        return f"https://t.me/c/{str(t['gchat'])[4:]}/{t['gmsg']}"
    return f"https://t.me/{t['target']}/{t['post']}"


def task_text(t):
    ty = TYPES[t["type"]]
    if t["type"] == "follow":
        how = ("۱) «🔗 باز کردن» رو بزن و توی کانال عضو شو.\n۲) برگرد و «بررسی» رو بزن.\n"
               f"⚠️ اگه تا {LEAVE_DAYS} روز بعد کانال رو ترک کنی، امتیازت کم می‌شه.")
    elif t["type"] == "comment":
        how = (f"۱) «🔗 باز کردن» رو بزن و زیر پست کامنت بذار (حداقل {MIN_COMMENT_LEN} حرف).\n"
               "۲) برگرد و «بررسی» رو بزن.")
    elif t.get("verified"):
        how = ("۱) «🔗 باز کردن» رو بزن؛ پست توی گروه گفتگو باز می‌شه.\n"
               "۲) روی همون پست ری‌اکشن بزن.\n۳) برگرد و «بررسی» رو بزن.")
    else:
        how = "۱) «🔗 باز کردن» رو بزن، پست رو ببین و لایک کن.\n۲) برگرد و «بررسی» رو بزن."
    return (f"{ty['emoji']} {ty['name']} — {t['title']}\n🎁 پاداش: {reward_of(t)} امتیاز\n\n{how}\n\n"
            "⏳ بعد از باز کردن لینک، دکمه‌ی بررسی نشون داده می‌شه و چند ثانیه صبر لازمه.")


def task_kb(t, claim):
    # مینی‌اپ داخل خود تلگرام باز می‌شه (بدون سوال و بدون مرورگر)، کلیک رو ثبت می‌کنه و یه‌راست کانال/پست رو باز می‌کنه
    rows = [[{"text": "🔗 باز کردن", "web_app": {"url": f"{BASE_URL}/go/{claim['tok']}"}}]]
    if claim.get("clicked"):
        rows.append([{"text": "✅ بررسی و دریافت پاداش", "callback_data": "c:" + str(t["_id"])}])
    rows.append([{"text": "🔙 بازگشت", "callback_data": "t"}])
    return {"inline_keyboard": rows}


def available_tasks(uid, limit=8):
    done = {c["task"] for c in db.completions.find({"user": uid}, {"task": 1})}
    out = []
    for t in db.tasks.find({"active": True, "remaining": {"$gt": 0}, "owner": {"$ne": uid}}).sort("created", 1).limit(300):
        if str(t["_id"]) not in done:
            out.append(t)
            if len(out) >= limit:
                break
    return out


def show_tasks(chat_id, uid, edit=None):
    ts = available_tasks(uid)
    if not ts:
        return show(chat_id, "فعلاً تسک جدیدی نیست 🙂 بعداً دوباره سر بزن.", edit=edit)
    rows = [[{"text": f"{TYPES[t['type']]['emoji']} {t['title'][:24]} · +{reward_of(t)}",
              "callback_data": "k:" + str(t["_id"])}] for t in ts]
    rows.append([{"text": "🏠 منوی اصلی", "callback_data": "m"}])
    show(chat_id, "📋 یکی از تسک‌ها رو انتخاب کن:", {"inline_keyboard": rows}, edit)


# ───────────────────────── کلیک ردیاب (مینی‌اپ) ─────────────────────────
def verify_init_data(init_data):
    try:
        pairs = dict(parse_qsl(init_data, keep_blank_values=True))
        got = pairs.pop("hash", None)
        if not got:
            return None
        check = "\n".join(f"{k}={v}" for k, v in sorted(pairs.items()))
        key = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        if not hmac.compare_digest(hmac.new(key, check.encode(), hashlib.sha256).hexdigest(), got):
            return None
        if time.time() - int(pairs.get("auth_date", 0)) > 86400:
            return None
        return json.loads(pairs["user"])
    except Exception:
        return None


GO_PAGE = """<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<style>body{margin:0;height:100vh;display:flex;flex-direction:column;align-items:center;justify-content:center;
gap:16px;font-family:sans-serif;background:var(--tg-theme-bg-color,#fff);color:var(--tg-theme-text-color,#000)}
button{display:none;padding:12px 22px;border:0;border-radius:10px;font-size:16px;
background:var(--tg-theme-button-color,#2481cc);color:var(--tg-theme-button-text-color,#fff)}</style></head>
<body><div id="m">⏳ در حال باز کردن...</div><button id="b">🔗 باز کردن</button><script>
const w = Telegram.WebApp; w.ready(); w.expand();
const m = document.getElementById("m"), b = document.getElementById("b");
function openIt(url) { try { w.openTelegramLink(url); } catch (e) { location.href = url; } }
fetch("/click/__TOK__", {method: "POST", headers: {"X-Init-Data": w.initData}})
  .then(r => r.json())
  .then(d => {
    if (!d.ok) throw 0;
    openIt(d.url);                       // بدون close فوری؛ بستنِ زودهنگام باعث می‌شد لینک باز نشه
    m.textContent = "✅ ثبت شد";
    setTimeout(() => { m.textContent = "اگه خودکار باز نشد، دکمه‌ی زیر رو بزن"; b.style.display = "block"; }, 1500);
    b.onclick = () => { openIt(d.url); setTimeout(() => w.close(), 600); };
  })
  .catch(() => { m.textContent = "خطا؛ ببند و دوباره از ربات امتحان کن"; });
</script></body></html>"""


@app.get("/go/<tok>")
def go(tok):
    return Response(GO_PAGE.replace("__TOK__", re.sub(r"[^A-Za-z0-9_-]", "", tok)), mimetype="text/html")


@app.post("/click/<tok>")
def click(tok):
    u = verify_init_data(request.headers.get("X-Init-Data", ""))
    c = db.claims.find_one({"tok": tok})
    t = get_task(c["task"]) if c else None
    if not u or not t or u["id"] != c["user"]:     # فقط خود کاربر؛ لینک فوروارد‌شده کار نمی‌کنه
        return jsonify(ok=False), 403
    if not c.get("clicked"):
        t0 = now()
        c2 = db.claims.find_one_and_update(
            {"_id": c["_id"], "clicked": {"$exists": False}},
            {"$set": {"clicked": t0, "ready": t0 + timedelta(seconds=random.randint(MIN_WAIT, MAX_WAIT))}},
            return_document=ReturnDocument.AFTER)
        if c2:   # اولین کلیک: دکمه‌ی بررسی توی ربات ظاهر می‌شه
            tg("editMessageReplyMarkup", chat_id=c2["chat"], message_id=c2["msg"], reply_markup=task_kb(t, c2))
    return jsonify(ok=True, url=target_url(t))


# ───────────────────────── سنجش و پاداش ─────────────────────────
def verify(t, uid):
    if t["type"] == "follow":
        r = tg("getChatMember", chat_id="@" + t["target"], user_id=uid)
        if not r.get("ok"):
            return False, "الان نمی‌تونم بررسی کنم، چند لحظه بعد دوباره بزن"
        m = r["result"]
        if m["status"] in ("member", "administrator", "creator") or (m["status"] == "restricted" and m.get("is_member")):
            return True, ""
        return False, "هنوز عضو کانال نشدی 🙂"
    key = f"{t.get('gchat')}:{t.get('gmsg')}:{uid}"
    if t["type"] == "comment":
        if db.comments.find_one({"_id": key}):
            return True, ""
        return False, f"کامنتی از تو زیر این پست پیدا نشد (حداقل {MIN_COMMENT_LEN} حرف بنویس) 💬"
    if t.get("verified"):
        if db.reacts.find_one({"_id": key}):
            return True, ""
        return False, "ری‌اکشنی از تو روی پست داخل گروه گفتگو پیدا نشد ❤️"
    return True, ""   # لایک نیمه‌مطمئن: فقط کلیک + زمان


def award(t, uid):
    """(points, reward) یا (None, پیام خطا)"""
    tid, reward = str(t["_id"]), reward_of(t)
    try:
        db.completions.insert_one({"_id": f"{tid}:{uid}", "task": tid, "user": uid, "type": t["type"],
                                   "target": t.get("target"), "reward": reward, "t": now()})
    except DuplicateKeyError:
        return None, "قبلاً پاداش این تسک رو گرفتی ✅"
    r = db.tasks.find_one_and_update({"_id": t["_id"], "active": True, "remaining": {"$gt": 0}},
                                     {"$inc": {"remaining": -1, "done": 1}}, return_document=ReturnDocument.AFTER)
    if not r:
        db.completions.delete_one({"_id": f"{tid}:{uid}"})
        return None, "ظرفیت این تسک تکمیل شد"
    if r["remaining"] == 0:
        db.tasks.update_one({"_id": r["_id"]}, {"$set": {"active": False, "finished": now()}})
        say(r["owner"], f"🎉 تسک «{r['title']}» کامل شد ({r['slots']} نفر).")
    u = db.users.find_one_and_update({"_id": uid}, {"$inc": {"points": reward}}, return_document=ReturnDocument.AFTER)
    return u["points"], reward


def do_check(cq, ans, chat_id, m, uid, tid):
    t = get_task(tid)
    if not t or not t.get("active"):
        return ans("این تسک دیگه فعال نیست", True)
    if db.completions.find_one({"_id": f"{tid}:{uid}"}):
        return ans("قبلاً پاداش این تسک رو گرفتی ✅", True)
    c = db.claims.find_one({"_id": f"{tid}:{uid}"})
    if not c or not c.get("clicked"):
        return ans("اول روی «🔗 باز کردن» بزن 🙂", True)
    left = (c["ready"] - now()).total_seconds()
    if left > 0:
        return ans(f"هنوز زوده ⏳ {int(left) + 1} ثانیه دیگه صبر کن", True)
    ok, why = verify(t, uid)
    if not ok:
        return ans(why, True)
    pts, info = award(t, uid)
    if pts is None:
        return ans(info, True)
    ans(f"✅ {info} امتیاز اضافه شد\n💰 موجودی: {pts}", True)
    show_tasks(chat_id, uid, edit=m)


# ───────────────────────── گروه گفتگو: کامنت و ری‌اکشن ─────────────────────────
def on_group(msg):
    gid = msg["chat"]["id"]
    if msg.get("is_automatic_forward"):   # کپی خودکار پست کانال داخل گروه
        fo = msg.get("forward_origin") or {}
        ch = fo.get("chat") or msg.get("forward_from_chat") or {}
        pid = fo.get("message_id") or msg.get("forward_from_message_id")
        if ch.get("username") and pid:
            db.threads.update_one({"_id": f"{gid}:{msg['message_id']}"}, {"$set": {
                "gchat": gid, "gmsg": msg["message_id"], "g_user": (msg["chat"].get("username") or "").lower(),
                "ch_user": ch["username"].lower(), "post": pid, "t": now()}}, upsert=True)
        return
    u = msg.get("from") or {}
    if not u or u.get("is_bot") or msg.get("sender_chat"):
        return
    root = msg.get("message_thread_id") or (msg.get("reply_to_message") or {}).get("message_id")
    text = (msg.get("text") or msg.get("caption") or "").strip()
    if not root or len(text) < MIN_COMMENT_LEN:
        return
    if db.threads.find_one({"_id": f"{gid}:{root}"}):
        db.comments.update_one({"_id": f"{gid}:{root}:{u['id']}"}, {"$setOnInsert": {"t": now()}}, upsert=True)


def on_reaction(r):
    user = r.get("user")
    if not user or user.get("is_bot"):
        return
    cid, mid = r["chat"]["id"], r["message_id"]
    if not db.threads.find_one({"_id": f"{cid}:{mid}"}):
        return
    key = f"{cid}:{mid}:{user['id']}"
    if r.get("new_reaction"):
        db.reacts.update_one({"_id": key}, {"$set": {"t": now()}}, upsert=True)
    else:
        db.reacts.delete_one({"_id": key})


# ───────────────────────── ثبت تسک ─────────────────────────
def check_channel(uid, username):
    r = tg("getChat", chat_id="@" + username)
    if not r.get("ok") or r["result"].get("type") != "channel":
        return None, "کانال پیدا نشد. فقط کانال‌های عمومی پشتیبانی می‌شن."
    ch = r["result"]
    b = tg("getChatMember", chat_id=ch["id"], user_id=BOT_ID)
    if not (b.get("ok") and b["result"]["status"] == "administrator"):
        return None, "اول ربات رو توی کانال ادمین کن، بعد دوباره بفرست."
    m = tg("getChatMember", chat_id=ch["id"], user_id=uid)
    if not (m.get("ok") and m["result"]["status"] in ("creator", "administrator")):
        return None, "فقط ادمین کانال می‌تونه براش تسک ثبت کنه."
    return ch, None


def handle_state(chat_id, user, st, text):
    uid = user["_id"]
    ty = st["type"]
    if st["step"] == "target":
        post = None
        if ty == "follow":
            mm = RE_CH.match(text)
            if not mm:
                return say(chat_id, "آیدی کانال معتبر نیست؛ مثل @mychannel بفرست (یا /cancel).")
        else:
            mm = RE_POST.search(text)
            if not mm:
                return say(chat_id, "لینک پست معتبر نیست؛ مثل https://t.me/mychannel/15 بفرست (یا /cancel).")
            post = int(mm.group(2))
        uname = mm.group(1).lower()
        ch, err = check_channel(uid, uname)
        if err:
            return say(chat_id, err)
        if db.tasks.find_one({"owner": uid, "type": ty, "target": uname, "post": post, "active": True}):
            return say(chat_id, "همین تسک رو الان فعال داری.")
        data = {"target": uname, "post": post, "title": ch.get("title") or uname, "verified": ty == "follow"}
        warn = ""
        if post:
            th = db.threads.find_one({"ch_user": uname, "post": post})
            if th:
                data.update(verified=True, gchat=th["gchat"], gmsg=th["gmsg"], g_user=th.get("g_user"))
            elif ty == "comment":
                return say(chat_id, "برای تسک کامنت، کانال باید گروه گفتگو داشته باشه و ربات باید قبل از "
                                    "انتشار پست داخل اون گروه ادمین بوده باشه (تا کپی پست رو ببینه). "
                                    "یه پست جدید بذار و لینکش رو بفرست.")
            else:
                warn = ("\n\n⚠️ این پست داخل گروه گفتگو ثبت نشده، پس لایکش دقیق سنجیده نمی‌شه "
                        f"و پاداش انجام‌دهنده نصفه. برای سنجش دقیق، ربات رو ادمین گروه گفتگو کن.")
        db.states.update_one({"_id": uid}, {"$set": {"step": "slots", "data": data}})
        cost = TYPES[ty]["cost"]
        return say(chat_id, f"چند نفر؟ ({MIN_SLOTS} تا {MAX_SLOTS})\nهزینه‌ی هر نفر: {cost} امتیاز\n"
                            f"💰 موجودی تو: {user['points']}{warn}")
    if st["step"] == "slots":
        try:
            n = int(text.translate(_DIG))
        except ValueError:
            return say(chat_id, "فقط یه عدد بفرست (یا /cancel).")
        if not MIN_SLOTS <= n <= MAX_SLOTS:
            return say(chat_id, f"تعداد باید بین {MIN_SLOTS} و {MAX_SLOTS} باشه.")
        spec, d = TYPES[ty], st["data"]
        cost = spec["cost"] * n
        r = db.users.find_one_and_update({"_id": uid, "points": {"$gte": cost}}, {"$inc": {"points": -cost}},
                                         return_document=ReturnDocument.AFTER)
        if not r:
            return say(chat_id, f"امتیازت کافی نیست. هزینه: {cost} | موجودی: {user['points']}\n"
                                "یه عدد کمتر بفرست یا اول چند تا تسک انجام بده (/cancel).")
        db.tasks.insert_one({"owner": uid, "type": ty, "target": d["target"], "post": d.get("post"),
                             "title": d["title"], "slots": n, "remaining": n, "done": 0,
                             "reward": spec["reward"], "cost": spec["cost"], "verified": d.get("verified", False),
                             "gchat": d.get("gchat"), "gmsg": d.get("gmsg"), "g_user": d.get("g_user"),
                             "active": True, "created": now()})
        db.states.delete_one({"_id": uid})
        show(chat_id, f"✅ تسک ثبت شد و {cost} امتیاز کم شد.\n💰 موجودی: {r['points']}", main_kb())


def my_tasks(chat_id, uid, edit=None):
    ts = list(db.tasks.find({"owner": uid}).sort("created", -1).limit(10))
    if not ts:
        return show(chat_id, "هنوز تسکی ثبت نکردی.", edit=edit)
    lines, rows = [], []
    for t in ts:
        st = "🟢" if t["active"] else "⚪️"
        lines.append(f"{st} {TYPES[t['type']]['emoji']} {t['title'][:20]} — {t['done']}/{t['slots']}")
        if t["active"]:
            rows.append([{"text": f"❌ لغو: {t['title'][:20]} (بازگشت {t['remaining'] * t['cost']})",
                          "callback_data": "x:" + str(t["_id"])}])
    rows.append([{"text": "🏠 منوی اصلی", "callback_data": "m"}])
    show(chat_id, "📊 تسک‌های من:\n\n" + "\n".join(lines), {"inline_keyboard": rows}, edit)


# ───────────────────────── هندلرها ─────────────────────────
def on_callback(cq):
    u, data, m = cq["from"], cq.get("data", ""), cq.get("message") or {}
    uid, chat_id = u["id"], (m.get("chat") or {}).get("id")

    def ans(text=None, alert=False):
        p = {"callback_query_id": cq["id"]}
        if text:
            p.update(text=text[:200], show_alert=alert)
        tg("answerCallbackQuery", **p)

    if not chat_id:
        return ans()
    user = get_user(u)
    if data == "m":
        db.states.delete_one({"_id": uid})
        ans()
        show(chat_id, menu_text(user["points"]), main_kb(), m)
    elif data == "me":
        ans()
        n = db.completions.count_documents({"user": uid})
        show(chat_id, f"💰 امتیاز تو: {user['points']}\n✅ تسک‌های انجام‌شده: {n}", edit=m)
    elif data == "t":
        ans()
        show_tasks(chat_id, uid, m)
    elif data.startswith("k:"):
        tid = data[2:]
        t = get_task(tid)
        if (not t or not t.get("active") or t["remaining"] <= 0 or t["owner"] == uid
                or db.completions.find_one({"_id": f"{tid}:{uid}"})):
            return ans("این تسک دیگه در دسترس نیست", True)
        ans()
        claim = db.claims.find_one_and_update(
            {"_id": f"{tid}:{uid}"},
            {"$set": {"chat": chat_id, "msg": m["message_id"]},
             "$setOnInsert": {"tok": secrets.token_urlsafe(9), "task": tid, "user": uid, "t": now()}},
            upsert=True, return_document=ReturnDocument.AFTER)
        show(chat_id, task_text(t), task_kb(t, claim), m)
    elif data.startswith("c:"):
        do_check(cq, ans, chat_id, m, uid, data[2:])
    elif data == "n":
        ans()
        rows = [[{"text": f"{v['emoji']} {v['name']} — {v['cost']} امتیاز/نفر", "callback_data": "nt:" + k}]
                for k, v in TYPES.items()]
        rows.append([{"text": "🏠 منوی اصلی", "callback_data": "m"}])
        show(chat_id, "نوع تسک رو انتخاب کن:", {"inline_keyboard": rows}, m)
    elif data.startswith("nt:") and data[3:] in TYPES:
        ty = data[3:]
        db.states.replace_one({"_id": uid}, {"_id": uid, "step": "target", "type": ty, "t": now()}, upsert=True)
        ans()
        if ty == "follow":
            msg = "آیدی کانال رو بفرست (مثل @mychannel).\nربات باید توی کانال ادمین باشه و تو هم ادمین کانال باشی."
        else:
            msg = ("لینک پست کانال رو بفرست (مثل https://t.me/mychannel/15).\n"
                   "ربات باید توی کانال ادمین باشه و تو هم ادمین کانال باشی.")
        show(chat_id, msg, edit=m)
    elif data == "my":
        ans()
        my_tasks(chat_id, uid, m)
    elif data.startswith("x:"):
        t = db.tasks.find_one_and_update({"_id": ObjectId(data[2:]), "owner": uid, "active": True},
                                         {"$set": {"active": False, "finished": now()}},
                                         return_document=ReturnDocument.BEFORE) if ObjectId.is_valid(data[2:]) else None
        if not t:
            return ans("این تسک فعال نیست", True)
        refund = t["remaining"] * t["cost"]
        db.users.update_one({"_id": uid}, {"$inc": {"points": refund}})
        ans(f"لغو شد؛ {refund} امتیاز برگشت 💰", True)
        my_tasks(chat_id, uid, m)
    else:
        ans()


def on_private(msg):
    u, chat_id = msg.get("from") or {}, msg["chat"]["id"]
    if not u:
        return
    uid, text = u["id"], (msg.get("text") or "").strip()
    user = get_user(u)
    if text.startswith("/"):
        cmd, _, arg = text[1:].partition(" ")
        cmd = cmd.split("@")[0].lower()
        if cmd in ("start", "cancel"):
            db.states.delete_one({"_id": uid})
            show(chat_id, menu_text(user["points"]), main_kb())
        elif uid in ADMIN_IDS and cmd == "add":
            try:
                target, n = arg.split()
                r = db.users.update_one({"_id": int(target)}, {"$inc": {"points": int(n)}})
                say(chat_id, "✅ انجام شد" if r.matched_count else "کاربر پیدا نشد")
            except ValueError:
                say(chat_id, "فرمت: /add USER_ID POINTS")
        elif uid in ADMIN_IDS and cmd == "stats":
            say(chat_id, f"👤 کاربران: {db.users.count_documents({})}\n"
                         f"📋 تسک فعال: {db.tasks.count_documents({'active': True})}\n"
                         f"✅ تسک انجام‌شده: {db.completions.count_documents({})}")
        return
    st = db.states.find_one({"_id": uid})
    if st and text:
        handle_state(chat_id, user, st, text)
    else:
        show(chat_id, menu_text(user["points"]), main_kb())


@app.post("/hook")
def hook():
    if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != SECRET:
        return "forbidden", 403
    upd = request.get_json(silent=True) or {}
    try:
        if "callback_query" in upd:
            on_callback(upd["callback_query"])
        elif "message_reaction" in upd:
            on_reaction(upd["message_reaction"])
        elif "message" in upd:
            msg = upd["message"]
            if msg["chat"]["type"] == "private":
                on_private(msg)
            elif msg["chat"]["type"] in ("group", "supergroup"):
                on_group(msg)
    except Exception:
        log.exception("hook error")
    return "ok"


# ───────────────────────── کرون: جریمه‌ی ترک کانال ─────────────────────────
@app.get("/cron/<s>")
def cron(s):
    if not hmac.compare_digest(s, CRON_SECRET):
        abort(404)
    since, n = now() - timedelta(days=LEAVE_DAYS), 0
    for c in db.completions.find({"type": "follow", "left": {"$exists": False}, "t": {"$gte": since}}).limit(300):
        r = tg("getChatMember", chat_id="@" + c["target"], user_id=c["user"])
        if r.get("ok") and r["result"]["status"] in ("left", "kicked"):
            db.completions.update_one({"_id": c["_id"]}, {"$set": {"left": True}})
            db.users.update_one({"_id": c["user"]}, {"$inc": {"points": -c["reward"] * 2}})
            say(c["user"], f"⚠️ کانال @{c['target']} رو ترک کردی و {c['reward'] * 2} امتیاز جریمه شدی.")
            n += 1
    return f"ok {n}"


def setup():
    r = tg("setWebhook", url=f"{BASE_URL}/hook", secret_token=SECRET,
           allowed_updates=["message", "callback_query", "message_reaction"])
    tg("setMyCommands", commands=[{"command": "start", "description": "منوی اصلی"},
                                  {"command": "cancel", "description": "لغو"}])
    log.info("webhook: %s", r)
    log.info("cron url: %s/cron/%s", BASE_URL, CRON_SECRET)


setup()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
