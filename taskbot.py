# -*- coding: utf-8 -*-
"""
TaskPlus — ربات دوزبانه‌ی تبادل تسک تلگرام (عضویت / واکنش / بازدید)
TaskPlus — bilingual (English / Persian) Telegram task-exchange bot

Stack : Flask + MongoDB (pymongo) + Webhook — تک‌فایل و بدون مینی‌اپ
Run   : gunicorn taskbot:app
Deps  : flask pymongo requests gunicorn  (mongomock فقط برای اجرای آزمایشی بدون MongoDB)

مدل کار / How it works
- کاربر با انجام تسک «کوین» می‌گیرد و با کوین، کمپین خودش را ثبت می‌کند (هزینه‌ی ثبت > پاداش؛ اقتصاد تورم نمی‌کند).
- عضویت  : getChatMember (ربات باید ادمین مقصد باشد) + بازبینی در پایان دوره‌ی معلق (خروج = برگشت کوین + اخطار)
- واکنش  : گروه → تأیید فردی با message_reaction | کانال → تأیید تجمیعی با message_reaction_count
           (هر واحد افزایش شمارش حداکثر یک ادعا را تأیید می‌کند؛ هویت واکنش‌دهنده اثبات نمی‌شود)
- بازدید  : فقط تأیید زمانی (با Bot API قابل راستی‌آزمایی نیست) → پاداش کم
- دعوت    : پاداش دومرحله‌ای (ورود دوست + فعال‌شدن دوست) با لینک start=ref_<uid>

زبان / Language
- هر کاربر زبان خودش را انتخاب می‌کند (English / فارسی)؛ پیش‌فرض: انگلیسی.
- پست‌های عمومی ویترین همیشه دوزبانه‌اند: ابتدا انگلیسی، سپس فارسی.
- اعداد در همه‌ی متن‌ها انگلیسی (0-9) هستند.

متغیرهای محیطی / Environment
BOT_TOKEN, BASE_URL, MONGO_URI, SECRET_KEY, ADMIN_IDS, TASKS_CHANNEL, TASKS_CHANNEL_URL (BotFather /setcustomurls may be needed for direct member redirect)
START_POINTS, HOLD_HOURS, DAILY_TASKS, STRIKE_LIMIT, REF_JOIN_BONUS, REF_ACTIVE_BONUS,
REF_ACTIVE_TASKS, REF_DAILY_CAP, MAX_QTY, MAX_ACTIVE, REACTION_UPDATE_GRACE,
REACTION_BASELINE_MAX_AGE, ADMIN_MONITOR_INTERVAL

Growth engine (v52) — optional env / متغیرهای اختیاری رشد:
DAILY_REWARDS="1,1,2,2,3,3,5"  WEEKLY_PRIZES="30,20,10"  REF_WELCOME_BONUS=5  REF_HOURLY_MAX=12
GOLDEN_HOUR="17-19" (UTC, خالی = غیرفعال)  GOLDEN_MULT=2  REMINDERS=1  REMIND_HOURS="5-18" (UTC)  DAILY_REPORT_HOUR=17
"""
import os, re, time, html, hashlib, random, logging, threading

import requests
from urllib.parse import quote
from flask import Flask, request
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from bson import ObjectId
from bson.errors import InvalidId

# ═════════════════════════════ Configuration ═════════════════════════════
BOT_TOKEN = os.environ["BOT_TOKEN"]
BASE_URL = os.environ["BASE_URL"].rstrip("/")                      # e.g. https://yourapp.onrender.com
MONGO_URI = os.environ.get("MONGO_URI", "").strip()                # empty = in-memory (not persisted)
SECRET_KEY = os.environ.get("SECRET_KEY", BOT_TOKEN)
ADMINS = {int(x) for x in re.findall(r"\d+", os.environ.get("ADMIN_IDS", ""))}
TASKS_CHANNEL = os.environ.get("TASKS_CHANNEL", "").strip()        # e.g. @MyTasksChannel (bot must be admin)
TASKS_CHANNEL_URL = os.environ.get("TASKS_CHANNEL_URL", "").strip()

START_POINTS = int(os.environ.get("START_POINTS", "20"))
HOLD_HOURS = float(os.environ.get("HOLD_HOURS", "24"))
DAILY_TASKS = int(os.environ.get("DAILY_TASKS", "60"))
STRIKE_LIMIT = int(os.environ.get("STRIKE_LIMIT", "3"))
REF_JOIN_BONUS = int(os.environ.get("REF_JOIN_BONUS", "2"))
REF_ACTIVE_BONUS = int(os.environ.get("REF_ACTIVE_BONUS", "8"))
REF_ACTIVE_TASKS = int(os.environ.get("REF_ACTIVE_TASKS", "3"))
REF_DAILY_CAP = int(os.environ.get("REF_DAILY_CAP", "20"))
MAX_QTY = int(os.environ.get("MAX_QTY", "500"))
MAX_ACTIVE = int(os.environ.get("MAX_ACTIVE", "10"))
REACTION_UPDATE_GRACE = int(os.environ.get("REACTION_UPDATE_GRACE", "180"))
REACTION_BASELINE_MAX_AGE = int(os.environ.get("REACTION_BASELINE_MAX_AGE", "900"))
ADMIN_MONITOR_INTERVAL = int(os.environ.get("ADMIN_MONITOR_INTERVAL", "60"))
VERSION = "v52-growth-engine"

# reward for the doer / cost per unit for the campaign owner / minimum wait (seconds)
TYPES = {
    "join":  {"reward": 3, "cost": 4, "wait": 0},
    "react": {"reward": 2, "cost": 3, "wait": 10},
    "view":  {"reward": 1, "cost": 2, "wait": 5},
}

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("taskbot")

app = Flask(__name__)
if MONGO_URI:
    db = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000).get_database("taskbot")
else:
    import mongomock
    db = mongomock.MongoClient().get_database("taskbot")
    log.warning("MONGO_URI is not set: using volatile in-memory storage (all data is lost on restart)")
db.completions.create_index([("uid", 1), ("cid", 1)], unique=True)     # one completion per user per campaign
db.completions.create_index([("status", 1), ("due", 1)])
db.completions.create_index([("status", 1), ("claim_expires", 1)])
db.campaigns.create_index([("status", 1), ("remaining", 1)])
db.campaigns.create_index("owner")
db.users.create_index("referred_by")
db.reaction_watch.create_index([("chat_id", 1), ("mid", 1)])
db.reaction_credits.create_index([("post_key", 1), ("event_at", 1)])
db.user_events.create_index([("uid", 1), ("t", -1)])
db.user_events.create_index([("t", -1)])

HOOK_SECRET = hashlib.sha256(("hook" + SECRET_KEY).encode()).hexdigest()[:32]
_me = {}
_last_admin_check = 0.0


# ═════════════════════════════ Telegram API helpers ═════════════════════════════
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


def B(text, data, style=None):
    """Callback button. style: primary | success | danger | None (neutral)."""
    out = {"text": str(text), "callback_data": data}
    if style in ("primary", "success", "danger"):
        out["style"] = style
    return out


def U(text, url, style=None):
    """URL button."""
    out = {"text": str(text), "url": url}
    if style in ("primary", "success", "danger"):
        out["style"] = style
    return out


def send(chat, text, kb=None):
    p = {"chat_id": chat, "text": str(text), "parse_mode": "HTML", "disable_web_page_preview": True}
    if kb:
        p["reply_markup"] = {"inline_keyboard": kb}
    return tg("sendMessage", **p)


def view(chat, mid, text, kb=None):
    """Edit the previous message in place (keeps menus tidy); fall back to a new message."""
    if mid:
        r = tg("editMessageText", chat_id=chat, message_id=mid, text=str(text), parse_mode="HTML",
               disable_web_page_preview=True, reply_markup={"inline_keyboard": kb or []})
        if r.get("ok") or "not modified" in str(r.get("description", "")).lower():
            return r
    return send(chat, text, kb)


def ans(cq, text=None, alert=False, url=None):
    p = {"callback_query_id": cq["id"]}
    if text:
        p.update(text=str(text)[:200], show_alert=alert)
    if url:
        p["url"] = url
    return tg("answerCallbackQuery", **p)


def member_ok(r):
    return r.get("status") in ("creator", "administrator", "member") or (r.get("status") == "restricted" and r.get("is_member"))


def bot_admin_status(chat_id):
    """True/False for a valid Telegram answer; None for a temporary API error."""
    r = tg("getChatMember", chat_id=chat_id, user_id=bot_id())
    if not r.get("ok") or not isinstance(r.get("result"), dict):
        return None
    return r["result"].get("status") in ("administrator", "creator")


def bot_is_admin(chat_id):
    return bot_admin_status(chat_id) is True


def resolve_chat(uname):
    r = tg("getChat", chat_id="@" + uname)
    if r.get("ok") and r["result"].get("type") in ("channel", "group", "supergroup"):
        return r["result"]
    return None


# ═════════════════════════════ i18n engine ═════════════════════════════
RLM = "\u200f"          # right-to-left mark: keeps Persian lines RTL even when they contain English words/digits
HR = "─" * 14


class H(str):
    """Trusted HTML fragment (never escaped again)."""


class K(str):
    """Reference to a plain-text translation key, resolved in the recipient's language."""


def _rtl(text):
    return "\n".join(ln if (not ln.strip() or ln.startswith(RLM)) else RLM + ln for ln in text.split("\n"))


def _arg(lang, v):
    if callable(v):
        v = v(lang)
    if isinstance(v, K):
        return tb(lang, str(v))
    if isinstance(v, H) or not isinstance(v, str):
        return v
    return html.escape(v, quote=False)


def t(lang, key, **kw):
    """Translate an HTML message. Dynamic strings are escaped automatically; wrap trusted HTML in H()."""
    pair = S[key]
    s = pair[1] if lang == "fa" else pair[0]
    if kw:
        s = s.format(**{k: _arg(lang, v) for k, v in kw.items()})
    return H(_rtl(s) if lang == "fa" else s)


def tb(lang, key, **kw):
    """Translate plain text (button labels, callback alerts)."""
    pair = S[key]
    s = pair[1] if lang == "fa" else pair[0]
    return s.format(**kw) if kw else s


def bi_btn(key):
    """Public (channel) button label: English first, then Persian."""
    return f"{tb('en', key)} · {tb('fa', key)}"


def tn(lang, typ):
    return tb(lang, "tn_" + typ)


def lang_of(u):
    return (u or {}).get("lang") or (u or {}).get("lang_hint") or "en"


def ulang(uid):
    return lang_of(db.users.find_one({"_id": uid}, {"lang": 1, "lang_hint": 1}))


def notify(uid, key, kb=None, **kw):
    """Send a localized notification to a user (language resolved from their profile)."""
    lang = ulang(uid)
    if callable(kb):
        kb = kb(lang)
    return send(uid, t(lang, key, **kw), kb)


def short(s, n=30):
    return str(s or "").strip()[:n]


# ═════════════════════════════ Strings (English, فارسی) ═════════════════════════════
# Rules: English digits everywhere · emoji only where useful · HTML only for structure · Persian is formal and uses half-spaces.
S = {
    # ───────── Buttons (plain text) ─────────
    "b_menu": ("🏠 Main menu", "🏠 منوی اصلی"),
    "b_back": ("↩️ Back", "↩️ بازگشت"),
    "b_cancel": ("Cancel", "⭕️ انصراف"),
    "b_earn": ("⭐ Earn Stars", "⭐ کسب استارز"),
    "b_new": ("➕ Create campaign", "➕ ایجاد کمپین"),
    "b_my": ("📣 Grow my channel", "📣 رشد کانال من"),
    "b_mine": ("📋 My campaigns", "📋 کمپین‌های من"),
    "b_ref": ("🎁 Compound Rewards+", "🎁 پاداش مرکب+"),
    "b_acc": ("👤 Account", "👤 حساب کاربری"),
    "b_help": ("📘 Help", "📘 راهنما"),
    "b_lang": ("🌐 Language", "🌐 زبان"),
    "b_refresh": ("🔄 Refresh", "🔄 به‌روزرسانی"),
    "b_showcase": ("⭐ Open tasks", "⭐ ورود به تسک‌ها"),
    "b_view_showcase": ("View in showcase", "مشاهده در ویترین"),
    "b_back_showcase": ("Back to showcase", "بازگشت به ویترین"),
    "b_share": ("Share link", "اشتراک‌گذاری لینک"),
    "b_done": ("I have completed it", "انجام دادم"),
    "b_back_target": ("Back to destination", "بازگشت به مقصد"),
    "b_target": ("🎯 Target", "مقصد"),
    "b_claim": ("🎁 Claim", "پاداش"),
    "b_confirm": ("Confirm and pay {cost} Stars", "تأیید و پرداخت {cost} استارز"),
    "b_yes_cancel": ("Yes, cancel campaign", "بله، کمپین لغو شود"),
    "b_cancel_n": ("Cancel #{n}", "لغو شماره {n}"),
    "b_next": ("Next task", "تسک بعدی"),
    "b_watch": ("Reaction monitor", "پایش واکنش‌ها"),
    "b_win2": ("2 minutes", "2 دقیقه"),
    "b_win5": ("5 minutes", "5 دقیقه"),
    "b_cancel_campaign": ("Cancel campaign", "لغو کمپین"),
    "b_full": ("Capacity reached", "ظرفیت تکمیل شد 🛑"),
    "b_legacy": ("Retired verification method", "روش تأیید قدیمی غیرفعال"),
    "type_btn": ("{name} — {cost} Stars", "{name} — {cost} استارز"),

    # ───────── Plain labels ─────────
    "tn_join": ("👥 Join", "👥 عضویت"),
    "tn_react": ("❤️‍🔥 Reaction", "❤️‍🔥 واکنش"),
    "tn_view": ("👁️ View", "👁️ بازدید"),
    "st_active": ("Active", "فعال"),
    "st_done": ("Completed", "تکمیل‌شده"),
    "st_cancelled": ("Cancelled", "لغو شده"),
    "st_waiting": ("Awaiting update", "در انتظار به‌روزرسانی"),
    "st_full": ("Capacity reached", "ظرفیت تکمیل شد 🛑"),
    "st_legacy": ("Inactive (retired verification)", "غیرفعال (روش تأیید قدیمی)"),
    "st_admin_paused": ("Paused (bot access)", "متوقف موقت (دسترسی ربات)"),
    "friend": ("friend", "کاربر"),
    "unknown": ("unknown", "نامشخص"),
    "lbl_custom": ("Custom emoji", "ایموجی اختصاصی"),
    "lbl_paid": ("Paid reaction", "واکنش پولی"),
    "user_fallback": ("A user", "یک کاربر"),
    "campaign_fallback": ("Campaign", "کمپین"),
    "share_text": ("🎁 Free Stars every day! Complete easy tasks, collect daily gifts and grow your channel on autopilot with TaskPlus.",
                   "🎁 هر روز استارز رایگان! با انجام تسک‌های ساده و هدیه‌ی روزانه، کانال خود را در تاسک‌پلاس به‌صورت خودکار رشد دهید."),

    # ───────── Failure reasons (used inside notifications) ─────────
    "r_stopped": ("The campaign was stopped before verification and the spot was returned.",
                  "کمپین پیش از تأیید متوقف شد و ظرفیت آن بازگردانده شد."),
    "r_paused": ("The campaign has been stopped.", "کمپین متوقف شده است."),
    "r_no_increase": ("No verifiable increase in the count was received within the time limit.",
                      "در مهلت تعیین‌شده، افزایش قابل‌تأییدی در شمارش دریافت نشد."),
    "r_left": ("You left the channel or removed your reaction, so the Stars for this task were reversed.",
               "از کانال خارج شده‌اید یا واکنش خود را برداشته‌اید؛ استارز این تسک برگشت داده شد."),

    # ───────── Language picker is built separately (see LANG_PICK) ─────────

    # ───────── Core screens ─────────
    "menu": (
        "<b>Grameeting👁‍🗨</b>\n<i>Task exchange for Telegram channels and groups</i>\n"
        "<blockquote><b>Balance:</b> <code>{points}</code> Stars⭐{hold}</blockquote>\n"
        "Welcome, {name}. Complete tasks to earn Stars, then spend them on campaigns that grow your own channel.",
        "<b>Grameeting👁‍🗨</b>\nپلتفرم نوین عضوگیری، ریکشن پست و ویو سریع؛ حرفه‌ای و رایگان برای تمامی کانال‌ها و گروه‌های تلگرام\n"
        "<blockquote><b>موجودی:</b> <code>{points}</code> استارز⭐{hold}</blockquote>\n"
        "{name} گرامی، خوش آمدید. با انجام تسک و روش های مختلف میتوانید استارز رایگان کسب کنید و از آن برای رشد کانال و یا گروه و ربات و.. خود استفاده نمایید."),
    "hold_line": ("\n<b>Pending:</b> <code>{hold}</code> Stars", "\n<b>در انتظار تأیید:</b> <code>{hold}</code> استارز"),

    "help": (
        "<b>Help</b>\n<i>How TaskPlus works</i>\n\n"
        "<b>⭐ Earn Stars</b>\n"
        "<blockquote>Open a task in the showcase channel, complete it, tap <b>Claim reward</b> under the post and confirm here.\n\n"
        "<b>Join</b> — {r_join} Stars. Membership is verified and paid instantly.\n"
        "<b>Reaction</b> — {r_react} Stars. Verified per user in groups and by count increase in channels.\n"
        "<b>View</b> — {r_view} Stars. Paid after the required waiting time.</blockquote>\n"
        "<b>Create a campaign</b>\n"
        "<blockquote>Choose a task type, then send the destination link and the quantity. The cost is shown before you confirm. "
        "For join and group-reaction tasks the bot must be an administrator of the destination.</blockquote>\n"
        "<b>Invite friends</b>\n"
        "<blockquote>Earn {ref_join} Stars when a friend joins, and {ref_active} more once they become active "
        "({ref_tasks} verified join or reaction tasks).</blockquote>\n"
        "<b>Daily rewards</b>\n"
        "<blockquote>🎁 Collect the daily gift every day — streaks pay more. 🎰 Finish one task to unlock a free lucky-wheel spin. "
        "🏆 Top earners and top inviters win weekly prizes, and levels unlock bigger gifts and higher daily limits.</blockquote>\n"
        "<b>Rules</b>\n"
        "<blockquote>Each campaign counts once per person. Leaving a channel or removing a reaction during the "
        "{hold_h}-hour review period reverses the reward and adds one warning. After {strike_limit} warnings, "
        "task completion is disabled.</blockquote>\n"
        "<blockquote expandable><b>About channel reactions</b>\n"
        "Channel reactions are verified by the increase in the post's total reaction count. This measures how many "
        "reactions were added, not who added them. Telegram may deliver count updates a few minutes late.</blockquote>",
        "<b>راهنما</b>\n<i>نحوه‌ی کار تاسک‌پلاس</i>\n\n"
        "<b>⭐ کسب استارز</b>\n"
        "<blockquote>تسک را در کانال ویترین انتخاب و انجام دهید، سپس زیر همان پست روی «دریافت پاداش» بزنید و اینجا تأیید کنید.\n\n"
        "<b>عضویت</b> — {r_join} استارز. عضویت بررسی و پاداش بلافاصله واریز می‌شود.\n"
        "<b>واکنش</b> — {r_react} استارز. در گروه به‌صورت فردی و در کانال بر اساس افزایش شمارش تأیید می‌شود.\n"
        "<b>بازدید</b> — {r_view} استارز. پس از رعایت زمان انتظار پرداخت می‌شود.</blockquote>\n"
        "<b>ایجاد کمپین</b>\n"
        "<blockquote>نوع تسک را انتخاب کنید و سپس لینک مقصد و تعداد را ارسال نمایید. هزینه پیش از تأیید نمایش داده می‌شود. "
        "برای تسک‌های عضویت و واکنش در گروه، ربات باید در مقصد ادمین باشد.</blockquote>\n"
        "<b>دعوت از دوستان</b>\n"
        "<blockquote>با ورود هر دوست {ref_join} استارز و پس از فعال شدن او ({ref_tasks} تسک عضویت یا واکنش تأییدشده) "
        "{ref_active} استارز دیگر دریافت می‌کنید.</blockquote>\n"
        "<b>پاداش‌های روزانه</b>\n"
        "<blockquote>🎁 هر روز هدیه‌ی روزانه را بگیرید؛ استریک بلندتر، پاداش بیشتر. 🎰 با انجام یک تسک، چرخش رایگان گردونه‌ی شانس باز می‌شود. "
        "🏆 برترین‌های هفته در درآمد و دعوت جایزه می‌گیرند و سطح‌های بالاتر هدیه‌ی بزرگ‌تر و سقف تسک بیشتری دارند.</blockquote>\n"
        "<b>قوانین</b>\n"
        "<blockquote>هر کمپین برای هر نفر فقط یک بار محاسبه می‌شود. خروج از کانال یا حذف واکنش در دوره‌ی بررسی "
        "({hold_h} ساعت) موجب برگشت پاداش و ثبت یک اخطار می‌شود. پس از {strike_limit} اخطار، انجام تسک غیرفعال خواهد شد.</blockquote>\n"
        "<blockquote expandable><b>درباره‌ی واکنش در کانال</b>\n"
        "واکنش‌های کانال بر اساس افزایش شمارش کل واکنش‌های پست تأیید می‌شوند. این روش تعداد واکنش‌های افزوده‌شده را "
        "می‌سنجد، نه هویت واکنش‌دهنده را. ممکن است تلگرام به‌روزرسانی شمارش را با چند دقیقه تأخیر ارسال کند.</blockquote>"),

    "account": (
        "<b>Account</b>\n<i>Balance and activity overview</i>\n"
        "<blockquote><b>Balance:</b> <code>{points}</code> Stars{hold}\n"
        "<b>Total earned:</b> <code>{earned}</code> Stars</blockquote>\n"
        "<blockquote><b>Invited:</b> <code>{refs}</code> ({ref_active} active)\n"
        "<b>Active campaigns:</b> <code>{active}</code> / {max_active}\n"
        "<b>Warnings:</b> <code>{strikes}</code> / {strike_limit}</blockquote>",
        "<b>حساب کاربری</b>\n<i>خلاصه‌ی موجودی و فعالیت</i>\n"
        "<blockquote><b>موجودی:</b> <code>{points}</code> استارز{hold}\n"
        "<b>مجموع درآمد:</b> <code>{earned}</code> استارز</blockquote>\n"
        "<blockquote><b>دعوت‌شده‌ها:</b> <code>{refs}</code> ({ref_active} فعال)\n"
        "<b>کمپین‌های فعال:</b> <code>{active}</code> از {max_active}\n"
        "<b>اخطارها:</b> <code>{strikes}</code> از {strike_limit}</blockquote>"),

    "earn": (
        "<b>⭐ Earn Stars</b>\n<i>Tasks are published in the showcase channel</i>\n"
        "<blockquote><b>1.</b> Choose a task and complete it.\n"
        "<b>2.</b> Tap <b>Claim reward</b> under the same post.\n"
        "<b>3.</b> Confirm completion here.</blockquote>\n"
        "Rewards are paid after an automatic check.",
        "<b>⭐ کسب استارز</b>\n<i>تسک‌ها در کانال ویترین منتشر می‌شوند</i>\n"
        "<blockquote><b>1.</b> یک تسک را انتخاب و انجام دهید.\n"
        "<b>2.</b> زیر همان پست روی «دریافت پاداش» بزنید.\n"
        "<b>3.</b> انجام تسک را اینجا تأیید کنید.</blockquote>\n"
        "پاداش پس از بررسی خودکار واریز می‌شود."),
    "earn_unset": (
        "<b>Showcase unavailable</b>\n\nThe task channel has not been configured yet. Please try again later.",
        "<b>ویترین در دسترس نیست</b>\n\nکانال تسک‌ها هنوز تنظیم نشده است. لطفاً کمی بعد دوباره مراجعه کنید."),

    # ───────── Referral ─────────
    "ref": (
        "<b>Invite friends</b>\n<i>Earn twice for every friend</i>\n"
        "<blockquote><b>1.</b> A friend joins with your link — <code>{join}</code> Stars\n"
        "<b>2.</b> The friend becomes active ({tasks} join or reaction tasks) — <code>{active}</code> Stars</blockquote>\n"
        "Every active friend is worth <code>{total}</code> Stars in total.\n"
        "<blockquote><b>Invited:</b> <code>{count}</code>\n"
        "<b>Active:</b> <code>{active_count}</code>\n"
        "<b>Referral earnings:</b> <code>{earned}</code> Stars</blockquote>{friends}{capnote}\n\n"
        "<b>Your personal link</b>\n{link}",
        "<b>دعوت از دوستان</b>\n<i>برای هر دوست، دو بار پاداش بگیرید</i>\n"
        "<blockquote><b>1.</b> ورود دوست با لینک شما — <code>{join}</code> استارز\n"
        "<b>2.</b> فعال شدن دوست ({tasks} تسک عضویت یا واکنش) — <code>{active}</code> استارز</blockquote>\n"
        "هر دوست فعال در مجموع <code>{total}</code> استارز برای شما دارد.\n"
        "<blockquote><b>دعوت‌شده:</b> <code>{count}</code>\n"
        "<b>فعال:</b> <code>{active_count}</code>\n"
        "<b>درآمد دعوت:</b> <code>{earned}</code> استارز</blockquote>{friends}{capnote}\n\n"
        "<b>لینک اختصاصی شما</b>\n{link}"),
    "ref_friends": ("\n\n<b>Latest referrals</b>\n<blockquote>{rows}</blockquote>",
                    "\n\n<b>آخرین دعوت‌ها</b>\n<blockquote>{rows}</blockquote>"),
    "ref_row": ("• {name} — {state}", "• {name} — {state}"),
    "ref_state_active": ("Active", "فعال"),
    "ref_state_progress": ("{n}/{total} tasks", "{n} از {total} تسک"),
    "ref_cap": ("\n\n<i>Today's join-reward cap has been reached. It resets tomorrow (UTC).</i>",
                "\n\n<i>سقف پاداش ورود برای امروز تکمیل شده است و فردا (به وقت UTC) دوباره فعال می‌شود.</i>"),
    "ref_link_missing": ("<i>The referral link is temporarily unavailable. Please try again shortly.</i>",
                         "<i>لینک دعوت موقتاً در دسترس نیست. لطفاً کمی بعد دوباره تلاش کنید.</i>"),
    "n_ref_join": (
        "<b>New referral</b>\n<blockquote>{name} joined with your link.\n<b>Reward:</b> <code>+{bonus}</code> ⭐ Stars</blockquote>\n"
        "You will receive {active_bonus} more Stars once they complete {tasks} tasks.",
        "<b>دعوت جدید</b>\n<blockquote>{name} با لینک شما وارد ربات شد.\n<b>پاداش:</b> <code>+{bonus}</code> ⭐ استارز</blockquote>\n"
        "پس از انجام {tasks} تسک توسط او، {active_bonus} استارز دیگر دریافت خواهید کرد."),
    "n_ref_active": (
        "<b>Referral activated</b>\n<blockquote>{name} completed {tasks} tasks.\n<b>Reward:</b> <code>+{bonus}</code> ⭐ Stars</blockquote>",
        "<b>فعال‌سازی دعوت</b>\n<blockquote>{name} {tasks} تسک انجام داد.\n<b>پاداش:</b> <code>+{bonus}</code> ⭐ استارز</blockquote>"),

    # ───────── Claim flow ─────────
    "claim_page": (
        "<b>Claim reward</b>\n\nIf you have completed the task{reacted}, tap <b>I have completed it</b>. "
        "The result will be shown here after verification.",
        "<b>دریافت پاداش</b>\n\nاگر تسک را انجام داده‌اید{reacted}، روی «انجام دادم» بزنید. "
        "نتیجه پس از بررسی همین‌جا اعلام می‌شود."),
    "claim_reacted": (" and added the reaction", " و واکنش را ثبت کرده‌اید"),
    "task_paid": (
        "<b>Task verified ✅</b>\n<blockquote><b>Reward:</b> <code>+{reward}</code> ⭐ Stars</blockquote>\nThe Stars were added to your balance.",
        "<b>تسک تأیید شد ✅</b>\n<blockquote><b>پاداش:</b> <code>+{reward}</code> ⭐ استارز</blockquote>\nاستارز به موجودی شما افزوده شد."),
    "count_pending": (
        "<b>Request received</b>\n<blockquote>🎁 <b>Reward:</b> <code>{reward}</code> ⭐ Stars\n<b>Time limit:</b> {window} minutes</blockquote>\n"
        "Waiting for Telegram's count update, which may arrive up to {grace} minutes late. "
        "If a valid increase is recorded, the reward is paid automatically and you will be notified.",
        "<b>درخواست ثبت شد</b>\n<blockquote>🎁 <b>پاداش:</b> <code>{reward}</code> ⭐ استارز\n<b>مهلت:</b> {window} دقیقه</blockquote>\n"
        "در انتظار به‌روزرسانی شمارش تلگرام هستیم؛ ممکن است تا {grace} دقیقه دیرتر برسد. "
        "در صورت ثبت افزایش معتبر، پاداش به‌صورت خودکار واریز می‌شود و به شما اطلاع داده خواهد شد."),
    "count_ok": (
        "<b>Increase verified ✅</b>\n\nThe reaction increase was verified and your reward has been paid.",
        "<b>افزایش تأیید شد ✅</b>\n\nافزایش واکنش تأیید و پاداش شما پرداخت شد."),
    "n_paid": (
        "<b>Task verified ✅</b>\n<blockquote><b>Reward:</b> <code>+{reward}</code> ⭐ Stars</blockquote>",
        "<b>تسک تأیید شد ✅</b>\n<blockquote><b>پاداش:</b> <code>+{reward}</code> ⭐ استارز</blockquote>"),
    "n_claim_failed": (
        "<b>No reward</b>\n\n{reason} No reward was paid; you may try again.",
        "<b>بدون پاداش</b>\n\n{reason} پاداشی پرداخت نشد؛ می‌توانید دوباره تلاش کنید."),
    "n_reversed": (
        "<b>Reward reversed</b>\n\n{why}{strike}",
        "<b>برگشت پاداش</b>\n\n{why}{strike}"),
    "n_strike": ("\nOne warning was recorded ({limit} warnings disable task completion).",
                 "\nیک اخطار ثبت شد (پس از {limit} اخطار، انجام تسک غیرفعال می‌شود)."),

    # ───────── Linked-task errors ─────────
    "e_link_invalid": ("This task link is invalid or incomplete. Choose an active task from the showcase.",
                       "این لینک تسک نامعتبر یا ناقص است. یک تسک فعال را از ویترین انتخاب کنید."),
    "e_no_capacity": ("This task has no remaining capacity. Choose another task from the showcase.",
                      "این تسک دیگر ظرفیت ندارد. تسک دیگری را از ویترین انتخاب کنید."),
    "e_legacy": ("This campaign used a retired verification method and can no longer be completed. Please choose another task.",
                 "این کمپین از روش تأیید قدیمی استفاده می‌کرد و دیگر قابل انجام نیست. لطفاً تسک دیگری انتخاب کنید."),
    "e_paused": ("This campaign is temporarily paused while the bot's access is restored.",
                 "این کمپین موقتاً متوقف شده است؛ دسترسی ربات در حال بازیابی است."),
    "e_admin_unavail": ("Destination access cannot be checked right now. Please try again shortly.",
                        "بررسی دسترسی مقصد در حال حاضر ممکن نیست. لطفاً کمی بعد دوباره تلاش کنید."),
    "e_paused_now": ("This campaign is temporarily paused. Please try again later.",
                     "این کمپین موقتاً متوقف است. لطفاً بعداً دوباره تلاش کنید."),
    "e_own": ("This campaign belongs to you, so you cannot complete it.",
              "این کمپین متعلق به شماست و نمی‌توانید آن را انجام دهید."),
    "e_done_before": ("This task is already recorded for your account. Return to the showcase for more tasks.",
                      "این تسک قبلاً برای حساب شما ثبت شده است. برای تسک‌های دیگر به ویترین بازگردید."),
    "e_strikes": ("Task completion is currently disabled for your account because the warning limit was reached.",
                  "انجام تسک برای حساب شما غیرفعال است، زیرا تعداد اخطارها به حد مجاز رسیده است."),
    "e_count_stale": ("The reaction count for this post is not fresh enough, so the task is temporarily unavailable. "
                      "Please try again from the showcase in a few minutes.",
                      "شمارش واکنش‌های این پست به‌روز نیست و تسک موقتاً در دسترس نیست. "
                      "لطفاً چند دقیقه بعد از ویترین دوباره تلاش کنید."),
    "e_expired": ("This step has expired. Start again from the menu.",
                  "این مرحله منقضی شده است. از منو دوباره شروع کنید."),
    "e_restart": ("Start again from the menu.", "از منو دوباره شروع کنید."),
    "e_continue_menu": ("Please continue from the menu.", "لطفاً از منو ادامه دهید."),
    "e_answer_prev": ("Please reply according to the previous step.", "لطفاً مطابق مرحله‌ی قبل پاسخ دهید."),
    "e_unknown_cmd": ("Unknown command. Use the menu below.", "دستور ناشناخته است. از منوی زیر استفاده کنید."),

    # ───────── Callback alerts (plain) ─────────
    "a_invalid": ("Invalid request.", "درخواست نامعتبر است."),
    "a_no_capacity": ("This task no longer has capacity.", "این تسک دیگر ظرفیت ندارد."),
    "a_legacy": ("The retired verification method is no longer supported.",
                 "روش تأیید قدیمی دیگر پشتیبانی نمی‌شود."),
    "a_paused": ("This campaign is paused while bot access is restored.",
                 "این کمپین تا بازیابی دسترسی ربات متوقف است."),
    "a_admin_unavail": ("Destination access cannot be checked now. Try again shortly.",
                        "بررسی دسترسی مقصد اکنون ممکن نیست. کمی بعد تلاش کنید."),
    "a_paused_now": ("Campaign paused: bot access was lost. Try again later.",
                     "کمپین متوقف شد: دسترسی ربات از دست رفته است. بعداً تلاش کنید."),
    "a_cant": ("You cannot complete this task.", "امکان انجام این تسک برای شما وجود ندارد."),
    "a_dup": ("You have already completed this task.", "این تسک را قبلاً انجام داده‌اید."),
    "a_open_first": ("Open the task from the showcase first.", "ابتدا تسک را از ویترین باز کنید."),
    "a_wait": ("Please wait {s} more seconds, then try again.", "لطفاً {s} ثانیه دیگر صبر کنید و دوباره تلاش کنید."),
    "a_check_unavail": ("Verification is unavailable right now. Try another task.",
                        "بررسی در حال حاضر ممکن نیست. تسک دیگری را امتحان کنید."),
    "a_not_member": ("Membership not detected. Join first, then try again.",
                     "عضویت شما شناسایی نشد. ابتدا عضو شوید، سپس دوباره تلاش کنید."),
    "a_no_react": ("No reaction from you was detected. React to the post, wait a few seconds and try again.",
                   "واکنشی از شما ثبت نشده است. روی پست واکنش بزنید، چند ثانیه صبر کنید و دوباره تلاش کنید."),
    "a_window_over": ("The {window}-minute window has ended. Start again from the channel.",
                      "مهلت {window} دقیقه‌ای این تسک پایان یافته است. از کانال دوباره شروع کنید."),
    "a_no_baseline": ("Baseline count unavailable. Reopen the task.", "شمارش پایه در دسترس نیست. تسک را دوباره باز کنید."),
    "a_count_pending": ("Awaiting count verification.", "در انتظار تأیید شمارش."),
    "a_paid": ("Verified ✅ Reward paid.", "تأیید شد ✅ پاداش واریز شد."),
    "a_limit": ("Daily limit of {n} tasks reached.", "سقف {n} تسک در روز تکمیل شده است."),
    "a_full": ("This task was just completed by others. Try the next one.", "ظرفیت این تسک همین الان تکمیل شد. تسک بعدی را امتحان کنید."),
    "a_task_id_bad": ("This task ID is invalid. Use a fresh post from the channel.",
                      "شناسه‌ی این تسک نامعتبر است. از یک پست تازه‌ی کانال استفاده کنید."),
    "a_inactive": ("This task is no longer active.", "این تسک دیگر فعال نیست."),
    "a_no_deeplink": ("The reward page link is unavailable.", "لینک صفحه‌ی دریافت پاداش در دسترس نیست."),
    "a_wait_first": ("To register the view, wait 5 seconds and make sure you have seen the task, then tap Claim reward again.",
                     "برای ثبت بازدید، 5 ثانیه صبر کنید و از دیدن تسک مطمئن شوید، سپس دوباره روی «دریافت پاداش» بزنید."),
    "a_wait_claim": ("Wait {s} more seconds, then tap Claim reward again.",
                     "{s} ثانیه دیگر صبر کنید و دوباره روی «دریافت پاداش» بزنید."),
    "a_step_expired": ("This step has expired. Start the campaign again.",
                       "این مرحله منقضی شده است. ثبت کمپین را دوباره شروع کنید."),
    "a_window_bad": ("Invalid time limit.", "مهلت نامعتبر است."),
    "a_window_only": ("Only 2 or 5 minutes can be selected.", "فقط 2 یا 5 دقیقه قابل انتخاب است."),
    "a_count_stale": ("The count is outdated. Add or remove a reaction to refresh it.",
                      "شمارش قدیمی است. برای به‌روزرسانی، یک واکنش اضافه یا حذف کنید."),
    "a_window_set": ("Time limit set to {window} minutes.", "مهلت روی {window} دقیقه تنظیم شد."),
    "a_max_active": ("You can have at most {n} active campaigns.", "حداکثر {n} کمپین فعال می‌توانید داشته باشید."),
    "a_no_funds": ("Insufficient balance.", "موجودی کافی نیست."),
    "a_admin_check_pre": ("The destination check is temporarily unavailable. No Stars were charged. Try again.",
                          "بررسی ادمین مقصد موقتاً ممکن نیست و استارزی کسر نشد. دوباره تلاش کنید."),
    "a_admin_needed": ("The bot must be an administrator of the destination until the campaign ends. No Stars were charged.",
                       "ربات باید تا پایان کمپین در مقصد ادمین باشد. استارزی کسر نشد."),
    "a_baseline_stale": ("The baseline count is outdated. Send the post link again to refresh monitoring.",
                         "شمارش پایه قدیمی است. برای تازه‌سازی پایش، لینک پست را دوباره ارسال کنید."),
    "a_showcase_down": ("The showcase is unavailable.", "ویترین در دسترس نیست."),
    "a_my_empty": ("You have not created any campaign yet.", "هنوز کمپینی ایجاد نکرده‌اید."),
    "a_published": ("Campaign published.", "کمپین منتشر شد."),
    "a_cancel_inactive": ("This campaign is not active.", "این کمپین فعال نیست."),
    "a_cancelled": ("Cancelled. {n} Stars refunded.", "لغو شد. {n} استارز بازگردانده شد."),

    # ───────── Create-campaign flow ─────────
    "new": (
        "<b>New campaign</b>\n<i>Choose what you want to promote</i>\n\nThe price per unit is shown on each button.",
        "<b>ایجاد کمپین</b>\n<i>فعالیت موردنظر برای ترویج را انتخاب کنید</i>\n\nهزینه‌ی هر واحد روی دکمه‌ها درج شده است."),
    "prompt_join": (
        "<b>Join destination</b>\n\nSend the public username or link of the channel or group, for example <code>@mychannel</code>.\n"
        "<blockquote>First add <code>@{bot}</code> as an administrator of the destination so memberships can be verified.</blockquote>",
        "<b>مقصد عضویت</b>\n\nنام کاربری یا لینک عمومی کانال یا گروه را ارسال کنید؛ برای مثال <code>@mychannel</code>.\n"
        "<blockquote>ابتدا ربات <code>@{bot}</code> را در مقصد ادمین کنید تا بتواند عضویت‌ها را بررسی کند.</blockquote>"),
    "prompt_post": (
        "<b>Destination post</b>\n\nSend the post link, for example <code>https://t.me/channel/123</code>, "
        "or forward the channel post to this chat.\n"
        "<blockquote>Forwarded group messages cannot be identified. For groups, always send the message link.</blockquote>",
        "<b>پست مقصد</b>\n\nلینک پست را ارسال کنید؛ برای مثال <code>https://t.me/channel/123</code>؛ "
        "یا خود پست کانال را به این گفتگو فوروارد نمایید.\n"
        "<blockquote>پیام‌های گروه با فوروارد قابل شناسایی نیستند. برای گروه‌ها حتماً لینک پیام را ارسال کنید.</blockquote>"),
    "err_link_join": ("<b>Invalid link</b>\nSend <code>@username</code> or <code>t.me/username</code>.",
                      "<b>لینک نامعتبر است</b>\n<code>@username</code> یا <code>t.me/username</code> ارسال کنید."),
    "err_link_post": ("<b>Invalid link</b>\nExpected format: <code>https://t.me/username/123</code>.",
                      "<b>لینک نامعتبر است</b>\nقالب صحیح: <code>https://t.me/username/123</code>"),
    "err_not_found": ("<b>Destination not found</b>\nIt must be a public channel or group with a username.",
                      "<b>مقصد پیدا نشد</b>\nمقصد باید کانال یا گروه عمومی و دارای نام کاربری باشد."),
    "err_not_admin": ("<b>Bot is not an administrator</b>\nAdd @{bot} as an administrator of <b>{title}</b>, then send the link again.",
                      "<b>ربات ادمین نیست</b>\nربات @{bot} را در «{title}» ادمین کنید و سپس لینک را دوباره ارسال نمایید."),
    "err_count_admin": ("<b>Bot is not an administrator</b>\nCount-based verification requires the bot to be an administrator of <b>{title}</b>. "
                        "Add it as admin, then send the post link again.",
                        "<b>ربات ادمین نیست</b>\nتأیید شمارشی نیازمند ادمین بودن ربات در «{title}» است. "
                        "ربات را ادمین کنید و سپس لینک پست را دوباره ارسال نمایید."),
    "err_react_dest": ("<b>Unsupported destination</b>\nReaction tasks require a channel where the bot is an administrator, "
                       "or a group where the bot is an administrator.",
                       "<b>مقصد پشتیبانی نمی‌شود</b>\nتسک واکنش نیازمند کانالی است که ربات در آن ادمین باشد، "
                       "یا گروهی که ربات در آن ادمین است."),
    "err_forward_noid": ("<b>Post could not be identified</b>\nTelegram did not expose the public post ID. "
                         "Send the post link instead, or enter the @username and message number.",
                         "<b>پست شناسایی نشد</b>\nتلگرام شناسه‌ی عمومی پست را در اختیار ربات قرار نداد. "
                         "به‌جای آن لینک پست را ارسال کنید یا @username و شماره‌ی پیام را وارد نمایید."),
    "err_forward_group": ("<b>Forwarded group message</b>\nTelegram does not include the original message ID of group forwards. "
                          "To verify reactions on the exact message, send the message link.",
                          "<b>پیام فورواردشده‌ی گروه</b>\nتلگرام شناسه‌ی پیام اصلی گروه را در فوروارد ارائه نمی‌دهد. "
                          "برای بررسی دقیق واکنش روی همان پیام، لینک پیام را ارسال کنید."),
    "err_forward_other": ("<b>Forward not recognized</b>\nThe source information is not enough to identify the post. "
                          "Send the post link. For private groups, Telegram may hide the original message ID.",
                          "<b>فوروارد شناسایی نشد</b>\nاطلاعات مبدأ برای شناسایی پست کافی نیست. "
                          "لینک پست را ارسال کنید. در گروه‌های خصوصی ممکن است تلگرام شناسه‌ی پیام اصلی را پنهان کند."),

    "baseline_wait": (
        "<b>Monitoring setup</b>\n\nA live update of this post's reaction count is required before monitoring can start.\n"
        "<blockquote>Add or remove a reaction on the post inside the channel (or ask an admin to), then wait one to a few minutes. "
        "When Telegram delivers the count, you will be moved to the next step automatically.</blockquote>\n"
        "<i>Telegram does not expose reaction counts instantly through the API, so updates can be delayed by a few minutes.</i>",
        "<b>راه‌اندازی پایش</b>\n\nپیش از شروع پایش، دریافت یک به‌روزرسانی زنده از شمارش واکنش‌های این پست لازم است.\n"
        "<blockquote>داخل کانال یک واکنش به همین پست اضافه یا حذف کنید (یا از یکی از ادمین‌ها بخواهید) و یک تا چند دو صبر کنید. "
        "هنگامی که تلگرام شمارش را ارسال کند، به‌صورت خودکار به مرحله‌ی بعد منتقل می‌شوید.</blockquote>\n"
),
    "baseline_pending": ("The count update has not arrived yet. Add or remove a reaction on the same post and wait a moment.",
                         "به‌روزرسانی شمارش هنوز نرسیده است. یک واکنش روی همان پست اضافه یا حذف کنید و کمی صبر نمایید."),
    "baseline_need_action": ("<b>Fresh count required</b>\nAdd or remove a reaction on the same post and wait for the update.",
                             "<b>شمارش تازه لازم است</b>\nیک واکنش به همان پست اضافه یا حذف کنید و منتظر به‌روزرسانی بمانید."),
    "baseline_restart": ("<b>Fresh count required</b>\nStart the reaction campaign again from the menu.",
                         "<b>شمارش تازه لازم است</b>\nثبت کمپین واکنش را دوباره از منو آغاز کنید."),
    "window_ready": (
        "<b>Monitoring ready</b>\n<blockquote><b>Latest count:</b> <code>{total}</code> reactions</blockquote>\n"
        "Choose the time limit each user has to complete the task. Telegram may deliver count updates with some delay.",
        "<b>پایش آماده است</b>\n<blockquote><b>آخرین شمارش:</b> <code>{total}</code> واکنش</blockquote>\n"
        "مهلتی را انتخاب کنید که هر کاربر برای انجام تسک در اختیار دارد. ممکن است تلگرام به‌روزرسانی شمارش را با تأخیر ارسال کند."),
    "window_received": (
        "<b>Count received</b>\n<blockquote><b>Reactions:</b> <code>{total}</code></blockquote>\nChoose the time limit for each task:",
        "<b>آمار دریافت شد✅</b>\n<blockquote><b>واکنش‌ها:</b> <code>{total}</code></blockquote>\nمهلت انجام هر تسک را انتخاب کنید:"),
    "window_hint": ("Choose the time limit using the buttons: 2 or 5 minutes.",
                    "مهلت را با دکمه‌ها انتخاب کنید: 2 یا 5 دقیقه."),
    "qty_prompt": ("<b>Quantity</b>\n\nHow many units do you need? Send a number from <code>1</code> to <code>{max}</code>.",
                   "<b>تعداد</b>\n\nچند واحد نیاز دارید؟ عددی بین <code>1</code> تا <code>{max}</code> ارسال کنید."),
    "qty_insufficient": (
        "<b>Insufficient balance</b>\n\nYour balance (<code>{points}</code> Stars) does not cover a single unit (<code>{cost}</code> Stars).\n"
        "Complete a few tasks or invite friends to earn Stars.",
        "<b>موجودی ناکافی</b>\n\nموجودی شما (<code>{points}</code> استارز) برای ثبت حتی یک واحد (<code>{cost}</code> استارز) کافی نیست.\n"
        "ابتدا چند تسک انجام دهید یا با دعوت از دوستان استارز کسب کنید."),
    "qty_nan": ("Please send a number only.", "لطفاً فقط یک عدد ارسال کنید."),
    "qty_range": ("The number must be between 1 and {max}.", "عدد باید بین 1 تا {max} باشد."),
    "summary": (
        "<b>Campaign summary</b>\n<blockquote><b>Type:</b> {type}\n<b>Target:</b> {title}\n"
        "<b>Quantity:</b> <code>{qty}</code>\n<b>Cost:</b> <code>{total}</code> Stars\n<b>Verification:</b> {how}</blockquote>"
        "{admin_note}\n\nConfirm to publish this campaign.",
        "<b>خلاصه‌ی کمپین</b>\n<blockquote><b>نوع:</b> {type}\n<b>مقصد:</b> {title}\n"
        "<b>تعداد:</b> <code>{qty}</code>\n<b>هزینه:</b> <code>{total}</code> استارز\n<b>روش تأیید:</b> {how}</blockquote>"
        "{admin_note}\n\nبرای انتشار کمپین، تأیید کنید."),
    "summary_admin": ("\n<i>The bot must remain an administrator of the destination until the campaign ends; otherwise the campaign pauses temporarily.</i>",
                      "\n<i>ربات باید تا پایان کمپین در مقصد ادمین بماند؛ در غیر این صورت کمپین موقتاً متوقف می‌شود.</i>"),
    "how_join": ("Automatic membership check, re-checked at the end of the review period",
                 "بررسی خودکار عضویت و بازبینی در پایان دوره‌ی بررسی"),
    "how_view": ("Time-based (views cannot be verified precisely)", "زمانی (بازدید دقیقاً قابل راستی‌آزمایی نیست)"),
    "how_react_auto": ("Per-user reaction check (group)", "بررسی فردی واکنش (گروه)"),
    "how_react_count": ("Channel count check, {window}-minute limit", "بررسی شمارشی کانال، مهلت {window} دقیقه"),
    "published": (
        "<b>Campaign published</b>\n\nYour task is live in the showcase channel. Rewards are paid automatically after each verified completion. "
        "Track progress under <b>My campaigns</b>.",
        "<b>کمپین منتشر شد</b>\n\nتسک شما در کانال ویترین قرار گرفت. پاداش پس از تأیید هر انجام، به‌صورت خودکار واریز می‌شود. "
        "پیشرفت کمپین را از بخش «کمپین‌های من» دنبال کنید."),
    "publish_failed": (
        "<b>Campaign not published — cost refunded</b>\n\nThe bot administrator must set <code>TASKS_CHANNEL</code>, "
        "add the bot to the showcase channel as an administrator and allow it to post and edit messages.\n\n"
        "You can create the campaign again once the showcase is ready. Your Stars have been returned.",
        "<b>کمپین منتشر نشد — هزینه بازگردانده شد</b>\n\nمدیر ربات باید متغیر <code>TASKS_CHANNEL</code> را تنظیم کند، "
        "ربات را در کانال ویترین ادمین نماید و دسترسی ارسال و ویرایش پیام را فعال کند.\n\n"
        "پس از آماده شدن ویترین می‌توانید کمپین را دوباره ثبت کنید. استارز‌های شما بازگردانده شده است."),

    # ───────── My campaigns ─────────
    "my": ("<b>My campaigns</b>\n<i>Your latest 10 campaigns</i>\n\n{rows}",
           "<b>کمپین‌های من</b>\n<i>10 کمپین آخر شما</i>\n\n{rows}"),
    "my_empty": ("<b>My campaigns</b>\n\nYou have not created any campaign yet.",
                 "<b>کمپین‌های من</b>\n\nهنوز کمپینی ایجاد نکرده‌اید."),
    "my_row": ("<b>{n}.</b> {type} — {title}\n{bar} <code>{done}/{target}</code> · {state}{pending}",
               "<b>{n}.</b> {type} — {title}\n{bar} <code>{done}/{target}</code> · {state}{pending}"),
    "my_pending": (" · {p} pending", " · {p} در انتظار"),
    "cancel_ask": ("<b>Cancel campaign</b>\n\nThe remaining capacity will be cancelled and its cost refunded. Continue?",
                   "<b>لغو کمپین</b>\n\nظرفیت باقی‌مانده لغو و هزینه‌ی آن بازگردانده می‌شود. ادامه می‌دهید؟"),

    # ───────── Reaction monitor ─────────
    "rw_head": ("<b>Reaction monitor</b>\n<i>Updates arrive only when a count changes. Telegram may group or delay them by a few minutes.</i>\n\n{rows}",
                "<b>پایش واکنش‌ها</b>\n<i>به‌روزرسانی‌ها فقط هنگام تغییر شمارش می‌رسند و ممکن است تلگرام آن‌ها را گروهی یا با چند دقیقه تأخیر ارسال کند.</i>\n\n{rows}"),
    "rw_empty": ("<b>Reaction monitor</b>\n\nNo posts are monitored yet. A channel post is added here when you create a reaction campaign.",
                 "<b>پایش واکنش‌ها</b>\n\nهنوز پستی برای پایش ثبت نشده است. با ایجاد کمپین واکنش، پست کانال به این فهرست افزوده می‌شود."),
    "rw_row": ("<b>{title}</b> — post <code>{mid}</code>\n<blockquote><b>Count:</b> <code>{total}</code>\n"
               "<b>Last update:</b> {age}\n<b>Delivery delay:</b> {delay}{extra}</blockquote>",
               "<b>{title}</b> — پست <code>{mid}</code>\n<blockquote><b>شمارش:</b> <code>{total}</code>\n"
               "<b>آخرین به‌روزرسانی:</b> {age}\n<b>تأخیر دریافت:</b> {delay}{extra}</blockquote>"),
    "rw_top": ("\n<b>Top reactions:</b> {list}", "\n<b>پرتکرارترین واکنش‌ها:</b> {list}"),
    "rw_camp": ("\n<b>Active campaign:</b> {target} ordered · {remaining} left · {window} min{delta}",
                "\n<b>کمپین فعال:</b> {target} سفارش · {remaining} باقی‌مانده · {window} دقیقه{delta}"),
    "rw_delta": (" · change since start {delta}", " · تغییر از شروع کمپین {delta}"),
    "rw_age": ("{m} min {s} s ago", "{m} دقیقه و {s} ثانیه پیش"),
    "rw_age_none": ("no update yet", "هنوز به‌روزرسانی نرسیده"),
    "rw_delay": ("about {d} s", "حدود {d} ثانیه"),

    # ───────── Campaign / showcase post ─────────
    "listing": (
        "<b>{type}</b> · {target}\n<blockquote> <b>Reward:</b> <code>{reward}</code> Stars⭐\n"
        " <b>Remaining spots:</b> <code>{remaining}</code>\n <b>Status:</b> {state}</blockquote>\n{instruction}",
        "<b>{type}</b> · {target}\n<blockquote> <b>پاداش:</b> <code>{reward}</code> استارز⭐\n"
        " <b>ظرفیت باقی‌مانده:</b> <code>{remaining}</code>\n <b>وضعیت:</b> {state}</blockquote>\n{instruction}"),
    "ins_count": ("Add the requested reaction to the post, then tap <b>Claim reward</b> within {window} minutes.",
                  "واکنش درخواستی را روی پست ثبت کنید و ظرف {window} دقیقه روی «دریافت پاداش» بزنید."),
    "ins_react": ("React to the destination message, then tap <b>Claim reward</b>.",
                  "روی پیام مقصد واکنش بزنید، سپس روی «دریافت پاداش» بزنید."),
    "ins_join": ("Join the destination, then tap <b>Claim reward</b>.",
                 "در مقصد عضو شوید، سپس روی «دریافت پاداش» بزنید."),
    "ins_view": ("Open the destination post, wait a few seconds, then tap <b>Claim reward</b>.",
                 "پست مقصد را مشاهده کنید، چند ثانیه صبر نمایید و سپس روی «دریافت پاداش» بزنید."),
    "ins_other": ("Complete the task at the destination, then tap <b>Claim reward</b>.",
                  "تسک را در مقصد انجام دهید، سپس روی «دریافت پاداش» بزنید."),

    # ───────── Notifications to campaign owners ─────────
    "n_full": (
        "<b>Campaign completed</b>\n<blockquote>{type} — {title}</blockquote>\n"
        "Final verification runs within {hold_h} hours. If a participant breaks the rules, their spot reopens.",
        "<b>کمپین تکمیل شد</b>\n<blockquote>{type} — {title}</blockquote>\n"
        "تأیید نهایی طی {hold_h} ساعت انجام می‌شود و در صورت تخلف هر شرکت‌کننده، ظرفیت او دوباره باز می‌شود."),
    "n_full_count": (
        "<b>Campaign completed</b>\n<blockquote>{type} — {title}</blockquote>\nThe reaction increases were verified.",
        "<b>کمپین تکمیل شد</b>\n<blockquote>{type} — {title}</blockquote>\nافزایش واکنش‌ها تأیید شد."),
    "n_admin_restored": (
        "<b>Access restored</b>\n\nThe bot's access to <b>{title}</b> is back and the campaign is active again.",
        "<b>دسترسی بازیابی شد</b>\n\nدسترسی ربات در «{title}» برگشت و کمپین دوباره فعال است."),
    "n_admin_lost": (
        "<b>Campaign paused</b>\n\nThe bot lost administrator access to <b>{title}</b>. The campaign is paused and resumes once access is restored.",
        "<b>کمپین متوقف شد</b>\n\nربات دسترسی ادمین «{title}» را از دست داده است. کمپین موقتاً متوقف شد و پس از بازیابی دسترسی ادامه می‌یابد."),

    # ───────── Admin ─────────
    "ad_stats": (
        "<b>Statistics</b>\n<blockquote><b>Users:</b> <code>{users}</code>\n<b>Active campaigns:</b> <code>{active}</code>\n"
        "<b>Pending:</b> <code>{pending}</code>\n<b>Paid:</b> <code>{paid}</code>\n<b>Reversed:</b> <code>{rev}</code>\n"
        "<b>Referred:</b> <code>{refs}</code> ({refs_active} active)</blockquote>",
        "<b>آمار</b>\n<blockquote><b>کاربران:</b> <code>{users}</code>\n<b>کمپین‌های فعال:</b> <code>{active}</code>\n"
        "<b>در انتظار:</b> <code>{pending}</code>\n<b>پرداخت‌شده:</b> <code>{paid}</code>\n<b>برگشتی:</b> <code>{rev}</code>\n"
        "<b>دعوت‌شده‌ها:</b> <code>{refs}</code> ({refs_active} فعال)</blockquote>"),
    "ad_done": ("Done.", "انجام شد."),
    "ad_unbanned": ("Warnings reset to zero.", "اخطارها صفر شد."),
    "ad_stopped": ("Campaign stopped and the remaining balance refunded.", "کمپین متوقف و باقی‌مانده بازگردانده شد."),
    "ad_not_found": ("No active campaign found.", "کمپین فعالی پیدا نشد."),
    "ad_usage": (
        "<b>Admin commands</b>\n\n<code>/stats</code>\n<code>/reactwatch</code>\n<code>/give &lt;uid&gt; &lt;n&gt;</code>\n"
        "<code>/unban &lt;uid&gt;</code>\n<code>/stop &lt;campaign_id&gt;</code>",
        "<b>دستورهای مدیر</b>\n\n<code>/stats</code>\n<code>/reactwatch</code>\n<code>/give &lt;uid&gt; &lt;n&gt;</code>\n"
        "<code>/unban &lt;uid&gt;</code>\n<code>/stop &lt;campaign_id&gt;</code>"),
}

# Shown to every new user, before a language is chosen: English first, then Persian.
LANG_PICK = ("<b>Choose your language⚙️</b>\n<i>Select the language the bot will use.</i>\n\n"
             + RLM + "<b>زبان خود را انتخاب کنید</b>\n" + RLM + "<i>زبان مورد استفاده‌ی ربات را انتخاب نمایید.</i>")


# ═════════════════════════════ Database helpers ═════════════════════════════
def day():
    return time.strftime("%Y-%m-%d", time.gmtime())


def ledger(uid, delta, kind, ref=""):
    db.ledger.insert_one({"uid": uid, "d": delta, "k": kind, "ref": str(ref), "t": time.time()})


def track_user_event(uid, action, detail=""):
    """Store a compact interaction audit trail without saving arbitrary message bodies."""
    try:
        now = time.time()
        label = str(detail or "")[:120]
        db.user_events.insert_one({"uid": int(uid), "action": str(action)[:40], "detail": label, "t": now})
        db.users.update_one({"_id": int(uid)}, {"$set": {"last_seen": now, "last_action": str(action)[:40], "remind_miss": 0, "blocked": False}})
    except Exception:
        log.exception("failed to track user event uid=%s action=%s", uid, action)


def user_is_banned(uid):
    u = db.users.find_one({"_id": int(uid)}, {"banned": 1}) or {}
    return bool(u.get("banned"))


def admin_text(lang, en, fa):
    return H(_rtl(fa) if lang == "fa" else en)


def admin_home_keyboard(lang):
    fa = lang == "fa"
    return [
        [B("📊 آمار کلی" if fa else "📊 Overview", "ad:stats", "primary")],
        [B("👥 کاربران" if fa else "👥 Users", "ad:users:0"), B("🔎 جست‌وجو" if fa else "🔎 Search", "ad:search")],
        [B("📣 کمپین‌ها" if fa else "📣 Campaigns", "ad:campaigns:0"), B("📢 همه‌پیامی" if fa else "📢 Broadcast", "ad:bc")],
        [B("🧾 تعاملات اخیر" if fa else "🧾 Recent activity", "ad:recent:0")],
        [B("↩️ بازگشت به منو" if fa else "↩️ Main menu", "home")],
    ]


def admin_home(chat, lang, mid=None):
    if lang == "fa":
        msg = "<b>🛡 پنل مدیریت</b>\n\nاز این بخش می‌توانید آمار، کاربران، گردش امتیازها، تعاملات و کمپین‌ها را مدیریت کنید. این پنل فقط برای شناسه‌های تنظیم‌شده در <code>ADMIN_IDS</code> فعال است."
    else:
        msg = "<b>🛡 Admin panel</b>\n\nManage statistics, users, point history, interactions and campaigns here. Access is restricted to IDs configured in <code>ADMIN_IDS</code>."
    return view(chat, mid, H(_rtl(msg) if lang == "fa" else msg), admin_home_keyboard(lang))


def admin_stats_text(lang):
    now = time.time()
    users = db.users.count_documents({})
    new_24h = db.users.count_documents({"joined": {"$gte": now - 86400}})
    active_24h = db.users.count_documents({"last_seen": {"$gte": now - 86400}})
    banned = db.users.count_documents({"banned": True})
    active_c = db.campaigns.count_documents({"status": "active"})
    all_c = db.campaigns.count_documents({})
    pending = db.completions.count_documents({"status": "pending"})
    paid = db.completions.count_documents({"status": "paid"})
    reversed_n = db.completions.count_documents({"status": "reversed"})
    interactions = db.user_events.count_documents({})
    if lang == "fa":
        msg = ("<b>📊 آمار کلی</b>\n<blockquote>"
               f"کل کاربران: <code>{users}</code>\nکاربران جدید در ۲۴ ساعت: <code>{new_24h}</code>\n"
               f"فعال در ۲۴ ساعت: <code>{active_24h}</code>\nمسدودشده‌ها: <code>{banned}</code>\n"
               f"کمپین فعال / کل: <code>{active_c} / {all_c}</code>\n"
               f"تسک‌های در انتظار: <code>{pending}</code>\nتسک‌های پرداخت‌شده: <code>{paid}</code>\n"
               f"تسک‌های برگشتی: <code>{reversed_n}</code>\nرویدادهای ثبت‌شده: <code>{interactions}</code></blockquote>")
    else:
        msg = ("<b>📊 Overview</b>\n<blockquote>"
               f"Total users: <code>{users}</code>\nNew users (24h): <code>{new_24h}</code>\n"
               f"Active (24h): <code>{active_24h}</code>\nBanned: <code>{banned}</code>\n"
               f"Active / total campaigns: <code>{active_c} / {all_c}</code>\n"
               f"Pending tasks: <code>{pending}</code>\nPaid tasks: <code>{paid}</code>\n"
               f"Reversed tasks: <code>{reversed_n}</code>\nRecorded events: <code>{interactions}</code></blockquote>")
    return H(_rtl(msg) if lang == "fa" else msg)


def admin_users_screen(chat, lang, offset=0, mid=None, search_results=None):
    offset = max(0, int(offset))
    rows = []
    kb = []
    if search_results is None:
        docs = list(db.users.find({}).sort([("last_seen", -1), ("joined", -1)]).skip(offset).limit(8))
        total = db.users.count_documents({})
        title = ("<b>👥 کاربران</b>\n" if lang == "fa" else "<b>👥 Users</b>\n")
        title += (f"صفحه از مجموع {total} کاربر\n\n" if lang == "fa" else f"Showing users · total {total}\n\n")
    else:
        docs = search_results[:10]
        title = "<b>🔎 نتایج جست‌وجو</b>\n\n" if lang == "fa" else "<b>🔎 Search results</b>\n\n"
        total = len(docs)
    for u in docs:
        uid = int(u.get("_id", 0))
        name = html.escape(str(u.get("name") or "بدون نام"), quote=False)[:35]
        uname = ("@" + html.escape(str(u.get("username")), quote=False)) if u.get("username") else "—"
        status = ("⛔ " if u.get("banned") else "")
        rows.append(f"{status}<b>{name}</b> · {uname} · <code>{uid}</code> · ⭐ {int(u.get('points', 0))}")
        kb.append([B(f"{name[:24]} · {uid}", f"ad:user:{uid}")])
    if not docs:
        rows.append("کاربری پیدا نشد." if lang == "fa" else "No users found.")
    if search_results is None:
        nav = []
        if offset > 0:
            nav.append(B("⬅️ قبلی" if lang == "fa" else "⬅️ Prev", f"ad:users:{max(0, offset-8)}"))
        if offset + 8 < total:
            nav.append(B("بعدی ➡️" if lang == "fa" else "Next ➡️", f"ad:users:{offset+8}"))
        if nav:
            kb.append(nav)
    kb.extend([[B("🔎 جست‌وجو" if lang == "fa" else "🔎 Search", "ad:search")],
               [B("↩️ پنل" if lang == "fa" else "↩️ Panel", "ad:home")]])
    body = title + "\n".join(rows)
    return view(chat, mid, H(_rtl(body) if lang == "fa" else body), kb)


def admin_user_detail(chat, lang, target_uid, mid=None):
    try:
        target_uid = int(target_uid)
    except (TypeError, ValueError):
        return view(chat, mid, admin_text(lang, "Invalid user ID.", "شناسه کاربر نامعتبر است."),
                    [[B("↩️ پنل" if lang == "fa" else "↩️ Panel", "ad:users:0")]])
    u = db.users.find_one({"_id": target_uid})
    if not u:
        return view(chat, mid, admin_text(lang, "User not found.", "کاربر پیدا نشد."),
                    [[B("↩️ کاربران" if lang == "fa" else "↩️ Users", "ad:users:0")]])
    comp_counts = {x: db.completions.count_documents({"uid": target_uid, "status": x})
                   for x in ("pending", "paid", "reversed", "count_pending")}
    campaigns = db.campaigns.count_documents({"owner": target_uid})
    active_campaigns = db.campaigns.count_documents({"owner": target_uid, "status": "active"})
    ledger_count = db.ledger.count_documents({"uid": target_uid})
    event_count = db.user_events.count_documents({"uid": target_uid})
    last_seen = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(u["last_seen"])) if u.get("last_seen") else "—"
    joined = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(u["joined"])) if u.get("joined") else "—"
    uname = "@" + str(u.get("username")) if u.get("username") else "—"
    status = "⛔ BANNED" if u.get("banned") else "✅ Active"
    name = html.escape(str(u.get("name") or "—"), quote=False)
    if lang == "fa":
        msg = (f"<b>👤 مشخصات کاربر</b>\n<blockquote>نام: <code>{name}</code>\nشناسه: <code>{target_uid}</code>\n"
               f"نام کاربری: <code>{html.escape(uname)}</code>\nوضعیت: <code>{status}</code>\n"
               f"امتیاز: <code>{u.get('points',0)}</code> · امتیاز معلق: <code>{u.get('hold',0)}</code>\n"
               f"کل کسب‌شده: <code>{u.get('earned',0)}</code> · اخطار: <code>{u.get('strikes',0)}</code>\n"
               f"عضویت: <code>{joined}</code>\nآخرین فعالیت: <code>{last_seen}</code>\n"
               f"کمپین‌ها: <code>{active_campaigns}/{campaigns}</code> (فعال/کل)\n"
               f"تسک‌ها — در انتظار: <code>{comp_counts['pending']+comp_counts['count_pending']}</code>، پرداخت‌شده: <code>{comp_counts['paid']}</code>، برگشتی: <code>{comp_counts['reversed']}</code>\n"
               f"گردش امتیاز: <code>{ledger_count}</code> · تعاملات: <code>{event_count}</code></blockquote>")
    else:
        msg = (f"<b>👤 User profile</b>\n<blockquote>Name: <code>{name}</code>\nID: <code>{target_uid}</code>\n"
               f"Username: <code>{html.escape(uname)}</code>\nStatus: <code>{status}</code>\n"
               f"Balance: <code>{u.get('points',0)}</code> · On hold: <code>{u.get('hold',0)}</code>\n"
               f"Total earned: <code>{u.get('earned',0)}</code> · Strikes: <code>{u.get('strikes',0)}</code>\n"
               f"Joined: <code>{joined}</code>\nLast activity: <code>{last_seen}</code>\n"
               f"Campaigns: <code>{active_campaigns}/{campaigns}</code> (active/total)\n"
               f"Tasks — pending: <code>{comp_counts['pending']+comp_counts['count_pending']}</code>, paid: <code>{comp_counts['paid']}</code>, reversed: <code>{comp_counts['reversed']}</code>\n"
               f"Point records: <code>{ledger_count}</code> · interactions: <code>{event_count}</code></blockquote>")
    kb = [[B("➕➖ امتیاز" if lang == "fa" else "➕➖ Adjust points", f"ad:points:{target_uid}")],
          [B(("🔓 رفع مسدودی" if lang == "fa" else "🔓 Unban") if u.get("banned") else ("⛔ مسدودکردن" if lang == "fa" else "⛔ Ban"), f"ad:toggle:{target_uid}")],
          [B("🧹 صفرکردن اخطارها" if lang == "fa" else "🧹 Reset strikes", f"ad:strikes:{target_uid}")],
          [B("🧾 گردش امتیاز" if lang == "fa" else "🧾 Point history", f"ad:ledger:{target_uid}"),
           B("🕘 تعاملات" if lang == "fa" else "🕘 Activity", f"ad:events:{target_uid}")],
          [B("↩️ فهرست کاربران" if lang == "fa" else "↩️ Users", "ad:users:0"),
           B("پنل" if lang == "fa" else "Panel", "ad:home")]]
    return view(chat, mid, H(_rtl(msg) if lang == "fa" else msg), kb)


def admin_user_ledger(chat, lang, target_uid, mid=None):
    items = list(db.ledger.find({"uid": int(target_uid)}).sort("t", -1).limit(10))
    lines = ["<b>🧾 گردش امتیاز</b>\n" if lang == "fa" else "<b>🧾 Point history</b>\n"]
    for x in items:
        stamp = time.strftime("%m-%d %H:%M", time.gmtime(x.get("t", 0)))
        lines.append(f"<code>{stamp}</code> · <code>{int(x.get('d',0)):+}</code> · <code>{html.escape(str(x.get('k','')))}</code> · <code>{html.escape(str(x.get('ref',''))[:24])}</code>")
    if not items:
        lines.append("رکوردی نیست." if lang == "fa" else "No records.")
    return view(chat, mid, H(_rtl("\n".join(lines)) if lang == "fa" else "\n".join(lines)),
                [[B("↩️ پروفایل" if lang == "fa" else "↩️ Profile", f"ad:user:{int(target_uid)}")]])


def admin_events_screen(chat, lang, target_uid=None, offset=0, mid=None):
    """Paginated interaction audit log; callers must pass through the admin callback guard."""
    offset = max(0, int(offset))
    query = {"uid": int(target_uid)} if target_uid is not None else {}
    total = db.user_events.count_documents(query)
    items = list(db.user_events.find(query).sort("t", -1).skip(offset).limit(12))
    if target_uid is not None:
        title = "<b>🕘 تعاملات کاربر</b>\n" if lang == "fa" else "<b>🕘 User activity</b>\n"
    else:
        title = "<b>🧾 تعاملات اخیر</b>\n" if lang == "fa" else "<b>🧾 Recent activity</b>\n"
    page = (offset // 12) + 1
    pages = max(1, (total + 11) // 12)
    title += (f"صفحه {page}/{pages} · مجموع {total} رویداد\n\n" if lang == "fa" else
              f"Page {page}/{pages} · {total} events total\n\n")
    lines = [title]
    for x in items:
        u = db.users.find_one({"_id": x.get("uid")}, {"name": 1, "username": 1}) or {}
        who = html.escape(str(("@" + u["username"]) if u.get("username") else (u.get("name") or x.get("uid"))))[:32]
        stamp = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(x.get("t", 0)))
        action = html.escape(str(x.get("action", "")))[:40]
        detail = html.escape(str(x.get("detail", "")))[:70]
        lines.append(f"<code>{stamp}</code> · <b>{who}</b> (<code>{x.get('uid')}</code>) · <code>{action}</code> {detail}")
    if not items:
        lines.append("تعامل ثبت‌شده‌ای وجود ندارد." if lang == "fa" else "No recorded interactions.")

    kb = []
    nav = []
    if offset > 0:
        nav.append(B("⬅️ قبلی" if lang == "fa" else "⬅️ Prev",
                     f"ad:eventspage:{target_uid}:{max(0, offset-12)}" if target_uid is not None else f"ad:recent:{max(0, offset-12)}"))
    if offset + 12 < total:
        nav.append(B("بعدی ➡️" if lang == "fa" else "Next ➡️",
                     f"ad:eventspage:{target_uid}:{offset+12}" if target_uid is not None else f"ad:recent:{offset+12}"))
    if nav:
        kb.append(nav)
    if target_uid is not None:
        kb.append([B("↩️ پروفایل" if lang == "fa" else "↩️ Profile", f"ad:user:{int(target_uid)}")])
    else:
        kb.append([B("↩️ پنل" if lang == "fa" else "↩️ Panel", "ad:home")])
    body = "\n".join(lines)
    return view(chat, mid, H(_rtl(body) if lang == "fa" else body), kb)


def admin_campaigns_screen(chat, lang, offset=0, mid=None):
    offset = max(0, int(offset))
    docs = list(db.campaigns.find({}).sort("created", -1).skip(offset).limit(8))
    total = db.campaigns.count_documents({})
    lines = ["<b>📣 کمپین‌ها</b>\n\n" if lang == "fa" else "<b>📣 Campaigns</b>\n\n"]
    kb = []
    for c in docs:
        owner = db.users.find_one({"_id": c.get("owner")}, {"name": 1}) or {}
        title = html.escape(str(c.get("title") or c.get("uname") or c.get("type") or "campaign"))[:35]
        cid = str(c.get("_id"))
        status = str(c.get("status", "?"))
        lines.append(f"• <b>{title}</b> · <code>{status}</code> · owner <code>{c.get('owner')}</code> · left <code>{c.get('remaining',0)}</code> · <code>{cid}</code>")
        if status == "active":
            kb.append([B(f"⛔ توقف {title[:22]}" if lang == "fa" else f"⛔ Stop {title[:22]}", f"ad:stop:{cid}")])
    nav = []
    if offset > 0:
        nav.append(B("⬅️ قبلی" if lang == "fa" else "⬅️ Prev", f"ad:campaigns:{max(0,offset-8)}"))
    if offset + 8 < total:
        nav.append(B("بعدی ➡️" if lang == "fa" else "Next ➡️", f"ad:campaigns:{offset+8}"))
    if nav:
        kb.append(nav)
    kb.append([B("↩️ پنل" if lang == "fa" else "↩️ Panel", "ad:home")])
    return view(chat, mid, H(_rtl("\n".join(lines)) if lang == "fa" else "\n".join(lines)), kb)


def admin_search_results(query):
    query = str(query or "").strip()
    if not query:
        return []
    if query.isdigit():
        u = db.users.find_one({"_id": int(query)})
        return [u] if u else []
    uname = query.lstrip("@")
    found = list(db.users.find({"username": {"$regex": "^" + re.escape(uname) + "$", "$options": "i"}}).limit(10))
    if found:
        return found
    return list(db.users.find({"name": {"$regex": re.escape(query), "$options": "i"}}).limit(10))


def admin_apply_point_delta(admin_uid, target_uid, delta):
    delta = int(delta)
    if delta == 0 or abs(delta) > 1000000:
        return False
    if delta > 0:
        add_points(int(target_uid), delta, "admin_adjust", f"admin:{admin_uid}")
        return True
    return spend(int(target_uid), abs(delta), "admin_adjust", f"admin:{admin_uid}")


def ensure_user(u):
    uid, name = u["id"], (u.get("first_name") or "")[:40]
    hint = "fa" if str(u.get("language_code") or "").lower().startswith("fa") else "en"
    doc = db.users.find_one({"_id": uid})
    if not doc:
        try:
            db.users.insert_one({"_id": uid, "name": name, "username": u.get("username"), "points": START_POINTS,
                                 "hold": 0, "earned": 0, "strikes": 0, "skip": [], "lang": None,
                                 "lang_hint": hint, "joined": time.time()})
            if START_POINTS:
                ledger(uid, START_POINTS, "bonus")
        except DuplicateKeyError:
            pass
        doc = db.users.find_one({"_id": uid})
    else:
        upd = {}
        if doc.get("name") != name or doc.get("username") != u.get("username"):
            upd.update(name=name, username=u.get("username"))
        if not doc.get("lang") and not doc.get("lang_hint"):
            upd["lang_hint"] = hint
        if upd:
            db.users.update_one({"_id": uid}, {"$set": upd})
            doc.update(upd)
    return doc


def add_points(uid, delta, kind, ref=""):
    db.users.update_one({"_id": uid}, {"$inc": {"points": delta}})
    ledger(uid, delta, kind, ref)


def spend(uid, amount, kind, ref=""):
    """Atomic debit; succeeds only when the balance is sufficient."""
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


def set_state(uid, step, d):
    db.states.replace_one({"_id": uid}, {"_id": uid, "step": step, "d": d, "t": time.time()}, upsert=True)


# ═════════════════════════════ UI builders ═════════════════════════════
def main_kb(lang, uid=None):
    # Earn Stars uses a callback so membership is checked at click time. Four focused buttons only.
    ready = True
    if uid is not None:
        ready = (db.users.find_one({"_id": int(uid)}, {"daily_last": 1}) or {}).get("daily_last") != day()
    rows = [[B(tb(lang, "b_earn"), "earn", "primary")],
            [B(tb(lang, "b_gift_ready" if ready else "b_gift"), "gift", "success" if ready else None), B(tb(lang, "b_top"), "top")],
            [B(tb(lang, "b_my"), "campaigns"), B(tb(lang, "b_acc"), "acc")]]
    if uid is not None and int(uid) in ADMINS:
        rows.append([B("🛡 پنل مدیریت" if lang == "fa" else "🛡 Admin panel", "ad:home", "danger")])
    return rows


def account_kb(lang, uid=None):
    off = bool((db.users.find_one({"_id": int(uid)}, {"remind_off": 1}) or {}).get("remind_off")) if uid is not None else False
    return [[B(tb(lang, "b_ref"), "ref", "success"), B(tb(lang, "b_ach"), "ach")],
            [B(tb(lang, "b_rm_off" if off else "b_rm_on"), "rm")],
            [B(tb(lang, "b_help"), "help"), B(tb(lang, "b_lang"), "lang")],
            [B(tb(lang, "b_menu"), "home")]]


def campaigns_kb(lang, uid=None):
    rows = [[B(tb(lang, "b_new"), "new", "primary")]]
    last = (db.users.find_one({"_id": int(uid)}, {"last_join": 1}) or {}).get("last_join") if uid is not None else None
    if last:
        rows.append([B(tb(lang, "b_repeat", name=last), "rep", "success")])      # one-tap repeat of the last join campaign
    return rows + [[B(tb(lang, "b_mine"), "my")], [B(tb(lang, "b_menu"), "home")]]


def home_kb(lang):
    # Help/account-related screens should return to Account, not reset to the main menu.
    return [[B(tb(lang, "b_back"), "acc")]]


def cancel_kb(lang):
    # Campaign creation flow: cancel/back returns to Campaign management.
    return [[B(tb(lang, "b_cancel"), "campaigns", "danger")]]


def lang_kb():
    return [[B("English🇺🇸", "lg:en"), B("فارسی🇮🇷", "lg:fa")]]


def bar(done, total, width=10):
    total = max(int(total), 1)
    done = min(max(int(done), 0), total)
    n = round(width * done / total)
    return "▰" * n + "▱" * (width - n)


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


def ref_link(uid):
    username = _me.get("username") or ""
    return f"https://t.me/{username}?start=ref_{uid}" if username else ""


def link_of(c):
    return f"https://t.me/{c['uname']}" + (f"/{c['mid']}" if c.get("mid") else "")


def post_key(chat_id, message_id):
    return f"{chat_id}:{message_id}"


def ctitle(c):
    return short(c.get("title") or c.get("uname") or "", 60)


def menu_text(u):
    lang = lang_of(u)
    hold = t(lang, "hold_line", hold=u["hold"]) if u.get("hold") else H("")
    name = short(u.get("name"), 30) or tb(lang, "friend")
    return H(str(t(lang, "menu", name=name, points=u.get("points", 0), hold=hold)) + growth_menu_extra(u, lang))


def help_text(lang):
    return t(lang, "help", r_join=TYPES["join"]["reward"], r_react=TYPES["react"]["reward"], r_view=TYPES["view"]["reward"],
             ref_join=REF_JOIN_BONUS, ref_active=REF_ACTIVE_BONUS, ref_tasks=REF_ACTIVE_TASKS,
             hold_h=f"{HOLD_HOURS:g}", strike_limit=STRIKE_LIMIT)


def acc_text(u):
    lang = lang_of(u)
    active = db.campaigns.count_documents({"owner": u["_id"], "status": "active"})
    hold = t(lang, "hold_line", hold=u["hold"]) if u.get("hold") else H("")
    base = t(lang, "account", points=u.get("points", 0), hold=hold, earned=u.get("earned", 0),
             refs=u.get("ref_count", 0), ref_active=u.get("ref_active_count", 0), active=active,
             max_active=MAX_ACTIVE, strikes=u.get("strikes", 0), strike_limit=STRIKE_LIMIT)
    return H(str(base) + "\n" + growth_acc_extra(u, lang))


def acc_kb(lang, uid=None):
    return account_kb(lang, uid)


def ref_text(u):
    uid, lang = u["_id"], lang_of(u)
    link = ref_link(uid)
    friends_html = H("")
    friends = list(db.users.find({"referred_by": uid}, sort=[("joined", -1)], limit=5))
    if friends:
        rows = []
        for f in friends:
            if f.get("ref_active"):
                state = tb(lang, "ref_state_active")
            else:
                state = tb(lang, "ref_state_progress", n=min(int(f.get("ref_tasks", 0)), REF_ACTIVE_TASKS), total=REF_ACTIVE_TASKS)
            rows.append(str(t(lang, "ref_row", name=short(f.get("name"), 18) or tb(lang, "user_fallback"), state=state)))
        friends_html = t(lang, "ref_friends", rows=H("\n".join(rows)))
    cap = t(lang, "ref_cap") if counter_get(f"refd:{uid}:{day()}") >= REF_DAILY_CAP else H("")
    link_html = H(f"<code>{html.escape(link)}</code>") if link else t(lang, "ref_link_missing")
    base = t(lang, "ref", join=REF_JOIN_BONUS, tasks=REF_ACTIVE_TASKS, active=REF_ACTIVE_BONUS,
             total=REF_JOIN_BONUS + REF_ACTIVE_BONUS, count=int(u.get("ref_count", 0)),
             active_count=int(u.get("ref_active_count", 0)), earned=int(u.get("ref_earned", 0)),
             friends=friends_html, capnote=cap, link=link_html)
    return H(str(base) + str(ladder_text(u, lang)))


def ref_kb(uid, lang):
    link = ref_link(uid)
    rows = []
    if link:
        rows.append([U(tb(lang, "b_share"),
                       f"https://t.me/share/url?url={quote(link, safe='')}&text={quote(tb(lang, 'share_text'))}", "primary")])
    rows.append([B(tb(lang, "b_back"), "acc")])
    return rows


# ═════════════════════════════ Showcase (public, bilingual) ═════════════════════════════
def campaign_needs_admin(c):
    if not c:
        return False
    required = c.get("admin_required")
    if required is None:
        required = c.get("type") == "join" or (c.get("type") == "react" and c.get("mode") == "auto")
    return bool(required and (c.get("type") == "join" or (c.get("type") == "react" and c.get("mode") == "auto")))


def is_legacy(c):
    return c.get("type") == "react" and c.get("mode") not in ("auto", "count")


def campaign_state_key(c):
    remaining = max(0, int(c.get("remaining", 0)))
    if c.get("status") == "cancelled":
        return "st_cancelled"
    if is_legacy(c):
        return "st_legacy"
    if c.get("admin_paused") and campaign_needs_admin(c):
        return "st_admin_paused"
    if c.get("status") == "active" and remaining:
        return "st_active"
    return "st_full"


def campaign_listing_text(c):
    """Public post: concise Persian block first, then English translation."""
    typ = c.get("type")
    remaining = max(0, int(c.get("remaining", 0)))
    if typ == "react" and c.get("mode") == "count":
        ins = "ins_count"
    elif typ == "react" and c.get("mode") == "auto":
        ins = "ins_react"
    elif typ == "join":
        ins = "ins_join"
    elif typ == "view":
        ins = "ins_view"
    else:
        ins = "ins_other"
    blocks = []
    for lang in ("en", "fa"):
        blocks.append(str(t(lang, "listing", type=tn(lang, typ) if typ in TYPES else tb(lang, "campaign_fallback"),
                            target=ctitle(c), reward=c.get("reward", 0), remaining=remaining,
                            state=K(campaign_state_key(c)),
                            instruction=t(lang, ins, window=int(c.get("window_min", 5))))))
    return blocks[1] + "\n────────\n" + blocks[0]


def campaign_listing_keyboard(c):
    if is_legacy(c):
        return [[B(bi_btn("b_legacy"), "noop")]]
    available = (c.get("status") == "active" and int(c.get("remaining", 0)) > 0
                 and not (c.get("admin_paused") and campaign_needs_admin(c)))
    if not available:
        return [[B(bi_btn("b_full"), "noop", "danger")]]
    rows = [U(bi_btn("b_target"), link_of(c), "primary")]
    if bot_deep_link(c.get("_id")):
        rows.append(B(bi_btn("b_claim"), f"rp:{c['_id']}", "success"))
    return [rows]


def publish_campaign(c):
    """Publish to the public showcase channel; a task that is not listed cannot be completed."""
    if not TASKS_CHANNEL:
        log.warning("TASKS_CHANNEL is not set; campaign %s was not published", c.get("_id"))
        return False
    r = tg("sendMessage", chat_id=TASKS_CHANNEL, text=campaign_listing_text(c), parse_mode="HTML",
           disable_web_page_preview=True, reply_markup={"inline_keyboard": campaign_listing_keyboard(c)})
    if not r.get("ok"):
        log.error("publish_campaign failed for %s: %s", c.get("_id"), r.get("description"))
        return False
    res = r.get("result", {})
    db.campaigns.update_one({"_id": c["_id"]}, {"$set": {"listing_chat_id": res.get("chat", {}).get("id"),
                                                          "listing_message_id": res.get("message_id"),
                                                          "listed_at": time.time()}})
    return True


def refresh_campaign_listing(cid):
    c = db.campaigns.find_one({"_id": cid})
    if not c or not c.get("listing_chat_id") or not c.get("listing_message_id"):
        return
    r = tg("editMessageText", chat_id=c["listing_chat_id"], message_id=c["listing_message_id"],
           text=campaign_listing_text(c), parse_mode="HTML", disable_web_page_preview=True,
           reply_markup={"inline_keyboard": campaign_listing_keyboard(c)})
    if not r.get("ok") and "not modified" not in str(r.get("description", "")).lower():
        log.warning("refresh_campaign_listing failed cid=%s: %s", cid, r.get("description"))


# ═════════════════════════════ Admin-access monitoring ═════════════════════════════
def pause_for_admin_loss(c):
    db.campaigns.update_one({"_id": c["_id"], "status": "active"},
                            {"$set": {"admin_paused": True, "admin_lost_at": time.time()}})
    refresh_campaign_listing(c["_id"])
    notify(c.get("owner"), "n_admin_lost", title=ctitle(c) or K("campaign_fallback"))


def monitor_campaign_admins(now=None):
    """Periodic admin check for join and group-reaction campaigns; an API error never changes state."""
    global _last_admin_check
    now = time.time() if now is None else now
    if now - _last_admin_check < max(15, ADMIN_MONITOR_INTERVAL):
        return
    _last_admin_check = now
    query = {"status": "active", "$and": [
        {"$or": [{"admin_required": True}, {"admin_required": {"$exists": False}}]},
        {"$or": [{"type": "join"}, {"type": "react", "mode": "auto"}]}]}
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
            notify(c.get("owner"), "n_admin_restored", title=ctitle(c) or K("campaign_fallback"))
        elif not state and not paused:
            db.campaigns.update_one({"_id": c["_id"], "status": "active"},
                                    {"$set": {"admin_paused": True, "admin_checked_at": now, "admin_lost_at": now}})
            refresh_campaign_listing(c["_id"])
            notify(c.get("owner"), "n_admin_lost", title=ctitle(c) or K("campaign_fallback"))
        else:
            db.campaigns.update_one({"_id": c["_id"], "status": "active"}, {"$set": {"admin_checked_at": now}})


# ═════════════════════════════ Tasks: earn / grant / claim ═════════════════════════════
def fresh_reaction_snapshot(chat_id, message_id, max_age=REACTION_BASELINE_MAX_AGE):
    w = db.reaction_watch.find_one({"_id": post_key(chat_id, message_id)})
    if not w or w.get("total") is None or not w.get("updated_at"):
        return None
    if time.time() - float(w["updated_at"]) > max_age:
        return None
    return w


def required_channel_member(uid):
    """Check membership in the configured task/showcase channel. None means the check failed."""
    if not TASKS_CHANNEL:
        return None
    r = tg("getChatMember", chat_id=TASKS_CHANNEL, user_id=uid)
    if not r.get("ok") or not isinstance(r.get("result"), dict):
        return None
    return member_ok(r["result"])


def show_join_required(lang, chat, mid=None):
    """Prompt the user to join the task channel, then verify membership on demand."""
    url = tasks_channel_url()
    if lang == "fa":
        msg = "<b>🔒 عضویت اجباری</b>\n\nبرای استفاده از بخش «کسب استارز رایگان»، ابتدا باید عضو کانال تاسک‌ها شوید. پس از عضویت روی «بررسی عضویت» بزنید."
        join_label, check_label = "📢 عضویت در کانال تاسک‌ها", "✅ بررسی عضویت"
    else:
        msg = "<b>🔒 Required membership</b>\n\nTo use Earn Stars, please join the task channel first. After joining, tap “Check membership”."
        join_label, check_label = "📢 Join task channel", "✅ Check membership"
    kb = []
    if url:
        kb.append([U(join_label, url, "primary")])
    kb.append([B(check_label, "earn_check", "success")])
    kb.append([B(tb(lang, "b_menu"), "home")])
    return view(chat, mid, H(_rtl(msg) if lang == "fa" else msg), kb)


def show_task(lang, chat, mid=None):
    """Earning always starts from the showcase channel, after the membership gate."""
    url = tasks_channel_url()
    if not url:
        return view(chat, mid, t(lang, "earn_unset"), [[B(tb(lang, "b_back"), "home")]])
    return view(chat, mid, t(lang, "earn"),
                [[U(tb(lang, "b_showcase"), url, "primary")], [B(tb(lang, "b_back"), "home")]])


def grant(uid, c, status, extra=None):
    """Take one spot from a campaign and record the completion. Returns (code, comp); code: ok | full | dup | limit."""
    if not counter_take(f"d:{uid}:{day()}", daily_cap(uid)):
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
        notify(c["owner"], "n_full", type=K("tn_" + c["type"]), title=ctitle(c), hold_h=f"{HOLD_HOURS:g}")
    return "ok", comp


def claim_alert(lang, code, uid=None):
    return {"limit": tb(lang, "a_limit", n=daily_cap(uid) if uid else DAILY_TASKS), "full": tb(lang, "a_full"), "dup": tb(lang, "a_dup")}[code]


def settle_instantly(comp, **extra):
    moved = db.completions.update_one({"_id": comp["_id"], "status": "pending"},
                                      {"$set": dict(status="settling", lock=time.time(), **extra)})
    if moved.modified_count:
        pay(comp, announce=False)


def post_url_of(c):
    """Public link to a campaign's post inside the showcase channel ("" when it cannot be built)."""
    mid = c.get("listing_message_id")
    if not mid:
        return ""
    name = TASKS_CHANNEL.lstrip("@")
    if re.fullmatch(r"[A-Za-z0-9_]{5,32}", name or ""):
        return f"https://t.me/{name}/{mid}"
    m = re.fullmatch(r"https?://t\.me/([A-Za-z0-9_]{5,32})/?", TASKS_CHANNEL_URL or "")
    if m:
        return f"https://t.me/{m.group(1)}/{mid}"
    chat_id = str(c.get("listing_chat_id") or TASKS_CHANNEL)
    if chat_id.startswith("-100"):
        return f"https://t.me/c/{chat_id[4:]}/{mid}"
    return ""


def latest_post_url(uid=None):
    """Link to the newest showcase post this user can still complete; falls back to the channel itself."""
    q = {"status": "active", "remaining": {"$gt": 0}, "admin_paused": {"$ne": True},
         "listing_message_id": {"$exists": True, "$ne": None}}
    for c in db.campaigns.find(q).sort("listed_at", -1).limit(25):
        if is_legacy(c):
            continue
        if uid is not None and (c.get("owner") == uid or db.completions.find_one({"uid": uid, "cid": c["_id"]})):
            continue
        url = post_url_of(c)
        if url:
            return url
    return tasks_channel_url()


def next_kb(lang, uid=None):
    """After a verified task: «Next task» opens the latest open post of the showcase channel."""
    url = latest_post_url(uid)
    nxt = U(tb(lang, "b_next"), url, "success") if url else B(tb(lang, "b_next"), "earn", "success")
    return [[nxt, B(tb(lang, "b_acc"), "acc")], [B(tb(lang, "b_back"), "earn")]]


def on_claim(cq, uid, chat, mid, cid_s):
    lang = lang_of(db.users.find_one({"_id": uid}))
    try:
        cid = ObjectId(cid_s)
    except InvalidId:
        return ans(cq, tb(lang, "a_invalid"), True)
    c = db.campaigns.find_one({"_id": cid})
    u = db.users.find_one({"_id": uid}) or {}
    if not c or c["status"] != "active" or c["remaining"] <= 0:
        ans(cq, tb(lang, "a_no_capacity"), True)
        return show_task(lang, chat, mid)
    if is_legacy(c):
        return ans(cq, tb(lang, "a_legacy"), True)
    if campaign_needs_admin(c):
        if c.get("admin_paused"):
            return ans(cq, tb(lang, "a_paused"), True)
        status = bot_admin_status(c.get("chat_id"))
        if status is None:
            return ans(cq, tb(lang, "a_admin_unavail"), True)
        if not status:
            pause_for_admin_loss(c)
            return ans(cq, tb(lang, "a_paused_now"), True)
    if u.get("strikes", 0) >= STRIKE_LIMIT or c["owner"] == uid:
        return ans(cq, tb(lang, "a_cant"), True)
    if db.completions.find_one({"uid": uid, "cid": cid}):
        ans(cq, tb(lang, "a_dup"), True)
        return show_task(lang, chat, mid)
    ses = db.sessions.find_one({"_id": f"{uid}:{cid}"})
    if not ses:
        ans(cq, tb(lang, "a_open_first"), True)
        return show_task(lang, chat, mid)
    is_count = c.get("type") == "react" and c.get("mode") == "count"
    wait = 0 if is_count else TYPES[c["type"]]["wait"] - (time.time() - ses["t"])
    if wait > 0:
        return ans(cq, tb(lang, "a_wait", s=int(wait) + 1), True)

    if c["type"] == "join":
        r = tg("getChatMember", chat_id=c["chat_id"], user_id=uid)
        if not r.get("ok"):
            return ans(cq, tb(lang, "a_check_unavail"), True)
        if not member_ok(r["result"]):
            return ans(cq, tb(lang, "a_not_member"), True)
    elif c["type"] == "react" and c.get("mode") == "auto":
        d = db.reacts.find_one({"_id": f"{c['chat_id']}:{c['mid']}:{uid}"})
        if not (d and d.get("on")):
            return ans(cq, tb(lang, "a_no_react"), True)
    elif is_count:
        now = time.time()
        window_min = int(c.get("window_min", 5))
        started = float(ses.get("task_started", float(ses.get("t", now)) - window_min * 60))
        deadline = float(ses.get("task_deadline", now))
        if now > deadline:
            return ans(cq, tb(lang, "a_window_over", window=window_min), True)
        if ses.get("baseline_total") is None or not ses.get("post_key"):
            return ans(cq, tb(lang, "a_no_baseline"), True)
        extra = {"post_key": ses["post_key"], "baseline_total": int(ses["baseline_total"]),
                 "baseline_at": float(ses.get("baseline_at", 0)), "task_started": started,
                 "task_deadline": deadline, "claim_expires": deadline, "day_key": f"d:{uid}:{day()}"}
        code, comp = grant(uid, c, "count_pending", extra=extra)
        if code != "ok":
            ans(cq, claim_alert(lang, code, uid), True)
            return show_task(lang, chat, mid)
        ans(cq, tb(lang, "a_count_pending"))
        if consume_credit_for_claim(comp):
            return view(chat, mid, H(str(t(lang, "count_ok")) + progress_line(uid, lang)), next_kb(lang, uid))
        return view(chat, mid, t(lang, "count_pending", reward=c["reward"], window=window_min,
                                 grace=REACTION_UPDATE_GRACE // 60),
                    [[U(tb(lang, "b_back_target"), link_of(c), "primary")], [B(tb(lang, "b_back"), "earn")]])

    # Join / group-reaction are verified right now, and views after the waiting time: pay immediately.
    code, comp = grant(uid, c, "pending")
    if code != "ok":
        ans(cq, claim_alert(lang, code, uid), True)
        return show_task(lang, chat, mid)
    settle_instantly(comp, **({"verified_at": time.time(), "due": time.time()} if c["type"] == "view" else {}))
    ans(cq, tb(lang, "a_paid"))
    return view(chat, mid, H(str(t(lang, "task_paid", reward=c["reward"])) + progress_line(uid, lang)), next_kb(lang, uid))


def open_linked_task(uid, chat, cid_s):
    u = db.users.find_one({"_id": uid}) or {}
    lang = lang_of(u)
    try:
        cid = ObjectId(cid_s)
    except (InvalidId, TypeError):
        return send(chat, t(lang, "e_link_invalid"), main_kb(lang, uid))
    c = db.campaigns.find_one({"_id": cid})
    if not c or c.get("status") != "active" or int(c.get("remaining", 0)) <= 0:
        return send(chat, t(lang, "e_no_capacity"), main_kb(lang, uid))
    if is_legacy(c):
        return send(chat, t(lang, "e_legacy"), main_kb(lang, uid))
    showcase = [[U(tb(lang, "b_back_showcase"), tasks_channel_url(), "primary")]] if tasks_channel_url() else main_kb(lang, uid)
    if campaign_needs_admin(c):
        if c.get("admin_paused"):
            return send(chat, t(lang, "e_paused"), showcase)
        status = bot_admin_status(c.get("chat_id"))
        if status is None:
            return send(chat, t(lang, "e_admin_unavail"), main_kb(lang, uid))
        if not status:
            pause_for_admin_loss(c)
            return send(chat, t(lang, "e_paused_now"), main_kb(lang, uid))
    if c.get("owner") == uid:
        return send(chat, t(lang, "e_own"), main_kb(lang, uid))
    if db.completions.find_one({"uid": uid, "cid": cid}):
        return send(chat, t(lang, "e_done_before"), showcase)
    if u.get("strikes", 0) >= STRIKE_LIMIT:
        return send(chat, t(lang, "e_strikes"), main_kb(lang, uid))
    if c.get("type") == "react" and c.get("mode") == "count":
        snap = fresh_reaction_snapshot(c.get("chat_id"), c.get("mid"))
        if not snap:
            return send(chat, t(lang, "e_count_stale"), main_kb(lang, uid))
        now = time.time()
        window_min = int(c.get("window_min", 5))
        ses = {"t": now, "task_started": now - window_min * 60, "task_deadline": now + REACTION_UPDATE_GRACE,
               "baseline_total": int(c.get("base_count", snap.get("total", 0))),
               "baseline_at": float(c.get("base_updated_at", snap.get("updated_at", now))),
               "post_key": post_key(c["chat_id"], c["mid"])}
    else:
        ses = {"t": time.time()}
    db.sessions.update_one({"_id": f"{uid}:{cid}"}, {"$set": ses}, upsert=True)
    reacted = tb(lang, "claim_reacted") if c.get("type") == "react" else ""
    return send(chat, t(lang, "claim_page", reacted=reacted),
                [[B(tb(lang, "b_done"), f"c:{cid}", "success")],
                 [U(tb(lang, "b_back_target"), link_of(c), "primary")]])


# ═════════════════════════════ Channel reaction verification (count-based) ═════════════════════════════
def accept_count_claim(comp):
    """Assign one unit of count increase to one claim inside its time window and pay immediately."""
    c = db.campaigns.find_one({"_id": comp["cid"]}) or {}
    if c.get("status") != "active":
        fail_count_claim(comp, "r_stopped")
        return False
    r = db.completions.find_one_and_update({"_id": comp["_id"], "status": "count_pending"},
                                           {"$set": {"status": "settling", "verified_at": time.time(),
                                                     "due": time.time(), "lock": time.time()}})
    if not r:
        return False
    db.users.update_one({"_id": r["uid"]}, {"$inc": {"hold": r["reward"]}})
    pay(r)
    c = db.campaigns.find_one({"_id": r["cid"]}) or {}
    if (c.get("status") == "active" and c.get("remaining", 1) == 0
            and db.completions.count_documents({"cid": r["cid"], "status": "count_pending"}) == 0):
        notify(c["owner"], "n_full_count", type=K("tn_react"), title=ctitle(c))
    return True


def fail_count_claim(comp, reason_key):
    """Close a claim that got no count increase, without reward, and return the spot safely."""
    r = db.completions.find_one_and_update({"_id": comp["_id"], "status": "count_pending"},
                                           {"$set": {"status": "failed", "failed_at": time.time()}})
    if not r:
        return False
    c = db.campaigns.find_one({"_id": r["cid"]}) or {}
    if c.get("status") == "active":
        db.campaigns.update_one({"_id": r["cid"], "status": "active"}, {"$inc": {"remaining": 1}})
        refresh_campaign_listing(r["cid"])
    elif c.get("cost"):
        add_points(r["owner"], c["cost"], "refund", r["cid"])
    if r.get("day_key"):
        db.counters.update_one({"_id": r["day_key"]}, {"$inc": {"n": -1}})
    notify(r["uid"], "n_claim_failed", reason=K(reason_key))
    # The user may retry the same campaign; the failure stays in the log for debugging.
    db.reaction_claim_logs.insert_one({"uid": r["uid"], "cid": r["cid"], "post_key": r.get("post_key"),
                                       "status": "failed", "reason": reason_key, "t": time.time()})
    db.completions.delete_one({"_id": r["_id"], "status": "failed"})
    return True


def consume_credit_for_claim(comp):
    """Consume one unit of a count-increase credit; each increase verifies at most its real number of units."""
    c = db.campaigns.find_one({"_id": comp["cid"]}) or {}
    if c.get("status") != "active":
        fail_count_claim(comp, "r_paused")
        return False
    # A per-user baseline taken when the user returns would already include their reaction, so unused aggregate
    # credits from the eligible window are used. This stays a count-based estimate: it cannot prove who reacted.
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
            # another worker consumed this unit; re-read the credit
    return False


def invalidate_count_credits(key, decrease):
    """When the count drops, neutralise unused increase credits by the same amount."""
    remaining = int(decrease)
    if remaining <= 0:
        return
    for credit in db.reaction_credits.find({"post_key": key}).sort([("event_at", -1)]):
        used, total_credit = int(credit.get("used", 0)), int(credit.get("delta", 0))
        cut = min(max(0, total_credit - used), remaining)
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
            credit_query = {"post_key": key, "event_at": {"$gte": float(comp.get("task_started", 0)),
                                                          "$lte": float(comp.get("task_deadline", 0))}}
            if not any(int(x.get("used", 0)) < int(x.get("delta", 0)) for x in db.reaction_credits.find(credit_query)):
                break


def expire_count_claims(now):
    for comp in db.completions.find({"status": "count_pending", "claim_expires": {"$lte": now}}).limit(200):
        fail_count_claim(comp, "r_no_increase")
    # Credits are kept for one day only (review + double-spend protection) to bound database size.
    db.reaction_credits.delete_many({"received_at": {"$lt": now - 86400}})
    db.reaction_events.delete_many({"received_at": {"$lt": now - 14 * 86400}})


def on_reaction_count(mr, update_id=None):
    """Store the channel's aggregate count snapshot and map each unit of increase to at most one claim."""
    chat = mr.get("chat") or {}
    chat_id, mid = chat.get("id"), mr.get("message_id")
    if chat_id is None or mid is None:
        return
    key = post_key(chat_id, mid)
    received_at = time.time()
    event_at = float(mr.get("date") or received_at)
    old = db.reaction_watch.find_one({"_id": key})
    if not old:
        # Only posts that someone created a campaign for are stored; never every channel.
        if not db.campaigns.count_documents({"chat_id": chat_id, "mid": int(mid), "type": "react", "mode": "count"}):
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
            label = str(typ.get("emoji") or "?")
        elif kind == "custom_emoji":
            label = "custom"
        elif kind == "paid":
            label = "paid"
        else:
            label = str(kind or "?")
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
    db.reaction_watch.update_one({"_id": key}, {"$set": set_fields, "$setOnInsert": {"created_at": received_at}}, upsert=True)

    if old_total is not None and total != old_total:
        delta = total - old_total
        if delta > 0:
            credit_id = int(update_id) if update_id is not None else f"{key}:{int(event_at)}:{random.randint(1000, 9999)}"
            try:
                db.reaction_credits.insert_one({"_id": credit_id, "post_key": key, "old_total": old_total,
                                                "new_total": total, "delta": delta, "used": 0,
                                                "event_at": event_at, "received_at": received_at})
            except DuplicateKeyError:
                pass
            process_count_claims(key, event_at, old_total, total, delta)
        else:
            invalidate_count_credits(key, -delta)

    # If the campaign owner was waiting for the first snapshot, move them on to choosing the time limit.
    latest = db.reaction_watch.find_one({"_id": key}) or {}
    owner = latest.get("calibrating_owner")
    if owner:
        st = db.states.find_one({"_id": int(owner)})
        if st and st.get("step") == "count_baseline":
            d = st.get("d", {})
            db.reaction_watch.update_one({"_id": key, "calibrating_owner": owner},
                                         {"$unset": {"calibrating_owner": "", "calibrating_at": ""}})
            set_state(int(owner), "window", d)
            notify(int(owner), "window_received", kb=window_kb, total=total)


def on_reaction(mr):
    """Per-user reactions in groups where the bot is an administrator (anonymous users/channels have no such update)."""
    u = mr.get("user")
    if not u:
        return
    db.reacts.update_one({"_id": f"{mr['chat']['id']}:{mr['message_id']}:{u['id']}"},
                         {"$set": {"on": bool(mr.get("new_reaction")), "t": time.time()}}, upsert=True)


def window_kb(lang):
    return [[B(tb(lang, "b_win2"), "rw:2", "primary"), B(tb(lang, "b_win5"), "rw:5", "primary")],
            [B(tb(lang, "b_cancel"), "home", "danger")]]


def reaction_watch_text(uid, lang, all_admin=False):
    query = {} if all_admin else {"$or": [{"owner": uid}, {"owners": uid}]}
    watches = list(db.reaction_watch.find(query, sort=[("updated_at", -1)], limit=8))
    if not watches:
        return t(lang, "rw_empty")
    now = time.time()
    rows = []
    for w in watches:
        total = w.get("total")
        upd = w.get("updated_at")
        if upd:
            age = max(0, int(now - upd))
            age_text = tb(lang, "rw_age", m=age // 60, s=age % 60)
            delay = max(0, int(upd - w.get("event_at", upd)))
            delay_text = tb(lang, "rw_delay", d=delay)
        else:
            age_text, delay_text = tb(lang, "rw_age_none"), tb(lang, "unknown")
        extra = ""
        if w.get("reactions"):
            labels = {"custom": tb(lang, "lbl_custom"), "paid": tb(lang, "lbl_paid")}
            lst = ", ".join(f"{labels.get(x['label'], x['label'])} {x['count']}" for x in w["reactions"][:5])
            extra += str(t(lang, "rw_top", list=lst))
        camps = list(db.campaigns.find({"type": "react", "mode": "count", "chat_id": w.get("chat_id"),
                                        "mid": w.get("mid"), "status": "active"},
                                       {"target": 1, "remaining": 1, "base_count": 1, "window_min": 1}))
        if camps:
            c = camps[-1]
            base = c.get("base_count")
            delta = ""
            if total is not None and base is not None:
                delta = tb(lang, "rw_delta", delta=f"{int(total) - int(base):+d}")
            extra += str(t(lang, "rw_camp", target=c.get("target", 0), remaining=c.get("remaining", 0),
                           window=c.get("window_min", 5), delta=delta))
        title = short(w.get("title") or w.get("uname") or str(w.get("chat_id")), 40)
        rows.append(str(t(lang, "rw_row", title=title, mid=w.get("mid", "?"),
                          total=total if total is not None else tb(lang, "unknown"),
                          age=age_text, delay=delay_text, extra=H(extra))))
    return t(lang, "rw_head", rows=H("\n\n".join(rows)))


# ═════════════════════════════ Referrals (two-stage reward) ═════════════════════════════
def ref_register(uid, ref_s, name=""):
    """Register the referral for a brand-new user and pay the inviter the small join bonus (daily cap applies)."""
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
    if REF_WELCOME_BONUS:                          # two-sided reward: the invited friend also gets a welcome bonus
        add_points(uid, REF_WELCOME_BONUS, "ref_welcome", ref)
        db.users.update_one({"_id": uid}, {"$set": {"ref_welcome": REF_WELCOME_BONUS}})
    if db.users.count_documents({"referred_by": ref, "joined": {"$gte": time.time() - 3600}}) > REF_HOURLY_MAX:
        db.users.update_one({"_id": uid}, {"$set": {"ref_suspect": True}})    # burst of joins from one link: no join bonus
        return
    if not REF_JOIN_BONUS or not counter_take(f"refd:{ref}:{day()}", REF_DAILY_CAP):
        return
    add_points(ref, REF_JOIN_BONUS, "ref_join", uid)
    db.users.update_one({"_id": ref}, {"$inc": {"ref_earned": REF_JOIN_BONUS}})
    notify(ref, "n_ref_join", kb=lambda lang: [[B(tb(lang, "b_ref"), "ref")]],
           name=short(name, 18) or K("user_fallback"), bonus=REF_JOIN_BONUS,
           active_bonus=REF_ACTIVE_BONUS, tasks=REF_ACTIVE_TASKS)


def ref_on_task_paid(uid, typ):
    """Each paid join/reaction task moves an invited user one step closer to 'active' (views do not count)."""
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
    if not won.modified_count:      # a concurrent worker already activated it; never pay twice
        return
    ref = u.get("referred_by")
    if not ref or not REF_ACTIVE_BONUS:
        return
    db.users.update_one({"_id": ref}, {"$inc": {"ref_active_count": 1, "ref_earned": REF_ACTIVE_BONUS}})
    add_points(ref, REF_ACTIVE_BONUS, "ref_active", uid)
    try:
        wk_add(ref, refs=1)
        check_achievements(ref)
    except Exception:
        log.exception("ref growth hooks")
    notify(ref, "n_ref_active", kb=lambda lang: [[B(tb(lang, "b_ref"), "ref")]],
           name=short(u.get("name"), 18) or K("user_fallback"), tasks=REF_ACTIVE_TASKS, bonus=REF_ACTIVE_BONUS)


# ═════════════════════════════ Reversal / payout / review ═════════════════════════════
def undo(comp, had_hold, strike, why_key):
    """Reverse a completion: drop pending coins, reopen the spot (or refund the owner if the campaign was cancelled)."""
    inc = {"hold": -comp["reward"]} if had_hold else {}
    if strike:
        inc["strikes"] = 1
    if inc:
        db.users.update_one({"_id": comp["uid"]}, {"$inc": inc})
    r = db.campaigns.update_one({"_id": comp["cid"], "status": "active"}, {"$inc": {"remaining": 1}})
    if r.matched_count:
        refresh_campaign_listing(comp["cid"])
    else:
        c = db.campaigns.find_one({"_id": comp["cid"]}) or {}
        if c.get("cost"):
            add_points(comp["owner"], c["cost"], "refund", comp["cid"])
    notify(comp["uid"], "n_reversed", why=K(why_key),
           strike=(lambda lang: t(lang, "n_strike", limit=STRIKE_LIMIT)) if strike else H(""))


def pay(comp, announce=True):
    r = db.completions.find_one_and_update({"_id": comp["_id"], "status": "settling"},
                                           {"$set": {"status": "paid", "paid": time.time()}})
    if not r:
        return
    db.users.update_one({"_id": comp["uid"]}, {"$inc": {"hold": -comp["reward"], "points": comp["reward"],
                                                         "earned": comp["reward"]}})
    ledger(comp["uid"], comp["reward"], "task", comp["cid"])
    try:
        gamify_on_paid(comp)                      # golden hour, level-ups, weekly score, achievements
    except Exception:
        log.exception("gamify_on_paid")
    if announce:
        lg = ulang(comp["uid"])
        send(comp["uid"], H(str(t(lg, "n_paid", reward=comp["reward"])) + progress_line(comp["uid"], lg)), next_kb(lg, comp["uid"]))
    try:
        ref_on_task_paid(comp["uid"], comp.get("type"))
    except Exception:
        log.exception("ref_on_task_paid")


def settle(comp):
    """End of the review period: re-check, then pay or reverse."""
    c = db.campaigns.find_one({"_id": comp["cid"]}) or {}
    ok = True
    if campaign_needs_admin(c):
        if c.get("admin_paused"):
            raise RuntimeError("campaign paused: bot admin access missing")
        status = bot_admin_status(c.get("chat_id"))
        if status is None:
            raise RuntimeError("temporary API error while checking bot admin status")
        if not status:
            pause_for_admin_loss(c)
            raise RuntimeError("campaign paused: bot admin access missing")
    if comp["type"] == "join":
        r = tg("getChatMember", chat_id=c.get("chat_id"), user_id=comp["uid"])
        if not r.get("ok"):
            raise RuntimeError("temporary API error while checking user membership")
        ok = member_ok(r["result"])
    elif comp["type"] == "react" and c.get("mode") == "auto":
        d = db.reacts.find_one({"_id": f"{c.get('chat_id')}:{c.get('mid')}:{comp['uid']}"})
        ok = bool(d and d.get("on"))
    if ok:
        return pay(comp)
    r = db.completions.find_one_and_update({"_id": comp["_id"], "status": "settling"}, {"$set": {"status": "reversed"}})
    if r:
        undo(comp, True, True, "r_left")


_last = {"clean": 0}


def tick():
    now = time.time()
    monitor_campaign_admins(now)
    expire_count_claims(now)
    db.completions.update_many({"status": "settling", "lock": {"$lt": now - 600}}, {"$set": {"status": "pending"}})
    while True:
        comp = db.completions.find_one_and_update({"status": "pending", "due": {"$lte": now}},
                                                  {"$set": {"status": "settling", "lock": now}})
        if not comp:
            break
        try:
            settle(comp)
        except Exception:
            log.exception("settle")
            db.completions.update_one({"_id": comp["_id"], "status": "settling"},
                                      {"$set": {"status": "pending", "due": time.time() + 60}})
            break
    if now - _last["clean"] > 3600:
        _last["clean"] = now
        db.reacts.delete_many({"t": {"$lt": now - 40 * 86400}})
        db.sessions.delete_many({"t": {"$lt": now - 7 * 86400}})


# ═════════════════════════════ Create campaign (multi-step) ═════════════════════════════
POST_RE = re.compile(r"(?:https?://)?(?:t\.me|telegram\.me)/([A-Za-z][A-Za-z0-9_]{3,31})/(\d+)", re.I)
CHAN_RE = re.compile(r"^(?:https?://)?(?:(?:t\.me|telegram\.me)/)?@?([A-Za-z][A-Za-z0-9_]{3,31})/?$", re.I)
FA_DIGITS = str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789")   # accept Persian/Arabic digits as input


def needs_admin_mode(typ, mode):
    return typ == "join" or (typ == "react" and mode == "auto")


def start_new(cq, lang, chat, mid):
    ans(cq)
    rows = [[B(tb(lang, "type_btn", name=tn(lang, k), cost=v["cost"]), f"nt:{k}")] for k, v in TYPES.items()]
    view(chat, mid, t(lang, "new"), rows + [[B(tb(lang, "b_back"), "campaigns")]])


def pick_type(cq, uid, lang, chat, mid, typ):
    if typ not in TYPES:
        return ans(cq)
    if db.campaigns.count_documents({"owner": uid, "status": "active"}) >= MAX_ACTIVE:
        return ans(cq, tb(lang, "a_max_active", n=MAX_ACTIVE), True)
    ans(cq)
    set_state(uid, "link", {"type": typ})
    key = "prompt_join" if typ == "join" else "prompt_post"
    view(chat, mid, t(lang, key, bot=_me.get("username", "")), cancel_kb(lang))


def ask_qty(chat, lang, d, cost):
    u = db.users.find_one({"_id": chat}) or {}
    mx = min(MAX_QTY, u.get("points", 0) // cost)
    if mx < 1:
        return send(chat, t(lang, "qty_insufficient", points=u.get("points", 0), cost=cost),
                    [[B(tb(lang, "b_earn"), "earn", "primary")], [B(tb(lang, "b_ref"), "ref", "success")]])
    opts = list(dict.fromkeys([n for n in (10, 25, 50) if n < mx] + [mx]))
    quick = [B(tb(lang, "b_max", n=n) if n == mx else str(n), f"q:{n}", "primary" if n == mx else None) for n in opts]
    send(chat, t(lang, "qty_prompt", max=mx), [quick] + cancel_kb(lang))
    return True


def on_state_text(uid, chat, text, st, lang):
    if time.time() - st.get("t", 0) > 3600:
        db.states.delete_one({"_id": uid})
        return send(chat, t(lang, "e_expired"), main_kb(lang, uid))
    step, d = st["step"], st.get("d", {})
    typ = d.get("type")
    cost = TYPES[typ]["cost"] if typ else 0

    if step == "link":
        if typ == "join":
            m = CHAN_RE.match(text.strip())
            uname, post_id = (m[1], None) if m else (None, None)
        else:
            m = POST_RE.search(text)
            uname, post_id = (m[1], int(m[2])) if m else (None, None)
        if not uname:
            return send(chat, t(lang, "err_link_join" if typ == "join" else "err_link_post"), cancel_kb(lang))
        ch = resolve_chat(uname)
        if not ch:
            return send(chat, t(lang, "err_not_found"), cancel_kb(lang))
        d.update(uname=ch.get("username") or uname, chat_id=ch["id"], title=(ch.get("title") or uname)[:60],
                 mid=post_id, mode=None)
        bot_name = _me.get("username", "")
        if typ == "join" and not bot_is_admin(ch["id"]):
            return send(chat, t(lang, "err_not_admin", bot=bot_name, title=d["title"]), cancel_kb(lang))
        if typ == "react":
            if ch["type"] in ("group", "supergroup") and bot_is_admin(ch["id"]):
                d["mode"] = "auto"
            elif ch["type"] == "channel":
                if not bot_is_admin(ch["id"]):
                    return send(chat, t(lang, "err_count_admin", title=d["title"]), cancel_kb(lang))
                d["mode"] = "count"
                key = post_key(d["chat_id"], d["mid"])
                db.reaction_watch.update_one({"_id": key}, {
                    "$set": {"chat_id": d["chat_id"], "mid": d["mid"], "uname": d["uname"], "title": d["title"],
                             "owner": uid, "registered_at": time.time()},
                    "$addToSet": {"owners": uid}}, upsert=True)
                snap = fresh_reaction_snapshot(d["chat_id"], d["mid"])
                if not snap:
                    db.reaction_watch.update_one({"_id": key}, {"$set": {"calibrating_owner": uid,
                                                                          "calibrating_at": time.time()}})
                    set_state(uid, "count_baseline", d)
                    return send(chat, t(lang, "baseline_wait"), cancel_kb(lang))
                set_state(uid, "window", d)
                return send(chat, t(lang, "window_ready", total=snap.get("total", 0)), window_kb(lang))
            else:
                return send(chat, t(lang, "err_react_dest"), cancel_kb(lang))
        set_state(uid, "qty", d)
        return ask_qty(uid, lang, d, cost)

    if step == "count_baseline":
        return send(chat, t(lang, "baseline_pending"), cancel_kb(lang))
    if step == "window":
        return send(chat, t(lang, "window_hint"), window_kb(lang))

    if step == "qty":
        try:
            q = int(text.translate(FA_DIGITS).strip())
        except ValueError:
            return send(chat, t(lang, "qty_nan"))
        u = db.users.find_one({"_id": uid}) or {}
        mx = min(MAX_QTY, u.get("points", 0) // cost)
        if not 1 <= q <= mx:
            return send(chat, t(lang, "qty_range", max=mx))
        d["qty"] = q
        set_state(uid, "confirm", d)
        if typ == "join":
            how = tb(lang, "how_join")
        elif typ == "view":
            how = tb(lang, "how_view")
        elif d.get("mode") == "auto":
            how = tb(lang, "how_react_auto")
        else:
            how = tb(lang, "how_react_count", window=d.get("window_min", 5))
        admin_note = t(lang, "summary_admin") if needs_admin_mode(typ, d.get("mode")) else H("")
        return send(chat, t(lang, "summary", type=K("tn_" + typ), title=d["title"], qty=q, total=q * cost,
                            how=how, admin_note=admin_note),
                    [[B(tb(lang, "b_confirm", cost=q * cost), "ok", "success")],
                     [B(tb(lang, "b_cancel"), "home", "danger")]])
    send(chat, t(lang, "e_continue_menu"), main_kb(lang, uid))


def choose_reaction_window(cq, uid, lang, chat, mid, value):
    st = db.states.find_one({"_id": uid})
    if not st or st.get("step") != "window":
        ans(cq, tb(lang, "a_step_expired"), True)
        return view(chat, mid, t(lang, "e_restart"), main_kb(lang, uid))
    try:
        window_min = int(value)
    except (TypeError, ValueError):
        return ans(cq, tb(lang, "a_window_bad"), True)
    if window_min not in (2, 5):
        return ans(cq, tb(lang, "a_window_only"), True)
    d = st.get("d", {})
    if not fresh_reaction_snapshot(d.get("chat_id"), d.get("mid")):
        ans(cq, tb(lang, "a_count_stale"), True)
        return view(chat, mid, t(lang, "baseline_need_action"), cancel_kb(lang))
    d["window_min"] = window_min
    set_state(uid, "qty", d)
    ans(cq, tb(lang, "a_window_set", window=window_min))
    return ask_qty(uid, lang, d, TYPES[d["type"]]["cost"])


def confirm_new(cq, uid, lang, chat, mid):
    st = db.states.find_one({"_id": uid})
    if not st or st.get("step") != "confirm":
        ans(cq, tb(lang, "a_step_expired"), True)
        return view(chat, mid, t(lang, "e_restart"), main_kb(lang, uid))
    d, typ = st["d"], st["d"]["type"]
    cfg = TYPES[typ]
    snap = None
    if typ == "react" and d.get("mode") == "count":
        snap = fresh_reaction_snapshot(d.get("chat_id"), d.get("mid"))
        if not snap:
            ans(cq, tb(lang, "a_baseline_stale"), True)
            db.states.delete_one({"_id": uid})
            return view(chat, mid, t(lang, "baseline_restart"), main_kb(lang, uid))
    if db.campaigns.count_documents({"owner": uid, "status": "active"}) >= MAX_ACTIVE:
        return ans(cq, tb(lang, "a_max_active", n=MAX_ACTIVE), True)
    # Final access check right before charging; an API error is never treated as approval.
    admin_required = needs_admin_mode(typ, d.get("mode"))
    if admin_required:
        status = bot_admin_status(d.get("chat_id"))
        if status is None:
            return ans(cq, tb(lang, "a_admin_check_pre"), True)
        if not status:
            return ans(cq, tb(lang, "a_admin_needed"), True)
    total_cost = d["qty"] * cfg["cost"]
    if not spend(uid, total_cost, "campaign", d["uname"]):
        return ans(cq, tb(lang, "a_no_funds"), True)
    db.states.delete_one({"_id": uid})
    campaign = {"owner": uid, "type": typ, "chat_id": d["chat_id"], "uname": d["uname"], "title": d["title"],
                "mid": d.get("mid"), "mode": d.get("mode"), "target": d["qty"], "remaining": d["qty"],
                "cost": cfg["cost"], "reward": cfg["reward"], "status": "active", "created": time.time(),
                "admin_required": admin_required, "admin_paused": False,
                "admin_checked_at": time.time() if admin_required else None}
    if typ == "react" and d.get("mode") == "count":
        campaign.update(window_min=int(d.get("window_min", 5)), base_count=int(snap.get("total", 0)),
                        base_updated_at=float(snap.get("updated_at", time.time())),
                        post_key=post_key(d["chat_id"], d["mid"]))
    campaign["_id"] = db.campaigns.insert_one(campaign).inserted_id
    if not publish_campaign(campaign):
        db.campaigns.update_one({"_id": campaign["_id"], "status": "active"}, {"$set": {"status": "cancelled"}})
        add_points(uid, total_cost, "refund", campaign["_id"])
        ans(cq, tb(lang, "a_showcase_down"), True)
        return view(chat, mid, t(lang, "publish_failed"),
                    [[B(tb(lang, "b_mine"), "my")], [B(tb(lang, "b_back"), "campaigns")]])
    if typ == "join":
        db.users.update_one({"_id": uid}, {"$set": {"last_join": d["uname"]}})
    db.users.update_one({"_id": uid}, {"$inc": {"campaigns_made": 1}})
    try:
        check_achievements(uid)
    except Exception:
        log.exception("check_achievements")
    ans(cq, tb(lang, "a_published"))
    kb = []
    if tasks_channel_url():
        kb.append([U(tb(lang, "b_view_showcase"), tasks_channel_url(), "primary")])
    kb += [[B(tb(lang, "b_mine"), "my")], [B(tb(lang, "b_back"), "campaigns")]]
    view(chat, mid, t(lang, "published"), kb)


# ═════════════════════════════ My campaigns ═════════════════════════════
def render_my(uid, lang, chat, mid):
    cs = list(db.campaigns.find({"owner": uid}, sort=[("created", -1)], limit=10))
    if not cs:
        return view(chat, mid, t(lang, "my_empty"),
                    [[B(tb(lang, "b_new"), "new", "primary")], [B(tb(lang, "b_back"), "campaigns")]])
    rows, kb = [], []
    for i, c in enumerate(cs, 1):
        pending = 0
        if c.get("type") == "react" and c.get("mode") == "count":
            done = db.completions.count_documents({"cid": c["_id"], "status": "paid"})
            pending = db.completions.count_documents({"cid": c["_id"], "status": "count_pending"})
        else:
            done = c["target"] - c["remaining"]
        if c["status"] != "active":
            state = "st_cancelled"
        elif pending:
            state = "st_waiting"
        elif c["remaining"] == 0:
            state = "st_done"
        else:
            state = "st_active"
        rows.append(str(t(lang, "my_row", n=i, type=tn(lang, c["type"]), title=ctitle(c), bar=bar(done, c["target"]),
                          done=done, target=c["target"], state=K(state),
                          pending=tb(lang, "my_pending", p=pending) if pending else "")))
        if c["status"] == "active" and c["remaining"] > 0:
            kb.append([B(tb(lang, "b_cancel_n", n=i), f"x:{c['_id']}", "danger")])
    if db.reaction_watch.count_documents({"$or": [{"owner": uid}, {"owners": uid}]}):
        kb.append([B(tb(lang, "b_watch"), "rwatch")])
    view(chat, mid, t(lang, "my", rows=H("\n\n".join(rows))), kb + [[B(tb(lang, "b_back"), "campaigns")]])


def cancel_campaign(cq, uid, lang, chat, mid, cid_s, sure):
    try:
        cid = ObjectId(cid_s)
    except InvalidId:
        return ans(cq)
    if not sure:
        ans(cq)
        return view(chat, mid, t(lang, "cancel_ask"),
                    [[B(tb(lang, "b_yes_cancel"), f"xy:{cid_s}", "danger")], [B(tb(lang, "b_back"), "my")]])
    r = db.campaigns.find_one_and_update({"_id": cid, "owner": uid, "status": "active"}, {"$set": {"status": "cancelled"}})
    if not r:
        ans(cq, tb(lang, "a_cancel_inactive"), True)
    else:
        refund = r["remaining"] * r["cost"]
        if refund:
            add_points(uid, refund, "refund", cid)
        refresh_campaign_listing(cid)
        ans(cq, tb(lang, "a_cancelled", n=refund), True)
    render_my(uid, lang, chat, mid)


# ═════════════════════════════ Admin commands ═════════════════════════════
def admin_cmd(uid, chat, cmd, args, lang):
    if uid not in ADMINS:
        return send(chat, admin_text(lang, "Access denied.", "دسترسی مجاز نیست."))
    if cmd in ("admin", "panel"):
        return admin_home(chat, lang)
    if cmd == "reactwatch":
        return send(chat, reaction_watch_text(uid, lang, all_admin=True))
    if cmd in ("broadcast", "bc"):
        return admin_bc_home(chat, lang)
    if cmd == "stats":
        by = {s: db.completions.count_documents({"status": s}) for s in ("pending", "paid", "reversed")}
        return send(chat, t(lang, "ad_stats", users=db.users.count_documents({}),
                            active=db.campaigns.count_documents({"status": "active"}),
                            pending=by["pending"], paid=by["paid"], rev=by["reversed"],
                            refs=db.users.count_documents({"referred_by": {"$exists": True}}),
                            refs_active=db.users.count_documents({"ref_active": True})))
    if cmd in ("users", "user"):
        if cmd == "user" and args:
            return admin_user_detail(chat, lang, args[0])
        return admin_users_screen(chat, lang)
    if cmd in ("events", "activity"):
        return admin_events_screen(chat, lang)
    if cmd == "give" and len(args) == 2 and args[0].isdigit() and args[1].lstrip("+-").isdigit():
        delta = int(args[1])
        target_uid = int(args[0])
        ok = db.users.find_one({"_id": target_uid}, {"_id": 1}) is not None and admin_apply_point_delta(uid, target_uid, delta)
        return send(chat, admin_text(lang, "Point balance updated." if ok else "Invalid amount or insufficient balance.",
                                     "موجودی امتیاز به‌روزرسانی شد." if ok else "مقدار نامعتبر یا موجودی ناکافی است."))
    if cmd == "unban" and args and args[0].isdigit():
        db.users.update_one({"_id": int(args[0])}, {"$set": {"strikes": 0, "banned": False}, "$unset": {"ban_reason": ""}})
        return send(chat, t(lang, "ad_unbanned"))
    if cmd == "ban" and args and args[0].isdigit():
        target = int(args[0])
        if target in ADMINS:
            return send(chat, admin_text(lang, "Admin accounts cannot be banned from this panel.", "از داخل پنل نمی‌توان حساب مدیر را مسدود کرد."))
        db.users.update_one({"_id": target}, {"$set": {"banned": True, "ban_reason": f"admin:{uid}", "banned_at": time.time()}})
        return send(chat, admin_text(lang, "User blocked.", "کاربر مسدود شد."))
    if cmd == "stop" and args:
        try:
            r = db.campaigns.find_one_and_update({"_id": ObjectId(args[0]), "status": "active"}, {"$set": {"status": "cancelled"}})
        except InvalidId:
            r = None
        if r:
            add_points(r["owner"], r["remaining"] * r["cost"], "refund", r["_id"])
            refresh_campaign_listing(r["_id"])
        return send(chat, t(lang, "ad_stopped" if r else "ad_not_found"))
    return send(chat, admin_text(lang,
        "<b>Admin panel</b>\n\n<code>/admin</code> · open panel\n<code>/stats</code> · statistics\n<code>/users</code> · user list\n<code>/user ID</code> · user profile\n<code>/events</code> · recent interactions\n<code>/give ID +/-N</code> · adjust points\n<code>/ban ID</code> · block user\n<code>/unban ID</code> · unblock/reset strikes\n<code>/stop CAMPAIGN_ID</code> · stop campaign\n<code>/reactwatch</code> · reaction diagnostics",
        "<b>پنل مدیریت</b>\n\n<code>/admin</code> · پنل\n<code>/stats</code> · آمار\n<code>/users</code> · فهرست کاربران\n<code>/user ID</code> · مشخصات کاربر\n<code>/events</code> · تعاملات اخیر\n<code>/give ID +/-N</code> · تغییر امتیاز\n<code>/ban ID</code> · مسدودسازی\n<code>/unban ID</code> · رفع مسدودی/صفرکردن اخطار\n<code>/stop CAMPAIGN_ID</code> · توقف کمپین\n<code>/reactwatch</code> · عیب‌یابی واکنش‌ها"))

# ═════════════════════════════ Growth engine (v52) ═════════════════════════════
# Daily gift + streak · lucky wheel · levels · achievements/referral ladder · weekly leaderboard with prizes
# smart reminders · golden hour · onboarding · admin broadcast queue · auto channel posts · growth analytics
def _env_ints(name, default):
    vals = [int(x) for x in re.findall(r"\d+", os.environ.get(name, default))]
    return vals or [int(x) for x in re.findall(r"\d+", default)]


def _env_window(name, default):
    m = re.fullmatch(r"\s*(\d{1,2})\s*-\s*(\d{1,2})\s*", os.environ.get(name, default) or "")
    return (int(m[1]) % 24, int(m[2]) % 24) if m else None


DAILY_REWARDS = _env_ints("DAILY_REWARDS", "1,1,2,2,3,3,5")            # reward per streak day (cycles every len())
WEEKLY_PRIZES = _env_ints("WEEKLY_PRIZES", "30,20,10")                  # Stars for ranks 1..n on each weekly board
REF_WELCOME_BONUS = int(os.environ.get("REF_WELCOME_BONUS", "5"))       # extra Stars for a friend who joins by invite
REF_HOURLY_MAX = int(os.environ.get("REF_HOURLY_MAX", "12"))            # more invites than this per hour = suspicious
GOLDEN_WINDOW = _env_window("GOLDEN_HOUR", "")                          # e.g. "17-19" (UTC); empty = disabled
GOLDEN_MULT = max(1, int(os.environ.get("GOLDEN_MULT", "2")))
REMIND_ON = os.environ.get("REMINDERS", "1") != "0"
REMIND_WINDOW = _env_window("REMIND_HOURS", "5-18") or (5, 18)          # UTC hours in which reminders may be sent
REPORT_HOUR = int(os.environ.get("DAILY_REPORT_HOUR", "17"))            # UTC hour of the daily channel report
STREAK_MILESTONES = {7: 3, 14: 6, 30: 15, 60: 30, 100: 60}

# need (verified tasks), icon, English, فارسی, one-time level-up bonus, gift multiplier, extra daily task cap
LEVELS = [
    (0,   "🥉", "Bronze",  "برنزی",     0,   1.0, 0),
    (25,  "🥈", "Silver",  "نقره‌ای",   5,   1.2, 10),
    (100, "🥇", "Gold",    "طلایی",     15,  1.4, 20),
    (300, "💎", "Diamond", "الماس",     40,  1.7, 40),
    (800, "👑", "Legend",  "افسانه‌ای", 100, 2.0, 80),
]
# key, icon, English, فارسی, reward, user field, needed value
ACH = [
    ("t1",   "🚀", "First task",          "اولین تسک",           2,   "done_tasks", 1),
    ("t10",  "⭐", "10 tasks",            "10 تسک",              3,   "done_tasks", 10),
    ("t50",  "🌟", "50 tasks",            "50 تسک",              5,   "done_tasks", 50),
    ("t250", "💫", "250 tasks",           "250 تسک",             15,  "done_tasks", 250),
    ("t1000", "🏆", "1000 tasks",         "1000 تسک",            50,  "done_tasks", 1000),
    ("r1",   "🤝", "1 active friend",     "1 دوست فعال",         3,   "ref_active_count", 1),
    ("r5",   "👥", "5 active friends",    "5 دوست فعال",         10,  "ref_active_count", 5),
    ("r25",  "🚀", "25 active friends",   "25 دوست فعال",        40,  "ref_active_count", 25),
    ("r100", "👑", "100 active friends",  "100 دوست فعال",       150, "ref_active_count", 100),
    ("s3",   "🔥", "3-day streak",        "استریک 3 روزه",       2,   "streak_best", 3),
    ("s7",   "🔥", "7-day streak",        "استریک 7 روزه",       5,   "streak_best", 7),
    ("s30",  "🌋", "30-day streak",       "استریک 30 روزه",      25,  "streak_best", 30),
    ("c1",   "📣", "First campaign",      "اولین کمپین",         3,   "campaigns_made", 1),
    ("c10",  "📢", "10 campaigns",        "10 کمپین",            10,  "campaigns_made", 10),
]
WHEEL = [(1, 40), (2, 30), (3, 15), (4, 8), (6, 4), (10, 2), (20, 1)]      # (Stars, weight) — average ≈ 2.4

db.weekly.create_index([("wk", 1), ("score", -1)])
db.weekly.create_index([("wk", 1), ("refs", -1)])
db.users.create_index("last_seen")
db.users.create_index("joined")
db.users.create_index([("earned", -1)])
db.broadcasts.create_index("status")

S.update({
    # ───────── Buttons ─────────
    "b_gift": ("🎁 Daily gift", "🎁 هدیه‌ی روزانه"),
    "b_gift_ready": ("🎁 Daily gift ✨", "🎁 هدیه‌ی روزانه ✨"),
    "b_top": ("🏆 Leaderboard", "🏆 جدول برترین‌ها"),
    "b_ach": ("🏅 Achievements", "🏅 دستاوردها"),
    "b_wheel": ("🎰 Lucky wheel", "🎰 گردونه‌ی شانس"),
    "b_spin": ("🎰 Spin now", "🎰 چرخاندن"),
    "b_collect": ("🎁 Collect gift", "🎁 دریافت هدیه"),
    "b_rm_on": ("🔔 Reminders: on", "🔔 یادآور: روشن"),
    "b_rm_off": ("🔕 Reminders: off", "🔕 یادآور: خاموش"),
    "b_rm_stop": ("🔕 Stop reminders", "🔕 خاموش کردن یادآور"),
    "b_repeat": ("🔁 Repeat: @{name}", "🔁 تکرار: @{name}"),
    "b_max": ("Max {n}", "حداکثر {n}"),
    "b_earners": ("⭐ Earners", "⭐ درآمدسازها"),
    "b_inviters": ("👥 Inviters", "👥 دعوت‌کننده‌ها"),
    "b_check": ("✅ Check membership", "✅ بررسی عضویت"),
    "b_join_channel": ("📢 Join task channel", "📢 عضویت در کانال تاسک‌ها"),
    "b_open_bot": ("🚀 Open the bot", "🚀 ورود به ربات"),

    # ───────── Daily gift ─────────
    "g_gift_ok": (
        "<b>🎁 Daily gift collected!</b>\n<blockquote><b>Reward:</b> <code>+{reward}</code> ⭐ Stars{bonus}\n"
        "<b>Streak:</b> 🔥 <code>{streak}</code> day(s)</blockquote>\n{cal}\n"
        "<i>Tomorrow: <code>+{next}</code> ⭐ — keep your streak alive!</i>",
        "<b>🎁 هدیه‌ی روزانه دریافت شد!</b>\n<blockquote><b>پاداش:</b> <code>+{reward}</code> ⭐ استارز{bonus}\n"
        "<b>استریک:</b> 🔥 <code>{streak}</code> روز</blockquote>\n{cal}\n"
        "<i>فردا: <code>+{next}</code> ⭐ — استریک خود را حفظ کنید!</i>"),
    "g_milestone": ("\n<b>Streak milestone bonus:</b> <code>+{n}</code> ⭐",
                    "\n<b>جایزه‌ی ویژه‌ی استریک:</b> <code>+{n}</code> ⭐"),
    "g_gift_wait": (
        "<b>🎁 Daily gift</b>\n<blockquote>Today's gift is already collected ✅\n"
        "<b>Streak:</b> 🔥 <code>{streak}</code> day(s)\n<b>Next gift in:</b> {hours}h {mins}m — <code>+{next}</code> ⭐</blockquote>\n{cal}\n"
        "<i>Tip: complete a task today to unlock your free lucky-wheel spin.</i>",
        "<b>🎁 هدیه‌ی روزانه</b>\n<blockquote>هدیه‌ی امروز را قبلاً دریافت کرده‌اید ✅\n"
        "<b>استریک:</b> 🔥 <code>{streak}</code> روز\n<b>هدیه‌ی بعدی:</b> {hours} ساعت و {mins} دقیقه دیگر — <code>+{next}</code> ⭐</blockquote>\n{cal}\n"
        "<i>نکته: با انجام یک تسک در امروز، چرخش رایگان گردونه‌ی شانس باز می‌شود.</i>"),
    "g_gift_join": (
        "<b>🔒 Members-only gift</b>\n\nThe daily gift is for members of the task channel. Join, then tap <b>Check membership</b>.",
        "<b>🔒 هدیه‌ی ویژه‌ی اعضا</b>\n\nهدیه‌ی روزانه مخصوص اعضای کانال تاسک‌هاست. عضو شوید و سپس روی «بررسی عضویت» بزنید."),

    # ───────── Lucky wheel ─────────
    "g_wheel_locked": (
        "<b>🎰 Lucky wheel</b>\n<blockquote>Complete at least <b>1 task</b> today to unlock your free spin.\n"
        "<b>Prizes:</b> <code>1</code> to <code>{top}</code> ⭐ Stars</blockquote>",
        "<b>🎰 گردونه‌ی شانس</b>\n<blockquote>برای باز شدن چرخش رایگان، امروز دست‌کم <b>1 تسک</b> انجام دهید.\n"
        "<b>جوایز:</b> از <code>1</code> تا <code>{top}</code> ⭐ استارز</blockquote>"),
    "g_wheel_ready": (
        "<b>🎰 Lucky wheel</b>\n<blockquote>Your free spin for today is ready!\n<b>Prizes:</b> <code>1</code> to <code>{top}</code> ⭐ Stars</blockquote>",
        "<b>🎰 گردونه‌ی شانس</b>\n<blockquote>چرخش رایگان امروز شما آماده است!\n<b>جوایز:</b> از <code>1</code> تا <code>{top}</code> ⭐ استارز</blockquote>"),
    "g_wheel_used": (
        "<b>🎰 Lucky wheel</b>\n<blockquote>You already spun today ✅\nCome back tomorrow for another free spin.</blockquote>",
        "<b>🎰 گردونه‌ی شانس</b>\n<blockquote>امروز چرخانده‌اید ✅\nفردا برای چرخش رایگان بعدی برگردید.</blockquote>"),
    "g_wheel_win": (
        "<b>🎰 Lucky wheel</b>\n\n<code>{reel}</code>\n<blockquote><b>You won:</b> <code>+{prize}</code> ⭐ Stars</blockquote>",
        "<b>🎰 گردونه‌ی شانس</b>\n\n<code>{reel}</code>\n<blockquote><b>برنده شدید:</b> <code>+{prize}</code> ⭐ استارز</blockquote>"),
    "g_wheel_spin": ("<b>🎰 Lucky wheel</b>\n\n<code>{reel}</code>", "<b>🎰 گردونه‌ی شانس</b>\n\n<code>{reel}</code>"),

    # ───────── Leaderboard ─────────
    "g_lb": (
        "<b>🏆 Weekly leaderboard</b>\n<i>{board} · resets every Monday (UTC)</i>\n\n{rows}\n\n"
        "<blockquote><b>Your rank:</b> {rank}\n<b>Your score:</b> <code>{score}</code></blockquote>\n<i>Weekly prizes: {prizes} ⭐</i>",
        "<b>🏆 جدول برترین‌های هفته</b>\n<i>{board} · هر دوشنبه (UTC) بازنشانی می‌شود</i>\n\n{rows}\n\n"
        "<blockquote><b>رتبه‌ی شما:</b> {rank}\n<b>امتیاز شما:</b> <code>{score}</code></blockquote>\n<i>جوایز هفتگی: {prizes} ⭐</i>"),
    "g_lb_empty": ("No scores yet — be the first this week!", "هنوز امتیازی ثبت نشده؛ این هفته اولین نفر باشید!"),
    "g_rank_none": ("not ranked yet", "هنوز رتبه‌ای ندارید"),
    "g_you": ("← you", "← شما"),

    # ───────── Levels / progress ─────────
    "g_levelup": (
        "<b>{icon} Level up!</b>\n<blockquote>You reached <b>{level}</b>.\n<b>Bonus:</b> <code>+{bonus}</code> ⭐ Stars\n"
        "<b>Perks:</b> gift x{mult} · {cap} tasks per day</blockquote>",
        "<b>{icon} ارتقای سطح!</b>\n<blockquote>به سطح <b>{level}</b> رسیدید.\n<b>جایزه:</b> <code>+{bonus}</code> ⭐ استارز\n"
        "<b>مزایا:</b> هدیه‌ی روزانه x{mult} · {cap} تسک در روز</blockquote>"),
    "g_progress": (
        "\n<blockquote>{icon} <b>{level}</b> {bar} <code>{prog}</code>\n🎯 Today: <code>{today}</code> / {cap}{golden}</blockquote>",
        "\n<blockquote>{icon} <b>{level}</b> {bar} <code>{prog}</code>\n🎯 امروز: <code>{today}</code> از {cap}{golden}</blockquote>"),
    "g_golden_line": ("\n⚡ Golden hour: rewards x{m}", "\n⚡ ساعت طلایی: پاداش x{m}"),
    "g_golden_menu": ("\n⚡ <b>Golden hour is live!</b> Task rewards are x{m} until {end}:00 UTC.",
                      "\n⚡ <b>ساعت طلایی فعال است!</b> پاداش تسک‌ها تا ساعت {end}:00 (UTC) {m} برابر است."),
    "g_menu_line": (
        "<blockquote>{icon} <b>{level}</b> {bar} <code>{prog}</code>\n🔥 Streak: <code>{streak}</code> day(s){gift}</blockquote>{golden}",
        "<blockquote>{icon} <b>{level}</b> {bar} <code>{prog}</code>\n🔥 استریک: <code>{streak}</code> روز{gift}</blockquote>{golden}"),
    "g_menu_gift": ("\n🎁 <b>Your daily gift is ready!</b>", "\n🎁 <b>هدیه‌ی روزانه‌ی شما آماده است!</b>"),
    "g_acc_extra": (
        "<blockquote>{icon} <b>{level}</b> {bar} <code>{prog}</code>\n<b>Perks:</b> gift x{mult} · {cap} tasks/day\n"
        "🔥 <b>Streak:</b> <code>{streak}</code> (best {best})\n🏅 <b>Achievements:</b> <code>{ach}</code> / {total}\n"
        "🏆 <b>Weekly rank:</b> {rank}</blockquote>",
        "<blockquote>{icon} <b>{level}</b> {bar} <code>{prog}</code>\n<b>مزایا:</b> هدیه x{mult} · {cap} تسک در روز\n"
        "🔥 <b>استریک:</b> <code>{streak}</code> (رکورد {best})\n🏅 <b>دستاوردها:</b> <code>{ach}</code> از {total}\n"
        "🏆 <b>رتبه‌ی هفتگی:</b> {rank}</blockquote>"),

    # ───────── Achievements ─────────
    "g_ach": ("<b>🏅 Achievements</b>\n<i>{n} of {total} unlocked</i>\n\n{rows}",
              "<b>🏅 دستاوردها</b>\n<i>{n} از {total} باز شده</i>\n\n{rows}"),
    "g_ach_new": ("<b>🏅 New achievement!</b>\n<blockquote>{rows}</blockquote>\n<b>Total reward:</b> <code>+{total}</code> ⭐ Stars",
                  "<b>🏅 دستاورد جدید!</b>\n<blockquote>{rows}</blockquote>\n<b>مجموع پاداش:</b> <code>+{total}</code> ⭐ استارز"),
    "g_ladder": ("\n\n<b>Invite milestones</b>\n<blockquote>{rows}</blockquote>",
                 "\n\n<b>پله‌های دعوت</b>\n<blockquote>{rows}</blockquote>"),
    "g_ladder_row": ("{mark} <b>{need}</b> active friends — <code>+{reward}</code> ⭐",
                     "{mark} <b>{need}</b> دوست فعال — <code>+{reward}</code> ⭐"),

    # ───────── Reminders / onboarding ─────────
    "g_rm_risk": (
        "<b>🔥 Your {streak}-day streak is at risk!</b>\nCollect today's gift to keep it alive — <code>+{reward}</code> ⭐ is waiting.",
        "<b>🔥 استریک {streak} روزه‌ی شما در خطر است!</b>\nبرای حفظ آن هدیه‌ی امروز را بگیرید — <code>+{reward}</code> ⭐ منتظر شماست."),
    "g_rm_gift": (
        "<b>🎁 Your daily gift is ready</b>\n<code>+{reward}</code> ⭐ Stars and <code>{open}</code> open tasks are waiting for you.",
        "<b>🎁 هدیه‌ی روزانه‌ی شما آماده است</b>\n<code>+{reward}</code> ⭐ استارز و <code>{open}</code> تسک باز منتظر شماست."),
    "g_rm_off": ("<b>🔕 Reminders turned off</b>\nYou can turn them back on from Account.",
                 "<b>🔕 یادآور خاموش شد</b>\nهر زمان خواستید از بخش «حساب کاربری» دوباره روشنش کنید."),
    "g_welcome": (
        "<b>Welcome, {name}! 🎉</b>\n<blockquote>🎁 <b>Starting balance:</b> <code>{start}</code> ⭐ Stars{welcome}</blockquote>\n"
        "<b>Start in 3 steps</b>\n<blockquote><b>1.</b> Tap <b>Earn Stars</b> and open a task.\n"
        "<b>2.</b> Complete it and tap <b>Claim reward</b>.\n<b>3.</b> Spend Stars on <b>Grow my channel</b>.</blockquote>\n"
        "<i>Collect your 🎁 daily gift every day — longer streaks pay more!</i>",
        "<b>{name} عزیز، خوش آمدید! 🎉</b>\n<blockquote>🎁 <b>موجودی اولیه:</b> <code>{start}</code> ⭐ استارز{welcome}</blockquote>\n"
        "<b>شروع در 3 قدم</b>\n<blockquote><b>1.</b> روی «کسب استارز» بزنید و یک تسک را باز کنید.\n"
        "<b>2.</b> تسک را انجام دهید و روی «دریافت پاداش» بزنید.\n<b>3.</b> استارز را برای «رشد کانال من» خرج کنید.</blockquote>\n"
        "<i>هر روز 🎁 هدیه‌ی روزانه را بگیرید؛ استریک بلندتر، پاداش بیشتر!</i>"),
    "g_welcome_bonus": ("\n🤝 <b>Invite bonus:</b> <code>+{n}</code> ⭐", "\n🤝 <b>جایزه‌ی دعوت:</b> <code>+{n}</code> ⭐"),
})


# ───────────────────────── locks & helpers ─────────────────────────
def take_lock(name, ttl):
    """Cross-worker lock: only one gunicorn worker runs a background job at a time."""
    now = time.time()
    try:
        db.locks.update_one({"_id": name, "until": {"$lt": now}}, {"$set": {"until": now + ttl}}, upsert=True)
        return True
    except DuplicateKeyError:
        return False


def release_lock(name):
    db.locks.update_one({"_id": name}, {"$set": {"until": 0}})


def in_window(win):
    if not win:
        return False
    h, (a, b) = time.gmtime().tm_hour, win
    return (a <= h < b) if a < b else (h >= a or h < b)


def golden_active():
    return GOLDEN_MULT > 1 and in_window(GOLDEN_WINDOW)


def level_index(done):
    idx = 0
    for i, lv in enumerate(LEVELS):
        if int(done or 0) >= lv[0]:
            idx = i
    return idx


def level_view(u, lang):
    done = int((u or {}).get("done_tasks", 0) or 0)
    i = level_index(done)
    lv = LEVELS[i]
    if i + 1 < len(LEVELS):
        need = LEVELS[i + 1][0]
        b, prog = bar(done - lv[0], need - lv[0]), f"{done}/{need}"
    else:
        b, prog = bar(1, 1), f"{done} · MAX"
    return {"icon": lv[1], "level": lv[3] if lang == "fa" else lv[2], "bar": b, "prog": prog,
            "mult": lv[5], "cap": DAILY_TASKS + lv[6], "idx": i}


def daily_cap(uid):
    u = db.users.find_one({"_id": uid}, {"done_tasks": 1}) or {}
    return DAILY_TASKS + LEVELS[level_index(u.get("done_tasks", 0))][6]


def utc_date(offset_days=0):
    return time.strftime("%Y-%m-%d", time.gmtime(time.time() + offset_days * 86400))


def week_id(ts=None):
    return time.strftime("%G-W%V", time.gmtime(ts if ts is not None else time.time()))


# ───────────────────────── daily gift & streak ─────────────────────────
def gift_reward(streak, u):
    base = DAILY_REWARDS[(max(int(streak), 1) - 1) % len(DAILY_REWARDS)]
    mult = LEVELS[level_index((u or {}).get("done_tasks", 0))][5]
    return max(base, int(round(base * mult))), STREAK_MILESTONES.get(int(streak), 0)


def gift_calendar(streak):
    n = len(DAILY_REWARDS)
    pos = ((int(streak) - 1) % n) + 1 if int(streak) > 0 else 0
    return "".join("✅" if i <= pos else ("🎁" if i == n else "⬜") for i in range(1, n + 1))


def gift_kb(lang):
    return [[B(tb(lang, "b_wheel"), "wheel"), B(tb(lang, "b_top"), "top")], [B(tb(lang, "b_menu"), "home")]]


def gift_wait_screen(u, lang):
    streak = int(u.get("streak", 0))
    secs = 86400 - int(time.time() % 86400)
    nxt, _ = gift_reward(streak + 1, u)
    return (t(lang, "g_gift_wait", streak=streak, hours=secs // 3600, mins=(secs % 3600) // 60, next=nxt,
              cal=gift_calendar(streak)), gift_kb(lang), None)


def gift_claim(uid, lang):
    """Collect today's gift. Returns (text, keyboard, toast)."""
    u = db.users.find_one({"_id": uid}) or {}
    today = day()
    if u.get("daily_last") == today:
        return gift_wait_screen(u, lang)
    if required_channel_member(uid) is False:               # members-only: the gift keeps the showcase channel growing
        kb = []
        if tasks_channel_url():
            kb.append([U(tb(lang, "b_join_channel"), tasks_channel_url(), "primary")])
        kb += [[B(tb(lang, "b_check"), "gift", "success")], [B(tb(lang, "b_menu"), "home")]]
        return t(lang, "g_gift_join"), kb, None
    old = db.users.find_one_and_update({"_id": uid, "daily_last": {"$ne": today}}, {"$set": {"daily_last": today}})
    if not old:                                               # lost a race with a double tap
        return gift_wait_screen(db.users.find_one({"_id": uid}) or u, lang)
    streak = int(old.get("streak", 0)) + 1 if old.get("daily_last") == utc_date(-1) else 1
    amount, milestone = gift_reward(streak, old)
    db.users.update_one({"_id": uid}, {"$set": {"streak": streak, "streak_best": max(int(old.get("streak_best", 0)), streak)}})
    add_points(uid, amount + milestone, "daily", f"s{streak}")
    try:
        check_achievements(uid)
    except Exception:
        log.exception("check_achievements")
    nxt, _ = gift_reward(streak + 1, old)
    bonus = t(lang, "g_milestone", n=milestone) if milestone else H("")
    return (t(lang, "g_gift_ok", reward=amount, bonus=bonus, streak=streak, cal=gift_calendar(streak), next=nxt),
            gift_kb(lang), "🎁 +%d" % (amount + milestone))


# ───────────────────────── lucky wheel ─────────────────────────
def wheel_state(uid):
    if counter_get(f"wheel:{uid}:{day()}") >= 1:
        return "used"
    return "ready" if counter_get(f"d:{uid}:{day()}") >= 1 else "locked"


def wheel_screen(uid, lang):
    st = wheel_state(uid)
    top = max(p for p, _ in WHEEL)
    kb = []
    if st == "ready":
        kb.append([B(tb(lang, "b_spin"), "spin", "success")])
    elif st == "locked":
        kb.append([B(tb(lang, "b_earn"), "earn", "primary")])
    kb.append([B(tb(lang, "b_gift"), "gift"), B(tb(lang, "b_menu"), "home")])
    return t(lang, "g_wheel_" + st, top=top), kb


def wheel_spin(cq, uid, lang, chat, mid):
    if wheel_state(uid) != "ready" or not counter_take(f"wheel:{uid}:{day()}", 1):
        ans(cq)
        text, kb = wheel_screen(uid, lang)
        return view(chat, mid, text, kb)
    ans(cq)
    prize = random.choices([p for p, _ in WHEEL], weights=[w for _, w in WHEEL])[0]
    for reel in ("🎲 ▫️ ▫️ ▫️", "🎲 🍀 ▫️ ▫️", "🎲 🍀 💫 ▫️"):
        view(chat, mid, t(lang, "g_wheel_spin", reel=reel), [])
        time.sleep(0.5)
    add_points(uid, prize, "wheel", day())
    kb = [[B(tb(lang, "b_earn"), "earn", "primary"), B(tb(lang, "b_gift"), "gift")], [B(tb(lang, "b_menu"), "home")]]
    return view(chat, mid, t(lang, "g_wheel_win", reel="🎲 🍀 💫 ⭐", prize=prize), kb)


# ───────────────────────── weekly leaderboard ─────────────────────────
def wk_add(uid, score=0, refs=0):
    inc = {k: int(v) for k, v in (("score", score), ("refs", refs)) if v}
    if not inc:
        return
    u = db.users.find_one({"_id": uid}, {"name": 1}) or {}
    wk = week_id()
    db.weekly.update_one({"_id": f"{wk}:{uid}"}, {"$inc": inc, "$set": {"wk": wk, "uid": uid, "name": short(u.get("name"), 20)}},
                         upsert=True)


def board_rows(wk, field, limit=10):
    return list(db.weekly.find({"wk": wk, field: {"$gt": 0}}).sort([(field, -1), ("uid", 1)]).limit(limit))


def board_rank(wk, field, uid):
    mine = db.weekly.find_one({"_id": f"{wk}:{uid}"}) or {}
    score = int(mine.get(field, 0) or 0)
    if score <= 0:
        return None, 0
    return db.weekly.count_documents({"wk": wk, field: {"$gt": score}}) + 1, score


def prize_text():
    return " · ".join(f"{m} {p}" for m, p in zip(("🥇", "🥈", "🥉", "4.", "5."), WEEKLY_PRIZES))


def lb_screen(uid, lang, which="e"):
    field = "refs" if which == "r" else "score"
    wk = week_id()
    rows = []
    for i, r in enumerate(board_rows(wk, field), 1):
        medal = ("🥇", "🥈", "🥉")[i - 1] if i <= 3 else f"{i}."
        me = " " + tb(lang, "g_you") if r.get("uid") == uid else ""
        rows.append(f"{medal} {html.escape(r.get('name') or tb(lang, 'user_fallback'))} — <code>{int(r.get(field, 0))}</code>{me}")
    rank, score = board_rank(wk, field, uid)
    text = t(lang, "g_lb", board=tb(lang, "b_inviters" if which == "r" else "b_earners"),
             rows=H("\n".join(rows) if rows else html.escape(tb(lang, "g_lb_empty"))),
             rank=f"#{rank}" if rank else tb(lang, "g_rank_none"), score=score, prizes=prize_text())
    kb = [[B(tb(lang, "b_earners"), "lb:e", "primary" if which != "r" else None),
           B(tb(lang, "b_inviters"), "lb:r", "primary" if which == "r" else None)],
          [B(tb(lang, "b_gift"), "gift"), B(tb(lang, "b_menu"), "home")]]
    return text, kb


def weekly_close():
    """Once per week: pay prizes to the top of both boards and post the results in the showcase channel."""
    prev = week_id(time.time() - 7 * 86400)
    try:
        db.meta.update_one({"_id": "wk_closed", "v": {"$ne": prev}}, {"$set": {"v": prev}}, upsert=True)
    except DuplicateKeyError:
        return
    sections, plain = [], {}
    for field, head_en, head_fa in (("score", "⭐ Top earners", "⭐ برترین درآمدسازها"), ("refs", "👥 Top inviters", "👥 برترین دعوت‌کننده‌ها")):
        rows_en, rows_fa = [], []
        for i, r in enumerate(board_rows(prev, field, len(WEEKLY_PRIZES)), 1):
            uid, prize = r["uid"], WEEKLY_PRIZES[i - 1]
            if user_is_banned(uid):
                continue
            add_points(uid, prize, "weekly_prize", f"{prev}:{field}:{i}")
            plain.setdefault(uid, []).append((i, field, prize))
            nm = html.escape(r.get("name") or "—")
            medal = ("🥇", "🥈", "🥉", "4.", "5.")[i - 1]
            rows_en.append(f"{medal} {nm} — <code>{int(r.get(field, 0))}</code> (+{prize} ⭐)")
        if rows_en:
            sections.append((head_en, head_fa, "\n".join(rows_en)))
    for uid, wins in plain.items():
        total = sum(w[2] for w in wins)
        lang = ulang(uid)
        msg = (f"<b>🏆 Weekly prize!</b>\nYou placed in last week's leaderboard and won <code>+{total}</code> ⭐ Stars."
               if lang != "fa" else f"<b>🏆 جایزه‌ی هفتگی!</b>\nدر جدول برترین‌های هفته‌ی گذشته جایگاه کسب کردید و <code>+{total}</code> ⭐ استارز برنده شدید.")
        send(uid, H(_rtl(msg) if lang == "fa" else msg), [[B(tb(lang, "b_top"), "top")]])
    if sections and TASKS_CHANNEL:
        en = f"<b>🏆 Weekly results · {prev}</b>\n\n" + "\n\n".join(f"<b>{h}</b>\n{rows}" for h, _, rows in sections) + \
             "\n\nPrizes were added to the winners' balances. A new week starts now — good luck!"
        fa = _rtl(f"<b>🏆 نتایج هفتگی · {prev}</b>\n\n" + "\n\n".join(f"<b>{h}</b>\n{rows}" for _, h, rows in sections) +
                  "\n\nجوایز به موجودی برندگان افزوده شد. هفته‌ی جدید از همین حالا آغاز می‌شود؛ موفق باشید!")
        post_to_channel(fa + "\n────────\n" + en)


# ───────────────────────── achievements & levels ─────────────────────────
def check_achievements(uid):
    u = db.users.find_one({"_id": uid}) or {}
    have, won = set(u.get("ach", [])), []
    for a in ACH:
        if a[0] not in have and int(u.get(a[5], 0) or 0) >= a[6]:
            if db.users.update_one({"_id": uid, "ach": {"$ne": a[0]}}, {"$push": {"ach": a[0]}}).modified_count:
                won.append(a)
    if not won:
        return
    total = sum(a[4] for a in won)
    add_points(uid, total, "achievement", ",".join(a[0] for a in won))
    lang = lang_of(u)
    rows = "\n".join(f"{a[1]} {html.escape(a[3] if lang == 'fa' else a[2])} — <code>+{a[4]}</code>" for a in won)
    send(uid, t(lang, "g_ach_new", rows=H(rows), total=total), [[B(tb(lang, "b_ach"), "ach")]])


def ach_screen(uid, lang):
    u = db.users.find_one({"_id": uid}) or {}
    have = set(u.get("ach", []))
    rows = [f"{'✅' if a[0] in have else '🔒'} {a[1]} {html.escape(a[3] if lang == 'fa' else a[2])} — <code>+{a[4]}</code> ⭐" for a in ACH]
    text = t(lang, "g_ach", n=len(have & {a[0] for a in ACH}), total=len(ACH), rows=H("\n".join(rows)))
    return text, [[B(tb(lang, "b_acc"), "acc")], [B(tb(lang, "b_menu"), "home")]]


def ladder_text(u, lang):
    have = set((u or {}).get("ach", []))
    rows = [str(t(lang, "g_ladder_row", mark="✅" if a[0] in have else "🔒", need=a[6], reward=a[4])) for a in ACH if a[5] == "ref_active_count"]
    return t(lang, "g_ladder", rows=H("\n".join(rows)))


def gamify_on_paid(comp):
    """Everything that happens when a task reward is finally paid: golden-hour bonus, counters, weekly score, level-up, badges."""
    uid, reward = comp["uid"], int(comp["reward"])
    extra = 0
    if golden_active():
        extra = reward * (GOLDEN_MULT - 1)
        if extra > 0:
            add_points(uid, extra, "golden", comp.get("cid"))
    cur = db.users.find_one({"_id": uid}, {"done_tasks": 1}) or {}
    if "done_tasks" not in cur:                                # lazy back-fill for users created before v52
        n = db.completions.count_documents({"uid": uid, "status": "paid"})
        db.users.update_one({"_id": uid, "done_tasks": {"$exists": False}}, {"$set": {"done_tasks": n}})
    else:
        db.users.update_one({"_id": uid}, {"$inc": {"done_tasks": 1}})
    u = db.users.find_one({"_id": uid}) or {}
    wk_add(uid, reward + extra)
    idx, old = level_index(u.get("done_tasks", 0)), int(u.get("level", 0) or 0)
    if idx > old and db.users.update_one({"_id": uid, "$or": [{"level": {"$lt": idx}}, {"level": {"$exists": False}}]},
                                         {"$set": {"level": idx}}).modified_count:
        bonus = sum(LEVELS[i][4] for i in range(old + 1, idx + 1))
        if bonus:
            add_points(uid, bonus, "level_up", idx)
        lang = lang_of(u)
        lv = level_view(u, lang)
        send(uid, t(lang, "g_levelup", icon=lv["icon"], level=lv["level"], bonus=bonus, mult=lv["mult"], cap=lv["cap"]),
             [[B(tb(lang, "b_earn"), "earn", "primary")]])
    check_achievements(uid)
    return {"golden": extra}


def progress_line(uid, lang):
    u = db.users.find_one({"_id": uid}) or {}
    lv = level_view(u, lang)
    gold = t(lang, "g_golden_line", m=GOLDEN_MULT) if golden_active() else H("")
    return str(t(lang, "g_progress", icon=lv["icon"], level=lv["level"], bar=lv["bar"], prog=lv["prog"],
                 today=counter_get(f"d:{uid}:{day()}"), cap=lv["cap"], golden=gold))


def growth_menu_extra(u, lang):
    lv = level_view(u, lang)
    gift = t(lang, "g_menu_gift") if (u or {}).get("daily_last") != day() else H("")
    gold = t(lang, "g_golden_menu", m=GOLDEN_MULT, end=GOLDEN_WINDOW[1]) if golden_active() else H("")
    streak = int(u.get("streak", 0)) if u.get("daily_last") in (day(), utc_date(-1)) else 0
    return str(t(lang, "g_menu_line", icon=lv["icon"], level=lv["level"], bar=lv["bar"], prog=lv["prog"],
                 streak=streak, gift=gift, golden=gold))


def growth_acc_extra(u, lang):
    lv = level_view(u, lang)
    rank, _ = board_rank(week_id(), "score", u["_id"])
    return str(t(lang, "g_acc_extra", icon=lv["icon"], level=lv["level"], bar=lv["bar"], prog=lv["prog"], mult=lv["mult"], cap=lv["cap"],
                 streak=int(u.get("streak", 0)) if u.get("daily_last") in (day(), utc_date(-1)) else 0,
                 best=int(u.get("streak_best", 0)), ach=len(set(u.get("ach", [])) & {a[0] for a in ACH}), total=len(ACH),
                 rank=f"#{rank}" if rank else tb(lang, "g_rank_none")))


# ───────────────────────── channel posts ─────────────────────────
def post_to_channel(text, kb=None):
    if not TASKS_CHANNEL:
        return
    p = {"chat_id": TASKS_CHANNEL, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if kb:
        p["reply_markup"] = {"inline_keyboard": kb}
    r = tg("sendMessage", **p)
    if not r.get("ok"):
        log.warning("post_to_channel failed: %s", r.get("description"))


def once(key):
    try:
        db.meta.insert_one({"_id": key, "t": time.time()})
        return True
    except DuplicateKeyError:
        return False


def daily_report():
    now = time.time()
    if time.gmtime().tm_hour < REPORT_HOUR or not TASKS_CHANNEL or not once(f"report:{day()}"):
        return
    start = now - now % 86400
    tasks = db.completions.count_documents({"status": "paid", "paid": {"$gte": start}})
    if tasks < 1:
        return
    new = db.users.count_documents({"joined": {"$gte": start}})
    live = db.campaigns.count_documents({"status": "active", "remaining": {"$gt": 0}})
    en = (f"<b>📊 Today on TaskPlus</b>\n<blockquote>✅ Tasks completed: <code>{tasks}</code>\n👥 New members: <code>{new}</code>\n"
          f"📣 Live campaigns: <code>{live}</code></blockquote>\nOpen tasks are waiting in this channel 👆")
    fa = _rtl(f"<b>📊 امروز در تاسک‌پلاس</b>\n<blockquote>✅ تسک‌های انجام‌شده: <code>{tasks}</code>\n👥 اعضای جدید: <code>{new}</code>\n"
              f"📣 کمپین‌های فعال: <code>{live}</code></blockquote>\nتسک‌های باز در همین کانال منتظر شما هستند 👆")
    post_to_channel(fa + "\n────────\n" + en)


def golden_announce():
    if not golden_active() or not TASKS_CHANNEL or not once(f"golden:{day()}"):
        return
    en = (f"⚡ <b>Golden hour is LIVE!</b>\nEvery task reward is <b>x{GOLDEN_MULT}</b> until {GOLDEN_WINDOW[1]}:00 UTC. "
          "Grab the open tasks now 👇")
    fa = _rtl(f"⚡ <b>ساعت طلایی شروع شد!</b>\nپاداش تمام تسک‌ها تا ساعت {GOLDEN_WINDOW[1]}:00 (UTC) <b>{GOLDEN_MULT} برابر</b> است. "
              "همین حالا تسک‌های باز را انجام دهید 👇")
    username = _me.get("username")
    post_to_channel(fa + "\n────────\n" + en, [[U(bi_btn("b_open_bot"), f"https://t.me/{username}", "primary")]] if username else None)


# ───────────────────────── smart reminders ─────────────────────────
def send_reminders(limit=25):
    if not REMIND_ON or not in_window(REMIND_WINDOW):
        return
    now, today = time.time(), day()
    q = {"remind_off": {"$ne": True}, "blocked": {"$ne": True}, "banned": {"$ne": True}, "last_remind_day": {"$ne": today},
         "daily_last": {"$ne": today}, "last_seen": {"$lt": now - 20 * 3600, "$gt": now - 14 * 86400},
         "$or": [{"remind_miss": {"$exists": False}}, {"remind_miss": {"$lt": 3}}]}
    open_n = db.campaigns.count_documents({"status": "active", "remaining": {"$gt": 0}, "admin_paused": {"$ne": True}})
    yesterday = utc_date(-1)
    for u in list(db.users.find(q).sort("last_seen", -1).limit(limit)):
        uid, lang = u["_id"], lang_of(u)
        db.users.update_one({"_id": uid}, {"$set": {"last_remind_day": today}, "$inc": {"remind_miss": 1}})
        streak = int(u.get("streak", 0))
        at_risk = u.get("daily_last") == yesterday and streak >= 2
        reward, _ = gift_reward(streak + 1 if at_risk else 1, u)
        text = t(lang, "g_rm_risk", streak=streak, reward=reward) if at_risk else t(lang, "g_rm_gift", reward=reward, open=open_n)
        r = send(uid, text, [[B(tb(lang, "b_collect"), "gift", "success")], [B(tb(lang, "b_rm_stop"), "rm:off")]])
        if not r.get("ok") and r.get("error_code") == 403:
            db.users.update_one({"_id": uid}, {"$set": {"blocked": True}})
        time.sleep(0.05)


# ───────────────────────── admin: broadcast queue & analytics ─────────────────────────
def seg_query(seg):
    now = time.time()
    q = {"banned": {"$ne": True}, "blocked": {"$ne": True}}
    if seg == "active":
        q["last_seen"] = {"$gte": now - 7 * 86400}
    elif seg == "inactive":
        q["last_seen"] = {"$lt": now - 7 * 86400}
    elif seg == "new":
        q["joined"] = {"$gte": now - 3 * 86400}
    return q


SEG_LABEL = {"all": ("👥 Everyone", "👥 همه"), "active": ("🟢 Active (7d)", "🟢 فعال (7 روز)"),
             "inactive": ("😴 Inactive (7d+)", "😴 غیرفعال (بیش از 7 روز)"), "new": ("🆕 New (3d)", "🆕 جدید (3 روز)")}


def admin_bc_home(chat, lang, mid=None):
    fa = lang == "fa"
    kb = [[B(f"{SEG_LABEL[s][1 if fa else 0]} · {db.users.count_documents(seg_query(s))}", f"ad:bcs:{s}")] for s in SEG_LABEL]
    last = list(db.broadcasts.find({}).sort("created", -1).limit(3))
    hist = "\n".join(f"• <code>{b.get('seg')}</code> — {b.get('status')} · {b.get('sent', 0)}/{b.get('total', 0)} (✖ {b.get('failed', 0)})" for b in last)
    msg = ("<b>📢 همه‌پیامی</b>\nگروه مخاطب را انتخاب کنید. پیام با سرعت ایمن و صف‌بندی‌شده ارسال می‌شود؛ کاربرانی که ربات را بلاک کرده‌اند خودکار حذف می‌شوند."
           if fa else "<b>📢 Broadcast</b>\nChoose the audience. Messages are sent through a rate-limited queue; users who blocked the bot are skipped automatically.")
    if hist:
        msg += "\n\n<b>" + ("آخرین ارسال‌ها" if fa else "Recent broadcasts") + "</b>\n" + hist
    kb.append([B("↩️ پنل" if fa else "↩️ Panel", "ad:home")])
    return view(chat, mid, H(_rtl(msg) if fa else msg), kb)


def admin_bc_callback(chat, lang, mid, uid, parts):
    fa, action = lang == "fa", parts[1]
    if action == "bc":
        db.states.delete_one({"_id": uid})
        return admin_bc_home(chat, lang, mid)
    if action == "bcs" and len(parts) > 2 and parts[2] in SEG_LABEL:
        set_state(uid, "admin_bc", {"seg": parts[2]})
        prompt = ("متن پیام را ارسال کنید (تگ‌های HTML تلگرام مجاز است: <code>&lt;b&gt;</code>، <code>&lt;i&gt;</code>، <code>&lt;a href&gt;</code>)."
                  if fa else "Send the message text (Telegram HTML is allowed: <code>&lt;b&gt;</code>, <code>&lt;i&gt;</code>, <code>&lt;a href&gt;</code>).")
        return view(chat, mid, H(_rtl(prompt) if fa else prompt), [[B("لغو" if fa else "Cancel", "ad:bc")]])
    if action == "bcgo":
        st = db.states.find_one({"_id": uid}) or {}
        if st.get("step") != "admin_bc_confirm":
            return admin_bc_home(chat, lang, mid)
        d = st["d"]
        db.states.delete_one({"_id": uid})
        db.broadcasts.insert_one({"seg": d["seg"], "text": d["text"], "status": "queued", "created": time.time(), "by": uid,
                                  "cursor": 0, "sent": 0, "failed": 0, "total": db.users.count_documents(seg_query(d["seg"]))})
        return view(chat, mid, admin_text(lang, "✅ Broadcast queued. You will get a report when it finishes.",
                                          "✅ همه‌پیامی در صف قرار گرفت. پس از پایان، گزارش برایتان ارسال می‌شود."),
                    [[B("↩️ پنل" if fa else "↩️ Panel", "ad:home")]])
    return admin_bc_home(chat, lang, mid)


def admin_bc_text(uid, chat, lang, text, st):
    fa, seg = lang == "fa", (st.get("d") or {}).get("seg", "all")
    r = send(chat, H(text))                                    # preview; also validates the HTML
    if not r.get("ok"):
        return send(chat, admin_text(lang, f"Invalid HTML: {html.escape(str(r.get('description')))}\nFix the text and send it again.",
                                     f"HTML نامعتبر است: {html.escape(str(r.get('description')))}\nمتن را اصلاح و دوباره ارسال کنید."),
                    [[B("لغو" if fa else "Cancel", "ad:bc")]])
    set_state(uid, "admin_bc_confirm", {"seg": seg, "text": text})
    n = db.users.count_documents(seg_query(seg))
    return send(chat, admin_text(lang, f"Send this message to <b>{n}</b> users ({SEG_LABEL[seg][0]})?",
                                 f"این پیام برای <b>{n}</b> کاربر ({SEG_LABEL[seg][1]}) ارسال شود؟"),
                [[B("✅ ارسال" if fa else "✅ Send", "ad:bcgo", "success")], [B("لغو" if fa else "Cancel", "ad:bc", "danger")]])


def pump_broadcasts(max_seconds=20):
    """Send queued broadcasts at ~15 messages/second, resumable (cursor = last user id)."""
    bc = db.broadcasts.find_one({"status": {"$in": ["queued", "sending"]}}, sort=[("created", 1)])
    if not bc or not take_lock("bc", max_seconds + 10):
        return False
    started = time.time()
    try:
        db.broadcasts.update_one({"_id": bc["_id"]}, {"$set": {"status": "sending"}})
        q = dict(seg_query(bc["seg"]))
        while time.time() - started < max_seconds:
            q["_id"] = {"$gt": bc.get("cursor", 0)}
            batch = list(db.users.find(q).sort("_id", 1).limit(30))
            if not batch:
                db.broadcasts.update_one({"_id": bc["_id"]}, {"$set": {"status": "done", "finished": time.time()}})
                fin = db.broadcasts.find_one({"_id": bc["_id"]})
                send(bc["by"], H(f"✅ <b>Broadcast finished</b>\nSent: <code>{fin.get('sent', 0)}</code> · Failed: <code>{fin.get('failed', 0)}</code> · "
                                 f"Audience: <code>{fin.get('total', 0)}</code>"))
                return False
            sent = failed = 0
            for u in batch:
                r = send(u["_id"], H(bc["text"]))
                if r.get("ok"):
                    sent += 1
                else:
                    failed += 1
                    if r.get("error_code") == 403:
                        db.users.update_one({"_id": u["_id"]}, {"$set": {"blocked": True}})
                time.sleep(0.065)
                bc["cursor"] = u["_id"]
            db.broadcasts.update_one({"_id": bc["_id"]}, {"$set": {"cursor": bc["cursor"]}, "$inc": {"sent": sent, "failed": failed}})
        return True
    finally:
        release_lock("bc")


def growth_stats_text(lang):
    now = time.time()
    start = now - now % 86400
    cnt = db.users.count_documents
    coins = next(iter(db.users.aggregate([{"$group": {"_id": None, "s": {"$sum": "$points"}}}])), {}).get("s", 0)

    def retention(lo, hi, gap):
        cohort = list(db.users.find({"joined": {"$gte": now - hi, "$lt": now - lo}}, {"joined": 1, "last_seen": 1}).limit(3000))
        if not cohort:
            return "—"
        back = sum(1 for c in cohort if float(c.get("last_seen", 0)) >= float(c["joined"]) + gap)
        return f"{round(100 * back / len(cohort))}% ({len(cohort)})"

    vals = dict(dau=cnt({"last_seen": {"$gte": now - 86400}}), wau=cnt({"last_seen": {"$gte": now - 7 * 86400}}),
                new=cnt({"joined": {"$gte": start}}), gifts=cnt({"daily_last": day()}), streak3=cnt({"streak": {"$gte": 3}}),
                tasks=db.completions.count_documents({"status": "paid", "paid": {"$gte": start}}),
                coins=int(coins or 0), blocked=cnt({"blocked": True}), refs=cnt({"referred_by": {"$exists": True}}),
                refs_active=cnt({"ref_active": True}), d1=retention(86400, 2 * 86400, 86400), d7=retention(7 * 86400, 8 * 86400, 7 * 86400))
    if lang == "fa":
        msg = ("\n<b>📈 رشد و تعامل</b>\n<blockquote>"
               "فعال روزانه / هفتگی: <code>{dau}</code> / <code>{wau}</code>\nکاربران جدید امروز: <code>{new}</code>\n"
               "هدیه‌ی روزانه‌ی دریافت‌شده امروز: <code>{gifts}</code>\nکاربران با استریک 3 روز و بیشتر: <code>{streak3}</code>\n"
               "تسک‌های پرداخت‌شده‌ی امروز: <code>{tasks}</code>\nمجموع استارز در گردش: <code>{coins}</code>\n"
               "بازگشت روز 1: {d1}\nبازگشت روز 7: {d7}\nدعوت‌شده‌ها / فعال: <code>{refs}</code> / <code>{refs_active}</code>\n"
               "بلاک‌کننده‌های ربات: <code>{blocked}</code></blockquote>")
    else:
        msg = ("\n<b>📈 Growth & engagement</b>\n<blockquote>"
               "DAU / WAU: <code>{dau}</code> / <code>{wau}</code>\nNew users today: <code>{new}</code>\n"
               "Daily gifts collected today: <code>{gifts}</code>\nUsers with a 3+ day streak: <code>{streak3}</code>\n"
               "Tasks paid today: <code>{tasks}</code>\nStars in circulation: <code>{coins}</code>\n"
               "Day-1 return: {d1}\nDay-7 return: {d7}\nReferred / active: <code>{refs}</code> / <code>{refs_active}</code>\n"
               "Blocked the bot: <code>{blocked}</code></blockquote>")
    return H(_rtl(msg.format(**vals)) if lang == "fa" else msg.format(**vals))


# ───────────────────────── background jobs ─────────────────────────
def growth_tick():
    for job in (weekly_close, golden_announce, daily_report, send_reminders):
        try:
            job()
        except Exception:
            log.exception("growth job %s", job.__name__)


def worker():
    last_tick, pending = 0.0, False
    while True:
        try:
            now = time.time()
            if now - last_tick >= 30:
                last_tick = now
                if take_lock("tick", 25):
                    tick()
                    growth_tick()
            pending = pump_broadcasts()
        except Exception:
            log.exception("tick")
        time.sleep(2 if pending else 5)


# ───────────────────────── callback router for the new screens ─────────────────────────
def growth_callback(cq, uid, u, lang, chat, mid, data):
    """Returns True when the callback belongs to the growth engine."""
    if data == "gift":
        text, kb, toast = gift_claim(uid, lang)
        ans(cq, toast)
        view(chat, mid, text, kb)
        return True
    if data == "wheel":
        ans(cq)
        text, kb = wheel_screen(uid, lang)
        view(chat, mid, text, kb)
        return True
    if data == "spin":
        wheel_spin(cq, uid, lang, chat, mid)
        return True
    if data == "top" or data in ("lb:e", "lb:r"):
        ans(cq)
        text, kb = lb_screen(uid, lang, "r" if data == "lb:r" else "e")
        view(chat, mid, text, kb)
        return True
    if data == "ach":
        ans(cq)
        text, kb = ach_screen(uid, lang)
        view(chat, mid, text, kb)
        return True
    if data in ("rm", "rm:off"):
        off = True if data == "rm:off" else not u.get("remind_off")
        db.users.update_one({"_id": uid}, {"$set": {"remind_off": off}})
        u["remind_off"] = off
        ans(cq)
        if data == "rm:off":
            view(chat, mid, t(lang, "g_rm_off"), [[B(tb(lang, "b_menu"), "home")]])
        else:
            view(chat, mid, acc_text(u), acc_kb(lang, uid))
        return True
    if data == "rep":
        last = u.get("last_join")
        if not last:
            ans(cq)
            return True
        if db.campaigns.count_documents({"owner": uid, "status": "active"}) >= MAX_ACTIVE:
            ans(cq, tb(lang, "a_max_active", n=MAX_ACTIVE), True)
            return True
        ans(cq)
        set_state(uid, "link", {"type": "join"})
        on_state_text(uid, chat, "@" + last, db.states.find_one({"_id": uid}), lang)
        return True
    if data.startswith("q:"):
        st = db.states.find_one({"_id": uid})
        if not st or st.get("step") != "qty":
            ans(cq, tb(lang, "a_step_expired"), True)
            return True
        ans(cq)
        on_state_text(uid, chat, data[2:], st, lang)
        return True
    return False


def welcome_screen(u, lang):
    bonus = t(lang, "g_welcome_bonus", n=u["ref_welcome"]) if u.get("ref_welcome") else H("")
    text = t(lang, "g_welcome", name=short(u.get("name"), 30) or tb(lang, "friend"), start=START_POINTS, welcome=bonus)
    kb = [[B(tb(lang, "b_earn"), "earn", "primary")], [B(tb(lang, "b_gift_ready"), "gift", "success")], [B(tb(lang, "b_menu"), "home")]]
    return text, kb


# ═════════════════════════════ Update routing ═════════════════════════════
def on_message(msg):
    if msg["chat"]["type"] != "private" or "from" not in msg:
        return
    uid, chat = msg["from"]["id"], msg["chat"]["id"]
    existed = db.users.find_one({"_id": uid}, {"_id": 1}) is not None
    u = ensure_user(msg["from"])
    lang = lang_of(u)
    text = (msg.get("text") or "").strip()
    event_action = "command" if text.startswith("/") else ("text" if text else "message")
    event_detail = text.split()[0][:40] if text.startswith("/") else (db.states.find_one({"_id": uid}) or {}).get("step", "message")
    track_user_event(uid, event_action, event_detail)
    if u.get("banned") and uid not in ADMINS:
        return send(chat, admin_text(lang, "⛔ Your access to this bot has been disabled.", "⛔ دسترسی شما به ربات غیرفعال شده است."))

    if text.startswith("/"):
        parts = text[1:].split()
        cmd, args = (parts[0].split("@")[0].lower() if parts else ""), parts[1:]
        if cmd in ("start", "menu"):
            db.states.delete_one({"_id": uid})
            if cmd == "start" and args and args[0].startswith("task_"):
                return open_linked_task(uid, chat, args[0][5:])
            if cmd == "start" and args and args[0].startswith("ref_") and not existed:
                ref_register(uid, args[0][4:], msg["from"].get("first_name"))
            if cmd == "start" and not existed:
                return send(chat, LANG_PICK, lang_kb())          # first contact: English first, then Persian
            return send(chat, menu_text(u), main_kb(lang, uid))
        if cmd == "help":
            return send(chat, help_text(lang), home_kb(lang))
        if cmd in ("invite", "ref"):
            return send(chat, ref_text(u), ref_kb(uid, lang))
        if cmd in ("language", "lang"):
            return send(chat, LANG_PICK, lang_kb())
        if cmd == "gift":
            text_, kb_, _ = gift_claim(uid, lang)
            return send(chat, text_, kb_)
        if cmd in ("top", "leaderboard"):
            text_, kb_ = lb_screen(uid, lang)
            return send(chat, text_, kb_)
        if uid in ADMINS:
            return admin_cmd(uid, chat, cmd, args, lang)
        return send(chat, menu_text(u), main_kb(lang, uid))

    st = db.states.find_one({"_id": uid})
    if st and st.get("step") == "admin_bc" and uid in ADMINS and text:
        return admin_bc_text(uid, chat, lang, text, st)
    if st and st.get("step") in ("admin_search", "admin_points") and uid in ADMINS and text:
        step = st.get("step")
        db.states.delete_one({"_id": uid})
        if step == "admin_search":
            matches = admin_search_results(text)
            return admin_users_screen(chat, lang, 0, search_results=matches)
        target = int((st.get("d") or {}).get("target", 0))
        if not re.fullmatch(r"[+-]?\d+", text):
            return send(chat, admin_text(lang, "Invalid number. Open the user profile and retry.", "عدد نامعتبر است. پروفایل کاربر را باز کنید و دوباره تلاش کنید."),
                        [[B("↩️ پروفایل" if lang == "fa" else "↩️ Profile", f"ad:user:{target}")]])
        delta = int(text)
        ok = admin_apply_point_delta(uid, target, delta)
        track_user_event(target, "admin_points", f"admin={uid}, delta={delta}, ok={ok}")
        if ok:
            return admin_user_detail(chat, lang, target)
        return send(chat, admin_text(lang, "Adjustment failed. Check the amount and user's current balance.", "تغییر انجام نشد؛ مقدار و موجودی فعلی کاربر را بررسی کنید."),
                    [[B("↩️ پروفایل" if lang == "fa" else "↩️ Profile", f"ad:user:{target}")]])
    if st and st.get("step") == "link" and msg.get("forward_origin"):
        origin = msg.get("forward_origin") or {}
        if origin.get("type") == "channel":
            source = origin.get("chat") or {}
            if source.get("username") and origin.get("message_id"):
                # The post link is built from the forward metadata, so the user need not copy it.
                return on_state_text(uid, chat, f"https://t.me/{source['username']}/{origin['message_id']}", st, lang)
            return send(chat, t(lang, "err_forward_noid"), cancel_kb(lang))
        if origin.get("type") == "chat":
            return send(chat, t(lang, "err_forward_group"), cancel_kb(lang))
        return send(chat, t(lang, "err_forward_other"), cancel_kb(lang))
    if st and text:
        return on_state_text(uid, chat, text, st, lang)
    if st:
        return send(chat, t(lang, "e_answer_prev"), cancel_kb(lang))
    send(chat, menu_text(u), main_kb(lang, uid))


def on_callback(cq):
    uid, data = cq["from"]["id"], cq.get("data", "")
    m = cq.get("message") or {}
    chat, mid = (m.get("chat") or {}).get("id"), m.get("message_id")
    u = ensure_user(cq["from"])
    track_user_event(uid, "callback", data[:100])
    if not chat:
        return ans(cq)
    if u.get("banned") and uid not in ADMINS:
        return ans(cq, "Access disabled" if lang_of(u) != "fa" else "دسترسی غیرفعال است", True)

    if data.startswith("lg:"):                                   # language selection
        choice = data[3:] if data[3:] in ("en", "fa") else "en"
        db.users.update_one({"_id": uid}, {"$set": {"lang": choice}})
        u["lang"] = choice
        ans(cq)
        db.states.delete_one({"_id": uid})
        if not u.get("onboarded") and time.time() - float(u.get("joined", 0) or 0) < 3600:      # first run: guided welcome
            db.users.update_one({"_id": uid}, {"$set": {"onboarded": True}})
            w_text, w_kb = welcome_screen(u, choice)
            return view(chat, mid, w_text, w_kb)
        return view(chat, mid, menu_text(u), main_kb(choice, uid))

    lang = lang_of(u)
    if data == "lang":
        ans(cq)
        return view(chat, mid, LANG_PICK, lang_kb())
    if data == "noop":
        return ans(cq)
    if data == "home":
        ans(cq)
        db.states.delete_one({"_id": uid})
        return view(chat, mid, menu_text(u), main_kb(lang, uid))
    if data == "help":
        ans(cq)
        return view(chat, mid, help_text(lang), home_kb(lang))
    if data == "acc":
        ans(cq)
        return view(chat, mid, acc_text(u), acc_kb(lang, uid))
    if growth_callback(cq, uid, u, lang, chat, mid, data):
        return
    if data == "campaigns":
        ans(cq)
        return view(chat, mid, "<b>" + ("Campaign management" if lang == "en" else "مدیریت کمپین‌ها") + "</b>\n\n" +
                    ("Create a campaign or review your existing campaigns." if lang == "en" else "کمپین جدید بسازید یا وضعیت کمپین‌های خود را بررسی کنید."),
                    campaigns_kb(lang, uid))
    if data in ("earn", "earn_check"):
        if not TASKS_CHANNEL or not tasks_channel_url():
            ans(cq)
            return view(chat, mid, t(lang, "earn_unset"), [[B(tb(lang, "b_menu"), "home")]])
        member = required_channel_member(uid)
        if member is None:
            ans(cq)
            msg = ("بررسی عضویت فعلاً ممکن نیست. مطمئن شوید ربات در کانال تاسک‌ها ادمین است و دوباره تلاش کنید."
                   if lang == "fa" else "Membership could not be checked right now. Make sure the bot is an administrator in the task channel and try again.")
            return view(chat, mid, H(_rtl(msg) if lang == "fa" else msg),
                        [[B("🔄 بررسی دوباره" if lang == "fa" else "🔄 Try again", "earn_check")],
                         [B(tb(lang, "b_menu"), "home")]])
        if not member:
            ans(cq)
            return show_join_required(lang, chat, mid)
        # Member: no intermediate message is rendered. Telegram clients may only open arbitrary
        # callback URLs when the bot's custom-URL feature is enabled; keep a URL-button fallback.
        opened = ans(cq, url=tasks_channel_url())
        if not opened.get("ok"):
            return view(chat, mid, t(lang, "earn"),
                        [[U(tb(lang, "b_showcase"), tasks_channel_url(), "primary")],
                         [B(tb(lang, "b_back"), "home")]])
        return
    if data.startswith("ad:"):
        if uid not in ADMINS:
            return ans(cq, "Access denied" if lang != "fa" else "دسترسی مجاز نیست", True)
        parts = data.split(":")
        action = parts[1] if len(parts) > 1 else "home"
        ans(cq)
        if action == "home":
            db.states.delete_one({"_id": uid})
            return admin_home(chat, lang, mid)
        if action == "stats":
            return view(chat, mid, H(str(admin_stats_text(lang)) + str(growth_stats_text(lang))), [[B("↩️ پنل" if lang == "fa" else "↩️ Panel", "ad:home")]])
        if action == "users":
            off = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
            return admin_users_screen(chat, lang, off, mid)
        if action == "search":
            set_state(uid, "admin_search", {})
            prompt = ("🔎 شناسه عددی، نام کاربری یا بخشی از نام کاربر را بفرستید:" if lang == "fa"
                      else "🔎 Send a numeric ID, username, or part of the user's name:")
            return view(chat, mid, admin_text(lang, prompt, prompt),
                        [[B("لغو" if lang == "fa" else "Cancel", "ad:home")]])
        if action == "user" and len(parts) > 2 and parts[2].isdigit():
            return admin_user_detail(chat, lang, int(parts[2]), mid)
        if action == "toggle" and len(parts) > 2 and parts[2].isdigit():
            target = int(parts[2])
            if target in ADMINS:
                return view(chat, mid, admin_text(lang, "Admin accounts cannot be blocked.", "حساب مدیر را نمی‌توان مسدود کرد."),
                            [[B("↩️ پروفایل" if lang == "fa" else "↩️ Profile", f"ad:user:{target}")]])
            target_user = db.users.find_one({"_id": target})
            if not target_user:
                return view(chat, mid, admin_text(lang, "User not found.", "کاربر پیدا نشد."), [[B("↩️ کاربران" if lang == "fa" else "↩️ Users", "ad:users:0")]])
            if target_user.get("banned"):
                db.users.update_one({"_id": target}, {"$set": {"banned": False}, "$unset": {"ban_reason": "", "banned_at": ""}})
                track_user_event(target, "admin_unban", str(uid))
            else:
                db.users.update_one({"_id": target}, {"$set": {"banned": True, "ban_reason": f"admin:{uid}", "banned_at": time.time()}})
                track_user_event(target, "admin_ban", str(uid))
            return admin_user_detail(chat, lang, target, mid)
        if action == "points" and len(parts) > 2 and parts[2].isdigit():
            target = int(parts[2])
            set_state(uid, "admin_points", {"target": target})
            prompt = (f"مقدار امتیاز را با علامت بفرستید؛ مثلاً <code>+50</code> یا <code>-20</code>. کسر امتیاز بیشتر از موجودی مجاز نیست.\n\nکاربر: <code>{target}</code>" if lang == "fa" else
                      f"Send a signed point adjustment, e.g. <code>+50</code> or <code>-20</code>. A deduction cannot exceed the balance.\n\nUser: <code>{target}</code>")
            return view(chat, mid, H(_rtl(prompt) if lang == "fa" else prompt), [[B("لغو" if lang == "fa" else "Cancel", f"ad:user:{target}")]])
        if action == "strikes" and len(parts) > 2 and parts[2].isdigit():
            target = int(parts[2])
            db.users.update_one({"_id": target}, {"$set": {"strikes": 0}})
            track_user_event(target, "admin_reset_strikes", str(uid))
            return admin_user_detail(chat, lang, target, mid)
        if action == "ledger" and len(parts) > 2 and parts[2].isdigit():
            return admin_user_ledger(chat, lang, int(parts[2]), mid)
        if action == "events" and len(parts) > 2 and parts[2].isdigit():
            return admin_events_screen(chat, lang, int(parts[2]), mid=mid)
        if action == "eventspage" and len(parts) > 3 and parts[2].isdigit() and parts[3].isdigit():
            return admin_events_screen(chat, lang, int(parts[2]), offset=int(parts[3]), mid=mid)
        if action == "recent":
            off = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
            return admin_events_screen(chat, lang, offset=off, mid=mid)
        if action in ("bc", "bcs", "bcgo"):
            return admin_bc_callback(chat, lang, mid, uid, parts)
        if action == "campaigns":
            off = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
            return admin_campaigns_screen(chat, lang, off, mid)
        if action == "stop" and len(parts) > 2:
            try:
                cid = ObjectId(parts[2])
            except InvalidId:
                cid = None
            camp = db.campaigns.find_one({"_id": cid, "status": "active"}) if cid else None
            if not camp:
                return view(chat, mid, admin_text(lang, "Active campaign not found.", "کمپین فعال پیدا نشد."), [[B("↩️ کمپین‌ها" if lang == "fa" else "↩️ Campaigns", "ad:campaigns:0")]])
            title = html.escape(str(camp.get("title") or camp.get("uname") or camp.get("type") or cid))[:70]
            prompt = (f"<b>توقف کمپین؟</b>\n\n{title}\nشناسه: <code>{cid}</code>\nامتیاز باقی‌مانده به مالک بازگردانده می‌شود." if lang == "fa" else
                      f"<b>Stop this campaign?</b>\n\n{title}\nID: <code>{cid}</code>\nUnused points will be refunded to its owner.")
            return view(chat, mid, H(_rtl(prompt) if lang == "fa" else prompt),
                        [[B("بله، توقف" if lang == "fa" else "Yes, stop", f"ad:confirmstop:{cid}", "danger")],
                         [B("لغو" if lang == "fa" else "Cancel", "ad:campaigns:0")]])
        if action == "confirmstop" and len(parts) > 2:
            try:
                cid = ObjectId(parts[2])
            except InvalidId:
                cid = None
            camp = db.campaigns.find_one({"_id": cid, "status": "active"}) if cid else None
            if not camp:
                return view(chat, mid, admin_text(lang, "Active campaign not found.", "کمپین فعال پیدا نشد."), [[B("↩️ کمپین‌ها" if lang == "fa" else "↩️ Campaigns", "ad:campaigns:0")]])
            changed = db.campaigns.update_one({"_id": cid, "status": "active"}, {"$set": {"status": "cancelled"}})
            if changed.modified_count:
                refund = int(camp.get("remaining", 0)) * int(camp.get("cost", 0))
                if refund:
                    add_points(camp["owner"], refund, "refund", cid)
                track_user_event(camp["owner"], "admin_campaign_stop", f"campaign={cid}, admin={uid}")
                track_user_event(uid, "admin_campaign_stop", str(cid))
                refresh_campaign_listing(cid)
            return admin_campaigns_screen(chat, lang, 0, mid)
        return admin_home(chat, lang, mid)

    if data == "ref":
        ans(cq)
        return view(chat, mid, ref_text(u), ref_kb(uid, lang))

    if data.startswith("rp:"):                                   # "Claim reward" button under a showcase post
        try:
            cid = ObjectId(data[3:])
        except (InvalidId, TypeError):
            return ans(cq, tb(lang, "a_task_id_bad"), True)
        c = db.campaigns.find_one({"_id": cid})
        if not c or c.get("status") != "active" or int(c.get("remaining", 0)) <= 0:
            return ans(cq, tb(lang, "a_inactive"), True)
        deep = bot_deep_link(cid)
        if not deep:
            return ans(cq, tb(lang, "a_no_deeplink"), True)
        # The delay is per user and per task, and applies only to the claim button.
        click_id = f"reward:{cid}:{uid}"
        now = time.time()
        click = db.campaign_clicks.find_one({"_id": click_id})
        if not click:
            db.campaign_clicks.update_one({"_id": click_id}, {"$set": {"first_click_at": now}}, upsert=True)
            return ans(cq, tb(lang, "a_wait_first"), True)
        remaining_wait = 5 - (now - float(click.get("first_click_at", now)))
        if remaining_wait > 0:
            return ans(cq, tb(lang, "a_wait_claim", s=int(remaining_wait + 0.999)), True)
        return ans(cq, url=deep)                                 # opens the bot deep link directly in Telegram

    if data.startswith("c:"):
        return on_claim(cq, uid, chat, mid, data[2:])
    if data == "new":
        return start_new(cq, lang, chat, mid)
    if data.startswith("nt:"):
        return pick_type(cq, uid, lang, chat, mid, data[3:])
    if data == "ok":
        return confirm_new(cq, uid, lang, chat, mid)
    if data == "my":
        if not db.campaigns.find_one({"owner": uid}, {"_id": 1}):
            return ans(cq, tb(lang, "a_my_empty"), True)
        ans(cq)
        return render_my(uid, lang, chat, mid)
    if data == "rwatch":
        ans(cq)
        return view(chat, mid, reaction_watch_text(uid, lang),
                    [[B(tb(lang, "b_refresh"), "rwatch", "primary")], [B(tb(lang, "b_back"), "my")]])
    if data.startswith("rw:"):
        return choose_reaction_window(cq, uid, lang, chat, mid, data[3:])
    if data.startswith("x:"):
        return cancel_campaign(cq, uid, lang, chat, mid, data[2:], False)
    if data.startswith("xy:"):
        return cancel_campaign(cq, uid, lang, chat, mid, data[3:], True)
    ans(cq)


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
    commands = {
        "en": [("start", "Main menu"), ("gift", "Collect your daily gift"), ("top", "Weekly leaderboard"),
               ("invite", "Invite friends and earn"), ("language", "Change language"), ("help", "Help")],
        "fa": [("start", "منوی اصلی"), ("gift", "دریافت هدیه‌ی روزانه"), ("top", "جدول برترین‌های هفته"),
               ("invite", "دعوت از دوستان و دریافت پاداش"), ("language", "تغییر زبان"), ("help", "راهنما")],
    }
    as_json = lambda items: [{"command": c, "description": d} for c, d in items]
    tg("setMyCommands", commands=as_json(commands["en"]))                       # default (English)
    tg("setMyCommands", commands=as_json(commands["fa"]), language_code="fa")   # Persian clients
    log.info("webhook: %s | version %s", r, VERSION)
    if TASKS_CHANNEL:
        log.info("TASKS_CHANNEL configured: %s", TASKS_CHANNEL)
    else:
        log.warning("TASKS_CHANNEL is not set; automatic publishing to the showcase channel is disabled")
    threading.Thread(target=worker, daemon=True).start()


setup()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
