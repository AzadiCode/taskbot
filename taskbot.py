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
BOT_TOKEN, BASE_URL, MONGO_URI, SECRET_KEY, ADMIN_IDS, TASKS_CHANNEL, TASKS_CHANNEL_URL
START_POINTS, HOLD_HOURS, DAILY_TASKS, STRIKE_LIMIT, REF_JOIN_BONUS, REF_ACTIVE_BONUS,
REF_ACTIVE_TASKS, REF_DAILY_CAP, MAX_QTY, MAX_ACTIVE, REACTION_UPDATE_GRACE,
REACTION_BASELINE_MAX_AGE, ADMIN_MONITOR_INTERVAL
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
VERSION = "v49-bilingual-rewrite"

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
    tg("answerCallbackQuery", **p)


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
# Rules: English digits everywhere · no emoji · HTML only for structure · Persian is formal and uses half-spaces.
S = {
    # ───────── Buttons (plain text) ─────────
    "b_menu": ("🏠 Main menu", "🏠 منوی اصلی"),
    "b_back": ("↩️ Back", "↩️ بازگشت"),
    "b_cancel": ("Cancel", "انصراف"),
    "b_earn": ("⭐ Earn Stars", "⭐ کسب استارز"),
    "b_new": ("➕ Create campaign", "➕ ایجاد کمپین"),
    "b_my": ("📊 Campaigns", "📊 مدیریت کمپین‌ها"),
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
    "b_target": ("🎯 Target", "🎯 مقصد"),
    "b_claim": ("🎁 Claim", "🎁 دریافت پاداش"),
    "b_confirm": ("Confirm and pay {cost} Stars", "تأیید و پرداخت {cost} استارز"),
    "b_yes_cancel": ("Yes, cancel campaign", "بله، کمپین لغو شود"),
    "b_cancel_n": ("Cancel #{n}", "لغو شماره {n}"),
    "b_next": ("Next task", "تسک بعدی"),
    "b_watch": ("Reaction monitor", "پایش واکنش‌ها"),
    "b_win2": ("2 minutes", "2 دقیقه"),
    "b_win5": ("5 minutes", "5 دقیقه"),
    "b_cancel_campaign": ("Cancel campaign", "لغو کمپین"),
    "b_full": ("Capacity reached", "ظرفیت تکمیل شد"),
    "b_legacy": ("Retired verification method", "روش تأیید قدیمی غیرفعال"),
    "type_btn": ("{name} — {cost} Stars", "{name} — {cost} استارز"),

    # ───────── Plain labels ─────────
    "tn_join": ("Join", "عضویت"),
    "tn_react": ("Reaction", "واکنش"),
    "tn_view": ("View", "بازدید"),
    "st_active": ("Active", "فعال"),
    "st_done": ("Completed", "تکمیل‌شده"),
    "st_cancelled": ("Cancelled", "لغو شده"),
    "st_waiting": ("Awaiting update", "در انتظار به‌روزرسانی"),
    "st_full": ("Capacity reached", "ظرفیت تکمیل شد"),
    "st_legacy": ("Inactive (retired verification)", "غیرفعال (روش تأیید قدیمی)"),
    "st_admin_paused": ("Paused (bot access)", "متوقف موقت (دسترسی ربات)"),
    "friend": ("friend", "کاربر"),
    "unknown": ("unknown", "نامشخص"),
    "lbl_custom": ("Custom emoji", "ایموجی اختصاصی"),
    "lbl_paid": ("Paid reaction", "واکنش پولی"),
    "user_fallback": ("A user", "یک کاربر"),
    "campaign_fallback": ("Campaign", "کمپین"),
    "share_text": ("Complete tasks on TaskPlus, earn Stars and grow your channel.",
                   "در تاسک‌پلاس تسک انجام دهید، استارز کسب کنید و کانال خود را رشد دهید."),

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
        "<b>TaskPlus</b>\n<i>Task exchange for Telegram channels and groups</i>\n\n"
        "<blockquote><b>Balance:</b> <code>{points}</code> Stars{hold}</blockquote>\n\n"
        "Welcome, {name}. Complete tasks to earn Stars, then spend them on campaigns that grow your own channel.",
        "<b>تاسک‌پلاس</b>\n<i>سامانه‌ی تبادل تسک برای کانال‌ها و گروه‌های تلگرام</i>\n\n"
        "<blockquote><b>موجودی:</b> <code>{points}</code> استارز{hold}</blockquote>\n\n"
        "{name} گرامی، خوش آمدید. با انجام تسک استارز کسب کنید و از آن برای رشد کانال خود استفاده نمایید."),
    "hold_line": ("\n<b>Pending:</b> <code>{hold}</code> Stars", "\n<b>در انتظار تأیید:</b> <code>{hold}</code> استارز"),

    "help": (
        "<b>Help</b>\n<i>How TaskPlus works</i>\n\n"
        "<b>⭐ Earn Stars</b>\n"
        "<blockquote>Open a task in the showcase channel, complete it, tap <b>Claim reward</b> under the post and confirm here.\n\n"
        "<b>Join</b> — {r_join} Stars. Membership is verified and paid instantly.\n"
        "<b>Reaction</b> — {r_react} Stars. Verified per user in groups and by count increase in channels.\n"
        "<b>View</b> — {r_view} coin. Paid after the required waiting time.</blockquote>\n\n"
        "<b>Create a campaign</b>\n"
        "<blockquote>Choose a task type, then send the destination link and the quantity. The cost is shown before you confirm. "
        "For join and group-reaction tasks the bot must be an administrator of the destination.</blockquote>\n\n"
        "<b>Invite friends</b>\n"
        "<blockquote>Earn {ref_join} Stars when a friend joins, and {ref_active} more once they become active "
        "({ref_tasks} verified join or reaction tasks).</blockquote>\n\n"
        "<b>Rules</b>\n"
        "<blockquote>Each campaign counts once per person. Leaving a channel or removing a reaction during the "
        "{hold_h}-hour review period reverses the reward and adds one warning. After {strike_limit} warnings, "
        "task completion is disabled.</blockquote>\n\n"
        "<blockquote expandable><b>About channel reactions</b>\n"
        "Channel reactions are verified by the increase in the post's total reaction count. This measures how many "
        "reactions were added, not who added them. Telegram may deliver count updates a few minutes late.</blockquote>",
        "<b>راهنما</b>\n<i>نحوه‌ی کار تاسک‌پلاس</i>\n\n"
        "<b>⭐ کسب استارز</b>\n"
        "<blockquote>تسک را در کانال ویترین انتخاب و انجام دهید، سپس زیر همان پست روی «دریافت پاداش» بزنید و اینجا تأیید کنید.\n\n"
        "<b>عضویت</b> — {r_join} استارز. عضویت بررسی و پاداش بلافاصله واریز می‌شود.\n"
        "<b>واکنش</b> — {r_react} استارز. در گروه به‌صورت فردی و در کانال بر اساس افزایش شمارش تأیید می‌شود.\n"
        "<b>بازدید</b> — {r_view} استارز. پس از رعایت زمان انتظار پرداخت می‌شود.</blockquote>\n\n"
        "<b>ایجاد کمپین</b>\n"
        "<blockquote>نوع تسک را انتخاب کنید و سپس لینک مقصد و تعداد را ارسال نمایید. هزینه پیش از تأیید نمایش داده می‌شود. "
        "برای تسک‌های عضویت و واکنش در گروه، ربات باید در مقصد ادمین باشد.</blockquote>\n\n"
        "<b>دعوت از دوستان</b>\n"
        "<blockquote>با ورود هر دوست {ref_join} استارز و پس از فعال شدن او ({ref_tasks} تسک عضویت یا واکنش تأییدشده) "
        "{ref_active} استارز دیگر دریافت می‌کنید.</blockquote>\n\n"
        "<b>قوانین</b>\n"
        "<blockquote>هر کمپین برای هر نفر فقط یک بار محاسبه می‌شود. خروج از کانال یا حذف واکنش در دوره‌ی بررسی "
        "({hold_h} ساعت) موجب برگشت پاداش و ثبت یک اخطار می‌شود. پس از {strike_limit} اخطار، انجام تسک غیرفعال خواهد شد.</blockquote>\n\n"
        "<blockquote expandable><b>درباره‌ی واکنش در کانال</b>\n"
        "واکنش‌های کانال بر اساس افزایش شمارش کل واکنش‌های پست تأیید می‌شوند. این روش تعداد واکنش‌های افزوده‌شده را "
        "می‌سنجد، نه هویت واکنش‌دهنده را. ممکن است تلگرام به‌روزرسانی شمارش را با چند دقیقه تأخیر ارسال کند.</blockquote>"),

    "account": (
        "<b>Account</b>\n<i>Balance and activity overview</i>\n\n"
        "<blockquote><b>Balance:</b> <code>{points}</code> Stars{hold}\n"
        "<b>Total earned:</b> <code>{earned}</code> Stars</blockquote>\n"
        "<blockquote><b>Invited:</b> <code>{refs}</code> ({ref_active} active)\n"
        "<b>Active campaigns:</b> <code>{active}</code> / {max_active}\n"
        "<b>Warnings:</b> <code>{strikes}</code> / {strike_limit}</blockquote>",
        "<b>حساب کاربری</b>\n<i>خلاصه‌ی موجودی و فعالیت</i>\n\n"
        "<blockquote><b>موجودی:</b> <code>{points}</code> استارز{hold}\n"
        "<b>مجموع درآمد:</b> <code>{earned}</code> استارز</blockquote>\n"
        "<blockquote><b>دعوت‌شده‌ها:</b> <code>{refs}</code> ({ref_active} فعال)\n"
        "<b>کمپین‌های فعال:</b> <code>{active}</code> از {max_active}\n"
        "<b>اخطارها:</b> <code>{strikes}</code> از {strike_limit}</blockquote>"),

    "earn": (
        "<b>⭐ Earn Stars</b>\n<i>Tasks are published in the showcase channel</i>\n\n"
        "<blockquote><b>1.</b> Choose a task and complete it.\n"
        "<b>2.</b> Tap <b>Claim reward</b> under the same post.\n"
        "<b>3.</b> Confirm completion here.</blockquote>\n\n"
        "Rewards are paid after an automatic check.",
        "<b>⭐ کسب استارز</b>\n<i>تسک‌ها در کانال ویترین منتشر می‌شوند</i>\n\n"
        "<blockquote><b>1.</b> یک تسک را انتخاب و انجام دهید.\n"
        "<b>2.</b> زیر همان پست روی «دریافت پاداش» بزنید.\n"
        "<b>3.</b> انجام تسک را اینجا تأیید کنید.</blockquote>\n\n"
        "پاداش پس از بررسی خودکار واریز می‌شود."),
    "earn_unset": (
        "<b>Showcase unavailable</b>\n\nThe task channel has not been configured yet. Please try again later.",
        "<b>ویترین در دسترس نیست</b>\n\nکانال تسک‌ها هنوز تنظیم نشده است. لطفاً کمی بعد دوباره مراجعه کنید."),

    # ───────── Referral ─────────
    "ref": (
        "<b>Invite friends</b>\n<i>Earn twice for every friend</i>\n\n"
        "<blockquote><b>1.</b> A friend joins with your link — <code>{join}</code> Stars\n"
        "<b>2.</b> The friend becomes active ({tasks} join or reaction tasks) — <code>{active}</code> Stars</blockquote>\n"
        "Every active friend is worth <code>{total}</code> Stars in total.\n\n"
        "<blockquote><b>Invited:</b> <code>{count}</code>\n"
        "<b>Active:</b> <code>{active_count}</code>\n"
        "<b>Referral earnings:</b> <code>{earned}</code> Stars</blockquote>{friends}{capnote}\n\n"
        "<b>Your personal link</b>\n{link}",
        "<b>دعوت از دوستان</b>\n<i>برای هر دوست، دو بار پاداش بگیرید</i>\n\n"
        "<blockquote><b>1.</b> ورود دوست با لینک شما — <code>{join}</code> استارز\n"
        "<b>2.</b> فعال شدن دوست ({tasks} تسک عضویت یا واکنش) — <code>{active}</code> استارز</blockquote>\n"
        "هر دوست فعال در مجموع <code>{total}</code> استارز برای شما دارد.\n\n"
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
        "<b>New referral</b>\n\n<blockquote>{name} joined with your link.\n<b>Reward:</b> <code>+{bonus}</code> Stars</blockquote>\n"
        "You will receive {active_bonus} more Stars once they complete {tasks} tasks.",
        "<b>دعوت جدید</b>\n\n<blockquote>{name} با لینک شما وارد ربات شد.\n<b>پاداش:</b> <code>+{bonus}</code> استارز</blockquote>\n"
        "پس از انجام {tasks} تسک توسط او، {active_bonus} استارز دیگر دریافت خواهید کرد."),
    "n_ref_active": (
        "<b>Referral activated</b>\n\n<blockquote>{name} completed {tasks} tasks.\n<b>Reward:</b> <code>+{bonus}</code> Stars</blockquote>",
        "<b>فعال‌سازی دعوت</b>\n\n<blockquote>{name} {tasks} تسک انجام داد.\n<b>پاداش:</b> <code>+{bonus}</code> استارز</blockquote>"),

    # ───────── Claim flow ─────────
    "claim_page": (
        "<b>Claim reward</b>\n\nIf you have completed the task{reacted}, tap <b>I have completed it</b>. "
        "The result will be shown here after verification.",
        "<b>دریافت پاداش</b>\n\nاگر تسک را انجام داده‌اید{reacted}، روی «انجام دادم» بزنید. "
        "نتیجه پس از بررسی همین‌جا اعلام می‌شود."),
    "claim_reacted": (" and added the reaction", " و واکنش را ثبت کرده‌اید"),
    "task_paid": (
        "<b>Task verified</b>\n\n<blockquote><b>Reward:</b> <code>+{reward}</code> Stars</blockquote>\nThe Stars were added to your balance.",
        "<b>تسک تأیید شد</b>\n\n<blockquote><b>پاداش:</b> <code>+{reward}</code> استارز</blockquote>\nاستارز به موجودی شما افزوده شد."),
    "count_pending": (
        "<b>Request received</b>\n\n<blockquote><b>Reward:</b> <code>{reward}</code> Stars\n<b>Time limit:</b> {window} minutes</blockquote>\n"
        "Waiting for Telegram's count update, which may arrive up to {grace} minutes late. "
        "If a valid increase is recorded, the reward is paid automatically and you will be notified.",
        "<b>درخواست ثبت شد</b>\n\n<blockquote><b>پاداش:</b> <code>{reward}</code> استارز\n<b>مهلت:</b> {window} دقیقه</blockquote>\n"
        "در انتظار به‌روزرسانی شمارش تلگرام هستیم؛ ممکن است تا {grace} دقیقه دیرتر برسد. "
        "در صورت ثبت افزایش معتبر، پاداش به‌صورت خودکار واریز می‌شود و به شما اطلاع داده خواهد شد."),
    "count_ok": (
        "<b>Increase verified</b>\n\nThe reaction increase was verified and your reward has been paid.",
        "<b>افزایش تأیید شد</b>\n\nافزایش واکنش تأیید و پاداش شما پرداخت شد."),
    "n_paid": (
        "<b>Task verified</b>\n\n<blockquote><b>Reward:</b> <code>+{reward}</code> Stars</blockquote>",
        "<b>تسک تأیید شد</b>\n\n<blockquote><b>پاداش:</b> <code>+{reward}</code> استارز</blockquote>"),
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
    "a_paid": ("Verified. Reward paid.", "تأیید شد. پاداش واریز شد."),
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
    "a_published": ("Campaign published.", "کمپین منتشر شد."),
    "a_cancel_inactive": ("This campaign is not active.", "این کمپین فعال نیست."),
    "a_cancelled": ("Cancelled. {n} Stars refunded.", "لغو شد. {n} استارز بازگردانده شد."),

    # ───────── Create-campaign flow ─────────
    "new": (
        "<b>New campaign</b>\n<i>Choose what you want to promote</i>\n\nThe price per unit is shown on each button.",
        "<b>ایجاد کمپین</b>\n<i>فعالیت موردنظر برای ترویج را انتخاب کنید</i>\n\nهزینه‌ی هر واحد روی دکمه‌ها درج شده است."),
    "prompt_join": (
        "<b>Join destination</b>\n\nSend the public username or link of the channel or group, for example <code>@mychannel</code>.\n\n"
        "<blockquote>First add <code>@{bot}</code> as an administrator of the destination so memberships can be verified.</blockquote>",
        "<b>مقصد عضویت</b>\n\nنام کاربری یا لینک عمومی کانال یا گروه را ارسال کنید؛ برای مثال <code>@mychannel</code>.\n\n"
        "<blockquote>ابتدا ربات <code>@{bot}</code> را در مقصد ادمین کنید تا بتواند عضویت‌ها را بررسی کند.</blockquote>"),
    "prompt_post": (
        "<b>Destination post</b>\n\nSend the post link, for example <code>https://t.me/channel/123</code>, "
        "or forward the channel post to this chat.\n\n"
        "<blockquote>Forwarded group messages cannot be identified. For groups, always send the message link.</blockquote>",
        "<b>پست مقصد</b>\n\nلینک پست را ارسال کنید؛ برای مثال <code>https://t.me/channel/123</code>؛ "
        "یا خود پست کانال را به این گفتگو فوروارد نمایید.\n\n"
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
        "<b>Monitoring setup</b>\n\nA live update of this post's reaction count is required before monitoring can start.\n\n"
        "<blockquote>Add or remove a reaction on the post inside the channel (or ask an admin to), then wait one to a few minutes. "
        "When Telegram delivers the count, you will be moved to the next step automatically.</blockquote>\n"
        "<i>Telegram does not expose reaction counts instantly through the API, so updates can be delayed by a few minutes.</i>",
        "<b>راه‌اندازی پایش</b>\n\nپیش از شروع پایش، دریافت یک به‌روزرسانی زنده از شمارش واکنش‌های این پست لازم است.\n\n"
        "<blockquote>داخل کانال یک واکنش به همین پست اضافه یا حذف کنید (یا از یکی از ادمین‌ها بخواهید) و یک تا چند دقیقه صبر کنید. "
        "هنگامی که تلگرام شمارش را ارسال کند، به‌صورت خودکار به مرحله‌ی بعد منتقل می‌شوید.</blockquote>\n"
        "<i>تلگرام شمارش واکنش‌ها را از طریق API فوراً در اختیار قرار نمی‌دهد؛ ممکن است به‌روزرسانی‌ها چند دقیقه تأخیر داشته باشند.</i>"),
    "baseline_pending": ("The count update has not arrived yet. Add or remove a reaction on the same post and wait a moment.",
                         "به‌روزرسانی شمارش هنوز نرسیده است. یک واکنش روی همان پست اضافه یا حذف کنید و کمی صبر نمایید."),
    "baseline_need_action": ("<b>Fresh count required</b>\nAdd or remove a reaction on the same post and wait for the update.",
                             "<b>شمارش تازه لازم است</b>\nیک واکنش به همان پست اضافه یا حذف کنید و منتظر به‌روزرسانی بمانید."),
    "baseline_restart": ("<b>Fresh count required</b>\nStart the reaction campaign again from the menu.",
                         "<b>شمارش تازه لازم است</b>\nثبت کمپین واکنش را دوباره از منو آغاز کنید."),
    "window_ready": (
        "<b>Monitoring ready</b>\n\n<blockquote><b>Latest count:</b> <code>{total}</code> reactions</blockquote>\n"
        "Choose the time limit each user has to complete the task. Telegram may deliver count updates with some delay.",
        "<b>پایش آماده است</b>\n\n<blockquote><b>آخرین شمارش:</b> <code>{total}</code> واکنش</blockquote>\n"
        "مهلتی را انتخاب کنید که هر کاربر برای انجام تسک در اختیار دارد. ممکن است تلگرام به‌روزرسانی شمارش را با تأخیر ارسال کند."),
    "window_received": (
        "<b>Count received</b>\n\n<blockquote><b>Reactions:</b> <code>{total}</code></blockquote>\nChoose the time limit for each task:",
        "<b>شمارش دریافت شد</b>\n\n<blockquote><b>واکنش‌ها:</b> <code>{total}</code></blockquote>\nمهلت انجام هر تسک را انتخاب کنید:"),
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
        "<b>Campaign summary</b>\n\n<blockquote><b>Type:</b> {type}\n<b>Target:</b> {title}\n"
        "<b>Quantity:</b> <code>{qty}</code>\n<b>Cost:</b> <code>{total}</code> Stars\n<b>Verification:</b> {how}</blockquote>"
        "{admin_note}\n\nConfirm to publish this campaign.",
        "<b>خلاصه‌ی کمپین</b>\n\n<blockquote><b>نوع:</b> {type}\n<b>مقصد:</b> {title}\n"
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
        "<b>{type}</b> · {target}\n\n<blockquote><b>Reward:</b> <code>{reward}</code> Stars\n"
        "<b>Remaining spots:</b> <code>{remaining}</code>\n<b>Status:</b> {state}</blockquote>\n{instruction}",
        "<b>{type}</b> · {target}\n\n<blockquote><b>پاداش:</b> <code>{reward}</code> استارز\n"
        "<b>ظرفیت باقی‌مانده:</b> <code>{remaining}</code>\n<b>وضعیت:</b> {state}</blockquote>\n{instruction}"),
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
        "<b>Campaign completed</b>\n\n<blockquote>{type} — {title}</blockquote>\n"
        "Final verification runs within {hold_h} hours. If a participant breaks the rules, their spot reopens.",
        "<b>کمپین تکمیل شد</b>\n\n<blockquote>{type} — {title}</blockquote>\n"
        "تأیید نهایی طی {hold_h} ساعت انجام می‌شود و در صورت تخلف هر شرکت‌کننده، ظرفیت او دوباره باز می‌شود."),
    "n_full_count": (
        "<b>Campaign completed</b>\n\n<blockquote>{type} — {title}</blockquote>\nThe reaction increases were verified.",
        "<b>کمپین تکمیل شد</b>\n\n<blockquote>{type} — {title}</blockquote>\nافزایش واکنش‌ها تأیید شد."),
    "n_admin_restored": (
        "<b>Access restored</b>\n\nThe bot's access to <b>{title}</b> is back and the campaign is active again.",
        "<b>دسترسی بازیابی شد</b>\n\nدسترسی ربات در «{title}» برگشت و کمپین دوباره فعال است."),
    "n_admin_lost": (
        "<b>Campaign paused</b>\n\nThe bot lost administrator access to <b>{title}</b>. The campaign is paused and resumes once access is restored.",
        "<b>کمپین متوقف شد</b>\n\nربات دسترسی ادمین «{title}» را از دست داده است. کمپین موقتاً متوقف شد و پس از بازیابی دسترسی ادامه می‌یابد."),

    # ───────── Admin ─────────
    "ad_stats": (
        "<b>Statistics</b>\n\n<blockquote><b>Users:</b> <code>{users}</code>\n<b>Active campaigns:</b> <code>{active}</code>\n"
        "<b>Pending:</b> <code>{pending}</code>\n<b>Paid:</b> <code>{paid}</code>\n<b>Reversed:</b> <code>{rev}</code>\n"
        "<b>Referred:</b> <code>{refs}</code> ({refs_active} active)</blockquote>",
        "<b>آمار</b>\n\n<blockquote><b>کاربران:</b> <code>{users}</code>\n<b>کمپین‌های فعال:</b> <code>{active}</code>\n"
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
LANG_PICK = ("<b>Choose your language</b>\n<i>Select the language the bot will use.</i>\n\n"
             + RLM + "<b>زبان خود را انتخاب کنید</b>\n" + RLM + "<i>زبان مورد استفاده‌ی ربات را انتخاب نمایید.</i>")


# ═════════════════════════════ Database helpers ═════════════════════════════
def day():
    return time.strftime("%Y-%m-%d", time.gmtime())


def ledger(uid, delta, kind, ref=""):
    db.ledger.insert_one({"uid": uid, "d": delta, "k": kind, "ref": str(ref), "t": time.time()})


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
def main_kb(lang):
    # Direct URL button for Earn Stars when the showcase URL is configured.
    showcase_url = tasks_channel_url()
    earn_button = U(tb(lang, "b_earn"), showcase_url, "primary") if showcase_url else B(tb(lang, "b_earn"), "earn", "primary")
    return [[earn_button],
            [B(tb(lang, "b_my"), "campaigns", "success")],
            [B(tb(lang, "b_acc"), "acc")]]


def account_kb(lang):
    return [[B(tb(lang, "b_ref"), "ref", "success")],
            [B(tb(lang, "b_help"), "help"), B(tb(lang, "b_lang"), "lang")],
            [B(tb(lang, "b_menu"), "home")]]


def campaigns_kb(lang):
    return [[B(tb(lang, "b_new"), "new", "primary")],
            [B(tb(lang, "b_my"), "my")],
            [B(tb(lang, "b_menu"), "home")]]


def home_kb(lang):
    return [[B(tb(lang, "b_menu"), "home")]]


def cancel_kb(lang):
    return [[B(tb(lang, "b_cancel"), "home", "danger")]]


def lang_kb():
    return [[B("English", "lg:en"), B("فارسی", "lg:fa")]]


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
    return t(lang, "menu", name=name, points=u.get("points", 0), hold=hold)


def help_text(lang):
    return t(lang, "help", r_join=TYPES["join"]["reward"], r_react=TYPES["react"]["reward"], r_view=TYPES["view"]["reward"],
             ref_join=REF_JOIN_BONUS, ref_active=REF_ACTIVE_BONUS, ref_tasks=REF_ACTIVE_TASKS,
             hold_h=f"{HOLD_HOURS:g}", strike_limit=STRIKE_LIMIT)


def acc_text(u):
    lang = lang_of(u)
    active = db.campaigns.count_documents({"owner": u["_id"], "status": "active"})
    hold = t(lang, "hold_line", hold=u["hold"]) if u.get("hold") else H("")
    return t(lang, "account", points=u.get("points", 0), hold=hold, earned=u.get("earned", 0),
             refs=u.get("ref_count", 0), ref_active=u.get("ref_active_count", 0), active=active,
             max_active=MAX_ACTIVE, strikes=u.get("strikes", 0), strike_limit=STRIKE_LIMIT)


def acc_kb(lang):
    return account_kb(lang)


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
    return t(lang, "ref", join=REF_JOIN_BONUS, tasks=REF_ACTIVE_TASKS, active=REF_ACTIVE_BONUS,
             total=REF_JOIN_BONUS + REF_ACTIVE_BONUS, count=int(u.get("ref_count", 0)),
             active_count=int(u.get("ref_active_count", 0)), earned=int(u.get("ref_earned", 0)),
             friends=friends_html, capnote=cap, link=link_html)


def ref_kb(uid, lang):
    link = ref_link(uid)
    rows = []
    if link:
        rows.append([U(tb(lang, "b_share"),
                       f"https://t.me/share/url?url={quote(link, safe='')}&text={quote(tb(lang, 'share_text'))}", "primary")])
    rows.append([B(tb(lang, "b_menu"), "home")])
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


def show_task(lang, chat, mid=None):
    """Earning always starts from the showcase channel."""
    url = tasks_channel_url()
    if not url:
        return view(chat, mid, t(lang, "earn_unset"), home_kb(lang))
    return view(chat, mid, t(lang, "earn"),
                [[U(tb(lang, "b_showcase"), url, "primary")], [B(tb(lang, "b_menu"), "home")]])


def grant(uid, c, status, extra=None):
    """Take one spot from a campaign and record the completion. Returns (code, comp); code: ok | full | dup | limit."""
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
        notify(c["owner"], "n_full", type=K("tn_" + c["type"]), title=ctitle(c), hold_h=f"{HOLD_HOURS:g}")
    return "ok", comp


def claim_alert(lang, code):
    return {"limit": tb(lang, "a_limit", n=DAILY_TASKS), "full": tb(lang, "a_full"), "dup": tb(lang, "a_dup")}[code]


def settle_instantly(comp, **extra):
    moved = db.completions.update_one({"_id": comp["_id"], "status": "pending"},
                                      {"$set": dict(status="settling", lock=time.time(), **extra)})
    if moved.modified_count:
        pay(comp, announce=False)


def next_kb(lang):
    return [[B(tb(lang, "b_next"), "earn", "success"), B(tb(lang, "b_acc"), "acc")], [B(tb(lang, "b_menu"), "home")]]


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
            ans(cq, claim_alert(lang, code), True)
            return show_task(lang, chat, mid)
        ans(cq, tb(lang, "a_count_pending"))
        if consume_credit_for_claim(comp):
            return view(chat, mid, t(lang, "count_ok"), next_kb(lang))
        return view(chat, mid, t(lang, "count_pending", reward=c["reward"], window=window_min,
                                 grace=REACTION_UPDATE_GRACE // 60),
                    [[U(tb(lang, "b_back_target"), link_of(c), "primary")], [B(tb(lang, "b_menu"), "home")]])

    # Join / group-reaction are verified right now, and views after the waiting time: pay immediately.
    code, comp = grant(uid, c, "pending")
    if code != "ok":
        ans(cq, claim_alert(lang, code), True)
        return show_task(lang, chat, mid)
    settle_instantly(comp, **({"verified_at": time.time(), "due": time.time()} if c["type"] == "view" else {}))
    ans(cq, tb(lang, "a_paid"))
    return view(chat, mid, t(lang, "task_paid", reward=c["reward"]), next_kb(lang))


def open_linked_task(uid, chat, cid_s):
    u = db.users.find_one({"_id": uid}) or {}
    lang = lang_of(u)
    try:
        cid = ObjectId(cid_s)
    except (InvalidId, TypeError):
        return send(chat, t(lang, "e_link_invalid"), main_kb(lang))
    c = db.campaigns.find_one({"_id": cid})
    if not c or c.get("status") != "active" or int(c.get("remaining", 0)) <= 0:
        return send(chat, t(lang, "e_no_capacity"), main_kb(lang))
    if is_legacy(c):
        return send(chat, t(lang, "e_legacy"), main_kb(lang))
    showcase = [[U(tb(lang, "b_back_showcase"), tasks_channel_url(), "primary")]] if tasks_channel_url() else main_kb(lang)
    if campaign_needs_admin(c):
        if c.get("admin_paused"):
            return send(chat, t(lang, "e_paused"), showcase)
        status = bot_admin_status(c.get("chat_id"))
        if status is None:
            return send(chat, t(lang, "e_admin_unavail"), main_kb(lang))
        if not status:
            pause_for_admin_loss(c)
            return send(chat, t(lang, "e_paused_now"), main_kb(lang))
    if c.get("owner") == uid:
        return send(chat, t(lang, "e_own"), main_kb(lang))
    if db.completions.find_one({"uid": uid, "cid": cid}):
        return send(chat, t(lang, "e_done_before"), showcase)
    if u.get("strikes", 0) >= STRIKE_LIMIT:
        return send(chat, t(lang, "e_strikes"), main_kb(lang))
    if c.get("type") == "react" and c.get("mode") == "count":
        snap = fresh_reaction_snapshot(c.get("chat_id"), c.get("mid"))
        if not snap:
            return send(chat, t(lang, "e_count_stale"), main_kb(lang))
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
    if announce:
        notify(comp["uid"], "n_paid", kb=next_kb, reward=comp["reward"])
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


def worker():
    while True:
        try:
            tick()
        except Exception:
            log.exception("tick")
        time.sleep(30)


# ═════════════════════════════ Create campaign (multi-step) ═════════════════════════════
POST_RE = re.compile(r"(?:https?://)?(?:t\.me|telegram\.me)/([A-Za-z][A-Za-z0-9_]{3,31})/(\d+)", re.I)
CHAN_RE = re.compile(r"^(?:https?://)?(?:(?:t\.me|telegram\.me)/)?@?([A-Za-z][A-Za-z0-9_]{3,31})/?$", re.I)
FA_DIGITS = str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789")   # accept Persian/Arabic digits as input


def needs_admin_mode(typ, mode):
    return typ == "join" or (typ == "react" and mode == "auto")


def start_new(cq, lang, chat, mid):
    ans(cq)
    rows = [[B(tb(lang, "type_btn", name=tn(lang, k), cost=v["cost"]), f"nt:{k}")] for k, v in TYPES.items()]
    view(chat, mid, t(lang, "new"), rows + [[B(tb(lang, "b_menu"), "home")]])


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
    send(chat, t(lang, "qty_prompt", max=mx), cancel_kb(lang))
    return True


def on_state_text(uid, chat, text, st, lang):
    if time.time() - st.get("t", 0) > 3600:
        db.states.delete_one({"_id": uid})
        return send(chat, t(lang, "e_expired"), main_kb(lang))
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
    send(chat, t(lang, "e_continue_menu"), main_kb(lang))


def choose_reaction_window(cq, uid, lang, chat, mid, value):
    st = db.states.find_one({"_id": uid})
    if not st or st.get("step") != "window":
        ans(cq, tb(lang, "a_step_expired"), True)
        return view(chat, mid, t(lang, "e_restart"), main_kb(lang))
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
        return view(chat, mid, t(lang, "e_restart"), main_kb(lang))
    d, typ = st["d"], st["d"]["type"]
    cfg = TYPES[typ]
    snap = None
    if typ == "react" and d.get("mode") == "count":
        snap = fresh_reaction_snapshot(d.get("chat_id"), d.get("mid"))
        if not snap:
            ans(cq, tb(lang, "a_baseline_stale"), True)
            db.states.delete_one({"_id": uid})
            return view(chat, mid, t(lang, "baseline_restart"), main_kb(lang))
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
                    [[B(tb(lang, "b_my"), "my")], [B(tb(lang, "b_menu"), "home")]])
    ans(cq, tb(lang, "a_published"))
    kb = []
    if tasks_channel_url():
        kb.append([U(tb(lang, "b_view_showcase"), tasks_channel_url(), "primary")])
    kb += [[B(tb(lang, "b_my"), "my")], [B(tb(lang, "b_menu"), "home")]]
    view(chat, mid, t(lang, "published"), kb)


# ═════════════════════════════ My campaigns ═════════════════════════════
def render_my(uid, lang, chat, mid):
    cs = list(db.campaigns.find({"owner": uid}, sort=[("created", -1)], limit=10))
    if not cs:
        return view(chat, mid, t(lang, "my_empty"), campaigns_kb(lang))
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
    view(chat, mid, t(lang, "my", rows=H("\n\n".join(rows))), kb + [[B(tb(lang, "b_menu"), "home")]])


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
    if cmd == "reactwatch":
        return send(chat, reaction_watch_text(uid, lang, all_admin=True))
    if cmd == "stats":
        by = {s: db.completions.count_documents({"status": s}) for s in ("pending", "paid", "reversed")}
        return send(chat, t(lang, "ad_stats", users=db.users.count_documents({}),
                            active=db.campaigns.count_documents({"status": "active"}),
                            pending=by["pending"], paid=by["paid"], rev=by["reversed"],
                            refs=db.users.count_documents({"referred_by": {"$exists": True}}),
                            refs_active=db.users.count_documents({"ref_active": True})))
    if cmd == "give" and len(args) == 2 and args[0].isdigit() and args[1].lstrip("-").isdigit():
        add_points(int(args[0]), int(args[1]), "admin")
        return send(chat, t(lang, "ad_done"))
    if cmd == "unban" and args and args[0].isdigit():
        db.users.update_one({"_id": int(args[0])}, {"$set": {"strikes": 0}})
        return send(chat, t(lang, "ad_unbanned"))
    if cmd == "stop" and args:
        try:
            r = db.campaigns.find_one_and_update({"_id": ObjectId(args[0]), "status": "active"}, {"$set": {"status": "cancelled"}})
        except InvalidId:
            r = None
        if r:
            add_points(r["owner"], r["remaining"] * r["cost"], "refund", r["_id"])
            refresh_campaign_listing(r["_id"])
        return send(chat, t(lang, "ad_stopped" if r else "ad_not_found"))
    send(chat, t(lang, "ad_usage"))


# ═════════════════════════════ Update routing ═════════════════════════════
def on_message(msg):
    if msg["chat"]["type"] != "private" or "from" not in msg:
        return
    uid, chat = msg["from"]["id"], msg["chat"]["id"]
    existed = db.users.find_one({"_id": uid}, {"_id": 1}) is not None
    u = ensure_user(msg["from"])
    lang = lang_of(u)
    text = (msg.get("text") or "").strip()

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
            return send(chat, menu_text(u), main_kb(lang))
        if cmd == "help":
            return send(chat, help_text(lang), home_kb(lang))
        if cmd in ("invite", "ref"):
            return send(chat, ref_text(u), ref_kb(uid, lang))
        if cmd in ("language", "lang"):
            return send(chat, LANG_PICK, lang_kb())
        if uid in ADMINS:
            return admin_cmd(uid, chat, cmd, args, lang)
        return send(chat, menu_text(u), main_kb(lang))

    st = db.states.find_one({"_id": uid})
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
    send(chat, menu_text(u), main_kb(lang))


def on_callback(cq):
    uid, data = cq["from"]["id"], cq.get("data", "")
    m = cq.get("message") or {}
    chat, mid = (m.get("chat") or {}).get("id"), m.get("message_id")
    u = ensure_user(cq["from"])
    if not chat:
        return ans(cq)

    if data.startswith("lg:"):                                   # language selection
        choice = data[3:] if data[3:] in ("en", "fa") else "en"
        db.users.update_one({"_id": uid}, {"$set": {"lang": choice}})
        u["lang"] = choice
        ans(cq)
        db.states.delete_one({"_id": uid})
        return view(chat, mid, menu_text(u), main_kb(choice))

    lang = lang_of(u)
    if data == "lang":
        ans(cq)
        return view(chat, mid, LANG_PICK, lang_kb())
    if data == "noop":
        return ans(cq)
    if data == "home":
        ans(cq)
        db.states.delete_one({"_id": uid})
        return view(chat, mid, menu_text(u), main_kb(lang))
    if data == "help":
        ans(cq)
        return view(chat, mid, help_text(lang), home_kb(lang))
    if data == "acc":
        ans(cq)
        return view(chat, mid, acc_text(u), acc_kb(lang))
    if data == "campaigns":
        ans(cq)
        return view(chat, mid, "<b>" + ("Campaign management" if lang == "en" else "مدیریت کمپین‌ها") + "</b>\n\n" +
                    ("Create a campaign or review your existing campaigns." if lang == "en" else "کمپین جدید بسازید یا وضعیت کمپین‌های خود را بررسی کنید."),
                    campaigns_kb(lang))
    if data == "earn":
        url = tasks_channel_url()
        if not url:
            ans(cq, tb(lang, "a_showcase_down"), True)
            return None
        return ans(cq, url=url)
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
        "en": [("start", "Main menu"), ("invite", "Invite friends and earn"), ("language", "Change language"), ("help", "Help")],
        "fa": [("start", "منوی اصلی"), ("invite", "دعوت از دوستان و دریافت پاداش"), ("language", "تغییر زبان"), ("help", "راهنما")],
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
