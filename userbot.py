# -*- coding: utf-8 -*-
"""
Helper account (MTProto / Telethon) that READS the live reaction counters of channel posts.

Why: the Bot API only sends aggregated `message_reaction_count` updates, batched and delayed (sometimes never).
A normal Telegram account can read the same counter on demand, within seconds.

Safety design (so the account is not limited):
  * strictly READ-ONLY: never joins, never sends, never reacts, no update stream (receive_updates=False)
  * one batched request per channel per cycle (all posts of a channel in a single getMessages call)
  * only posts of live orders are read; posts with a pending claim/draft ("hot") every ~8 s, the rest ("warm") ~45 s
  * random jitter, minimum gap between requests, hard cap of requests per minute
  * FloodWait is never retried early: it is honoured, intervals are doubled, and after 3 floods in an hour the
    helper switches itself off and tells the admins
  * enabled / disabled from the web panel; if it is off or unhealthy the normal bot flow keeps working
  * the session string is stored encrypted (Fernet, key derived from SECRET_KEY)

Only one gunicorn worker polls at a time (leader lock in MongoDB).
"""
import asyncio
import base64
import hashlib
import logging
import os
import random
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor

from pymongo.errors import DuplicateKeyError

log = logging.getLogger("userbot")

HOT_SEC = float(os.environ.get("UB_HOT_SEC", "8"))
WARM_SEC = float(os.environ.get("UB_WARM_SEC", "45"))
MAX_PER_MIN = int(os.environ.get("UB_MAX_PER_MIN", "20"))
MIN_GAP = (1.5, 2.5)          # seconds between two requests (random)
FEED_REFRESH_SEC = 45         # unchanged totals are re-sent at most this often (keeps the snapshot "fresh")
CONNECT_TIMEOUT = 20         # seconds to reach Telegram over MTProto before giving up
JOB_TIMEOUT = 40             # login steps always finish (or fail with a clear message) within this time
FLOOD_LIMIT = 3               # floods per hour before the helper switches itself off


_DIGITS = str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789")


def ascii_digits(text):
    """Persian/Arabic digits -> ASCII (Telegram rejects them in phone numbers and codes)."""
    return str(text or "").translate(_DIGITS)


def _libs_ok():
    try:
        import telethon  # noqa: F401
        import cryptography  # noqa: F401
        return True
    except Exception:
        return False


class Manager:
    def __init__(self, db, api_id, api_hash, secret, live_posts, feed, notify=None):
        self.db = db
        self.api_id = int(api_id) if str(api_id or "").strip().isdigit() else 0
        self.api_hash = (api_hash or "").strip()
        self.secret = secret or "x"
        self.live_posts = live_posts          # () -> {key: {"chat_id","mid","uname","hot"}}
        self.feed = feed                      # (mr_dict) -> None  (taskbot.on_reaction_count)
        self.notify = notify or (lambda text: None)
        self.configured = bool(self.api_id and self.api_hash)
        self.libs_ok = _libs_ok()
        self.me = f"{os.getpid()}-{random.randint(1000, 9999)}"
        self.loop = None
        self._ready = threading.Event()
        self._cfg_cache = (0.0, {})
        self._started = False
        # polling state (only used by the leader)
        self.client = None
        self._ents = {}
        self._bad = {}                        # chat_id -> retry-after timestamp (unreachable)
        self._next = {}                       # post key -> next due time
        self._last = {}                       # post key -> (total, fed_at)
        self._req = deque()                   # timestamps of requests (last minute)
        self._flood_until = 0.0
        self._floods = deque()
        self._slow = 1.0
        self._slow_since = time.time()
        self._hb = 0.0
        self._polled = 0
        self._qr = None
        self._lead_until = 0.0
        self._thread = None
        self._thread_err = ""
        self._pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="ub-bg")

    # ───────────────────────── crypto / storage ─────────────────────────
    def _fernet(self):
        from cryptography.fernet import Fernet
        key = base64.urlsafe_b64encode(hashlib.sha256(("userbot-session:" + self.secret).encode()).digest())
        return Fernet(key)

    def _enc(self, s):
        return self._fernet().encrypt(s.encode()).decode()

    def _dec(self, s):
        return self._fernet().decrypt(s.encode()).decode()

    def cfg(self):
        return self.db.userbot.find_one({"_id": "cfg"}) or {}

    def _cfg_cached(self, ttl=4.0):
        t, c = self._cfg_cache
        if time.time() - t > ttl:
            c = self.cfg()
            self._cfg_cache = (time.time(), c)
        return c

    def _set(self, **fields):
        self.db.userbot.update_one({"_id": "cfg"}, {"$set": fields}, upsert=True)
        self._cfg_cache = (0.0, {})

    def enabled_cached(self):
        """Cheap check used by the bot: is the helper switched on (and usable)?"""
        c = self._cfg_cached(8.0)
        return bool(self.configured and self.libs_ok and c.get("enabled") and c.get("session") and not c.get("session_invalid"))

    def healthy(self):
        c = self._cfg_cached(8.0)
        return self.enabled_cached() and time.time() - float(c.get("last_ok", 0) or 0) < 120

    # ───────────────────────── loop thread ─────────────────────────
    def start(self):
        """Start (or restart) the loop thread. Safe to call on every request: does nothing while the thread is alive."""
        if not (self.configured and self.libs_ok):
            return
        t = getattr(self, "_thread", None)
        if t is not None and t.is_alive():
            return
        self._started = True
        self._ready.clear()
        self._thread = threading.Thread(target=self._thread_main, daemon=True, name="userbot")
        self._thread.start()

    def _thread_main(self):
        try:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            # fresh executors owned by us (never the shared/default ones that may already be shut down)
            self._pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="ub-bg")
            self.loop.set_default_executor(ThreadPoolExecutor(max_workers=16, thread_name_prefix="ub-dns"))
            self._thread_err = ""
            self._ready.set()
            self.loop.create_task(self._main())
            self.loop.run_forever()
        except BaseException as e:
            self._thread_err = f"{type(e).__name__}: {str(e)[:160]}"
            log.exception("userbot loop thread crashed")
        finally:
            self._ready.clear()

    def _not_running(self):
        return RuntimeError("userbot loop is not running" + (f" ({self._thread_err})" if getattr(self, "_thread_err", "") else ""))

    def call(self, coro, timeout=60):
        if not self._ready.wait(8):
            coro.close()
            raise self._not_running()
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result(timeout)

    async def _bg(self, fn, *a):
        # Own pool instead of the loop's default executor: the default one is shut down
        # when the interpreter/worker exits -> "cannot schedule new futures after shutdown".
        try:
            return await self.loop.run_in_executor(self._pool, fn, *a)
        except RuntimeError as e:
            if "shutdown" not in str(e) and "interpreter" not in str(e):
                raise
            try:
                self._pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="ub-bg")
                return await self.loop.run_in_executor(self._pool, fn, *a)
            except RuntimeError:
                return fn(*a)          # last resort: tiny DB call, run inline

    async def _connect(self, client):
        await asyncio.wait_for(client.connect(), CONNECT_TIMEOUT)

    @staticmethod
    async def _bye(client):
        try:
            await asyncio.wait_for(client.disconnect(), 5)
        except Exception:
            pass

    async def _client(self, session="", updates=False):
        from telethon import TelegramClient
        from telethon.sessions import StringSession
        return TelegramClient(StringSession(session), self.api_id, self.api_hash,
                              device_model="TaskPlus Server", system_version="Linux", app_version="1.0", lang_code="en",
                              receive_updates=updates, flood_sleep_threshold=0,
                              connection_retries=3, retry_delay=3, request_retries=2, auto_reconnect=True)

    # ───────────────────────── panel API (sync wrappers) ─────────────────────────
    def checks(self):
        """What is missing? (values are never shown)"""
        def mod(name):
            try:
                m = __import__(name)
                return str(getattr(m, "__version__", "ok"))
            except Exception:
                return None
        return {"TG_API_ID": bool(self.api_id), "TG_API_HASH": len(self.api_hash) == 32,
                "telethon": mod("telethon"), "cryptography": mod("cryptography"), "segno": mod("segno")}

    def status(self):
        c = self.cfg()
        login = self.db.userbot.find_one({"_id": "login"}) or {}
        now = time.time()
        floods = [t for t in (c.get("floods") or []) if now - t < 3600]
        return {"configured": self.configured, "libs": self.libs_ok, "logged_in": bool(c.get("session")),
                "enabled": bool(c.get("enabled")), "user": c.get("user"), "phone": c.get("phone_masked"),
                "state": c.get("state") or "", "last_ok": c.get("last_ok"), "last_error": c.get("last_error") or "",
                "floods_1h": len(floods), "polled": int(c.get("polled", 0) or 0), "healthy": self.healthy(),
                "invalid": bool(c.get("session_invalid")),
                "login_stage": login.get("stage") if login and now - float(login.get("at", 0)) < 900 else ""}

    def _submit(self, coro, timeout=None):
        """Run a slow Telegram call in the background; the HTTP request returns at once (gunicorn kills requests > 30 s)."""
        timeout = timeout or JOB_TIMEOUT
        jid = f"{int(time.time() * 1000)}{random.randint(100, 999)}"
        now = time.time()
        try:
            self.db.userbot.delete_many({"_id": {"$regex": "^job:"}, "at": {"$lt": now - 600}})
        except Exception:
            pass
        self.db.userbot.update_one({"_id": "job:" + jid}, {"$set": {"status": "running", "at": now}}, upsert=True)

        async def runner():
            try:
                res = await asyncio.wait_for(coro, timeout)
            except asyncio.TimeoutError:
                res = {"ok": False, "error": "اتصال به سرورهای تلگرام (MTProto) برقرار نشد؛ سرور شما شاید دسترسی خروجی TCP به تلگرام ندارد."}
            except Exception as e:
                log.exception("userbot job")
                res = {"ok": False, "error": f"{type(e).__name__}: {str(e)[:160]}"}
            await self._bg(self._job_done, jid, res)

        if not self._ready.wait(8):
            coro.close()
            raise self._not_running()
        asyncio.run_coroutine_threadsafe(runner(), self.loop)
        return {"ok": True, "job": jid}

    def _job_done(self, jid, res):
        self.db.userbot.update_one({"_id": "job:" + jid}, {"$set": {"status": "done", "res": res, "at": time.time()}}, upsert=True)

    def _job_status(self, jid):
        j = self.db.userbot.find_one({"_id": "job:" + str(jid)})
        if not j:
            return {"ok": False, "error": "job lost"}
        if j.get("status") != "done":
            return {"ok": True, "running": True}
        return j.get("res") or {"ok": False, "error": "empty result"}

    def op(self, name, body):
        """Single entry point for the panel. Never raises, never blocks longer than a few seconds."""
        try:
            self.start()
            if not (self.configured and self.libs_ok) and name != "status":
                return {"ok": False, "error": "TG_API_ID / TG_API_HASH یا کتابخانه‌ی telethon تنظیم نشده است."}
            if name == "status":
                return {"ok": True, **self.status()}
            if name == "job":
                return self._job_status(body.get("id"))
            if name == "checks":
                return {"ok": True, **self.checks()}
            if name == "send_code":
                return self._submit(self._send_code(str(body.get("phone") or "")))
            if name == "sign_in":
                return self._submit(self._sign_in(str(body.get("code") or "").strip(), str(body.get("password") or "")))
            if name == "qr_start":
                return self._submit(self._qr_start())
            if name == "qr_poll":
                return self.call(self._qr_poll(str(body.get("password") or "")), 20)
            if name == "toggle":
                return self._toggle(bool(body.get("enabled")))
            if name == "logout":
                return self._submit(self._logout(), 30)
            return {"ok": False, "error": "unknown op"}
        except Exception as e:
            log.exception("userbot op %s", name)
            return {"ok": False, "error": f"{type(e).__name__}: {str(e)[:160]}"}

    def _toggle(self, on):
        c = self.cfg()
        if on:
            if not c.get("session") or c.get("session_invalid"):
                return {"ok": False, "error": "ابتدا وارد حساب شوید."}
            self._set(enabled=True, state="starting", last_error="", floods=[])
            self._slow = 1.0
            self._flood_until = 0.0
            self.start()
        else:
            self._set(enabled=False, state="off")
        return {"ok": True, **self.status()}

    # ───────────────────────── login: phone + code (stateless across workers) ─────────────────────────
    async def _send_code(self, phone):
        phone = "".join(ch for ch in ascii_digits(phone) if ch in "+0123456789")
        if not phone.startswith("+"):
            phone = "+" + phone
        if len(phone) < 8:
            return {"ok": False, "error": "شماره را با کد کشور وارد کنید (مثل +98912...)."}
        client = await self._client("")
        try:
            await self._connect(client)
            sent = await client.send_code_request(phone)
            tmp = client.session.save()
        finally:
            await self._bye(client)
        self.db.userbot.update_one({"_id": "login"}, {"$set": {"phone": phone, "hash": sent.phone_code_hash,
                                                                "tmp": self._enc(tmp), "stage": "code", "at": time.time()}}, upsert=True)
        return {"ok": True, "stage": "code"}

    async def _sign_in(self, code, password):
        from telethon import errors
        L = self.db.userbot.find_one({"_id": "login"})
        if not L or time.time() - float(L.get("at", 0)) > 900:
            return {"ok": False, "error": "مهلت کد تمام شد؛ دوباره «ارسال کد» بزنید."}
        client = await self._client(self._dec(L["tmp"]))
        try:
            await self._connect(client)
            try:
                if L.get("stage") == "password":
                    if not password:
                        return {"ok": False, "need_password": True}
                    await client.sign_in(password=password)
                else:
                    if not code:
                        return {"ok": False, "error": "کد را وارد کنید."}
                    await client.sign_in(phone=L["phone"], code=code, phone_code_hash=L["hash"])
            except errors.SessionPasswordNeededError:
                self.db.userbot.update_one({"_id": "login"}, {"$set": {"stage": "password", "tmp": self._enc(client.session.save())}})
                if not password:
                    return {"ok": False, "need_password": True}
                await client.sign_in(password=password)
            except errors.PhoneCodeInvalidError:
                return {"ok": False, "error": "کد اشتباه است."}
            except errors.PhoneCodeExpiredError:
                return {"ok": False, "error": "کد منقضی شد؛ دوباره «ارسال کد» بزنید."}
            except errors.PasswordHashInvalidError:
                return {"ok": False, "need_password": True, "error": "رمز تأیید دو مرحله‌ای اشتباه است."}
            return await self._finish_login(client, L.get("phone"))
        finally:
            await self._bye(client)

    async def _finish_login(self, client, phone):
        me = await client.get_me()
        session = client.session.save()
        masked = (phone[:4] + "•••" + phone[-3:]) if phone else (("+" + str(getattr(me, "phone", "") or "")) if getattr(me, "phone", None) else "")
        name = " ".join(x for x in [getattr(me, "first_name", None), getattr(me, "last_name", None)] if x) or str(me.id)
        self._set(session=self._enc(session), session_invalid=False, enabled=False, state="logged_in", last_error="",
                  user={"id": me.id, "name": name[:60], "username": getattr(me, "username", None)},
                  phone_masked=masked, floods=[], polled=0)
        self.db.userbot.delete_one({"_id": "login"})
        return {"ok": True, "logged_in": True, "user": name}

    # ───────────────────────── login: QR ─────────────────────────
    def _svg(self, url):
        try:
            import segno
            return segno.make(url, error="m").svg_inline(scale=5, border=2, dark="#000", light="#fff")
        except Exception:
            return ""

    async def _qr_drop(self):
        s, self._qr = self._qr, None
        if s:
            try:
                s["task"].cancel()
            except Exception:
                pass
            try:
                await self._bye(s["client"])
            except Exception:
                pass

    async def _qr_runner(self, s):
        from telethon import errors
        deadline = time.time() + 240
        try:
            while time.time() < deadline:
                try:
                    await s["qr"].wait(timeout=25)
                    s["done"] = True
                    return
                except asyncio.TimeoutError:
                    await s["qr"].recreate()
                    s["url"] = s["qr"].url
        except errors.SessionPasswordNeededError:
            s["need_password"] = True
        except asyncio.CancelledError:
            raise
        except Exception as e:
            s["error"] = f"{type(e).__name__}: {str(e)[:120]}"

    async def _qr_start(self):
        await self._qr_drop()
        client = await self._client("", updates=True)      # QR login needs the update stream
        await self._connect(client)
        qr = await client.qr_login()
        s = {"client": client, "qr": qr, "url": qr.url, "at": time.time()}
        s["task"] = asyncio.ensure_future(self._qr_runner(s))
        self._qr = s
        return {"ok": True, "url": s["url"], "svg": self._svg(s["url"])}

    async def _qr_poll(self, password):
        s = self._qr
        if not s:
            return {"ok": False, "error": "no_qr", "restart": True}
        if s.get("done"):
            res = await self._finish_login(s["client"], None)
            await self._qr_drop()
            return res
        if s.get("need_password"):
            if not password:
                return {"ok": False, "need_password": True}
            from telethon import errors
            try:
                await s["client"].sign_in(password=password)
            except errors.PasswordHashInvalidError:
                return {"ok": False, "need_password": True, "error": "رمز تأیید دو مرحله‌ای اشتباه است."}
            res = await self._finish_login(s["client"], None)
            await self._qr_drop()
            return res
        if s.get("error") or time.time() - s["at"] > 250:
            err = s.get("error") or "مهلت QR تمام شد"
            await self._qr_drop()
            return {"ok": False, "error": err, "restart": True}
        return {"ok": True, "pending": True, "url": s["url"], "svg": self._svg(s["url"])}

    async def _logout(self):
        c = self.cfg()
        self._set(enabled=False, state="off")
        await self._drop_client()
        if c.get("session"):
            try:
                client = await self._client(self._dec(c["session"]))
                await self._connect(client)
                try:
                    await client.log_out()
                finally:
                    await self._bye(client)
            except Exception:
                log.exception("userbot logout")
        self.db.userbot.update_one({"_id": "cfg"}, {"$unset": {"session": "", "user": "", "phone_masked": ""},
                                                     "$set": {"enabled": False, "state": "logged_out", "session_invalid": False}}, upsert=True)
        self._cfg_cache = (0.0, {})
        return {"ok": True, **self.status()}

    # ───────────────────────── leader election ─────────────────────────
    def _lead(self):
        now = time.time()
        if self._lead_until - now > 15:                       # lease still long enough: no DB write
            return True
        r = self.db.userbot.update_one({"_id": "leader", "owner": self.me}, {"$set": {"until": now + 40}})
        if r.matched_count:
            self._lead_until = now + 40
            return True
        try:
            self.db.userbot.update_one({"_id": "leader", "until": {"$lt": now}}, {"$set": {"owner": self.me, "until": now + 40}}, upsert=True)
            self._lead_until = now + 40
            return True
        except DuplicateKeyError:
            self._lead_until = 0.0
            return False

    # ───────────────────────── polling ─────────────────────────
    async def _drop_client(self):
        c, self.client = self.client, None
        self._ents.clear()
        if c:
            try:
                await self._bye(c)
            except Exception:
                pass

    async def _open_client(self, c):
        try:
            session = self._dec(c["session"])
        except Exception:
            self._set(session_invalid=True, enabled=False, state="invalid", last_error="decrypt failed (SECRET_KEY changed?)")
            return False
        client = await self._client(session)
        await self._connect(client)
        if not await client.is_user_authorized():
            await self._bye(client)
            self._set(session_invalid=True, enabled=False, state="invalid", last_error="session is no longer authorized")
            self.notify("⚠️ حساب کمکی ریکشن از تلگرام خارج شده است؛ از پنل دوباره وارد کنید.")
            return False
        self.client = client
        self._set(state="running", last_error="")
        return True

    async def _main(self):
        while True:
            try:
                c = await self._bg(self._cfg_cached)
                want = bool(c.get("enabled") and c.get("session") and not c.get("session_invalid"))
                if not want or not await self._bg(self._lead):
                    await self._drop_client()
                    await asyncio.sleep(2)
                    continue
                if time.time() < self._flood_until:
                    await asyncio.sleep(5)
                    continue
                if not self.client and not await self._open_client(c):
                    await asyncio.sleep(10)
                    continue
                if self._slow > 1 and time.time() - self._slow_since > 600:     # slow down decays after 10 quiet minutes
                    self._slow, self._slow_since = max(1.0, self._slow / 2), time.time()
                await self._poll_once()
                await self._heartbeat()
                await asyncio.sleep(random.uniform(1.0, 2.0))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.exception("userbot main")
                try:
                    self._set(last_error=(f"{type(e).__name__}: {str(e)[:120]}" or "error"), state="error")
                except Exception:
                    pass
                await asyncio.sleep(15)

    async def _heartbeat(self):
        now = time.time()
        if now - self._hb >= 10 and self.client:
            self._hb = now
            self._set(last_ok=now, polled=self._polled, state="running")

    def _allow(self):
        now = time.time()
        while self._req and now - self._req[0] > 60:
            self._req.popleft()
        return len(self._req) < MAX_PER_MIN

    async def _entity(self, chat_id, uname):
        ent = self._ents.get(chat_id)
        if ent:
            return ent
        from telethon.tl.types import PeerChannel
        from telethon.utils import resolve_id
        cid, _ = resolve_id(int(chat_id))
        client = self.client
        try:
            ent = await client.get_input_entity(PeerChannel(cid))
        except ValueError:
            ent = None
        if ent is None and uname:
            ent = await client.get_input_entity(str(uname).lstrip("@"))        # resolveUsername, once per channel
        if ent is None:
            raise LookupError("channel is not reachable for the helper account (private channel it has not joined)")
        self._ents[chat_id] = ent
        return ent

    @staticmethod
    def _reactions(m):
        out = []
        r = getattr(m, "reactions", None)
        for rc in (getattr(r, "results", None) or []):
            typ = rc.reaction
            name = type(typ).__name__
            if name == "ReactionEmoji":
                t = {"type": "emoji", "emoji": typ.emoticon}
            elif name == "ReactionCustomEmoji":
                t = {"type": "custom_emoji"}
            elif name == "ReactionPaid":
                t = {"type": "paid"}
            else:
                t = {"type": "emoji", "emoji": "?"}
            out.append({"type": t, "total_count": int(rc.count)})
        return out

    async def _poll_once(self):
        posts = await self._bg(self.live_posts)
        now = time.time()
        # a post that just became hot (someone pressed the reward button) is read right away, not at its slow schedule
        due = [(0 if p.get("hot") else 1, k, p) for k, p in posts.items()
               if (self._next.get(k, 0) <= now or (p.get("hot") and self._next.get(k, 0) - now > HOT_SEC * 1.5))
               and self._bad.get(p["chat_id"], 0) <= now]
        due.sort(key=lambda x: x[0])
        chats = {}
        for _, k, p in due:
            chats.setdefault(p["chat_id"], []).append((k, p))
        for chat_id, items in chats.items():
            if not self._allow() or time.time() < self._flood_until or not self.client:
                break
            await self._poll_chat(chat_id, items[:50])
            await asyncio.sleep(random.uniform(*MIN_GAP))
        for k in [k for k in self._next if k not in posts]:                    # forget finished posts
            self._next.pop(k, None)
            self._last.pop(k, None)

    async def _poll_chat(self, chat_id, items):
        from telethon import errors
        uname = next((p.get("uname") for _, p in items if p.get("uname")), None)
        now = time.time()
        try:
            ent = await self._entity(chat_id, uname)
            self._req.append(time.time())
            msgs = await self.client.get_messages(ent, ids=[p["mid"] for _, p in items])
        except errors.FloodWaitError as e:
            await self._on_flood(int(getattr(e, "seconds", 60) or 60))
            return
        except tuple(getattr(errors, n) for n in ("AuthKeyUnregisteredError", "UserDeactivatedError", "UserDeactivatedBanError",
                                                  "SessionRevokedError", "SessionExpiredError", "AuthKeyDuplicatedError")
                     if hasattr(errors, n)) as e:
            await self._fatal(e)
            return
        except Exception as e:
            self._bad[chat_id] = now + 600
            for k, p in items:
                self.db.reaction_watch.update_one({"_id": k}, {"$set": {"ub_bad_until": now + 600, "ub_err": f"{type(e).__name__}: {str(e)[:120]}"}})
            log.warning("userbot cannot read %s: %s", chat_id, e)
            return
        self._ents.setdefault(chat_id, ent)
        for (k, p), m in zip(items, msgs or []):
            hot = bool(p.get("hot"))
            self._next[k] = time.time() + (HOT_SEC if hot else WARM_SEC) * self._slow * random.uniform(0.85, 1.25)
            if m is None:                                                       # deleted post
                self._next[k] = time.time() + 300
                continue
            reactions = self._reactions(m)
            total = sum(x["total_count"] for x in reactions)
            last = self._last.get(k)
            if last is None or last[0] != total or time.time() - last[1] >= FEED_REFRESH_SEC:
                self._last[k] = (total, time.time())
                mr = {"chat": {"id": chat_id, "username": p.get("uname") or None}, "message_id": p["mid"],
                      "date": time.time(), "reactions": reactions}
                try:
                    await self._bg(self.feed, mr)
                    self.db.reaction_watch.update_one({"_id": k}, {"$set": {"ub_at": time.time()}, "$unset": {"ub_bad_until": "", "ub_err": ""}})
                except Exception:
                    log.exception("userbot feed")
        self._polled += len(items)

    async def _on_flood(self, seconds):
        now = time.time()
        self._flood_until = now + seconds + 10
        self._slow = min(4.0, self._slow * 2)
        self._slow_since = now
        self._floods.append(now)
        while self._floods and now - self._floods[0] > 3600:
            self._floods.popleft()
        self._set(floods=list(self._floods), last_error=f"FloodWait {seconds}s", state="flood-wait")
        log.warning("userbot FloodWait %ss (floods in last hour: %s)", seconds, len(self._floods))
        if len(self._floods) >= FLOOD_LIMIT:
            self._set(enabled=False, state="auto-off", last_error="too many FloodWait errors — switched off to protect the account")
            await self._drop_client()
            self.notify("⚠️ حساب کمکی ریکشن به‌خاطر محدودیت‌های مکرر تلگرام (FloodWait) خودکار خاموش شد تا حساب محدود نشود.")

    async def _fatal(self, e):
        self._set(enabled=False, session_invalid=True, state="invalid", last_error=type(e).__name__)
        await self._drop_client()
        self.notify(f"⚠️ حساب کمکی ریکشن از کار افتاد ({type(e).__name__}). از پنل دوباره وارد کنید.")
