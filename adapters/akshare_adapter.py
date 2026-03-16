#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta
import socket
from io import StringIO
import json
import logging
import os
import ssl
import threading
import time
from typing import Any, Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import urlopen


logger = logging.getLogger("akshare_proxy")


class AkshareAdapter:
    _proxy_patch_lock = threading.Lock()
    _proxy_patch_installed = False
    _request_ctx = threading.local()
    _symbol_cache_thread_lock = threading.Lock()
    _symbol_cache_thread_started = False

    def __init__(self) -> None:
        self._proxy_url = os.getenv("AKSHARE_PROXY_URL", "http://umwhniat-rotate:eudczfs5mkzt@p.webshare.io:80/").strip()
        self._proxy_api = os.getenv("AKSHARE_PROXY_API", "https://share.proxy.qg.net/get?key=KY5JZ4X2")
        self._proxy_auth_key = os.getenv("AKSHARE_PROXY_AUTH_KEY", "KY5JZ4X2")
        self._proxy_auth_pwd = os.getenv("AKSHARE_PROXY_AUTH_PWD", "5C2D184F943D")
        self._proxy_strict = os.getenv("AKSHARE_PROXY_STRICT", "1").lower() not in {"0", "false", "no"}
        self._proxy_log_enabled = os.getenv("AKSHARE_PROXY_LOG", "1").lower() not in {"0", "false", "no"}
        self._proxy_log_level = os.getenv("AKSHARE_PROXY_LOG_LEVEL", "INFO").strip().upper() or "INFO"
        self._proxy_fetch_timeout = float(os.getenv("AKSHARE_PROXY_FETCH_TIMEOUT", "8"))
        self._proxy_fetch_retries = max(1, int(os.getenv("AKSHARE_PROXY_FETCH_RETRIES", "3")))
        self._proxy_fetch_backoff_ms = max(0, int(os.getenv("AKSHARE_PROXY_FETCH_BACKOFF_MS", "300")))
        self._api_call_retries = max(1, int(os.getenv("AKSHARE_API_RETRIES", "3")))
        self._api_call_backoff_ms = max(0, int(os.getenv("AKSHARE_API_BACKOFF_MS", "400")))
        self._symbol_cache_ttl_sec = max(60, int(os.getenv("AKSHARE_SYMBOL_CACHE_TTL_SEC", "1800")))
        self._symbol_cache_refresh_sec = max(60, int(os.getenv("AKSHARE_SYMBOL_CACHE_REFRESH_SEC", "900")))
        self._symbol_cache_preload = os.getenv("AKSHARE_SYMBOL_CACHE_PRELOAD", "1").lower() not in {"0", "false", "no"}
        self._symbol_cache_backend = os.getenv("AKSHARE_SYMBOL_CACHE_BACKEND", "redis").strip().lower() or "redis"
        self._redis_url = os.getenv("AKSHARE_REDIS_URL", "redis://host.docker.internal:6379/0").strip()
        self._redis_prefix = os.getenv("AKSHARE_REDIS_PREFIX", "akshare:symbol_cache:").strip() or "akshare:symbol_cache:"
        self._redis_socket_timeout_sec = max(0.2, float(os.getenv("AKSHARE_REDIS_SOCKET_TIMEOUT_SEC", "1.5")))
        self._redis_client = None
        self._redis_ready = False
        self._finnhub_api_key = os.getenv("AKSHARE_FINNHUB_API_KEY", os.getenv("FINNHUB_API_KEY", "")).strip()
        self._finnhub_enabled = bool(self._finnhub_api_key)
        self._finnhub_base_url = os.getenv("AKSHARE_FINNHUB_BASE_URL", "https://finnhub.io").strip().rstrip("/")
        self._finnhub_min_interval_sec = max(0.2, float(os.getenv("AKSHARE_FINNHUB_MIN_INTERVAL_SEC", "1.2")))
        self._finnhub_daily_budget = max(1, int(os.getenv("AKSHARE_FINNHUB_DAILY_BUDGET", "1000")))
        self._finnhub_search_cache_ttl_sec = max(600, int(os.getenv("AKSHARE_FINNHUB_SEARCH_CACHE_TTL_SEC", "86400")))
        self._finnhub_quote_cache_ttl_sec = max(60, int(os.getenv("AKSHARE_FINNHUB_QUOTE_CACHE_TTL_SEC", "7200")))
        self._finnhub_cache: dict[str, tuple[float, dict]] = {}
        self._finnhub_cache_lock = threading.Lock()
        self._finnhub_rate_lock = threading.Lock()
        self._finnhub_last_call_ts = 0.0
        self._finnhub_daily_date = datetime.now().strftime("%Y-%m-%d")
        self._finnhub_daily_count = 0

        self._configure_proxy_logger()
        self._init_symbol_cache_backend()
        self._install_dynamic_proxy_for_requests()

        self._ak = None
        self._import_error = None
        try:
            import akshare as ak  # type: ignore

            self._ak = ak
        except Exception as exc:
            self._import_error = str(exc)

        self._start_symbol_cache_refresher()

    def _configure_proxy_logger(self) -> None:
        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
            logger.addHandler(handler)

        level = getattr(logging, self._proxy_log_level, logging.INFO)
        logger.setLevel(level if self._proxy_log_enabled else logging.WARNING)
        logger.propagate = False

    def _init_symbol_cache_backend(self) -> None:
        if self._symbol_cache_backend != "redis":
            raise RuntimeError(
                f"AKSHARE_SYMBOL_CACHE_BACKEND must be 'redis', got: {self._symbol_cache_backend}"
            )

        try:
            import redis  # type: ignore

            self._redis_client = redis.Redis.from_url(
                self._redis_url,
                decode_responses=True,
                socket_timeout=self._redis_socket_timeout_sec,
                socket_connect_timeout=self._redis_socket_timeout_sec,
            )
            self._redis_client.ping()
            self._redis_ready = True
            if self._proxy_log_enabled:
                logger.info("symbol_cache_backend_selected backend=redis redis_url=%s", self._redis_url)
        except Exception as exc:
            self._redis_client = None
            self._redis_ready = False
            raise RuntimeError(f"redis cache init failed: {exc}") from exc

    def _build_proxy_dict(self) -> tuple[Dict[str, str], Dict[str, str]]:
        if not self._proxy_api or not self._proxy_auth_key or not self._proxy_auth_pwd:
            raise RuntimeError("proxy settings are incomplete")

        start = time.monotonic()
        last_exc: Optional[Exception] = None

        for attempt in range(1, self._proxy_fetch_retries + 1):
            try:
                with urlopen(self._proxy_api, timeout=self._proxy_fetch_timeout) as resp:  # nosec B310
                    raw = resp.read().decode("utf-8", errors="replace")

                payload = json.loads(raw)
                if payload.get("code") != "SUCCESS":
                    raise RuntimeError(f"proxy api failed: {payload}")

                data = payload.get("data") or []
                if not data:
                    raise RuntimeError("proxy api returned empty data")

                server = str(data[0].get("server") or "").strip()
                if not server:
                    raise RuntimeError(f"proxy api returned invalid server: {payload}")

                proxy_url = f"http://{self._proxy_auth_key}:{self._proxy_auth_pwd}@{server}"
                elapsed_ms = int((time.monotonic() - start) * 1000)
                proxy_meta = {
                    "request_id": str(payload.get("request_id") or ""),
                    "server": server,
                    "proxy_ip": str(data[0].get("proxy_ip") or ""),
                    "deadline": str(data[0].get("deadline") or ""),
                    "area": str(data[0].get("area") or ""),
                    "isp": str(data[0].get("isp") or ""),
                    "attempts": attempt,
                    "elapsed_ms": elapsed_ms,
                }
                return {"http": proxy_url, "https": proxy_url}, proxy_meta
            except Exception as exc:
                last_exc = exc
                if self._proxy_log_enabled:
                    logger.warning(
                        "proxy_pool_fetch_retry_failed attempt=%s/%s timeout=%.1fs error=%s",
                        attempt,
                        self._proxy_fetch_retries,
                        self._proxy_fetch_timeout,
                        exc,
                    )
                if attempt < self._proxy_fetch_retries and self._proxy_fetch_backoff_ms > 0:
                    time.sleep(self._proxy_fetch_backoff_ms / 1000)

        raise RuntimeError(
            f"proxy api failed after {self._proxy_fetch_retries} attempts: {last_exc}"
        )

    def _symbol_cache_key(self, fn_name: str, kwargs: dict) -> str:
        return f"{fn_name}|{json.dumps(kwargs or {}, sort_keys=True, ensure_ascii=False)}"

    def _redis_symbol_cache_key(self, cache_key: str) -> str:
        return f"{self._redis_prefix}{cache_key}"

    def _get_symbol_records_from_cache(self, fn_name: str, kwargs: dict) -> Optional[list[dict]]:
        cache_key = self._symbol_cache_key(fn_name, kwargs)

        if not self._redis_ready or self._redis_client is None:
            raise RuntimeError("redis cache is not ready")

        try:
            raw = self._redis_client.get(self._redis_symbol_cache_key(cache_key))
            if not raw:
                return None
            payload = json.loads(raw)
            records = payload.get("records")
            if isinstance(records, list):
                return records
            return None
        except Exception as exc:
            raise RuntimeError(f"symbol cache redis get failed: {exc}") from exc

    def _set_symbol_records_cache(self, fn_name: str, kwargs: dict, records: list[dict]) -> None:
        cache_key = self._symbol_cache_key(fn_name, kwargs)

        if not self._redis_ready or self._redis_client is None:
            raise RuntimeError("redis cache is not ready")

        try:
            payload = json.dumps({"records": records}, ensure_ascii=False)
            self._redis_client.setex(
                self._redis_symbol_cache_key(cache_key),
                self._symbol_cache_ttl_sec,
                payload,
            )
        except Exception as exc:
            raise RuntimeError(f"symbol cache redis set failed: {exc}") from exc

    def _fetch_symbol_records(self, fn_name: str, kwargs: Optional[dict] = None) -> tuple[Optional[list[dict]], Optional[str]]:
        if self._ak is None:
            return None, "akshare unavailable"

        call_kwargs = kwargs or {}
        cached = self._get_symbol_records_from_cache(fn_name, call_kwargs)
        if cached is not None:
            return cached, None

        func = getattr(self._ak, fn_name, None)
        if func is None:
            return None, f"{fn_name} not found"

        result, error = self._call_with_retries(fn_name=fn_name, func=func, kwargs=call_kwargs)
        if error is not None:
            return None, error

        records = self._to_records(result, top_n=10000)
        if not isinstance(records, list):
            return None, f"{fn_name} returned non-list records"

        self._set_symbol_records_cache(fn_name, call_kwargs, records)
        return records, None

    def _build_symbol_preload_plan(self) -> list[tuple[str, list[dict]]]:
        plan = [
            ("stock_info_a_code_name", [{}]),
            ("stock_zh_a_spot_em", [{}]),
            ("stock_hk_spot_em", [{}]),
            ("stock_zh_index_spot_em", [{}]),
            ("stock_zh_index_spot_sina", [{}]),
        ]
        if not self._finnhub_enabled:
            plan.extend([
                ("stock_us_spot_em", [{}]),
                (
                    "stock_us_famous_spot_em",
                    [
                        {"symbol": "科技类"},
                        {"symbol": "金融类"},
                        {"symbol": "医药食品类"},
                        {"symbol": "媒体类"},
                        {"symbol": "汽车能源类"},
                        {"symbol": "制造零售类"},
                    ],
                ),
            ])
        return plan

    def _finnhub_cache_key(self, endpoint: str, params: dict) -> str:
        return f"{endpoint}|{json.dumps(params or {}, sort_keys=True, ensure_ascii=False)}"

    def _get_finnhub_cache(self, endpoint: str, params: dict, ttl_sec: int) -> Optional[dict]:
        now = time.time()
        key = self._finnhub_cache_key(endpoint, params)
        with self._finnhub_cache_lock:
            cached = self._finnhub_cache.get(key)
            if not cached:
                return None
            ts, payload = cached
            if now - ts > ttl_sec:
                self._finnhub_cache.pop(key, None)
                return None
            return payload

    def _set_finnhub_cache(self, endpoint: str, params: dict, payload: dict) -> None:
        key = self._finnhub_cache_key(endpoint, params)
        with self._finnhub_cache_lock:
            self._finnhub_cache[key] = (time.time(), payload)

    def _finnhub_reserve_quota(self) -> tuple[bool, str]:
        with self._finnhub_rate_lock:
            today = datetime.now().strftime("%Y-%m-%d")
            if self._finnhub_daily_date != today:
                self._finnhub_daily_date = today
                self._finnhub_daily_count = 0

            if self._finnhub_daily_count >= self._finnhub_daily_budget:
                return False, f"finnhub daily budget exceeded: {self._finnhub_daily_count}/{self._finnhub_daily_budget}"

            sleep_sec = self._finnhub_min_interval_sec - (time.monotonic() - self._finnhub_last_call_ts)
            if sleep_sec > 0:
                time.sleep(sleep_sec)

            self._finnhub_last_call_ts = time.monotonic()
            self._finnhub_daily_count += 1
            return True, ""

    def _finnhub_get_json(self, endpoint: str, params: dict, ttl_sec: int) -> tuple[Optional[dict], Optional[str]]:
        if not self._finnhub_enabled:
            return None, "finnhub disabled"

        query_params = dict(params or {})
        cached = self._get_finnhub_cache(endpoint, query_params, ttl_sec)
        if cached is not None:
            return cached, None

        ok, quota_error = self._finnhub_reserve_quota()
        if not ok:
            return None, quota_error

        query_params["token"] = self._finnhub_api_key
        query = urlencode(query_params)
        url = f"{self._finnhub_base_url}{endpoint}?{query}"

        try:
            with urlopen(url, timeout=max(3.0, self._proxy_fetch_timeout)) as resp:  # nosec B310
                raw = resp.read().decode("utf-8", errors="replace")
            payload = json.loads(raw)
            if isinstance(payload, dict) and payload.get("error"):
                return None, f"finnhub error: {payload.get('error')}"

            if isinstance(payload, dict):
                self._set_finnhub_cache(endpoint, params or {}, payload)
                return payload, None
            return None, "finnhub returned non-object payload"
        except Exception as exc:
            return None, str(exc)

    def _resolve_symbol_with_finnhub(self, target: str) -> str:
        payload, error = self._finnhub_get_json(
            endpoint="/api/v1/search",
            params={"q": target},
            ttl_sec=self._finnhub_search_cache_ttl_sec,
        )
        if error is not None or not payload:
            return ""

        candidates = payload.get("result")
        if not isinstance(candidates, list):
            return ""

        target_upper = str(target or "").strip().upper()
        if not target_upper:
            return ""

        scored: list[tuple[int, str]] = []
        for row in candidates:
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            desc = str(row.get("description") or "").strip().upper()
            display_symbol = str(row.get("displaySymbol") or "").strip().upper()
            score = 0
            if symbol == target_upper or display_symbol == target_upper:
                score += 100
            if target_upper in symbol:
                score += 40
            if target_upper in desc:
                score += 20
            if "." not in symbol:
                score += 5
            scored.append((score, symbol))

        if not scored:
            return ""
        scored.sort(key=lambda x: x[0], reverse=True)
        return scored[0][1]

    def _get_finnhub_us_quote(self, symbol: str) -> tuple[Optional[dict], Optional[str]]:
        clean_symbol = str(symbol or "").strip().upper()
        if not clean_symbol:
            return None, "empty symbol"

        payload, error = self._finnhub_get_json(
            endpoint="/api/v1/quote",
            params={"symbol": clean_symbol},
            ttl_sec=self._finnhub_quote_cache_ttl_sec,
        )
        if error is not None or not payload:
            return None, error or "empty payload"

        current = payload.get("c")
        if current in (None, 0) and payload.get("t") in (None, 0):
            return None, "finnhub quote empty"

        prev_close = payload.get("pc")
        pct = None
        try:
            if prev_close not in (None, 0):
                pct = ((float(current) - float(prev_close)) / float(prev_close)) * 100
        except Exception:
            pct = None

        quote = {
            "symbol": clean_symbol,
            "price": current,
            "change": payload.get("d"),
            "change_pct": payload.get("dp") if payload.get("dp") not in (None, "") else pct,
            "high": payload.get("h"),
            "low": payload.get("l"),
            "open": payload.get("o"),
            "prev_close": prev_close,
            "timestamp": payload.get("t"),
        }
        return quote, None

    def _refresh_symbol_cache_once(self) -> None:
        if self._ak is None:
            return

        for fn_name, kwargs_list in self._build_symbol_preload_plan():
            for kwargs in kwargs_list:
                _, error = self._fetch_symbol_records(fn_name, kwargs)
                if error and self._proxy_log_enabled:
                    logger.warning("symbol_cache_refresh_failed api=%s kwargs=%s error=%s", fn_name, kwargs, error)

    def _symbol_cache_refresh_loop(self) -> None:
        if self._proxy_log_enabled:
            logger.info(
                "symbol_cache_refresh_started ttl_sec=%s refresh_sec=%s",
                self._symbol_cache_ttl_sec,
                self._symbol_cache_refresh_sec,
            )

        while True:
            try:
                self._refresh_symbol_cache_once()
            except Exception as exc:
                if self._proxy_log_enabled:
                    logger.warning("symbol_cache_refresh_loop_error error=%s", exc)
            time.sleep(self._symbol_cache_refresh_sec)

    def _start_symbol_cache_refresher(self) -> None:
        if not self._symbol_cache_preload or self._ak is None:
            return

        if AkshareAdapter._symbol_cache_thread_started:
            return

        with AkshareAdapter._symbol_cache_thread_lock:
            if AkshareAdapter._symbol_cache_thread_started:
                return

            thread = threading.Thread(target=self._symbol_cache_refresh_loop, name="akshare-symbol-cache", daemon=True)
            thread.start()
            AkshareAdapter._symbol_cache_thread_started = True

    def _install_dynamic_proxy_for_requests(self) -> None:
        if AkshareAdapter._proxy_patch_installed:
            return

        with AkshareAdapter._proxy_patch_lock:
            if AkshareAdapter._proxy_patch_installed:
                return

            try:
                import requests
            except Exception:
                return

            original_request = requests.sessions.Session.request

            def request_with_dynamic_proxy(session, method, url, **kwargs):
                def _safe_host(raw: str) -> str:
                    if not raw:
                        return ""
                    parsed = urlparse(raw)
                    host = parsed.netloc or parsed.path or raw
                    return host.split("@")[-1]

                fixed_proxy_url = os.getenv("AKSHARE_PROXY_URL", self._proxy_url).strip()
                proxy_api_url = os.getenv("AKSHARE_PROXY_API", self._proxy_api)
                auth_key = os.getenv("AKSHARE_PROXY_AUTH_KEY", self._proxy_auth_key)
                auth_pwd = os.getenv("AKSHARE_PROXY_AUTH_PWD", self._proxy_auth_pwd)
                strict_mode = os.getenv("AKSHARE_PROXY_STRICT", "1").lower() not in {"0", "false", "no"}
                log_enabled = os.getenv("AKSHARE_PROXY_LOG", "1").lower() not in {"0", "false", "no"}
                fetch_timeout = os.getenv("AKSHARE_PROXY_FETCH_TIMEOUT")
                fetch_retries = os.getenv("AKSHARE_PROXY_FETCH_RETRIES")
                fetch_backoff_ms = os.getenv("AKSHARE_PROXY_FETCH_BACKOFF_MS")
                target_host = ""
                if isinstance(url, str):
                    target_host = urlparse(url).netloc or url
                selected_mode = "direct"
                selected_proxy = ""
                proxy_api_host = _safe_host(proxy_api_url or "")

                # The proxy-provider API itself must be called directly.
                bypass_proxy = isinstance(url, str) and proxy_api_url and url.startswith(proxy_api_url)
                if bypass_proxy:
                    selected_mode = "proxy_api_direct"
                    selected_proxy = proxy_api_host

                if not bypass_proxy and fixed_proxy_url:
                    kwargs["proxies"] = {"http": fixed_proxy_url, "https": fixed_proxy_url}
                    selected_mode = "fixed"
                    selected_proxy = _safe_host(fixed_proxy_url)
                    if log_enabled:
                        logger.info(
                            "proxy_fixed_allocated method=%s target=%s proxy=%s",
                            method,
                            target_host,
                            fixed_proxy_url,
                        )
                elif not bypass_proxy and auth_key and auth_pwd and proxy_api_url:
                    try:
                        self._proxy_url = fixed_proxy_url
                        self._proxy_api = proxy_api_url
                        self._proxy_auth_key = auth_key
                        self._proxy_auth_pwd = auth_pwd
                        self._proxy_strict = strict_mode
                        self._proxy_log_enabled = log_enabled
                        if fetch_timeout is not None:
                            self._proxy_fetch_timeout = max(1.0, float(fetch_timeout))
                        if fetch_retries is not None:
                            self._proxy_fetch_retries = max(1, int(fetch_retries))
                        if fetch_backoff_ms is not None:
                            self._proxy_fetch_backoff_ms = max(0, int(fetch_backoff_ms))
                        proxy_dict, proxy_meta = self._build_proxy_dict()
                        kwargs["proxies"] = proxy_dict
                        selected_mode = "pool"
                        selected_proxy = str(proxy_meta.get("server") or "")
                        if log_enabled:
                            logger.info(
                                "proxy_pool_allocated method=%s target=%s request_id=%s server=%s proxy_ip=%s deadline=%s area=%s isp=%s attempts=%s elapsed_ms=%s",
                                method,
                                target_host,
                                proxy_meta.get("request_id"),
                                proxy_meta.get("server"),
                                proxy_meta.get("proxy_ip"),
                                proxy_meta.get("deadline"),
                                proxy_meta.get("area"),
                                proxy_meta.get("isp"),
                                proxy_meta.get("attempts"),
                                proxy_meta.get("elapsed_ms"),
                            )
                    except Exception as exc:
                        if log_enabled:
                            logger.warning(
                                "proxy_pool_allocation_failed method=%s target=%s strict=%s error=%s",
                                method,
                                target_host,
                                strict_mode,
                                exc,
                            )
                        selected_mode = "pool_failed"
                        selected_proxy = proxy_api_host
                        if strict_mode:
                            raise RuntimeError(f"dynamic proxy allocation failed: {exc}") from exc

                if log_enabled:
                    logger.info(
                        "proxy_route_selected method=%s target=%s mode=%s selected_proxy=%s proxy_api=%s strict=%s",
                        method,
                        target_host,
                        selected_mode,
                        selected_proxy,
                        proxy_api_host,
                        strict_mode,
                    )

                AkshareAdapter._request_ctx.target_host = target_host
                AkshareAdapter._request_ctx.proxy_mode = selected_mode
                AkshareAdapter._request_ctx.selected_proxy = selected_proxy
                AkshareAdapter._request_ctx.proxy_api = proxy_api_host

                return original_request(session, method, url, **kwargs)

            requests.sessions.Session.request = request_with_dynamic_proxy
            AkshareAdapter._proxy_patch_installed = True

    def _wrap(self, fn_name: str, **payload: Any) -> Dict[str, Any]:
        return {
            "ok": True,
            "source": "akshare",
            "api": fn_name,
            "data": payload,
        }

    def _error(self, fn_name: str, message: str) -> Dict[str, Any]:
        return {
            "ok": False,
            "source": "akshare",
            "api": fn_name,
            "error": message,
        }

    def _ready_or_error(self, fn_name: str) -> Optional[Dict[str, Any]]:
        if self._ak is None:
            return self._error(fn_name, f"akshare import failed: {self._import_error}")
        return None

    def _to_records(self, data: Any, top_n: int = 10) -> Any:
        if data is None:
            return []

        if hasattr(data, "head") and hasattr(data, "to_dict"):
            try:
                if top_n and top_n > 0:
                    return data.head(top_n).to_dict(orient="records")
                return data.to_dict(orient="records")
            except Exception:
                return str(data)

        return data

    def _data_len(self, data: Any) -> int:
        try:
            return int(len(data))
        except Exception:
            return 0

    def _normalize_trade_date(self, value: Optional[str]) -> str:
        if not value or value in {"today", "今日", "今天"}:
            return datetime.now().strftime("%Y%m%d")
        if value in {"yesterday", "昨日", "昨天"}:
            return (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        return str(value).replace("-", "").replace("/", "")

    def _clean_symbol(self, symbol: Optional[str]) -> str:
        if not symbol:
            return ""
        return str(symbol).lower().replace("sz", "").replace("sh", "").replace("bj", "")

    def _market_from_symbol(self, symbol: str) -> str:
        market = "sh"
        if symbol.startswith(("0", "3")):
            market = "sz"
        elif symbol.startswith(("8", "4")):
            market = "bj"
        return market

    def _to_prefixed_a_symbol(self, symbol: str) -> str:
        s = self._clean_symbol(symbol)
        if len(s) == 6 and s.isdigit():
            return f"{self._market_from_symbol(s)}{s}"
        return s

    def _to_em_a_symbol(self, symbol: str) -> str:
        s = self._clean_symbol(symbol)
        if len(s) == 6 and s.isdigit():
            market = self._market_from_symbol(s).upper()
            return f"{s}.{market}"
        return s

    def _to_hk_symbol(self, symbol: str) -> str:
        s = str(symbol or "").strip().upper()
        if s.startswith("HK"):
            s = s[2:]
        if s.isdigit() and len(s) in {4, 5}:
            return s.zfill(5)
        return s

    def _is_explicit_symbol(self, symbol: str) -> bool:
        s = str(symbol or "").strip()
        if not s:
            return False

        s_upper = s.upper()
        s_lower = s.lower()

        # A/HK numeric codes, e.g. 600519 / 00700 / HK00700.
        if s.isdigit() and len(s) in {5, 6}:
            return True
        if s_upper.startswith("HK") and s_upper[2:].isdigit() and len(s_upper[2:]) in {4, 5}:
            return True
        if s_lower.startswith(("sh", "sz", "bj")) and len(s) == 8 and s[2:].isdigit():
            return True

        # US and cross-market code styles, e.g. BRK.A / 105.TTE / TSLA.
        if "." in s:
            left, right = s.split(".", 1)
            if left and right and left.replace("-", "").isalnum() and right.replace("-", "").isalnum():
                return True

        if any(ord(ch) > 127 for ch in s):
            return False

        if any(ch.isspace() for ch in s):
            return False

        if any(ch.isalpha() for ch in s) and all(ch.isalnum() or ch in {".", "-", "_"} for ch in s):
            return True

        return False

    def _match_target_in_records(
        self,
        records: list[dict],
        target: str,
        return_keys: list[str],
        match_keys: list[str],
    ) -> str:
        if not target or not isinstance(records, list):
            return ""

        target_norm = str(target).strip().lower()
        if not target_norm:
            return ""

        def pick_value(row: dict) -> str:
            for key in return_keys:
                value = row.get(key)
                if value not in (None, ""):
                    return str(value).strip()
            return ""

        exact_hits: list[str] = []
        fuzzy_hits: list[str] = []
        for row in records:
            if not isinstance(row, dict):
                continue
            for key in match_keys:
                value = row.get(key)
                if value in (None, ""):
                    continue
                value_norm = str(value).strip().lower()
                if value_norm == target_norm:
                    picked = pick_value(row)
                    if picked:
                        exact_hits.append(picked)
                    break
                if target_norm in value_norm or value_norm in target_norm:
                    picked = pick_value(row)
                    if picked:
                        fuzzy_hits.append(picked)
                    break

        if exact_hits:
            return exact_hits[0]
        if fuzzy_hits:
            return fuzzy_hits[0]
        return ""

    def _resolve_symbol_with_akshare(self, symbol: Optional[str], query: str = "") -> str:
        raw = str(symbol or "").strip()
        if not raw:
            return ""

        if self._is_explicit_symbol(raw):
            return raw

        q = query or ""
        q_lower = q.lower()
        target = raw.strip()
        market_hints = {
            "hk": any(k in q for k in ["港股", "恒生", "恒指"]),
            "us": any(k in q_lower for k in ["美股", "nasdaq", "dow", "spx", "标普", "道琼斯", "纳指", "us"]),
            "index": any(k in q for k in ["指数", "大盘"]),
        }

        if market_hints["us"] and self._finnhub_enabled:
            finnhub_symbol = self._resolve_symbol_with_finnhub(target)
            if finnhub_symbol:
                return finnhub_symbol

        search_plan: list[tuple[str, list[dict], list[str]]] = []
        if market_hints["hk"]:
            search_plan.extend([
                ("stock_hk_spot_em", [{}], ["代码"]),
                ("stock_hk_main_board_spot_em", [{}], ["代码"]),
            ])
        if market_hints["us"]:
            search_plan.extend([
                ("stock_us_spot_em", [{}], ["代码"]),
                ("stock_us_famous_spot_em", [{"symbol": "科技类"}, {"symbol": "金融类"}, {"symbol": "医药食品类"}, {"symbol": "媒体类"}, {"symbol": "汽车能源类"}, {"symbol": "制造零售类"}], ["代码"]),
            ])
        if market_hints["index"] or not (market_hints["hk"] or market_hints["us"]):
            search_plan.extend([
                ("stock_zh_index_spot_em", [{}], ["代码", "symbol"]),
                ("stock_zh_index_spot_sina", [{}], ["代码", "symbol"]),
                ("index_global_spot_em", [{}], ["代码", "symbol", "指数代码"]),
            ])
        if not (market_hints["hk"] or market_hints["us"]):
            search_plan.extend([
                ("stock_info_a_code_name", [{}], ["code", "代码"]),
                ("stock_zh_a_spot_em", [{}], ["代码"]),
                ("stock_sh_a_spot_em", [{}], ["代码"]),
                ("stock_sz_a_spot_em", [{}], ["代码"]),
                ("stock_bj_a_spot_em", [{}], ["代码"]),
            ])

        match_keys = ["名称", "name", "证券简称", "股票简称", "中文名称", "代码", "code", "symbol", "指数名称", "简称"]

        for fn_name, kwargs_list, return_keys in search_plan:
            for kwargs in kwargs_list or [{}]:
                records, error = self._fetch_symbol_records(fn_name=fn_name, kwargs=kwargs)
                if error is not None:
                    continue
                matched = self._match_target_in_records(records, target=target, return_keys=return_keys, match_keys=match_keys)
                if matched:
                    return matched

        return raw

    def _filter_records_by_symbol(self, records: list[dict], symbol: str) -> list[dict]:
        if not symbol:
            return records

        key_pool = ["代码", "股票代码", "证券代码", "symbol", "代码简称"]
        filtered = []
        for row in records:
            if not isinstance(row, dict):
                continue
            for key in key_pool:
                val = row.get(key)
                if val is not None and symbol in str(val):
                    filtered.append(row)
                    break
        return filtered

    def _call_api_candidates(self, candidates: list[tuple[str, list[dict]]]) -> tuple[Optional[str], Any, str]:
        errors = []

        for fn_name, kwargs_list in candidates:
            func = getattr(self._ak, fn_name, None)
            if func is None:
                continue

            args_pool = kwargs_list or [{}]
            for kwargs in args_pool:
                result, error = self._call_with_retries(fn_name=fn_name, func=func, kwargs=kwargs)
                if error is None:
                    return fn_name, result, ""
                errors.append(error)

        return None, None, "; ".join(errors) if errors else "no callable api found"

    def _call_with_retries(self, fn_name: str, func: Any, kwargs: Optional[dict] = None) -> tuple[Any, Optional[str]]:
        args = kwargs or {}
        last_error = ""

        for attempt in range(1, self._api_call_retries + 1):
            try:
                return func(**args), None
            except Exception as exc:
                last_error = str(exc)
                retryable = self._is_retryable_exception(exc)
                if self._proxy_log_enabled:
                    proxy_mode = getattr(AkshareAdapter._request_ctx, "proxy_mode", "unknown")
                    selected_proxy = getattr(AkshareAdapter._request_ctx, "selected_proxy", "")
                    proxy_api = getattr(AkshareAdapter._request_ctx, "proxy_api", "")
                    target_host = getattr(AkshareAdapter._request_ctx, "target_host", "")
                    logger.warning(
                        "akshare_api_call_failed api=%s attempt=%s/%s retryable=%s proxy_mode=%s selected_proxy=%s proxy_api=%s target=%s error=%s",
                        fn_name,
                        attempt,
                        self._api_call_retries,
                        retryable,
                        proxy_mode,
                        selected_proxy,
                        proxy_api,
                        target_host,
                        exc,
                    )

                if not retryable:
                    return None, f"{fn_name}({args}) non-retryable: {last_error}"

                if attempt < self._api_call_retries and self._api_call_backoff_ms > 0:
                    time.sleep(self._api_call_backoff_ms / 1000)

        return None, f"{fn_name}({args}) retries={self._api_call_retries}: {last_error}"

    def _is_retryable_exception(self, exc: Exception) -> bool:
        # 常见网络层异常：连接超时、代理失败、DNS 失败、TLS 失败等。
        retryable_types: tuple[type, ...] = (
            TimeoutError,
            ConnectionError,
            socket.timeout,
            socket.gaierror,
            URLError,
            ssl.SSLError,
        )
        if isinstance(exc, retryable_types):
            return True

        # HTTPError 中，5xx/429 更可能是临时性故障；4xx 参数类错误不重试。
        if isinstance(exc, HTTPError):
            return exc.code >= 500 or exc.code == 429

        # requests 异常（akshare 主要经 requests 发起 HTTP）
        try:
            import requests  # type: ignore

            req_retryable_types = (
                requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.ProxyError,
                requests.exceptions.SSLError,
            )
            if isinstance(exc, req_retryable_types):
                return True

            if isinstance(exc, requests.exceptions.HTTPError):
                status = None
                if getattr(exc, "response", None) is not None:
                    status = getattr(exc.response, "status_code", None)
                return status == 429 or (isinstance(status, int) and status >= 500)
        except Exception:
            pass

        # 第三方库有时会重新包装异常，这里兜底关键词匹配网络类故障。
        msg = str(exc).lower()
        network_markers = [
            "timed out",
            "timeout",
            "temporarily unavailable",
            "temporary failure",
            "connection aborted",
            "connection reset",
            "connection refused",
            "max retries exceeded",
            "failed to establish a new connection",
            "name or service not known",
            "nodename nor servname",
            "proxyerror",
            "unable to connect to proxy",
            "remote end closed connection",
            "ssl",
            "tls",
        ]
        return any(marker in msg for marker in network_markers)

    def index_spot(self, top_n: int = 300) -> Dict[str, Any]:
        primary_fn = "stock_zh_index_spot_sina"
        err = self._ready_or_error(primary_fn)
        if err:
            return err

        try:
            df = self._ak.stock_zh_index_spot_sina()
            return self._wrap(primary_fn, items=self._to_records(df, top_n=top_n))
        except Exception as exc:
            fallback_fn = "stock_zh_index_spot_em"
            try:
                df = self._ak.stock_zh_index_spot_em()
                return self._wrap(fallback_fn, items=self._to_records(df, top_n=top_n))
            except Exception as fallback_exc:
                return self._error(primary_fn, f"sina failed: {exc}; em failed: {fallback_exc}")

    def _index_name_candidates(self, symbol: str, query: str) -> list[str]:
        raw = (symbol or "").strip()
        raw_upper = raw.upper()

        names: list[str] = []
        if raw_upper:
            names.append(raw_upper)
        if raw_upper.startswith("^"):
            names.append(raw_upper[1:])
        if raw_upper.startswith("HK") and raw_upper[2:].isdigit():
            names.append(raw_upper[2:].zfill(5))

        # De-duplicate while preserving order.
        uniq: list[str] = []
        seen: set[str] = set()
        for name in names:
            if name and name not in seen:
                seen.add(name)
                uniq.append(name)
        return uniq

    def _build_kline_candidates(
        self,
        symbol: str,
        period: str,
        start: str,
        end: str,
        query: Optional[str] = None,
    ) -> list[tuple[str, list[dict]]]:
        raw = (symbol or "").strip()
        raw_upper = raw.upper()
        q = query or ""
        q_lower = q.lower()

        is_a_share = raw.isdigit() and len(raw) == 6
        is_hk_numeric = raw_upper.isdigit() and len(raw_upper) in {4, 5}
        is_hk_hint = raw_upper.startswith("HK") or "港股" in q
        is_us_hint = any(k in q_lower for k in ["美股", "nasdaq", "dow", "sp500", "s&p", "纳指", "道琼斯", "标普"])
        index_names = self._index_name_candidates(symbol=raw, query=q)
        is_index_hint = bool(index_names) or "指数" in q

        candidates: list[tuple[str, list[dict]]] = []

        if is_a_share:
            candidates.append((
                "stock_zh_a_hist",
                [{"symbol": raw, "period": period, "start_date": start, "end_date": end, "adjust": ""}],
            ))

        if is_hk_hint:
            hk_symbol = raw_upper[2:] if raw_upper.startswith("HK") else raw_upper
            if hk_symbol.isdigit() and len(hk_symbol) in {4, 5}:
                hk_symbol = hk_symbol.zfill(5)
            if hk_symbol:
                candidates.append((
                    "stock_hk_hist",
                    [{"symbol": hk_symbol, "period": period, "start_date": start, "end_date": end, "adjust": ""}],
                ))
        elif is_hk_numeric:
            candidates.append((
                "stock_hk_hist",
                [{"symbol": raw_upper.zfill(5), "period": period, "start_date": start, "end_date": end, "adjust": ""}],
            ))

        if is_us_hint and raw_upper:
            us_symbol_pool = [raw_upper]
            if raw_upper in {"IXIC", "DJI", "SPX"}:
                us_symbol_pool.extend([".IXIC", ".DJI", ".INX"])
            candidates.append((
                "stock_us_hist",
                [
                    {"symbol": s, "period": period, "start_date": start, "end_date": end, "adjust": ""}
                    for s in us_symbol_pool
                ],
            ))

        if is_index_hint:
            for idx_name in index_names:
                candidates.append(("index_global_hist_em", [{"symbol": idx_name}]))

        # Broad fallbacks for ambiguous symbols.
        if raw and not is_a_share and not is_index_hint:
            candidates.append(("index_global_hist_em", [{"symbol": raw}]))
            candidates.append((
                "stock_hk_hist",
                [{"symbol": raw_upper, "period": period, "start_date": start, "end_date": end, "adjust": ""}],
            ))
            candidates.append((
                "stock_us_hist",
                [{"symbol": raw_upper, "period": period, "start_date": start, "end_date": end, "adjust": ""}],
            ))

        # Keep A-share fallback only for six-digit numeric symbols.
        if is_a_share:
            candidates.append((
                "stock_zh_a_hist",
                [{"symbol": raw, "period": period, "start_date": start, "end_date": end, "adjust": ""}],
            ))

        return candidates

    def _is_valid_kline_result(self, data: Any) -> bool:
        if data is None:
            return False

        if self._data_len(data) <= 0:
            return False

        if not hasattr(data, "columns"):
            return True

        try:
            columns = [str(c).strip().lower() for c in data.columns]
        except Exception:
            return True

        ohlc_markers = {
            "open", "high", "low", "close", "volume",
            "开盘", "最高", "最低", "收盘", "成交量",
        }
        return any(any(marker in col for marker in ohlc_markers) for col in columns)

    def _fetch_kline_df(
        self,
        symbol: str,
        period: str,
        start: str,
        end: str,
        query: Optional[str] = None,
    ) -> tuple[Optional[str], Any, str]:
        candidates = self._build_kline_candidates(symbol=symbol, period=period, start=start, end=end, query=query)
        errors = []

        for fn_name, kwargs_list in candidates:
            func = getattr(self._ak, fn_name, None)
            if func is None:
                continue

            args_pool = kwargs_list or [{}]
            for kwargs in args_pool:
                df, error = self._call_with_retries(fn_name=fn_name, func=func, kwargs=kwargs)
                if error is not None:
                    errors.append(error)
                    continue

                if not self._is_valid_kline_result(df):
                    errors.append(f"{fn_name}({kwargs}) returned empty or non-kline data")
                    continue

                if hasattr(df, "iloc"):
                    try:
                        df = df.iloc[::-1]
                    except Exception:
                        pass
                return fn_name, df, ""

        return None, None, "; ".join(errors) if errors else "no callable api found"

    def stock_kline(
        self,
        symbol: str,
        period: str = "daily",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        top_n: int = 60,
        query: Optional[str] = None,
    ) -> Dict[str, Any]:
        fn_name = "stock_kline"
        err = self._ready_or_error(fn_name)
        if err:
            return err

        if not start_date:
            end_dt = datetime.now()
            if period == "weekly":
                days = top_n * 7
            elif period == "monthly":
                days = top_n * 30
            else:
                days = top_n
            start_dt = end_dt - timedelta(days=days + 50)
            start = start_dt.strftime("%Y%m%d")
        else:
            start = start_date.replace("-", "")

        end = self._normalize_trade_date(end_date)
        symbol = self._resolve_symbol_with_akshare(symbol, query=query or "")

        try:
            used_fn, df, error = self._fetch_kline_df(symbol=symbol, period=period, start=start, end=end, query=query)
            if used_fn is None:
                return self._error(fn_name, error)

            return self._wrap(
                used_fn,
                symbol=symbol,
                period=period,
                start_date=start,
                end_date=end,
                items=self._to_records(df, top_n=top_n),
            )
        except Exception as exc:
            return self._error(fn_name, str(exc))

    def stock_chart(self, symbol: str, period: str = "daily", days: int = 30, query: Optional[str] = None) -> Dict[str, Any]:
        """生成股票K线图"""
        fn_name = "stock_chart"
        err = self._ready_or_error(fn_name)
        if err:
            return err

        try:
            import matplotlib
            matplotlib.use('Agg')
            import matplotlib.pyplot as plt
            import matplotlib.patches as mpatches
            import matplotlib.font_manager as fm
            import base64
            from io import BytesIO

            # 设置中文字体
            font_paths = [
                '/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc',
                '/usr/share/fonts/wqy-zenhei/wqy-zenhei.ttc',
                '/System/Library/Fonts/STHeiti Medium.ttc',
                '/Library/Fonts/Arial Unicode MS.ttf',
            ]
            found_font = None
            for fp in font_paths:
                if os.path.exists(fp):
                    found_font = fp
                    break
            if found_font:
                plt.rcParams['font.family'] = fm.FontProperties(fname=found_font).get_name()
            else:
                plt.rcParams['font.family'] = ['DejaVu Sans', 'sans-serif']
            plt.rcParams['axes.unicode_minus'] = False

            end_dt = datetime.now()
            start_dt = end_dt - timedelta(days=days + 50)
            start = start_dt.strftime("%Y%m%d")
            end = end_dt.strftime("%Y%m%d")
            symbol = self._resolve_symbol_with_akshare(symbol, query=query or "")

            used_fn, df, error = self._fetch_kline_df(symbol=symbol, period=period, start=start, end=end, query=query)
            if used_fn is None:
                return self._error(fn_name, error)

            if df is None or len(df) == 0:
                return self._error(fn_name, "no data returned")

            df = df.tail(days)

            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), gridspec_kw={'height_ratios': [3, 1]})
            fig.patch.set_facecolor('#1a1a2e')
            ax1.set_facecolor('#16213e')
            ax2.set_facecolor('#16213e')

            dates = range(len(df))
            open_col = next((c for c in df.columns if '开盘' in c or c.lower() == 'open'), None)
            close_col = next((c for c in df.columns if '收盘' in c or c.lower() == 'close'), None)
            high_col = next((c for c in df.columns if '最高' in c or c.lower() == 'high'), None)
            low_col = next((c for c in df.columns if '最低' in c or c.lower() == 'low'), None)
            vol_col = next((c for c in df.columns if '成交量' in c or c.lower() == 'volume'), None)

            if not all([open_col, close_col, high_col, low_col]):
                return self._error(fn_name, "cannot identify OHLC columns")

            for i, (_, row) in enumerate(df.iterrows()):
                o, c, h, l = row[open_col], row[close_col], row[high_col], row[low_col]
                color = '#ef5350' if c >= o else '#26a69a'
                ax1.plot([i, i], [l, h], color=color, linewidth=0.8)
                ax1.bar(i, abs(c - o), bottom=min(o, c), color=color, width=0.6, alpha=0.9)

            if vol_col:
                for i, (_, row) in enumerate(df.iterrows()):
                    o, c, vol = row[open_col], row[close_col], row[vol_col]
                    color = '#ef5350' if c >= o else '#26a69a'
                    ax2.bar(i, vol, color=color, width=0.6, alpha=0.8)

            for ax in [ax1, ax2]:
                ax.tick_params(colors='#aaaaaa', labelsize=8)
                ax.spines['bottom'].set_color('#444444')
                ax.spines['top'].set_color('#444444')
                ax.spines['left'].set_color('#444444')
                ax.spines['right'].set_color('#444444')
                ax.yaxis.label.set_color('#aaaaaa')

            date_col = next((c for c in df.columns if '日期' in c or c.lower() == 'date'), None)
            if date_col:
                n = max(1, len(df) // 6)
                ticks = list(range(0, len(df), n))
                labels = [str(df.iloc[i][date_col])[:10] for i in ticks]
                ax1.set_xticks(ticks)
                ax1.set_xticklabels([])
                ax2.set_xticks(ticks)
                ax2.set_xticklabels(labels, rotation=30, ha='right', fontsize=7)

            ax1.set_title(f'{symbol} K线图', color='white', fontsize=13, pad=8)
            ax1.set_ylabel('价格', color='#aaaaaa', fontsize=9)
            ax2.set_ylabel('成交量', color='#aaaaaa', fontsize=9)
            plt.tight_layout(h_pad=0.3)

            buf = BytesIO()
            plt.savefig(buf, format='png', dpi=120, bbox_inches='tight', facecolor=fig.get_facecolor())
            plt.close(fig)
            buf.seek(0)
            img_b64 = base64.b64encode(buf.read()).decode()

            return {
                "ok": True,
                "source": "akshare",
                "api": fn_name,
                "data": {
                    "symbol": symbol,
                    "period": period,
                    "days": days,
                    "image_base64": img_b64,
                    "image_format": "png",
                }
            }
        except Exception as exc:
            return self._error(fn_name, str(exc))

    def stock_intraday(self, symbol: str, period: str = "1", top_n: int = 30, query: str = "") -> Dict[str, Any]:
        fn_name = "stock_intraday_em"
        err = self._ready_or_error(fn_name)
        if err:
            return err

        symbol = self._resolve_symbol_with_akshare(symbol, query=query)
        clean_symbol = self._clean_symbol(symbol)
        prefixed_symbol = self._to_prefixed_a_symbol(clean_symbol)
        hk_symbol = self._to_hk_symbol(symbol)

        # AkShare 参数规则：
        # 1) stock_intraday_em 仅需 symbol，不支持 period
        # 2) stock_zh_a_hist_min_em 支持 A 股 period={1,5,15,30,60}
        # 3) stock_hk_hist_min_em 支持港股分时
        candidates = [
            (
                "stock_zh_a_hist_min_em",
                [{
                    "symbol": clean_symbol,
                    "period": period,
                    "adjust": "" if period == "1" else "qfq",
                }],
            ),
            (
                "stock_zh_a_minute",
                [{"symbol": prefixed_symbol, "period": period, "adjust": ""}],
            ),
            (
                "stock_hk_hist_min_em",
                [{
                    "symbol": hk_symbol,
                    "period": period,
                    "adjust": "" if period == "1" else "qfq",
                }],
            ),
            ("stock_intraday_em", [{"symbol": clean_symbol}]),
        ]
        used_fn, df, error = self._call_api_candidates(candidates)
        if used_fn is None:
            return self._error(fn_name, error)

        return self._wrap(used_fn, symbol=clean_symbol or symbol, period=period, items=self._to_records(df, top_n=top_n))

    def limit_pool(self, date: Optional[str] = None, top_n: int = 20) -> Dict[str, Any]:
        fn_name = "stock_zt_pool_em"
        err = self._ready_or_error(fn_name)
        if err:
            return err

        trade_date = self._normalize_trade_date(date)
        candidates = [
            ("stock_zt_pool_em", [{"date": trade_date}]),
            ("stock_zt_pool_em", [{}]),
        ]
        used_fn, df, error = self._call_api_candidates(candidates)
        if used_fn is None:
            return self._error(fn_name, error)

        return self._wrap(used_fn, date=trade_date, items=self._to_records(df, top_n=top_n))

    def stock_overview(self, symbol: str, query: str = "") -> Dict[str, Any]:
        fn_name = "stock_individual_info_em"
        err = self._ready_or_error(fn_name)
        if err:
            return err

        symbol = self._resolve_symbol_with_akshare(symbol, query=query)
        symbol = self._clean_symbol(symbol)

        results: Dict[str, Any] = {}

        # 基本信息
        try:
            df = self._ak.stock_individual_info_em(symbol=symbol)
            results["info"] = self._to_records(df, top_n=50)
        except Exception as exc:
            results["info_error"] = str(exc)

        # 实时行情
        try:
            df2 = self._ak.stock_bid_ask_em(symbol=symbol)
            results["realtime"] = self._to_records(df2, top_n=30)
        except Exception:
            pass

        return self._wrap(fn_name, symbol=symbol, **results)

    def money_flow(self, symbol: str, top_n: int = 10, query: str = "") -> Dict[str, Any]:
        fn_name = "stock_individual_fund_flow"
        err = self._ready_or_error(fn_name)
        if err:
            return err

        symbol = self._resolve_symbol_with_akshare(symbol, query=query)
        symbol = self._clean_symbol(symbol)
        market = self._market_from_symbol(symbol)

        candidates = [
            ("stock_individual_fund_flow", [{"stock": symbol, "market": market}]),
            ("stock_fund_flow_individual", [{"stock": symbol}]),
        ]
        used_fn, df, error = self._call_api_candidates(candidates)
        if used_fn is None:
            return self._error(fn_name, error)

        return self._wrap(used_fn, symbol=symbol, items=self._to_records(df, top_n=top_n))

    def market_money_flow(self, top_n: int = 10, date: Optional[str] = None) -> Dict[str, Any]:
        fn_name = "stock_fund_flow_big_deal"
        err = self._ready_or_error(fn_name)
        if err:
            return err

        trade_date = self._normalize_trade_date(date)
        candidates = [
            ("stock_hsgt_fund_flow_summary_em", [{}]),
            ("stock_hsgt_fund_min_em", [{"symbol": "北向资金"}, {"symbol": "南向资金"}]),
            (
                "stock_hsgt_hist_em",
                [{"symbol": "北向资金"}, {"symbol": "沪股通"}, {"symbol": "深股通"}, {"symbol": "南向资金"}],
            ),
        ]
        used_fn, df, error = self._call_api_candidates(candidates)
        if used_fn is None:
            return self._error(fn_name, error)

        return self._wrap(used_fn, date=trade_date, items=self._to_records(df, top_n=top_n))

    def sector_money_flow(self, top_n: int = 10) -> Dict[str, Any]:
        fn_name = "stock_fund_flow_industry"
        err = self._ready_or_error(fn_name)
        if err:
            return err

        candidates = [
            ("stock_fund_flow_industry", [{"symbol": "即时"}]),
            ("stock_sector_fund_flow_rank", [{"indicator": "今日", "sector_type": "行业资金流"}]),
            ("stock_sector_fund_flow_rank", [{"indicator": "5日", "sector_type": "概念资金流"}]),
        ]
        used_fn, df, error = self._call_api_candidates(candidates)
        if used_fn is None:
            return self._error(fn_name, error)

        return self._wrap(used_fn, items=self._to_records(df, top_n=top_n))

    def fundamental(self, symbol: str, top_n: int = 20, query: str = "") -> Dict[str, Any]:
        fn_name = "stock_financial_abstract_ths"
        err = self._ready_or_error(fn_name)
        if err:
            return err

        symbol = self._resolve_symbol_with_akshare(symbol, query=query)
        symbol = self._clean_symbol(symbol)
        em_symbol = self._to_em_a_symbol(symbol)

        results: Dict[str, Any] = {}

        candidates_profit = [
            ("stock_financial_abstract_new_ths", [{"symbol": symbol, "indicator": "按年度"}]),
            ("stock_financial_analysis_indicator_em", [{"symbol": em_symbol, "indicator": "按报告期"}]),
        ]
        used_fn, df, error = self._call_api_candidates(candidates_profit)
        if used_fn:
            results["profit"] = self._to_records(df, top_n=top_n)

        candidates_balance = [
            ("stock_balance_sheet_by_report_em", [{"symbol": em_symbol}]),
        ]
        used_fn2, df2, _ = self._call_api_candidates(candidates_balance)
        if used_fn2:
            results["balance"] = self._to_records(df2, top_n=top_n)

        candidates_profile = [
            ("stock_individual_info_em", [{"symbol": symbol}]),
        ]
        used_fn3, df3, _ = self._call_api_candidates(candidates_profile)
        if used_fn3:
            results["profile"] = self._to_records(df3, top_n=top_n)

        if not results:
            return self._error(fn_name, error)

        return self._wrap(fn_name, symbol=symbol, em_symbol=em_symbol, **results)

    def margin_lhb(self, symbol: Optional[str], date: Optional[str], top_n: int = 10) -> Dict[str, Any]:
        fn_name = "stock_lhb_detail_em"
        err = self._ready_or_error(fn_name)
        if err:
            return err

        trade_date = self._normalize_trade_date(date)

        results: Dict[str, Any] = {}

        # 龙虎榜
        try:
            df_lhb = self._ak.stock_lhb_detail_em(start_date=trade_date, end_date=trade_date)
            if symbol:
                records = self._filter_records_by_symbol(self._to_records(df_lhb, top_n=200), self._clean_symbol(symbol))
                results["lhb"] = records[:top_n]
            else:
                results["lhb"] = self._to_records(df_lhb, top_n=top_n)
        except Exception as exc:
            results["lhb_error"] = str(exc)

        # 融资融券
        try:
            df_margin = self._ak.stock_margin_underlying_info_szse(date=trade_date)
            results["margin"] = self._to_records(df_margin, top_n=top_n)
        except Exception:
            try:
                df_margin = self._ak.stock_margin_detail_sse(date=trade_date)
                results["margin"] = self._to_records(df_margin, top_n=top_n)
            except Exception:
                try:
                    df_margin = self._ak.stock_margin_detail_szse(date=trade_date)
                    results["margin"] = self._to_records(df_margin, top_n=top_n)
                except Exception:
                    pass

        return self._wrap(fn_name, date=trade_date, **results)

    def news(self, top_n: int = 10) -> Dict[str, Any]:
        fn_name = "stock_news_em"
        err = self._ready_or_error(fn_name)
        if err:
            return err

        candidates = [
            ("stock_news_main_cx", [{}]),
            ("stock_news_em", [{"symbol": "财经"}]),
            ("stock_news_em", [{"symbol": "A股"}]),
            ("futures_news_baidu", [{"symbol": "全部", "page": "1"}]),
        ]
        used_fn, df, error = self._call_api_candidates(candidates)
        if used_fn is None:
            return self._error(fn_name, error)

        return self._wrap(used_fn, items=self._to_records(df, top_n=top_n))

    def research_report(self, symbol: str, top_n: int = 10, query: str = "") -> Dict[str, Any]:
        fn_name = "stock_research_report_em"
        err = self._ready_or_error(fn_name)
        if err:
            return err

        symbol = self._resolve_symbol_with_akshare(symbol, query=query)
        symbol = self._clean_symbol(symbol)

        candidates = [
            ("stock_research_report_em", [{"symbol": symbol}]),
        ]
        used_fn, df, error = self._call_api_candidates(candidates)
        if used_fn is None:
            return self._error(fn_name, error)

        return self._wrap(used_fn, symbol=symbol, items=self._to_records(df, top_n=top_n))

    def stock_pick(self, top_n: int = 5, sector: Optional[str] = None) -> Dict[str, Any]:
        fn_name = "stock_rank_forecast_cninfo"
        err = self._ready_or_error(fn_name)
        if err:
            return err

        candidates = [
            ("stock_rank_forecast_cninfo", [{"symbol": "预增"}]),
            ("stock_hot_rank_em", [{}]),
            ("stock_rank_forecast_cninfo", [{"symbol": "扭亏"}]),
        ]
        used_fn, df, error = self._call_api_candidates(candidates)
        if used_fn is None:
            return self._error(fn_name, error)

        records = self._to_records(df, top_n=top_n * 5)
        if sector and isinstance(records, list):
            filtered = [r for r in records if sector in str(r)]
            if filtered:
                records = filtered[:top_n]
            else:
                records = records[:top_n]
        elif isinstance(records, list):
            records = records[:top_n]

        return self._wrap(used_fn, sector=sector, items=records)

    def sector_analysis(self, sector_type: str = "industry", top_n: int = 10) -> Dict[str, Any]:
        fn_name = "stock_board_industry_name_em"
        err = self._ready_or_error(fn_name)
        if err:
            return err

        if sector_type == "concept":
            candidates = [
                ("stock_board_concept_name_em", [{}]),
            ]
        else:
            candidates = [
                ("stock_board_industry_name_em", [{}]),
            ]

        used_fn, df, error = self._call_api_candidates(candidates)
        if used_fn is None:
            return self._error(fn_name, error)

        return self._wrap(used_fn, sector_type=sector_type, items=self._to_records(df, top_n=top_n))

    def fund_bond(self, scope: str = "fund", symbol: Optional[str] = None, top_n: int = 10) -> Dict[str, Any]:
        fn_name = "fund_open_fund_rank_em"
        err = self._ready_or_error(fn_name)
        if err:
            return err

        if scope == "bond":
            candidates = [
                ("bond_zh_cov", [{}]),
                ("bond_cov_comparison", [{}]),
            ]
        else:
            candidates = [
                ("fund_open_fund_rank_em", [{"symbol": "全部"}]),
                ("fund_etf_spot_em", [{}]),
            ]

        used_fn, df, error = self._call_api_candidates(candidates)
        if used_fn is None:
            return self._error(fn_name, error)

        return self._wrap(used_fn, scope=scope, items=self._to_records(df, top_n=top_n))

    def hk_us_market(self, market: str = "hk", top_n: int = 10, symbol: Optional[str] = None, query: str = "") -> Dict[str, Any]:
        fn_name = "stock_hk_spot_em"
        err = self._ready_or_error(fn_name)
        if err:
            return err

        resolved_symbol = self._resolve_symbol_with_akshare(symbol, query=query) if symbol else None

        if market == "us" and resolved_symbol and self._finnhub_enabled:
            quote, quote_error = self._get_finnhub_us_quote(resolved_symbol)
            if quote is not None:
                return self._wrap("finnhub_quote", market=market, symbol=resolved_symbol, items=[quote])
            if self._proxy_log_enabled:
                logger.warning("finnhub_quote_failed symbol=%s error=%s", resolved_symbol, quote_error)

        used_fn = "stock_us_spot_em" if market == "us" else "stock_hk_spot_em"
        records, error = self._fetch_symbol_records(used_fn, {})
        if error is not None or records is None:
            # Fallback to direct call if cache path fails.
            candidates = [(used_fn, [{}])]
            fallback_fn, df, fallback_error = self._call_api_candidates(candidates)
            if fallback_fn is None:
                return self._error(fn_name, fallback_error)
            used_fn = fallback_fn
            records = self._to_records(df, top_n=top_n * 5)

        if resolved_symbol and isinstance(records, list):
            filtered = self._filter_records_by_symbol(records, resolved_symbol)
            records = filtered[:top_n] if filtered else records[:top_n]
        elif isinstance(records, list):
            records = records[:top_n]

        return self._wrap(used_fn, market=market, symbol=resolved_symbol, items=records)

    def derivatives(self, scope: str = "futures", symbol: Optional[str] = None, top_n: int = 10) -> Dict[str, Any]:
        fn_name = "futures_main_sina"
        err = self._ready_or_error(fn_name)
        if err:
            return err

        if scope == "options":
            candidates = [
                ("option_finance_board", [{}]),
                ("option_current_em", [{}]),
            ]
        else:
            candidates = [
                ("futures_main_sina", [{}]),
                ("futures_spot_em", [{}]),
            ]

        used_fn, df, error = self._call_api_candidates(candidates)
        if used_fn is None:
            return self._error(fn_name, error)

        records = self._to_records(df, top_n=top_n * 5)
        if symbol and isinstance(records, list):
            filtered = self._filter_records_by_symbol(records, symbol)
            records = filtered[:top_n] if filtered else records[:top_n]
        elif isinstance(records, list):
            records = records[:top_n]

        return self._wrap(used_fn, scope=scope, items=records)