#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta
from io import StringIO
import json
import logging
import os
import threading
from typing import Any, Dict, Optional
from urllib.parse import urlparse
from urllib.request import urlopen


logger = logging.getLogger("akshare_proxy")


class AkshareAdapter:
    _proxy_patch_lock = threading.Lock()
    _proxy_patch_installed = False

    def __init__(self) -> None:
        self._proxy_api = os.getenv("AKSHARE_PROXY_API", "https://share.proxy.qg.net/get?key=KY5JZ4X2")
        self._proxy_auth_key = os.getenv("AKSHARE_PROXY_AUTH_KEY", "KY5JZ4X2")
        self._proxy_auth_pwd = os.getenv("AKSHARE_PROXY_AUTH_PWD", "5C2D184F943D")
        self._proxy_strict = os.getenv("AKSHARE_PROXY_STRICT", "1").lower() not in {"0", "false", "no"}
        self._proxy_log_enabled = os.getenv("AKSHARE_PROXY_LOG", "1").lower() not in {"0", "false", "no"}

        self._install_dynamic_proxy_for_requests()

        self._ak = None
        self._import_error = None
        try:
            import akshare as ak  # type: ignore

            self._ak = ak
        except Exception as exc:
            self._import_error = str(exc)

    def _build_proxy_dict(self) -> tuple[Dict[str, str], Dict[str, str]]:
        if not self._proxy_api or not self._proxy_auth_key or not self._proxy_auth_pwd:
            raise RuntimeError("proxy settings are incomplete")

        with urlopen(self._proxy_api, timeout=8) as resp:  # nosec B310
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
        proxy_meta = {
            "request_id": str(payload.get("request_id") or ""),
            "server": server,
            "proxy_ip": str(data[0].get("proxy_ip") or ""),
            "deadline": str(data[0].get("deadline") or ""),
            "area": str(data[0].get("area") or ""),
            "isp": str(data[0].get("isp") or ""),
        }
        return {"http": proxy_url, "https": proxy_url}, proxy_meta

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
                proxy_api_url = os.getenv("AKSHARE_PROXY_API", self._proxy_api)
                auth_key = os.getenv("AKSHARE_PROXY_AUTH_KEY", self._proxy_auth_key)
                auth_pwd = os.getenv("AKSHARE_PROXY_AUTH_PWD", self._proxy_auth_pwd)
                strict_mode = os.getenv("AKSHARE_PROXY_STRICT", "1").lower() not in {"0", "false", "no"}
                log_enabled = os.getenv("AKSHARE_PROXY_LOG", "1").lower() not in {"0", "false", "no"}
                target_host = ""
                if isinstance(url, str):
                    target_host = urlparse(url).netloc or url

                # The proxy-provider API itself must be called directly.
                bypass_proxy = isinstance(url, str) and proxy_api_url and url.startswith(proxy_api_url)

                if not bypass_proxy and auth_key and auth_pwd and proxy_api_url:
                    try:
                        self._proxy_api = proxy_api_url
                        self._proxy_auth_key = auth_key
                        self._proxy_auth_pwd = auth_pwd
                        self._proxy_strict = strict_mode
                        self._proxy_log_enabled = log_enabled
                        proxy_dict, proxy_meta = self._build_proxy_dict()
                        kwargs["proxies"] = proxy_dict
                        if log_enabled:
                            logger.info(
                                "proxy_pool_allocated method=%s target=%s request_id=%s server=%s proxy_ip=%s deadline=%s area=%s isp=%s",
                                method,
                                target_host,
                                proxy_meta.get("request_id"),
                                proxy_meta.get("server"),
                                proxy_meta.get("proxy_ip"),
                                proxy_meta.get("deadline"),
                                proxy_meta.get("area"),
                                proxy_meta.get("isp"),
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
                        if strict_mode:
                            raise RuntimeError(f"dynamic proxy allocation failed: {exc}") from exc

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
                try:
                    result = func(**kwargs)
                    return fn_name, result, ""
                except Exception as exc:
                    errors.append(f"{fn_name}({kwargs}): {exc}")

        return None, None, "; ".join(errors) if errors else "no callable api found"

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

    def stock_kline(
        self,
        symbol: str,
        period: str = "daily",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        top_n: int = 60,
    ) -> Dict[str, Any]:
        fn_name = "stock_zh_a_hist"
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

        try:
            df = self._ak.stock_zh_a_hist(
                symbol=symbol,
                period=period,
                start_date=start,
                end_date=end,
                adjust="",
            )
            if hasattr(df, "iloc"):
                df = df.iloc[::-1]
            return self._wrap(
                fn_name,
                symbol=symbol,
                period=period,
                start_date=start,
                end_date=end,
                items=self._to_records(df, top_n=top_n),
            )
        except Exception as exc:
            return self._error(fn_name, str(exc))

    def stock_chart(self, symbol: str, period: str = "daily", days: int = 30) -> Dict[str, Any]:
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

            df = self._ak.stock_zh_a_hist(
                symbol=symbol,
                period=period,
                start_date=start,
                end_date=end,
                adjust="",
            )
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

    def stock_intraday(self, symbol: str, period: str = "1", top_n: int = 30) -> Dict[str, Any]:
        fn_name = "stock_intraday_em"
        err = self._ready_or_error(fn_name)
        if err:
            return err

        candidates = [
            ("stock_intraday_em", [{"symbol": symbol, "period": period}]),
            ("stock_zh_a_minute", [{"symbol": symbol, "period": period}]),
        ]
        used_fn, df, error = self._call_api_candidates(candidates)
        if used_fn is None:
            return self._error(fn_name, error)

        return self._wrap(used_fn, symbol=symbol, period=period, items=self._to_records(df, top_n=top_n))

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

    def stock_overview(self, symbol: str) -> Dict[str, Any]:
        fn_name = "stock_individual_info_em"
        err = self._ready_or_error(fn_name)
        if err:
            return err

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

    def money_flow(self, symbol: str, top_n: int = 10) -> Dict[str, Any]:
        fn_name = "stock_individual_fund_flow"
        err = self._ready_or_error(fn_name)
        if err:
            return err

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

        candidates = [
            ("stock_hsgt_fund_flow_summary_em", [{}]),
            ("stock_hsgt_hist_em", [{}]),
        ]
        used_fn, df, error = self._call_api_candidates(candidates)
        if used_fn is None:
            return self._error(fn_name, error)

        return self._wrap(used_fn, items=self._to_records(df, top_n=top_n))

    def sector_money_flow(self, top_n: int = 10) -> Dict[str, Any]:
        fn_name = "stock_fund_flow_industry"
        err = self._ready_or_error(fn_name)
        if err:
            return err

        candidates = [
            ("stock_fund_flow_industry", [{"symbol": "即时"}]),
            ("stock_sector_fund_flow_rank", [{"symbol": "即时", "sector_type": "行业资金流"}]),
        ]
        used_fn, df, error = self._call_api_candidates(candidates)
        if used_fn is None:
            return self._error(fn_name, error)

        return self._wrap(used_fn, items=self._to_records(df, top_n=top_n))

    def fundamental(self, symbol: str, top_n: int = 20) -> Dict[str, Any]:
        fn_name = "stock_financial_abstract_ths"
        err = self._ready_or_error(fn_name)
        if err:
            return err

        symbol = self._clean_symbol(symbol)

        results: Dict[str, Any] = {}

        candidates_profit = [
            ("stock_financial_abstract_ths", [{"symbol": symbol, "indicator": "按年度"}]),
            ("stock_profit_sheet_by_annual_em", [{"symbol": symbol}]),
        ]
        used_fn, df, error = self._call_api_candidates(candidates_profit)
        if used_fn:
            results["profit"] = self._to_records(df, top_n=top_n)

        candidates_balance = [
            ("stock_balance_sheet_by_annual_em", [{"symbol": symbol}]),
        ]
        used_fn2, df2, _ = self._call_api_candidates(candidates_balance)
        if used_fn2:
            results["balance"] = self._to_records(df2, top_n=top_n)

        if not results:
            return self._error(fn_name, error)

        return self._wrap(fn_name, symbol=symbol, **results)

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
            df_margin = self._ak.stock_margin_underlying_info_szse()
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
            ("stock_news_em", [{"symbol": "全部", "page": "1"}]),
            ("stock_news_em", [{}]),
            ("futures_news_baidu", [{"symbol": "全部", "page": "1"}]),
        ]
        used_fn, df, error = self._call_api_candidates(candidates)
        if used_fn is None:
            return self._error(fn_name, error)

        return self._wrap(used_fn, items=self._to_records(df, top_n=top_n))

    def research_report(self, symbol: str, top_n: int = 10) -> Dict[str, Any]:
        fn_name = "stock_research_report_em"
        err = self._ready_or_error(fn_name)
        if err:
            return err

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

    def hk_us_market(self, market: str = "hk", top_n: int = 10, symbol: Optional[str] = None) -> Dict[str, Any]:
        fn_name = "stock_hk_spot_em"
        err = self._ready_or_error(fn_name)
        if err:
            return err

        if market == "us":
            candidates = [
                ("stock_us_spot_em", [{}]),
            ]
        else:
            candidates = [
                ("stock_hk_spot_em", [{}]),
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

        return self._wrap(used_fn, market=market, items=records)

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