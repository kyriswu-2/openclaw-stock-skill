#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
akshare 数据查询服务 - FastAPI 应用入口

这是云端部署组件，使用 Docker 运行在云服务器。
对外提供一个 POST /query 接口，接收意图 JSON，返回数据结果。
"""

import os
from typing import Any, Dict, Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from adapters import AkshareAdapter

# ---------------------------------------------------------------------------
# 意图常量
# ---------------------------------------------------------------------------
INDEX_REALTIME = "INDEX_REALTIME"
KLINE_ANALYSIS = "KLINE_ANALYSIS"
KLINE_CHART = "KLINE_CHART"
INTRADAY_ANALYSIS = "INTRADAY_ANALYSIS"
VOLUME_ANALYSIS = "VOLUME_ANALYSIS"
LIMIT_STATS = "LIMIT_STATS"
MONEY_FLOW = "MONEY_FLOW"
FUNDAMENTAL = "FUNDAMENTAL"
STOCK_OVERVIEW = "STOCK_OVERVIEW"
MARGIN_LHB = "MARGIN_LHB"
SECTOR_ANALYSIS = "SECTOR_ANALYSIS"
DERIVATIVES = "DERIVATIVES"
FUND_BOND = "FUND_BOND"
HK_US_MARKET = "HK_US_MARKET"
NEWS = "NEWS"
RESEARCH_REPORT = "RESEARCH_REPORT"
STOCK_PICK = "STOCK_PICK"
HELP = "HELP"
PORTFOLIO = "PORTFOLIO"

# ---------------------------------------------------------------------------
# 应用初始化
# ---------------------------------------------------------------------------
app = FastAPI(
    title="AKShare Stock Query Service",
    description="A股数据查询微服务，基于 akshare",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 单例 Adapter
_adapter: Optional[AkshareAdapter] = None


def get_adapter() -> AkshareAdapter:
    global _adapter
    if _adapter is None:
        _adapter = AkshareAdapter()
    return _adapter


# ---------------------------------------------------------------------------
# 请求模型
# ---------------------------------------------------------------------------
class IntentRequest(BaseModel):
    intent: str
    query: str = ""
    symbol: Optional[str] = None
    target: Optional[str] = None
    date: Optional[str] = None
    period: Optional[str] = None
    top_n: Optional[int] = None


# ---------------------------------------------------------------------------
# 分发逻辑
# ---------------------------------------------------------------------------
def dispatch(req: IntentRequest, adapter: AkshareAdapter) -> Dict[str, Any]:
    subject = req.symbol or req.target

    if req.intent == INDEX_REALTIME:
        return adapter.index_spot(top_n=300)

    if req.intent == KLINE_ANALYSIS:
        top_n = req.top_n or 10
        symbol = subject
        if not symbol:
            return {"ok": False, "error": "请输入股票/指数代码或名称", "intent": KLINE_ANALYSIS}
        period = req.period or "daily"
        return adapter.stock_kline(symbol=symbol, period=period, top_n=top_n, query=req.query)

    if req.intent == KLINE_CHART:
        symbol = subject
        if not symbol:
            return {"ok": False, "error": "请输入股票/指数代码或名称", "intent": KLINE_CHART}
        period = req.period or "daily"
        days = req.top_n or 30
        return adapter.stock_chart(symbol=symbol, period=period, days=days, query=req.query)

    if req.intent == INTRADAY_ANALYSIS:
        top_n = req.top_n or 30
        symbol = subject
        if not symbol:
            return {"ok": False, "error": "请输入股票/指数代码或名称", "intent": INTRADAY_ANALYSIS}
        period = req.period if req.period in {"1", "5", "15", "30", "60"} else "1"
        return adapter.stock_intraday(symbol=symbol, period=period, top_n=top_n, query=req.query)

    if req.intent == VOLUME_ANALYSIS:
        # 直接复用 stock_intraday 提供分时数据供分析
        symbol = subject
        if not symbol:
            return {"ok": False, "error": "请输入股票/指数代码或名称", "intent": VOLUME_ANALYSIS}
        return adapter.stock_intraday(symbol=symbol, period="1", top_n=60, query=req.query)

    if req.intent == LIMIT_STATS:
        top_n = req.top_n or 20
        return adapter.limit_pool(date=req.date, top_n=top_n)

    if req.intent == STOCK_OVERVIEW:
        symbol = subject
        if not symbol:
            return {"ok": False, "error": "请输入股票代码或名称", "intent": STOCK_OVERVIEW}
        return adapter.stock_overview(symbol=symbol, query=req.query)

    if req.intent == MONEY_FLOW:
        top_n = req.top_n or 10
        query = req.query or ""
        if any(k in query for k in ["北向", "南向", "东向", "市场资金", "大盘资金"]):
            return adapter.market_money_flow(top_n=top_n, date=req.date)
        if any(k in query for k in ["行业资金", "板块资金", "行业流入", "板块流入"]):
            return adapter.sector_money_flow(top_n=top_n)
        symbol = subject
        if not symbol:
            return {"ok": False, "error": "请输入股票代码或名称", "intent": MONEY_FLOW}
        return adapter.money_flow(symbol=symbol, top_n=top_n, query=req.query)

    if req.intent == FUNDAMENTAL:
        top_n = req.top_n or 20
        symbol = subject
        if not symbol:
            return {"ok": False, "error": "请输入股票代码或名称", "intent": FUNDAMENTAL}
        return adapter.fundamental(symbol=symbol, top_n=top_n, query=req.query)

    if req.intent == MARGIN_LHB:
        top_n = req.top_n or 10
        return adapter.margin_lhb(symbol=req.symbol, date=req.date, top_n=top_n)

    if req.intent == NEWS:
        top_n = min(req.top_n or 10, 10)
        return adapter.news(top_n=top_n)

    if req.intent == RESEARCH_REPORT:
        top_n = min(req.top_n or 10, 10)
        symbol = subject
        if not symbol:
            return {"ok": False, "error": "请输入股票代码或名称", "intent": RESEARCH_REPORT}
        return adapter.research_report(symbol=symbol, top_n=top_n, query=req.query)

    if req.intent == STOCK_PICK:
        query = req.query or ""
        sector = None
        sector_keywords = [
            "半导体", "电子", "汽车", "医药生物", "医药",
            "银行", "保险", "证券", "金融",
            "房地产", "地产", "电力", "传媒",
            "锂电池", "电池", "光伏", "光伏设备",
            "软件", "军工", "食品", "饮料", "白酒", "家电", "纺织"
        ]
        for kw in sector_keywords:
            if kw in query:
                sector = kw
                break
        return adapter.stock_pick(top_n=5, sector=sector)

    if req.intent == SECTOR_ANALYSIS:
        top_n = req.top_n or 10
        query = req.query or ""
        if any(k in query for k in ["概念", "题材"]):
            return adapter.sector_analysis(sector_type="concept", top_n=top_n)
        return adapter.sector_analysis(sector_type="industry", top_n=top_n)

    if req.intent == FUND_BOND:
        top_n = req.top_n or 10
        query = (req.query or "").lower()
        scope = "bond" if any(k in query for k in ["可转债", "转债", "债"]) else "fund"
        return adapter.fund_bond(scope=scope, symbol=subject, top_n=top_n)

    if req.intent == HK_US_MARKET:
        top_n = req.top_n or 10
        query = (req.query or "").lower()
        us_tokens = ["美股", "nasdaq", "dow", "道琼斯", "标普", "sp500", "s&p", "纳指", "us"]
        market = "us" if any(t in query for t in us_tokens) else "hk"
        return adapter.hk_us_market(market=market, top_n=top_n, symbol=subject, query=req.query)

    if req.intent == DERIVATIVES:
        top_n = req.top_n or 10
        query = req.query or ""
        scope = "options" if any(k in query for k in ["期权", "option", "Option", "OPTIONS"]) else "futures"
        return adapter.derivatives(scope=scope, symbol=subject, top_n=top_n)

    if req.intent in (HELP, PORTFOLIO):
        return {
            "ok": False,
            "error": f"{req.intent} 应在本地 OpenClaw 处理，请勿发到云端。",
        }

    return {
        "ok": False,
        "error": f"未知意图: {req.intent}",
        "intent": req.intent,
    }


# ---------------------------------------------------------------------------
# 路由
# ---------------------------------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/query")
def query(req: IntentRequest):
    adapter = get_adapter()
    return dispatch(req, adapter)
