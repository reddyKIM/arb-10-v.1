#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ARB 데이터 집계기 (공개 REST API 전용, Manual Dispatch)
- Binance USDⓈ-M Futures + Bybit v5 Open Interest
- 산출물 10종: data/01_...csv ~ 10_agg_config_costs.json
- 연구 목적 전용 · 투자 조언 아님
"""

import os, time, json
from datetime import datetime, timedelta, timezone
import pandas as pd
import requests

# -------------------- 공통 설정 --------------------
BINANCE_BASE = "https://fapi.binance.com"  # USDⓈ-M Futures REST
BYBIT_BASE   = "https://api.bybit.com"     # v5 REST
DATA_DIR = "data"
SYMBOL   = os.environ.get("SYMBOL", "ARBUSDT")
PAIR     = SYMBOL                           # indexPriceKlines는 pair 파라미터
UA       = "arb-github-collector/1.0"

def iso_z(ms:int) -> str:
    return datetime.fromtimestamp(ms/1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

class Http:
    def __init__(self, base, timeout=20):
        self.base = base
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": UA})
        self.timeout = timeout
    def get(self, path, params=None, retry=5, backoff=0.5):
        url = self.base + path
        for i in range(retry):
            try:
                r = self.s.get(url, params=params, timeout=self.timeout)
                if r.status_code == 429:
                    time.sleep(min(2**i*backoff, 5))
                    continue
                r.raise_for_status()
                return r.json()
            except Exception as e:
                if i == retry-1:
                    raise
                time.sleep(min(2**i*backoff, 5))

bin = Http(BINANCE_BASE)
byb = Http(BYBIT_BASE)

def ensure_dir(p): os.makedirs(p, exist_ok=True)
def now_ms(): return int(time.time()*1000)

def drift_check_ms():
    j = bin.get("/fapi/v1/time")  # 서버 타임
    return abs(int(j["serverTime"]) - now_ms())

def write_csv(df: pd.DataFrame, path: str): df.to_csv(path, index=False)
def write_json(obj, path: str):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

# -------------------- 공통 Klines --------------------
def fetch_klines(symbol: str, interval: str, start_ms: int, end_ms: int, limit=1500):
    out, cur = [], start_ms
    while True:
        arr = bin.get("/fapi/v1/klines", {
            "symbol": symbol, "interval": interval,
            "startTime": cur, "endTime": end_ms, "limit": limit
        })
        if not arr: break
        out.extend(arr)
        last_close = arr[-1][6]
        nxt = last_close + 1
        if nxt >= end_ms: break
        cur = nxt
        time.sleep(0.01)
    return out

def klines_to_df(arr):
    cols = ["open_time","open","high","low","close","volume","close_time","quote_volume","trades",
            "taker_buy_base","taker_buy_quote","ignore"]
    df = pd.DataFrame(arr, columns=cols)
    for c in ["open","high","low","close","volume","quote_volume","taker_buy_base","taker_buy_quote"]:
        df[c] = df[c].astype(float)
    df["trades"] = df["trades"].astype(int)
    df["ts_utc"] = df["close_time"].apply(iso_z)
    return df

# -------------------- 01: 15m · 20D --------------------
def job_01_arb_15m_20d():
    end_ms = now_ms()
    start_ms = end_ms - 20*24*60*60*1000
    df = klines_to_df(fetch_klines(SYMBOL, "15m", start_ms, end_ms))
    slot_ms = 15*60*1000
    df["slot_idx"] = ((df["close_time"] // slot_ms) % (24*60*60*1000 // slot_ms)).astype(int)
    slot_avg = df.groupby("slot_idx")["volume"].mean().rename("avg_vol_slot_20d")
    df = df.merge(slot_avg, on="slot_idx", how="left")
    df["vol_ratio"] = df["volume"] / df["avg_vol_slot_20d"]
    out = df[["ts_utc","open","high","low","close","volume","quote_volume","trades",
              "taker_buy_base","taker_buy_quote","slot_idx","avg_vol_slot_20d","vol_ratio"]]
    write_csv(out, os.path.join(DATA_DIR, "01_arb_15m_20d.csv"))

# -------------------- 02: 1d · 180~190d --------------------
def job_02_arb_1d_180d():
    end_ms = now_ms()
    start_ms = end_ms - 190*24*60*60*1000
    df = klines_to_df(fetch_klines(SYMBOL, "1d", start_ms, end_ms))
    df["date_utc"] = df["ts_utc"].str.slice(0,10)
    df["prev_day_high"]  = df["high"].shift(1)
    df["prev_day_low"]   = df["low"].shift(1)
    df["prev_day_close"] = df["close"].shift(1)
    lookback = 60
    df["swing_high"] = df["high"].rolling(lookback, min_periods=1).max()
    df["swing_low"]  = df["low"].rolling(lookback, min_periods=1).min()
    rng = (df["swing_high"] - df["swing_low"]).replace(0, pd.NA)
    df["fibo_382"] = df["swing_high"] - 0.382 * rng
    df["fibo_618"] = df["swing_high"] - 0.618 * rng
    out = df[["date_utc","open","high","low","close","volume",
              "prev_day_high","prev_day_low","prev_day_close",
              "swing_high","swing_low","fibo_382","fibo_618"]].dropna()
    write_csv(out, os.path.join(DATA_DIR, "02_arb_1d_180d.csv"))

# -------------------- 03: 1m · 오늘 · VWAP --------------------
def job_03_arb_1m_today():
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    df = klines_to_df(fetch_klines(SYMBOL, "1m", int(start.timestamp()*1000), int(now.timestamp()*1000)))
    df["price_x_vol"] = df["close"] * df["volume"]
    df["cum_vol"] = df["volume"].cumsum()
    df["cum_pv"]  = df["price_x_vol"].cumsum()
    df["vwap"] = df["cum_pv"] / df["cum_vol"]
    df["above_vwap"] = df["close"] >= df["vwap"]
    tol = 0.0005
    tmp = (abs(df["close"] - df["vwap"]) / df["vwap"] <= tol).rolling(10, min_periods=1).max().astype(bool)
    df["vwap_retest_success"] = tmp & df["above_vwap"]
    out = df[["ts_utc","open","high","low","close","volume","vwap","above_vwap","vwap_retest_success"]]
    write_csv(out, os.path.join(DATA_DIR, "03_arb_1m_today.csv"))

# -------------------- 04: Funding 8h · 30d --------------------
def job_04_arb_funding_8h_30d():
    end_ms = now_ms()
    start_ms = end_ms - 30*24*60*60*1000
    # History
    out = []
    cur = start_ms
    while True:
        arr = bin.get("/fapi/v1/fundingRate", {
            "symbol": SYMBOL, "startTime": cur, "endTime": end_ms, "limit": 1000
        })
        if not arr: break
        out.extend(arr)
        last = arr[-1]["fundingTime"]
        nxt = last + 1
        if nxt >= end_ms: break
        cur = nxt
        time.sleep(0.01)
    fdf = pd.DataFrame(out)
    if not fdf.empty:
        fdf["funding_time_utc"] = fdf["fundingTime"].apply(iso_z)
        fdf["funding_realized_pct"] = fdf["fundingRate"].astype(float)*100.0
    else:
        fdf = pd.DataFrame(columns=["funding_time_utc","funding_realized_pct"])

    # Next funding & lastFundingRate (predictedFundingRate REST 미제공 → proxy 라벨)
    pi = bin.get("/fapi/v1/premiumIndex", {"symbol": SYMBOL})
    next_ms = int(pi["nextFundingTime"])
    last_fr = float(pi["lastFundingRate"])*100.0
    meta = pd.DataFrame([{
        "funding_time_utc": iso_z(next_ms),
        "funding_realized_pct": None,
        "funding_est_pct": last_fr,
        "next_funding_ts": iso_z(next_ms),
        "next_funding_ms": next_ms,
        "symbol": SYMBOL,
        "est_source": "proxy_lastFundingRate"
    }])
    out = pd.concat([fdf[["funding_time_utc","funding_realized_pct"]], meta], ignore_index=True)
    write_csv(out, os.path.join(DATA_DIR, "04_arb_funding_8h_30d.csv"))

# -------------------- 05: OI 1h · 30d (Binance + Bybit) --------------------
def job_05_arb_oi_1h_30d():
    end_ms = now_ms()
    start_ms = end_ms - 30*24*60*60*1000
    # Binance 1h OI Hist (최근 1개월)
    b = bin.get("/futures/data/openInterestHist", {
        "symbol": SYMBOL, "period": "1h", "startTime": start_ms, "endTime": end_ms, "limit": 500
    })
    bdf = pd.DataFrame(b)
    if not bdf.empty:
        bdf["ts_ms"] = bdf["timestamp"].astype(int)
        bdf["ts_utc"] = bdf["ts_ms"].apply(iso_z)
        bdf["binance_oi_contracts"] = bdf["sumOpenInterest"].astype(float)
        bdf["binance_oi_usd"] = bdf["sumOpenInterestValue"].astype(float)
        base = bdf[["ts_ms","ts_utc","binance_oi_contracts","binance_oi_usd"]]
    else:
        base = pd.DataFrame(columns=["ts_ms","ts_utc","binance_oi_contracts","binance_oi_usd"])

    # Bybit v5 open-interest (cursor 페이징)
    by_list, cursor = [], None
    while True:
        params = {
            "category": "linear", "symbol": SYMBOL,
            "intervalTime": "1h", "startTime": start_ms, "endTime": end_ms, "limit": 200
        }
        if cursor: params["cursor"] = cursor
        j = byb.get("/v5/market/open-interest", params)
        res = j.get("result", {})
        for it in res.get("list", []):
            by_list.append({"ts_ms": int(it["timestamp"]), "bybit_oi_contracts": float(it["openInterest"])})
        cursor = res.get("nextPageCursor")
        if not cursor: break
        time.sleep(0.02)
    ydf = pd.DataFrame(by_list)

    out = base.merge(ydf, on="ts_ms", how="left").sort_values("ts_ms")
    out["oi_1h_pct"]  = out["binance_oi_usd"].pct_change()*100.0
    out["oi_24h_pct"] = out["binance_oi_usd"].pct_change(24)*100.0
    out["total_oi_usd"] = out["binance_oi_usd"]
    write_csv(out[["ts_utc","binance_oi_contracts","binance_oi_usd","bybit_oi_contracts",
                   "total_oi_usd","oi_1h_pct","oi_24h_pct"]],
              os.path.join(DATA_DIR, "05_arb_oi_1h_30d.csv"))

# -------------------- 06: 오더북 스냅샷 1분 · 8h 롤링 --------------------
def job_06_arb_orderbook_depth_1min_8h():
    # /fapi/v1/depth limit 최대 1000
    j = bin.get("/fapi/v1/depth", {"symbol": SYMBOL, "limit": 1000})
    bids = [(float(p), float(q)) for p,q in j["bids"]]
    asks = [(float(p), float(q)) for p,q in j["asks"]]
    best_bid, best_ask = bids[0][0], asks[0][0]
    mid = (best_bid + best_ask)/2.0
    spread_bps = (best_ask - best_bid)/mid * 10000.0

    def band_sum(levels, side, pct):
        if side == "bid":
            cut = mid*(1-pct); sel = [(p,q) for p,q in levels if p >= cut]
        else:
            cut = mid*(1+pct); sel = [(p,q) for p,q in levels if p <= cut]
        qty = sum(q for p,q in sel)
        usd = sum(p*q for p,q in sel)
        return qty, usd

    b05q,b05u = band_sum(bids,"bid",0.005); a05q,a05u = band_sum(asks,"ask",0.005)
    b10q,b10u = band_sum(bids,"bid",0.01 ); a10q,a10u = band_sum(asks,"ask",0.01 )

    row = pd.DataFrame([{
        "ts_utc": iso_z(j.get("T", now_ms())),
        "mid": mid, "spread_bps": spread_bps,
        "depth_bid_0p5pct": b05q, "depth_bid_0p5pct_usd": b05u,
        "depth_ask_0p5pct": a05q, "depth_ask_0p5pct_usd": a05u,
        "depth_bid_1pct": b10q,  "depth_bid_1pct_usd": b10u,
        "depth_ask_1pct": a10q,  "depth_ask_1pct_usd": a10u
    }])

    path = os.path.join(DATA_DIR, "06_arb_orderbook_depth_1min_8h.csv")
    if os.path.exists(path):
        df0 = pd.read_csv(path)
        df = pd.concat([df0, row], ignore_index=True)
        cutoff = datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(hours=8)
        df["ts_dt"] = pd.to_datetime(df["ts_utc"])
        df = df[df["ts_dt"] >= cutoff].drop(columns=["ts_dt"])
    else:
        df = row
    write_csv(df, path)

# -------------------- 07: 리퀴데이션 1m · 7d --------------------
def job_07_arb_liquidations_1m_7d():
    # REST allForceOrders: 7일 범위 제한(변경 로그) – 휴리스틱 매핑 포함
    end_ms = now_ms()
    start_ms = end_ms - 7*24*60*60*1000
    out, cur = [], start_ms
    while True:
        arr = bin.get("/fapi/v1/allForceOrders", {
            "symbol": SYMBOL, "startTime": cur, "endTime": end_ms, "limit": 1000
        })
        if not arr: break
        for it in arr:
            price = float(it.get("avgPrice") or it.get("price") or 0.0)
            qty   = float(it.get("executedQty") or it.get("origQty") or 0.0)
            side  = it.get("side","")
            ts    = int(it.get("time") or it.get("updateTime") or 0)
            out.append({
                "ts_ms": ts,
                "raw_side": side,
                "raw_notional_usd": price*qty,
                "mapping_assumption": "SELL->long_liq, BUY->short_liq"
            })
        last_ts = int(arr[-1].get("time") or arr[-1].get("updateTime") or 0)
        nxt = last_ts + 1
        if nxt >= end_ms: break
        cur = nxt
        time.sleep(0.02)
    if not out:
        write_csv(pd.DataFrame(columns=["ts_utc","liq_long_usd","liq_short_usd","count_long","count_short",
                                        "raw_sell_usd","raw_buy_usd","mapping_assumption"]),
                  os.path.join(DATA_DIR, "07_arb_liquidations_1m_7d.csv"))
        return
    df = pd.DataFrame(out)
    df["ts_utc_min"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True).dt.floor("min")
    raw_sell = df[df["raw_side"]=="SELL"].groupby("ts_utc_min")["raw_notional_usd"].sum().rename("raw_sell_usd")
    raw_buy  = df[df["raw_side"]=="BUY" ].groupby("ts_utc_min")["raw_notional_usd"].sum().rename("raw_buy_usd")
    c_sell   = df[df["raw_side"]=="SELL"].groupby("ts_utc_min")["raw_notional_usd"].size().rename("count_sell")
    c_buy    = df[df["raw_side"]=="BUY" ].groupby("ts_utc_min")["raw_notional_usd"].size().rename("count_buy")
    g = pd.concat([raw_sell, raw_buy, c_sell, c_buy], axis=1).fillna(0.0).reset_index()
    g["liq_long_usd"]  = g["raw_sell_usd"]
    g["liq_short_usd"] = g["raw_buy_usd"]
    g["count_long"]    = g["count_sell"]
    g["count_short"]   = g["count_buy"]
    g["mapping_assumption"] = "SELL->long_liq, BUY->short_liq"
    g["ts_utc"] = g["ts_utc_min"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    outdf = g[["ts_utc","liq_long_usd","liq_short_usd","count_long","count_short",
               "raw_sell_usd","raw_buy_usd","mapping_assumption"]].sort_values("ts_utc")
    write_csv(outdf, os.path.join(DATA_DIR, "07_arb_liquidations_1m_7d.csv"))

# -------------------- 08: 마크/인덱스 베이시스 1m · 24h --------------------
def job_08_arb_basis_mark_index_1m_24h():
    end_ms = now_ms()
    start_ms = end_ms - 24*60*60*1000
    mk = bin.get("/fapi/v1/markPriceKlines", {
        "symbol": SYMBOL, "interval": "1m", "startTime": start_ms, "endTime": end_ms, "limit": 1500
    })
    ix = bin.get("/fapi/v1/indexPriceKlines", {
        "pair": PAIR, "interval": "1m", "startTime": start_ms, "endTime": end_ms, "limit": 1500
    })
    mdf = klines_to_df(mk)[["close_time","close"]].rename(columns={"close":"mark_price"})
    idf = klines_to_df(ix)[["close_time","close"]].rename(columns={"close":"index_price"})
    df = mdf.merge(idf, on="close_time", how="inner").sort_values("close_time")
    df["ts_utc"] = df["close_time"].apply(iso_z)
    df["basis_pct"] = (df["mark_price"]/df["index_price"] - 1.0)*100.0
    write_csv(df[["ts_utc","mark_price","index_price","basis_pct"]],
              os.path.join(DATA_DIR, "08_arb_basis_mark_index_1m_24h.csv"))

# -------------------- 09: 체결 델타 1m · 24h --------------------
def job_09_arb_trades_imbalance_1m_24h():
    end_ms = now_ms()
    start_ms = end_ms - 24*60*60*1000
    df = klines_to_df(fetch_klines(SYMBOL, "1m", start_ms, end_ms))
    df["buy_qty"]   = df["taker_buy_base"]
    df["sell_qty"]  = df["volume"] - df["taker_buy_base"]
    df["buy_quote"] = df["taker_buy_quote"]
    df["sell_quote"]= df["quote_volume"] - df["taker_buy_quote"]
    df["delta_quote"] = df["buy_quote"] - df["sell_quote"]
    write_csv(df[["ts_utc","buy_qty","sell_qty","buy_quote","sell_quote","delta_quote"]],
              os.path.join(DATA_DIR, "09_arb_trades_imbalance_1m_24h.csv"))

# -------------------- 10: 실행 파라미터 JSON --------------------
def job_10_agg_config_costs_json():
    conf = {
        "symbol": SYMBOL,
        "taker_fee_bps": 5.0,      # 0.05%
        "maker_fee_bps": 2.0,      # 0.02%
        "slippage_bps_est": 5.0,
        "vol_ratio_strong": 1.5,
        "oi_spike_pct": 10.0,
        "funding_hot_abs": 0.10,
        "schedules": {
            "entry_fast":   "*/5 * * * *",
            "stable":       "*/15 * * * *",
            "funding_fast": "*/20 * * * *"
        },
        "drift_threshold_ms": 1500
    }
    write_json(conf, os.path.join(DATA_DIR, "10_agg_config_costs.json"))

# -------------------- 메인 --------------------
def main():
    ensure_dir(DATA_DIR)
    drift = drift_check_ms()
    if drift > 1500:
        print(f"[WARN] server-local drift {drift}ms > 1500ms")

    jobs_env = os.environ.get("JOBS","").strip()
    selected = set(j.strip() for j in jobs_env.split(",") if j.strip())

    def do(n, fn):
        if (not selected) or (str(n) in selected):
            print(f"[RUN] job {n:02d} start"); fn(); print(f"[RUN] job {n:02d} done")

    do(1, job_01_arb_15m_20d)
    do(2, job_02_arb_1d_180d)
    do(3, job_03_arb_1m_today)
    do(4, job_04_arb_funding_8h_30d)
    do(5, job_05_arb_oi_1h_30d)
    do(6, job_06_arb_orderbook_depth_1min_8h)
    do(7, job_07_arb_liquidations_1m_7d)
    do(8, job_08_arb_basis_mark_index_1m_24h)
    do(9, job_09_arb_trades_imbalance_1m_24h)
    do(10, job_10_agg_config_costs_json)

    print("[OK] generated under ./data")

if __name__ == "__main__":
    main()
