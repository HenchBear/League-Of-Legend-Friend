"""
롤 회원 대시보드 (회원 리스트 · 챔피언별 최고 숙련자)

설계:
  - 회원 명단 + 각 회원의 레벨/아이콘/숙련도를 DB(Postgres)에 저장
  - 공개 페이지는 DB만 읽어서 즉시 렌더 (API 호출 없음 → 빠르고 타임아웃 없음)
  - 관리자가 '정보 갱신' 버튼을 누를 때만 Riot API로 최신 데이터를 받아 DB에 저장

Riot API 라우팅:
  - ACCOUNT-V1  → asia (라이엇 ID → puuid)
  - SUMMONER-V4 / CHAMPION-MASTERY-V4 → kr

환경변수: RIOT_API_KEY, DATABASE_URL, ADMIN_PASSWORD, SECRET_KEY
실행: gunicorn app:app --bind 0.0.0.0:$PORT --timeout 120
"""

import os
import time
import math
import random
import threading
from functools import wraps
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor

import requests
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from flask import Flask, request, jsonify, render_template_string, session, redirect

# ===================== 설정 =====================
RIOT_API_KEY = os.environ.get("RIOT_API_KEY", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "")
SECRET_KEY = os.environ.get("SECRET_KEY", "change-me")
PLATFORM = "kr"      # summoner/mastery 호스트
REGION = "asia"      # account 호스트
WORKERS = 6          # 갱신 시 동시에 처리할 회원 수
# ===============================================

PLATFORM_HOST = f"https://{PLATFORM}.api.riotgames.com"
REGION_HOST = f"https://{REGION}.api.riotgames.com"

app = Flask(__name__)
app.secret_key = SECRET_KEY

_ddragon = {"ts": 0, "version": None, "champions": {}}

# 갱신 진행 상태(백그라운드) — gunicorn은 --workers 1 로 실행해야 상태가 일관됨
REFRESH = {"running": False, "updated": 0, "failed": 0, "total": 0,
           "errors": [], "finished_at": None}
REFRESH_LOCK = threading.Lock()


# ---------- DB ----------
def get_db():
    return psycopg2.connect(DATABASE_URL)


def init_db():
    conn = get_db()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS players (
                    id             SERIAL PRIMARY KEY,
                    game_name      TEXT NOT NULL,
                    tag_line       TEXT NOT NULL,
                    puuid          TEXT UNIQUE NOT NULL,
                    summoner_level INT,
                    profile_icon_id INT,
                    updated_at     TIMESTAMPTZ,
                    added_at       TIMESTAMPTZ DEFAULT now(),
                    UNIQUE (game_name, tag_line)
                );
            """)
            # 기존 DB(구버전 테이블) 대비 컬럼 보강
            cur.execute("ALTER TABLE players ADD COLUMN IF NOT EXISTS summoner_level INT;")
            cur.execute("ALTER TABLE players ADD COLUMN IF NOT EXISTS profile_icon_id INT;")
            cur.execute("ALTER TABLE players ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;")
            cur.execute("ALTER TABLE players ADD COLUMN IF NOT EXISTS view_count INT DEFAULT 0;")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS mastery (
                    player_id   INT REFERENCES players(id) ON DELETE CASCADE,
                    champion_id INT NOT NULL,
                    points      INT NOT NULL,
                    level       INT NOT NULL,
                    PRIMARY KEY (player_id, champion_id)
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS join_requests (
                    id           SERIAL PRIMARY KEY,
                    riot_id      TEXT NOT NULL,
                    requested_at TIMESTAMPTZ DEFAULT now()
                );
            """)
            cur.execute("CREATE TABLE IF NOT EXISTS app_meta (k TEXT PRIMARY KEY, v TIMESTAMPTZ);")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS battle_effects (
                    id       SERIAL PRIMARY KEY,
                    context  TEXT NOT NULL DEFAULT 'both',   -- 'round1' | 'round2' | 'both'
                    label    TEXT NOT NULL DEFAULT '',       -- 화면 표시 문구(비우면 기호만)
                    op       TEXT NOT NULL,                  -- 'add','sub','mul','div'
                    symbol   TEXT NOT NULL,                  -- 표시 기호
                    weight   INT  NOT NULL DEFAULT 1,        -- 뽑힐 가중치(클수록 자주)
                    active   BOOLEAN NOT NULL DEFAULT true
                );
            """)
            cur.execute("""
                INSERT INTO battle_effects (context, label, op, symbol, weight)
                SELECT * FROM (VALUES
                    ('both','','add','+',1),
                    ('both','','sub','−',1),
                    ('both','','mul','×',1),
                    ('both','','div','÷',1)
                ) v(context,label,op,symbol,weight)
                WHERE NOT EXISTS (SELECT 1 FROM battle_effects);
            """)
            cur.execute("ALTER TABLE battle_effects ADD COLUMN IF NOT EXISTS title TEXT NOT NULL DEFAULT '';")
            cur.execute("ALTER TABLE battle_effects ADD COLUMN IF NOT EXISTS description TEXT NOT NULL DEFAULT '';")
            cur.execute("ALTER TABLE battle_effects ADD COLUMN IF NOT EXISTS operand INT;")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS battle_events (
                    id          SERIAL PRIMARY KEY,
                    title       TEXT NOT NULL DEFAULT '',
                    description TEXT NOT NULL DEFAULT '',
                    kind        TEXT NOT NULL,           -- 'none' | 'ban_op' | 'start_bonus'
                    param       TEXT,                    -- ban_op: 'add'/'sub'/'mul'/'div', start_bonus: '500'
                    weight      INT NOT NULL DEFAULT 1,
                    active      BOOLEAN NOT NULL DEFAULT true
                );
            """)
            cur.execute("""
                INSERT INTO battle_events (title, description, kind, param, weight)
                SELECT * FROM (VALUES
                    ('평범한 협곡','별다른 사건 없이 정정당당','none',NULL,3),
                    ('곱셈 봉인','이번 판은 곱셈이 금지된다','ban_op','mul',1),
                    ('덧셈 봉인','이번 판은 덧셈이 금지된다','ban_op','add',1),
                    ('뺄셈 봉인','이번 판은 뺄셈이 금지된다','ban_op','sub',1),
                    ('나눗셈 봉인','이번 판은 나눗셈이 금지된다','ban_op','div',1),
                    ('축복의 시작','전 회원 시작 점수 +500','start_bonus','500',1)
                ) v(title,description,kind,param,weight)
                WHERE NOT EXISTS (SELECT 1 FROM battle_events);
            """)
    finally:
        conn.close()


try:
    if DATABASE_URL:
        init_db()
except Exception as e:
    print("init_db 실패(부팅은 계속):", e)


# ---------- Riot ----------
def riot_get(host, path, params=None):
    for _ in range(5):
        r = requests.get(host + path, headers={"X-Riot-Token": RIOT_API_KEY},
                         params=params, timeout=(5, 12))
        if r.status_code == 429:
            time.sleep(min(int(r.headers.get("Retry-After", 3)), 60))
            continue
        r.raise_for_status()
        return r.json()
    r.raise_for_status()
    return r.json()


def fetch_player_store(puuid):
    """저장용 데이터: 레벨, 아이콘, 전체 숙련도 (티어/리그는 사용 안 함)."""
    summ = riot_get(PLATFORM_HOST, f"/lol/summoner/v4/summoners/by-puuid/{puuid}")
    mastery = riot_get(PLATFORM_HOST, f"/lol/champion-mastery/v4/champion-masteries/by-puuid/{puuid}")
    return {
        "level": summ.get("summonerLevel"),
        "iconId": summ.get("profileIconId"),
        "mastery": [{"championId": m["championId"], "points": m["championPoints"],
                     "level": m["championLevel"]} for m in mastery],
    }


def store_player(conn, player_id, game_name, tag_line, puuid=None):
    # 저장된 puuid를 먼저 사용. 키가 바뀌어 400이 나면 이름으로 다시 받아온다.
    data = None
    used = puuid
    if puuid:
        try:
            data = fetch_player_store(puuid)
        except requests.HTTPError as e:
            if not (e.response is not None and e.response.status_code == 400):
                raise
            data = None
    if data is None:
        acc = riot_get(REGION_HOST,
                       f"/riot/account/v1/accounts/by-riot-id/{quote(game_name)}/{quote(tag_line)}")
        used = acc["puuid"]
        data = fetch_player_store(used)
    with conn.cursor() as cur:
        cur.execute("""UPDATE players
                       SET puuid=%s, summoner_level=%s, profile_icon_id=%s, updated_at=now()
                       WHERE id=%s""",
                    (used, data["level"], data["iconId"], player_id))
        cur.execute("DELETE FROM mastery WHERE player_id=%s", (player_id,))
        if data["mastery"]:
            execute_values(cur,
                "INSERT INTO mastery (player_id, champion_id, points, level) VALUES %s",
                [(player_id, m["championId"], m["points"], m["level"]) for m in data["mastery"]])
    conn.commit()


# ---------- Data Dragon ----------
def get_ddragon():
    now = time.time()
    if _ddragon["version"] and now - _ddragon["ts"] < 86400:
        return _ddragon
    ver = requests.get("https://ddragon.leagueoflegends.com/api/versions.json", timeout=10).json()[0]
    data = requests.get(f"https://ddragon.leagueoflegends.com/cdn/{ver}/data/ko_KR/champion.json",
                        timeout=10).json()["data"]
    id_map = {int(c["key"]): {"id": c["id"], "name": c["name"]} for c in data.values()}
    _ddragon.update(ts=now, version=ver, champions=id_map)
    return _ddragon


def safe_ddragon():
    try:
        return get_ddragon()
    except Exception:
        return {"version": None, "champions": {}}


# ---------- 관리자 인증 ----------
def admin_required(fn):
    @wraps(fn)
    def wrapper(*a, **k):
        if not session.get("admin"):
            return jsonify({"error": "관리자 로그인이 필요합니다."}), 401
        return fn(*a, **k)
    return wrapper


# ---------- 공개 API (DB만 읽음) ----------
@app.route("/api/members")
def api_members():
    dd = safe_ddragon()
    ver = dd["version"]
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""SELECT id, game_name, tag_line, summoner_level, profile_icon_id, updated_at, view_count
                           FROM players
                           ORDER BY summoner_level DESC NULLS LAST, added_at""")
            rows = cur.fetchall()
            cur.execute("""
                SELECT player_id, champion_id FROM (
                    SELECT player_id, champion_id,
                           ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY points DESC) AS rn
                    FROM mastery
                ) t WHERE rn <= 5 ORDER BY player_id, rn
            """)
            top_rows = cur.fetchall()
            cur.execute("SELECT player_id, SUM(points) AS total FROM mastery GROUP BY player_id")
            total_by_player = {r["player_id"]: int(r["total"]) for r in cur.fetchall()}
            cur.execute("SELECT v FROM app_meta WHERE k = 'last_refresh'")
            mrow = cur.fetchone()
            refreshed_at = mrow["v"] if mrow else None
    finally:
        conn.close()

    top_by_player = {}
    for r in top_rows:
        top_by_player.setdefault(r["player_id"], []).append(r["champion_id"])

    # 인기쟁이: 조회수 상위 3명 (조회수 0은 제외)
    viewed = sorted([r for r in rows if (r["view_count"] or 0) > 0],
                    key=lambda r: r["view_count"], reverse=True)
    popular_ids = {r["id"] for r in viewed[:3]}

    members = []
    for r in rows:
        champs = []
        for cid in top_by_player.get(r["id"], []):
            info = dd["champions"].get(cid)
            champs.append({"name": info["name"] if info else str(cid),
                           "img": info["id"] if info else None})
        members.append({"id": r["id"], "name": r["game_name"], "tag": r["tag_line"],
                        "level": r["summoner_level"], "iconId": r["profile_icon_id"],
                        "views": r["view_count"] or 0, "popular": r["id"] in popular_ids,
                        "total": total_by_player.get(r["id"], 0),
                        "topMastery": champs})
    return jsonify({"version": ver, "members": members,
                    "refreshedAt": refreshed_at.isoformat() if refreshed_at else None})


@app.route("/api/member/<int:pid>")
def api_member(pid):
    dd = safe_ddragon()
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""UPDATE players SET view_count = COALESCE(view_count,0)+1
                           WHERE id = %s
                           RETURNING game_name, tag_line, summoner_level, profile_icon_id,
                                     updated_at, view_count""", (pid,))
            p = cur.fetchone()
            if not p:
                return jsonify({"error": "회원을 찾을 수 없습니다."}), 404
            cur.execute("SELECT COUNT(*) AS c FROM players WHERE COALESCE(view_count,0) > %s",
                        (p["view_count"],))
            pop_rank = cur.fetchone()["c"] + 1
            cur.execute("""SELECT champion_id, points, level FROM mastery
                           WHERE player_id = %s ORDER BY points DESC""", (pid,))
            mrows = cur.fetchall()
        conn.commit()
    finally:
        conn.close()

    champs, total = [], 0
    for m in mrows:
        info = dd["champions"].get(m["champion_id"])
        champs.append({"name": info["name"] if info else str(m["champion_id"]),
                       "img": info["id"] if info else None,
                       "points": m["points"], "level": m["level"]})
        total += m["points"]
    opgg = "https://www.op.gg/summoners/kr/" + quote(f"{p['game_name']}-{p['tag_line']}")
    return jsonify({
        "version": dd["version"], "name": p["game_name"], "tag": p["tag_line"],
        "level": p["summoner_level"], "iconId": p["profile_icon_id"],
        "total": total, "champCount": len(champs),
        "views": p["view_count"], "popRank": pop_rank if p["view_count"] > 0 else None,
        "updatedAt": p["updated_at"].isoformat() if p["updated_at"] else None,
        "opgg": opgg, "champions": champs,
    })


@app.route("/api/compare")
def api_compare():
    a = request.args.get("a", type=int)
    b = request.args.get("b", type=int)
    if not a or not b or a == b:
        return jsonify({"error": "서로 다른 두 회원을 선택하세요."}), 400
    dd = safe_ddragon()
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            side = {}
            for key, pid in (("a", a), ("b", b)):
                cur.execute("""SELECT game_name, tag_line, summoner_level, profile_icon_id, view_count
                               FROM players WHERE id = %s""", (pid,))
                p = cur.fetchone()
                if not p:
                    return jsonify({"error": "회원을 찾을 수 없습니다."}), 404
                cur.execute("SELECT COALESCE(SUM(points),0) AS total, COUNT(*) AS cnt FROM mastery WHERE player_id = %s", (pid,))
                srow = cur.fetchone()
                total, cnt = int(srow["total"]), int(srow["cnt"])
                cur.execute("""SELECT champion_id FROM mastery WHERE player_id = %s
                               ORDER BY points DESC LIMIT 5""", (pid,))
                top_ids = [r["champion_id"] for r in cur.fetchall()]
                side[key] = {"name": p["game_name"], "tag": p["tag_line"],
                             "level": p["summoner_level"], "iconId": p["profile_icon_id"],
                             "views": p["view_count"] or 0, "total": total,
                             "champCount": cnt, "_top": top_ids}

            union_ids = list(set(side["a"]["_top"]) | set(side["b"]["_top"]))

            def points_on(pid):
                if not union_ids:
                    return {}
                cur.execute("""SELECT champion_id, points FROM mastery
                               WHERE player_id = %s AND champion_id = ANY(%s)""", (pid, union_ids))
                return {r["champion_id"]: r["points"] for r in cur.fetchall()}
            pa, pb = points_on(a), points_on(b)
    finally:
        conn.close()

    champs = []
    for cid in union_ids:
        info = dd["champions"].get(cid, {})
        champs.append({"championId": cid, "name": info.get("name", str(cid)),
                       "img": info.get("id"), "aM": pa.get(cid, 0), "bM": pb.get(cid, 0)})
    champs.sort(key=lambda c: c["aM"] + c["bM"], reverse=True)
    champs = champs[:14]
    for s in (side["a"], side["b"]):
        s.pop("_top", None)

    log = build_battle(side["a"], side["b"], champs)
    return jsonify({"version": dd["version"],
                    "a": {k: side["a"][k] for k in ("name", "tag", "iconId")},
                    "b": {k: side["b"][k] for k in ("name", "tag", "iconId")},
                    **log})


# ---------- 로그라이크 전투 엔진 ----------
OPS = [("add", "+"), ("sub", "−"), ("mul", "×"), ("div", "÷")]
SYM = {"add": "+", "sub": "−", "mul": "×", "div": "÷", "rev": "↺", "swap": "⇄"}
UNARY_OPS = ("rev", "swap")   # 피연산자 없이 점수 자체를 변형


def load_effects(context):
    """DB battle_effects에서 활성 효과를 로드. 실패/빈 경우 코드 기본값으로 폴백."""
    pool = []
    try:
        conn = get_db()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""SELECT op, symbol, title, description, operand, weight FROM battle_effects
                               WHERE active AND weight > 0
                                 AND (context = %s OR context = 'both')""", (context,))
                pool = [{"op": r["op"], "sym": r["symbol"],
                         "title": r["title"] or "", "desc": r["description"] or "",
                         "operand": r["operand"], "w": int(r["weight"])} for r in cur.fetchall()]
        finally:
            conn.close()
    except Exception as e:
        print("load_effects 실패:", e)
    if not pool:
        pool = [{"op": o, "sym": s, "title": "", "desc": "", "operand": None, "w": 1} for o, s in OPS]
    return pool


def pick_effect(pool):
    total = sum(e["w"] for e in pool)
    r = random.uniform(0, total)
    acc = 0
    for e in pool:
        acc += e["w"]
        if r <= acc:
            return e
    return pool[-1]


def load_events():
    pool = []
    try:
        conn = get_db()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""SELECT title, description, kind, param, weight FROM battle_events
                               WHERE active AND weight > 0""")
                pool = [{"title": r["title"] or "", "desc": r["description"] or "",
                         "kind": r["kind"], "param": r["param"], "w": int(r["weight"])}
                        for r in cur.fetchall()]
        finally:
            conn.close()
    except Exception as e:
        print("load_events 실패:", e)
    return pool


def pick_event():
    pool = load_events()
    if not pool:
        return {"title": "평범한 협곡", "desc": "", "kind": "none", "param": None}
    total = sum(e["w"] for e in pool)
    r = random.uniform(0, total)
    acc = 0
    for e in pool:
        acc += e["w"]
        if r <= acc:
            return e
    return pool[-1]


def digital_root(n):
    """자릿수를 반복해서 더해 한 자리로 (0~9). 밸런스 모드 압축."""
    n = abs(int(n or 0))
    return 0 if n == 0 else 1 + (n - 1) % 9


def _reverse_digits(n):
    n = int(n)
    sign = -1 if n < 0 else 1
    return sign * int(str(abs(n))[::-1] or "0")


def _swap_ends(n):
    n = int(n)
    sign = -1 if n < 0 else 1
    s = list(str(abs(n)))
    if len(s) > 1:
        s[0], s[-1] = s[-1], s[0]
    return sign * int("".join(s))


def apply_op(score, op, operand):
    if op == "add":
        return score + operand
    if op == "sub":
        return score - operand
    if op == "mul":
        return score * operand
    if op == "div":
        if operand == 0:
            operand = 1                      # ÷0 방어
        return math.floor(score / operand + 0.5)   # 반올림
    if op == "rev":
        return _reverse_digits(score)        # 자릿수 뒤집기 (피연산자 무시)
    if op == "swap":
        return _swap_ends(score)             # 앞뒤 자리 교환 (피연산자 무시)
    return score


def _winner(a, b):
    return "a" if a > b else ("b" if b > a else "")


def build_battle(A, B, champs):
    event = pick_event()
    pool1 = load_effects("round1")
    pool2 = load_effects("round2")
    pool_lucky = load_effects("lucky")
    if event["kind"] == "ban_op":
        banned = event["param"]

        def _ban(p):
            q = [e for e in p if e["op"] != banned]
            if not q:
                q = [{"op": o, "sym": s, "title": "", "desc": "", "operand": None, "w": 1}
                     for o, s in OPS if o != banned]
            return q
        # 연산 금지는 1라운드(행운의 뽑기 포함)에만 적용. 2라운드는 영향 없음.
        pool1, pool_lucky = _ban(pool1), _ban(pool_lucky)
    try:
        bonus = int(event["param"]) if event["kind"] == "start_bonus" else 0
    except (TypeError, ValueError):
        bonus = 0
    start = 1000 + bonus

    # ---- 라운드 1: 스탯 대결 (start점 시작, 5전투 누적) ----
    a_score = b_score = start
    r1 = []
    stat_defs = [
        ("레벨", A.get("level") or 0, B.get("level") or 0),
        ("총 숙련도", A.get("total") or 0, B.get("total") or 0),
        ("플레이한 챔피언", A.get("champCount") or 0, B.get("champCount") or 0),
        ("조회수", A.get("views") or 1, B.get("views") or 1),   # 0은 1로 간주
    ]
    for label, av, bv in stat_defs:
        eff = pick_effect(pool1)
        op, sym = eff["op"], eff["sym"]
        aop, bop = digital_root(av), digital_root(bv)
        ab, bb = a_score, b_score
        a_score, b_score = apply_op(a_score, op, aop), apply_op(b_score, op, bop)
        r1.append({"label": label, "effTitle": eff["title"], "effDesc": eff["desc"], "op": op, "sym": sym,
                   "aOperand": aop, "bOperand": bop,
                   "aBefore": ab, "bBefore": bb, "aAfter": a_score, "bAfter": b_score,
                   "lead": _winner(a_score, b_score)})
    # 5전투: 랜덤 0~9 (양쪽 각각 뽑기, 재미요소로 0 포함) — 행운의 뽑기 전용 풀
    eff = pick_effect(pool_lucky)
    op, sym = eff["op"], eff["sym"]
    aop, bop = random.randint(0, 9), random.randint(0, 9)
    ab, bb = a_score, b_score
    a_score, b_score = apply_op(a_score, op, aop), apply_op(b_score, op, bop)
    r1.append({"label": "행운의 뽑기", "effTitle": eff["title"], "effDesc": eff["desc"],
               "op": op, "sym": sym, "random": True,
               "aOperand": aop, "bOperand": bop,
               "aBefore": ab, "bBefore": bb, "aAfter": a_score, "bAfter": b_score,
               "lead": _winner(a_score, b_score)})
    round1 = {"start": start, "battles": r1, "aFinal": a_score, "bFinal": b_score,
              "winner": _winner(a_score, b_score)}

    # ---- 라운드 2: 챔피언 대결 (원 숙련도 기준, 각 회원 각자 랜덤 사칙연산) ----
    r2 = []
    aw = bw = 0
    for c in champs:
        ea = pick_effect(pool2)
        a_operand = ea["operand"] if ea["operand"] is not None else random.randint(0, 9)
        eb = pick_effect(pool2)
        b_operand = eb["operand"] if eb["operand"] is not None else random.randint(0, 9)
        a_res = apply_op(c["aM"], ea["op"], a_operand)
        b_res = apply_op(c["bM"], eb["op"], b_operand)
        w = _winner(a_res, b_res)
        if w == "a":
            aw += 1
        elif w == "b":
            bw += 1
        r2.append({"championId": c["championId"], "name": c["name"], "img": c["img"],
                   "aSym": ea["sym"], "bSym": eb["sym"],
                   "aOp": ea["op"], "bOp": eb["op"],
                   "aTitle": ea["title"], "aDesc": ea["desc"],
                   "bTitle": eb["title"], "bDesc": eb["desc"],
                   "aBase": c["aM"], "bBase": c["bM"],
                   "aOperand": a_operand, "bOperand": b_operand,
                   "aScore": a_res, "bScore": b_res, "winner": w})
    round2 = {"battles": r2, "aWins": aw, "bWins": bw, "winner": _winner(aw, bw)}

    # ---- 최종: 두 라운드 중 더 많이 이긴 쪽 ----
    ra = (1 if round1["winner"] == "a" else 0) + (1 if round2["winner"] == "a" else 0)
    rb = (1 if round1["winner"] == "b" else 0) + (1 if round2["winner"] == "b" else 0)
    return {"round1": round1, "round2": round2,
            "event": {"title": event["title"], "description": event["desc"],
                      "kind": event["kind"], "param": event["param"]},
            "roundsWon": {"a": ra, "b": rb}, "overall": _winner(ra, rb)}


@app.route("/api/champion-top")
def api_champion_top():
    dd = safe_ddragon()
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT DISTINCT ON (m.champion_id)
                       m.champion_id, m.points, m.level, p.game_name, p.tag_line
                FROM mastery m JOIN players p ON p.id = m.player_id
                ORDER BY m.champion_id, m.points DESC
            """)
            tops = {r["champion_id"]: r for r in cur.fetchall()}
    finally:
        conn.close()

    out = []
    for cid, info in dd["champions"].items():
        t = tops.get(cid)
        out.append({
            "championId": cid, "name": info["name"], "img": info["id"],
            "top": ({"name": t["game_name"], "tag": t["tag_line"],
                     "points": t["points"], "level": t["level"]} if t else None),
        })
    # 기록 있는 챔피언 먼저(포인트 내림차순), 그다음 기록 없는 챔피언(이름순)
    out.sort(key=lambda x: (0 if x["top"] else 1,
                            -(x["top"]["points"] if x["top"] else 0), x["name"]))
    return jsonify({"version": dd["version"], "champions": out})


@app.route("/api/champion-detail")
def api_champion_detail():
    cid = request.args.get("championId", type=int)
    if not cid:
        return jsonify({"error": "championId가 필요합니다."}), 400
    dd = safe_ddragon()
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""SELECT p.game_name, p.tag_line, m.points, m.level
                           FROM mastery m JOIN players p ON p.id = m.player_id
                           WHERE m.champion_id = %s
                           ORDER BY m.points DESC""", (cid,))
            rows = cur.fetchall()
    finally:
        conn.close()
    champ = dd["champions"].get(cid, {})
    players = [{"name": r["game_name"], "tag": r["tag_line"],
                "points": r["points"], "level": r["level"]} for r in rows]
    return jsonify({"champion": champ.get("name"), "img": champ.get("id"),
                    "version": dd["version"], "players": players})


@app.route("/api/ranking")
def api_ranking():
    dd = safe_ddragon()
    teemo_id = next((cid for cid, info in dd["champions"].items() if info["name"] == "티모"), 17)

    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""SELECT game_name, tag_line, summoner_level AS val, NULL::int AS champion_id
                           FROM players WHERE summoner_level IS NOT NULL
                           ORDER BY summoner_level DESC""")
            level = cur.fetchall()
            # 회원별 최고 숙련 챔피언 1개씩 → 파이썬에서 val 내림차순 정렬
            cur.execute("""SELECT DISTINCT ON (p.id) p.game_name, p.tag_line, m.points AS val, m.champion_id
                           FROM mastery m JOIN players p ON p.id = m.player_id
                           ORDER BY p.id, m.points DESC""")
            single = cur.fetchall()
            single.sort(key=lambda r: r["val"], reverse=True)
            cur.execute("""SELECT p.game_name, p.tag_line, SUM(m.points) AS val, NULL::int AS champion_id
                           FROM mastery m JOIN players p ON p.id = m.player_id
                           GROUP BY p.id, p.game_name, p.tag_line
                           ORDER BY val DESC""")
            total = cur.fetchall()
            cur.execute("""SELECT p.game_name, p.tag_line, COUNT(*) AS val, NULL::int AS champion_id
                           FROM (
                             SELECT DISTINCT ON (champion_id) champion_id, player_id
                             FROM mastery ORDER BY champion_id, points DESC
                           ) t JOIN players p ON p.id = t.player_id
                           GROUP BY p.id, p.game_name, p.tag_line
                           ORDER BY val DESC""")
            most1st = cur.fetchall()
            cur.execute("""SELECT p.game_name, p.tag_line, m.points AS val, m.champion_id
                           FROM mastery m JOIN players p ON p.id = m.player_id
                           WHERE m.champion_id = %s ORDER BY m.points DESC""", (teemo_id,))
            teemo = cur.fetchall()
    finally:
        conn.close()

    def simp(rows, with_champ=False):
        out = []
        for r in rows:
            e = {"name": r["game_name"], "tag": r["tag_line"], "value": int(r["val"])}
            if with_champ and r.get("champion_id") is not None:
                info = dd["champions"].get(r["champion_id"])
                if info:
                    e["champName"] = info["name"]
                    e["champImg"] = info["id"]
            out.append(e)
        return out

    rankings = [
        {"key": "level", "title": "최고 레벨", "unit": "레벨", "players": simp(level)},
        {"key": "single", "title": "단일 챔피언 최고 숙련", "unit": "점", "players": simp(single, True)},
        {"key": "total", "title": "숙련도 총합", "unit": "점", "players": simp(total)},
        {"key": "most1st", "title": "챔피언 1등 최다", "unit": "개", "players": simp(most1st)},
        {"key": "teemo", "title": "귀여운 티모 숙련도 1등", "unit": "점",
         "players": simp(teemo), "img": dd["champions"].get(teemo_id, {}).get("id")},
    ]
    return jsonify({"version": dd["version"], "rankings": rankings})


# ---------- 관리자 API ----------
@app.route("/admin/login", methods=["POST"])
def admin_login():
    if not ADMIN_PASSWORD:
        return jsonify({"error": "서버에 ADMIN_PASSWORD가 없습니다."}), 500
    if (request.form.get("password") or "") == ADMIN_PASSWORD:
        session["admin"] = True
        return redirect("/admin")
    return redirect("/admin?err=1")


@app.route("/admin/logout")
def admin_logout():
    session.pop("admin", None)
    return redirect("/admin")


@app.route("/api/admin/list")
@admin_required
def admin_list():
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""SELECT id, game_name, tag_line, summoner_level, updated_at
                           FROM players ORDER BY game_name, tag_line""")
            rows = cur.fetchall()
    finally:
        conn.close()
    for r in rows:
        r["updated_at"] = r["updated_at"].isoformat() if r["updated_at"] else None
    return jsonify({"players": rows})


def add_member(raw):
    """라이엇 ID 문자열로 회원을 해석·등록·저장. (status_code, body dict) 반환."""
    raw = (raw or "").strip()
    if not raw:
        return 400, {"error": "라이엇 ID를 입력하세요."}
    if "#" in raw:
        name, tag = raw.rsplit("#", 1)
    else:
        name, tag = raw, "KR1"
    name, tag = name.strip(), tag.strip()
    try:
        acc = riot_get(REGION_HOST, f"/riot/account/v1/accounts/by-riot-id/{quote(name)}/{quote(tag)}")
    except requests.HTTPError as e:
        code = e.response.status_code
        if code == 404:
            return 404, {"error": "라이엇 ID를 찾을 수 없습니다."}
        if code in (401, 403):
            return 401, {"error": "API 키가 만료/무효입니다."}
        return 502, {"error": f"조회 오류 (HTTP {code})"}

    puuid = acc["puuid"]
    real_name = acc.get("gameName", name)
    real_tag = acc.get("tagLine", tag)
    conn = get_db()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""INSERT INTO players (game_name, tag_line, puuid)
                           VALUES (%s, %s, %s)
                           ON CONFLICT (puuid) DO NOTHING RETURNING id""",
                        (real_name, real_tag, puuid))
            row = cur.fetchone()
        new_id = row[0] if row else None
    finally:
        conn.close()

    if new_id:
        try:
            c = get_db()
            try:
                store_player(c, new_id, real_name, real_tag, puuid)
            finally:
                c.close()
        except Exception:
            pass
    return 200, {"ok": True, "added": bool(new_id), "name": f"{real_name}#{real_tag}"}


@app.route("/api/admin/add", methods=["POST"])
@admin_required
def admin_add():
    code, body = add_member((request.json or {}).get("riotId"))
    return jsonify(body), code


# ---------- 회원 등록 요청 (공개) ----------
@app.route("/api/request-join", methods=["POST"])
def request_join():
    raw = ((request.json or {}).get("riotId") or "").strip()
    if not raw or len(raw) > 60:
        return jsonify({"error": "라이엇 ID를 올바르게 입력하세요."}), 400
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM players WHERE lower(game_name || '#' || tag_line) = lower(%s)", (raw,))
            if cur.fetchone():
                return jsonify({"ok": True, "already": "member"})
            cur.execute("SELECT 1 FROM join_requests WHERE lower(riot_id) = lower(%s)", (raw,))
            if cur.fetchone():
                return jsonify({"ok": True, "already": "request"})
            cur.execute("INSERT INTO join_requests (riot_id) VALUES (%s)", (raw,))
        conn.commit()
    finally:
        conn.close()
    return jsonify({"ok": True})


@app.route("/api/admin/requests")
@admin_required
def admin_requests():
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, riot_id, requested_at FROM join_requests ORDER BY requested_at")
            rows = cur.fetchall()
    finally:
        conn.close()
    for r in rows:
        r["requested_at"] = r["requested_at"].isoformat() if r["requested_at"] else None
    return jsonify({"requests": rows})


@app.route("/api/admin/approve", methods=["POST"])
@admin_required
def admin_approve():
    rid = (request.json or {}).get("id")
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT riot_id FROM join_requests WHERE id = %s", (rid,))
            r = cur.fetchone()
    finally:
        conn.close()
    if not r:
        return jsonify({"error": "요청을 찾을 수 없습니다."}), 404
    code, body = add_member(r[0])
    if code == 200:
        conn = get_db()
        try:
            with conn, conn.cursor() as cur:
                cur.execute("DELETE FROM join_requests WHERE id = %s", (rid,))
        finally:
            conn.close()
    return jsonify(body), code


@app.route("/api/admin/reject", methods=["POST"])
@admin_required
def admin_reject():
    rid = (request.json or {}).get("id")
    conn = get_db()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM join_requests WHERE id = %s", (rid,))
    finally:
        conn.close()
    return jsonify({"ok": True})


# ---------- 전투 효과 관리 ----------
@app.route("/api/admin/effects")
@admin_required
def admin_effects():
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""SELECT id, context, title, description, op, symbol, operand, weight, active
                           FROM battle_effects ORDER BY id""")
            rows = cur.fetchall()
    finally:
        conn.close()
    return jsonify({"effects": rows})


@app.route("/api/admin/effects/add", methods=["POST"])
@admin_required
def admin_effects_add():
    d = request.json or {}
    op = (d.get("op") or "").strip()
    if op not in SYM:
        return jsonify({"error": "연산을 선택하세요."}), 400
    context = (d.get("context") or "both").strip()
    if context not in ("both", "round1", "round2", "lucky"):
        context = "both"
    label = (d.get("label") or "").strip()[:40]
    title = (d.get("title") or "").strip()[:40]
    description = (d.get("description") or "").strip()[:120]
    operand = d.get("operand")
    try:
        operand = int(operand) if operand not in (None, "", []) else None
    except (ValueError, TypeError):
        operand = None
    try:
        weight = max(1, min(100, int(d.get("weight", 1))))
    except (ValueError, TypeError):
        weight = 1
    conn = get_db()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""INSERT INTO battle_effects (context, label, title, description, op, symbol, operand, weight)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                        (context, label, title, description, op, SYM[op], operand, weight))
    finally:
        conn.close()
    return jsonify({"ok": True})


@app.route("/api/admin/effects/toggle", methods=["POST"])
@admin_required
def admin_effects_toggle():
    eid = (request.json or {}).get("id")
    conn = get_db()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("UPDATE battle_effects SET active = NOT active WHERE id = %s", (eid,))
    finally:
        conn.close()
    return jsonify({"ok": True})


@app.route("/api/admin/effects/remove", methods=["POST"])
@admin_required
def admin_effects_remove():
    eid = (request.json or {}).get("id")
    conn = get_db()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM battle_effects WHERE id = %s", (eid,))
    finally:
        conn.close()
    return jsonify({"ok": True})


# ---------- 상황·환경 이벤트 관리 ----------
@app.route("/api/admin/events")
@admin_required
def admin_events():
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""SELECT id, title, description, kind, param, weight, active
                           FROM battle_events ORDER BY id""")
            rows = cur.fetchall()
    finally:
        conn.close()
    return jsonify({"events": rows})


@app.route("/api/admin/events/add", methods=["POST"])
@admin_required
def admin_events_add():
    d = request.json or {}
    kind = (d.get("kind") or "").strip()
    if kind not in ("none", "ban_op", "start_bonus"):
        return jsonify({"error": "종류를 선택하세요."}), 400
    title = (d.get("title") or "").strip()[:40]
    description = (d.get("description") or "").strip()[:120]
    param = d.get("param")
    if kind == "ban_op":
        if param not in SYM:
            return jsonify({"error": "금지할 연산을 선택하세요."}), 400
    elif kind == "start_bonus":
        try:
            param = str(int(param))
        except (ValueError, TypeError):
            return jsonify({"error": "보너스 점수를 숫자로 입력하세요."}), 400
    else:
        param = None
    try:
        weight = max(1, min(100, int(d.get("weight", 1))))
    except (ValueError, TypeError):
        weight = 1
    conn = get_db()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""INSERT INTO battle_events (title, description, kind, param, weight)
                           VALUES (%s, %s, %s, %s, %s)""", (title, description, kind, param, weight))
    finally:
        conn.close()
    return jsonify({"ok": True})


@app.route("/api/admin/events/toggle", methods=["POST"])
@admin_required
def admin_events_toggle():
    eid = (request.json or {}).get("id")
    conn = get_db()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("UPDATE battle_events SET active = NOT active WHERE id = %s", (eid,))
    finally:
        conn.close()
    return jsonify({"ok": True})


@app.route("/api/admin/events/remove", methods=["POST"])
@admin_required
def admin_events_remove():
    eid = (request.json or {}).get("id")
    conn = get_db()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM battle_events WHERE id = %s", (eid,))
    finally:
        conn.close()
    return jsonify({"ok": True})


@app.route("/api/admin/remove", methods=["POST"])
@admin_required
def admin_remove():
    pid = (request.json or {}).get("id")
    conn = get_db()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM players WHERE id = %s", (pid,))
    finally:
        conn.close()
    return jsonify({"ok": True})


def do_refresh():
    try:
        conn = get_db()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT id, game_name, tag_line, puuid FROM players")
                players = cur.fetchall()
        finally:
            conn.close()
        with REFRESH_LOCK:
            REFRESH["total"] = len(players)

        def work(p):
            try:
                c = get_db()
            except Exception as e:
                with REFRESH_LOCK:
                    REFRESH["failed"] += 1
                    if len(REFRESH["errors"]) < 3:
                        REFRESH["errors"].append(f"DB연결: {e}")
                return
            try:
                store_player(c, p["id"], p["game_name"], p["tag_line"], p["puuid"])
                with REFRESH_LOCK:
                    REFRESH["updated"] += 1
            except Exception as e:
                print("refresh error:", repr(e))
                with REFRESH_LOCK:
                    REFRESH["failed"] += 1
                    if len(REFRESH["errors"]) < 3:
                        REFRESH["errors"].append(f"{type(e).__name__}: {e}")
            finally:
                try:
                    c.close()
                except Exception:
                    pass

        if players:
            with ThreadPoolExecutor(max_workers=WORKERS) as ex:
                list(ex.map(work, players))
        conn2 = get_db()
        try:
            with conn2, conn2.cursor() as cur:
                cur.execute("""INSERT INTO app_meta (k, v) VALUES ('last_refresh', now())
                               ON CONFLICT (k) DO UPDATE SET v = now()""")
        finally:
            conn2.close()
    finally:
        with REFRESH_LOCK:
            REFRESH["running"] = False
            REFRESH["finished_at"] = time.time()


@app.route("/api/admin/refresh", methods=["POST"])
@admin_required
def admin_refresh():
    if not RIOT_API_KEY:
        return jsonify({"error": "서버에 RIOT_API_KEY가 없습니다."}), 500
    with REFRESH_LOCK:
        if REFRESH["running"]:
            return jsonify({"ok": True, "already": True})
        REFRESH.update(running=True, updated=0, failed=0, total=0, errors=[], finished_at=None)
    threading.Thread(target=do_refresh, daemon=True).start()
    return jsonify({"ok": True, "started": True})


@app.route("/api/admin/refresh-status")
@admin_required
def admin_refresh_status():
    with REFRESH_LOCK:
        return jsonify(dict(REFRESH))


# ---------- 페이지 ----------
@app.route("/")
def index():
    return render_template_string(PAGE)


@app.route("/admin")
def admin_page():
    return render_template_string(ADMIN_PAGE, logged_in=bool(session.get("admin")))


THEME = r"""
  :root{
    --ink:#0A1220; --surface:#111C2E; --surface2:#16233A; --line:#23344F;
    --gold:#C8AA6E; --gold-bright:#E4D5A8; --blue:#4B9CD3; --red:#C0475A;
    --text:#E6EAF0; --muted:#8FA1BB;
  }
  *{box-sizing:border-box}
  body{margin:0;background:radial-gradient(1200px 500px at 50% -200px,#16283f 0%,transparent 70%),var(--ink);
       color:var(--text);font-family:'Inter',system-ui,sans-serif;min-height:100vh}
  .wrap{max-width:820px;margin:0 auto;padding:40px 18px 80px}
  h1{font-family:'Marcellus',serif;font-weight:400;font-size:34px;margin:0 0 6px}
  .eyebrow{font-size:12px;letter-spacing:.28em;text-transform:uppercase;color:var(--gold);margin-bottom:10px}
  .sub{color:var(--muted);font-size:14px;margin:0 0 26px}
  a{color:var(--gold)}
  input,select{background:var(--ink);border:1px solid var(--line);border-radius:9px;color:var(--text);
       padding:11px 13px;font:inherit;outline:none}
  input:focus,select:focus{border-color:var(--gold)}
  button{background:var(--gold);color:#1a1204;border:none;border-radius:9px;padding:11px 18px;
       font:600 14px 'Inter',sans-serif;cursor:pointer}
  button:hover{background:var(--gold-bright)}
  button:disabled{opacity:.55;cursor:default}
"""

PAGE = r"""
<!doctype html><html lang="ko"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>아럽롤 회원 대시보드</title>
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Marcellus&family=Inter:wght@400;500;600&family=JetBrains+Mono:wght@600&display=swap" rel="stylesheet">
<style>__THEME__
  .tabs{display:flex;gap:8px;margin-bottom:14px}
  .tab{padding:9px 16px;border:1px solid var(--line);border-radius:9px;background:var(--surface);color:var(--muted);cursor:pointer}
  .tab-link{text-decoration:none;color:var(--gold);border-color:var(--gold);display:inline-flex;align-items:center}
  .tab-link:hover{background:var(--surface2)}
  .tab.on{background:var(--gold);color:#1a1204;border-color:var(--gold);font-weight:600}
  .updated{color:var(--muted);font-size:12px;margin:0 2px 16px}
  .row{display:flex;align-items:center;gap:14px;padding:11px 14px;border:1px solid var(--line);border-radius:11px;background:var(--surface);margin-bottom:7px}
  .no{font-family:'JetBrains Mono',monospace;color:var(--gold-bright);width:30px;text-align:center;font-size:15px}
  .icon{width:42px;height:42px;border-radius:10px;border:1px solid var(--line)}
  .who{flex:1;min-width:0}
  .who .nm{font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  .who .sm{color:var(--muted);font-size:12px}
  .lvl{font-family:'JetBrains Mono',monospace;color:var(--gold-bright);text-align:right}
  .lvl small{color:var(--muted);font-weight:400;font-size:11px}
  .pts{font-family:'JetBrains Mono',monospace;color:var(--gold-bright)}
  .champ-nm{font-weight:600}
  .top-who{color:var(--text)}
  .norec{color:var(--muted);font-size:13px}
  #search{width:100%;margin-bottom:12px}
  .muted{color:var(--muted)}
  #status{color:var(--muted);text-align:center;padding:24px}
  .mini{display:flex;gap:4px;margin-top:5px}
  .mini img{width:22px;height:22px;border-radius:5px;border:1px solid var(--line)}
  .pop{display:inline-block;background:rgba(200,170,110,.16);color:var(--gold-bright);border:1px solid var(--gold);
       border-radius:99px;padding:1px 8px;font-size:11px;margin-left:6px;vertical-align:middle;white-space:nowrap}
  .clickable{cursor:pointer}
  .clickable:hover{border-color:var(--gold)}
  .back{background:transparent;border:1px solid var(--line);color:var(--muted);padding:8px 14px}
  .back:hover{background:var(--surface2);color:var(--text)}
  .bar-champ{display:flex;align-items:center;gap:10px;margin:12px 2px 16px}
  .bar-champ img{width:46px;height:46px;border-radius:9px;border:1px solid var(--line)}
  .stats{display:flex;gap:10px;margin:0 0 16px}
  .stat{flex:1;background:var(--surface);border:1px solid var(--line);border-radius:10px;padding:12px;text-align:center}
  .s-val{font-family:'JetBrains Mono',monospace;color:var(--gold-bright);font-size:18px}
  .s-lab{color:var(--muted);font-size:12px;margin-top:2px}
  .opgg{align-self:center;background:transparent;border:1px solid var(--blue);color:var(--blue);
        border-radius:9px;padding:8px 14px;font-size:13px;font-weight:600;text-decoration:none;white-space:nowrap}
  .opgg:hover{background:var(--blue);color:#fff}
  .reqbox{display:flex;gap:8px;margin:18px 0 6px}
  .reqbox input{flex:1}
  .reqmsg{color:var(--muted);font-size:12.5px;margin:0 2px 18px;min-height:16px}
  .mbar{display:flex;gap:8px;margin-bottom:12px}
  .mbar select{flex:0 0 auto}
  .mbar input{flex:1}
  .rk-card{background:var(--surface);border:1px solid var(--line);border-radius:12px;padding:14px 16px;margin-bottom:12px}
  .rk-title{font-family:'Marcellus',serif;font-size:17px;margin-bottom:8px;display:flex;align-items:center;gap:8px}
  .rk-title img{width:24px;height:24px;border-radius:6px;border:1px solid var(--line)}
  .rk-row{display:flex;align-items:center;gap:10px;padding:6px 0;border-top:1px solid var(--line)}
  .rk-row:first-of-type{border-top:none}
  .rk-no{font-family:'JetBrains Mono',monospace;color:var(--muted);width:18px;text-align:center}
  .rk-name{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  .rk-champ{width:22px;height:22px;border-radius:5px;border:1px solid var(--line)}
  .rk-val{font-family:'JetBrains Mono',monospace;color:var(--gold-bright)}
  .rk-val small{color:var(--muted);font-weight:400;font-size:11px;margin-left:2px}
  .rk-row.first .rk-name{font-weight:600;color:var(--gold-bright)}
  .rk-row.first .rk-no{color:var(--gold)}
  .rk-card.expandable{cursor:pointer}
  .rk-card .rk-row.extra{display:none}
  .rk-card.open .rk-row.extra{display:flex}
  .rk-caret{margin-left:auto;color:var(--muted);font-size:12px;transition:transform .15s}
  .rk-card.open .rk-caret{transform:rotate(180deg)}
  .cmp-pick{display:flex;gap:8px;align-items:center}
  .cmp-pick select{flex:1;min-width:0}
  .cmp-pick .vsm{font-family:'Marcellus',serif;color:var(--gold)}
  .faceoff{display:flex;align-items:center;justify-content:space-between;margin:20px 0 4px}
  .faceoff .fighter{flex:1;text-align:center;min-width:0}
  .faceoff img{width:64px;height:64px;border-radius:14px;border:1px solid var(--line)}
  .faceoff .fn{margin-top:6px;font-weight:600;font-size:14px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  .faceoff .vs-big{font-family:'Marcellus',serif;font-size:26px;color:var(--gold);padding:0 8px}
  .round-title{text-align:center;font-family:'Marcellus',serif;font-size:16px;color:var(--gold);margin:20px 0 10px}
  .scoreboard{display:flex;align-items:center;justify-content:center;gap:14px;margin:6px 0 4px;
    background:var(--surface);border:1px solid var(--line);border-radius:12px;padding:12px}
  .env{text-align:center;font-size:13px;color:var(--text);background:rgba(75,156,211,.12);
    border:1px solid var(--blue);border-radius:10px;padding:9px 12px;margin:14px 0 4px}
  .env b{color:var(--blue)}
  .scoreboard .sb{display:flex;align-items:baseline;gap:8px}
  .scoreboard .sb.b{flex-direction:row}
  .scoreboard .sb-nm{color:var(--muted);font-size:12px;max-width:90px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  .scoreboard .sb-val{font-family:'JetBrains Mono',monospace;font-size:22px;color:var(--gold-bright)}
  .scoreboard .sb-col{color:var(--muted)}
  .sb-val.bump{animation:bump .3s}
  @keyframes bump{0%{transform:scale(1)}40%{transform:scale(1.25);color:#fff}100%{transform:scale(1)}}
  .scoreboard .sb.lead .sb-val{color:#fff;text-shadow:0 0 14px rgba(200,170,110,.75)}
  .bt-math .op{display:inline-block;min-width:14px;text-align:center;color:var(--gold);font-weight:700}
  .bt-math .op.locked{animation:oplock .28s}
  .eff{color:var(--gold-bright);font-size:11px;background:rgba(200,170,110,.14);border-radius:5px;padding:0 5px}
  .bt-effs{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:6px}
  .bt-effs.one{grid-template-columns:1fr;text-align:center}
  .bt-effs .el.a{text-align:left}
  .bt-effs .el.b{text-align:right}
  .eff-line{display:inline-block;font-size:11px;color:var(--gold-bright);background:rgba(200,170,110,.12);border-radius:6px;padding:2px 8px;line-height:1.55}
  .eff-line b{color:var(--gold-bright)}
  @keyframes oplock{0%{transform:scale(1.7);color:#fff}100%{transform:scale(1);color:var(--gold)}}
  .bt{border:1px solid var(--line);border-radius:10px;padding:10px 12px;margin-bottom:7px;background:var(--surface)}
  .bt-lab{display:flex;align-items:center;gap:6px;color:var(--muted);font-size:12.5px;margin-bottom:6px}
  .bt-lab img{width:22px;height:22px;border-radius:5px}
  .op-chip{margin-left:auto;font-family:'JetBrains Mono',monospace;color:var(--gold);border:1px solid var(--gold);border-radius:6px;padding:0 7px;font-size:13px}
  .bt-math{display:flex;justify-content:space-between;gap:10px;font-family:'JetBrains Mono',monospace;font-size:13px;color:var(--muted)}
  .bt-math .m{position:relative}
  .bt-math .m.b{text-align:right}
  .bt-math .m b{color:var(--text)}
  .bt-math .m.lead b{color:var(--gold-bright);text-shadow:0 0 10px rgba(200,170,110,.5)}
  .duel{display:grid;grid-template-columns:1fr auto 1fr;align-items:center;gap:10px;padding:10px 12px;border:1px solid var(--line);border-radius:10px;margin-bottom:7px;background:var(--surface)}
  .duel .side{min-width:0}
  .duel .side.a{text-align:right}
  .duel .side.b{text-align:left}
  .duel .val{font-family:'JetBrains Mono',monospace;font-size:15px}
  .duel .side.win .val{color:var(--gold-bright);text-shadow:0 0 12px rgba(200,170,110,.6);font-weight:700}
  .duel .lab{color:var(--muted);font-size:12px;text-align:center;white-space:nowrap}
  .duel .champ-lab{display:flex;align-items:center;gap:6px;justify-content:center;color:var(--text)}
  .duel .champ-lab img{width:24px;height:24px;border-radius:5px}
  .round-sum{text-align:center;color:var(--muted);font-size:13px;margin:6px 0 6px;
    background:var(--surface);border:1px solid var(--line);border-radius:9px;padding:8px}
  .round-sum b{color:var(--gold-bright)}
  .round-sum.show{animation:cutin .5s both}
  @keyframes cutin{0%{opacity:0;transform:translateX(-26px)}60%{transform:translateX(5px)}100%{opacity:1;transform:none}}
  .verdict{text-align:center;margin-top:20px;padding:20px;border-radius:14px;border:1px solid var(--gold);background:rgba(200,170,110,.10)}
  .verdict .who{font-family:'Marcellus',serif;font-size:26px;color:var(--gold-bright);display:block;margin:4px 0}
  .verdict .score{color:var(--muted);font-size:13px;margin-top:4px}
  @keyframes slideL{from{opacity:0;transform:translateX(-24px)}to{opacity:1;transform:none}}
  @keyframes slideR{from{opacity:0;transform:translateX(24px)}to{opacity:1;transform:none}}
  @keyframes pop{0%{opacity:0;transform:scale(.85)}60%{transform:scale(1.05)}100%{opacity:1;transform:scale(1)}}
  .faceoff .left{animation:slideL .5s both}
  .faceoff .right{animation:slideR .5s both}
  .reveal{opacity:0;transform:translateY(8px)}
  .reveal.show{opacity:1;transform:none;transition:opacity .3s, transform .3s}
  .verdict.show{animation:pop .55s both, glowpulse 1.8s .5s ease-in-out infinite}
  .help{display:inline-block;width:18px;height:18px;line-height:16px;text-align:center;border:1px solid var(--gold);
    color:var(--gold);border-radius:50%;font-size:12px;cursor:pointer;vertical-align:middle;margin-left:6px}
  .help:hover{background:var(--gold);color:#1a1204}
  .modal{position:fixed;inset:0;background:rgba(0,0,0,.62);display:flex;align-items:center;justify-content:center;z-index:60;padding:20px}
  .modal-box{background:var(--surface);border:1px solid var(--gold);border-radius:14px;max-width:420px;width:100%;padding:22px;max-height:82vh;overflow:auto}
  .modal-title{font-family:'Marcellus',serif;font-size:18px;color:var(--gold-bright);margin-bottom:12px}
  .modal-body{color:var(--text);font-size:13.5px;line-height:1.7}
  .modal-body b{color:var(--gold-bright)}
  .modal-body ul{margin:6px 0 6px;padding-left:18px}
  .modal-body li{margin:3px 0}
  .stamp{display:inline-block;margin:0 6px;font-family:'Marcellus',serif;font-size:12px;letter-spacing:.05em;
    color:var(--gold);border:2px solid var(--gold);border-radius:6px;padding:0 5px;transform:rotate(-12deg);opacity:0}
  .reveal.show .stamp{animation:stampin .4s .12s both}
  @keyframes stampin{0%{opacity:0;transform:rotate(-12deg) scale(2.4)}70%{opacity:1;transform:rotate(-12deg) scale(.9)}100%{opacity:1;transform:rotate(-12deg) scale(1)}}
  @keyframes glowpulse{0%,100%{box-shadow:0 0 0 rgba(200,170,110,0)}50%{box-shadow:0 0 28px rgba(200,170,110,.55)}}
  .verdict{position:relative;overflow:hidden}
  .verdict::after{content:'';position:absolute;top:0;left:-60%;width:45%;height:100%;
    background:linear-gradient(100deg,transparent,rgba(228,213,168,.38),transparent);transform:skewX(-20deg)}
  .verdict.show::after{animation:shine 1.8s .6s ease-in-out infinite}
  @keyframes shine{0%{left:-60%}55%,100%{left:130%}}
  .spark{position:absolute;font-size:14px;opacity:0;pointer-events:none}
  .verdict.show .spark{animation:tw 1.5s ease-in-out infinite}
  @keyframes tw{0%,100%{opacity:0;transform:scale(.5)}50%{opacity:1;transform:scale(1.15)}}
</style></head><body>
<div class="wrap">
  <div class="eyebrow">League of Legends</div>
  <h1>아럽롤 회원 대시보드</h1>

  <div class="reqbox">
    <input id="reqId" placeholder="회원 등록 요청 — 소환사명#KR1" autocomplete="off">
    <button id="reqBtn">요청</button>
    <button onclick="location.href='/admin'">관리자</button>
  </div>
  <div id="reqMsg" class="reqmsg"></div>

  <div class="tabs">
    <div class="tab on" data-tab="members">회원 리스트</div>
    <div class="tab" data-tab="mastery">챔피언 숙련도</div>
    <div class="tab" data-tab="ranking">랭킹</div>
    <div class="tab" data-tab="compare">결투</div>
    <a class="tab tab-link" href="https://league-of-legend-saboteur.onrender.com/" target="_blank" rel="noopener">게임 ↗</a>
  </div>
  <div class="updated" id="updated"></div>

  <div id="view-members">
    <div id="member-browse">
      <div class="mbar">
        <select id="msort">
          <option value="level">레벨순</option>
          <option value="name">이름순</option>
          <option value="total">총 숙련도순</option>
          <option value="views">조회수순</option>
        </select>
        <input id="msearch" placeholder="회원 이름 검색…" autocomplete="off">
      </div>
      <div id="status">불러오는 중…</div><div id="member-list"></div>
    </div>
    <div id="member-detail" style="display:none"></div>
  </div>

  <div id="view-mastery" style="display:none">
    <div id="champ-browse">
      <input id="search" placeholder="챔피언 이름 검색…" autocomplete="off">
      <div id="champ-list"><div class="muted" style="text-align:center;padding:16px">불러오는 중…</div></div>
    </div>
    <div id="champ-detail" style="display:none"></div>
  </div>

  <div id="view-ranking" style="display:none">
    <div id="ranking-list"><div class="muted" style="text-align:center;padding:16px">불러오는 중…</div></div>
  </div>

  <div id="view-compare" style="display:none">
    <div class="cmp-pick">
      <select id="cmpA"></select>
      <span class="vsm">VS</span>
      <select id="cmpB"></select>
    </div>
    <button id="cmpBtn" style="width:100%;margin-top:8px">⚔️ 대결 시작</button>
    <div id="cmp-result"></div>
    <div id="rulesModal" class="modal" style="display:none">
      <div class="modal-box">
        <div class="modal-title" id="rulesTitle"></div>
        <div class="modal-body" id="rulesBody"></div>
        <button class="modal-close" style="width:100%;margin-top:16px" onclick="closeRules()">닫기</button>
      </div>
    </div>
  </div>
</div>

<script>
const $=s=>document.querySelector(s);
let VERSION=null, CHAMPS=[], MEMBERS=[];
const dd=sub=>`https://ddragon.leagueoflegends.com/cdn/${VERSION}/img/${sub}`;
const esc=s=>(s==null?'':(''+s)).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));

async function loadMembers(){
  try{
    const r=await fetch('/api/members'); const d=await r.json();
    VERSION=d.version; $('#status').style.display='none';
    MEMBERS=d.members||[];
    $('#updated').textContent = d.refreshedAt
      ? ('마지막 전체 갱신: '+d.refreshedAt.slice(0,16).replace('T',' '))
      : '아직 전체 갱신 안 됨 — 관리자 페이지에서 정보 갱신을 눌러주세요.';
    renderMembers();
  }catch(e){ $('#status').textContent='불러오기 실패'; }
}

function renderMembers(){
  const sort=$('#msort').value, q=$('#msearch').value.trim();
  let list=MEMBERS.slice();
  if(q) list=list.filter(m=>m.name.includes(q));
  const cmp={
    level:(a,b)=>(b.level??-1)-(a.level??-1),
    total:(a,b)=>(b.total||0)-(a.total||0),
    views:(a,b)=>(b.views||0)-(a.views||0),
    name:(a,b)=>a.name.localeCompare(b.name,'ko'),
  }[sort]||((a,b)=>0);
  list.sort(cmp);
  const rightVal=(m)=>{
    if(sort==='total') return `${(m.total||0).toLocaleString()} <small>숙련도</small>`;
    if(sort==='views') return `${(m.views||0).toLocaleString()} <small>조회</small>`;
    return `${m.level??'-'} <small>레벨</small>`;   // 레벨순·이름순
  };
  if(!list.length){ $('#member-list').innerHTML='<div class="muted" style="text-align:center;padding:24px">'+(q?'검색 결과가 없습니다.':'등록된 회원이 없습니다.')+'</div>'; return; }
  $('#member-list').innerHTML=list.map((m,i)=>{
    const icon=(VERSION&&m.iconId!=null)?`<img class="icon" src="${dd('profileicon/'+m.iconId+'.png')}">`:`<div class="icon"></div>`;
    const mini=(VERSION&&m.topMastery&&m.topMastery.length)
      ? `<div class="mini">${m.topMastery.map(c=>c.img?`<img title="${esc(c.name)}" src="${dd('champion/'+c.img+'.png')}">`:'').join('')}</div>` : '';
    const pop=m.popular?` <span class="pop" title="조회 ${m.views}회">🔥 인기쟁이</span>`:'';
    return `<div class="row clickable" onclick="showMemberDetail(${m.id})"><div class="no">${i+1}</div>${icon}
      <div class="who"><div class="nm">${esc(m.name)} <span class="sm">#${esc(m.tag)}</span>${pop}</div>${mini}</div>
      <div class="lvl">${rightVal(m)}</div></div>`;
  }).join('');
}

async function showMemberDetail(pid){
  $('#member-browse').style.display='none';
  $('#member-detail').style.display='';
  $('#member-detail').innerHTML='<div class="muted" style="text-align:center;padding:16px">불러오는 중…</div>';
  try{
    const r=await fetch('/api/member/'+pid); const d=await r.json();
    if(!r.ok){ $('#member-detail').innerHTML=`<button class="back" onclick="backToMembers()">← 목록으로</button><div class="norec" style="text-align:center;padding:12px">${esc(d.error||'오류')}</div>`; return; }
    VERSION=d.version||VERSION;
    const icon=(VERSION&&d.iconId!=null)?`<img src="${dd('profileicon/'+d.iconId+'.png')}">`:'';
    let h=`<button class="back" onclick="backToMembers()">← 목록으로</button>`;
    h+=`<div class="bar-champ">${icon}<div style="flex:1"><b>${esc(d.name)}</b> <span class="muted">#${esc(d.tag)}</span><div class="muted" style="font-size:13px">Lv.${d.level??'-'} · 조회 ${d.views??0}회${d.popRank?` · 인기 ${d.popRank}위`:''}</div></div>`;
    if(d.opgg){ h+=`<a class="opgg" href="${d.opgg}" target="_blank" rel="noopener">OP.GG ↗</a>`; }
    h+=`</div>`;
    h+=`<div class="stats">
        <div class="stat"><div class="s-val">${d.total.toLocaleString()}</div><div class="s-lab">총 숙련도</div></div>
        <div class="stat"><div class="s-val">${d.champCount}</div><div class="s-lab">플레이한 챔피언</div></div>
        <div class="stat"><div class="s-val">${d.level??'-'}</div><div class="s-lab">레벨</div></div>
      </div>`;
    if(!d.champions.length){ h+='<div class="muted" style="text-align:center;padding:12px">숙련도 기록이 없습니다. 관리자 갱신이 필요할 수 있어요.</div>'; }
    else{ h+=d.champions.map((c,i)=>{
      const ci=(VERSION&&c.img)?`<img class="icon" src="${dd('champion/'+c.img+'.png')}">`:`<div class="icon"></div>`;
      return `<div class="row"><div class="no">${i+1}</div>${ci}
        <div class="who"><div class="nm">${esc(c.name)}</div><div class="sm">숙련도 ${c.level}레벨</div></div>
        <div class="lvl"><span class="pts">${c.points.toLocaleString()}</span> <small>점</small></div></div>`;
    }).join(''); }
    $('#member-detail').innerHTML=h;
  }catch(e){ $('#member-detail').innerHTML='<button class="back" onclick="backToMembers()">← 목록으로</button><div class="muted" style="text-align:center;padding:16px">불러오기 실패</div>'; }
}
function backToMembers(){ $('#member-detail').style.display='none'; $('#member-browse').style.display=''; }

async function submitRequest(){
  const v=$('#reqId').value.trim(); if(!v) return;
  $('#reqBtn').disabled=true; $('#reqMsg').textContent='요청 중…';
  try{
    const r=await fetch('/api/request-join',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({riotId:v})});
    const d=await r.json();
    if(!r.ok){ $('#reqMsg').textContent=d.error||'오류가 발생했습니다.'; }
    else if(d.already==='member'){ $('#reqMsg').textContent='이미 등록된 회원입니다.'; }
    else if(d.already==='request'){ $('#reqMsg').textContent='이미 등록 요청된 아이디입니다.'; }
    else{ $('#reqMsg').textContent='등록 요청이 접수되었습니다. 관리자 승인 후 추가됩니다.'; $('#reqId').value=''; }
  }catch(e){ $('#reqMsg').textContent='요청에 실패했습니다.'; }
  finally{ $('#reqBtn').disabled=false; }
}
$('#msort').addEventListener('change', renderMembers);
$('#msearch').addEventListener('input', renderMembers);
$('#reqBtn').addEventListener('click', submitRequest);
$('#reqId').addEventListener('keydown', e=>{ if(e.key==='Enter') submitRequest(); });

function renderChamps(filter){
  const f=(filter||'').trim();
  const list=f?CHAMPS.filter(c=>c.name.includes(f)):CHAMPS;
  if(!list.length){ $('#champ-list').innerHTML='<div class="muted" style="text-align:center;padding:16px">해당 챔피언이 없습니다.</div>'; return; }
  $('#champ-list').innerHTML=list.map(c=>{
    const icon=VERSION?`<img class="icon" src="${dd('champion/'+c.img+'.png')}">`:`<div class="icon"></div>`;
    const top=c.top
      ? `<div class="who"><div class="top-who">${esc(c.top.name)} <span class="muted">#${esc(c.top.tag)}</span></div></div><div class="lvl"><span class="pts">${c.top.points.toLocaleString()}</span> <small>점</small></div>`
      : `<div class="who"><span class="norec">기록 없음</span></div>`;
    return `<div class="row clickable" onclick="showChampDetail(${c.championId})">${icon}<div style="min-width:90px"><span class="champ-nm">${esc(c.name)}</span></div>${top}</div>`;
  }).join('');
}

async function showChampDetail(cid){
  const c=CHAMPS.find(x=>x.championId===cid);
  $('#champ-browse').style.display='none';
  $('#champ-detail').style.display='';
  $('#champ-detail').innerHTML='<div class="muted" style="text-align:center;padding:16px">불러오는 중…</div>';
  try{
    const r=await fetch('/api/champion-detail?championId='+cid);
    const d=await r.json();
    const img=(c&&c.img)||d.img;
    let h=`<button class="back" onclick="backToList()">← 목록으로</button>`;
    h+=`<div class="bar-champ">${VERSION?`<img src="${dd('champion/'+img+'.png')}">`:''}<div><b>${esc(d.champion||(c&&c.name))}</b> 숙련도 순위</div></div>`;
    if(!d.players.length){ h+='<div class="muted" style="text-align:center;padding:12px">이 챔피언 숙련도 기록이 있는 회원이 없습니다.</div>'; }
    else{ h+=d.players.map((p,i)=>`<div class="row"><div class="no">${i+1}</div>
      <div class="who"><div class="nm">${esc(p.name)} <span class="muted">#${esc(p.tag)}</span></div>
        <div class="sm">숙련도 ${p.level}레벨</div></div>
      <div class="lvl"><span class="pts">${p.points.toLocaleString()}</span> <small>점</small></div></div>`).join(''); }
    $('#champ-detail').innerHTML=h;
  }catch(e){ $('#champ-detail').innerHTML='<button class="back" onclick="backToList()">← 목록으로</button><div class="muted" style="text-align:center;padding:16px">불러오기 실패</div>'; }
}
function backToList(){ $('#champ-detail').style.display='none'; $('#champ-browse').style.display=''; }

async function loadChamps(){
  try{
    const r=await fetch('/api/champion-top'); const d=await r.json();
    VERSION=d.version||VERSION; CHAMPS=d.champions||[];
    renderChamps('');
  }catch(e){ $('#champ-list').innerHTML='<div class="muted" style="text-align:center">불러오기 실패</div>'; }
}

let RANK_LOADED=false;
async function loadRanking(){
  try{
    const r=await fetch('/api/ranking'); const d=await r.json();
    VERSION=d.version||VERSION;
    $('#ranking-list').innerHTML=d.rankings.map(cat=>{
      const head=(cat.img&&VERSION)?`<img src="${dd('champion/'+cat.img+'.png')}">`:'';
      const players=cat.players||[];
      const body=players.length
        ? players.map((p,i)=>{
            const champ=(p.champImg&&VERSION)?`<img class="rk-champ" title="${esc(p.champName)}" src="${dd('champion/'+p.champImg+'.png')}">`:'';
            const cls='rk-row'+(i===0?' first':'')+(i>=3?' extra':'');
            return `<div class="${cls}"><span class="rk-no">${i+1}</span>
              <span class="rk-name">${esc(p.name)} <span class="muted">#${esc(p.tag)}</span></span>
              ${champ}<span class="rk-val">${p.value.toLocaleString()}<small>${esc(cat.unit)}</small></span></div>`;
          }).join('')
        : '<div class="muted" style="padding:6px 2px">기록 없음</div>';
      const expandable=players.length>3;
      const caret=expandable?'<span class="rk-caret">▾</span>':'';
      const cls=expandable?'rk-card expandable':'rk-card';
      const onclick=expandable?" onclick=\"this.classList.toggle('open')\"":'';
      return `<div class="${cls}"${onclick}><div class="rk-title">${head}${esc(cat.title)}${caret}</div>${body}</div>`;
    }).join('');
  }catch(e){ $('#ranking-list').innerHTML='<div class="muted" style="text-align:center">불러오기 실패</div>'; }
}

$('#search').addEventListener('input', e=>renderChamps(e.target.value));

function populateCompare(){
  if(!MEMBERS.length) return;
  const sorted=MEMBERS.slice().sort((a,b)=>a.name.localeCompare(b.name,'ko'));
  const opts='<option value="">회원 선택…</option>'+sorted.map(m=>`<option value="${m.id}">${esc(m.name)} #${esc(m.tag)}</option>`).join('');
  if($('#cmpA').options.length<=1){ $('#cmpA').innerHTML=opts; $('#cmpB').innerHTML=opts; }
}
const nf=n=>(n==null?0:n).toLocaleString();
const UNARY=['rev','swap'];

async function runBattle(){
  const a=$('#cmpA').value, b=$('#cmpB').value;
  const box=$('#cmp-result');
  if(!a||!b){ box.innerHTML='<div class="norec" style="text-align:center;padding:12px">두 회원을 선택하세요.</div>'; return; }
  if(a===b){ box.innerHTML='<div class="norec" style="text-align:center;padding:12px">서로 다른 회원을 선택하세요.</div>'; return; }
  box.innerHTML='<div class="muted" style="text-align:center;padding:16px">대결 준비 중…</div>';
  try{
    const r=await fetch(`/api/compare?a=${a}&b=${b}`); const d=await r.json();
    if(!r.ok){ box.innerHTML=`<div class="norec" style="text-align:center;padding:12px">${esc(d.error||'오류')}</div>`; return; }
    VERSION=d.version||VERSION; renderBattle(d);
  }catch(e){ box.innerHTML='<div class="norec" style="text-align:center;padding:12px">불러오기 실패</div>'; }
}

function fico(P){ return (VERSION&&P.iconId!=null)?`<img src="${dd('profileicon/'+P.iconId+'.png')}">`:'<div class="icon"></div>'; }
function roundText(w,A,B,aw,bw){
  if(w==='a') return `<b>${esc(A.name)}</b> 승리 (${aw} : ${bw})`;
  if(w==='b') return `<b>${esc(B.name)}</b> 승리 (${bw} : ${aw})`;
  return `무승부 (${aw} : ${bw})`;
}
function spinOp(el, finalSym, dur){
  const syms=['+','−','×','÷','↺','⇄']; const start=performance.now();
  (function tick(){
    if(performance.now()-start>=dur){ el.textContent=finalSym; el.classList.add('locked'); return; }
    el.textContent=syms[(Math.random()*4)|0]; setTimeout(tick,55);
  })();
}
function countTo(el, to, dur, from){
  from=(from==null)?(parseInt((el.textContent||'0').replace(/[^\d-]/g,''))||0):from;
  const start=performance.now();
  (function tick(now){
    const t=Math.min(1,(now-start)/dur);
    el.textContent=Math.round(from+(to-from)*t).toLocaleString();
    if(t<1) requestAnimationFrame(tick);
  })(start);
}
function markLead(el){
  const win = el.dataset.win!==undefined ? el.dataset.win
            : ((+el.dataset.a > +el.dataset.b) ? 'a' : ((+el.dataset.b > +el.dataset.a) ? 'b' : ''));
  if(!win) return;
  const m=el.querySelector('.bt-math .m.'+win);
  if(m){ m.classList.add('lead'); if(el.classList.contains('champ')) m.insertAdjacentHTML('beforeend','<span class="stamp">WIN</span>'); }
}

function renderBattle(d){
  const A=d.a, B=d.b, R1=d.round1, R2=d.round2, EV=d.event||{};

  let h=`<div class="faceoff">
    <div class="fighter left">${fico(A)}<div class="fn">${esc(A.name)}</div></div>
    <div class="vs-big">VS</div>
    <div class="fighter right">${fico(B)}<div class="fn">${esc(B.name)}</div></div>
  </div>`;

  h+=`<div class="env reveal">🌍 <b>${esc(EV.title||'평범한 협곡')}</b>${EV.description?' · <span class="muted">'+esc(EV.description)+'</span>':''}</div>`;

  // 실시간 점수판
  h+=`<div class="scoreboard">
    <div class="sb a"><span class="sb-nm">${esc(A.name)}</span><span class="sb-val" id="sbA">${nf(R1.start)}</span></div>
    <div class="sb-col">:</div>
    <div class="sb b"><span class="sb-val" id="sbB">${nf(R1.start)}</span><span class="sb-nm">${esc(B.name)}</span></div>
  </div>`;

  // 라운드 1
  h+=`<div class="round-title reveal">⚔️ ROUND 1 · 스탯 대결 <span class="help" onclick="showRules('round1')">?</span></div>`;
  R1.battles.forEach(bt=>{
    const un=UNARY.includes(bt.op);
    h+=`<div class="bt reveal" data-sym="${bt.sym}" data-a="${bt.aAfter}" data-b="${bt.bAfter}">
      <div class="bt-lab">${bt.random?'🎲 ':''}${esc(bt.label)}</div>
      <div class="bt-math">
        <span class="m a">${nf(bt.aBefore)} <span class="op">?</span>${un?'':' '+bt.aOperand} = <b>?</b></span>
        <span class="m b">${nf(bt.bBefore)} <span class="op">?</span>${un?'':' '+bt.bOperand} = <b>?</b></span>
      </div>
      ${bt.effTitle?`<div class="bt-effs one"><span class="eff-line">🎭 <b>${esc(bt.effTitle)}</b>${bt.effDesc?' — '+esc(bt.effDesc):''}</span></div>`:''}
    </div>`;
  });
  h+=`<div class="round-sum reveal">1라운드 최종 ${nf(R1.aFinal)} : ${nf(R1.bFinal)} — ${roundText(R1.winner,A,B,Math.max(R1.aFinal,R1.bFinal),Math.min(R1.aFinal,R1.bFinal))}</div>`;

  // 라운드 2
  h+=`<div class="round-title reveal gap">⚔️ ROUND 2 · 챔피언 대결 (Top 5) <span class="help" onclick="showRules('round2')">?</span></div>`;
  if(!R2.battles.length){ h+=`<div class="reveal muted" style="text-align:center;padding:8px">공통으로 비교할 챔피언이 없습니다.</div>`; }
  R2.battles.forEach(c=>{
    const ci=(VERSION&&c.img)?`<img src="${dd('champion/'+c.img+'.png')}">`:'';
    const ua=UNARY.includes(c.aOp), ub=UNARY.includes(c.bOp);
    h+=`<div class="bt champ reveal" data-asym="${c.aSym}" data-bsym="${c.bSym}" data-win="${c.winner}">
      <div class="bt-lab">${ci}<span>${esc(c.name)}</span></div>
      <div class="bt-math">
        <span class="m a">${nf(c.aBase)} <span class="op">?</span>${ua?'':' '+c.aOperand} = <b data-to="${c.aScore}">?</b></span>
        <span class="m b">${nf(c.bBase)} <span class="op">?</span>${ub?'':' '+c.bOperand} = <b data-to="${c.bScore}">?</b></span>
      </div>
      ${(c.aTitle||c.bTitle)?`<div class="bt-effs">
        <span class="el a">${c.aTitle?'<span class="eff-line">🎭 <b>'+esc(c.aTitle)+'</b>'+(c.aDesc?' — '+esc(c.aDesc):'')+'</span>':''}</span>
        <span class="el b">${c.bTitle?'<span class="eff-line">🎭 <b>'+esc(c.bTitle)+'</b>'+(c.bDesc?' — '+esc(c.bDesc):'')+'</span>':''}</span>
      </div>`:''}
    </div>`;
  });
  h+=`<div class="round-sum reveal">2라운드 — ${roundText(R2.winner,A,B,R2.aWins,R2.bWins)}</div>`;

  // 최종
  const ra=d.roundsWon.a, rb=d.roundsWon.b;
  let who;
  if(d.overall==='a') who=`🏆<span class="who">${esc(A.name)} 승리!</span>`;
  else if(d.overall==='b') who=`🏆<span class="who">${esc(B.name)} 승리!</span>`;
  else who=`🤝<span class="who">무승부!</span>`;
  const sparks=`<span class="spark" style="top:14%;left:8%;animation-delay:0s">✨</span>
    <span class="spark" style="top:22%;right:10%;animation-delay:.4s">✨</span>
    <span class="spark" style="bottom:16%;left:16%;animation-delay:.8s">✨</span>
    <span class="spark" style="bottom:20%;right:14%;animation-delay:.6s">⭐</span>`;
  h+=`<div class="verdict reveal gap">${sparks}${who}<div class="score">라운드 획득 ${ra} : ${rb}</div></div>`;

  $('#cmp-result').innerHTML=h;
  const sbA=$('#sbA'), sbB=$('#sbB');
  let prevA=R1.start, prevB=R1.start;
  const setLead=(na,nb)=>{ $('.scoreboard .sb.a').classList.toggle('lead',na>nb); $('.scoreboard .sb.b').classList.toggle('lead',nb>na); };

  const els=[...document.querySelectorAll('#cmp-result .reveal')];
  let delay=250;
  els.forEach(el=>{
    if(el.classList.contains('gap')) delay+=1000;
    const isBt=el.classList.contains('bt'), champ=el.classList.contains('champ');
    setTimeout(()=>{
      el.classList.add('show');
      if(isBt){
        const spin=champ?380:460;
        const ops=el.querySelectorAll('.op');
        if(champ){ spinOp(ops[0],el.dataset.asym,spin); spinOp(ops[1],el.dataset.bsym,spin); }
        else{ ops.forEach(o=>spinOp(o,el.dataset.sym,spin)); }
        setTimeout(()=>{
          const bs=el.querySelectorAll('.bt-math b');
          if(champ){ bs.forEach(b=>countTo(b,+b.dataset.to,420,0)); }
          else{
            const na=+el.dataset.a, nb=+el.dataset.b;
            bs[0].textContent=na.toLocaleString(); bs[1].textContent=nb.toLocaleString();
            countTo(sbA,na,560,prevA); countTo(sbB,nb,560,prevB); setLead(na,nb);
            prevA=na; prevB=nb;
          }
          markLead(el);
        }, spin+30);
      }
    }, delay);
    delay += isBt ? (champ?740:1060) : 340;
  });
}
const RULES={
  round1:{t:'ROUND 1 · 스탯 대결', b:`
    <ul>
      <li>두 회원 모두 <b>1000점</b>에서 시작합니다.</li>
      <li>레벨 → 총 숙련도 → 플레이한 챔피언 → 조회수 → <b>🎲 행운의 뽑기</b> 순으로 <b>5전투</b>를 치르며 점수가 누적됩니다.</li>
      <li>전투마다 연산자(효과)가 <b>무작위</b>로 뽑혀 양쪽에 적용됩니다.</li>
      <li>스탯 수치는 자릿수를 모두 더해 <b>0~9로 압축</b>해서 씁니다(밸런스). 나눗셈은 반올림, ÷0은 무효 처리.</li>
      <li>마지막 행운의 뽑기는 스탯과 무관하게 <b>0~9 랜덤 숫자</b>로 겨룹니다.</li>
      <li>5전투 후 <b>최종 점수</b>가 높은 쪽이 1라운드 승리.</li>
      <li>시작 시 뽑힌 <b>상황·환경</b>(연산 금지·시작 보너스 등)은 이 1라운드에만 적용됩니다.</li>
    </ul>`},
  round2:{t:'ROUND 2 · 챔피언 대결', b:`
    <ul>
      <li>두 회원의 <b>숙련도 Top 5</b> 챔피언을 합칩니다(겹치면 최소 5, 안 겹치면 최대 10).</li>
      <li>챔피언마다 두 회원이 <b>각자</b> 자신의 원래 숙련도에 무작위 사칙연산을 적용합니다.</li>
      <li>결과가 높은 쪽이 그 챔피언 승. <b>이긴 챔피언 수</b>가 많은 회원이 2라운드 승리.</li>
      <li>관리자가 지정한 <b>후처리 숫자</b> 효과는 여기서 ×9처럼 고정 연산으로 걸립니다.</li>
      <li>사칙연산 외에 <b>↺ 자릿수 뒤집기</b>, <b>⇄ 앞뒤 자리 교환</b> 같은 특수 연산도 나올 수 있습니다.</li>
    </ul>
    <div style="margin-top:6px"><b>최종 승자</b> — 1·2라운드 중 더 많이 이긴 쪽이 승리, 1:1이면 무승부입니다.</div>`},
};
function showRules(k){ const r=RULES[k]; $('#rulesTitle').textContent=r.t; $('#rulesBody').innerHTML=r.b; $('#rulesModal').style.display='flex'; }
function closeRules(){ $('#rulesModal').style.display='none'; }
$('#rulesModal').addEventListener('click', e=>{ if(e.target.id==='rulesModal') closeRules(); });
$('#cmpBtn').addEventListener('click', runBattle);

document.querySelectorAll('.tab').forEach(t=>t.addEventListener('click',()=>{
  if(!t.dataset.tab) return;   // 외부 링크 탭(게임)은 새 탭 이동만
  document.querySelectorAll('.tab').forEach(x=>x.classList.remove('on')); t.classList.add('on');
  backToList(); backToMembers();
  const tab=t.dataset.tab;
  $('#view-members').style.display = tab==='members'?'':'none';
  $('#view-mastery').style.display = tab==='mastery'?'':'none';
  $('#view-ranking').style.display = tab==='ranking'?'':'none';
  $('#view-compare').style.display = tab==='compare'?'':'none';
  if(tab==='ranking' && !RANK_LOADED){ RANK_LOADED=true; loadRanking(); }
  if(tab==='compare') populateCompare();
}));

loadMembers(); loadChamps();
</script></body></html>
"""
PAGE = PAGE.replace("__THEME__", THEME)

ADMIN_PAGE = r"""
<!doctype html><html lang="ko"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>관리자</title>
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Marcellus&family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
<style>__THEME__
  .card{background:var(--surface);border:1px solid var(--line);border-radius:12px;padding:20px;max-width:480px}
  .tabs{display:flex;gap:6px;max-width:480px;margin-bottom:12px}
  .tab{flex:1;text-align:center;padding:10px;border:1px solid var(--line);border-radius:9px;background:var(--surface);color:var(--muted);cursor:pointer;font-size:14px}
  .tab.on{border-color:var(--gold);color:var(--gold-bright);background:var(--surface2)}
  .prow{display:flex;align-items:center;gap:10px;padding:10px 12px;border:1px solid var(--line);border-radius:9px;background:var(--surface2);margin-bottom:7px}
  .prow .nm{flex:1}
  .prow .lv{color:var(--muted);font-size:12px}
  .del{background:transparent;border:1px solid var(--red);color:var(--red);padding:6px 12px}
  .del:hover{background:var(--red);color:#fff}
  .appr{background:transparent;border:1px solid var(--gold);color:var(--gold);padding:6px 12px;margin-right:6px}
  .appr:hover{background:var(--gold);color:#1a1204}
  .reqhdr{font-family:'Marcellus',serif;font-size:16px;margin:0 2px 8px}
  .reqhdr .badge{font-family:'Inter';font-size:12px;color:#1a1204;background:var(--gold);border-radius:99px;padding:0 7px;margin-left:6px}
  .reqtime{color:var(--muted);font-size:11px}
  .eff-form{display:flex;flex-direction:column;gap:6px}
  .eff-form input,.eff-form select{padding:9px 10px}
  .eff-row{display:flex;flex-wrap:wrap;gap:6px}
  .eff-row #effOperand{width:100px}
  .eff-row #effWeight{width:70px}
  .prow.off{opacity:.45}
  .refresh{background:transparent;border:1px solid var(--blue);color:var(--blue)}
  .refresh:hover{background:var(--blue);color:#fff}
  .err{color:#E9A2AD;font-size:13px;margin-top:8px}
  .ok{color:var(--gold-bright);font-size:13px;margin-top:8px}
  .bar{display:flex;gap:8px;margin-bottom:6px}
</style></head><body>
<div class="wrap">
  <div class="eyebrow">Admin</div><h1>회원 관리</h1>
  <p class="sub"><a href="/">← 대시보드로</a></p>
  {% if not logged_in %}
    <div class="card">
      <form method="post" action="/admin/login">
        <div style="margin-bottom:12px"><input type="password" name="password" placeholder="관리자 비밀번호" style="width:100%"></div>
        <button type="submit">로그인</button>
      </form>
      <div class="err" id="loginerr" style="display:none">비밀번호가 틀립니다.</div>
    </div>
  {% else %}
    <div class="tabs">
      <div class="tab on" data-atab="members">멤버 관리</div>
      <div class="tab" data-atab="effects">전투 이펙트</div>
      <div class="tab" data-atab="events">상황·환경</div>
    </div>

    <div id="atab-members">
      <div class="card">
        <div class="bar">
          <input id="riotId" placeholder="소환사명#KR1" style="flex:1" autocomplete="off">
          <button id="addBtn">추가</button>
        </div>
        <div class="bar"><button id="refreshBtn" class="refresh" style="width:100%">전체 정보 갱신 (레벨·숙련도)</button></div>
        <div id="msg"></div>
        <div id="reqList" style="margin-top:16px"></div>
        <div id="list" style="margin-top:16px"></div>
      </div>
    </div>

    <div id="atab-effects" style="display:none">
      <div class="card">
        <div class="reqhdr">전투 효과 관리</div>
        <div class="eff-form">
          <input id="effTitle" placeholder="제목 (예: 완벽한 바텀 듀오)" autocomplete="off">
          <input id="effDesc" placeholder="설명 (예: 눈빛만 봐도 킬각을 잡는 협곡 최강의 호흡)" autocomplete="off">
          <div class="eff-row">
            <select id="effOp">
              <option value="add">덧셈 +</option>
              <option value="sub">뺄셈 −</option>
              <option value="mul">곱셈 ×</option>
              <option value="div">나눗셈 ÷</option>
              <option value="rev">자릿수 뒤집기 ↺</option>
              <option value="swap">앞뒤 자리 교환 ⇄</option>
            </select>
            <select id="effCtx">
              <option value="both">전체 라운드</option>
              <option value="round1">1라운드</option>
              <option value="round2">2라운드</option>
              <option value="lucky">행운의 뽑기</option>
            </select>
            <input id="effOperand" type="number" min="0" max="9" placeholder="후처리 숫자" title="2라운드 전용, 비우면 랜덤">
            <input id="effWeight" type="number" value="1" min="1" max="100" title="가중치">
            <button id="effAddBtn">추가</button>
          </div>
        </div>
        <div id="effMsg"></div>
        <div id="effList" style="margin-top:12px"></div>
      </div>
    </div>

    <div id="atab-events" style="display:none">
      <div class="card">
        <div class="reqhdr">상황·환경 이벤트 <span class="muted" style="font-size:12px">(라운드 시작 시 1개 추첨)</span></div>
        <div class="eff-form">
          <input id="evTitle" placeholder="제목 (예: 곱셈 봉인)" autocomplete="off">
          <input id="evDesc" placeholder="설명 (예: 이번 판은 곱셈이 금지된다)" autocomplete="off">
          <div class="eff-row">
            <select id="evKind">
              <option value="none">없음(평범)</option>
              <option value="ban_op">연산 금지</option>
              <option value="start_bonus">시작 점수 보너스</option>
            </select>
            <select id="evBanOp" style="display:none">
              <option value="add">덧셈 금지</option>
              <option value="sub">뺄셈 금지</option>
              <option value="mul">곱셈 금지</option>
              <option value="div">나눗셈 금지</option>
            </select>
            <input id="evBonus" type="number" placeholder="보너스 점수" style="display:none;width:120px">
            <input id="evWeight" type="number" value="1" min="1" max="100" title="가중치">
            <button id="evAddBtn">추가</button>
          </div>
        </div>
        <div id="evMsg"></div>
        <div id="evList" style="margin-top:12px"></div>
      </div>
    </div>

    <div style="margin-top:16px"><a href="/admin/logout">로그아웃</a></div>
  {% endif %}
</div>
<script>
if(location.search.includes('err=1')){const e=document.getElementById('loginerr'); if(e)e.style.display='block';}
const $=s=>document.querySelector(s);
const esc=s=>(s==null?'':(''+s)).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));

async function loadList(){
  const r=await fetch('/api/admin/list'); if(!r.ok) return;
  const d=await r.json();
  $('#list').innerHTML=d.players.map(p=>{
    const lv=p.summoner_level!=null?`Lv.${p.summoner_level}`:'미갱신';
    return `<div class="prow"><span class="nm">${esc(p.game_name)} <span class="muted">#${esc(p.tag_line)}</span></span>
      <span class="lv">${lv}</span><button class="del" onclick="removePlayer(${p.id})">삭제</button></div>`;
  }).join('') || '<div class="muted">등록된 회원이 없습니다.</div>';
}
async function addPlayer(){
  const v=$('#riotId').value.trim(); if(!v) return;
  $('#msg').innerHTML='<div class="muted">추가 중…</div>';
  const r=await fetch('/api/admin/add',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({riotId:v})});
  const d=await r.json();
  if(!r.ok){ $('#msg').innerHTML=`<div class="err">${esc(d.error||'오류')}</div>`; return; }
  $('#msg').innerHTML=`<div class="ok">${d.added?'추가됨':'이미 등록됨'}: ${esc(d.name)}</div>`;
  $('#riotId').value=''; loadList();
}
async function removePlayer(id){
  await fetch('/api/admin/remove',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id})});
  loadList();
}
async function refreshAll(){
  const btn=$('#refreshBtn'); btn.disabled=true;
  $('#msg').innerHTML='<div class="muted">갱신 시작 중…</div>';
  try{
    const r=await fetch('/api/admin/refresh',{method:'POST'}); const d=await r.json();
    if(!r.ok){ $('#msg').innerHTML=`<div class="err">${esc(d.error||'오류')}</div>`; btn.disabled=false; return; }
    pollStatus();
  }catch(e){ $('#msg').innerHTML='<div class="err">갱신 시작 실패</div>'; btn.disabled=false; }
}
async function pollStatus(){
  try{
    const r=await fetch('/api/admin/refresh-status'); const s=await r.json();
    if(s.running){
      $('#msg').innerHTML=`<div class="muted">갱신 중… ${s.updated+s.failed}/${s.total||'?'}</div>`;
      setTimeout(pollStatus,2000);
    }else{
      let m=`<div class="ok">${s.updated}명 갱신 완료${s.failed?`, ${s.failed}명 실패`:''}</div>`;
      if(s.errors&&s.errors.length){ m+=`<div class="err">사유: ${esc(s.errors.join(' | '))}</div>`; }
      $('#msg').innerHTML=m; $('#refreshBtn').disabled=false; loadList();
    }
  }catch(e){ setTimeout(pollStatus,2500); }
}
async function loadRequests(){
  const r=await fetch('/api/admin/requests'); if(!r.ok) return;
  const d=await r.json();
  if(!d.requests.length){ $('#reqList').innerHTML=''; return; }
  $('#reqList').innerHTML=`<div class="reqhdr">등록 요청<span class="badge">${d.requests.length}</span></div>`+
    d.requests.map(q=>`<div class="prow"><span class="nm">${esc(q.riot_id)}<div class="reqtime">${q.requested_at?esc(q.requested_at.slice(0,16).replace('T',' ')):''}</div></span>
      <button class="appr" onclick="approveReq(${q.id})">승인</button>
      <button class="del" onclick="rejectReq(${q.id})">거절</button></div>`).join('');
}
async function approveReq(id){
  $('#msg').innerHTML='<div class="muted">승인 처리 중…</div>';
  const r=await fetch('/api/admin/approve',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id})});
  const d=await r.json();
  if(!r.ok){ $('#msg').innerHTML=`<div class="err">${esc(d.error||'오류')}</div>`; }
  else{ $('#msg').innerHTML=`<div class="ok">${d.added?'등록됨':'이미 등록됨'}: ${esc(d.name||'')}</div>`; }
  loadRequests(); loadList();
}
async function rejectReq(id){
  await fetch('/api/admin/reject',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id})});
  loadRequests();
}
const CTXKO={both:'전체',round1:'1R',round2:'2R',lucky:'🎲행운'};
async function loadEffects(){
  const r=await fetch('/api/admin/effects'); if(!r.ok) return;
  const d=await r.json();
  $('#effList').innerHTML=d.effects.map(e=>{
    const opnd=(e.operand!=null)?e.symbol+e.operand:e.symbol;
    const title=e.title?esc(e.title):'<span class="muted">(제목 없음)</span>';
    const desc=e.description?`<div class="reqtime">${esc(e.description)}</div>`:'';
    return `<div class="prow ${e.active?'':'off'}"><span class="nm"><b>${opnd}</b> ${title}${desc}
      <span class="muted" style="font-size:11px">${CTXKO[e.context]||e.context} · 가중치 ${e.weight}</span></span>
      <button class="appr" onclick="toggleEff(${e.id})">${e.active?'끄기':'켜기'}</button>
      <button class="del" onclick="removeEff(${e.id})">삭제</button></div>`;
  }).join('') || '<div class="muted">효과가 없습니다.</div>';
}
async function addEff(){
  const body={op:$('#effOp').value, context:$('#effCtx').value,
    title:$('#effTitle').value.trim(), description:$('#effDesc').value.trim(),
    operand:$('#effOperand').value, weight:$('#effWeight').value||1};
  $('#effMsg').innerHTML='<div class="muted">추가 중…</div>';
  const r=await fetch('/api/admin/effects/add',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  const d=await r.json();
  if(!r.ok){ $('#effMsg').innerHTML=`<div class="err">${esc(d.error||'오류')}</div>`; return; }
  $('#effMsg').innerHTML='<div class="ok">추가됨</div>';
  $('#effTitle').value=''; $('#effDesc').value=''; $('#effOperand').value=''; loadEffects();
}
async function toggleEff(id){ await fetch('/api/admin/effects/toggle',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id})}); loadEffects(); }
async function removeEff(id){ await fetch('/api/admin/effects/remove',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id})}); loadEffects(); }

const KINDKO={none:'평범',ban_op:'연산 금지',start_bonus:'시작 보너스'};
const OPKO={add:'덧셈',sub:'뺄셈',mul:'곱셈',div:'나눗셈'};
function evParamText(e){
  if(e.kind==='ban_op') return (OPKO[e.param]||e.param)+' 금지';
  if(e.kind==='start_bonus') return '+'+e.param+'점';
  return '';
}
async function loadEvents(){
  const r=await fetch('/api/admin/events'); if(!r.ok) return;
  const d=await r.json();
  $('#evList').innerHTML=d.events.map(e=>{
    const title=e.title?esc(e.title):'<span class="muted">(제목 없음)</span>';
    const desc=e.description?`<div class="reqtime">${esc(e.description)}</div>`:'';
    const pt=evParamText(e); const ptxt=pt?' · '+pt:'';
    return `<div class="prow ${e.active?'':'off'}"><span class="nm">${title}${desc}
      <span class="muted" style="font-size:11px">${KINDKO[e.kind]||e.kind}${ptxt} · 가중치 ${e.weight}</span></span>
      <button class="appr" onclick="toggleEvt(${e.id})">${e.active?'끄기':'켜기'}</button>
      <button class="del" onclick="removeEvt(${e.id})">삭제</button></div>`;
  }).join('') || '<div class="muted">이벤트가 없습니다.</div>';
}
function syncEvKind(){
  const k=$('#evKind').value;
  $('#evBanOp').style.display = k==='ban_op'?'':'none';
  $('#evBonus').style.display = k==='start_bonus'?'':'none';
}
async function addEvt(){
  const kind=$('#evKind').value;
  let param=null;
  if(kind==='ban_op') param=$('#evBanOp').value;
  else if(kind==='start_bonus') param=$('#evBonus').value;
  const body={kind, param, title:$('#evTitle').value.trim(), description:$('#evDesc').value.trim(), weight:$('#evWeight').value||1};
  $('#evMsg').innerHTML='<div class="muted">추가 중…</div>';
  const r=await fetch('/api/admin/events/add',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  const d=await r.json();
  if(!r.ok){ $('#evMsg').innerHTML=`<div class="err">${esc(d.error||'오류')}</div>`; return; }
  $('#evMsg').innerHTML='<div class="ok">추가됨</div>'; $('#evTitle').value=''; $('#evDesc').value=''; $('#evBonus').value=''; loadEvents();
}
async function toggleEvt(id){ await fetch('/api/admin/events/toggle',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id})}); loadEvents(); }
async function removeEvt(id){ await fetch('/api/admin/events/remove',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id})}); loadEvents(); }

document.querySelectorAll('.tab').forEach(t=>t.addEventListener('click',()=>{
  document.querySelectorAll('.tab').forEach(x=>x.classList.remove('on')); t.classList.add('on');
  const a=t.dataset.atab;
  $('#atab-members').style.display=a==='members'?'':'none';
  $('#atab-effects').style.display=a==='effects'?'':'none';
  $('#atab-events').style.display=a==='events'?'':'none';
}));
if($('#addBtn')){
  $('#addBtn').addEventListener('click',addPlayer);
  $('#riotId').addEventListener('keydown',e=>{if(e.key==='Enter')addPlayer();});
  $('#refreshBtn').addEventListener('click',refreshAll);
  $('#effAddBtn').addEventListener('click',addEff);
  $('#evKind').addEventListener('change',syncEvKind);
  $('#evAddBtn').addEventListener('click',addEvt);
  loadRequests(); loadList(); loadEffects(); loadEvents();
}
</script></body></html>
"""
ADMIN_PAGE = ADMIN_PAGE.replace("__THEME__", THEME)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
