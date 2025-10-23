# ---------- Finance MiniApp (оптимизированная версия) ----------
from flask import Flask, jsonify, render_template, request, url_for
from flask_caching import Cache
from flask_compress import Compress
from flask_socketio import SocketIO, emit
from apscheduler.schedulers.background import BackgroundScheduler
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from pathlib import Path
import gspread, json, os, urllib.request, logging

# ---------- Инициализация ----------
app = Flask(__name__)
Compress(app)

# Cache init — ДО всех @cache.memoize
redis_url = os.getenv("REDIS_URL", "").strip()
cache_cfg = {"CACHE_DEFAULT_TIMEOUT": 300}
if redis_url:
    cache_cfg.update({"CACHE_TYPE": "redis", "CACHE_REDIS_URL": redis_url})
else:
    cache_cfg.update({"CACHE_TYPE": "simple"})
cache = Cache(app, config=cache_cfg)

socketio = SocketIO(app, cors_allowed_origins="*", async_mode="eventlet", logger=False, engineio_logger=False)

# ---------- Кэшируемые функции ----------
@cache.memoize(300)
def get_svod():
    ws = open_sheet(SPREAD_KEY_MAIN, "Свод")
    p1, p2, p3 = ws.batch_get(["A2:B5","D2:E7","G2:H12"])
    return {"p1":p1,"p2":p2,"p3":p3}

@cache.memoize(300)
def get_metric():
    ws = open_sheet(SPREAD_KEY_MAIN, "Свод")
    return (ws.acell("B6").value or "").strip()

@cache.memoize(300)
def get_svod_detail():
    ws = open_sheet(SPREAD_KEY_MAIN, "Свод")
    pvt, high, acad = ws.batch_get(["A18:B22", "A32:B36", "A46:B50"])
    return {"private":pvt,"highschool":high,"academy":acad}

@cache.memoize(300)
def get_pk(branch):
    ws = get_gspread_client().open("СВОД 25-26").worksheet("PKBot")
    header_ranges = {"Private":"A1:B3","Highschool":"F1:G3","Academy":"K1:L3"}
    table_ranges  = {"Private":"A4:C63","Highschool":"F4:H63","Academy":"K4:M63"}
    header = ws.get(header_ranges.get(branch,"A1:B3"))
    table  = ws.get(table_ranges.get(branch,"A4:C63"))
    return {"header":header,"table":table}

# ---------- Google Sheets ----------
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON", "").strip()

def get_gspread_client():
    """Создаём клиент Google Sheets один раз и переиспользуем"""
    if not hasattr(get_gspread_client, "_client"):
        if GOOGLE_SA_JSON:
            try:
                creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(GOOGLE_SA_JSON), scope)
            except Exception:
                tmp = Path("/tmp/googlesheet.json")
                tmp.write_text(GOOGLE_SA_JSON, encoding="utf-8")
                creds = ServiceAccountCredentials.from_json_keyfile_name(str(tmp), scope)
        else:
            creds = ServiceAccountCredentials.from_json_keyfile_name("googlesheet.json", scope)
        get_gspread_client._client = gspread.authorize(creds)
    return get_gspread_client._client

def open_sheet(key, sheet):
    return get_gspread_client().open_by_key(key).worksheet(sheet)

# Книга "СВОД 25-26"
spreadsheet = get_gspread_client().open("СВОД 25-26")

def get_worksheet_names(branch: str):
    branch = (branch or "Private").strip()
    mapping = {
        "Private":    {"money":"DengiBotPrivate",    "students":"UchenikiBotPrivate",   "staff":"SotrudnikiBotPrivate"},
        "Highschool": {"money":"DengiBotHighschool", "students":"UchenikiBotHighschool","staff":"SotrudnikiBotHighschool"},
        "Academy":    {"money":"DengiBotAcademy",    "students":"UchenikiBotAcademy",   "staff":"SotrudnikiBotAcademy"},
    }
    return mapping.get(branch, mapping["Private"])

# TelegramBot-таблицы (ДДС/баланс/кошельки)
DDS_SOURCES = {
    "Private":    {"key":"1FIBAlCkUL2qT9ztd3gfH5kOd3eHLKE53eYKLJzD75dw", "sheet":"TelegramBotPrivate"},
    "Highschool": {"key":"1N_8nASKsuLaQPbs8BuonLGn5tkjM803X--JyC2_OUt8", "sheet":"TelegramBotHighschool"},
    "Academy":    {"key":"1NkomZvK6mw-QBa7PWW8MhnFN7DdJ_r2a9PSfg095L4Y", "sheet":"TelegramBotAcademy"},
}
def open_dds_sheet(branch: str):
    src = DDS_SOURCES.get(branch, DDS_SOURCES["Private"])
    return get_gspread_client().open_by_key(src["key"]).worksheet(src["sheet"])

DDS_RANGES = {
    'текущий': ['A3:B15','A17:B21','A23:B25'],
    'дата':    ['E3:F15','E17:F21','E23:F25'],
    'месяц':   ['G3:H15','G17:H21','G23:H25']
}

# ---------- РАСШИФРОВКА ----------
BREAKDOWN_SHEETS = {"текущий":"Расшифровка ДДС сегодня","месяц":"Расшифровка ДДС на месяц","дата":"Расшифровка ДДС на дату"}
BD_RANGES = ["B3:B1000","D3:D1000","E3:E1000","F3:F1000"]  # amount, counterparty, purpose, article

def read_breakdown(branch: str, scope: str):
    src = DDS_SOURCES.get(branch, DDS_SOURCES["Private"])
    ws = get_gspread_client().open_by_key(src["key"]).worksheet(BREAKDOWN_SHEETS.get(scope,"Расшифровка ДДС сегодня"))
    b_amount, b_counterparty, b_purpose, b_article = ws.batch_get(BD_RANGES)
    max_len = max(len(b_amount), len(b_article), len(b_counterparty), len(b_purpose))

    def g(a,i):
        try:
            v = a[i][0]
            return (v or "").strip()
        except Exception:
            return ""

    out=[]
    for i in range(max_len):
        amount=g(b_amount,i); article=g(b_article,i)
        if not amount and not article:
            continue
        out.append({"amount":amount,"article":article,"counterparty":g(b_counterparty,i),"purpose":g(b_purpose,i)})
    return out

@app.route('/breakdown')
def breakdown():
    try:
        branch = request.args.get("branch","Private")
        scope  = request.args.get("scope","текущий")
        page   = max(1, int(request.args.get("page",1)))
        limit  = max(1, min(500, int(request.args.get("limit",100))))
        search = (request.args.get("search","") or "").strip().lower()
        items = read_breakdown(branch, scope)
        if search:
            items=[x for x in items if any(search in (x[k] or "").lower() for k in ("counterparty","purpose","article"))]
        total=len(items); start=(page-1)*limit
        return jsonify({"branch":branch,"scope":scope,"total":total,"data":items[start:start+limit]})
    except Exception as e:
        return jsonify({"error":str(e)}),500

# ---------- «Свод» на главной ----------
SVOD_KEY = "1FIBAlCkUL2qT9ztd3gfH5kOd3eHLKE53eYKLJzD75dw"

@app.route('/svod')
def svod():
    try:
        ws = get_gspread_client().open_by_key(SVOD_KEY).worksheet("Свод")
        p1, p2, p3 = ws.batch_get(["A2:B5","D2:E7","G2:H12"])
        return jsonify({"p1":p1 or [], "p2":p2 or [], "p3":p3 or []})
    except Exception as e:
        return jsonify({"error":str(e)}),500

# Кнопка «Сумма» и детальная страница «Свод»
@app.route('/svod-metric')
def svod_metric():
    try:
        ws = get_gspread_client().open_by_key(SVOD_KEY).worksheet("Свод")
        metric = (ws.acell("B6").value or "").strip()
        return jsonify({"metric":metric})
    except Exception as e:
        return jsonify({"error":str(e)}),500

@app.route('/svod-detail')
def svod_detail():
    """Три блока для Private / Highschool / Academy"""
    try:
        ws = get_gspread_client().open_by_key(SVOD_KEY).worksheet("Свод")
        pvt, high, acad = ws.batch_get(["A18:B22", "A32:B36", "A46:B50"])
        return jsonify({"private":pvt or [], "highschool":high or [], "academy":acad or []})
    except Exception as e:
        return jsonify({"error":str(e)}),500

# ---------- Отчёты ----------
REPORTS_ROOT = Path(os.getenv("REPORTS_DIR","static/reports")); REPORTS_ROOT.mkdir(parents=True, exist_ok=True)
RU_MONTHS = {"01":"Январь","02":"Февраль","03":"Март","04":"Апрель","05":"Май","06":"Июнь","07":"Июль","08":"Август","09":"Сентябрь","10":"Октябрь","11":"Ноябрь","12":"Декабрь"}

@app.route('/reports')
def list_reports():
    res=[]
    if REPORTS_ROOT.exists():
        for sub in sorted(REPORTS_ROOT.iterdir()):
            if not sub.is_dir(): continue
            parts=sub.name.split('-')
            title=f"{RU_MONTHS.get(parts[1].zfill(2),parts[1])} {parts[0]}" if len(parts)==2 and parts[0].isdigit() else sub.name
            files=[{"name":f.name,"url":url_for('static',filename=f"reports/{sub.name}/{f.name}")} for f in sorted(sub.iterdir()) if f.is_file() and f.suffix.lower()=='.pdf']
            if files: res.append({"key":sub.name,"title":title,"files":files})
    res.sort(key=lambda x:x["key"], reverse=True)
    return jsonify({"months":res})

@app.route('/reports/upload', methods=['POST'])
def upload_report():
    ym = request.form.get("ym"); file = request.files.get("file")
    if not ym or not file: return jsonify({"error":"Нужны ym (YYYY-MM) и file"}),400
    if not file.filename.lower().endswith(".pdf"): return jsonify({"error":"Только .pdf"}),400
    dst = REPORTS_ROOT / ym; dst.mkdir(parents=True, exist_ok=True)
    safe = file.filename.replace("/","_").replace("\\","_"); file.save(dst/safe)
    return jsonify({"status":"ок"})

@app.route('/reports/delete', methods=['POST'])
def delete_report():
    payload = request.get_json(silent=True) or {}
    ym=(payload.get("ym") or "").strip(); name=(payload.get("name") or "").strip()
    if not ym or not name: return jsonify({"error":"Нужны ym и name"}),400
    target = REPORTS_ROOT/ym/name
    if not target.exists(): return jsonify({"error":"Файл не найден"}),404
    try:
        os.remove(target); p=target.parent
        if p.exists() and not any(p.iterdir()): p.rmdir()
        return jsonify({"status":"ok"})
    except Exception as e:
        return jsonify({"error":str(e)}),500

# ---------- БАЗОВЫЕ СТРАНИЦЫ ----------
@app.route('/')
def home(): return 'Finance MiniApp работает!'
@app.route('/app')
def app_page(): return render_template("index.html")

# ---------- ДДС ----------
@app.route('/dds')
def get_dds_data():
    sheet = spreadsheet.worksheet("ДДС:факт Private")
    return jsonify(sheet.get_all_values())

@app.route('/balance')
def get_balance():
    try:
        branch = request.args.get("branch","Private"); ws = open_dds_sheet(branch)
        balance = (ws.acell("D6").value or "").replace("\u00a0"," ").strip()
        wallet_rows = ws.get("C2:D5") or []
        wallets=[]
        for r in wallet_rows:
            name=(r[0] if len(r)>0 else "").strip()
            val =(r[1] if len(r)>1 else "").replace("\u00a0"," ").strip()
            if name or val: wallets.append([name, val])
        return jsonify({"branch":branch,"worksheet":ws.title,"balance":balance,"wallets":wallets})
    except Exception as e:
        return jsonify({"error":str(e)}),500

@app.route('/summary')
def get_summary():
    try:
        mode = request.args.get('mode','текущий'); branch = request.args.get("branch","Private")
        if mode not in DDS_RANGES: return jsonify({"error":"Некорректный режим"}),400
        sheet = open_dds_sheet(branch); result=[]
        for cell_range in DDS_RANGES[mode]:
            result.extend(sheet.get(cell_range))
        return jsonify(result)
    except Exception as e:
        return jsonify({"error":str(e)}),500

# ---------- Ученики ----------
@app.route('/students')
def students_summary():
    try:
        branch = request.args.get("branch","Private"); mode = request.args.get("mode","current")
        ws = spreadsheet.worksheet(get_worksheet_names(branch)["students"])
        rng = "A3:B7" if mode=="current" else "C3:D7" if mode=="month" else None
        if not rng: return jsonify({"error":"Некорректный режим"}),400
        return jsonify({"branch":branch,"mode":mode,"rows":ws.get(rng)})
    except Exception as e:
        return jsonify({"error":str(e)}),500

@app.route('/students-set-month')
def students_set_month():
    try:
        branch = request.args.get("branch","Private"); value = request.args.get("value")
        if not value: return jsonify({"error":"Не задан месяц"}),400
        spreadsheet.worksheet(get_worksheet_names(branch)["students"]).update_acell("D2", value)
        return jsonify({"status":"ok","written":value,"branch":branch})
    except Exception as e:
        return jsonify({"error":str(e)}),500

# ---------- Сотрудники ----------
@app.route('/staff')
def staff_summary():
    try:
        branch = request.args.get("branch","Private"); mode = request.args.get("mode","current")
        ws = spreadsheet.worksheet(get_worksheet_names(branch)["staff"])
        rng = "A3:B13" if mode=="current" else "C3:D13" if mode=="month" else None
        if not rng: return jsonify({"error":"Некорректный режим"}),400
        return jsonify({"branch":branch,"mode":mode,"rows":ws.get(rng)})
    except Exception as e:
        return jsonify({"error":str(e)}),500

@app.route('/staff-set-month')
def staff_set_month():
    try:
        branch = request.args.get("branch","Private"); value = request.args.get("value")
        if not value: return jsonify({"error":"Не задан месяц"}),400
        spreadsheet.worksheet(get_worksheet_names(branch)["staff"]).update_acell("D1", value)
        return jsonify({"status":"ok","written":value,"branch":branch})
    except Exception as e:
        return jsonify({"error":str(e)}),500

# ---------- Установка даты / месяца ----------
@app.route('/set-date')
def set_date():
    value = request.args.get('value'); branch = request.args.get("branch","Private")
    try:
        open_dds_sheet(branch).update_acell("F1", value)
        return jsonify({"status":"ok","written":value,"branch":branch})
    except Exception as e:
        return jsonify({"error":str(e)}),500

@app.route('/set-month')
def set_month():
    value = request.args.get('value'); branch = request.args.get("branch","Private")
    try:
        open_dds_sheet(branch).update_acell("H1", value)
        return jsonify({"status":"ok","written":value,"branch":branch})
    except Exception as e:
        return jsonify({"error":str(e)}),500

# ---------- Платёжный календарь ----------
@app.route('/pk')
def pk():
    """
    Возвращаем структуру как раньше, но UI теперь рисует всё одной таблицей.
    """
    try:
        branch = request.args.get("branch","Private")
        sheet = spreadsheet.worksheet("PKBot")
        header_ranges = {"Private":"A1:B3","Highschool":"F1:G3","Academy":"K1:L3"}
        table_ranges  = {"Private":"A4:C63","Highschool":"F4:H63","Academy":"K4:M63"}
        header = sheet.get(header_ranges.get(branch,"A1:B3"))
        table  = sheet.get(table_ranges.get(branch,"A4:C63"))
        return jsonify({"branch":branch,"header":header,"table":table})
    except Exception as e:
        return jsonify({"error":str(e)}),500

# ---------- Тренд остатка ----------
@app.route('/balance-trend')
def balance_trend():
    try:
        branch = request.args.get("branch","Private")
        ws = spreadsheet.worksheet(get_worksheet_names(branch)["money"])
        rows = ws.get("J2:K200"); labels=[]; values=[]
        for r in rows:
            if len(r)<2: continue
            dstr=(r[0] or "").strip(); vstr=(r[1] or "").strip()
            if not dstr or not vstr: continue
            try:
                d=datetime.strptime(dstr,"%d.%m.%Y"); labels.append(d.strftime("%d.%m"))
            except Exception:
                labels.append(dstr)
            clean=vstr.replace("\xa0"," ").replace("?","").replace(" ","").replace(",",".")
            try: values.append(float(clean))
            except Exception: values.append(0.0)
        return jsonify({"labels":labels,"values":values,"branch":branch})
    except Exception as e:
        return jsonify({"error":str(e)}),500

@app.after_request
def apply_headers(resp):
    resp.headers["ngrok-skip-browser-warning"]="true"; return resp

# -------- Telegram webhook --------
WEBAPP_URL = os.getenv("WEBAPP_URL","https://finance-miniapp.onrender.com/app").strip()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN","").strip()
TG_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}" if TELEGRAM_TOKEN else ""

def tg_send_message(chat_id:int, text:str, reply_markup:dict|None=None):
    if not TG_API: return
    data={"chat_id":chat_id,"text":text,"parse_mode":"HTML"}
    if reply_markup: data["reply_markup"]=reply_markup
    body=json.dumps(data).encode("utf-8")
    req=urllib.request.Request(f"{TG_API}/sendMessage", data=body, headers={"Content-Type":"application/json"}, method="POST")
    with urllib.request.urlopen(req) as resp: resp.read()

@app.route("/telegram-webhook", methods=["POST"])
@app.route(f"/telegram-webhook/{TELEGRAM_TOKEN}", methods=["POST"])
def telegram_webhook():
    upd = request.get_json(silent=True) or {}
    msg = upd.get("message") or upd.get("edited_message") or {}
    chat = msg.get("chat") or {}; chat_id = chat.get("id")
    text = (msg.get("text") or "").strip()
    if chat_id and text.startswith("/start"):
        kb={"inline_keyboard":[[{"text":"Открыть Финансовое Приложение","web_app":{"url":WEBAPP_URL}}]]}
        tg_send_message(chat_id,"Добро пожаловать!", reply_markup=kb)
    return jsonify(ok=True)

# ---------- Background Refresh ----------
scheduler = BackgroundScheduler(daemon=True)
def refresh_cache():
    try:
        cache.delete_memoized(get_svod)
        cache.delete_memoized(get_metric)
        cache.delete_memoized(get_svod_detail)
        for br in DDS_SOURCES.keys():
            cache.delete_memoized(get_pk, br)
        # пересоздать кэш
        get_svod(); get_metric(); get_svod_detail()
        for br in DDS_SOURCES.keys(): get_pk(br)
        socketio.emit("refresh", {"status":"ok"})
        logging.info("✅ Кэш обновлён")
    except Exception as e:
        logging.error(f"Ошибка обновления кэша: {e}")

scheduler.add_job(refresh_cache, "interval", minutes=5)
scheduler.start()

# ---------- After-request headers ----------
@app.after_request
def apply_headers(resp):
    resp.headers["ngrok-skip-browser-warning"] = "true"
    resp.headers["Cache-Control"] = "public, max-age=60"
    return resp

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT","5000")))
