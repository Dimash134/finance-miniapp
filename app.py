from flask import Flask, jsonify, render_template, request, url_for
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from pathlib import Path
import os
from time import time  # <-- для кэша

# --- читаем ключ Google из переменной окружения, а не из файла ---
GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON")
CREDS_FILE = "googlesheet.json"
if GOOGLE_SA_JSON:
    tmp_path = Path("/tmp/googlesheet.json")
    tmp_path.write_text(GOOGLE_SA_JSON)
    CREDS_FILE = str(tmp_path)

app = Flask(__name__)

# ---------- Google Sheets ----------
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, scope)
client = gspread.authorize(creds)

# Книга "СВОД 25-26" — используется для учеников/сотрудников/ПК, как раньше
spreadsheet = client.open("СВОД 25-26")

# --- простой кэш в памяти ---
_cache = {}
def get_cached(key, ttl, fetch_func):
    """
    key: любой hashable (кортеж строк и т.п.)
    ttl: секунды жизни
    fetch_func: функция без аргументов, которая вернёт данные
    """
    now = time()
    hit = _cache.get(key)
    if hit and (now - hit["t"]) < ttl:
        return hit["v"]
    v = fetch_func()
    _cache[key] = {"v": v, "t": now}
    return v

# Карта листов по филиалам (для учеников/сотрудников/ПК)
def get_worksheet_names(branch: str):
    branch = (branch or "Private").strip()
    mapping = {
        "Private": {
            "money":      "DengiBotPrivate",
            "students":   "UchenikiBotPrivate",
            "staff":      "SotrudnikiBotPrivate",
        },
        "Highschool": {
            "money":      "DengiBotHighschool",
            "students":   "UchenikiBotHighschool",
            "staff":      "SotrudnikiBotHighschool",
        },
        "Academy": {
            "money":      "DengiBotAcademy",
            "students":   "UchenikiBotAcademy",
            "staff":      "SotrudnikiBotAcademy",
        },
    }
    return mapping.get(branch, mapping["Private"])

# ---- Таблицы/листы ДДС (источник для ДДС, остатка и кошельков)
DDS_SOURCES = {
    "Private":    {"key": "1FIBAlCkUL2qT9ztd3gfH5kOd3eHLKE53eYKLJzD75dw", "sheet": "TelegramBotPrivate"},
    "Highschool": {"key": "1N_8nASKsuLaQPbs8BuonLGn5tkjM803X--JyC2_OUt8", "sheet": "TelegramBotHighschool"},
    "Academy":    {"key": "1NkomZvK6mw-QBa7PWW8MhnFN7DdJ_r2a9PSfg095L4Y", "sheet": "TelegramBotAcademy"},
}

def open_dds_sheet(branch: str):
    src = DDS_SOURCES.get(branch, DDS_SOURCES["Private"])
    book = client.open_by_key(src["key"])
    return book.worksheet(src["sheet"])

# Диапазоны ДДС
DDS_RANGES = {
    'текущий': ['A3:B15', 'A17:B21', 'A23:B25'],
    'дата':    ['E3:F15', 'E17:F21', 'E23:F25'],
    'месяц':   ['G3:H15', 'G17:H21', 'G23:H25']
}

# ---------- Общие PDF-отчёты ----------
REPORTS_ROOT = Path(os.getenv("REPORTS_DIR", "static/reports"))
REPORTS_ROOT.mkdir(parents=True, exist_ok=True)

RU_MONTHS = {
    "01": "Январь", "02": "Февраль", "03": "Март", "04": "Апрель",
    "05": "Май", "06": "Июнь", "07": "Июль", "08": "Август",
    "09": "Сентябрь", "10": "Октябрь", "11": "Ноябрь", "12": "Декабрь",
}

@app.route('/reports')
def list_reports():
    result = []
    if REPORTS_ROOT.exists():
        for sub in sorted(REPORTS_ROOT.iterdir()):
            if not sub.is_dir():
                continue
            parts = sub.name.split('-')
            if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                yyyy, mm = parts
                title = f"{RU_MONTHS.get(mm.zfill(2), mm)} {yyyy}"
            else:
                title = sub.name
            files = []
            for f in sorted(sub.iterdir()):
                if f.is_file() and f.suffix.lower() == ".pdf":
                    files.append({"name": f.name, "url": url_for('static', filename=f"reports/{sub.name}/{f.name}")})
            if files:
                result.append({"key": sub.name, "title": title, "files": files})
    result.sort(key=lambda x: x["key"], reverse=True)
    return jsonify({"months": result})

@app.route('/reports/upload', methods=['POST'])
def upload_report():
    ym = request.form.get("ym")
    file = request.files.get("file")
    if not ym or not file:
        return jsonify({"error": "Нужны ym (YYYY-MM) и file"}), 400
    if not file.filename.lower().endswith(".pdf"):
        return jsonify({"error": "Только .pdf"}), 400
    dst_dir = REPORTS_ROOT / ym
    dst_dir.mkdir(parents=True, exist_ok=True)
    safe_name = file.filename.replace("/", "_").replace("\\", "_")
    file.save(dst_dir / safe_name)
    return jsonify({"status": "ok"})

@app.route('/reports/delete', methods=['POST'])
def delete_report():
    payload = request.get_json(silent=True) or {}
    ym = (payload.get("ym") or "").strip()
    name = (payload.get("name") or "").strip()
    if not ym or not name:
        return jsonify({"error": "Нужны ym и name"}), 400
    target = REPORTS_ROOT / ym / name
    if not target.exists():
        return jsonify({"error": "Файл не найден"}), 404
    try:
        os.remove(target)
        month_dir = target.parent
        if month_dir.exists() and not any(month_dir.iterdir()):
            month_dir.rmdir()
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/')
def home():
    return '📊 Finance MiniApp работает!'

@app.route('/app')
def app_page():
    return render_template("index.html")

# ---------- ДДС (сырой, как было) ----------
@app.route('/dds')
def get_dds_data():
    sheet = spreadsheet.worksheet("ДДС:факт Private")
    data = sheet.get_all_values()
    return jsonify(data)

# ---------- Баланс и кошельки ----------
@app.route('/balance')
def get_balance():
    """
    Для всех филиалов читаем из соответствующей TelegramBot-таблицы и листа:
      - баланс: D6
      - кошельки: C2:D5
    """
    try:
        branch = request.args.get("branch", "Private")

        def fetch():
            ws = open_dds_sheet(branch)
            balance = (ws.acell("D6").value or "").replace("\u00a0", " ").strip()
            wallet_rows = ws.get("C2:D5") or []
            wallets = []
            for row in wallet_rows:
                name = (row[0] if len(row) > 0 else "").strip()
                val  = (row[1] if len(row) > 1 else "").replace("\u00a0", " ").strip()
                if name or val:
                    wallets.append([name, val])
            return {
                "branch": branch,
                "worksheet": ws.title,
                "balance": balance,
                "wallets": wallets
            }

        data = get_cached(("balance", branch), 20, fetch)  # TTL 20 сек
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ---------- Summary по режимам (из TelegramBot* листов) ----------
@app.route('/summary')
def get_summary():
    try:
        mode = request.args.get('mode', 'текущий')
        branch = request.args.get("branch", "Private")
        if mode not in DDS_RANGES:
            return jsonify({"error": "Некорректный режим"}), 400

        def fetch():
            sheet = open_dds_sheet(branch)
            # Один запрос вместо 3-х
            blocks = sheet.batch_get(DDS_RANGES[mode])  # list[list[list[str]]]
            # склеиваем в один список строк
            return sum(blocks, [])

        result = get_cached(("summary", branch, mode), 20, fetch)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ---------- Ученики ----------
@app.route('/students')
def students_summary():
    try:
        branch = request.args.get("branch", "Private")
        mode   = request.args.get("mode", "current")
        ws_name = get_worksheet_names(branch)["students"]
        sheet = spreadsheet.worksheet(ws_name)
        rng = "A3:B7" if mode == "current" else "C3:D7" if mode == "month" else None
        if not rng:
            return jsonify({"error": "Некорректный режим"}), 400

        def fetch():
            return sheet.get(rng)

        rows = get_cached(("students", branch, mode, rng), 20, fetch)
        return jsonify({"branch": branch, "mode": mode, "rows": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/students-set-month')
def students_set_month():
    try:
        branch = request.args.get("branch", "Private")
        value  = request.args.get("value")
        if not value:
            return jsonify({"error": "Не задан месяц"}), 400
        ws_name = get_worksheet_names(branch)["students"]
        sheet = spreadsheet.worksheet(ws_name)
        sheet.update_acell("D2", value)
        # сбросим кэш по students-month
        _cache.pop(("students", branch, "month", "C3:D7"), None)
        return jsonify({"status": "ok", "written": value, "branch": branch})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ---------- Сотрудники ----------
@app.route('/staff')
def staff_summary():
    try:
        branch = request.args.get("branch", "Private")
        mode   = request.args.get("mode", "current")
        ws_name = get_worksheet_names(branch)["staff"]
        sheet = spreadsheet.worksheet(ws_name)
        rng = "A3:B13" if mode == "current" else "C3:D13" if mode == "month" else None
        if not rng:
            return jsonify({"error": "Некорректный режим"}), 400

        def fetch():
            return sheet.get(rng)

        rows = get_cached(("staff", branch, mode, rng), 20, fetch)
        return jsonify({"branch": branch, "mode": mode, "rows": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/staff-set-month')
def staff_set_month():
    try:
        branch = request.args.get("branch", "Private")
        value  = request.args.get("value")
        if not value:
            return jsonify({"error": "Не задан месяц"}), 400
        ws_name = get_worksheet_names(branch)["staff"]
        sheet = spreadsheet.worksheet(ws_name)
        sheet.update_acell("D1", value)
        # сброс кэша по staff-month
        _cache.pop(("staff", branch, "month", "C3:D13"), None)
        return jsonify({"status": "ok", "written": value, "branch": branch})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ---------- Установка даты / месяца (TelegramBot* листы) ----------
@app.route('/set-date')
def set_date():
    value = request.args.get('value')
    branch = request.args.get("branch", "Private")
    try:
        sheet = open_dds_sheet(branch)
        sheet.update_acell("F1", value)
        # сбросим кэш по summary-дата
        _cache.pop(("summary", branch, "дата"), None)
        return jsonify({"status": "ok", "written": value, "branch": branch})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/set-month')
def set_month():
    value = request.args.get('value')
    branch = request.args.get("branch", "Private")
    try:
        sheet = open_dds_sheet(branch)
        sheet.update_acell("H1", value)
        # сбросим кэш по summary-месяц
        _cache.pop(("summary", branch, "месяц"), None)
        return jsonify({"status": "ok", "written": value, "branch": branch})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ---------- Платёжный календарь ----------
@app.route('/pk')
def pk():
    """
    Лист PKBot в книге «СВОД 25-26»
      Заголовок:
        Private     -> A1:B3
        Highschool  -> F1:G3
        Academy     -> K1:L3
      Таблица:
        Private     -> A4:C63
        Highschool  -> F4:H63
        Academy     -> K4:M63
    """
    try:
        branch = request.args.get("branch", "Private")
        sheet = spreadsheet.worksheet("PKBot")

        header_ranges = {"Private": "A1:B3", "Highschool": "F1:G3", "Academy": "K1:L3"}
        table_ranges  = {"Private": "A4:C63", "Highschool": "F4:H63", "Academy": "K4:M63"}

        hr = header_ranges.get(branch, "A1:B3")
        tr = table_ranges.get(branch, "A4:C63")

        def fetch():
            # одним вызовом получаем и хедер, и таблицу
            blocks = sheet.batch_get([hr, tr])  # [header_values, table_values]
            header = blocks[0] if len(blocks) > 0 else []
            table  = blocks[1] if len(blocks) > 1 else []
            return {"branch": branch, "header": header, "table": table}

        data = get_cached(("pk", branch, hr, tr), 20, fetch)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ---------- (опционально) тренд остатка ----------
@app.route('/balance-trend')
def balance_trend():
    try:
        branch = request.args.get("branch", "Private")
        ws_name = get_worksheet_names(branch)["money"]
        sheet = spreadsheet.worksheet(ws_name)
        rows = sheet.get("J2:K200")
        labels, values = [], []
        for r in rows:
            if len(r) < 2:
                continue
            date_str = (r[0] or "").strip()
            val_str = (r[1] or "").strip()
            if not date_str or not val_str:
                continue
            try:
                d = datetime.strptime(date_str, "%d.%m.%Y")
                labels.append(d.strftime("%d.%m"))
            except Exception:
                labels.append(date_str)
            clean = val_str.replace("\xa0", " ").replace("₸", "").replace(" ", "").replace(",", ".")
            try:
                v = float(clean)
            except Exception:
                v = 0.0
            values.append(v)
        return jsonify({"labels": labels, "values": values, "branch": branch})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.after_request
def apply_headers(response):
    response.headers["ngrok-skip-browser-warning"] = "true"
    return response

if __name__ == '__main__':
    app.run(debug=True)
