from flask import Flask, render_template, abort, Response
import sqlite3
from datetime import datetime
import globals

app = Flask(__name__)

def get_all_pastes():
    conn = sqlite3.connect(globals.DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("""
        SELECT key, title, author, date
        FROM pastes
        ORDER BY date DESC
    """)
    rows = cur.fetchall()
    conn.close()
    pastes = []
    for r in rows:
        pastes.append({
            "key": r["key"],
            "title": r["title"] or "(no title)",
            "author": r["author"],
            "date": datetime.fromisoformat(r["date"]).strftime("%Y-%m-%d")
        })
    return pastes

def get_paste_content(key):
    conn = sqlite3.connect(globals.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT content FROM pastes WHERE key = ?",
        (key,)
    )
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None

@app.route("/")
def index():
    pastes = get_all_pastes()
    return render_template(
        "index.html",
        pastes=pastes,
        authors=globals.AUTHORS + ["none"],
    )

@app.route("/<key>")
def archive(key):
    content = get_paste_content(key)
    if content is None:
        abort(404)
    return Response(content, mimetype="text/plain")

if __name__ == "__main__":
    app.run(debug=True)
