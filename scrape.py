import re
import time
import certifi
import requests
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import sqlite3
# import cloudscraper
# import curl_cffi
import globals

BATCH_SIZE = 50

def db_init(conn):
    conn.execute("PRAGMA journal_mode=WAL;")
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS pastes (
        key TEXT PRIMARY KEY,
        title TEXT,
        author TEXT,
        date DATETIME,
        content TEXT,
        last_checked DATETIME
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS scraper_state (
        key TEXT PRIMARY KEY,
        value INTEGER
    )
    """)
    cur.execute("""
    INSERT OR IGNORE INTO scraper_state (key, value)
    VALUES ('run_number', 0)
    """)

def get_and_increment_run_number(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT value FROM scraper_state
        WHERE key = 'run_number'
    """)
    run_number = cur.fetchone()[0]
    cur.execute("""
        UPDATE scraper_state
        SET value = value + 1
        WHERE key = 'run_number'
    """)
    conn.commit()
    return run_number

def db_paste_exists(cur, key):
    cur.execute(
        "SELECT 1 FROM pastes WHERE key = ?",
        (key,)
    )
    exists = cur.fetchone() is not None
    return exists

def db_upsert_paste(cur, paste, log = True):
    cur.execute("""
    INSERT INTO pastes (key, title, author, date, content, last_checked)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(key) DO UPDATE SET
        title = excluded.title,
        author = excluded.author,
        date = excluded.date,
        content = excluded.content,
        last_checked = excluded.last_checked
    """, (
        paste["key"],
        paste["title"],
        paste["author"],
        paste["date"],
        paste["content"],
        datetime.now(timezone.utc).isoformat()
    ))
    if log: print(f'Updated "{paste["title"]}" from {paste["author"]}')

def parse_date(date_str):
    clean = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', date_str)
    return datetime.strptime(clean, "%b %d, %Y").isoformat()

def scrape_user_list(user):
    page = 1
    pastes, last_page = scrape_user_list_page(user, page)
    while page < last_page:
        page += 1
        page_pastes, last_page = scrape_user_list_page(user, page)
        pastes.extend(page_pastes)
    return pastes

def scrape_user_list_page(user, page):
    pastes = []
    last_page = 1
    try:
        r = requests.get(f"https://pastebin.com/u/{user}/{page}", timeout=3)
    except Exception as e:
        print(f"Error fetching user {user} page {page}: {e}")
        return pastes, last_page
    if r.status_code != 200:
        print(f"Error {r.status_code} fetching user {user} page {page}")
        return pastes, last_page
    parsed = BeautifulSoup(r.content, "html.parser")
    pagination = parsed.find("div", class_="pagination")
    if pagination is not None:
        pages = pagination.find_all("a")
        if len(pages) > 0:
            last_page = int(pages[-1].get("data-page")) + 1
    table = parsed.find("table", class_="maintable")
    if table is None:
        print(f"No table found for {user}")
        return pastes, last_page
    table = table.tbody
    if table is None:
        print(f"No table found for {user}")
        return pastes, last_page
    rows = table.find_all("tr")
    for row in rows:
        cols = row.find_all("td")
        pastes.append({
            "key": cols[0].a['href'][1:],
            "title": cols[0].text.strip(),
            "author": user,
            "date": parse_date(cols[1].text.strip()),
            "content": None,
        })
    return pastes, last_page

def scrape_content(paste):
    try:
        r = requests.get(f"https://pastebin.com/raw/{paste['key']}", verify=certifi.where(), timeout=3)
    except Exception as e:
        print(f"Error fetching paste {paste['key']} from {paste['author']}: {e}")
        return
    if r.status_code != 200:
        print(f"Error {r.status_code} fetching paste {paste['key']} from {paste['author']}")
        return
    paste["content"] = r.text

def scrape_paste(paste):
    try:
        r = requests.get(f"https://pastebin.com/{paste['key']}", timeout=3)
    except Exception as e:
        print(f"Error fetching paste {paste['key']}: {e}")
        return
    if r.status_code != 200:
        print(f"Error {r.status_code} fetching paste {paste['key']}")
        return
    parsed = BeautifulSoup(r.content, "html.parser")
    title = parsed.find("div", class_="info-top")
    if title is None:
        print(f"No title found in {paste['key']}")
        return
    author = parsed.find("div", class_="username")
    if author is None:
        print(f"No author found in {paste['key']}")
        return
    date = parsed.find("div", class_="date")
    if date is None:
        print(f"No date found in {paste['key']}")
        return
    date = date.find("span")
    if date is None:
        print(f"No date found in {paste['key']}")
        return
    paste["title"] = title.text.strip()
    paste["author"] = author.text.strip() if author.text.strip() != "a guest" else "none"
    paste["date"] = parse_date(date.text.strip())
    scrape_content(paste)

def check_new_pastes(conn):
    cur = conn.cursor()
    for user in globals.AUTHORS:
        pastes = scrape_user_list(user)
        for paste in pastes:
            if not db_paste_exists(cur, paste["key"]):
                scrape_content(paste)
                if paste.get("content") is not None:
                    db_upsert_paste(cur, paste)
                    conn.commit()
    for key in globals.NO_AUTHOR:
        if not db_paste_exists(cur, key):
            paste = {"key": key}
            scrape_paste(paste)
            if paste.get("content") is not None:
                db_upsert_paste(cur, paste)
                conn.commit()

def check_old_pastes(conn):
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("""
        SELECT key, title, author, date, content
        FROM pastes
        ORDER BY
            last_checked IS NOT NULL,
            last_checked ASC
        LIMIT ?
    """, (BATCH_SIZE,))
    rows = cur.fetchall()
    for r in rows:
        paste = {
            "key": r["key"],
            "title": r["title"],
            "author": r["author"],
            "date": r["date"],
            "content": r["content"],
        }
        scrape_content(paste)
        db_upsert_paste(cur, paste, False)
        conn.commit()

def main():
    with sqlite3.connect(globals.DB_PATH, timeout=5) as conn:
        db_init(conn)
        run_number = get_and_increment_run_number(conn)
        if run_number % 3 == 0:
            print(f"{time.strftime('%Y-%m-%d %X')} Checking new pastes")
            check_new_pastes(conn)
        else:
            print(f"{time.strftime('%Y-%m-%d %X')} Checking old pastes")
            check_old_pastes(conn)
        print(f"{time.strftime('%Y-%m-%d %X')} Done")

if __name__ == "__main__":
    main()

# p = {"key": "jUrxu46r"}
# scrape_content(p)
# print(p)
# exit()
