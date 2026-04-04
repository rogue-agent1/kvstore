#!/usr/bin/env python3
"""kvstore - Persistent key-value store with TTL and namespaces.

SQLite-backed CLI key-value store. Zero dependencies.
"""

import argparse
import json
import os
import sqlite3
import sys
import time


DB_PATH = os.environ.get("KVSTORE_DB", os.path.expanduser("~/.kvstore.db"))


def get_db():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    db.execute("""CREATE TABLE IF NOT EXISTS kv (
        ns TEXT DEFAULT 'default',
        key TEXT NOT NULL,
        value TEXT,
        created_at REAL,
        expires_at REAL,
        PRIMARY KEY (ns, key)
    )""")
    db.commit()
    return db


def prune(db):
    db.execute("DELETE FROM kv WHERE expires_at IS NOT NULL AND expires_at < ?", (time.time(),))
    db.commit()


def parse_ttl(s):
    if not s:
        return None
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    if s[-1] in units:
        return float(s[:-1]) * units[s[-1]]
    return float(s)


def cmd_set(args):
    db = get_db()
    prune(db)
    now = time.time()
    ttl = parse_ttl(args.ttl)
    expires = now + ttl if ttl else None
    db.execute("INSERT OR REPLACE INTO kv (ns, key, value, created_at, expires_at) VALUES (?,?,?,?,?)",
               (args.ns, args.key, args.value, now, expires))
    db.commit()
    print(f"OK")


def cmd_get(args):
    db = get_db()
    prune(db)
    row = db.execute("SELECT value FROM kv WHERE ns=? AND key=?", (args.ns, args.key)).fetchone()
    if row:
        print(row["value"])
    else:
        if args.default is not None:
            print(args.default)
        else:
            sys.exit(1)


def cmd_del(args):
    db = get_db()
    n = db.execute("DELETE FROM kv WHERE ns=? AND key=?", (args.ns, args.key)).rowcount
    db.commit()
    print(f"Deleted {n}")


def cmd_list(args):
    db = get_db()
    prune(db)
    rows = db.execute("SELECT key, value, expires_at FROM kv WHERE ns=? ORDER BY key", (args.ns,)).fetchall()
    if args.json:
        d = {r["key"]: r["value"] for r in rows}
        print(json.dumps(d, indent=2))
    else:
        for r in rows:
            ttl_str = ""
            if r["expires_at"]:
                remaining = r["expires_at"] - time.time()
                if remaining > 0:
                    ttl_str = f" (TTL: {int(remaining)}s)"
            print(f"  {r['key']} = {r['value']}{ttl_str}")
        print(f"\n{len(rows)} keys in '{args.ns}'", file=sys.stderr)


def cmd_ns(args):
    db = get_db()
    prune(db)
    rows = db.execute("SELECT ns, COUNT(*) as c FROM kv GROUP BY ns ORDER BY ns").fetchall()
    for r in rows:
        print(f"  {r['ns']}: {r['c']} keys")


def cmd_export(args):
    db = get_db()
    prune(db)
    rows = db.execute("SELECT ns, key, value FROM kv WHERE ns=? ORDER BY key", (args.ns,)).fetchall()
    d = {r["key"]: r["value"] for r in rows}
    print(json.dumps(d, indent=2))


def cmd_import(args):
    db = get_db()
    data = json.load(open(args.file))
    now = time.time()
    count = 0
    for k, v in data.items():
        db.execute("INSERT OR REPLACE INTO kv (ns, key, value, created_at) VALUES (?,?,?,?)",
                   (args.ns, k, str(v), now))
        count += 1
    db.commit()
    print(f"Imported {count} keys into '{args.ns}'")


def cmd_flush(args):
    db = get_db()
    if args.ns != "default":
        n = db.execute("DELETE FROM kv WHERE ns=?", (args.ns,)).rowcount
    else:
        n = db.execute("DELETE FROM kv").rowcount
    db.commit()
    print(f"Flushed {n} keys")


def cmd_stats(args):
    db = get_db()
    prune(db)
    total = db.execute("SELECT COUNT(*) as c FROM kv").fetchone()["c"]
    namespaces = db.execute("SELECT COUNT(DISTINCT ns) as c FROM kv").fetchone()["c"]
    expiring = db.execute("SELECT COUNT(*) as c FROM kv WHERE expires_at IS NOT NULL").fetchone()["c"]
    size = os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0
    print(f"  Keys: {total}")
    print(f"  Namespaces: {namespaces}")
    print(f"  With TTL: {expiring}")
    print(f"  DB size: {size} bytes")


def main():
    p = argparse.ArgumentParser(description="Persistent key-value store")
    p.add_argument("--ns", default="default", help="Namespace")
    sub = p.add_subparsers(dest="cmd")

    sp = sub.add_parser("set", help="Set a key")
    sp.add_argument("key")
    sp.add_argument("value")
    sp.add_argument("--ttl", help="Time to live (e.g. 30s, 5m, 1h, 7d)")

    gp = sub.add_parser("get", help="Get a key")
    gp.add_argument("key")
    gp.add_argument("-d", "--default", help="Default if missing")

    dp = sub.add_parser("del", help="Delete a key")
    dp.add_argument("key")

    lp = sub.add_parser("list", help="List keys")
    lp.add_argument("--json", action="store_true")

    sub.add_parser("ns", help="List namespaces")
    sub.add_parser("stats", help="Store statistics")

    ep = sub.add_parser("export", help="Export namespace as JSON")
    ip = sub.add_parser("import", help="Import JSON into namespace")
    ip.add_argument("file")

    fp = sub.add_parser("flush", help="Delete all keys (or namespace)")

    args = p.parse_args()
    if not args.cmd:
        p.print_help()
        sys.exit(1)
    {"set": cmd_set, "get": cmd_get, "del": cmd_del, "list": cmd_list,
     "ns": cmd_ns, "export": cmd_export, "import": cmd_import,
     "flush": cmd_flush, "stats": cmd_stats}[args.cmd](args)


if __name__ == "__main__":
    main()
