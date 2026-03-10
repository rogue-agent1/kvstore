#!/usr/bin/env python3
"""kvstore - Persistent key-value store with namespaces.

One file. Zero deps. Remember things.

Usage:
  kvstore.py set mykey "some value"     → store a value
  kvstore.py get mykey                  → retrieve it
  kvstore.py del mykey                  → delete it
  kvstore.py list                       → list all keys
  kvstore.py search "pattern"           → search keys/values
  kvstore.py export                     → dump as JSON
  kvstore.py import data.json           → load from JSON
  kvstore.py -n myapp set key val       → namespaced store
"""

import argparse
import fnmatch
import json
import os
import sys
import time

STORE_DIR = os.path.expanduser("~/.local/share/kvstore")


def store_path(ns: str) -> str:
    os.makedirs(STORE_DIR, exist_ok=True)
    return os.path.join(STORE_DIR, f"{ns}.json")


def load(ns: str) -> dict:
    try:
        with open(store_path(ns)) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save(ns: str, data: dict):
    with open(store_path(ns), "w") as f:
        json.dump(data, f, indent=2)


def cmd_set(args):
    data = load(args.ns)
    data[args.key] = {"value": args.value, "updated": time.time()}
    save(args.ns, data)
    print(f"✅ {args.key} = {args.value}")

def cmd_get(args):
    data = load(args.ns)
    entry = data.get(args.key)
    if entry:
        print(entry["value"])
    else:
        print(f"Key '{args.key}' not found", file=sys.stderr)
        return 1

def cmd_del(args):
    data = load(args.ns)
    if args.key in data:
        del data[args.key]
        save(args.ns, data)
        print(f"✅ Deleted '{args.key}'")
    else:
        print(f"Key '{args.key}' not found", file=sys.stderr)
        return 1

def cmd_list(args):
    data = load(args.ns)
    if not data:
        print("(empty)")
        return
    for k, v in sorted(data.items()):
        val = str(v["value"])[:60]
        print(f"  {k:30s} {val}")

def cmd_search(args):
    data = load(args.ns)
    pattern = args.pattern.lower()
    found = 0
    for k, v in data.items():
        if pattern in k.lower() or pattern in str(v["value"]).lower():
            print(f"  {k:30s} {str(v['value'])[:60]}")
            found += 1
    if not found:
        print(f"No matches for '{args.pattern}'")

def cmd_export(args):
    data = load(args.ns)
    simple = {k: v["value"] for k, v in data.items()}
    print(json.dumps(simple, indent=2))

def cmd_import(args):
    with open(args.file) as f:
        incoming = json.load(f)
    data = load(args.ns)
    for k, v in incoming.items():
        data[k] = {"value": v, "updated": time.time()}
    save(args.ns, data)
    print(f"✅ Imported {len(incoming)} keys")

def main():
    p = argparse.ArgumentParser(description="Persistent key-value store")
    p.add_argument("-n", "--ns", default="default", help="Namespace")
    sub = p.add_subparsers(dest="cmd")

    s = sub.add_parser("set"); s.add_argument("key"); s.add_argument("value")
    s = sub.add_parser("get"); s.add_argument("key")
    s = sub.add_parser("del"); s.add_argument("key")
    sub.add_parser("list")
    s = sub.add_parser("search"); s.add_argument("pattern")
    sub.add_parser("export")
    s = sub.add_parser("import"); s.add_argument("file")

    args = p.parse_args()
    if not args.cmd:
        p.print_help()
        return 1
    cmds = {"set": cmd_set, "get": cmd_get, "del": cmd_del, "list": cmd_list,
            "search": cmd_search, "export": cmd_export, "import": cmd_import}
    return cmds[args.cmd](args) or 0

if __name__ == "__main__":
    sys.exit(main())
