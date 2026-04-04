# kvstore

Persistent key-value store with TTL, namespaces, and import/export.

## Usage

```bash
python3 kvstore.py set mykey "myvalue"
python3 kvstore.py set token "abc" --ttl 1h
python3 kvstore.py get mykey
python3 kvstore.py --ns cache set url "https://..."
python3 kvstore.py list --json
python3 kvstore.py ns                  # list namespaces
python3 kvstore.py export > data.json
python3 kvstore.py import data.json
python3 kvstore.py stats
python3 kvstore.py flush
```

## Features

- TTL with auto-expiry (seconds, minutes, hours, days)
- Namespaces for key isolation
- JSON export/import
- Store statistics
- SQLite persistence
- Zero dependencies
