"""Copy playlists and favorite tracks between Tidal accounts, with a local SQLite backup.

Source account: reuses the OAuth session stored in `.session.yml` (written by spotidal).
Destination account: interactive OAuth login on first run, stored in `.session_dest.yml`.

Tidal track IDs are identical across accounts, so the copy is 1:1 with no matching.
Every run that reads the source also saves a full snapshot (playlists, tracks with
metadata, favorites) to a local SQLite file, `tidal_backup.db` by default. If Tidal ever
restricts API access for the source account, the snapshot can be restored with
`--from-backup`, which never touches the source account.

Safe to re-run: playlists whose name already exists in the destination are skipped,
and favorites already present in the destination are not added again.

Backup semantics: each run replaces the stored playlists it read and the whole favorites
list. Playlists deleted at the source stay in the backup. `--skip-playlists` and
`--skip-favorites` leave that part of the backup untouched.

Usage:
    uv run python scripts/migrate_tidal_to_tidal.py --dry-run         # read source, print only
    uv run python scripts/migrate_tidal_to_tidal.py --backup-only     # read source, write DB only
    uv run python scripts/migrate_tidal_to_tidal.py                   # backup, then copy to destination
    uv run python scripts/migrate_tidal_to_tidal.py --from-backup     # copy from DB, no source account
    uv run python scripts/migrate_tidal_to_tidal.py --only-config     # only playlists listed in config.yml
"""

import argparse
import datetime
import json
import sqlite3
import sys
import time
import webbrowser

import requests
import tidalapi
import yaml
from tidalapi.types import ItemOrder, OrderDirection

SOURCE_SESSION = ".session.yml"
DEST_SESSION = ".session_dest.yml"
DEFAULT_DB = "tidal_backup.db"
CHUNK_SIZE = 20

SCHEMA = """
CREATE TABLE IF NOT EXISTS tracks (
    track_id INTEGER PRIMARY KEY,
    name TEXT,
    version TEXT,
    isrc TEXT,
    duration_s INTEGER,
    artists TEXT,            -- JSON list of {"id", "name"}
    album_id INTEGER,
    album_name TEXT,
    track_number INTEGER,
    volume_number INTEGER,
    explicit INTEGER,
    available INTEGER,
    backed_up_at TEXT
);
CREATE TABLE IF NOT EXISTS playlists (
    playlist_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    num_tracks INTEGER,
    created TEXT,
    last_updated TEXT,
    backed_up_at TEXT
);
CREATE TABLE IF NOT EXISTS playlist_tracks (
    playlist_id TEXT NOT NULL REFERENCES playlists(playlist_id),
    position INTEGER NOT NULL,
    track_id INTEGER NOT NULL,
    PRIMARY KEY (playlist_id, position)
);
CREATE TABLE IF NOT EXISTS favorites (
    position INTEGER PRIMARY KEY,   -- 0 = oldest favorite
    track_id INTEGER NOT NULL
);
"""

# Data shapes passed between functions:
#   playlist  = {"id": str, "name": str, "description": str, "created": str|None,
#                "last_updated": str|None, "track_ids": list[int]}
#   favorites = list[int], oldest first
#   tracks    = dict[int, dict]  (rows for the `tracks` table)


# -- Sessions ---------------------------------------------------------------

def save_session(session: tidalapi.Session, path: str) -> None:
    with open(path, "w") as f:
        yaml.dump(
            {
                "session_id": session.session_id,
                "token_type": session.token_type,
                "access_token": session.access_token,
                "refresh_token": session.refresh_token,
            },
            f,
        )


def load_session(path: str) -> tidalapi.Session | None:
    try:
        with open(path) as f:
            saved = yaml.safe_load(f)
    except OSError:
        return None
    if not saved:
        return None
    session = tidalapi.Session()
    try:
        ok = session.load_oauth_session(
            saved["token_type"], saved["access_token"], saved["refresh_token"]
        )
    except Exception as e:
        print(f"Could not load session from {path}: {e}")
        return None
    if ok and session.check_login():
        save_session(session, path)  # persist refreshed tokens
        return session
    return None


def interactive_login(path: str) -> tidalapi.Session:
    session = tidalapi.Session()
    login, future = session.login_oauth()
    url = login.verification_uri_complete
    if not url.startswith("https://"):
        url = "https://" + url
    print("\nLog in with the DESTINATION Tidal account (use a private/incognito window):")
    print(f"  {url}\n")
    webbrowser.open(url)
    future.result()
    if not session.check_login():
        sys.exit("Destination login failed.")
    save_session(session, path)
    return session


def get_source() -> tidalapi.Session:
    session = load_session(SOURCE_SESSION)
    if session is None:
        sys.exit(f"Source session in {SOURCE_SESSION} is missing or invalid.")
    return session


def get_dest() -> tidalapi.Session:
    session = load_session(DEST_SESSION)
    if session is not None:
        return session
    return interactive_login(DEST_SESSION)


def config_playlist_ids(config_path: str) -> set[str]:
    with open(config_path) as f:
        config = yaml.safe_load(f)
    return {p["tidal_id"] for p in config["sync"]["playlists"] if p.get("tidal_id")}


# -- Reading the source account ---------------------------------------------

def _iso(value) -> str | None:
    return value.isoformat() if isinstance(value, datetime.datetime) else None


def _flag(value) -> int | None:
    return None if value is None else int(bool(value))


def track_row(t, now: str) -> dict:
    album = getattr(t, "album", None)
    return {
        "track_id": t.id,
        "name": t.name,
        "version": getattr(t, "version", None),
        "isrc": getattr(t, "isrc", None),
        "duration_s": getattr(t, "duration", None),
        "artists": json.dumps(
            [{"id": a.id, "name": a.name} for a in (getattr(t, "artists", None) or [])]
        ),
        "album_id": getattr(album, "id", None),
        "album_name": getattr(album, "name", None),
        "track_number": getattr(t, "track_num", None),
        "volume_number": getattr(t, "volume_num", None),
        "explicit": _flag(getattr(t, "explicit", None)),
        "available": _flag(getattr(t, "available", None)),
        "backed_up_at": now,
    }


def fetch_source(
    src: tidalapi.Session,
    only_ids: set[str] | None,
    want_playlists: bool,
    want_favorites: bool,
) -> tuple[list[dict] | None, list[int] | None, dict[int, dict]]:
    now = datetime.datetime.now().isoformat(timespec="seconds")
    tracks: dict[int, dict] = {}
    playlists: list[dict] | None = None
    favorites: list[int] | None = None

    if want_playlists:
        playlists = []
        for sp in src.user.playlists():
            if only_ids is not None and sp.id not in only_ids:
                continue
            pl_tracks = sp.tracks_paginated()
            for t in pl_tracks:
                tracks[t.id] = track_row(t, now)
            playlists.append({
                "id": sp.id,
                "name": sp.name,
                "description": sp.description or "",
                "created": _iso(getattr(sp, "created", None)),
                "last_updated": _iso(getattr(sp, "last_updated", None)),
                "track_ids": [t.id for t in pl_tracks],
            })

    if want_favorites:
        # Oldest first, so the added-date order in the destination matches the source.
        favs = src.user.favorites.tracks_paginated(
            order=ItemOrder.Date, order_direction=OrderDirection.Ascending
        )
        for t in favs:
            tracks[t.id] = track_row(t, now)
        favorites = [t.id for t in favs]

    return playlists, favorites, tracks


# -- Local backup -----------------------------------------------------------

def save_backup(
    db_path: str,
    playlists: list[dict] | None,
    favorites: list[int] | None,
    tracks: dict[int, dict],
) -> None:
    now = datetime.datetime.now().isoformat(timespec="seconds")
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA)
        with conn:  # single transaction: all or nothing
            cols = list(next(iter(tracks.values())).keys()) if tracks else []
            if cols:
                conn.executemany(
                    f"INSERT OR REPLACE INTO tracks ({','.join(cols)}) "
                    f"VALUES ({','.join(':' + c for c in cols)})",
                    list(tracks.values()),
                )
            for p in playlists or []:
                conn.execute("DELETE FROM playlist_tracks WHERE playlist_id = ?", (p["id"],))
                conn.execute(
                    "INSERT OR REPLACE INTO playlists "
                    "(playlist_id, name, description, num_tracks, created, last_updated, backed_up_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (p["id"], p["name"], p["description"], len(p["track_ids"]),
                     p["created"], p["last_updated"], now),
                )
                conn.executemany(
                    "INSERT INTO playlist_tracks (playlist_id, position, track_id) VALUES (?, ?, ?)",
                    [(p["id"], pos, tid) for pos, tid in enumerate(p["track_ids"])],
                )
            if favorites is not None:
                conn.execute("DELETE FROM favorites")
                conn.executemany(
                    "INSERT INTO favorites (position, track_id) VALUES (?, ?)",
                    list(enumerate(favorites)),
                )
    finally:
        conn.close()

    print(f"\nBackup saved to {db_path}: "
          f"{len(playlists) if playlists is not None else 'untouched'} playlists, "
          f"{len(favorites) if favorites is not None else 'untouched'} favorites, "
          f"{len(tracks)} distinct tracks")


def load_backup(db_path: str) -> tuple[list[dict], list[int]]:
    try:
        with open(db_path, "rb"):
            pass
    except OSError:
        sys.exit(f"Backup file {db_path} not found.")
    conn = sqlite3.connect(db_path)
    try:
        playlists = []
        for pid, name, description, created, last_updated in conn.execute(
            "SELECT playlist_id, name, description, created, last_updated FROM playlists ORDER BY rowid"
        ).fetchall():
            ids = [r[0] for r in conn.execute(
                "SELECT track_id FROM playlist_tracks WHERE playlist_id = ? ORDER BY position", (pid,)
            )]
            playlists.append({
                "id": pid, "name": name, "description": description or "",
                "created": created, "last_updated": last_updated, "track_ids": ids,
            })
        favorites = [r[0] for r in conn.execute("SELECT track_id FROM favorites ORDER BY position")]
    finally:
        conn.close()
    return playlists, favorites


# -- Writing the destination account ----------------------------------------

def add_in_chunks(playlist, track_ids: list[int]) -> None:
    for start in range(0, len(track_ids), CHUNK_SIZE):
        chunk = track_ids[start:start + CHUNK_SIZE]
        for attempt in range(3):
            try:
                playlist.add(chunk, allow_duplicates=True)
                break
            except requests.exceptions.HTTPError as e:
                # 412 = stale etag; reload playlist and retry
                if e.response is not None and e.response.status_code == 412 and attempt < 2:
                    time.sleep(1)
                    playlist = playlist.session.playlist(playlist.id)
                else:
                    raise


def write_playlists(dst: tidalapi.Session, playlists: list[dict]) -> None:
    existing_names = {p.name for p in dst.user.playlists()}
    created = skipped = 0
    problems: list[str] = []
    for p in playlists:
        label = f"{p['name']!r} ({len(p['track_ids'])} tracks)"
        if p["name"] in existing_names:
            print(f"[skip] {label}: name already exists in destination")
            skipped += 1
            continue
        print(f"[copy] {label}")
        new = dst.user.create_playlist(p["name"], p["description"])
        add_in_chunks(new, p["track_ids"])
        final = dst.playlist(new.id).num_tracks
        created += 1
        if final != len(p["track_ids"]):
            problems.append(f"{p['name']}: expected {len(p['track_ids'])}, destination {final}")
    print(f"Playlists created: {created}, skipped: {skipped}")
    for line in problems:
        print(f"  [count mismatch, likely tracks unavailable in destination region] {line}")


def write_favorites(dst: tidalapi.Session, favorites: list[int]) -> None:
    already = {t.id for t in dst.user.favorites.tracks_paginated()}
    missing = [i for i in favorites if i not in already]
    print(f"{len(already)} already in destination, adding {len(missing)}")
    failed = [i for i in missing if not dst.user.favorites.add_track(i)]
    print(f"Favorites added: {len(missing) - len(failed)}")
    if failed:
        print(f"  [failed] {failed}")


def describe(playlists: list[dict] | None, favorites: list[int] | None) -> None:
    if playlists is not None:
        print(f"\n== Playlists: {len(playlists)} ==")
        for p in playlists:
            print(f"[dry-run] would copy {p['name']!r} ({len(p['track_ids'])} tracks)")
    if favorites is not None:
        print(f"\n== Favorites: {len(favorites)} ==")
        print("[dry-run] would copy favorites")


# -- Entry point ------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="print only: no backup, no destination login, no writes")
    parser.add_argument("--backup-only", action="store_true", help="read source and write the local DB; do not touch a destination")
    parser.add_argument("--from-backup", action="store_true", help="read the local DB instead of the source account")
    parser.add_argument("--db", default=DEFAULT_DB, help=f"backup SQLite file (default: {DEFAULT_DB})")
    parser.add_argument("--only-config", action="store_true", help="only playlists listed in config.yml")
    parser.add_argument("--config", default="config.yml")
    parser.add_argument("--skip-favorites", action="store_true")
    parser.add_argument("--skip-playlists", action="store_true")
    args = parser.parse_args()

    if args.from_backup and (args.backup_only or args.only_config):
        parser.error("--from-backup cannot be combined with --backup-only or --only-config")
    if args.backup_only and args.dry_run:
        parser.error("--backup-only and --dry-run are mutually exclusive")

    want_playlists = not args.skip_playlists
    want_favorites = not args.skip_favorites

    source_id = None
    if args.from_backup:
        playlists, favorites = load_backup(args.db)
        playlists = playlists if want_playlists else None
        favorites = favorites if want_favorites else None
        print(f"Loaded backup {args.db}")
    else:
        src = get_source()
        source_id = src.user.id
        print(f"Source user id: {source_id}")
        only_ids = config_playlist_ids(args.config) if args.only_config else None
        playlists, favorites, tracks = fetch_source(src, only_ids, want_playlists, want_favorites)
        if not args.dry_run:
            save_backup(args.db, playlists, favorites, tracks)

    if args.dry_run:
        describe(playlists, favorites)
        return
    if args.backup_only:
        return

    dst = get_dest()
    print(f"Destination user id: {dst.user.id}")
    if source_id is not None and dst.user.id == source_id:
        sys.exit("Source and destination are the same account. Aborting.")

    if playlists is not None:
        print(f"\n== Playlists: {len(playlists)} ==")
        write_playlists(dst, playlists)
    if favorites is not None:
        print(f"\n== Favorites: {len(favorites)} ==")
        write_favorites(dst, favorites)


if __name__ == "__main__":
    main()
