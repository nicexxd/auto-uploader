import os
import sys
import json
import time
import signal
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Set

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler

# -----------------------------
# Config & State Management
# -----------------------------

APP_NAME = "android-auto-uploader-pc-agent"
STATE_FILE = "state.json"


def get_app_base_dir() -> Path:
    if getattr(sys, "frozen", False):
        # Running as bundled exe
        return Path(sys.executable).resolve().parent
    # Running as script
    return Path(__file__).resolve().parent


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_env() -> Dict[str, str]:
    # Load .env from executable/script directory if present; fallback to CWD
    base_dir = get_app_base_dir()
    env_path = base_dir / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    else:
        load_dotenv()

    cfg = {
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "").strip(),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "").strip(),
        "AWS_DEFAULT_REGION": os.getenv("AWS_DEFAULT_REGION", "").strip(),
        "S3_BUCKET": os.getenv("S3_BUCKET", "").strip(),
        "S3_PREFIX": os.getenv("S3_PREFIX", "").strip(),
        "DESTINATION_DIR": os.getenv("DESTINATION_DIR", "").strip(),
        "POLL_INTERVAL_SECONDS": os.getenv("POLL_INTERVAL_SECONDS", "5").strip(),
        "DELETE_AFTER_DOWNLOAD": os.getenv("DELETE_AFTER_DOWNLOAD", "false").strip().lower(),
        "MAX_WORKERS": os.getenv("MAX_WORKERS", "4").strip(),
    }

    # Basic validation
    missing = [k for k, v in cfg.items() if k in {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_DEFAULT_REGION", "S3_BUCKET", "DESTINATION_DIR"} and not v]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}. Please create a .env file based on .env.example")

    # Normalize and cast
    try:
        cfg["POLL_INTERVAL_SECONDS"] = max(1, int(cfg["POLL_INTERVAL_SECONDS"]))
    except ValueError:
        cfg["POLL_INTERVAL_SECONDS"] = 5

    cfg["DELETE_AFTER_DOWNLOAD"] = cfg["DELETE_AFTER_DOWNLOAD"] in {"1", "true", "yes", "y"}

    try:
        cfg["MAX_WORKERS"] = max(1, int(cfg["MAX_WORKERS"]))
    except ValueError:
        cfg["MAX_WORKERS"] = 4

    # Normalize prefix: allow empty; remove leading '/'; ensure no leading os.sep impact
    prefix = cfg.get("S3_PREFIX", "")
    if prefix.startswith("/"):
        prefix = prefix[1:]
    cfg["S3_PREFIX"] = prefix

    # Normalize destination dir
    dest = Path(cfg["DESTINATION_DIR"]).expanduser().resolve()
    cfg["DESTINATION_DIR"] = str(dest)

    return cfg


class StateStore:
    """Simple JSON-based state store tracking downloaded objects by key->etag."""

    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.file_path = base_dir / STATE_FILE
        self._lock = threading.Lock()
        self._data: Dict[str, str] = {}
        self._load()

    def _load(self):
        ensure_dir(self.base_dir)
        if self.file_path.exists():
            try:
                self._data = json.loads(self.file_path.read_text(encoding="utf-8"))
            except Exception:
                logging.warning("State file corrupted; starting fresh")
                self._data = {}

    def save(self):
        with self._lock:
            tmp = self.file_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self._data, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(self.file_path)

    def has(self, key: str, etag: str) -> bool:
        with self._lock:
            return self._data.get(key) == etag

    def set(self, key: str, etag: str):
        with self._lock:
            self._data[key] = etag

    def prune(self, keep_keys: Set[str]):
        with self._lock:
            removed = [k for k in self._data.keys() if k not in keep_keys]
            for k in removed:
                self._data.pop(k, None)
            if removed:
                logging.debug(f"Pruned {len(removed)} state entries not seen in listing")


# -----------------------------
# S3 Client
# -----------------------------

class S3Client:
    def __init__(self, cfg: Dict[str, str]):
        session = boto3.session.Session(
            aws_access_key_id=cfg["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=cfg["AWS_SECRET_ACCESS_KEY"],
            region_name=cfg["AWS_DEFAULT_REGION"],
        )
        self.client = session.client(
            "s3",
            config=BotoConfig(retries={"max_attempts": 10, "mode": "standard"}),
        )
        self.bucket = cfg["S3_BUCKET"]
        self.prefix = cfg["S3_PREFIX"]

    def list_objects(self):
        paginator = self.client.get_paginator("list_objects_v2")
        kwargs = {"Bucket": self.bucket}
        if self.prefix:
            kwargs["Prefix"] = self.prefix
        for page in paginator.paginate(**kwargs):
            for obj in page.get("Contents", []):
                yield obj

    def download_to_temp(self, key: str, tmp_path: Path):
        ensure_dir(tmp_path.parent)
        self.client.download_file(self.bucket, key, str(tmp_path))

    def delete_object(self, key: str):
        self.client.delete_object(Bucket=self.bucket, Key=key)


# -----------------------------
# Core Logic
# -----------------------------

def s3_key_to_local_path(key: str, prefix: str, dest_dir: Path) -> Path:
    # Strip prefix if present
    rel = key
    if prefix and key.startswith(prefix):
        rel = key[len(prefix):]
        if rel.startswith("/"):
            rel = rel[1:]
    # Normalize any backslashes
    rel = rel.replace("\\", "/")
    # Ensure no traversal
    rel = os.path.normpath(rel)
    if rel.startswith(".."):
        raise ValueError(f"Unsafe key path: {key}")
    return dest_dir / rel


def worker_download(s3: S3Client, state: StateStore, cfg: Dict[str, str], obj: Dict) -> bool:
    key = obj["Key"]
    etag = obj.get("ETag", "").strip('"')

    if state.has(key, etag):
        logging.debug(f"Skip (already downloaded): {key}")
        return False

    dest_dir = Path(cfg["DESTINATION_DIR"])    
    target_path = s3_key_to_local_path(key, s3.prefix, dest_dir)
    tmp_path = target_path.with_suffix(target_path.suffix + ".part")

    try:
        logging.info(f"Downloading: s3://{s3.bucket}/{key} -> {target_path}")
        s3.download_to_temp(key, tmp_path)
        ensure_dir(target_path.parent)
        # Atomic replace
        tmp_path.replace(target_path)
        state.set(key, etag)
        state.save()
        if cfg["DELETE_AFTER_DOWNLOAD"]:
            try:
                s3.delete_object(key)
                logging.info(f"Deleted from S3 after download: {key}")
            except ClientError as e:
                logging.warning(f"Failed to delete {key} from S3: {e}")
        return True
    except Exception as e:
        logging.error(f"Failed to download {key}: {e}")
        # Cleanup partial
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass
        return False


stop_event = threading.Event()


def handle_signal(signum, frame):
    logging.info("Received termination signal. Shutting down...")
    stop_event.set()


for sig in (signal.SIGINT, signal.SIGTERM):
    try:
        signal.signal(sig, handle_signal)
    except Exception:
        # Not all platforms support all signals
        pass



def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        cfg = load_env()
    except Exception as e:
        logging.error(str(e))
        sys.exit(1)

    dest_dir = Path(cfg["DESTINATION_DIR"])    
    ensure_dir(dest_dir)

    # State directory under destination dir to keep alongside files
    state_dir = dest_dir / f".{APP_NAME}_state"
    ensure_dir(state_dir)
    state = StateStore(state_dir)

    # File logging to state directory (rotating)
    try:
        log_file = state_dir / "agent.log"
        fh = RotatingFileHandler(log_file, maxBytes=2 * 1024 * 1024, backupCount=3, encoding="utf-8")
        fh.setLevel(logging.INFO)
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        logging.getLogger().addHandler(fh)
    except Exception as e:
        logging.warning(f"Failed to initialize file logging: {e}")

    s3 = S3Client(cfg)

    interval = cfg["POLL_INTERVAL_SECONDS"]
    max_workers = cfg["MAX_WORKERS"]

    logging.info("Starting S3 polling loop...")
    logging.info(f"Bucket={s3.bucket}, Prefix='{s3.prefix}', Dest='{dest_dir}', Interval={interval}s, Workers={max_workers}")

    while not stop_event.is_set():
        try:
            objects = list(s3.list_objects())
            # Track keys seen to prune state (optional)
            seen_keys = {o["Key"] for o in objects}
            state.prune(seen_keys)

            # Filter objects needing download
            to_download = []
            for obj in objects:
                key = obj["Key"]
                etag = obj.get("ETag", "").strip('"')
                if not state.has(key, etag):
                    to_download.append(obj)

            if to_download:
                logging.info(f"Found {len(to_download)} new/updated object(s). Downloading...")
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = [executor.submit(worker_download, s3, state, cfg, obj) for obj in to_download]
                    for _ in as_completed(futures):
                        pass
            else:
                logging.debug("No new objects.")
        except ClientError as e:
            logging.error(f"S3 error: {e}")
            time.sleep(min(30, interval))
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
        finally:
            # Wait for next tick
            stop_event.wait(interval)

    logging.info("Exited main loop.")


if __name__ == "__main__":
    main()