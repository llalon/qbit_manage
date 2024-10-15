import os
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor

from modules import util


class RemoveStalled:
    def __init__(self, qbit_manager):
        self.logger = util.logger

        self.qbt = qbit_manager
        self.config = qbit_manager.config
        self.client = qbit_manager.client
        self.stats = None

        self.db_file = os.path.join(self.config.default_dir, "stalled.db")
        self.setup_database()

        self.executor = ThreadPoolExecutor(max_workers=max(os.cpu_count() - 1, 1))

        self.logger.separator("Checking Stalled Torrents", space=False, border=False)

        future_collect = self.executor.submit(self.collect_torrent_info)
        future_action = self.executor.submit(self.action_stalled)

        future_collect.result()
        future_action.result()

        self.executor.shutdown()

    def get_connection(self):
        """Open a sqlite connection"""
        return sqlite3.connect(self.db_file)

    def setup_database(self):
        """Create required tables for sqlite data for tracking torrent history"""

        try:
            self.logger.debug(f"Connected to SQLite database at {self.db_file}")

            schema = """
                CREATE TABLE IF NOT EXISTS stalled_torrents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    torrent_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    state TEXT NOT NULL
                );
                """
            with self.get_connection() as db_conn:
                db_conn.execute(schema)
        except sqlite3.Error as e:
            self.logger.error(f"SQLite connection error: {e}")

    def action_stalled(self):
        """Action torrents based on the configuration"""
        self.logger.debug("Processing stalled torrents...")

        self.stats = 0

        with self.get_connection() as db_conn:
            db_conn.execute("SELECT * FROM stalled_torrents")

    def collect_torrent_info(self):
        """Collect and save torrent info with timestamps to database"""

        self.logger.debug("Collecting torrent info...")

        torrents = self.client.torrents_info()

        self.logger.print_line(f"Saving info for {len(torrents)} torrents", self.config.loglevel)

        insert_query = """
            INSERT INTO stalled_torrents (torrent_id, timestamp, state)
            VALUES (?, ?, ?)
            """

        timestamp = int(time.time())

        data = [(torrent.hash, timestamp, torrent.state) for torrent in torrents]
        try:
            with self.get_connection() as db_conn:
                db_conn.executemany(insert_query, data)
            self.logger.debug(f"Collected and inserted info for {len(data)} torrents")
        except sqlite3.Error as e:
            self.logger.error(f"SQLite insertion error: {e}")
