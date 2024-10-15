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
        self.stats = 0

        self.db_file = os.path.join(self.config.default_dir, "stalled.db")
        self.db_conn = None

        self.setup_database()

        self.executor = ThreadPoolExecutor(max_workers=max(os.cpu_count() - 1, 1))
        self.collect_torrent_info()
        self.action_stalled()
        self.executor.shutdown()

        self.close_database()

    def setup_database(self):
        """Initializes SQLite database and creates table if it doesn't exist."""
        try:
            self.db_conn = sqlite3.connect(self.db_file)
            self.logger.debug(f"Connected to SQLite database at {self.db_file}")

            schema = """
            CREATE TABLE IF NOT EXISTS stalled_torrents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                torrent_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                state TEXT NOT NULL
            );
            """
            with self.db_conn:
                self.db_conn.execute(schema)
        except sqlite3.Error as e:
            self.logger.error(f"SQLite connection error: {e}")

    def close_database(self):
        """Closes the SQLite database connection safely."""
        if self.db_conn:
            try:
                self.db_conn.close()
                self.logger.debug(f"Connection to SQLite database at {self.db_file} closed")
            except sqlite3.Error as e:
                self.logger.error(f"SQLite connection error during close: {e}")

    def action_stalled(self):
        pass

    def collect_torrent_info(self):
        """Collect current torrent info with timestamp"""

        self.logger.separator("Checking Stalled Torrents", space=False, border=False)

        torrents = self.client.torrents_info()

        self.logger.print_line(f"Saving info for {len(torrents)} torrents", self.config.loglevel)

        insert_query = """
        INSERT INTO stalled_torrents (torrent_id, timestamp, state)
        VALUES (?, ?, ?)
        """

        timestamp = int(time.time())

        # Batch insertion to optimize performance
        data_to_insert = [(torrent.hash, timestamp, torrent.state) for torrent in torrents]
        try:
            with self.db_conn:
                self.db_conn.executemany(insert_query, data_to_insert)
            self.logger.debug(f"Collected and inserted info for {len(data_to_insert)} torrents")
        except sqlite3.Error as e:
            self.logger.error(f"SQLite insertion error: {e}")
