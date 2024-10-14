import os
from concurrent.futures import ThreadPoolExecutor
from fnmatch import fnmatch

from modules import util

logger = util.logger


class RemoveStalled:
    def __init__(self, qbit_manager):
        self.qbt = qbit_manager
        self.config = qbit_manager.config
        self.client = qbit_manager.client
        self.stats = 0

        self.remote_dir = qbit_manager.config.remote_dir
        self.root_dir = qbit_manager.config.root_dir
        self.orphaned_dir = qbit_manager.config.orphaned_dir

        max_workers = max(os.cpu_count() - 1, 1)
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.rem_stalled()
        self.executor.shutdown()

    def rem_stalled(self):
        self.stats = 100