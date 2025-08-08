# resource_usage_tracker.py
from robot.api import logger
from robot.api.deco import library
from robot.libraries.BuiltIn import BuiltIn
import re
import json
from pathlib import Path


class KeywordResourceMapper:
    def __init__(self):
        self.keyword_map = {}

    def register_keyword(self, keyword_name, handler_function):
        self.keyword_map[keyword_name] = handler_function

    def get_resource_metadata(self, keyword_name, args):
        handler = self.keyword_map.get(keyword_name)
        if handler:
            return handler(*args)
        return None


@library
class ResourceUsageTracker:
    ROBOT_LISTENER_API_VERSION = 3

    def __init__(self):
        self.current_test = None
        self.resource_map = {}
        self.mapper = KeywordResourceMapper()
        self._register_default_keywords()

    def _register_default_keywords(self):
        self.mapper.register_keyword("Use DB Table", lambda table: {"type": "DB_TABLE", "name": table})
        self.mapper.register_keyword("Use ADLS Path", lambda path: {"type": "ADLS_PATH", "name": path})
        self.mapper.register_keyword("Run Databricks Job", lambda job: {"type": "DATABRICKS_JOB", "name": job})

    def start_test(self, data, result):
        self.current_test = data.name
        self.resource_map[self.current_test] = []

    def start_keyword(self, data, result):
        if self.current_test:
            metadata = self.mapper.get_resource_metadata(data.kwname, data.args)
            if metadata:
                self.resource_map[self.current_test].append(metadata)

    def end_test(self, data, result):
        self.current_test = None

    def close(self):
        output_path = Path("resource_usage_map.json")
        with output_path.open("w") as f:
            json.dump(self.resource_map, f, indent=4)
        logger.info(f"Resource usage map written to {output_path}")


# Optional: Library keywords for use in tests
class ResourceUsageLibrary:
    def __init__(self):
        self.builtin = BuiltIn()

    def use_db_table(self, table_name):
        logger.info(f"[RESOURCE] DB_TABLE:{table_name}", also_console=True)

    def use_adls_path(self, path):
        logger.info(f"[RESOURCE] ADLS_PATH:{path}", also_console=True)

    def run_databricks_job(self, job_name):
        logger.info(f"[RESOURCE] DATABRICKS_JOB:{job_name}", also_console=True)
