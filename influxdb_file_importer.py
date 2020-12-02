"""Import data from files into InfluxDB"""
import contextlib
import abc
import datetime as dt
from pathlib import Path
import json

import influxdb_client


__version__ = "0.1.1"


class InfluxDBFileImporter(abc.ABC):
    """Base InfluxDB file importer class

    To make a custom importer, implement abstract methods:
    - load_metadata
    - import_file
    """
    BATCH_SIZE = 5_000

    def __init__(self, database_cfg, files_cfg, import_cfg):
        self._database_cfg = database_cfg
        self._files_cfg = files_cfg
        self._import_cfg = import_cfg
        self._client = None
        self._write_api = None

    @contextlib.contextmanager
    def connection(self):
        """Open (and close) an InfluxDB client and write_api"""
        self._client = influxdb_client.InfluxDBClient(
            url=self._database_cfg["url"],
            token=self._database_cfg["token"],
            org=self._database_cfg["org"],
        )
        write_options = influxdb_client.WriteOptions(
            batch_size=self.BATCH_SIZE)
        self._write_api = self._client.write_api(write_options=write_options)
        yield
        self._write_api.__del__()
        self._client.__del__()
        self._write_api = None
        self._client = None

    def write(self, record):
        """Write record to InfluxDB database"""
        self._write_api.write(
            bucket=self._database_cfg["bucket"],
            record=record
        )

    @abc.abstractmethod
    def import_file(self, csv_file_path, name, metadata):
        """Import data from one file

        Implementation should call `self.write` to write records.
        Config info may be passed in self._import_cfg.
        """

    @abc.abstractmethod
    def load_metadata(self, path, file_type):
        """Import metadata from description file

        file_type is provided as a hint as metadata description may differ
        accross files types.
        """

    def import_files(self):
        """Import data from all files"""
        data_base_dir = Path(self._files_cfg["data_base_dir"])
        status_file = self._files_cfg["status_file"]
        # Local TZ is used to store last mtime in status file
        # as aware datetime but in a usable TZ
        local_tz = dt.datetime.utcnow().astimezone().tzinfo

        metadata = {
            k: self.load_metadata(v["metadata"], k)
            for k, v in self._files_cfg["types"].items()
        }

        def get_mtime(file_path):
            return file_path.stat().st_mtime

        for name, config in self._files_cfg["data"].items():
            data_files_dir = data_base_dir / config["subdir"]
            suffixes = self._files_cfg["types"][config["type"]]["suffixes"]

            # Get last modification time from status file
            with open(status_file) as status_f:
                status = json.load(status_f)
            last_mtime = status.setdefault(name, {}).get(
                "last_mtime",
                dt.datetime(1970, 1, 1, tzinfo=local_tz).isoformat()
            )
            last_mtime_ts = dt.datetime.fromisoformat(last_mtime).timestamp()
            next_mtime_ts = last_mtime_ts

            # Import files
            with self.connection():
                file_paths = (
                    p for p in Path(data_files_dir).iterdir()
                    if (
                        (get_mtime(p) > last_mtime_ts) and
                        (not suffixes or p.suffix in suffixes)
                    )
                )
                for csv_file_path in sorted(file_paths, key=get_mtime):
                    self.import_file(
                        csv_file_path, name, metadata[config["type"]]
                    )
                    next_mtime_ts = max(
                        next_mtime_ts,
                        get_mtime(csv_file_path)
                    )

            # Update last modification time in status file
            # Note that the timestamp is rounded in the process
            # so the last file may be imported again next time
            status[name]["last_mtime"] = dt.datetime.fromtimestamp(
                next_mtime_ts, tz=local_tz).isoformat()
            with open(status_file, "w") as status_f:
                json.dump(status, status_f, indent=2)
