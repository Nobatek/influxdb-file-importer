"""Import data from files into InfluxDB"""
import contextlib
import abc
import datetime as dt
from pathlib import Path
import json

import rx
import influxdb_client


__version__ = "0.5.0"


class InfluxDBFileImporterWriteError(Exception):
    """Error while writing to InfluxDB"""


class InfluxDBFileImporter(abc.ABC):
    """Base InfluxDB file importer class

    To make a custom importer, implement abstract methods:
    - load_metadata
    - parse_file
    """
    BATCH_SIZE = 5_000

    def __init__(self, database_cfg, files_cfg, import_cfg):
        self._database_cfg = database_cfg
        self._files_cfg = files_cfg
        self._import_cfg = import_cfg

    @contextlib.contextmanager
    def connection(self):
        """Provide InfluxDB client write_api"""
        retries = influxdb_client.client.write.retry.WritesRetry(
            total=3,
            backoff_factor=1,
            exponential_base=2,
        )
        with influxdb_client.InfluxDBClient(
            url=self._database_cfg["url"],
            token=self._database_cfg["token"],
            org=self._database_cfg["org"],
            retries=retries,
        ) as client, client.write_api(
            write_options=influxdb_client.client.write_api.SYNCHRONOUS
        ) as write_api:
            yield write_api

    @abc.abstractmethod
    def parse_file(self, csv_file_path, name, metadata):
        """Import data from one file

        Implementation should yield records.
        Config info may be passed in self._import_cfg.
        """

    @abc.abstractmethod
    def load_metadata(self, path, file_type):
        """Import metadata from description file

        file_type is provided as a hint as metadata description may differ
        accross files types.
        """

    def import_files(self, dry_run=False):
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
            """Get file mtime, None if file not found"""
            try:
                return file_path.stat().st_mtime
            except FileNotFoundError:
                return None

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

            # Get new files since last time
            # Files may disappear during the process (e.g. temp files),
            # so we remove any file path for which get_mtime returns None
            file_mtimes_paths = (
                (p, get_mtime(p)) for p in Path(data_files_dir).iterdir()
            )
            file_mtimes_paths = (
                (p, t) for (p, t) in file_mtimes_paths
                if (
                    (t is not None and t > last_mtime_ts) and
                    (not suffixes or p.suffix in suffixes)
                )
            )
            if not file_mtimes_paths:
                continue
            sorted_file_mtimes_paths = sorted(
                file_mtimes_paths, key=lambda tp: tp[1]
            )

            # Build records generator spanning on several files
            records = (
                r
                for f, _ in sorted_file_mtimes_paths
                for r in self.parse_file(f, name, metadata[config["type"]])
            )

            with self.connection() as write_api:

                # Write callback
                if not dry_run:
                    def _write(record):
                        # pylint: disable = cell-var-from-loop
                        """Write record to InfluxDB database"""
                        write_api.write(
                            bucket=self._database_cfg["bucket"],
                            record=record
                        )
                else:
                    _write = None

                # Exception callback
                def _on_write_error(exc):
                    raise InfluxDBFileImporterWriteError from exc

                # Feed records generator to write function
                # Reraise errors to exit if something went wrong
                batches = (
                    rx
                    .from_iterable(records)
                    .pipe(rx.operators.buffer_with_count(self.BATCH_SIZE))
                )
                batches.subscribe(
                    on_next=_write,
                    on_error=_on_write_error,
                )

            # Update last modification time in status file
            # Note that the timestamp is rounded in the process
            # so the last file may be imported again next time
            status[name]["last_mtime"] = dt.datetime.fromtimestamp(
                sorted_file_mtimes_paths[-1][1], tz=local_tz).isoformat()
            if not dry_run:
                with open(status_file, "w") as status_f:
                    json.dump(status, status_f, indent=2)
