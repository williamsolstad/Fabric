# -*- coding: utf-8 -*-
"""PySpark notebook cell for loading the legacy CSV dump into Delta."""

# COMMAND ----------
# Legacy CSV dump → LH_ClientDemo.A_Bronze (Delta)
from csv_dump_resource import (
    CSV_DUMP_BRONZE_TABLE,
    DEFAULT_PREVIEW_LIMIT,
    load_csv_dump_to_bronze,
    preview_csv_dump,
)

TOTAL_ROWS = load_csv_dump_to_bronze(overwrite=True)
print(
    f"Previewing the first rows from {CSV_DUMP_BRONZE_TABLE} "
    f"(total written: {TOTAL_ROWS})"
)
preview_csv_dump(limit=DEFAULT_PREVIEW_LIMIT)
