# Databricks CDC Star Schema Engine

A config-driven data pipeline built on Databricks and Apache Spark that ingests flight-booking CSV files incrementally, cleans them with a Delta Live Tables (Lakeflow Declarative Pipelines) silver layer that applies CDC, and builds a gold star schema (dimension tables with surrogate keys plus an incremental fact table). The gold notebooks are parameterised so the same logic can be reused for different dimension and fact tables by changing configuration rather than code.

## Architecture

```
CSV files in a Unity Catalog volume (raw)
        │  Auto Loader (cloudFiles), availableNow trigger
        ▼
Bronze: Delta files per source (bookings, flights, customers, airports)
        │  DLT pipeline: expectations, type casting, AUTO CDC (SCD Type 1)
        ▼
Silver: silver_bookings, silver_flight, silver_passengers, silver_airports
        │  Config-driven notebooks: surrogate keys + Delta MERGE upserts
        ▼
Gold:   dimpassengers, dimflights, dimairports  →  factbookings
```

`SrcParameters.py` publishes the list of sources as a job task value (`output_key`), which suggests the bronze notebook is run once per source from a Databricks Job (for example a For Each task).

## Tech stack

- Databricks (notebooks, Jobs task values, Unity Catalog volumes)
- Apache Spark / PySpark and Spark SQL
- Auto Loader (`cloudFiles`) with schema evolution in `rescue` mode
- Delta Lake (`DeltaTable.merge`)
- Delta Live Tables / Lakeflow Declarative Pipelines (`dlt`, expectations, `create_auto_cdc_flow`)

## Repository structure

```
.
├── Setup.py                  # Creates the raw volume and source folders
├── SrcParameters.py          # List of sources, published as a job task value
├── BronzeLayer.py            # Auto Loader ingestion of one source (widget "src")
├── DLT_Bookings/             # DLT pipeline source
│   ├── transformations/DLTPipeline_silver.py  # Silver tables and CDC flows
│   ├── explorations/sample_exploration.py     # Ad-hoc exploration (not part of the pipeline)
│   └── utilities/utils.py                     # Email-validation UDF (template utility)
├── Gold/
│   ├── GOLD_DIMS.py          # Generic SCD Type 1 dimension builder
│   └── GOLD_FACT.py          # Incremental fact builder with dynamic dimension joins
└── Data/                     # Sample CSVs: initial, *_increment and *_scd files
```

## What each stage does

**Setup (`Setup.py`)** – creates the `workspace.raw.volume` volume and a raw folder per source, and contains a quick check query against the bronze flights data.

**Bronze (`BronzeLayer.py`)** – reads the `src` widget, streams CSV files from `/Volumes/workspace/raw/volume/rawdata/<src>/` with Auto Loader and appends them as Delta to `/Volumes/workspace/bronze/bronzevolume/<src>/data` using a checkpoint and an `availableNow` trigger, so each run only picks up new files.

**Silver (`DLT_Bookings/transformations/DLTPipeline_silver.py`)**
- Bookings: drops rows where `booking_id` or `passenger_id` is null (`expect_all_or_drop`), casts `amount` to `decimal(18,2)` and `booking_date` to date, adds `modifiedDate`, and writes `silver_bookings`.
- Flights, passengers (from the `customers` source) and airports: drop `_rescued_data`, add `modifiedDate`, and upsert into streaming tables with `create_auto_cdc_flow` keyed on `flight_id`, `passenger_id` and `airport_id`, sequenced by `modifiedDate`, stored as SCD Type 1.

**Gold dimensions (`Gold/GOLD_DIMS.py`)** – configured with catalog, source/target table, key columns, CDC column, surrogate key name and an optional backdated refresh date. It:
1. Finds the last load timestamp from the target (or `1900-01-01` on first load).
2. Reads source rows changed since then and left-joins them to the existing target on the business keys.
3. Keeps existing surrogate keys for matched rows (setting `update_date`) and generates new keys for new rows (`max key + monotonically_increasing_id() + 1`, with `create_date`/`update_date`).
4. Upserts with a Delta MERGE on the surrogate key, or creates the table on first run.

The notebook contains ready-made configuration blocks for `DimPassengers` and `DimAirports` (commented out) and an active block for `dimflights`.

**Gold fact (`Gold/GOLD_FACT.py`)** – reads `silver_bookings`, and from a list of dimension configs (table, alias, join keys, surrogate key) generates a SQL query that left-joins each dimension, selects `amount`, `booking_date`, `modifiedDate` and the three surrogate keys, and filters on `modifiedDate > last_load`. The result is merged into `workspace.gold.factbookings` on the surrogate key columns.

## Sample data

`Data/` holds CSV files for bookings, flights, passengers and airports. Each entity has an initial file, plus `*_increment.csv` (new records) and, for dimensions, `*_scd.csv` (changed records) that can be dropped into the raw volume to exercise incremental loads and CDC updates.

## How to run

Prerequisites: a Databricks workspace with Unity Catalog, a catalog named `workspace` with `raw`, `bronze`, `silver` and `gold` schemas, and the `workspace.bronze.bronzevolume` volume (the notebooks use these names directly).

1. Run `Setup.py` and upload the CSV files from `Data/` into `/Volumes/workspace/raw/volume/rawdata/<source>/` (`bookings`, `flights`, `customers`, `airports`).
2. Run `BronzeLayer.py` for each source, either manually with the `src` widget or from a job that iterates over the task value set by `SrcParameters.py`.
3. Create a DLT / Lakeflow pipeline from `DLT_Bookings/` targeting the `silver` schema and run it.
4. Run `Gold/GOLD_DIMS.py` once per dimension (switching the active configuration block), then `Gold/GOLD_FACT.py`.
5. To test incremental behaviour, upload the `*_increment.csv` and `*_scd.csv` files and re-run the steps.

## Possible improvements

- Pass the gold configuration as widgets or job parameters instead of editing commented blocks, and replace `eval()` on the key lists with a safer parser.
- `GOLD_DIMS.py` uses `DeltaTable` without importing `delta.tables`; adding the import makes the notebook self-contained.
- New surrogate keys are cast to string while existing keys keep their original type; keeping one numeric type avoids mismatches.
