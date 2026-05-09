# `.scripts/run.sh` — Build all SDKs in Docker

The only host requirement is Docker. Source is COPYd into per-SDK images;
build artifacts land in `mount/<sdk>/`. All SDKs build and extract in
parallel — wall time is roughly the longest single SDK, not the sum.

## Databricks Setup

* Creds: [link](https://teams.microsoft.com/l/message/48:notes/1778344882444?context=%7B%22contextType%22%3A%22chat%22%2C%22oid%22%3A%228%3Aorgid%3Ab99a3530-636d-4621-8662-bc5c8022b125%22%7D)
* Databricks: [link](https://adb-7405615836070899.19.azuredatabricks.net/settings/workspace/identity-and-access/service-principals)
* Tutorial: [link](https://learn.microsoft.com/en-us/azure/databricks/ingestion/zerobus-ingest)
* Nice product demo: [link](https://www.databricks.com/resources/demos/tours/lakeflow/connector/zerobus-ingest)
* Launch blog: [link](https://community.databricks.com/t5/technical-blog/deep-dive-on-zerobus-ingest-now-ga/ba-p/148385)

One-time, in your Databricks workspace:

1. **Pre-create the test table.**

   When creating the catalog, **UNSELECT** the `Use default storage`

   In a SQL editor / notebook:

   ```sql
   CREATE TABLE zerobus.dbo.unit_test (
       device_name STRING,
       temp        INT,
       humidity    BIGINT
   ) USING DELTA;

   GRANT USE CATALOG    ON CATALOG zerobus     TO `<sp-application-id>`;
   GRANT USE SCHEMA     ON SCHEMA  zerobus.dbo TO `<sp-application-id>`;
   GRANT SELECT, MODIFY ON TABLE   zerobus.dbo.unit_test TO `<sp-application-id>`;
   ```

1. **Get your Zerobus endpoint.**:

   ```text
   # <workspace-id>.zerobus.<region>.azuredatabricks.net
   7405615836070899.zerobus.eastus.azuredatabricks.net
   ```

6. **Plug the values into `.env`.** Copy `.env.example` to `.env` at the
   repo root and fill in `ZEROBUS_SERVER_ENDPOINT`,
   `DATABRICKS_WORKSPACE_URL`, `ZEROBUS_TABLE_NAME`,
   `DATABRICKS_CLIENT_ID`, and `DATABRICKS_CLIENT_SECRET`.

Notes:

- The integration tests append rows; they don't truncate. Re-running
  accumulates data in the table — drop and recreate the table if you
  want a clean slate.
- Tests in a single suite share the table and assume serial execution
  against it; don't run the Java and TypeScript suites in parallel
  against the same `ZEROBUS_TABLE_NAME`.

## Usage

```bash
# Build every SDK (parallel)
/home/mdrrahman/zerobus-sdk/.scripts/run.sh

# Build a subset (parallel)
SDK=rust,python /home/mdrrahman/zerobus-sdk/.scripts/run.sh

# Single SDK
SDK=java /home/mdrrahman/zerobus-sdk/.scripts/run.sh
```

Valid `SDK` values: `rust`, `python`, `typescript`, `java`, `go`, `all`
(default: `all`).

## Tests

If `.env` exists at the repo root, tests run inside the containers.
If not, only **build + lint** runs and a notice is printed.

To enable the full test suite (Java + TypeScript integration tests
need real Databricks credentials):

```bash
cp .env.example .env
# then edit .env with your service principal credentials
/home/mdrrahman/zerobus-sdk/.scripts/run.sh
```

## Outputs

| SDK        | `mount/<sdk>/…`                                      |
| ---------- | ---------------------------------------------------- |
| rust       | `libzerobus_ffi.a`, `libzerobus_jni.so`, `zerobus.h` |
| python     | `*.whl`                                              |
| typescript | `*.node`, `index.js`, `index.d.ts`                   |
| java       | `zerobus-ingest-sdk-*.jar`                           |
| go         | `lib/linux_amd64/libzerobus_ffi.a`, `examples/`      |
