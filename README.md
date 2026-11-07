# Lakehouse//RT Benchmark App: Setup Guide

A Databricks App (Streamlit) that provisions TPC-H data, runs the 22 TPC-H queries against a
Lakehouse//RT warehouse (sequential or parallel, result cache disabled), and shows interactive
results. No notebooks or code are required to use it.

## What is in this package

| File | Purpose |
| --- | --- |
| `app.py` | The application |
| `benchmark_queries.py` | TPC-H and ClickBench query text |
| `app.yaml` | App start command (no environment-specific values) |
| `requirements.txt` | Python dependencies, installed automatically at deploy |
| `help.md` | In-app help, shown in the sidebar |
| `.streamlit/config.toml` | Theme |

---

## 1. Prerequisites (workspace admin)

1. **Unity Catalog** enabled on the workspace, with **serverless SQL** available.
2. **Lakehouse//RT enabled** on the workspace. Your Databricks account team enables this.
   To check: SQL Warehouses > Create, and confirm a Lakehouse//RT type is offered.
3. **Databricks Apps** enabled (Compute > Apps is visible).
4. **Apps user authorization (on-behalf-of user)** enabled. If it appears under
   Settings > Previews, turn it on. The app runs every query as the signed-in user, so
   this is required.
5. **TPC-H sample data** is readable. Run this on any SQL warehouse; it should return
   `tpch_sf1`, `tpch_sf10`, `tpch_sf100`, `tpch_sf1000`:
   ```sql
   SHOW SCHEMAS IN samples LIKE 'tpch*';
   ```
   If a scale factor is missing, that scale factor cannot be provisioned by the app.
6. **Outbound access to PyPI** from the app environment, so `requirements.txt` can install.
   If PyPI is blocked, configure a private package mirror for Apps.

## 2. Get the source into your workspace

1. In the workspace, go to **Workspace > your user folder (or Shared) > Create > Git folder**.
2. Git repository URL: `https://github.com/shannon-barrow/lakehouse-rt-benchmark`, provider **GitHub**.
   The repo is public, so no Git credential is needed to clone it.
3. Create the folder, for example `/Workspace/Users/<you>/lakehouse-rt-benchmark`.

To pick up later updates, open the Git folder and click **Pull**, then redeploy the app (step 3).

Alternatively, download the repo as a zip from GitHub and use Workspace > Import.

## 3. Create and deploy the app

### Option A: UI
1. Compute > Apps > **Create app** > **Create a custom app**.
2. Name: `lakehouse-rt-benchmark`. Compute size: **Medium** (recommended).
3. Under **User authorization**, add the scope **`sql`**. This is the only scope required.
4. Create, then click **Deploy** and select the Git folder from step 2.

### Option B: CLI
```bash
databricks apps create lakehouse-rt-benchmark
# Add the "sql" user-authorization scope in the UI (App > Edit > User authorization)
databricks apps deploy lakehouse-rt-benchmark \
  --source-code-path /Workspace/Users/<you>/lakehouse-rt-benchmark
```

Deployment takes about 1 to 2 minutes. Status should show **Running**.
After any source change, deploy again. Edits are not picked up automatically.

## 4. Permissions

The app runs **as the signed-in user** (on-behalf-of token). Every query, table creation,
and warehouse action uses that user's own permissions. The app's service principal is
**not** used to read or write data, so it needs **no** Unity Catalog grants.

Grant each benchmark user the following:

| Need | Grant |
| --- | --- |
| Open the app | App > Permissions > **CAN USE** |
| Run benchmarks | **CAN USE** on the Lakehouse//RT warehouse(s) |
| Provision data | **CAN USE** on one serverless/pro (non-Lakehouse//RT) SQL warehouse |
| Read sample data | `USE CATALOG` + `SELECT` on `samples` (granted to all users by default) |
| Create benchmark tables | On the target catalog: `USE CATALOG`, `CREATE SCHEMA`; after creation, `USE SCHEMA`, `SELECT`, `MODIFY` on the `benchmark_*` schemas |
| Create warehouses in-app (optional) | Workspace admin, or the unrestricted cluster creation entitlement |

Example SQL (replace `<catalog>` and `<group>`):
```sql
GRANT USE CATALOG, CREATE SCHEMA ON CATALOG <catalog> TO `<group>`;
-- after the first user provisions data, let others reuse it read-only:
GRANT USE SCHEMA, SELECT ON SCHEMA <catalog>.benchmark_tpch_sf100 TO `<group>`;
```

On first open, each user sees a one-time consent prompt that authorizes the app to use the
`sql` scope on their behalf. They must accept it.

## 5. Using the app

1. **Select benchmarks**: TPC-H SF1, SF10, SF100, and/or SF1000.
2. **Choose a warehouse**: pick an existing Lakehouse//RT warehouse, or create one. The app
   filters to the size the workload needs:

   | Workload | Data size | Warehouse size |
   | --- | --- | --- |
   | TPC-H SF1 | ~1 GB | Small |
   | TPC-H SF10 | ~10 GB | Small |
   | TPC-H SF100 | ~100 GB | Medium |
   | TPC-H SF1000 | ~1 TB | Large |

3. **Pick a catalog**: where `benchmark_tpch_sfN` schemas will live.
4. **Validate**: the app checks tables, row counts, and statistics. If data is missing, select
   a **non-Lakehouse//RT SQL warehouse** to provision it. Lakehouse//RT warehouses are read-only,
   so table creation must run on a standard serverless warehouse. Provisioning runs CTAS from
   `samples`, then OPTIMIZE (clustered tables), then ANALYZE, in parallel per table.
   SF1000 provisioning can take a significant amount of time; start it early.
5. **Run**: sequential or parallel, 3 repetitions (1 cold + 2 warm), result cache disabled.
6. **Results**: geomean, per-query latency, cold vs warm, and parallel wall-clock time.

## 6. Notes and limitations

* **ClickBench** expects source data at `samples.clickbench.hits`, which is not in the
  standard `samples` catalog. Use TPC-H only, unless `<catalog>.benchmark_clickbench.hits`
  has already been loaded separately (about 100M rows). The app validates and uses an
  existing table if one is present.
* Results are displayed in the app only and are not persisted to a table.
* If the dependency install fails on `databricks-sql-connector[kernel]>=4.9,<5`, change that
  line to `databricks-sql-connector>=4.0.0` and redeploy. The app automatically falls back
  to a slower connector mode for query execution.

## 7. Troubleshooting

| Symptom | Likely cause / fix |
| --- | --- |
| App page shows "Could not connect" | User authorization not enabled, or the `sql` scope is missing. Add it and redeploy |
| No warehouses in dropdown | User lacks CAN USE, or no Lakehouse//RT warehouse of the required size exists |
| Provisioning fails with permission error | User lacks `CREATE SCHEMA` on the catalog, or a Lakehouse//RT warehouse was chosen for writes |
| "row count != expected" | Partial earlier load. Use the fix option in validation, or drop the schema and re-provision |
| App fails to start after deploy | Check App > Logs. Usually a blocked PyPI install (see section 6) |

Open the **Help & Troubleshooting** panel in the app sidebar for more detail.
