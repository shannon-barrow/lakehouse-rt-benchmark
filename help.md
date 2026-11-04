# Lakehouse//RT Benchmark — Help & Troubleshooting

---

## Warehouse Issues

### Could not connect to warehouse
- The selected warehouse may be stopped or starting up. Wait 1–2 minutes and retry.
- Verify you have **CAN_USE** permission on the warehouse.
- Lakehouse//RT warehouses use type `REYDEN` in the API. If yours doesn't appear, it may not be Lakehouse//RT.

### Warehouse not appearing in the dropdown
- Click **⟳ Refresh** to reload the warehouse list.
- Only Lakehouse//RT warehouses matching the required size are shown. For example, selecting TPC-H SF1000 requires a **Large** warehouse — smaller ones are filtered out.
- The system-provisioned warehouse is excluded (it is PRO type, not REYDEN).

### "No Lakehouse//RT warehouses of size X found"
- Switch to **Create new** to provision a warehouse of the required size.
- Or change your workload selection to one that needs a smaller warehouse.

---

## Data Validation Issues

### "Schema needs to be created"
- The benchmark tables don't exist in the selected catalog yet.
- Click **Create Data & Run Benchmark** — this requires a **DBSQL warehouse** (non-Reyden) with write permissions.
- Data is created via `CREATE TABLE AS SELECT` from pre-existing source schemas.

### "Stats missing" warnings
- Table statistics are needed for optimal query planning.
- The app auto-fixes by running `ANALYZE TABLE ... COMPUTE STATISTICS` using the DBSQL warehouse.
- If auto-fix fails, the exact SQL commands appear in an expandable section — run them manually or ask a table owner.
- You can click **Run Anyway** to proceed without stats; results may be slightly suboptimal.

### "Clustering missing" warnings
- Large tables (e.g. `hits` for ClickBench, `lineitem`/`orders`/`customer` for TPC-H SF1000) need liquid clustering.
- The app applies clustering via `ALTER TABLE ... CLUSTER BY (...)` followed by `OPTIMIZE`.
- Requires write access via the DBSQL warehouse.

### DBSQL warehouse selector only appears when needed
- If all data validates successfully, the DBSQL picker is hidden.
- It only appears when tables need creation, stats fixes, or clustering fixes.

---

## Benchmark Execution Issues

### Queries failing with errors
- Check if the warehouse is still running — it may have auto-stopped during a long run.
- Permission errors usually mean your user doesn't have SELECT on the benchmark tables.
- Failed queries appear in the live progress table with status **error**.

### Benchmark seems slow
- **Rep 1 (Cold)** is expected to be significantly slower — it includes compilation, planning, and initial data loading.
- TPC-H SF1000 cold runs can take 10+ minutes per query on a Large warehouse.
- If warm runs (Rep 2–3) are also slow, check that table stats and clustering are properly applied.

### "Benchmark completed with some errors"
- Some queries failed while others succeeded. Check the results for queries with status **error**.
- Common causes: query timeout, warehouse stopped mid-run, permission issues on specific tables.

---

## Understanding Results

### Color coding
- **Cold (blue)** — first execution, includes all startup/compilation overhead.
- **Warm (red/orange)** — subsequent executions, represents steady-state performance.

### Charts explained
- **Total Execution Time by Workload** — aggregate wall-clock time per workload, cold vs warm.
- **End-to-End Time by Repetition** — total time for all queries in each repetition.
- **Per-Query Median Warm Latency** — median of warm reps per query; best indicator of steady-state performance.
- **Cold vs Warm by Query** — side-by-side comparison showing the cold penalty per query.

### Tips
- Use the workload tabs to view per-benchmark results.
- The **All Workloads** tab shows aggregate charts across all selected benchmarks.

---

## Methodology

| Parameter | Value |
|---|---|
| Repetitions | 3 (1 cold + 2 warm) |
| Result cache | Disabled (`use_cached_result = false`) |
| Measurement | Client-side latency via Statement Execution API |
| Execution | Sequential (one query at a time) or Parallel |

---

## Supported Workloads

| Workload | Queries | Data Size | Recommended WH Size |
|---|---|---|---|
| ClickBench | 43 | ~15 GB | Medium |
| TPC-H SF1 | 22 | ~1 GB | Small |
| TPC-H SF10 | 22 | ~10 GB | Small |
| TPC-H SF100 | 22 | ~100 GB | Medium |
| TPC-H SF1000 | 22 | ~1 TB | Large |

---

## Architecture

- **Authentication**: OBO (On-Behalf-Of) tokens — the app acts as the logged-in user via `X-Forwarded-Access-Token`.
- **Data location**: `{catalog}.benchmark_clickbench` and `{catalog}.benchmark_tpch_sfN`.
- **Warehouse sizing**: The app auto-selects the largest required size across all selected workloads.
- **Lakehouse//RT**: `warehouse_type = REYDEN` — the benchmark target (read-optimized).
- **DBSQL**: Used only for data creation and fixes (write operations).

---

## FAQ

**Can I run multiple benchmarks at once?**
Yes — select ClickBench and/or multiple TPC-H scale factors. They run sequentially with combined results.

**Why is the warehouse size auto-determined?**
The app picks the largest required size. Selecting TPC-H SF1 (Small) and SF1000 (Large) results in a Large warehouse.

**Can I compare Lakehouse//RT vs Serverless?**
Run the same workloads on different warehouse types in separate sessions and compare.

**Where are results stored?**
Results are displayed inline only. Persistence to a Delta table may be re-enabled in a future version.

**What if I don't have any Lakehouse//RT warehouses?**
Use **Create new** in the warehouse selector. You need CREATE WAREHOUSE permissions.
