# Timezone Handling for ES|QL External Data Sources

## The Two Timezone Layers

Timestamp handling in any query engine involves two distinct layers:

**Interpretation timezone** — the timezone assumed when raw bytes or strings are converted into epoch values during data reading. This determines *what instant in time* a value like `2026-03-26T10:30:00` represents. It is applied once, at read time, and the result (epoch millis) is what gets stored, compared, and aggregated. This is the layer where the contentious decision lives.

**Presentation timezone** — the timezone used to display, format, round, and bucket epoch values for human consumption. `DATE_TRUNC(1 day, @timestamp)` rounds to midnight in the presentation timezone. `DATE_EXTRACT(HOUR, @timestamp)` extracts the hour in the presentation timezone. This never changes the underlying epoch value — it only affects how it appears in results. In ES|QL, this is `EsqlConfiguration.zoneId()`, set via the `time_zone` REST parameter or `SET time_zone`.

These two layers are independent. The interpretation timezone converts ambiguous input into a canonical UTC epoch. The presentation timezone converts that canonical epoch into a human-readable form. Conflating them — using the presentation timezone for interpretation — is the root of most timezone bugs across the industry.

## The Problem

Parquet, ORC, NDJSON, and CSV files may contain timestamp values without timezone information. When ES|QL reads these files via `EXTERNAL`, it must choose an interpretation timezone. This choice affects correctness, hot/cold consistency with ES indices, and interoperability with other engines reading the same files.

### How Parquet encodes timestamps

- **`isAdjustedToUTC=true`**: The int64 is an absolute instant (epoch millis/micros/nanos). All engines agree — no interpretation needed.
- **`isAdjustedToUTC=false`**: The int64 is a wall-clock reading with no timezone. `10:30:00` could be New York or Tokyo. This is where engines diverge.
- **INT96** (legacy Hive/Spark): No `isAdjustedToUTC` flag at all. Ambiguous by design.

The problem case is `isAdjustedToUTC=false`. Uncommon in cloud log data (CloudTrail, VPC Flow, Filebeat all write UTC), but present in enterprise exports, IoT data, and older Hadoop pipelines.

## What Competitors Do (Interpretation Layer)

### For `isAdjustedToUTC=false`:

| Engine | Interpretation behavior | Type produced | Presentation TZ affects reading? |
|--------|------------------------|---------------|--------------------------------|
| **Trino** | Raw int64 as-is, no conversion | `TIMESTAMP` (wall-clock type) | No |
| **DuckDB** | Raw int64 as-is, no conversion | `TIMESTAMP` (naive type) | No |
| **Spark 3.3+** (inferNTZ=on) | Raw int64 as-is | `TimestampNTZType` (wall-clock) | No |
| **Spark 4.0** (inferNTZ=off) | Raw int64 labeled as UTC | `TimestampType` (instant) | Display only |
| **ClickHouse** | Raw int64 labeled as UTC | `DateTime64(N, 'UTC')` | No |

### The architectural split

Engines that have a **wall-clock type** (Trino, DuckDB, Spark NTZ) can correctly represent `isAdjustedToUTC=false` — the value `10:30:00` stays `10:30:00` with no timezone, and the presentation layer doesn't touch it.

Engines with **only instant types** (ClickHouse, Spark default, Elasticsearch) must label the value as *some* timezone. They choose UTC — preserving the numeric value but corrupting the semantic meaning. ClickHouse's developers call this "less incorrect."

**Elasticsearch has only instant types** — `date` is always epoch millis. We face the same constraint as ClickHouse and Spark.

### Per-datasource or per-column interpretation timezone

| Engine | Per-datasource interpretation TZ? | Per-column interpretation TZ? |
|--------|-----------------------------------|-------------------------------|
| Trino | `hive.parquet.time-zone` (catalog-level, INT96 only) | No |
| DuckDB | No | No |
| Spark | No | No |
| ClickHouse | No | `DateTime64(3, 'TZ')` — but **presentation only**, not interpretation |
| Snowflake | No | No |

**No engine changes interpretation based on a per-datasource or per-column timezone setting.** ClickHouse's per-column TZ only affects how values are displayed, not how Parquet bytes are read.

## Elasticsearch Precedent

| Operation | Interpretation TZ | Presentation TZ |
|-----------|-------------------|-----------------|
| **Indexing** `"2026-03-26T10:30:00"` (no TZ) | **UTC** (hardcoded in `DateFormatters.from()`) | N/A |
| **DSL `range` query** `"gte": "2026-03-26"` | N/A (stored data already epoch millis) | `time_zone` param shifts query bounds |
| **DSL `date_histogram`** | N/A | `time_zone` param shifts bucket boundaries |
| **ESQL** `WHERE @timestamp > "2026-03-26"` | N/A (stored data already epoch millis) | `time_zone` shifts the string literal's interpretation |
| **ESQL** `DATE_TRUNC(1 day, @timestamp)` | N/A | `time_zone` determines where midnight falls |
| **ESQL** `TO_DATETIME("2026-03-26T10:30:00")` | Request TZ (via `configuration.zoneId()`) | N/A |

The pattern: **data entering Elasticsearch uses UTC for interpretation. The presentation timezone only affects query-side operations** — how literals are parsed, how rounding boundaries are placed, how values are formatted. It never changes what's stored.

The exception is `TO_DATETIME()` from a string literal, which uses the request timezone as interpretation. But this is a query-side operation (converting a user-provided string), not a data-reading operation.

## Hot/Cold Consistency

The critical scenario: hot data in ES, cold data on S3 as Parquet, queried together.

- ES indexes `"2026-03-26T10:30:00"` (no TZ) → interpretation TZ = **UTC** → stored as `2026-03-26T10:30:00Z` epoch millis.
- Well-written Parquet has `isAdjustedToUTC=true` → already epoch millis → consistent.
- Parquet with `isAdjustedToUTC=false` → if we use **UTC** for interpretation → same epoch millis as ES. **Consistent.**
- If we used **request TZ** for interpretation → different epoch millis than ES. A user in `America/New_York` would see the S3 data shifted 4-5 hours from the ES data for the same original timestamps. **Broken.**

## Recommendation

**Use UTC as the default interpretation timezone for all external sources.**

| Source | Interpretation behavior |
|--------|------------------------|
| `isAdjustedToUTC=true` (Parquet/ORC) | Read as UTC instant — all engines agree |
| `isAdjustedToUTC=false` (Parquet/ORC) | Treat raw value as UTC instant — same as ClickHouse, same as ES indexing |
| INT96 (legacy Parquet) | Treat as UTC — same as Iceberg connector in Trino |
| NDJSON/CSV strings without TZ | Parse as UTC — same as ES `DateFieldMapper` |
| Explicit TZ in string (`Z`, `+05:00`) | Always honored — overrides interpretation TZ |
| Path dates (`year=2025/month=03/`) | Always UTC — follows cloud partitioning standard |

**The presentation timezone (`time_zone` / `SET time_zone`) does not affect interpretation.** It controls `DATE_TRUNC`, `DATE_EXTRACT`, `DATE_FORMAT`, and string-to-date conversion in query literals — exactly as it does for ES indices today.

### Why this is the right default

1. **Consistent with ES indexing** — both use UTC for interpretation of timezone-less values.
2. **Deterministic** — same file, same query, different users → same results. Using request TZ for interpretation means different users get different epoch millis from the same data.
3. **Hot/cold safe** — data moving between ES and S3 produces consistent timestamps across the boundary.
4. **Matches cloud log reality** — 90%+ of cloud log data is UTC. The default should optimize for the common case.
5. **Separation of concerns** — interpretation (UTC, deterministic) is cleanly separated from presentation (user TZ, per-query).

### Trade-offs

| For UTC interpretation | Against |
|------------------------|---------|
| Consistent with ES indexing | If data is genuinely local, values are wrong by the TZ offset |
| Deterministic results | Diverges from Trino/DuckDB wall-clock semantics (but they have a type we don't) |
| Hot/cold consistency | Users from Spark/Trino may expect session TZ to affect reading |
| Matches 90%+ of cloud data | Enterprise data from non-UTC systems needs an escape hatch |

### Escape hatch for non-UTC data

For the minority case of genuinely local timestamps, a per-query interpretation override:

```
EXTERNAL "s3://..." WITH { "interpretation_timezone": "America/New_York" }
```

Applied at read time — converts local values to UTC epoch millis before any processing. After conversion, the data is indistinguishable from UTC-written data. The presentation timezone still controls display independently.

Whether to also support persistent interpretation timezone on datasource definitions (CRUD) is a separate decision. The argument for: configure once. The argument against: silent inconsistency when mixing datasources with different interpretation timezones in a single query. **Recommendation: per-query only for MVP. Evaluate per-datasource during Tech Preview based on user feedback.**

Per-column interpretation timezone (like ClickHouse's `DateTime64(3, 'TZ')`) is not recommended. No competitor uses per-column TZ for interpretation, and it adds complexity for a scenario (different columns in the same file with different timezones) that is vanishingly rare.
