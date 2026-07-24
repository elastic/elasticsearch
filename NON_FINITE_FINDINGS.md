# Non-Finite IEEE-754 Doubles in ES|QL — Investigation Findings

> **Status**: Throwaway investigation branch. Not intended to merge.  
> **Date**: 2026-07-22  
> **Enabling change**: `TO_DOUBLE` keyword/text overload now parses `NaN`, `Infinity`, `-Infinity`
> (via `EsqlDataTypeConverter.stringToDoubleAllowNonFinite`), gated on capability
> `to_double_non_finite`.  
> **How to reproduce**: All CsvIT results are in `non_finite_doubles.csv-spec` and run green.
>
> **Policy context**: ES|QL's historical design goal was that non-finite doubles must not appear in
> responses — the `asFiniteNumber` guards and null+warning pattern enforce this. SUM overflow
> producing `Infinity` was considered a bug, not a feature. That policy is now under review:
> ES|QL may move to fully supporting non-finite doubles. This investigation maps what would need
> to change and what already works correctly.

---

## Summary

ES|QL has broad but inconsistent defences against non-finite doubles.  
Most operators and functions **do** guard — they convert non-finite results to `null` + warning.  
But the guards are **per-function** (no central policy) and a few critical paths are **unguarded**,
including `MIN`/`MAX`, the `IS NULL`/`IS NOT NULL` predicates, and `MV_DEDUPE`.  
The `STATS BY` grouping key accepts `NaN` as a real group key, which is correct behaviour by
Java's `doubleToLongBits` semantics but surprising to users.

---

## Section 1 — Parsing (TO_DOUBLE)

| Input string   | Result after change | Before change |
|----------------|---------------------|---------------|
| `"NaN"`        | `NaN`               | `null` + warning |
| `"Infinity"`   | `Infinity`          | `null` + warning |
| `"-Infinity"`  | `-Infinity`         | `null` + warning |
| `"+Infinity"`  | `Infinity`          | `null` + warning |
| `"-0.0"`       | `-0.0`              | `-0.0` (unchanged; was already working) |
| `"foo"`        | `null` + warning    | `null` + warning (unchanged) |

`TO_STRING` round-trips correctly: `TO_STRING(NaN)` → `"NaN"`, `TO_STRING(Infinity)` → `"Infinity"`.
The `TO_DOUBLE(TO_STRING(NaN))` round-trip works end-to-end.

---

## Section 2 — IS NULL / IS NOT NULL

All non-finite doubles are real, non-null values. Both predicates behave correctly for all four cases:

| Expression                  | IS NULL | IS NOT NULL |
|-----------------------------|---------|-------------|
| `NaN IS NULL`               | `false` | `true` |
| `Infinity IS NULL`          | `false` | `true` |
| `-Infinity IS NULL`         | `false` | `true` |
| `-0.0 IS NULL`              | `false` | `true` |

**Assessment**: correct. Among databases that admit non-finite floats as first-class values
(PostgreSQL `FLOAT8`, Snowflake `FLOAT`, BigQuery `FLOAT64` — all confirmed to support NaN/Inf),
`IS NULL` correctly returns false for them; non-finite is not the same as absent. Databases that
reject non-finite floats entirely (MySQL, SQL Server) never encounter this case.

The only subtlety: ES|QL null semantics map to the absence of a value in a block slot, which is a
separate concept from IEEE-754 "not a number". `NaN` occupies a real slot with a real bit pattern —
it is never null.

---

## Section 3 — Arithmetic Operators (+, -, *, /)

**Key finding**: All four basic arithmetic operators (+, -, *, /) have post-result non-finite guards.
Any operation that *produces* a non-finite result (including NaN) emits `null` + warning.
The input values themselves pass through the evaluators, but the guard fires on the output.

| Expression            | IEEE-754 result | ES|QL result  | Warning                       |
|-----------------------|-----------------|---------------|-------------------------------|
| `NaN + 1.0`           | NaN             | `null`        | "not a finite double number: NaN" |
| `Infinity + 1.0`      | Infinity        | `null`        | "not a finite double number: Infinity" |
| `Infinity + (-Infinity)` | NaN          | `null`        | "not a finite double number: NaN" |
| `NaN * 0.0`           | NaN             | `null`        | "not a finite double number: NaN" |
| `Infinity * 0.0`      | NaN             | `null`        | "not a finite double number: NaN" |
| `Infinity - Infinity` | NaN             | `null`        | "not a finite double number: NaN" |
| `NaN / 1.0`           | NaN             | `null`        | "not a finite double number: NaN" |
| `-NaN`                | NaN             | `NaN`         | none (negation has no guard!) |
| `-(+0.0)`             | -0.0            | `-0.0`        | none |
| `-(-0.0)`             | +0.0            | +0.0          | none |

**Surprising**: Unary negation (`-d`) has **no** non-finite guard — `-NaN` passes through as `NaN`.

### Modulus `%`

`Mod.processDoubles` guards on the **result** being non-finite (any NaN or ±Infinity output → null +
warning "/ by zero"). Cases where the result is finite pass through without a warning.

| Expression       | Java/IEEE-754 result | ES|QL result | Warning |
|------------------|----------------------|-------------|---------|
| `NaN % 2.0`      | NaN                  | `null`      | "/ by zero" ⚠️ message is misleading — divisor is non-zero |
| `Infinity % 2.0` | NaN                  | `null`      | "/ by zero" ⚠️ message is misleading — divisor is non-zero |
| `1.0 % 0.0`      | NaN                  | `null`      | "/ by zero" ✅ message is accurate here |
| `1.0 % Infinity` | 1.0 (finite)         | `1.0`       | none ✅ |
| `-0.0 % 2.0`     | -0.0 (finite)        | `-0.0`      | none ✅ |

The "/ by zero" message is sourced from a single `ArithmeticException("/ by zero")` in `Mod.java:processDoubles`.
It is only accurate for the `x % 0.0` case. For `NaN % y` and `Infinity % y` (y ≠ 0) the cause is the
non-finite *input*, not a zero divisor — the message misleads anyone reading the warning in a log.

---

## Section 4 — Comparisons

The runtime evaluation path uses Java primitive `double` operations, which are fully IEEE-754 correct.
All ordered comparisons (`<`, `>`, `<=`, `>=`) with a NaN operand return `false`; `!=` returns `true`.

| Expression                    | Result  | Notes |
|-------------------------------|---------|-------|
| `NaN == NaN`                  | `false` | Correct IEEE-754 |
| `NaN != NaN`                  | `true`  | Correct IEEE-754 |
| `NaN > 1.0`                   | `false` | Correct IEEE-754 |
| `NaN < 1.0`                   | `false` | Correct IEEE-754 |
| `NaN >= 1.0`                  | `false` | Correct IEEE-754 |
| `NaN <= 1.0`                  | `false` | Correct IEEE-754 |
| `NaN >= NaN`                  | `false` | Correct IEEE-754 (self-comparison) |
| `NaN <= NaN`                  | `false` | Correct IEEE-754 (self-comparison) |
| `NaN == Infinity`             | `false` | Correct IEEE-754 |
| `NaN != Infinity`             | `true`  | Correct IEEE-754 |
| `NaN > Infinity`              | `false` | Correct IEEE-754 |
| `Infinity == Infinity`        | `true`  | Correct |
| `Infinity != -Infinity`       | `true`  | Correct |
| `-Infinity < Infinity`        | `true`  | Correct |
| `Infinity >= Infinity`        | `true`  | Correct |
| `-Infinity <= -Infinity`      | `true`  | Correct |
| `-0.0 == 0.0`                 | `true`  | Correct IEEE-754 |
| `Infinity > MAX_VALUE`        | `true`  | Correct |

**Caveat — Lucene pushdown path**: `EsqlBinaryComparison.translateOutOfRangeComparisons` has a NaN
short-circuit at line 584 that fires **only** when the left operand is an indexed `FieldAttribute`.
For the `!=` operator this would incorrectly return 0 rows (should return all rows, since everything
is `!= NaN`). This path cannot be triggered in practice today because non-finite doubles cannot exist
in an indexed `double` field.

**Important implication for WHERE**: `WHERE d == d` (where d=NaN) returns 0 rows because
`NaN != NaN` at the filter level. Any equality-based filter on a NaN column silently returns 0 rows.
`WHERE d IS NOT NULL` correctly passes NaN rows through.

---

## Section 5 — SORT Ordering

`SORT` uses `Double.compareTo` which defines: `-Inf < finite < Inf < NaN`.

| SORT ASC order (left=smallest)  |
|---------------------------------|
| -Infinity, -1.0, 1.0, Infinity, NaN |

NaN sorts **last** in ascending order (treated as the largest value). This is correct by Java's
`Comparable<Double>` contract but differs from IEEE-754 (where NaN has no defined order).
`-0.0` and `0.0` compare as equal in `SORT`, so their relative order is stable across seeds in a
single run but not guaranteed to be consistent.

---

## Section 6 — STATS Aggregations

### SUM
| Input              | Result   | Notes |
|--------------------|----------|-------|
| `[1, NaN, 2]`      | `NaN`    | NaN propagates through accumulation |
| `[1, Infinity, 2]` | `Infinity`| Infinity propagates |

**Key finding**: `SUM` does NOT have a non-finite result guard. Non-finite sums escape to the output.
This is **inconsistent** with the arithmetic operators (`+`, `-`, `*`, `/`), which all guard.
This is also the known existing inconsistency: SUM on overflow already produces Infinity.

### AVG
| Input              | Result   | Warning |
|--------------------|----------|---------|
| `[1, NaN, 2]`      | `null`   | "not a finite double number: NaN" |

AVG *does* have a non-finite result guard (unlike SUM). The guard fires and emits the warning.

### MIN / MAX
| Input              | Result    | Notes |
|--------------------|-----------|-------|
| `[Infinity, -Infinity]` | MIN=-Inf, MAX=Inf | Correct |
| `[1, NaN, 2]` (MIN) | `NaN`  | **No non-finite result guard on MIN/MAX** |
| `[1, NaN, 2]` (MAX) | `NaN`  | **No non-finite result guard on MIN/MAX** |

**Key finding**: MIN and MAX do NOT guard against non-finite results. They emit NaN directly.
This is inconsistent with AVG and SUM (overflow guards).

The aggregation unit tests confirm this: `MaxTests` fails for `-Infinity` inputs because the test
expects `null` (per the existing mapping in the test's expected-value builder) but the aggregator
returns `-Infinity`. Similarly for `MinTests`.

### WEIGHTED_AVG

`WEIGHTED_AVG` is implemented as a surrogate expression that decomposes into simpler aggregates.
The decomposition path depends on whether the `field` argument is a compile-time constant
(foldable):

- **Field is a constant literal** (e.g. `WEIGHTED_AVG(1.0, w)`): `WeightedAvg.surrogate()` detects
  the foldable field and short-circuits to `MvAvg(field)`. The `weight` argument is **discarded
  entirely**. A non-finite weight has no effect; the result is the field value itself with no
  warning.
- **Field is a runtime expression** (e.g. `WEIGHTED_AVG(d, 1.0)` where `d` is a column): the
  surrogate expands to `Div(Sum(field), Count(field))` when weight is foldable, or
  `Div(Sum(field * weight), Sum(weight))` when neither is foldable. A non-finite result at the
  final `Div` fires a guard → `null` + warning.

| Input (value, weight)                 | Result   | Decomposition |
|---------------------------------------|----------|---------------|
| `NaN` column, `1.0` literal weight    | `null` + warning | `Sum(NaN) / Count(NaN)` = NaN → guard |
| `Infinity` column, `1.0` literal weight | `null` + warning | `Sum(Inf) / Count(Inf)` = Inf → guard |
| `1.0` literal field, `NaN` column weight | **`1.0`** (no warning) | surrogate = `MvAvg(1.0)`, weight discarded |
| `1.0` literal field, `Infinity` column weight | **`1.0`** (no warning) | surrogate = `MvAvg(1.0)`, weight discarded |

The weight-ignored cases are a surprising silent correctness issue: a user who passes a non-finite
weight column while using a constant value field gets a plausible-looking result (the constant
itself) with no indication that the weight was unused.

**Assessment**: inconsistent with ES|QL's own `AVG` (for the constant-field path). Diverges from
all comparison databases (PostgreSQL, DuckDB, ClickHouse), which would propagate `NaN` through the
weighted average.

### VARIANCE / STD_DEV

`VARIANCE` and `STD_DEV` use Welford's online algorithm (`WelfordAlgorithm.java`). A NaN or
Infinity input makes `m2` non-finite. `VarianceStates.evaluateFinal()` checks
`Double.isFinite(m2) == false` and returns `newConstantNullBlock(1)` — a **silent null with no
warning**. There is no `driverContext.addWarning(...)` call in this path.

| Input              | Result       | Warning? |
|--------------------|--------------|---------|
| `[1, NaN, 2]`      | `null`       | **No** |
| `[1, Infinity, 2]` | `null`       | **No** |

This means the same input `[1, NaN, 2]` produces three different outcomes across three aggregations:
- `SUM` → `NaN` (no guard at all)
- `AVG` → `null` + warning (Div guard fires and reports)
- `VARIANCE` / `STD_DEV` → `null`, silently (guard fires but emits nothing)

### MEDIAN / MEDIAN_ABSOLUTE_DEVIATION

`Median.surrogate()` has two paths:
- **Field is foldable** (compile-time constant) → delegates to `MvMedian`, which sorts values and
  picks the middle — does NOT use TDigest, handles NaN gracefully (sorts NaN to the end; median of
  a single-element `[NaN]` list returns `NaN`).
- **Field is a runtime expression** (e.g. from `EVAL d = TO_DOUBLE(...)`) → the `FieldAttribute` is
  NOT constant-folded by the optimizer, so `field.foldable()` is `false` → delegates to
  `Percentile(field, 50)` → TDigest → crash.

For the typical query pattern (`ROW ... | EVAL d = ... | STATS MEDIAN(d)`), the field is NOT foldable
and MEDIAN crashes identically to PERCENTILE (confirmed empirically).

`MEDIAN_ABSOLUTE_DEVIATION` uses TDigest directly with no foldable shortcut — always crashes.

| Input      | MEDIAN | MEDIAN_ABSOLUTE_DEVIATION |
|------------|--------|--------------------------|
| `NaN`      | query failure (`IllegalArgumentException: Invalid value: NaN`) | same |
| `Infinity` | query failure | same |

### COUNT
NaN is not null, so `COUNT(d)` counts NaN rows. `COUNT(*)` unaffected.

### COUNT_DISTINCT

`COUNT_DISTINCT` uses HyperLogLog++ which hashes values via `doubleToLongBits`. NaN has a
canonical bit pattern (`0x7ff8000000000000` for Java's quiet NaN), so two NaN values hash
identically and are counted as **one** distinct value. NaN is not filtered out.

| Input                  | Result | Notes |
|------------------------|--------|-------|
| `[NaN, NaN, 1.0]`      | `2`    | NaN counted as 1 distinct; HLL++ exact for small cardinalities |
| `[NaN]`                | `1`    | Correct |

### VALUES

`VALUES` collects all values with no non-finite filtering. NaN inputs are preserved and returned
in the multivalue result. Order is unspecified (depends on segment order).

| Input         | Result  | Notes |
|---------------|---------|-------|
| `[NaN]`       | `NaN`   | Preserved unchanged |
| `[1.0, NaN, 2.0]` | `[1.0, NaN, 2.0]` (in some order) | No filtering; order unspecified |

### TOP

`TOP` uses `DoubleBucketedSort` with the same total order as `SORT` (NaN > +Infinity > finite > -Infinity). NaN inputs are retained and rank above Infinity in descending sort.

| Input                    | `TOP(..., 2, 'DESC')` | Notes |
|--------------------------|----------------------|-------|
| `[NaN, Infinity, 1.0]`   | `[NaN, Infinity]`    | NaN ranks first in DESC order |
| `[NaN, NaN, 1.0]`        | `[NaN, NaN]`         | Duplicate NaN values are preserved |

---

## Section 7 — STATS BY with Non-Finite Grouping Keys

`BlockHash` hashes double group keys via `doubleToLongBits`, so grouping identity is by bit pattern,
not by `==`. All non-finite values tested directly:

| Grouping key | Behaviour | Counterintuitive? |
|---|---|---|
| Multiple `NaN` values | All land in **one group** | Yes — `NaN == NaN` is false, yet they group together |
| Multiple `Infinity` values | All land in **one group** | No |
| Multiple `-Infinity` values | All land in **one group** | No |
| `Infinity` vs `-Infinity` | **Separate groups** | No |
| `-0.0` vs `0.0` | **Separate groups** | Yes — `-0.0 == 0.0` is true, yet they are separate groups |

The `-0.0` / `0.0` split is the sharpest inconsistency: equality says they're the same value, but
grouping says they're different. The NaN coalescence is the mirror: equality says they differ, but
grouping says they're the same.

**Reproduction note**: the two-group behaviour requires scalar group keys. `MV_EXPAND` the values
first so each row carries a scalar double, then `STATS BY` that column. Using a multivalue key
directly (e.g. `z = MV_APPEND(0.0, -0.0)` without `MV_EXPAND`) produces one group — the
multivalue-key path does not split per value. The separate-groups finding is confirmed for the
scalar path:
```
ROW xs = ["-0.0", "0.0"] | MV_EXPAND xs | EVAL d = TO_DOUBLE(xs) | STATS c = COUNT(*) BY d
```

---

## Section 8 — INLINE STATS

`INLINE STATS` with non-finite values follows the same aggregation behaviour as `STATS`.
The `BY` grouping key uses the same `BlockHash` / `doubleToLongBits` mechanism, confirmed for all
non-finite key types:

| BY key | Behaviour |
|---|---|
| `NaN` | All NaN rows grouped together |
| `Infinity` | All Infinity rows grouped together |
| `-Infinity` | All -Infinity rows grouped together |
| `-0.0` vs `0.0` | Separate groups |

The result column order in `INLINE STATS` puts the agg column first (before BY keys).

---

## Section 9 — MV_* Functions

| Function         | NaN behaviour | -0.0 vs 0.0 behaviour |
|------------------|---------------|------------------------|
| `MV_SUM`         | Propagates NaN | — |
| `MV_AVG`         | Propagates NaN | — |
| `MV_MIN`         | Position/path-dependent (see below) | — |
| `MV_MAX`         | Position/path-dependent (see below) | — |
| `MV_MEDIAN`      | NaN sorts to end; median of remaining values (see below) | — |
| `MV_DEDUPE(NaN)` | **Does NOT deduplicate** — `==` is false for NaN | Deduplicates -0.0 and 0.0 (equal by `Double.compareTo`) — but nondeterministic! |
| `MV_SORT`        | NaN is sorted last (ASC) | -0.0 and 0.0 compare equal |

**MV_MEDIAN NaN behaviour in detail**:

`MvMedian.finish(Doubles)` calls `Arrays.sort(doubles.values, 0, doubles.count)` then picks the
middle element. `Arrays.sort(double[])` puts NaN last (same total order as `Double.compareTo`).

| Input               | Result | Explanation |
|---------------------|--------|-------------|
| `[NaN, 1.0, 2.0]`   | `2.0`  | Sorted → `[1.0, 2.0, NaN]`; middle index 1 → 2.0 |
| `[NaN]`             | `NaN`  | Single element → NaN |
| `[2.0, NaN]` (even) | `NaN`  | Sorted → `[2.0, NaN]`; average = (2.0 + NaN)/2 = NaN |

For odd-count lists where NaN is the only non-finite value, `MV_MEDIAN` returns the finite median
(NaN sorts to the tail and the middle index falls on a finite value). For even-count lists, NaN in
the top-half position contaminates the average. This is subtly different from `MV_MIN`/`MV_MAX`
where NaN always propagates regardless of count.

Note: `MV_MEDIAN` (the scalar MV function) never touches TDigest. The `MEDIAN` *aggregation* can
crash via TDigest — these are two separate functions with different implementations.

**MV_MIN / MV_MAX NaN behaviour in detail**:

`MvMin.process` and `MvMax.process` delegate to `Math.min`/`Math.max`, which return NaN whenever
either argument is NaN. The `@MvEvaluator` iterates values as `current = process(current, v)`,
so NaN should propagate once encountered. However, there is an `ascending` fast-path: if internal
block metadata indicates the values are already sorted in ascending order (per `Double.compareTo`,
which sorts NaN last), the evaluator skips iteration and returns the first element directly.

Concretely for `MV_MIN`:

| Condition | Result |
|---|---|
| NaN is the only value | NaN (confirmed) |
| NaN is first (becomes initial accumulator) | NaN — propagates through all subsequent `Math.min` calls (confirmed) |
| NaN is not first, block not flagged ascending | NaN — propagates once `Math.min(current, NaN)` is reached |
| NaN is not first, block flagged ascending (fast-path) | Finite minimum — NaN silently skipped |

The fast-path / normal-path split depends on internal block metadata, not on the literal order in
a `ROW` or query result. It is not user-controllable. Tests where NaN is in a non-first position
use assertion wildcards (`{any}`) because either outcome is possible depending on which path fires.
`MV_MAX` has the same structure with `Math.max` instead.

**Critical finding — MV_DEDUPE inconsistency**:
- `NaN` values are NOT deduplicated (each NaN stays because `NaN == NaN` is false by `==`).
- `-0.0` and `0.0` ARE always collapsed to one value by MV_DEDUPE, because both dedup paths use
  `==` / `!=` on primitive doubles, and `-0.0 == 0.0` is `true`. The dedup always happens —
  what is implementation-defined is **which** value survives: `copyMissing` (small MV) keeps
  whichever comes first in block order; `copyAndSort` (large MV) calls `Arrays.sort`, which is a
  non-stable dual-pivot quicksort and does not guarantee a relative order for equal elements like
  `-0.0` and `0.0`. The test uses `{any}` to reflect this, not because dedup is random, but
  because the surviving value is not user-controllable.

---

## Section 10 — Conversions

### TO_STRING / round-trip

| Conversion                        | Input    | Result      | Notes |
|-----------------------------------|----------|-------------|-------|
| `TO_STRING(NaN)`                  | NaN      | `"NaN"`     | Correct |
| `TO_STRING(Infinity)`             | Infinity | `"Infinity"`| Correct |
| `TO_STRING(-Infinity)`            | -Infinity| `"-Infinity"`| Correct |
| `TO_DOUBLE(TO_STRING(NaN))`       | NaN → "NaN" → NaN | `NaN` | Round-trips! |

### TO_INTEGER (double → int)

`safeToInt(double x)` checks `x > Integer.MAX_VALUE || x < Integer.MIN_VALUE`. Both IEEE-754
comparisons return **false for NaN**, so the guard is silently bypassed. `Math.round(NaN)` returns
`0L` by JLS §5.1.3 (`(long) NaN = 0`), cast to `int` gives **0**.

| Input      | Result         | Notes |
|------------|----------------|-------|
| `NaN`      | **`0`** (no warning!) | Guard bypassed: NaN comparisons return false |
| `Infinity` | `null` + warning | `Infinity > MAX` = true → guard fires |
| `-Infinity`| `null` + warning | `-Infinity < MIN` = true → guard fires |
| `-0.0`     | `0`            | Correct |

**Key finding**: `TO_INTEGER(NaN)` silently returns `0` — a wrong answer with no diagnostic.
This is a **bug**: the range check happens to not catch NaN because `NaN > x` and `NaN < x` are
both false in IEEE-754. The fix is to add an explicit `Double.isNaN(x)` check (or use
`Double.isFinite(x)`) before the range comparison in `safeToInt(double)`.

**Cross-database comparison** (sourced):

| Database | `CAST(NaN AS INTEGER)` | Source |
|---|---|---|
| PostgreSQL | **ERROR** — explicit `isnan()` guard in `dtoi4` (int4, int2, int8 all checked) | [PostgreSQL source: float8_to_int4](https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/float.c) |
| DuckDB | **ERROR** (`TRY_CAST` → NULL) — NaN/Inf to integer cast throws; open JSON serialization bug (#17329) separate issue | [DuckDB issue #17329](https://github.com/duckdb/duckdb/issues/17329); [DuckDB issue #14905](https://github.com/duckdb/duckdb/issues/14905) |
| Spark SQL (ANSI / 4.0+) | **ERROR** (`TRY_CAST` → NULL) | [Spark JSON docs — allowNonNumericNumbers](https://spark.apache.org/docs/latest/sql-data-sources-json.html); [SPARK-38060](https://issues.apache.org/jira/browse/SPARK-38060) |
| Spark SQL (non-ANSI) | **0 or NULL** (unverified — Java `(int) Double.NaN` = 0 without explicit guard) | No confirmed primary source for exact Spark non-ANSI behavior |
| Trino | **ERROR** at Hive JSON file write (since release 469, Jan 2025); REST client API behavior for NaN in DOUBLE column not explicitly documented | [Trino PR #24558](https://github.com/trinodb/trino/pull/24558); [Trino release 469](https://trino.io/docs/current/release/release-469.html) |
| Snowflake | FLOAT **supports** NaN/Inf natively; JSON API returns them as strings (exact format unspecified in docs) | [Snowflake numeric types](https://docs.snowflake.com/en/sql-reference/data-types-numeric); [Snowflake SQL API response format](https://docs.snowflake.com/en/developer-guide/sql-api/handling-responses) |
| BigQuery | FLOAT64 **supports** NaN/Inf; REST API returns JSON strings `"NaN"`, `"+inf"`, `"-inf"` — confirmed by client library bug report | [google-cloud-ruby #3488](https://github.com/googleapis/google-cloud-ruby/issues/3488); [BigQuery FLOAT64 docs](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types) |
| **ES\|QL** | **0, no warning** — NaN bypasses `> MAX \|\| < MIN` guard because NaN comparisons always return false | This investigation |

The consensus for `CAST(NaN AS INTEGER)` is error (strict) or NULL (safe cast). Silent zero is
universally treated as a bug. ES|QL currently matches the class of pre-fix behavior seen in older
database versions. Note that Snowflake and BigQuery take the opposite approach on non-finite floats
generally: they treat NaN/Inf as first-class FLOAT values rather than errors.

### TO_LONG (double → long)

`safeDoubleToLong(double x)` has the identical structure to `safeToInt`: checks
`x > Long.MAX_VALUE || x < Long.MIN_VALUE`, which is also bypassed for NaN.

| Input      | Result         | Notes |
|------------|----------------|-------|
| `NaN`      | **`0`** (no warning!) | Same guard bypass as TO_INTEGER |
| `Infinity` | `null` + warning | Guard fires correctly |
| `-Infinity`| `null` + warning | Guard fires correctly |
| `-0.0`     | `0`            | Correct |

**Key finding**: `TO_LONG(NaN)` is the same bug as `TO_INTEGER(NaN)` — silently returns `0`.
The cross-database picture is identical to TO_INTEGER (see sourced table above).

### TO_UNSIGNED_LONG (double → unsigned_long)

`inUnsignedLongRange(double d)` checks `d >= 0 && d < MAX`. The `>= 0` comparison returns
**false for NaN** (unlike `> MAX` which also returns false but is reached second), so NaN is
**correctly rejected** here.

| Input      | Result         | Notes |
|------------|----------------|-------|
| `NaN`      | `null` + warning | `NaN >= 0` = false → guard fires correctly |
| `Infinity` | `null` + warning | `Infinity < MAX` = false → guard fires |
| `-Infinity`| `null` + warning | `-Infinity >= 0` = false → guard fires |

**Assessment**: `TO_UNSIGNED_LONG` handles NaN correctly by accident — the `>= 0` lower-bound
check catches NaN whereas `> MAX` would not. The `TO_INTEGER` and `TO_LONG` paths are genuinely
buggy. The correct fix for all three functions would be to use `Double.isFinite(x)` as a guard,
matching the pattern PostgreSQL (`isnan()` check) and DuckDB use.

---

## Section 11 — Conditional Functions

| Function              | Input | Result | Notes |
|-----------------------|-------|--------|-------|
| `COALESCE(NaN, 42.0)` | NaN   | `NaN`  | COALESCE correctly treats NaN as non-null |
| `CASE(NaN > 0, ...)` | NaN   | else branch | NaN > 0 is false → else branch taken |

### GREATEST / LEAST

Both functions iterate with `Math.max` / `Math.min`. Java's `Math.max(a, b)` checks `a != a`
(NaN self-inequality) first: if `a` is NaN it returns `a` immediately. If `b` is NaN and `a` is
not, then `a >= b` is false (NaN comparison) so it returns `b`. Either way, **NaN propagates
regardless of which position it occupies**.

| Expression | Result | Notes |
|---|---|---|
| `GREATEST(NaN, 1.0, 2.0)` | `NaN` | NaN is the seed; `a != a` guard returns NaN immediately |
| `GREATEST(1.0, NaN, 2.0)` | `NaN` | `Math.max(1.0, NaN)`: `1.0 >= NaN` is false → returns NaN |
| `GREATEST(1.0, 2.0, NaN)` | `NaN` | `Math.max(2.0, NaN)` → NaN on the last call |
| `GREATEST(NaN, Infinity)` | `NaN` | `a != a` guard fires; Infinity never seen |
| `GREATEST(Infinity, NaN)` | `NaN` | `Inf >= NaN` is false → NaN wins over Infinity |
| `GREATEST(-Infinity, NaN, Infinity)` | `NaN` | NaN poisons the accumulator mid-sequence |
| `GREATEST(Infinity, -Infinity, NaN)` | `NaN` | Accumulates to Infinity, then `Math.max(Inf, NaN)` → NaN |
| `GREATEST(Infinity, -Infinity, 0.0)` | `Infinity` | Standard ordering, no NaN |
| `LEAST(NaN, -1.0, -2.0)` | `NaN` | Same propagation logic via `Math.min` |
| `LEAST(-1.0, -2.0, NaN)` | `NaN` | `Math.min(-2.0, NaN)` → NaN |
| `LEAST(NaN, -Infinity)` | `NaN` | `a != a` fires; -Infinity never seen |
| `LEAST(-Infinity, NaN)` | `NaN` | `-Inf <= NaN` is false → NaN wins |
| `LEAST(Infinity, NaN, -Infinity)` | `NaN` | NaN poisons mid-sequence |
| `LEAST(Infinity, -Infinity, 0.0)` | `-Infinity` | Standard ordering, no NaN |

No warnings are emitted — GREATEST/LEAST have no non-finite result guard. NaN in, NaN out,
silently. This is consistent with the `MIN`/`MAX` aggregations and the arithmetic `+`/`-`/`*`
operators (all unguarded), and inconsistent with `AVG`, `WEIGHTED_AVG`, `DIV`, and `MOD`
(all guarded).

---

## Section 12 — Math Functions

| Function        | Input     | Result    | Guard type |
|-----------------|-----------|-----------|------------|
| `ABS(NaN)`      | NaN       | `NaN`     | No guard |
| `ABS(-Inf)`     | -Infinity | `Infinity`| No guard |
| `FLOOR(NaN)`    | NaN       | `NaN`     | No guard |
| `CEIL(NaN)`     | NaN       | `NaN`     | No guard |
| `FLOOR(Inf)`    | Infinity  | `Infinity`| No guard |
| `ROUND(NaN)`    | NaN       | **`0.0`** | Explicit `isNaN` early-return in `Maths.round` (line 23) |
| `ROUND(Inf)`    | Infinity  | `Infinity`| `middleResult == Infinity` → returns n (line 35) |
| `SQRT(NaN)`     | NaN       | `NaN`     | No guard on NaN input |
| `SQRT(-Inf)`    | -Infinity | `null`    | Negative input guard ("Square root of negative") |
| `LOG(NaN)`      | NaN       | `NaN`     | No guard |
| `LOG(Inf)`      | Infinity  | `Infinity`| No guard |
| `SIN(NaN)`      | NaN       | `NaN`     | No guard |
| `SIN(Inf)`      | Infinity  | `NaN`     | No guard — Java produces NaN, which passes through |
| `COS(NaN)`      | NaN       | `NaN`     | No guard |
| `COS(Inf)`      | Infinity  | `NaN`     | No guard — `Math.cos(Infinity)` = NaN (undefined in IEEE-754) |
| `SIGNUM(NaN)`   | NaN       | `NaN`     | No guard — `Math.signum(NaN)` = NaN |
| `SIGNUM(Inf)`   | Infinity  | `1.0`     | No guard needed — finite result |
| `SIGNUM(-Inf)`  | -Infinity | `-1.0`    | No guard needed — finite result |
| `POW(NaN, 2)`   | NaN       | `null` + warning | `asFiniteNumber(Math.pow(NaN,2)=NaN)` throws |
| `POW(2, NaN)`   | NaN exp   | `null` + warning | `asFiniteNumber(Math.pow(2,NaN)=NaN)` throws |
| `POW(Inf, 2)`   | Infinity  | `null` + warning | `asFiniteNumber(Math.pow(Inf,2)=Inf)` throws |
| `POW(NaN, 0)`   | NaN base, 0 exp | **`1.0`** | Java special case: `Math.pow(x,0)=1.0` for any `x`, incl. NaN |
| `EXP(NaN)`      | NaN       | `NaN`     | No guard — raw `Math.exp`, no `asFiniteNumber` |
| `EXP(Inf)`      | Infinity  | `Infinity`| No guard |
| `EXP(-Inf)`     | -Infinity | `0.0`     | No guard — `Math.exp(-Inf)=0.0` (finite, no issue) |
| `COSH(NaN)`     | NaN       | `null`    | Has non-finite result guard ("cosh overflow") |
| `SINH(NaN)`     | NaN       | `null`    | Has non-finite result guard ("sinh overflow") |
| `BUCKET(NaN,…)` | NaN       | `null`    | Has isFinite input guard |

**Pattern**: `ABS`, `FLOOR`, `CEIL`, `SQRT`, `LOG`, `SIN`, `COS`, `SIGNUM`, `EXP` have NO non-finite
result guards — non-finite inputs produce non-finite outputs (or NaN for `SIN(Inf)` and `COS(Inf)`).
`COSH` and `SINH` have guards (they overflow for large inputs, so NaN is caught too).
`POW` guards via `asFiniteNumber` (inherited from the arithmetic evaluator pattern).
`ROUND` is unique: it has an explicit `isNaN` check that short-circuits to `0.0` — the only math
function with this specific NaN → zero behavior. `BUCKET` guards on the input being finite.

**`ROUND(NaN) = 0.0` is a silent correctness issue**: the `Maths.round` implementation has an
early-return `if (Double.isNaN(nDouble)) return 0.0d`, added to avoid downstream overflow in
`Math.round`. The return value `0.0` is wrong (NaN is not 0) and no warning is emitted. This
contrasts with `FLOOR(NaN)` and `CEIL(NaN)` which correctly propagate NaN.

---

## Section 13 — SET approximation=true

The approximation machinery uses `Double.NaN` internally as a sentinel for "empty bucket"
in the bootstrap confidence-interval computation (`ApproximationPlan.java`).
Tests with only 3 rows never trigger approximation (need >= 10,000 rows), so approximation
tests with ROW/MV_EXPAND produce exact results and 0 CI columns in the output.

**Status**: No collision detected in the 3-row tests. A proper test would need a larger dataset.
The internal comment in `ApproximationPlan.java` ("these values stay inside here and ... never
reach the user") is correct for the expected path, but if user NaN can now enter the system,
the sentinel assumption may be violated in principle for larger inputs.

---

## Section 14 — Function Unit Test Results (Phase C)

### Scalar function tests (100% pass)

All scalar function tests pass with NaN/±Infinity inputs added. This means every scalar function
either:
1. Has explicit per-function test matchers that already expect `null` for non-finite results
   (e.g., `ToDegreesTests` maps non-finite to `null`), or
2. Has a correct non-finite result guard and produces `null`.

### Aggregation function tests (failures observed)

The following aggregation tests fail because their expected values assume non-finite inputs
produce `null`, but the actual aggregators return non-finite values:

| Test class | Failing case | Expected | Actual |
|------------|-------------|---------|--------|
| `AvgTests` | `NaN` / `±Infinity` input | `null` | NaN / ±Infinity (or vice versa) |
| `AvgOverTimeTests` | same | same | same |
| `MaxTests` | `-Infinity` input | `null` | `-Infinity` |
| `MaxOverTimeTests` | same | same | same |
| `MedianTests` | non-finite | `null` | non-finite (or crash) |
| `MedianAbsoluteDeviationTests` | non-finite | varies | varies |
| `PercentileTests` | non-finite | varies | varies |
| `PercentileOverTimeTests` | non-finite | varies | varies |
| `StdDevTests` | non-finite | `null` | non-finite |
| `StddevOverTimeTests` | same | same | same |
| `ValuesTests` | non-finite | varies | varies |
| `VarianceTests` | non-finite | `null` | non-finite |
| `VarianceOverTimeTests` | same | same | same |

**Key insight from MaxTests**: The test expects `-Infinity` (correctly derived by Java's `max()`)
but the actual aggregator returns `-MAX_VALUE` (i.e. `-1.7976931348623157E308`). This means the
MAX aggregator implementation **does not correctly handle -Infinity as a value to be compared** —
it likely initializes its accumulator to `-Double.MAX_VALUE` and never updates it to `-Infinity`.
This is a correctness bug: `MAX([-Infinity]) = -MAX_VALUE`, not `-Infinity`.

---

## Section 15 — PERCENTILE

`PERCENTILE` uses a TDigest sketch (`TDigestState` → `SortingDigest`). The TDigest `add()` method
calls `TDigest.checkValue(x)`, which throws `IllegalArgumentException("Invalid value: NaN/Infinity")` for any non-finite input. There is no try-catch in `QuantileStates.SingleState.add()` or in the generated `PercentileDoubleAggregatorFunction.addRawVector()`, so the exception propagates as a **hard query failure** — not a null+warning like AVG or SUM.

| Input                    | Result |
|--------------------------|--------|
| `PERCENTILE(NaN, 50)`    | `IllegalArgumentException: Invalid value: NaN` (query failure) |
| `PERCENTILE(Infinity, 50)` | `IllegalArgumentException: Invalid value: Infinity` (query failure) |
| `PERCENTILE(-Infinity, 50)` | `IllegalArgumentException: Invalid value: -Infinity` (query failure) |
| `PERCENTILE([1, NaN, 2], 50)` | `IllegalArgumentException: Invalid value: NaN` (one bad row kills the whole query) |

**Note**: `-0.0` is finite, so `PERCENTILE(-0.0, 50)` works fine (result: `0.0`).

The behavior diverges from every other aggregation:
- `SUM`/`MIN`/`MAX`: propagate non-finite silently.
- `AVG`/`WEIGHTED_AVG`: emit null + warning.
- `PERCENTILE`: hard query failure.

csv-spec has no mechanism to assert on hard query errors, so these cases cannot be tested in the
CsvIT battery; they are documented here only.

**Assessment**: hardest failure mode of any aggregation — severity HIGH. A single non-finite row in
a large dataset crashes the entire query rather than emitting a null with a warning. The fix would
add a non-finite check to `QuantileStates.add()` analogous to what `AVG` does.

---

## Section 16 — Cross-cutting architectural observations (from comparative review)

These findings were confirmed by code reading against `origin/main`.

### C3 — Pushdown `field != NaN` matches nothing (latent wrong-answer bug)

`EsqlBinaryComparison.fold()` has a special-case at the top of its constant-folding path
(`EsqlBinaryComparison.java:584`):

```java
if (Double.isNaN(((Number) value).doubleValue())) {
    return new MatchAll(source()).negate(source());  // match-none
}
```

This fires for **every** comparison operator — including `!=`. So a folded `field != NaN` becomes
match-none (0 rows), whereas IEEE-754 requires it to match every row (`x != NaN` is always true).

Currently unreachable from pure ES|QL (NaN is not a valid numeric literal), but reachable via PromQL
conversions or future features that produce NaN constant expressions. The code is consistently wrong
(match-none for all operators) rather than partially wrong — `field == NaN` accidentally gives the
correct answer (match-none), but for the wrong reason.

### D1 — Three output formats produce three different representations of the same non-finite value

Jackson's `JsonGenerator.Feature.QUOTE_NON_NUMERIC_NUMBERS` is **enabled by default**. Its Javadoc
states: *"Feature that determines whether 'exceptional' (not real number) float/double values are
output as quoted strings … Feature is enabled by default."* Elasticsearch's `JsonXContentImpl`
does not override this default. As a result:

| Format | Output for `Infinity` in a `double` column | RFC 8259 compliant? |
|---|---|---|
| **JSON (REST)** | quoted string `"Infinity"` (Jackson default) | Yes — but type contract broken: column says `double`, wire value is a string |
| **CSV / TSV** | bare token `Infinity` | N/A — not JSON; ES|QL's own CSV re-ingest would reject it |
| **Arrow / binary** | raw IEEE-754 bit pattern | N/A — not JSON; fully round-trips |

The JSON result is RFC 8259-compliant (JSON arrays are heterogeneous; `[1.234, "Infinity"]` is
valid JSON). But the semantic type contract is broken: a client that trusts the column metadata
(`"type":"double"`) and deserializes values accordingly will encounter a string where it expects a
number. This is what caused the `CsvAssert.ValueTransformer` failure in the multi-node REST
test — `new BigDecimal("Infinity")` throws, because the value arrived as a String rather than
a Number.

The CsvIT test path (Arrow/binary) is unaffected, which is why this never surfaced in the main
test battery.

**If ES|QL moves to full non-finite support, the wire-format question becomes a design decision.**
All RFC-compliant options involve a type mismatch for clients that trust column metadata:

| Approach | RFC valid? | Type contract? | Who uses this |
|---|---|---|---|
| Quoted strings `"Infinity"` (current Jackson default) | Yes | Broken | BigQuery (`"NaN"`, `"+inf"`, `"-inf"` — documented) |
| Bare literals `Infinity` (Jackson feature disabled) | **No** | Intact | PostgreSQL `row_to_json`, DuckDB (both have open bugs; DuckDB #17329, PostgreSQL mailing-list discussion) |
| `null` | Yes | Intact | Snowflake (errors instead, so no nulls) |
| Typed wrapper | Yes | Intact if clients agree | MongoDB Extended JSON v2 |

RFC 8259 § 6 states explicitly: *"Numeric values that cannot be represented in the grammar below
(such as Infinity and NaN) are not permitted."* So bare literals are non-conformant regardless of
parser tolerance.

BigQuery's approach (document that FLOAT64 columns return JSON strings for special values, and
require clients to handle them) is the most pragmatic precedent for a system that wants to keep
RFC-compliant output while fully supporting non-finites.

### E2 — `histogram_quantile` converts internal NaN to null on output

`PromqlHistogramQuantileStates.java:348-349` (and the grouping variant at :438-439) contains an
explicit comment: *"NaN signals 'no estimate'; emit null rather than placing NaN in a DoubleBlock,
where it would break equality."* The final evaluation converts any NaN result to a null block entry.

This is a deliberate design decision: the PromQL histogram quantile computation produces NaN for
degenerate inputs (empty buckets, no observations), and the aggregator explicitly suppresses it.
This is the correct policy for that function, but it means a user-supplied NaN flowing *into* the
computation would be silently misinterpreted as "no estimate" and suppressed.

### E3 — ES|QL can compute a value it cannot index back

`NumberFieldMapper.java:1026-1027`:
```java
if (Double.isFinite(value) == false) {
    throw new IllegalArgumentException("[double] supports only finite values, but got [" + value + "]");
}
```

A query returning `Infinity` (e.g. `SUM` on overflowing input) succeeds at query time. Any
write-back path — alerting rules, transforms, ENRICH, materialized views — that attempts to index
that result will fail with this error. There is no warning in the query response that the value is
unindexable.

### F1 — Prometheus StaleNaN cannot round-trip

The remote-write ingest path drops every non-finite sample (including the StaleNaN bit pattern
`0x7ff0000000000002` used for Prometheus staleness markers). The wire encoding additionally
canonicalizes all NaN bit patterns to a single representation, so distinct NaN payloads are lost
even before reaching the drop check. Prometheus staleness semantics are therefore unrepresentable
in ES|QL regardless of any decision about non-finite doubles in the query layer. This is a
cross-team concern, not an ES|QL-local fix.

---

## Summary of Issues Found

| Severity | Issue | Where |
|----------|-------|-------|
| **Bug** | `MAX([-Infinity])` returns `-MAX_VALUE` instead of `-Infinity` | `MaxTests`; MAX aggregator |
| **Inconsistency** | `SUM` emits non-finite results; arithmetic `+`/`-`/`*`/`/` converts them to null | Aggregation layer vs expression layer |
| **Inconsistency** | `AVG` has non-finite guard; `MIN`/`MAX` do not | Aggregation layer |
| **Inconsistency** | Unary `-d` has no non-finite guard (passes `NaN` through) | Expression layer |
| **Subtle bug** | `MV_DEDUPE` does not deduplicate NaN values (NaN ≠ NaN by ==) | MV functions |
| **Surprising** | MV_DEDUPE's -0.0 vs 0.0 dedup behaviour is nondeterministic | MV functions |
| **Surprising** | `STATS BY NaN` groups all NaN rows together (correct but counterintuitive) | BlockHash |
| **Surprising** | `STATS BY -0.0` and `STATS BY 0.0` are SEPARATE groups | BlockHash |
| **Missing guard** | `SIN(Infinity)` produces `NaN` without a warning | `Sin` function |
| **Misleading message** | `%` warning says "/ by zero" for `NaN % y` and `Infinity % y` (y≠0); only accurate for `x % 0.0` | `Mod.java:processDoubles` |
| **Potential** | User NaN may collide with internal NaN sentinel in `approximation` mode | ApproximationPlan |
| **Bug (HIGH)** | `PERCENTILE` hard-fails (uncaught `IllegalArgumentException`) for any non-finite input; one bad row kills the whole query | `TDigest.checkValue()`, `QuantileStates.add()` |
| **Bug** | `TO_INTEGER(NaN)` silently returns `0` — NaN bypasses the `> MAX \|\| < MIN` range guard because NaN comparisons always return false | `DataTypeConverter.safeToInt(double)` |
| **Bug** | `TO_LONG(NaN)` silently returns `0` — same guard bypass as `TO_INTEGER` | `DataTypeConverter.safeDoubleToLong(double)` |
| **Inconsistency (WEIGHTED_AVG)** | When `field` is a constant literal, `WeightedAvg.surrogate()` short-circuits to `MvAvg(field)` and silently discards a non-finite weight column | `WeightedAvg.surrogate()` |
| **Inconsistency** | `VARIANCE`/`STD_DEV` return null silently (no warning) for non-finite input; `AVG` emits a warning, `SUM` passes through — three behaviors on the same data | `VarianceStates.evaluateFinal()` |
| **Bug (HIGH)** | `MEDIAN` crashes on non-finite input via TDigest (for non-foldable fields, which is the common EVAL pattern). `MV_MEDIAN` (the scalar function) does NOT crash — it uses `Arrays.sort` which puts NaN last. | `QuantileStates.add()`, `TDigest.checkValue()` |
| **Bug** | `ROUND(NaN)` silently returns `0.0` instead of `NaN` — an explicit `isNaN` early-return in `Maths.round` intended to prevent overflow, with no warning emitted. `FLOOR(NaN)` and `CEIL(NaN)` correctly propagate NaN; `ROUND` is uniquely broken. | `Maths.java:23` |
| **Latent wrong-answer** | Pushdown folds `field != NaN` to match-none; IEEE-754 requires match-all. Currently unreachable from pure ES|QL but reachable via PromQL | `EsqlBinaryComparison.java:584` |
| **Serialization divergence** | Same non-finite value renders as `"Infinity"` (bare string) in JSON, `Infinity` token in CSV/TSV, and raw IEEE bits in Arrow — no consistent contract | JSON/CSV/Arrow output paths |
| **Silent sentinel collision** | `histogram_quantile` converts internal NaN to null; a user-supplied NaN is silently treated as "no estimate" | `PromqlHistogramQuantileStates.java:348` |
| **Compute/storage mismatch** | Queries can return `Infinity` (SUM overflow) but indexing it is rejected — any write-back path fails silently at index time | `NumberFieldMapper.java:1027` |
| **Unrepresentable (PromQL)** | Prometheus StaleNaN bit pattern is dropped at ingest and canonicalized on the wire; staleness semantics cannot round-trip | Remote-write ingest, wire encoding |
