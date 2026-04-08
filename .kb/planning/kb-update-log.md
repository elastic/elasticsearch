# KB Update Log

## 2026-04-07 (late): KB update — CRUD implementation plan

### Changes
- **#287** rewritten with full architectural plan (index abstraction explanation, SPI design, storage pattern, query integration, security deferral)
- **5 child issues created**: #451 (SPI), #452 (metadata+cluster state), #453 (datasource REST), #454 (dataset REST), #455 (EXTERNAL integration)
- All added to project board with estimates (0.5w, 0.75w, 0.5w, 0.5w, 0.5w) and Task type
- #287 estimate updated 2.0w → 2.75w
- Project MEMORY.md updated with CRUD implementation decisions

### Key implementation decisions captured
- New `DatasourceTypeProvider` SPI in `esql-datasource-spi` (separate from ExternalSourceFactory)
- `ValidatedSettings` record: plugin validates and classifies settings in one pass
- New `IndexAbstraction.Type.DATASET` (not reusing Type.VIEW)
- Views pattern for cluster state (DatasourceMetadata + DatasetMetadata as Metadata.ProjectCustom)
- Schema resolved fresh at query time (no caching in dataset definition)
- Secret masking on GET via secretFieldNames, code structured for encryption later

---

## 2026-04-07: Update KB + Push KB — Full reconciliation + ergonomics landscape + runtime evaluator research

### Code checked
- origin/main merged twice (0caad95d87f)

### Reconciliation: #189 (External Data Sources)
- 76 total sub-issues (36 open, 40+ closed)
- **#189 Est: 40.7w → 33.7w** (Parquet-MR path, open only)
- **Closed**: #280 (schema union-by-name, PR #145220), #332 (ORC type gaps, PR #145074), #414 (CRUD design done), #420 (Snappy Parquet, PR #145393), #413 (dup of #252)
- **Estimates reduced**: #251 (2→1w), #284 (2→0.25w), #286 (3→1w), #426 (2→1.5w)
- **Estimates increased**: #344 (0.5→0.75w)
- **New**: #439 (multi-schema optimizer gaps, 5 children, ~3.75w total — deep dived all 5)
- **Key stale claims fixed**: Parquet strategy decided (was "under investigation"), ORC type gaps all closed (was "DECIMAL open"), #280 done (was "open")

### New deep dives (written to /tmp/, not yet in .kb/)
- knn() function end-to-end (pushdown-only, ExactKnnQueryBuilder brute-force but hardwired to Lucene)
- match()/full-text function hierarchy (LuceneQueryEvaluator, no evaluator fallback)
- TEXT_EMBEDDING (async operator, GA 9.4.0, returns DENSE_VECTOR)
- RERANK (pure inference, works on any data)
- _score/scoring (Lucene-only production, RERANK replaces it)
- Vector functions (V_COSINE etc. — all have evaluators, pure compute)
- MemoryIndex in ES (27 files, percolator is closest precedent for runtime match)
- semantic_text field type (TO_SEMANTIC concept — complex, index-time inference)
- #142554 TooComplexToDeterminizeException (20+ infix wildcards in _source.includes + vector fields)

### Bugs filed
- elastic/elasticsearch#145799 — source loader discards field projection when vector fields excluded (introduced by PR #138013). Fix + tests written locally.

### New KB reviews
- `.kb/reviews/esql-ergonomics-review-2026-04-07.md` — 8 Philipp Kahr issues
- `.kb/reviews/esql-ergonomics-landscape-2026-04-07.md` — full landscape (15 tagged + 51 untagged candidates)

### Global memory promoted
- `esql-evaluator-principle.md` — pushdown vs evaluator principle, current state of all functions
- `esql-ergonomics.md` — relaxed mode vision, core fixes, biggest clusters

### Project MEMORY.md updated
- Project structure section rewritten with Apr 7 reconciliation data
- Type support issues all closed
- Parquet strategy decided (was "under investigation")
- Key findings updated with runtime evaluator principle, keyless auth OIDC, Parquet TZ bug
- KB reviews section added

---

## 2026-03-30: Update KB + Push KB — Reconciliation + CRUD design iteration

### Code checked
- origin/main merged (03b6f2dad51)

### Reconciliation: #189 (External Data Sources)
- 68 total sub-issues (37 open, 31 closed)
- **#379 closed** — aggregate pushdown, PR #144828 merged Mar 27
- Changes since Mar 25: #282, #331, #353 closed by others (correctly). #284 moved to M2.
- New issues created: #409 (Parquet tuning settings), #410 (NDJSON reader settings)
- **#296 assigned to Johannes** (jfreden) — secrets impl excluded from our team's rollup
- Rollup: 40.7w remaining (37 open), 11.6 cal weeks @3.5 engineers
  - M1: **Done** (0 open)
  - M2: 17.0w our team (4.9 cal weeks)
  - M3: 11.0w (3.1 cal weeks)
  - M4: 9.7w (2.8 cal weeks)

### Reconciliation: #35 (LIMIT BY)
- 18 sub-issues (12 open, 6 closed)
- **#233 SHOULD-CLOSE** — all planning rules reviewed, tested, working
- **#237 SHOULD-CLOSE** — GroupedTopNBenchmark delivered (PR #144003)
- **#262 NEEDS-UPDATE** — 2/10 items done (telemetry, approximation), 6 open, 2 partial
- Notable: no reduce plan for TopNByExec/LimitByExec, still snapshot-only, no FORK interaction tests
- 9 items correctly open

### CRUD design updates
- Payloads flattened (no `settings` bag — all fields at top level)
- `_update` endpoint removed (deferred)
- POST with auto-ID removed (deferred)
- Alternatives Considered section added (11 options documented)
- Requirements section added before Mental Model
- Lifecycle section added (CRUD layer vs plugin responsibilities)
- Comment feedback from Google Doc addressed (injection, SSRF, privilege phasing, DLS/FLS)
- Parquet (#409) and NDJSON (#410) settings issues created

### Promoted to memory
- CRUD design decisions updated in MEMORY.md

---

## 2026-03-28: Update KB + Push KB — CRUD design finalized with feedback

### Code checked
- origin/main — already up to date

### Validation
- CRUD design: 6/7 claims confirmed. Minor: ConnectorIndexService has 15 updateConnector* methods (not 16+).
- Connector secrets: confirmed plaintext keyword field with `index: false` in `.connector-secrets`
- No native connector execution in Java — confirmed zero external system connections in ent-search plugin

### CRUD design updates (Alex's feedback incorporated)
- `type` always required (no inference) — `s3`, `gcs`, `azure`, `iceberg`, `jdbc`, `flight`
- Flat predefined fields per type — no opaque `config`/`credentials` bags. Server-driven secret detection.
- PUT+ID primary, POST+auto-ID alternative
- Cribl API comparison table validated (3 behavioral differences: secret redaction, merge semantics, auto-ID)
- Google Doc v4 generated with appendix

### Research completed
- Cribl Search API deep dive (endpoint-by-endpoint comparison, PATCH=full-replace, no secret redaction, no auto-ID)
- ES Connector architecture deep dive (control plane only, Python data plane, `.elastic-connectors-v1` storage, `ConnectorConfiguration.sensitive` is UI metadata not encryption)
- S3 connector workflow (self-managed only in 9.0+, content extraction pipeline)
- Potential connector-datasource unification discussed (shared connection layer, deferred)

### Promoted to memory
- CRUD design decisions summary added to MEMORY.md KB index

---

## 2026-03-28: Update KB + Push KB — CRUD design, RBAC, timezone

### Code checked
- origin/main — already up to date

### Validation
- CRUD design claims: 10/10 confirmed (inference _update, transform _update, SecureString, FileSourceFactory.type(), ManageRolesPrivilege, manage_connector exclusions)

### New KB documents
- `datasource-crud-design.md` — complete CRUD API design with 11 decisions captured

### Key decisions captured
1. URL namespace: `_query/datasource` + `_query/dataset`
2. Secrets: same PUT body, split internally
3. Storage: cluster state (encrypted credentials, AES-256-GCM, SecureString at query time)
4. Dataset privileges: existing index-level (read/manage)
5. Datasource privileges: global/configurable per Johannes' model (manage, read_metadata, read scoped to name patterns)
6. Type: optional on datasource, default "file", required for Iceberg/JDBC/Flight
7. All credentials on datasource, nothing on dataset
8. `config` (readable) + `credentials` (write-only, encrypted) JSON structure
9. PUT = full replace, _update = merge (secrets preserved if absent)
10. Composable datasources: long-term, not MVP
11. Per-datasource scoping from day one via configurable cluster privileges

### Promoted to memory
- CRUD design document added to MEMORY.md KB Deep Dives index

### Research completed (supporting CRUD design)
- ES CRUD API precedent analysis (inference, connectors, transforms, pipelines, templates)
- Cribl data model (connections/datasets/datatypes workflow)
- All 10 plugin property inventories (S3, GCS, Azure, HTTP, Parquet, ORC, NDJSON, CSV, Iceberg, Flight)
- Competitor secret storage comparison (11 systems)
- Johannes' security proposal review

---

## 2026-03-28: Update KB + Push KB — RBAC, timezone, secrets research

### Code checked
- origin/main merged (294a6f34f17)

### Validation
- RBAC reference document: 10/10 claims confirmed against codebase
- Recent RBAC changes on main: PR #144685 (automaton optimization for literal app privs), PR #144313 (Alerting V2 privileges). No impact on reference accuracy.

### New KB documents created
| Document | Content |
|----------|---------|
| `elasticsearch-rbac-reference.md` | Full ES RBAC model: mental model, privilege types, role structure, DLS/FLS, automaton-based authorization, implementation call chain, how features register privileges, built-in roles |
| `timezone-handling-recommendation.md` | Interpretation TZ vs Presentation TZ analysis. UTC-by-default for reading. Competitor deep dives (Trino, DuckDB, Spark, ClickHouse). |

### Knowledge promoted to memory
| Knowledge | Destination |
|-----------|-------------|
| KB deep dive index (4 documents) | `MEMORY.md` new "KB Deep Dives" section |
| CCS+EXTERNAL confirmed non-issue | `MEMORY.md` command support (already done Mar 25) |
| Command support fully updated (match/kql/knn fixed) | `MEMORY.md` (already done Mar 25) |

### Research completed (not yet in KB docs)
- Competitor secret storage comparison (11 systems)
- Cribl data model (connections/datasets/datatypes)
- Johannes' security proposal (`_query/connector`, `_query/dataset`) reviewed
- ClickHouse Named Collections deep dive

### Unchanged
- `arrow.md` — current
- `external-sources.md` — current (updated Mar 25)
- `secrets-storage.md` — current

---

## 2026-03-25: Update KB + Push KB — Post-reconciliation sync

### Code checked
- origin/main merged — already up to date (68b4b97a946)

### Validation (2 agents)
- **MEMORY.md**: 10/10 claims confirmed (plugins, filter pushdown, rate limiting, I/O pool, CCS+EXTERNAL, multi_match removal, PR #144852 merged, @AwaitsFix removed)
- **external-sources.md**: 10/10 claims confirmed. One minor omission: `SourceMetadata.sourceMetadata()` method not listed → fixed.

### Corrections applied

| File | Change |
|------|--------|
| `external-sources.md` | Added `sourceMetadata()` to SourceMetadata interface methods |
| `MEMORY.md` Command Support | Updated: match/kql/knn/qstr/match_phrase now produce clear analyzer errors (PR #144852). RERANK works. score() works. Broken list now accurately describes current state. |
| `MEMORY.md` Project Structure | Full rewrite reflecting reconciliation: 60 sub-issues, 41.75w remaining, 8 closed, 4 new created, 2 re-parented, dependencies flagged, milestone counts |

### Promoted from session to memory
- Command support status fully updated (match/kql/knn fixed by FullTextFunction.fieldVerifier())
- CCS+EXTERNAL confirmed non-issue (grammar, PreAnalyzer separation)
- RERANK confirmed working with EXTERNAL (pure ML inference, no Lucene dep)
- Reconciliation skill created and executed (merged "Reconcile issues" + "Update issues")
- 5 new skills formalized: Do a rebase, Local PR review, Create companion PR, Read Google Doc, Export to Google Doc

### Unchanged
- `arrow.md` — current
- `secrets-storage.md` — current
- `secrets-storage-deep-dive.md` — current (validated earlier this session)

---

## 2026-03-25: Push KB — Promote new architecture knowledge to global memory

### Corrected in global memory

| File | Claim | Fix |
|------|-------|-----|
| `external-sources.md` | FormatReader signature | Updated to `read(StorageObject, FormatReadContext)` — old signature was stale (pre-FormatReadContext refactor) |
| `external-sources.md` | SegmentableFormatReader methods | `readSegment()` → `findNextRecordBoundary()` + `minimumSegmentSize()` |
| `external-sources.md` | RangeAwareFormatReader methods | `readRange()` → `discoverSplitRanges()` + `readRange()` (two methods, not one) |
| `external-sources.md` | Plugin count | "13+" → "17+" (added Brotli, LZ4, Snappy, compression-libs) |
| `secrets-storage-deep-dive.md` | ConsistentSettingsService algorithm | "PBKDF2 hashes" → "PBKDF2WithHmacSHA512, 5000 iterations" |

### Promoted from KB to memory

| Knowledge | Destination | Source |
|-----------|-------------|--------|
| Format-specific filter pushdown (Parquet bloom/stats, ORC SearchArgument, RECHECK semantics, FilterPushdownRegistry) | `external-sources.md` new section | Update KB agent findings, PRs #144832 + #144686 |
| Production readiness infrastructure (cloud rate limiting, I/O pool isolation, byte-based backpressure) | `external-sources.md` new section | Update KB agent findings, PRs #144734 + #144596 + #144218 |
| PushStatsToExternalSource EVAL/RENAME extension | `external-sources.md` optimizer rules table | PR #144806 |
| FilterPushdownSupport implementations | `external-sources.md` data types section | Code review |
| FormatReadContext/FormatSpec/SourceMetadata detailed signatures | `external-sources.md` data types section | SPI agent verification |
| compression-libs shared plugin pattern | `external-sources.md` plugin table | New changes agent |

### New files
None — all promotions went into existing `external-sources.md`

### Unchanged
- `arrow.md` — current, no new Arrow-related changes on main
- `secrets-storage.md` — current (created this session)
- `skills.md` — procedures, no code claims
- `MEMORY.md` — already updated during Update KB step

---

## 2026-03-25: Update KB — Full validation + new main changes

### Code checked
- origin/main merged into `esql/pipeline-breaker-distribution-gap` (807f2c750a8..2cd1cbbc687)

### Validation scope
4 parallel agents: SPI interfaces (14 claims), distribution/optimizer (12 claims), secrets infrastructure (12 claims), new main changes

### Validation results

**SPI interfaces**: 14/14 CONFIRMED — all match current code
**Distribution/optimizer**: 12/12 CONFIRMED — all match current code
**Secrets infrastructure**: 11/12 CONFIRMED, 1 SHIFTED:

| Claim | Was | Now | Status |
|-------|-----|-----|--------|
| ConsistentSettingsService hash algorithm | "PBKDF2 hashes" (ambiguous) | PBKDF2WithHmacSHA512, 5000 iterations | SHIFTED → fixed in deep dive |

### New changes on main (since Mar 11)

| Change | PR | Impact |
|--------|----|--------|
| **Parquet bloom filter + statistics pushdown** | #144832 | `ParquetFilterPushdownSupport` — row-group skipping via stats, dictionary, bloom. RECHECK semantics. May close #368 (row-group filter pushdown, 1.5w). |
| **ORC predicate pushdown** | #144686 | `OrcFilterPushdownSupport` — file/stripe/row-group skipping via SearchArgument API |
| **Stats pushdown past EVAL/RENAME** | #144806 | `PushStatsToExternalSource` extended to handle intermediate EvalExec/ProjectExec with alias resolution |
| **Cloud API rate limiting** | #144734 | Three-layer defense (semaphore, throttle-aware retry, adaptive backoff). New settings. |
| **External source I/O pool isolation** | #144596 | Routes external I/O through `esql_worker` pool, not SEARCH |
| **GCS native async I/O** | #144733 | `GcsStorageObject.readBytesAsync()` via ReadChannel |
| **New compression codecs** | — | Brotli, LZ4, Snappy + `esql-datasource-compression-libs` shared plugin |
| **ViewUnionAll plan node** | #143564 | Subclass of UnionAll, differentiates views from subqueries |
| **LookupLogicalOptimizer** | — | New dedicated logical optimizer for LOOKUP operations |
| **multi_match removed** | #144512 | No longer in ESQL |

### Items potentially closeable on #189

| Issue | Title | Evidence |
|-------|-------|----------|
| #368 | Parquet row-group filter pushdown (1.5w) | `ParquetFilterPushdownSupport` merged (#144832) — need to verify if this fully covers the issue |

### MEMORY.md updates
- Added 5 new main changes: Parquet/ORC filter pushdown, cloud rate limiting, I/O pool isolation, stats pushdown past EVAL
- Plugin count updated (added Brotli, LZ4, Snappy, compression-libs)
- `multi_match` noted as removed from ESQL
- ConsistentSettingsService algorithm corrected in secrets deep dive

---

## 2026-03-17: Update KB + Push KB — Secrets deep dive validation + global memory sync

### Code checked
- origin/main merged into `esql/pipeline-breaker-distribution-gap` — already up to date

### Secrets Storage Deep Dive Validation (7 parallel agents)

Full validation of `.kb/deep-dives/ga/secrets-storage-deep-dive.md` against current codebase.

**Critical errors found and fixed:**

| Claim | Was | Now | Impact |
|-------|-----|-----|--------|
| TokenService key size | "AES-256-GCM" throughout | **AES-128-GCM** (`PBEKeySpec(..., 128)` at line 2224) | Document now correctly describes TokenService as 128-bit; our SecretsService will use 256-bit |
| Gateway persistence scope | "master-eligible nodes only" | **ALL master + data nodes** (`GatewayMetaState.java:147`: `isMasterNode \|\| canContainData`) | Auto-bootstrap threat model weakened — key co-locates with `.secrets` shards on data nodes |

**Factual errors fixed:**

| Claim | Was | Now |
|-------|-----|-----|
| Secret settings class count | "7 classes" | 6 (EmptySecretSettings has no secrets) |
| keystore.seed consistency | "registered as consistent" | NOT marked `Property.Consistent` — independent per node |
| ESQL WITH syntax | `WITH (key="value")` | `WITH { "key": "value" }` (JSON object) |
| Datasource plugin count | "3 plugins" | 5+ (S3, Azure, GCS, HTTP, Iceberg) |
| CustomSecretSettings line | 76 | 77 |
| KeyStoreWrapper.upgrade() line | 301 | 302 |

**Threat model impact:** Auto-bootstrap path (`Metadata.ClusterCustom` GATEWAY-only) provides weaker separation than originally described — key on all data nodes, not just masters. Still protects against snapshot exfiltration and system-index browsing. Recommendation unchanged (Option A: Shared SecretsService).

### Other KB/Memory Validation

**MEMORY.md corrections:**
- Branch name: `esql/connector-spi-v3` → `esql/pipeline-breaker-distribution-gap`
- FormatReader SPI: removed `readSplit()` claim — parameters now bundled into `FormatReadContext` record
- Added secrets storage deep dive summary to Key Findings

**New memory topic file:** `memory/secrets-storage.md` — encryption patterns, key bootstrap, gateway persistence, SecretsService proposal

**Verified (no changes needed):**
- SPI interfaces (ExternalSourceFactory, ConnectorFactory, TableCatalog, etc.) — all confirmed
- Distribution pipeline (SplitProvider → SplitDiscoveryPhase → AdaptiveStrategy → DataNodeComputeHandler) — correct
- 13 datasource plugins — all present
- Arrow 18.3.0 — confirmed
- Iceberg metadata-only — confirmed
- Byte-based backpressure in AsyncExternalSourceBuffer — confirmed

### KB planning doc spot-checks
- `v1-planning.md` — accurate
- `v1-current-state-and-audit.md` — one stale claim: response serialization for FloatBlock already handled (PositionToXContent:250, ResponseValueUtils:204). Low impact.
- `primitive-types-deep-dive.md` — PlannerUtils line 492 shifted to 497. Low impact.

---

## 2026-03-17: Update KB — Reconciliation + local doc updates

### Code checked
- origin/main merged into `esql/pipeline-breaker-distribution-gap`

### Validation scope
4 parallel agents validated: SPI interfaces, codebase changes, planning doc claims, #189 sub-issue reconciliation

### Newly closed items found (not previously tracked as closed)

| Issue | Title | Est | Evidence |
|-------|-------|-----|----------|
| #341 | Text format parallel parsing | 0.5w | `SegmentableFormatReader` SPI merged, CLOSED on GitHub |
| #342 | Parquet row-group parallelism | 1.0w | `RangeAwareFormatReader` SPI merged (PR #144018), CLOSED |
| #334 | CSV type support gaps | 1.8w | All 4 MVP CSV type issues fixed, CLOSED |
| #337 | Parquet type support gaps | 4.0w | All 7 MVP Parquet type issues fixed, CLOSED |
| #289 | Byte-based memory safety | 2.0w | `AsyncExternalSourceBuffer` byte-based backpressure (PR #144218), CLOSED |

### New untracked work on main

| Work | PR | Description |
|------|----|-------------|
| Stats-via-metadata | #143940 | `PushStatsToExternalSource` optimizer rule — COUNT(*)/MIN/MAX from file metadata |
| WeightedRoundRobin | *(part of distribution)* | LPT algorithm for size-aware split assignment |
| Byte-based backpressure | #144218 | Buffer tracks `ramBytesUsedByBlocks()` not just page count |
| RangeAwareFormatReader | #144018 | New SPI for Parquet row-group splitting |

### Local documents updated

| Document | Changes |
|----------|---------|
| `.kb/planning/v1-current-state-and-audit.md` | Fixed stale claims: LIMIT pushdown now merged, buffer now byte-based, distribution now has WeightedRoundRobin, Parquet reader now columnar with row-group parallelism, buffer backpressure fixed, distributed execution tests exist |
| `.kb/planning/v1-planning.md` | Added 5 newly closed items to Already Merged table; updated Remaining Gaps (CSV/Parquet types done, byte-based memory done); recalculated estimates (32.0w remaining, ~9.1 cal weeks) |
| `.kb/planning/v1-product-shape.md` | Marked TP-4 (LIMIT pushdown), TP-7 (CSV/TSV), GA-8 (distributed aggregation) as DONE; marked TP-10 as partially done |
| `memory/MEMORY.md` | Updated project structure (13 closed items, 32.0w remaining), type-support status (CSV/Parquet done), performance gaps (stats-via-metadata merged), new SPI interfaces |

### Estimate impact
- Previous remaining: 41.3w
- Newly closed: 9.3w (0.5 + 1.0 + 1.8 + 4.0 + 2.0)
- New remaining: **32.0w** at 3.5 engineers = **~9.1 calendar weeks**

---

## 2026-03-11: Update KB — Full validation + #189 reconciliation

### Code checked
- origin/main merged into `esql/pipeline-breaker-distribution-gap` — already up to date

### Validation scope
4 parallel agents validated: SPI interfaces, Arrow claims, MEMORY.md codebase claims, #189 sub-issue reconciliation

### Corrections applied to MEMORY.md

| Claim | Was | Now | Status |
|-------|-----|-----|--------|
| Column pruning perf gap | "column projection (PruneColumns doesn't handle ExternalRelation)" in Performance gaps section | Removed — PruneColumns DOES handle ExternalRelation via `pruneColumnsInExternalRelation()` (merged as #294, already listed in CLOSED items) | STALE → fixed |
| STATS distributed perf gap | "STATS distributed (single-phase)" in Performance gaps section | Removed — two-phase aggregation implemented (INITIAL/FINAL modes in Mapper, merged as #277) | STALE → fixed |
| CCS + EXTERNAL guard | "`ExternalRelation.writeTo()` throws UnsupportedOperationException" | `writeTo()` does normal serialization now; old guard removed | WRONG → fixed |

### #189 Sub-Issue Reconciliation

**25 OPEN issues**: All confirmed correctly open — no hidden merges found.

**8 CLOSED issues**: 6 verified present on main, 2 flagged:

| Issue | Finding | Evidence |
|-------|---------|----------|
| #281 (Arrow allocator CB) | **Closed prematurely** — code only in unmerged PR #142981 (still OPEN). No `arrowAllocator()` or AllocationListener bridge on main. | `gh pr view 142981` → state: OPEN |
| #293 (Intra-file parallelism) | **Partially done** — infrastructure exists (ParallelParsingCoordinator, SegmentableFormatReader) but `parallelRead()` not wired into production code (AsyncExternalSourceOperatorFactory). Parquet row-group parallelism also unimplemented. | `grep parallelRead` → only in ParallelParsingCoordinator.java + tests |

### Verified (no changes needed)

**SPI Interfaces (10/10 verified)**:
- ExternalSourceFactory (6 methods), ConnectorFactory, TableCatalog, Connector, SplitProvider, ExternalSplit, FilterPushdownSupport, SourceMetadata, QueryRequest — all match
- OperatorFactoryRegistry dispatch, ExternalRelation/ExternalSourceExec, distribution pipeline — all correct
- FormatReader: rowLimit, ErrorPolicy, readSplit, FormatSpec, SegmentableFormatReader — all verified

**Arrow (10/10 verified)**:
- Version 18.3.0, three usage areas (output format, Flight, Iceberg), memory gap (unlimited RootAllocator)
- Dependency management (ExcludeAllTransitivesRule, AllocationManagerShim), gRPC 1.78.0 pinned — all correct

**MEMORY.md codebase claims (9/10 verified)**:
- All 13 plugins exist, all 4 storage backends, all merged features confirmed
- LIMIT pushdown, distributed execution, splittable BZIP2, bracket MV CSV — all on main
- AsyncExternalSourceBuffer page-count tracking, FormatReader → Page output — correct

## 2026-03-06: Push KB — LOOKUP JOIN promotion + corrections (f25ff476e7d)

### Code checked
- origin/main at f25ff476e7d (12 ESQL commits since last push)

### Corrections applied to global memory (`~/.claude/memory/esql-architecture.md`)

| Claim | Was | Now | Status |
|-------|-----|-----|--------|
| Lookup plan node base class | `BinaryPlan` (line 116 of transformation nodes table) | `UnaryPlan` with SurrogateLogicalPlan (surrogate → Join) | WRONG → fixed |

### Knowledge promoted to global memory

| Knowledge | Destination | Source |
|-----------|-------------|--------|
| LOOKUP JOIN architecture (two syntax paths, surrogate pattern, physical planning, async lookup, RightChunkedLeftJoin, three Lucene coupling points, generalization path) | `esql-architecture.md` new section | `.kb/deep-dives/ga/lookup-join-how-it-works.md` + `lookup-join-lucene-coupling.md` |
| LIMIT BY command (GroupedLimitOperator, GroupKeyEncoder) | `esql-architecture.md` Recent Developments | PR #143458 |
| Any-value aggregators (FIRST/LAST optimization) | `esql-architecture.md` Recent Developments | PR #143619 |
| T-Digest RAM accounting (circuit breaker integration) | `esql-architecture.md` Circuit Breaker section | PR #143662 |
| Pre-Task Loading rule | `MEMORY.md` new section | session feedback |

### New KB documents created
- `.kb/deep-dives/ga/lookup-join-how-it-works.md` — comprehensive teaching guide for LOOKUP JOIN
- `.kb/deep-dives/ga/lookup-join-lucene-coupling.md` — why Lucene-tied + generalization analysis

### Verified (no changes needed)
- LOOKUP JOIN: Mapper.mapBinary() lines 183-245, isIndexModeLookup() lines 235-245 — verified
- LookupJoinExec: BinaryExec with leftFields/rightFields/addedFields/joinOnConditions — verified
- LookupFromIndexOperator: AsyncOperator with streaming/non-streaming variants — verified
- RightChunkedLeftJoin: positions block pattern, non-decreasing IntBlock — verified
- AbstractLookupService: single-shard validation — verified
- LookupJoin: extends Join, implements SurrogateLogicalPlan — verified
- LocalExecutionPlanner.planLookupJoin() at line 815 — verified
- IndexMode.LOOKUP shards=1 enforcement — verified
- PushFiltersToSource handles external via planFilterExecForExternalSource() — verified
- PushLimitToExternalSource optimizer rule — verified
- HashJoinExec / LookupJoinExec both exist — verified
- `esql-knowledge.md` — all patterns still accurate
- `arrow-deep-dive.md` — all claims still accurate

### Architecture-impacting commits on main
1. **LIMIT BY** (PR #143458) — new GroupedLimitOperator, `LIMIT N BY field` syntax
2. **Any-value aggregators** (PR #143619) — internal FIRST/LAST optimization
3. **T-Digest RAM accounting** (PR #143662) — circuit breaker integration
4. **Lookup Join large text fix** (PR #143627) — respects planner jumbo size setting (minor)
5. **Parquet buffer reuse** (PR #143700) — I/O optimization (no architecture change)

## 2026-03-05: Push KB — global memory sync (13d78a770e9)

### Code checked
- origin/main at 13d78a770e9 (2 new ESQL commits since last update: docs README + sum test precision — no architecture changes)

### Corrections applied to global memory (`~/.claude/memory/esql-architecture.md`)

| Claim | Was | Now | Status |
|-------|-----|-----|--------|
| AggregatorMode enum | `SINGLE, PARTIAL, FINAL` | `INITIAL(false,true), INTERMEDIATE(true,true), FINAL(true,false), SINGLE(false,false)` | WRONG → fixed |
| External source framework section | 8 plugins, coordinator-only execution | 13+ plugins, three-path SPI, distributed execution, optimizer rules, compression | STALE → rewritten |
| Filter pushdown rule name | `PushFiltersToExternalSource` | `PushFiltersToSource` (handles external via `planFilterExecForExternalSource()`) | WRONG → fixed |
| Column pruning rule | `PruneColumnsOnExternalRelation` (implied exists) | Does not exist; `PruneColumns` doesn't handle ExternalRelation (gap) | WRONG → fixed |

### Knowledge promoted to global memory

| Knowledge | Destination | Source |
|-----------|-------------|--------|
| Three-stage physical planning (Mapper → breakPlan → LocalMapper) | `esql-architecture.md` new section | pipeline-breakers deep dives |
| Plan serialization flow (PlanStreamOutput → DataNodeRequest → operators) | same section | pipeline-breakers deep dives |
| Exchange insertion gate (`addExchangeForFragment` instanceof check) | same section | pipeline-breakers deep dives |
| Pipeline breaker distribution gap | `esql-architecture.md` external sources subsection | ga8-summary |
| ExternalRelation logical plan node | source nodes table | KB |
| ExternalSourceExec physical plan node | physical nodes table | KB |
| Command support on external sources (works/crashes/blocked/gaps) | `esql-architecture.md` new subsection | esql-command-support-deep-dive |
| Circuit breaker integration patterns | `esql-architecture.md` new section | tp11-circuit-breaker deep dive |
| Arrow key findings (packaging, ES usage, memory bridge) | `arrow-deep-dive.md` new section | arrow deep dives |

### Verified (no changes needed)
- All 7 Mapper/planning claims re-verified against current code
- All 7/10 SPI claims verified; 3 corrected (above)
- `esql-knowledge.md` — all build/test/CI patterns still accurate
- `MEMORY.md` — skills, research rules, user preferences all current

## 2026-03-05: Full validation against main (36de8d8a4cd)

### Code pulled
- Merged origin/main (4da6c699d4b..36de8d8a4cd) — 14 ESQL-related commits

### Corrections applied to MEMORY.md

| Claim | Was | Now | Status |
|-------|-----|-----|--------|
| TableCatalog methods | `resolveTable()`, `planScan()` | `metadata()`, `planScan(path, config, predicates)`, extends Closeable | STALE → fixed |
| Connector.discoverSplits() | On Connector interface | On SplitProvider interface (not Connector) | WRONG → fixed |
| Filter model | 3-layer (L1/L2/L3) | 2-layer (source-level + engine remainder); partition pruning integrated into SplitDiscoveryPhase | STALE → fixed |
| FormatReader.read() | No config/filter param | Now accepts rowLimit param (PR #143515) | NEW → added |
| LIMIT pushdown | Listed as performance gap | Now merged (PR #143515, PushLimitToExternalSource rule) | NEW → updated |
| Splittable BZIP2 | Not mentioned | PR #143534 merged, SplittableDecompressionCodec | NEW → added |
| TP-4 status | Planned | DONE (PR #143515) | NEW → marked |

### Verified (no changes needed)
- All 13 plugins exist with correct names
- ExternalSourceFactory, ConnectorFactory, SplitProvider, ExternalSplit, FilterPushdownSupport, SourceMetadata, QueryRequest — all verified
- OperatorFactoryRegistry dispatch via instanceof ConnectorFactory — correct
- Mapper.addExchangeForFragment() lines 269-278 — correct
- Pipeline breaker handling in mapUnary (Aggregate/Limit/TopN) — correct
- PlannerUtils.breakPlanBetweenCoordinatorAndDataNode — correct
- collapseExternalSourceExchanges — correct
- AdaptiveStrategy distribution logic — correct (LIMIT-only stays LOCAL)
- DataNodeComputeHandler.handleExternalSourceRequest — correct
- Distribution pipeline (SplitProvider → SplitDiscoveryPhase → AdaptiveStrategy → DataNodeComputeHandler) — fully wired
- ExternalRelation/ExternalSourceExec generic and shared — correct
- Arrow 18.3.0, gRPC 1.78.0 (pinned), AllocationManagerShim, RootAllocator unlimited — all correct
- AggregatorMode enum (INITIAL/INTERMEDIATE/FINAL/SINGLE) with inputPartial/outputPartial — correct
- HashAggregationOperator.shouldEmitPartialResultsPeriodically() gated on isOutputPartial() — correct
- AsyncExternalSourceBuffer tracks by page count not bytes — correct
- Iceberg metadata-only (operatorFactory()=null, splitProvider()=SINGLE) — correct
- IcebergPushdownFilters exists but orphaned — correct

### Key new developments on main
1. **LIMIT pushdown** (PR #143515) — PushLimitToExternalSource optimizer rule pushes rowLimit into ExternalSourceExec. FormatReader.read() accepts rowLimit. LimitingIterator trims final page. AdaptiveStrategy still keeps LIMIT-only queries local.
2. **Splittable BZIP2** (PR #143534) — SplittableDecompressionCodec interface with findBlockBoundaries() + decompressRange(). BZIP2 now supports split-aware reading.
3. **Memory tracking for TS_INFO/METRICS_INFO** (PR #143491)
