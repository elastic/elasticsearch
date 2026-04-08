# ES|QL External Data Sources — Development Velocity Analysis

Generated: 2026-03-04

## Methodology

PRs were found by searching the elastic/elasticsearch repository for merged PRs matching
keywords: external, datasource, data source, parquet, NDJSON, storage, split, distributed,
ExternalSource, EXTERNAL, FormatReader, StorageProvider, connector, flight, grpc, iceberg,
ORC, CSV, ZSTD, BZIP2, compression, Azure, S3, GCS. Results were deduplicated and manually
filtered to include only PRs directly related to the external data sources feature.

Wall-clock days = merged_at - created_at (includes review time, CI, weekends).

## All PRs (sorted by merge date)

| PR# | Title | Author | Area | Created | Merged | Days | +lines | -lines | Files |
|-----|-------|--------|------|---------|--------|------|--------|--------|-------|
| #141678 | [ESQL] Introduce pluggable external datasource framework | costin | Core Framework | 2026-02-02 | 2026-02-16 | 13.6 | +30005 | -3609 | 228 |
| #142563 | ESQL: Add Google Cloud Storage data source plugin | bpintea | Storage Plugins | 2026-02-16 | 2026-02-17 | 0.7 | +4741 | -5 | 62 |
| #142565 | [ESQL] Review fixes for datasource framework | costin | Core Framework | 2026-02-16 | 2026-02-18 | 1.2 | +143 | -174 | 16 |
| #142663 | Revert "[ESQL] Introduce pluggable external datasource framework (#141678)" | smalyshev | Core Framework | 2026-02-18 | 2026-02-19 | 0.2 | +3524 | -34592 | 280 |
| #142707 | ESQL: Reapply "Introduce pluggable external datasource framework (#141678)" | costin | Core Framework | 2026-02-19 | 2026-02-20 | 0.8 | +34648 | -3540 | 283 |
| #142667 | ESQL: Add Connector SPI and gRPC/Arrow Flight module | costin | Connector SPI / Flight | 2026-02-18 | 2026-02-22 | 3.6 | +5612 | -358 | 50 |
| #142815 | ESQL: Make datasources plugins lazy | costin | Core Framework | 2026-02-22 | 2026-02-23 | 0.8 | +1044 | -96 | 20 |
| #142560 | [esql] NDJSON datasource | swallez | Format Readers | 2026-02-16 | 2026-02-23 | 6.7 | +1784 | -2 | 24 |
| #142840 | ESQL: Fix Windows file URI construction in datasource tests | costin | Core Framework | 2026-02-23 | 2026-02-23 | 0.2 | +24 | -13 | 5 |
| #142839 | ESQL: Fix datasource release-build failures | costin | Core Framework | 2026-02-23 | 2026-02-23 | 0.3 | +114 | -70 | 6 |
| #142855 | ESQL: reapply "NDJSON datasource" | swallez | Format Readers | 2026-02-23 | 2026-02-23 | 0.1 | +1819 | -3 | 26 |
| #142900 | ESQL: Add support for ORC file format | bpintea | Format Readers | 2026-02-23 | 2026-02-24 | 0.7 | +2974 | -6 | 25 |
| #142989 | ESQL: Fix license dependency mappings | bpintea | Core Framework | 2026-02-24 | 2026-02-24 | 0.1 | +1 | -1 | 1 |
| #143035 | ESQL: Datasources: GZIP | bpintea | Compression Codecs | 2026-02-25 | 2026-02-25 | 0.3 | +1145 | -28 | 25 |
| #142938 | ESQL: Fix Parquet reader on missing projected columns | bpintea | Format Readers | 2026-02-24 | 2026-02-25 | 1.3 | +47 | -0 | 2 |
| #143005 | ESQL: Add split SPI, partition detection, and filter hint extraction | costin | Distributed Execution | 2026-02-24 | 2026-02-25 | 1.0 | +1714 | -18 | 22 |
| #143114 | ESQL: Add split discovery and distribution for external sources | costin | Distributed Execution | 2026-02-25 | 2026-02-26 | 0.6 | +971 | -21 | 13 |
| #143152 | ESQL: Guard EXTERNAL tests behind capability | ivancea | Core Framework | 2026-02-26 | 2026-02-26 | 0.1 | +2 | -6 | 2 |
| #143154 | ESQL: Add local parallelism and partition detection for external sources | costin | Distributed Execution | 2026-02-26 | 2026-02-26 | 0.3 | +1216 | -92 | 15 |
| #143194 | ESQL: Add distribution strategy for external sources | costin | Distributed Execution | 2026-02-26 | 2026-02-27 | 0.4 | +1006 | -1 | 14 |
| #143209 | ESQL: Add data node execution for external sources | costin | Distributed Execution | 2026-02-27 | 2026-02-27 | 0.0 | +916 | -39 | 9 |
| #143228 | ESQL: Data sources: ZSTD, BZIP2 | bpintea | Compression Codecs | 2026-02-27 | 2026-02-27 | 0.1 | +837 | -22 | 28 |
| #143236 | ESQL: Data sources: Azure plugin | bpintea | Storage Plugins | 2026-02-27 | 2026-02-28 | 0.9 | +2646 | -86 | 52 |
| #143333 | ESQL: Add error handling and propagation for external source execution | costin | Distributed Execution | 2026-02-28 | 2026-03-01 | 1.3 | +1247 | -10 | 15 |
| #143336 | ESQL: Add spec-driven distributed integration tests for external sources | costin | Distributed Tests | 2026-02-28 | 2026-03-01 | 1.2 | +390 | -118 | 6 |
| #143331 | ESQL: Bridge Connector SPI to ExternalSplit | costin | Connector SPI / Flight | 2026-02-28 | 2026-03-01 | 1.4 | +618 | -14 | 12 |
| #143341 | ESQL: Add distribution property tests for external sources | costin | Distributed Tests | 2026-02-28 | 2026-03-01 | 1.1 | +295 | -13 | 3 |
| #143349 | ESQL: External source parallel execution and distribution | costin | Distributed Execution | 2026-03-01 | 2026-03-02 | 1.0 | +811 | -43 | 11 |
| #143417 | ESQL: Fix datasource test failures on Windows and FIPS | costin | Core Framework | 2026-03-02 | 2026-03-03 | 0.6 | +88 | -16 | 6 |
| #143420 | ESQL: Add extended distribution tests and fault injection for external sources | costin | Distributed Tests | 2026-03-02 | 2026-03-03 | 0.6 | +662 | -0 | 5 |

## Summary Statistics

- **Total PRs**: 30
- **Date range**: 2026-02-02 to 2026-03-03 (28 calendar days)
- **Total lines added**: +101,044
- **Total lines removed**: -42,996
- **Net lines**: +58,048
- **Total files changed**: 1,266

### Note on Revert/Reapply

PR #142663 (revert) and #142707 (reapply) are mechanical operations on PR #141678.
The revert removed ~34.6k lines; the reapply re-added ~34.6k lines. These inflate both
addition and deletion counts. Excluding the revert/reapply pair:

- **Adjusted lines added** (excl. revert/reapply): +62,872
- **Adjusted lines removed** (excl. revert/reapply): -4,864
- **Adjusted net lines**: +58,008

PR #142855 (NDJSON reapply) duplicates #142560. Excluding all revert/reapply pairs:
- **Fully adjusted lines added**: +61,053
- **Fully adjusted lines removed**: -4,861
- **Fully adjusted net lines**: +56,192

### Authors and PR Counts

| Author | PRs | Total +lines |
|--------|-----|-------------|
| costin | 19 | +81,524 |
| bpintea | 7 | +12,391 |
| swallez | 2 | +3,603 |
| smalyshev | 1 | +3,524 |
| ivancea | 1 | +2 |

### Wall-Clock Time

- **Average wall-clock days per PR**: 1.4
- **Median wall-clock days per PR**: 0.7
- **Min**: 0.0 days
- **Max**: 13.6 days

### Velocity Metrics

- **Gross lines changed per calendar day**: 5,144 lines/day
- **Adjusted net lines per calendar day** (excl. revert/reapply): 2,007 lines/day
- **PRs per calendar day**: 1.1
- **PRs per week** (over 28 days): 7.5

## Per-Area Breakdown

| Area | PRs | +lines | -lines | Net | Files | Avg Days | Authors |
|------|-----|--------|--------|-----|-------|----------|---------|
| Core Framework | 10 | +69,593 | -42,117 | +27,476 | 847 | 1.8 | bpintea, costin, ivancea, smalyshev |
| Connector SPI / Flight | 2 | +6,230 | -372 | +5,858 | 62 | 2.5 | costin |
| Distributed Execution | 7 | +7,881 | -224 | +7,657 | 99 | 0.7 | costin |
| Distributed Tests | 3 | +1,347 | -131 | +1,216 | 14 | 1.0 | costin |
| Storage Plugins | 2 | +7,387 | -91 | +7,296 | 114 | 0.8 | bpintea |
| Format Readers | 4 | +6,624 | -11 | +6,613 | 77 | 2.2 | bpintea, swallez |
| Compression Codecs | 2 | +1,982 | -50 | +1,932 | 53 | 0.2 | bpintea |

## Per-Area Detail

### Core Framework

- **PRs**: 10
- **Span**: 2026-02-02 to 2026-03-03 (28 days)
- **Lines**: +69,593 / -42,117 (net +27,476)
- **Net lines per day**: 981
- **Authors**: bpintea, costin, ivancea, smalyshev

- **Adjusted** (excl. revert/reapply): +31,421 / -3,985 (net +27,436)
- **Adjusted net lines per day**: 980

  - #141678: [ESQL] Introduce pluggable external datasource framework (costin, 13.6d, +30,005/-3,609)
  - #142565: [ESQL] Review fixes for datasource framework (costin, 1.2d, +143/-174)
  - #142663: Revert "[ESQL] Introduce pluggable external datasource framework (#141678)" (smalyshev, 0.2d, +3,524/-34,592)
  - #142707: ESQL: Reapply "Introduce pluggable external datasource framework (#141678)" (costin, 0.8d, +34,648/-3,540)
  - #142815: ESQL: Make datasources plugins lazy (costin, 0.8d, +1,044/-96)
  - #142840: ESQL: Fix Windows file URI construction in datasource tests (costin, 0.2d, +24/-13)
  - #142839: ESQL: Fix datasource release-build failures (costin, 0.3d, +114/-70)
  - #142989: ESQL: Fix license dependency mappings (bpintea, 0.1d, +1/-1)
  - #143152: ESQL: Guard EXTERNAL tests behind capability (ivancea, 0.1d, +2/-6)
  - #143417: ESQL: Fix datasource test failures on Windows and FIPS (costin, 0.6d, +88/-16)

### Connector SPI / Flight

- **PRs**: 2
- **Span**: 2026-02-18 to 2026-03-01 (10 days)
- **Lines**: +6,230 / -372 (net +5,858)
- **Net lines per day**: 586
- **Authors**: costin

  - #142667: ESQL: Add Connector SPI and gRPC/Arrow Flight module (costin, 3.6d, +5,612/-358)
  - #143331: ESQL: Bridge Connector SPI to ExternalSplit (costin, 1.4d, +618/-14)

### Distributed Execution

- **PRs**: 7
- **Span**: 2026-02-24 to 2026-03-02 (5 days)
- **Lines**: +7,881 / -224 (net +7,657)
- **Net lines per day**: 1,531
- **Authors**: costin

  - #143005: ESQL: Add split SPI, partition detection, and filter hint extraction (costin, 1.0d, +1,714/-18)
  - #143114: ESQL: Add split discovery and distribution for external sources (costin, 0.6d, +971/-21)
  - #143154: ESQL: Add local parallelism and partition detection for external sources (costin, 0.3d, +1,216/-92)
  - #143194: ESQL: Add distribution strategy for external sources (costin, 0.4d, +1,006/-1)
  - #143209: ESQL: Add data node execution for external sources (costin, 0.0d, +916/-39)
  - #143333: ESQL: Add error handling and propagation for external source execution (costin, 1.3d, +1,247/-10)
  - #143349: ESQL: External source parallel execution and distribution (costin, 1.0d, +811/-43)

### Distributed Tests

- **PRs**: 3
- **Span**: 2026-02-28 to 2026-03-03 (2 days)
- **Lines**: +1,347 / -131 (net +1,216)
- **Net lines per day**: 608
- **Authors**: costin

  - #143336: ESQL: Add spec-driven distributed integration tests for external sources (costin, 1.2d, +390/-118)
  - #143341: ESQL: Add distribution property tests for external sources (costin, 1.1d, +295/-13)
  - #143420: ESQL: Add extended distribution tests and fault injection for external sources (costin, 0.6d, +662/-0)

### Storage Plugins

- **PRs**: 2
- **Span**: 2026-02-16 to 2026-02-28 (11 days)
- **Lines**: +7,387 / -91 (net +7,296)
- **Net lines per day**: 663
- **Authors**: bpintea

  - #142563: ESQL: Add Google Cloud Storage data source plugin (bpintea, 0.7d, +4,741/-5)
  - #143236: ESQL: Data sources: Azure plugin (bpintea, 0.9d, +2,646/-86)

### Format Readers

- **PRs**: 4
- **Span**: 2026-02-16 to 2026-02-25 (9 days)
- **Lines**: +6,624 / -11 (net +6,613)
- **Net lines per day**: 735
- **Authors**: bpintea, swallez

- **Adjusted** (excl. NDJSON reapply): +4,805 / -8 (net +4,797)

  - #142560: [esql] NDJSON datasource (swallez, 6.7d, +1,784/-2)
  - #142855: ESQL: reapply "NDJSON datasource" (swallez, 0.1d, +1,819/-3)
  - #142900: ESQL: Add support for ORC file format (bpintea, 0.7d, +2,974/-6)
  - #142938: ESQL: Fix Parquet reader on missing projected columns (bpintea, 1.3d, +47/-0)

### Compression Codecs

- **PRs**: 2
- **Span**: 2026-02-25 to 2026-02-27 (2 days)
- **Lines**: +1,982 / -50 (net +1,932)
- **Net lines per day**: 966
- **Authors**: bpintea

  - #143035: ESQL: Datasources: GZIP (bpintea, 0.3d, +1,145/-28)
  - #143228: ESQL: Data sources: ZSTD, BZIP2 (bpintea, 0.1d, +837/-22)

## Development Timeline

### Week-by-Week Activity

| Week | PRs Merged | +lines | -lines |
|------|-----------|--------|--------|
| 2026-W08 | 6 | +78,673 | -42,278 |
| 2026-W09 | 21 | +20,810 | -659 |
| 2026-W10 | 3 | +1,561 | -59 |

## Key Observations for Estimate Calibration

### 1. Initial Framework was the Largest Single PR
PR #141678 was 30k+ lines across 228 files and took 14 days (Feb 2 - Feb 16).
This included the full SPI, S3/Parquet/HTTP plugins, tests, and build infrastructure.
It was reverted and reapplied within 2 days, suggesting CI/stability concerns rather than design issues.

### 2. Costin Leau Dominates Velocity
Costin authored 19 of 30 PRs (63%), 
contributing +81,524 lines added. Primary contributor for framework, distributed execution,
and connector SPI. Average turnaround: 1.6 days per PR.

### 3. Bogdan Pintea Covers Formats + Storage
Bogdan authored 7 of 30 PRs, contributing +12,391 lines. Handles GCS, Azure,
ORC, Parquet fixes, GZIP, ZSTD/BZIP2. Average turnaround: 0.6 days per PR.

### 4. Distributed Execution Was Fast (7 PRs in 6 Days)
7 PRs totaling +7,881 lines merged between Feb 25 and Mar 2 (6 calendar days).
This is ~1,314 lines added per day — very high velocity, all by costin.

### 5. Storage Plugins Are ~1 Day Each
GCS plugin: 4,741 lines, 0.7 days. Azure plugin: 2,646 lines, 0.9 days.
These leverage existing ES repository plugins (S3/GCS/Azure) and follow a template pattern.

### 6. Compression Codecs Are Fast (<1 Day)
GZIP: 1,145 lines, 0.3 days. ZSTD+BZIP2: 837 lines, 0.1 days.
Purely additive, leveraging existing compression libraries.

### 7. Format Readers Take 1-7 Days
ORC: 2,974 lines, 0.7 days. NDJSON: 1,784 lines, 6.7 days (longer review cycle).
Parquet fix: 47 lines, 1.3 days.

### 8. Implied Velocity Rates for Estimation

| Work Type | Typical Size | Typical Duration | Implied Rate |
|-----------|-------------|-----------------|-------------|
| Large framework PR | 30k+ lines | 14 days | ~2,100 net lines/day |
| Distributed execution PR | 800-1,700 lines | 0.1-1.0 days | ~1,000-1,700 lines/day |
| Storage plugin | 2,600-4,700 lines | 0.7-0.9 days | ~3,500-5,200 lines/day |
| Format reader | 1,800-3,000 lines | 0.7-6.7 days | 400-4,200 lines/day |
| Compression codec | 800-1,100 lines | 0.1-0.3 days | ~3,600-8,000 lines/day |
| Bug fix / small | 2-143 lines | 0.1-1.3 days | varies |
| Connector SPI | 5,600 lines | 3.6 days | ~1,500 lines/day |

### 9. Team Size and Specialization

Effectively 3 active contributors with clear specialization:
- **costin** (Costin Leau): Framework architect — SPI, distributed, connectors
- **bpintea** (Bogdan Pintea): Format & storage specialist — readers, codecs, cloud plugins
- **swallez** (Sylvain Wallez): Format reader — NDJSON
- **ivancea** (Ivan Cea), **smalyshev** (Stanislav Malyshev): Supporting roles (test guards, revert)

### 10. Overall Project Velocity

Over 28 calendar days, 30 PRs were merged by 5 authors.
Excluding revert/reapply pairs, this is +61,053 / -4,861 = +56,192 net lines.
That is approximately **2,007 net lines per calendar day** or
**14,048 net lines per week**.
