## Summary

This PR extracts the shared TSDB doc values wire-format logic from the `ES819` implementation into abstract base classes (`AbstractTSDBDocValuesConsumer`, `AbstractTSDBDocValuesProducer`), so that introducing a new codec version (e.g., `ES94`) only requires a thin format class, a consumer subclass, and a producer subclass.

It also introduces `TSDBDocValuesFormatConfig` to group all format-specific tuning parameters into meaningful sub-records, replacing the long list of individual constructor parameters and decoupling the base classes from ES819-specific constants.

### What's included

**Shared base classes**

| Component | Purpose |
|---|---|
| `AbstractTSDBDocValuesConsumer` | Owns the full write path for numeric, binary, sorted, sorted-numeric, and sorted-set doc values. Subclasses provide the numeric block encoding strategy via `createNumericBlockWriter`. All format-specific parameters accessed through `TSDBDocValuesFormatConfig` (single source of truth) |
| `AbstractTSDBDocValuesProducer` | Owns the full read path and field metadata parsing. Subclasses provide the numeric block decoding strategy via `createNumericBlockReader` |
| `NumericBlockWriter` | Symmetric two-method interface (`write` + `writeOrdinals`) for pluggable numeric block encoding |
| `NumericBlockReader` | Symmetric two-method interface (`read` + `readOrdinals`) for pluggable numeric block decoding |
| `TSDBDocValuesFormatConfig` | Groups format parameters into sub-records (`VersionConfig`, `TermsDictConfig`, `SkipIndexConfig`, `NumericConfig`, `BinaryConfig`) with `directMonotonicBlockShift` at the top level (cross-cutting, used by all field types) and delegation methods for flat access |

**DocValuesSource type hierarchy**

The original `ES819` consumer used `TsdbDocValuesProducer` — a single class with two constructors (one for merge, one for add/indexing) and `if (actual != null)` null-checks in every doc values getter. Since `TsdbDocValuesProducer` was `es819`-specific, the shared consumer needs a replacement to carry `MergeStats` alongside a `DocValuesProducer`. Rather than threading `MergeStats` as a separate parameter through every method signature, we split the original into two classes:

| Component | Purpose |
|---|---|
| `DocValuesSource` | Abstract base carrying `MergeStats` alongside doc values data. On the merge path, anonymous subclasses override only the doc values methods they need. Factory method `fromProducer()` normalizes any `DocValuesProducer` at the boundary, eliminating `instanceof` checks in consumer code |
| `IndexingDocValuesSource` | Add/indexing path: wraps a real Lucene `DocValuesProducer`, delegates all doc values methods, no null-checks. Stats are `UNSUPPORTED` |

**Promoted to `tsdb` package**

Moved from `es819` to the shared `tsdb` package with minimal changes (package declaration, visibility bump where needed, hardcoded `ES819` references replaced with constructor parameters):

| Component | Previous location | Visibility |
|---|---|---|
| `DocOffsetsCodec` | `es819` | public (used by format classes) |
| `DISIAccumulator` | `es819` | public (used by tests) |
| `BlockMetadataAccumulator` | `es819` | package-private |
| `DocValuesConsumerUtil` | `es819` | public |
| `OffsetsAccumulator` | `es819` | public (used by tests) |
| `XDocValuesConsumer` | `es819` | public |

**ES819 subclasses**

| Component | Purpose |
|---|---|
| `ES819TSDBDocValuesConsumer` | Thin subclass providing `TSDBDocValuesEncoder`-based block writing and `DocOffsetsCodec` encoder |
| `ES819TSDBDocValuesProducer` | Thin subclass providing `TSDBDocValuesEncoder`-based block reading and `DocOffsetsCodec` decoder |
| `ES819TSDBDocValuesFormatFactory` | Renamed from `TSDBDocValuesFormatFactory` for naming consistency |

### Design highlights

- **Template Method + Strategy**: the abstract base classes own the wire format; subclasses inject codec-specific behavior via `createNumericBlockWriter`, `createNumericBlockReader`, and `docOffsetsEncoder`/`docOffsetsDecoder`
- **TSDBDocValuesFormatConfig as single source of truth**: all format-specific parameters grouped into 5 sub-records with `directMonotonicBlockShift` at the top level (it is used across all field types for offset/address encoding via `DirectMonotonicWriter`, not just numerics); delegation methods provide flat access
- **DocValuesSource hierarchy**: replaces the original `TsdbDocValuesProducer` (which had two constructors and null-checks in every getter) with a clean split — `DocValuesSource` for the merge path (stats known, override one getter), `IndexingDocValuesSource` for the add path (delegate everything, no stats). `fromProducer()` normalizes at the API boundary so downstream code works uniformly
- **Symmetric ordinal encoding**: ordinals now flow through the pluggable `NumericBlockWriter.writeOrdinals` / `NumericBlockReader.readOrdinals` instead of being hardcoded to `TSDBDocValuesEncoder`, making ordinal encoding customizable per codec version
- **Minimal visibility**: promoted classes use the narrowest visibility that satisfies their consumers; `IndexingDocValuesSource` is package-private
- **Named constants**: magic values `-1` and `-2` in sorted numeric index encoding replaced with `INDEX_SINGLE_ORDINAL` and `INDEX_ORDINAL_RANGE`; type tag constants (`NUMERIC`, `BINARY`, `SORTED`, etc.) moved to the abstract base class

### Cleanup

- Replaced `TsdbDocValuesProducer` with `DocValuesSource` / `IndexingDocValuesSource` hierarchy
- Renamed `ES819BinaryDocValues` to `TSDBBinaryDocValues` (version-neutral, lives in shared abstract base)
- Removed dead constants (`SKIP_INDEX_INTERVAL_BYTES`, `SKIP_INDEX_JUMP_LENGTH_PER_LEVEL`)
- Removed duplicate type tag constants from `ES819TSDBDocValuesFormat`
- Fixed test method name typo (`testuUe...` → `testUse...`)
- Added consistent javadoc with `@param`/`@return`/`@throws` tags across new public APIs
- Fixed latent bug where `addNumericField` lost merge stats by wrapping in `IndexingDocValuesSource`

### Testing

- New `OffsetsAccumulatorTests`: round-trip tests (single doc, multiple docs, zero/mixed counts, random, large values)
- New `BlockMetadataAccumulatorTests`: round-trip tests (single/multiple/uniform blocks, random)
- Moved `DISIAccumulatorTests` and `DocOffsetsCodecTests` from `es819` to `tsdb` package (minimal changes: package declaration only)
- Existing `ES819TSDBDocValuesFormatTests` and `ES94TSDBDocValuesFormatTests` provide end-to-end codec coverage

```
./gradlew :server:test --tests "*ES819TSDBDocValuesFormatTests*"
./gradlew :server:test --tests "*ES819TSDBDocValuesFormatFactoryTests*"
./gradlew :server:test --tests "*OffsetsAccumulatorTests*"
./gradlew :server:test --tests "*BlockMetadataAccumulatorTests*"
./gradlew :server:test --tests "*DocOffsetsCodecTests*"
./gradlew :server:test --tests "*DISIAccumulatorTests*"
```
