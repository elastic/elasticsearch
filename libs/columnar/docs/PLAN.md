# ColumNAR — plan & roadmap

The direction, the decisions that constrain it, and the build order. Update as decisions land.

## Locked decisions

- **Binary format.** One adaptive binary substrate under every field; served through ColumNAR's own
  range-query and block-loader APIs, not Lucene's typed shapes (those throw). No delegate.
- **Type-tagged, open.** Every field is a `BINARY` field tagged with a `ColumnarFieldType`
  (`columnar.type`). Numeric (`LONG`/`DOUBLE`) today; string and more slot in behind the same
  attribute + framing.
- **Per-field encoding is the driver.** The integration picks the encoding from what it knows about
  the field (type, sorted, metric role). Keep the seam open.
- **Ordinals are internal and per-segment.** A string column decides plain vs. ordinal per segment
  from that segment's cardinality; ordinals never surface (the read API stays binary), and a segment
  carries a dictionary only if it chose ordinals. The upper layer sees bytes, never ordinal shapes.
- **Reuse native Zstd for block compression.** Zstd is planned as the last encoder in the block
  pipeline, backed by the existing `org.elasticsearch.nativeaccess.Zstd` binding rather than a Java
  LZ4/Zstd (the native codec is faster).
- **Order preserved; nothing column-sized on the heap.** See `AGENTS.md`.
- **Metadata separate from content.** `.cnm` holds fixed-size per-column records only (read in full
  at open, the only part on the heap); everything column-proportional goes to `.cnd`, and a structure
  read before the values — the skip index — gets its own `.cns`. See `AGENTS.md`.

## Done

- Binary surface: `addBinaryField`/`getBinary` only; typed shapes throw; no delegate.
- `ColumnarFieldType` tag + generic field framing (fieldNumber, type id, metadata); dispatch on type.
- Numeric long column: adaptive `NumericBlockEncoder` (delta/offset/GCD/`ForUtil`), single + multi in
  one store, off-heap `DirectMonotonic` tables, presence (dense / `IndexedDISI` sparse).
- `NumericBinaryPayload` seam (payload ↔ longs); `binaryValue()` re-emit.
- Read fast paths on `ColumnarNumericBinaryDocValues`: `bulkLongs` (block loader) and `rangeIterator`
  (SIMD `inRangeBitmask`, `intoBitSet`/`docIDRunEnd`), skipper-aware and no-skipper.
- Native multi-level skip index inside the column (`NumericSkipWriter`/`NumericColumnSkipper`),
  written to its own `.cns` file.
- `ColumnarNumericRangeQuery`: self-contained Lucene range query over `getBinary`.
- Tests: round-trip, fast-path + skipper vs brute force, end-to-end range query, multi-segment merge.
- `SplitDeltaTransform` (frozen id 3) and `AlpDoubleTransform` (frozen id 4) registered in
  `NumericPipeline.Registry` and exposed through named factories (`monotonicLongPipeline`,
  `doubleGaugePipeline`, `doubleCounterPipeline`) on `NumericPipeline`.
- Per-stage encode/decode JMH benchmarks (`EncodeBlockTransformBenchmark`,
  `DecodeBlockTransformBenchmark`) covering Delta, Offset, GCD, SplitDelta, ALP, and FOR
  across ten block shapes.
- Per-field pipeline selection: `NumericPipelineSelector` (`@FunctionalInterface`
  `select(fieldName, type) -> NumericPipelineTemplate`) injected into `ColumNARDocValuesFormat`
  at construction time alongside an explicit `blockSize`. The selector answers "which pipeline
  type?" without knowing the block size; the format applies it via
  `NumericPipelineTemplate.build(int)`. The four named factories (`defaultPipeline`,
  `monotonicLongPipeline`, `doubleGaugePipeline`, `doubleCounterPipeline`) are usable as method
  references: `(f, t) -> NumericPipeline::defaultPipeline`. Server-side wiring into
  `PerFieldFormatSupplier` is a follow-up (see Next).

## Next

- **Required before the first format bump**: readers must validate recorded ids against the segment
  header version while loading metadata; add v0 fixture reads and a BWC fixture test class.
  While ColumNAR is behind a feature flag and has no stable on-disk compatibility commitment,
  a format-version bump is required only for layout changes, not for id additions.

- **Server-side selector wiring**: implement a concrete `NumericPipelineSelector` in server that
  inspects `FieldType`, `IndexMode`, and `MetricType` to route each field to the correct pipeline
  factory, and wire it into `PerFieldFormatSupplier`.
- **Adaptive keyword (string) column**: measure cardinality while writing a segment and pick the
  layout per segment — **plain** (values stored directly) for high-cardinality segments, **ordinals**
  (a per-segment terms dictionary + ordinal codes) for low-cardinality ones. Ordinals stay entirely
  internal: the surface remains binary (`getBinary`), never `SortedSet`/ordinal shapes, and only the
  segments that chose ordinals carry a dictionary. The cardinality threshold and dictionary layout are
  the tuning knobs.
- **Block compression via native Zstd**: add Zstd as the last encoder in the block pipeline (its own
  frozen id, applied after the terminal, so it stays additive and BWC), backed by
  `org.elasticsearch.nativeaccess.Zstd` rather than a Java LZ4. Most useful on the low-entropy stages
  (terms dictionary, plain keyword bytes).
- **Benchmark expansion**: four follow-up items tracked in
  `~/workspace/todo/es96-columnar/followup-benchmark-expansion.md`:
  1. Isolated force-merge benchmark (`ColumnarNumericForceMergeBenchmark`): builds N segments in
     `@Setup`, measures only `forceMerge(1)` in `@Benchmark`; params: `format`, `workload`,
     `blockSize`, `segmentCount`.
  2. Sparse workloads: add `SPARSE_10` / `SPARSE_50` fill-factor variants to `NumericData` (or a
     wrapper); add `fillFactor` `@Param` to ingest and decode benchmarks.
  3. Sparse random-access decode (`ColumnarNumericRandomAccessBenchmark`): seeks to pre-generated
     random doc IDs via `advanceExact`; exercises the skip index; params `accessFraction`.
  4. Expanded range selectivity: add `0.01` and `0.1` to `ColumnarNumericRangeSlicingBenchmark`'s
     `selectivity` `@Param`.
- **Multi-value benchmark coverage**: `ColumnarNumericIngestBenchmark` and `ColumnarNumericDecodeBenchmark`
  only exercise the single-value path (`FIELD_TYPE_PACKED_LONG`). The multi-value path
  (`FIELD_TYPE_PACKED_LONGS_MV`) is implemented in the consumer but has no JMH coverage. A realistic
  multi-value workload (histogram bucket counts, multiple readings per TSDB series) should be designed
  and added before GA.
- Multi-segment merge efficiency (sequential merge reads).
- Block-loader binding to ES|QL (server-side adapter).
- **Decompose the write loop**: the single pass in `NumericColumnWriter.write` drives three orthogonal
  consumers (block encoder, skip writer, address table) off shared loop state. A producer/consumer
  split lets each be unit-tested against a controlled `NumericColumnValues` without a `Directory`, and
  removes the round-trip ambiguity where a shared encode/decode bug hides which consumer failed.

## Working agreements

- Small self-contained changes proceed directly; anything touching on-disk framing, a frozen id, or
  the read contract is discussed first.
- Every format change ships with correctness tests.
- Every new `BlockTransform` ships with encode and decode entries in
  `EncodeBlockTransformBenchmark` and `DecodeBlockTransformBenchmark`. Add a block shape to
  `NumericData` only if no existing shape exercises the new stage. See `docs/BENCHMARKS.md`.
- Server-tier work (mapping, the binary bridge, synthetic source) lives in other modules.
