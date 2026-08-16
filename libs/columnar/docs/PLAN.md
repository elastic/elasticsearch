# ColumNAR — plan & roadmap

The direction, the decisions that constrain it, and the build order. Update as decisions land.

## Locked decisions

- **Binary format.** One adaptive binary substrate under every field; served through ColumNAR's own
  range-query and block-loader APIs, not Lucene's typed shapes (those throw). No delegate.
- **Type-tagged, open.** Every field is a `BINARY` field tagged with a `ColumnarFieldType`
  (`columnar.type`). Numeric (`LONG`/`DOUBLE`) and `STRING` today; more slot in behind the same
  attribute + framing. How a column encodes within its type (a numeric pipeline, or plain vs.
  dictionary for a string) is internal to the column and recorded in its own metadata, never in the
  type tag.
- **Per-field encoding is the driver.** The integration picks the encoding from what it knows about
  the field (type, sorted, metric role). Keep the seam open.
- **Ordinals are internal and per-segment.** A string column decides plain vs. ordinal per segment
  from that segment's cardinality; ordinals never surface (the read API stays binary), and a segment
  carries a dictionary only if it chose ordinals. The upper layer sees bytes, never ordinal shapes.
- **The terms dictionary is sorted.** Ordinals follow term byte order, so within a segment comparing
  ordinals is a valid proxy for comparing term bytes. This is an internal representation and does not
  weaken the decision above: the read API still takes and returns bytes, and a range query resolves its
  byte bounds to ordinals inside the codec, per segment. Decided but not yet built — the shipped column
  still writes first-seen order, and the switch is in Next. It changes a frozen layout, so it lands
  before the format is depended on.
- **Reuse native Zstd for block compression.** Zstd is planned as the last encoder in the block
  pipeline, backed by the existing `org.elasticsearch.nativeaccess.Zstd` binding rather than a Java
  LZ4/Zstd (the native codec is faster).
- **Order preserved; nothing column-sized on the heap.** See `AGENTS.md`.

## Done

- Binary surface: `addBinaryField`/`getBinary` only; typed shapes throw; no delegate.
- `ColumnarFieldType` tag + generic field framing (fieldNumber, type id, metadata); dispatch on type.
- Numeric long column: adaptive `NumericBlockEncoder` (delta/offset/GCD/`ForUtil`), single + multi in
  one store, off-heap `DirectMonotonic` tables, presence (dense / `IndexedDISI` sparse).
- `NumericBinaryPayload` seam (payload ↔ longs); `binaryValue()` re-emit.
- Read fast paths on `ColumnarNumericBinaryDocValues`: `bulkLongs` (block loader) and `rangeIterator`
  (SIMD `inRangeBitmask`, `intoBitSet`/`docIDRunEnd`), skipper-aware and no-skipper.
- Native multi-level skip index inside the column (`NumericSkipWriter`/`NumericColumnSkipper`).
- `ColumnarNumericRangeQuery`: self-contained Lucene range query over `getBinary`.
- Tests: round-trip, fast-path + skipper vs brute force, end-to-end range query, multi-segment merge.
- `SplitDeltaTransform` (frozen id 3) and `AlpDoubleTransform` (frozen id 4) registered in
  `NumericPipeline.Registry` and exposed through named factories (`monotonicLongPipeline`,
  `doubleGaugePipeline`, `doubleCounterPipeline`) on `NumericPipeline`.
- Per-stage encode/decode JMH benchmarks (`EncodeBlockTransformBenchmark`,
  `DecodeBlockTransformBenchmark`) covering Delta, Offset, GCD, SplitDelta, ALP, and FOR
  across ten block shapes.
- Adaptive keyword (string) column: `ColumnarFieldType.STRING` served at `getBinary` through
  `ColumnarStringBinaryDocValues`. A per-segment cardinality probe picks the layout — `PLAIN`
  (`[vint length][bytes]` per value) above the threshold, `DICTIONARY` (a capped, first-seen-order
  terms dictionary plus one ordinal per value, ordinals encoded through
  `NumericPipeline.defaultPipeline`) at or below it. `StringDictionary.MAX_SIZE` is 256, so an ordinal
  fits 8 bits. Ordinals stay internal; only a segment that chose `DICTIONARY` carries a dictionary.
  Dense and sparse; single-valued only (see Next). Ported from the original POC's dict-binary path.
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
- **Sort the string terms dictionary** — decided, not yet built, and wanted before the format ships
  because it changes a frozen on-disk layout. Terms are currently stored in first-seen order (the POC's
  `LinkedHashMap`), so an ordinal carries no ordering relative to the term bytes. Sorting the dictionary
  makes ordinals order-preserving within a segment, which is what unlocks the rest: sort-by-ordinal
  becomes sort-by-value, the dictionary supports binary search rather than a hash map (what makes
  raising the cap viable), and a range or prefix query gains a fast path.

  That fast path is worth stating precisely, because it is easy to read as a breach of the
  ordinals-are-internal rule and is not one. A `ColumnarStringRangeQuery` takes byte bounds, like every
  other read entry point. A `DICTIONARY` segment can then resolve those bounds against its sorted
  dictionary *once per segment* — a binary search — and from there answer the range by comparing
  ordinals, which is numeric-style min/max skipping over the ordinal stream. The ordinals never leave
  the codec, exactly as `ColumnarNumericRangeQuery` keeps its block decoding internal. Note this is a
  per-layout fast path, not a uniform mechanism: a `PLAIN` segment has no ordinals, so it needs byte
  comparisons and a byte-oriented skip structure. A string range query is therefore two paths, and the
  skip index a `DICTIONARY` column writes is not the one a `PLAIN` column needs.

  Sorting is cheap at the current cap (256 entries, once per segment per field). The one structural
  change is that `StringDictionary.Builder` can no longer assign an ordinal when it first sees a term —
  a later term may sort ahead of an earlier one — so ordinals are assigned once the probe completes.
  That fits the existing shape, since the probe already finishes before encoding starts.

  **Measure before landing.** First-seen order assigns ordinals in the order value clusters first
  appear, so clustered data yields a non-decreasing ordinal stream (`0,0,0,1,1,1,2,2,2`) that delta
  collapses almost entirely. A sorted dictionary makes ordinals follow byte order, so the same column
  yields a permutation (`5,5,5,2,2,2,9,9,9`) — runs survive, monotonicity does not. The two orders
  coincide when the index is sorted by the field itself; they diverge for a keyword field that is
  clustered by some *other* sort key, which is the common logs shape. Capture the ordinal-stream
  footprint on that workload before and after; if the regression is material, the fix is a better
  ordinal encoder (see the ordinal-pipeline item) rather than abandoning the sort.

- **Remap ordinals on merge instead of rehashing values** — a merge currently rebuilds the destination
  dictionary from scratch: `ColumNARDocValuesConsumer.writeStringColumn` runs a fresh
  `StringDictionary.Builder` over every surviving value, and for a `DICTIONARY` source each value makes
  a full round trip (source ordinal → `dictionary.term(ord)` → bytes → hash → destination ordinal).
  That is O(numValues) hashing on the merge path.

  A `DICTIONARY` → `DICTIONARY` merge can instead build the destination dictionary from the source
  *dictionaries* (at most `MAX_SIZE` terms each) and derive one `int[]` remap per source segment, so
  writing a value becomes an array index. A `PLAIN` source still has to be walked. Two caveats worth
  recording: building from source dictionaries retains terms whose documents were all deleted, which
  both carries dead terms into the merged segment and makes the cap decision pessimistic — a merge could
  fall back to `PLAIN` when the live cardinality would have fit — so it needs a liveness pass or an
  accepted approximation. And Lucene's `OrdinalMap` is the wrong tool at this cap: it builds
  `PackedLongValues` with monotonic compression for millions of terms, where a plain `int[]` remap is
  simpler and faster. Sorting the dictionary is not a prerequisite; it only replaces the union hash map
  with a merge of sorted lists, which is a marginal gain at 256 terms. Measure before building — the
  per-value saving is one byte-resolution plus a hash replaced by an array index, and block decode and
  I/O may well dominate.

- **String column follow-ups** — the initial column is a faithful port of the POC's dict-binary path;
  each of these was deliberately left out to keep that port reviewable, and each is an open question on
  the porting PR rather than a settled decision:
  1. **Multi-valued string columns**: single-valued only today (the writer rejects a document with more
     than one value). The substrate already supplies presence and a value-address table, so this mirrors
     what `NumericColumnWriter` does; note `ColumnarStringBinaryDocValues.binaryValue` currently relies
     on one reused `BytesRef` per document and has to copy once several values are collected.
  2. **Cardinality policy**: the probe accepts a dictionary purely on distinct count
     (`StringDictionary.MAX_SIZE`, 256) with no ratio guard, so a small column whose values are nearly
     all distinct still pays for a dictionary that cannot pay for itself. A ratio guard
     (`distinct * 2 <= numValues`) and a larger cap are both worth measuring — the cap and the
     dictionary layout are the tuning knobs. Raising the cap is also a heap decision, since the
     dictionary is the column's one heap-resident structure.
  3. **Skip index and a string range query**: the string column writes no skip index, so there is no
     `ColumnarStringRangeQuery` counterpart yet. The `DICTIONARY` half depends on the sorted dictionary
     above and can then reuse numeric-style min/max skipping over ordinals; the `PLAIN` half cannot, and
     needs a byte-oriented structure (min/max term per interval) that does not exist yet. Worth deciding
     whether `PLAIN` gets a skip index at all, or whether high-cardinality string columns simply scan.
  4. **Ordinal pipeline selection**: the ordinal stream is hardcoded to
     `NumericPipeline.defaultPipeline`. Routing it through `NumericPipelineSelector`, or giving it a
     dedicated ordinal pipeline, is untested either way. `NumericBlockEncoder.encodeOrdinals` /
     `decodeOrdinals` (the run / two-run / cycle / bit-packed codec) is present but unused, is
     insensitive to whether ordinals are monotonic, and is therefore both the obvious candidate to
     measure against and the likely answer if sorting costs ordinal-stream footprint.
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
