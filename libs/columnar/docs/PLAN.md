# ColumNAR — plan & roadmap

The direction, the decisions that constrain it, and the build order. Update as decisions land.

## Locked decisions

- **Binary format.** One adaptive binary substrate under every field; served through ColumNAR's own
  range-query and block-loader APIs, not Lucene's typed shapes (those throw). No delegate.
- **Type-tagged, open.** Every field is a `BINARY` field whose `ColumnarFieldType` is resolved by an
  injected `ColumnarFieldTypeSelector` at write time and re-read from the column metadata at read time.
  Numeric (`LONG`/`DOUBLE`) and `STRING` today; more slot in behind the same selector + framing. How a
  column encodes within its type (a numeric pipeline, or plain vs. dictionary for a string) is internal
  to the column and recorded in its own metadata, never in the type tag.
- **Per-field encoding is the driver.** The integration picks the encoding from what it knows about
  the field (type, sorted, metric role). Keep the seam open.
- **Ordinals are internal.** Ordinals never surface — the read API stays binary and the upper layer sees
  bytes, never ordinal shapes. Only a segment that uses ordinals carries a dictionary.
- **The layout decision follows segment lifecycle.** A flush makes a small, short-lived segment that is
  usually merged away, so it writes the cheapest thing — plain — and emits statistics. A merge makes the
  large, long-lived segment where footprint matters and the data is being rewritten anyway, so that is
  where ordinals arrive. Later discovery, better decision: a 100-document flush knows almost nothing
  about a column, while the merge that unions twenty of them knows a lot.
- **Layout is a decision, statistics are knowledge — do not infer one from the other.** A layout byte
  that doubles as the cardinality verdict is what stops a small flush from deferring the choice without
  discarding what it observed. Statistics travel forward; the layout is decided from them.
- **Coverage, not cardinality, decides whether ordinals pay.** `coverage(N)` is the fraction of a
  column's values that a dictionary of the `N` most frequent terms would cover. Distinct count alone
  misleads in both directions: 50k distinct values still favour ordinals when the top 1k cover 90% of
  documents, while 500 roughly-uniform values may not be worth encoding at all. The uncovered tail stays
  plain inline, so a dictionary stops being all-or-nothing.
- **The terms dictionary is sorted lexicographically.** Frequency decides which terms are in the
  dictionary; term byte order decides which ordinal each one gets. That keeps an ordinal range equal to a
  value range, so a range query resolves its byte bounds once per segment and then compares ordinals,
  where an unsorted dictionary would have to be scanned whole. Sorting is an internal representation and
  does not weaken the rule above: the read API still takes and returns bytes.
- **Blocks count values, chunks bound bytes.** A block is an *encoding* and addressing unit counted in
  values; a chunk is a *compression* unit closed by a byte target or a value count, whichever comes first.
- **Native Zstd for compression.** Chunks and compressed ordinal blocks go through the existing
  `org.elasticsearch.nativeaccess.Zstd` binding rather than a Java implementation.
- **Order preserved; nothing column-sized on the heap.** See `AGENTS.md`.
- **One file per concern, shared by every field.** `.cnm` holds fixed-size per-column records only (read
  in full at open, the only part on the heap). Content is split by how it is read: values in `.cnd`,
  per-document addressing in `.cna`, a plain column's lengths in `.cnl`, per-block and per-chunk
  navigation in `.cnn` (fetched up front, so it stays coarse and small), the skip index in `.cns`.
  Every table streams into its file; nothing is staged but a dictionary's ordinals and escapes.
  See `AGENTS.md`.

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
- Keyword (string) column: `ColumnarFieldType.STRING` served at `getBinary` through
  `ColumnarStringBinaryDocValues`. `StringColumnLayout.PLAIN` stores the values' bytes one after another
  in chunks and a code per slot in `.cnl` (`0` null, `1` repeat of the slot before, otherwise length + 2),
  bit-packed 128 to a block, with one start per block of codes in the navigation. A column of one length
  with no null keeps no codes. Dense and sparse; single- or multi-valued.
- Dictionary layout: `StringColumnLayout.DICTIONARY`, an ordinal per slot into a sorted dictionary of the
  terms the column repeats, chosen by coverage under a `DictionaryPolicy`; the uncovered tail escapes into
  a stream of its own. A flush surveys, a merge takes the union of its inputs' dictionaries or sums their
  recorded summaries before surveying again. Ordinals are packed or Zstd-compressed per column, whichever
  the trial shows pays, and carry no value addresses of their own.
- Chunks: `ChunkedBytesWriter`/`ChunkedBytesReader` with `ChunkBounds` (bytes and values) and a
  `ChunkCodec` (identity or native Zstd); the chunk index lives in the navigation.
- Per-field pipeline selection: `NumericPipelineSelector` (`@FunctionalInterface`
  `select(fieldName, type) -> NumericPipelineTemplate`) injected into `ColumNARDocValuesFormat`
  at construction time alongside an explicit `blockSize`. The selector answers "which pipeline
  type?" without knowing the block size; the format applies it via
  `NumericPipelineTemplate.build(int)`. The four named factories (`defaultPipeline`,
  `monotonicLongPipeline`, `doubleGaugePipeline`, `doubleCounterPipeline`) are usable as method
  references: `(f, t) -> NumericPipeline::defaultPipeline`. Server-side wiring into
  `PerFieldFormatSupplier` is a follow-up (see Next).
- Multi-valued string columns, which real keyword fields need. A *slot* is a value or a null. Each layout
  answers "where does this document's run of slots begin?" with a slot count per document in the
  addressing and a base per block of counts in the navigation, written by `AddressingWriter`; finding the
  start is the same question regardless of how the layout names the values. Null representation is
  layout-specific: PLAIN stores a null as its own length code (bytes have no spare value to mean null
  with); DICTIONARY reserves ordinal zero for null, shifting all terms to ordinal one and above, so nothing null reaches the dictionary
  or the escape store and a null lies in no term's ordinal range. The null-slot count is shared in the
  column metadata — a merge reads it off each input and the layout is not chosen until after the count.

## Next

- **Required before the first format bump**: readers must validate recorded ids against the segment
  header version while loading metadata; add v0 fixture reads and a BWC fixture test class.
  While ColumNAR is behind a feature flag and has no stable on-disk compatibility commitment,
  a format-version bump is required only for layout changes, not for id additions.

- **Server-side selector wiring**: implement a concrete `NumericPipelineSelector` in server that
  inspects `FieldType`, `IndexMode`, and `MetricType` to route each field to the correct pipeline
  factory, and wire it into `PerFieldFormatSupplier`.
- **Skip index and a string range query** — the string column writes no skip index, so there is no
  `ColumnarStringRangeQuery` counterpart to `ColumnarNumericRangeQuery`. These are two paths, not one
  mechanism. An ordinal layout can resolve byte bounds against its sorted dictionary once per segment and
  then reuse numeric-style min/max skipping over ordinals. Plain has no ordinals and needs a
  byte-oriented structure (min/max term per interval), which does not exist — and since plain is now what
  every flushed segment writes, whether it gets a skip index at all is the more pressing half of the
  question.

- **Keyword query latency** — `logsdb` has both an inverted index and sorted-set ordinals;
  `logsdb_columnar` has neither, because ColumNAR keeps ordinals inside the binary substrate, so the code
  paths that detect ordinal support fall back to their slow path. The missing-index regression is
  accepted. Whether ordinals recover the rest is unproven, and cold-cache behaviour is part
  of the bar — plain is reliably good there. Rally coverage needs auditing so every keyword query worth
  watching is represented, and the queries we intend to improve annotated, so indexed-versus-ordinal
  effects can be told apart.
- **Benchmark expansion**:
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
- **Numeric merge totals** — the string column no longer counts what it is about to write when the cursor
  already knows: a merge whose inputs are all ColumNAR columns, none of them dropping a deleted document,
  sums the totals each segment recorded rather than walking every input's iterator and addressing tables to
  re-derive per-document counts and add them straight back up. `writeNumericColumn` still counts
  unconditionally. The same shape applies — a `totals()` opt-in on `NumericColumnValues` defaulting to null,
  filled in by `numericMergeCursor` under the same `mergeState.liveDocs[i] == null` gate, since a segment
  with deletions gives up fewer documents than it recorded.
- Multi-segment merge efficiency (sequential merge reads).
- Block-loader binding to ES|QL (server-side adapter).
- **Decompose the write loop**: the single pass in `NumericColumnWriter.write` drives three orthogonal
  consumers (block encoder, skip writer, address table) off shared loop state. A producer/consumer
  split lets each be unit-tested against a controlled `NumericColumnValues` without a `Directory`, and
  removes the round-trip ambiguity where a shared encode/decode bug hides which consumer failed.
- **Assert which vocabulary a merge takes, not only what it produces**: `mergedVocabulary` returns the
  terms and which of `DICTIONARY_UNION`, `COMBINED_SUMMARIES` or `SURVEY` found them, and the tests drive
  all three shapes and read the merged column back value by value. Nothing holds the choice itself, so a
  column that quietly fell back to a survey would pass all of them while costing the merge the work the
  other two exist to save. Asserting it needs a `MergeState` a test can build or observe, which is what
  the seam is meant to make small rather than something to plumb for one assertion.

## Working agreements

- Small self-contained changes proceed directly; anything touching on-disk framing, a frozen id, or
  the read contract is discussed first.
- Every format change ships with correctness tests.
- Every new `BlockTransform` ships with encode and decode entries in
  `EncodeBlockTransformBenchmark` and `DecodeBlockTransformBenchmark`. Add a block shape to
  `NumericData` only if no existing shape exercises the new stage. See `docs/BENCHMARKS.md`.
- Server-tier work (mapping, the binary bridge, synthetic source) lives in other modules.
- Compression comes before query latency. Latency wins are still required for columnar index-mode GA, so
  this is an ordering rather than a trade — but a layout is not justified by latency alone.

