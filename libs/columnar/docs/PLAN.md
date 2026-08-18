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
- **Blocks are derived by byte size, not only by value count.** A block is an *encoding* unit, a chunk is
  a *compression* unit; see the chunking item under Next.
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
- Keyword (string) column: `ColumnarFieldType.STRING` served at `getBinary` through
  `ColumnarStringBinaryDocValues`. `StringColumnLayout.PLAIN` stores the values as one byte blob plus a
  `DirectMonotonic` table holding every value's offset, so a read is two offset lookups and a read of
  exactly that span, and a length needs no prefix on disk. No block, and so no block size, block cache or
  byte codec on this path — a block belongs to an encoding defined over a group, which plain is not. Dense
  and sparse; single-valued only (see Next). The POC's dictionary path is deliberately not carried over —
  the layout is decided from statistics at merge rather than from a per-segment probe, so ordinals arrive
  as a later layout id (see Next).
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
- **String ordinals, across the flush/merge split.** Plain ships first — the compression story comes
  before anything else — and the ordinal layout arrives later as a new layout id, decided from statistics
  rather than from a per-segment probe. The work splits on flush versus merge rather than on low versus
  high cardinality, and the interface between the halves is the statistics format, agreed upfront so they
  proceed independently.

  1. **Flush** — write plain and emit per-value frequency statistics. A histogram was considered and
     rejected: strings have no natural bucketing, and it costs more than it tells us. **Open:** a
     per-value frequency map is column-proportional heap on a high-cardinality field, which is what the
     no-column-on-the-heap rule forbids, so the format needs a bound — a cap with a spill, a sketch, or
     sampling. Settle that when the format is agreed, not while implementing against it.
  2. **Merge** — compute coverage over the accumulated statistics, pick the budget, build the sorted
     dictionary, re-encode. Expressing the budget in bytes rather than a term count ties it to the
     byte-derived block rule and is measurable directly from `BytesRef` lengths; the units are endorsed
     but not settled. Statistics should accumulate across merge generations so each starts better
     informed than the last.

  Two things to carry forward. The terms dictionary belongs in the data file, read on demand and
  prefix-compressed, rather than in the meta stream as a `BytesRef[]` materialised at segment open:
  metadata is read for every field in every segment whether the field is queried or not, so a resident
  dictionary scales with fields × segments. And a merge that unions ordinal segments can remap source
  ordinals instead of rehashing every value — worth measuring rather than assuming, and noting that
  building the destination dictionary from the source dictionaries retains terms whose documents were all
  deleted.

- **Move chunking into the byte codec.** One `blockSize` in values currently does two jobs. A *block* is
  an encoding quantum: 128 for bit-packed longs, because the `ForUtil` kernels unroll over it and FOR
  needs the group's min/max. A *chunk* is a byte-bounded compression unit and applies to any column
  whatever sits underneath. Both are real in every combination — ordinals under zstd want the 128 quantum
  *and* a byte-bounded chunk on top; plain bytes want a chunk and no block at all. So the column emits
  values or blocks plus offsets, and the byte codec decides how much to accumulate before emitting:
  identity means no chunks, zstd means byte-bounded chunks plus an index storing the global uncompressed
  offset, which stays monotonic where a (chunk, offset-within-chunk) pair would not. `BlockBytesCodec` is
  named for the wrong level, and 128 is a quantum rather than a size — `DocValuesForUtil.encode` already
  walks any multiple of it. Blocks and chunks address **values**, not documents, so sparsity is absorbed
  a level up in docId → rank.

- **Multi-valued string columns** — required for real keyword fields, which are commonly arrays. The
  column is single-valued today: `ColumNARDocValuesConsumer.writeStringColumn` rejects a document
  carrying more than one value, and `StringColumnReader` asserts the same. The substrate already supplies
  presence and a value-address table, so this mirrors what `NumericColumnWriter` does — the string
  metadata already carries `numValues` separately from `numDocsWithField` for exactly this. One thing to
  carry over rather than rediscover: `ColumnarStringBinaryDocValues.binaryValue` relies on the reader
  handing back one reused `BytesRef` per call, so collecting several values before encoding would alias
  them onto the last one; either copy each value out or encode into the payload while walking the value
  addresses. An assert marks the spot.

- **Skip index and a string range query** — the string column writes no skip index, so there is no
  `ColumnarStringRangeQuery` counterpart to `ColumnarNumericRangeQuery`. These are two paths, not one
  mechanism. An ordinal layout can resolve byte bounds against its sorted dictionary once per segment and
  then reuse numeric-style min/max skipping over ordinals. Plain has no ordinals and needs a
  byte-oriented structure (min/max term per interval), which does not exist — and since plain is now what
  every flushed segment writes, whether it gets a skip index at all is the more pressing half of the
  question.

- **Ordinal encoder selection** — when the ordinal layout lands its stream needs an encoder chosen for it.
  The obvious candidate to measure against is `NumericBlockEncoder.encodeOrdinals` / `decodeOrdinals` (the
  run / two-run / cycle / bit-packed codec), which is present but unused and does not care whether
  ordinals are monotonic. That makes it the likely answer if the lexicographic sort costs ordinal-stream
  footprint. Routing the choice through `NumericPipelineSelector` versus giving ordinals a dedicated
  pipeline is untested either way.

- **Keyword query latency** — `logsdb` has both an inverted index and sorted-set ordinals;
  `logsdb_columnar` has neither, because ColumNAR keeps ordinals inside the binary substrate, so the code
  paths that detect ordinal support fall back to their slow path. The missing-index regression is
  accepted. Whether ordinals recover the rest is unproven, and cold-cache behaviour in serverless is part
  of the bar — plain is reliably good there. Rally coverage needs auditing so every keyword query worth
  watching is represented, and the queries we intend to improve annotated, so indexed-versus-ordinal
  effects can be told apart.
- **Block compression via native Zstd**: add Zstd as the terminal byte codec (its own frozen id, applied
  after the encoding stage, so it stays additive and BWC), backed by
  `org.elasticsearch.nativeaccess.Zstd` rather than a Java LZ4. Most useful on the low-entropy stages
  (terms dictionary, plain keyword bytes). This is the codec that wants byte-bounded chunks, so it lands
  with or after the chunking item above.
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
- Compression comes before query latency. Latency wins are still required for columnar index-mode GA, so
  this is an ordering rather than a trade — but a layout is not justified by latency alone.

