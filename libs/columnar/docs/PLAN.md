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

## Next

- **Per-field pipeline selection**: let a field carry its pipeline — the ordered list of encoder ids —
  via an attribute, instead of always using the default. Metadata already records the list and decode
  already rebuilds it (`NumericPipeline.Registry` maps id → stage); this just adds the write-side
  selector, so counter (`SplitDelta`), ALP-for-doubles, etc. can be chosen and plugged in.
- **String columns**: ordinal + terms-dictionary (prefix + block-LZ4), ordinals internal.
- Multi-segment merge efficiency (sequential merge reads).
- Terminal LZ4/Zstd byte codecs behind the frozen `BlockBytesCodec` ids.
- Block-loader binding to ES|QL (server-side adapter).

## Working agreements

- Small self-contained changes proceed directly; anything touching on-disk framing, a frozen id, or
  the read contract is discussed first.
- Every format change ships with correctness tests.
- Server-tier work (mapping, the binary bridge, synthetic source) lives in other modules.
