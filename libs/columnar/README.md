# ColumNAR — a binary doc-values format

ColumNAR is a Lucene `DocValuesFormat` for columnar, analytics-oriented workloads. It stores every
field on one adaptive binary substrate and serves it through its **own** query APIs — a range query
and a block loader — not through Lucene's typed doc-values shapes. It lives in `libs/columnar/` and
depends on `lucene-core` plus `libs:simdvec` (vectorized range scan).

**NAR** — Native Adaptive Representation. Values live in a binary substrate; each block is encoded by
a pipeline that picks transforms from the data.

## Surface

Every field is a `BinaryDocValues` field tagged with a `ColumnarFieldType` (the `columnar.type`
`FieldInfo` attribute), set by the integration:

- **`LONG` / `DOUBLE`** — packed as a `NumericBinaryPayload` and stored on the adaptive long column
  (a double arrives as a sortable long). Read back through:
  - `ColumnarNumericRangeQuery` — a self-contained Lucene range query, vectorized and skipper-aware;
  - `ColumnarNumericBinaryDocValues.bulkLongs` — column-at-a-time reads for aggregation/block loading;
  - `binaryValue()` — re-emits the payload for a classic binary consumer.
- **`STRING` (keyword)** — values stored plain in blocks, read back through `binaryValue()`, which re-emits
  them as a `StringBinaryPayload`. Single-valued for now (a document with more than one value is rejected at
  write time). An ordinal layout is planned as a second layout id, decided at merge from statistics a flush
  emits rather than from a per-segment probe; ordinals will stay internal, so the read API stays binary
  either way. See `docs/PLAN.md`.

The typed shapes (`Numeric`, `SortedNumeric`, `Sorted`, `SortedSet`) are **not** this library's
surface: they throw. There is no delegate format — a type it can't handle is an error. A typed view,
where a classic consumer needs one, is a bridge above this format.

## Encoder pipeline

Each block goes through a `NumericPipeline`: an ordered chain of **transforms** then one **terminal**.

- A `BlockTransform` (delta, offset, GCD) is an adaptive, reversible, in-place transform on the block;
  it fires only when it shrinks the block. A per-block fire-bitmask records which fired.
- A `BlockTerminal` (FOR bit-packing) serializes the residual longs to bytes.

The pipeline is chosen per field: the default (`NumericPipeline.defaultPipeline` = delta, offset, GCD +
FOR) runs all detection, while a field with a known shape can be handed an explicit pipeline to skip
it. A column records its stage ids in metadata, and the per-block layout is self-describing —
`fireBitmask`, terminal payload, then each fired transform's params in reverse order.

**Frozen ids.** Every stage has a frozen `byte` id recorded in column metadata. Adding an encoder
requires a `FormatVersion` bump; without one, old readers fail mid-decode on the unknown id instead
of at header open. Once shipped, ids must never be reused or renumbered. Because a column records
the ids it was written with, older data lists only old ids and a newer reader rebuilds unchanged.

## Storage

- **Presence.** Which documents hold a value, and a document's value ordinal. A dense column stores
  nothing per document (doc id == ordinal); a sparse column reuses Lucene `IndexedDISI`. Supplies the
  `intoBitSet` fast path and is shared by every column type.
- **Numeric column.** Single- and multi-valued in one store: values in written order (never
  reordered) in configurable fixed-size blocks (default 128), a block offset table, and — only when multi-valued — a
  per-document value-address table. A block decodes whole into a reused buffer with a single-block
  cache; the range and bulk paths read straight out of it.
- **String column.** Under `StringColumnLayout.PLAIN` a block holds `[vint length][bytes]` per value, and a
  `DirectMonotonic` table holds each block's byte offset. Per block rather than per value, so the table costs a
  fraction of the column and a scan pays one bulk read per block instead of one read per value; a block decodes
  whole into a reused buffer with a single-block cache, as the numeric column does. The layout id is the
  extension point a later ordinal layout arrives on. No skip index yet.
- **Skip index.** Range pushdown lives inside the column (a `BINARY` field can't carry a Lucene
  skipper): a multi-level per-interval min/max index the range query consults.

## Memory & versioning

Nothing column-proportional is on the heap — read, write and merge stream one block at a time; offset
and address tables use `DirectMonotonic` (temp file on write, mapped slice on read). Each segment
carries a format version stamp in both the `.cnd` and `.cnm` headers. The on-disk component ids
(field type, block encoding, block-bytes codec, skip-index codec) are frozen once shipped and must
never be reused or renumbered. A format bump is required for layout changes — new fields in `readFrom`,
different block framing, changed offset-table encoding — not for id additions: an unknown id already
fails loudly at first field access. See `AGENTS.md` for full policy.

## Chunks

Compression is a layer below encoding, in `ChunkedBytesWriter`/`ChunkedBytesReader`. A column's byte
stream is written as **chunks**: byte-bounded units, each compressed whole by a `ChunkCodec` (identity
or Zstd, on frozen ids). A **block** stays what it was — a fixed count of values, addressed by its
offset in the *uncompressed* stream — and a chunk only ever closes on a block boundary, so no block
straddles two chunks and no read spans more than one. Two `DirectMonotonic` tables locate a chunk: its
start in the uncompressed stream, and where it landed in the file.

Sizing the compression unit in bytes rather than values is what makes the ratio independent of the
data: a 128-value block of 20-byte values gives a codec 2.5 KB to work with, the same block of
200-byte values gives it 25 KB. Under the identity codec the uncompressed stream is the file, so the
chunk layer is a pass-through and values are read straight from the mapped input.

Zstd runs through the native `org.elasticsearch.nativeaccess.Zstd` binding rather than a Java
implementation, and both directions hand it memory it addresses directly — the compressed bytes as a
`MemorySegment` slice of the mapped file, the decoded chunk as a heap array passed through a critical
downcall.

See `docs/PLAN.md` for the roadmap, `docs/BENCHMARKS.md` for the benchmarks, and `AGENTS.md` for
conventions.
