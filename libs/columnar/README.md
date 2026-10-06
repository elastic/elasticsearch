# ColumNAR — a binary doc-values format

ColumNAR is a Lucene `DocValuesFormat` for columnar, analytics-oriented workloads. It stores every
field on one adaptive binary substrate and serves it through its **own** query APIs — a range query
and a block loader — not through Lucene's typed doc-values shapes. It lives in `libs/columnar/` and
depends on `lucene-core`, `libs:simdvec` (vectorized scans) and `libs:zstd` (chunk compression).

**NAR** — Native Adaptive Representation. Values live in a binary substrate; each block is encoded by
a pipeline that picks transforms from the data.

## Terms

Reading a value walks **three** distinct positions, and most confusion in this codec comes from
conflating two of them:

```
doc id  ──presence──▶  rank  ──value-address table──▶  value address  ──block──▶  bytes
```

| Term | Means | Range |
| --- | --- | --- |
| **doc id** | Lucene's document number within the segment. | `[0, maxDoc)` |
| **rank** | Where a document sits among the documents that *have* a value. Identifies a **document**, not a value — two documents with identical bytes have different ranks. | `[0, numDocsWithField)` |
| **value address** | Where a value sits in the column's value store, in write order. | `[0, numValues)` |
| **ordinal** | A term id assigned by a string column's dictionary, as in Lucene. Internal to the dictionary layout; never use it for a rank or a value address. | `[0, escapeOrdinal]` |

A single-valued column maps a rank straight to a value address, so the middle arrow is identity and
the two are numerically equal — which is exactly why they need different names. A multi-valued column
resolves a rank to a *range* of value addresses through the value-address table.

Vocabulary for the storage layer:

| Term | Means |
| --- | --- |
| **block** | The **encoding** and addressing quantum, counted in **values**. 128 for bit-packed longs, because the `ForUtil` kernels unroll over it and FOR needs the group's min/max. What locates a block is kept once per block, in the navigation. |
| **chunk** | The **compression** unit, bounded by **bytes** and by a value count, whichever it reaches first. Cut wherever the byte bound falls, so a value or a block may span two chunks; the reader puts it back together. |
| **layout** | How a column encodes its values, recorded as a frozen id in metadata so a later layout arrives without a format bump. |
| **presence** | Which documents have a value — dense (every document) or sparse (an `IndexedDISI`). Turns a doc id into a rank. |
| **table** | A `DirectMonotonic` table (block offsets, value addresses, chunk starts, length-block starts), written straight into its file and read off-heap. |
| **pipeline** | A numeric column's encoding chain: ordered **transforms** (delta, offset, GCD) then one **terminal** (FOR bit-packing). |
| **skip index** | Per-interval min/max held *inside* the column, since a `BINARY` field cannot carry a Lucene skipper. |

Blocks and chunks address **values**, not documents: sparsity is absorbed one level up, in
doc id → rank.

## Surface

Every field is a `BinaryDocValues` field whose `ColumnarFieldType` the integration supplies through an
injected `ColumnarFieldTypeSelector` (and which is re-read from the column metadata at read time):

- **`LONG` / `DOUBLE`** — packed as a `NumericBinaryPayload` and stored on the adaptive long column
  (a double arrives as a sortable long). Read back through:
  - `ColumnarNumericRangeQuery` — a self-contained Lucene range query, vectorized and skipper-aware;
  - `ColumnarNumericBinaryDocValues.bulkLongs` — column-at-a-time reads for aggregation/block loading;
  - `binaryValue()` — re-emits the payload for a classic binary consumer.
- **`STRING` (keyword, text)** — single- or multi-valued, nulls included, read back through
  `binaryValue()`, which hands back the bytes the mapper wrote, and through the column's own term,
  prefix, pattern and block-loader paths. A column is plain or a dictionary (see Storage); its ordinals
  stay internal, so the read API stays binary either way.

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

**Frozen ids.** Every stage has a frozen `byte` id recorded in column metadata. Adding an encoder does
not require a `FormatVersion` bump: an unknown id fails loudly at first field access. Once shipped, ids
must never be reused or renumbered. Because a column records
the ids it was written with, older data lists only old ids and a newer reader rebuilds unchanged.

## Storage

- **Presence.** Which documents hold a value, and a document's rank. A dense column stores nothing per
  document (doc id == rank); a sparse column reuses Lucene `IndexedDISI`. Supplies the `intoBitSet`
  fast path and is shared by every column type.
- **Numeric column.** Single- and multi-valued in one store: values in written order (never
  reordered) in configurable fixed-size blocks (default 128) in the data, their block offsets in the
  navigation, and — only when multi-valued — a per-document value-address table in the addressing. A block decodes whole into a reused buffer with a single-block
  cache; the range and bulk paths read straight out of it.
- **String column.** Two layouts, chosen per column from what the column repeats:
  - `StringColumnLayout.PLAIN` stores the values' bytes one after another in chunks, and a code per slot
    in the lengths file: `0` for a null, `1` for the value of the slot before (stored once), otherwise two
    more than the byte count. The codes are bit-packed 128 to a block, and the navigation keeps where each
    block of 128 begins, so a read decodes one small block of codes and a block of values is one span of
    bytes. A column whose values all have one length and no null keeps no codes: a value begins at its
    address times that length.
  - `StringColumnLayout.DICTIONARY` stores an ordinal per slot into a sorted dictionary of the terms the
    column repeats; a value the dictionary does not hold escapes into a stream of its own. Ordinal `0`
    names a null.

  Where a document's slots begin is shared by both: a slot count per document in the addressing, with a
  base per block of counts in the navigation. No skip index yet.
- **Skip index.** Range pushdown lives inside the column (a `BINARY` field can't carry a Lucene
  skipper): a multi-level per-interval min/max index the range query consults.

## Files

A segment writes one file of each kind, shared by every field, split by how they are read:

- **`.cnm` — metadata.** One fixed-size record per column: offsets, lengths, counts, the pipeline's
  stage ids, and the `DirectMonotonic` table headers. Read in full when the segment opens, and the
  only part held on the heap — so nothing in it may scale with the column.
- **`.cnd` — data.** The values: string chunks, numeric blocks, dictionary terms, ordinals and escapes.
- **`.cna` — addressing.** What is kept per document: presence (`IndexedDISI`), slot counts, numeric
  value addresses.
- **`.cnl` — lengths.** A plain column's per-slot length codes.
- **`.cnn` — navigation.** What is kept per block or per chunk and locates everything else: block
  offsets, the chunk index, slot-count bases, escape ranks, length-block starts. Every read consults it,
  so it is opened as an index file and fetched whole by a cache that warms metadata files; it stays a
  small fraction of the columns.
- **`.cns` — skip index.** The per-column multi-level min/max structure, which a range query scans to
  choose which intervals to visit before touching any value bytes.

Everything but `.cnm` is read through the mapped input, never materialized.

## Memory & versioning

Nothing column-proportional is on the heap — read, write and merge stream one block at a time; tables
use the `DirectMonotonic` layout, written straight into their file and read from a mapped slice. Only a
dictionary column stages anything in a temporary file: its ordinals and escapes, for its second pass.
Each segment carries a format version stamp in every file's header. The on-disk component ids
(field type, block encoding, block-bytes codec, skip-index codec) are frozen once shipped and must
never be reused or renumbered. A format bump is required for layout changes — new fields in `readFrom`,
different block framing, changed offset-table encoding — not for id additions: an unknown id already
fails loudly at first field access. See `AGENTS.md` for full policy.

## Chunks

Compression is a layer below encoding, in `ChunkedBytesWriter`/`ChunkedBytesReader`. A column's byte
stream is written as **chunks**, each compressed whole by a `ChunkCodec` (identity or Zstd, on frozen
ids) and closed by a byte target or a value count, whichever it reaches first. A **block** stays what it
was — a fixed count of values, addressed by its offset in the *uncompressed* stream — and a chunk is cut
wherever its byte bound falls, so a block may straddle two chunks and the reader stitches that range.
Two `DirectMonotonic` tables in the navigation locate a chunk: its start in the uncompressed stream, and
where it landed in the file.

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
