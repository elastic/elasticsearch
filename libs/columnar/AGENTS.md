# ColumNAR — contributor & agent guide

Read `README.md` for the architecture first, then this. It covers what is expensive to get wrong.

## Non-negotiable rules

1. **Binary only.** ColumNAR stores and serves fields at the `BINARY` surface (`addBinaryField` /
   `getBinary`); the typed shapes (`Numeric`/`SortedNumeric`/`Sorted`/`SortedSet`) throw. There is no
   delegate format — an unsupported type is an error, not a fallback.

2. **Type-tagged and open.** Every field carries a `ColumnarFieldType`, resolved by an injected
   `ColumnarFieldTypeSelector` at write time and re-read from the column metadata at read time.
   `LONG`/`DOUBLE` are the numeric column and `STRING` is the string column; further types slot in by
   extending the write dispatch (consumer) and read dispatch (producer) — the field framing is generic.
   The type tag names the *logical* type only. How a column encodes within that type — a numeric
   pipeline, or the layout a string column picked — is internal to the column and lives in its own
   metadata; never widen the type enum to express an encoding choice.

3. **The integration chooses the encoding.** Encoding is a per-field decision driven by what the
   integration knows (type, sorted, metric role). Keep that seam open; don't hard-wire one pipeline.

4. **Insertion order is preserved.** No column sorts or deduplicates its values; a value address is
   assigned in written order and stays internal to the column.

5. **Never hold a column on the heap.** Read, write and merge stream one block at a time. Tables use
   the `DirectMonotonic` layout, written straight into their file by `MonotonicWriter` (no entry count
   up front, and several tables may share a file) and read off-heap from a mapped slice; presence uses
   `IndexedDISI`. Only bounded metadata and one decode block stay in memory. Note that "bounded" is not
   the same as "small enough": metadata is read for every field in every segment whether the field is
   queried or not, so anything resident scales with fields × segments. A per-field structure earns its
   place in the meta stream only if it is needed to open the column at all; everything else belongs in
   one of the content files, read on demand.

6. **No temporary files, except where a second pass needs one.** Each content file has one stream writing
   into it at a time while a column is written, so values and tables go into their files as they are
   produced. The dictionary layout is the exception: it stages its ordinals and its escapes, which it
   replays once their count is known. A new structure streams too; staging needs a reason.

7. **Metadata and content live in different files**, one of each per segment and shared by every field:

   | file | holds | read |
   | --- | --- | --- |
   | `.cnm` metadata | fixed-size per-column records | in full at open; the only part on the heap |
   | `.cnd` data | values: string chunks, numeric blocks, dictionary terms, ordinals, escapes | on demand |
   | `.cna` addressing | per document: presence, slot counts, numeric value addresses | on demand |
   | `.cnl` lengths | a plain column's per-slot length codes | on demand |
   | `.cnn` navigation | per block or per chunk: block offsets, the chunk index, slot bases, escape ranks, length-block starts | up front (index file) |
   | `.cns` skip index | per-interval min/max | before the values |

   A structure goes where its read pattern puts it. Navigation is opened as an index file and fetched
   whole by a cache that warms metadata files, so it must stay coarse — per block or per chunk, never per
   document or per value — and a small fraction of the columns. Anything per document goes in the
   addressing. `.cnm` must not grow with the data.

## Chunks

`ChunkedBytesWriter`/`ChunkedBytesReader` sit below the encoders: they store a column's byte stream as
chunks, each compressed whole by a `ChunkCodec` on a frozen `byte` id, and closed by whichever of its
`ChunkBounds` it reaches first — a byte target or a value count. A chunk is cut wherever the byte bound
falls, inside a value if need be, so a range the caller addresses may span two chunks and the reader
puts it back together. A caller calls `boundary()` ahead of the values it is about to append, so the
value bound counts them.

Two rules to keep: the byte bound is what sizes the compression unit, so the ratio does not move with
value width; and nothing the writer holds grows with the column — one chunk is buffered, and the chunk
index streams into the navigation as chunks are cut.

## Encoders

A block is encoded by a `NumericPipeline`: adaptive `BlockTransform`s (delta, offset, GCD — reversible
in-place transforms that fire only when they shrink the block) then one `BlockTerminal` (FOR
bit-packing) that serializes the residuals. The default pipeline runs all detection; a field can be
handed an explicit pipeline to skip it.

**Adding an encoder** — additive and backward-compatible:

1. Implement `BlockTransform` (adaptive, mutates the `long[]` in place) or `BlockTerminal` (serializes
   it) with a new, **frozen** `byte` id.
2. Register the id in `NumericPipeline.Registry`.
3. Add it to a pipeline — the default or a per-field one.

A column records its stage ids in metadata, so old data lists only old ids and a newer reader rebuilds
the exact pipeline and decodes it unchanged. Never reuse or renumber a shipped id. An unknown id
already fails loudly at first field access; a format bump is not required for id additions alone.

## Versioning

Each segment stamps a `FormatVersion` in both headers; `ColumnarCodecUtil.checkHeader` returns it
and threads it through `readFrom`. Three tiers: format version (header `int`), frozen column ids
(`byte`, per-column), encoding bitmask (`vint`, per-block). Bump `FormatVersion.CURRENT` on layout
changes — new fields in `readFrom`, different block framing, a different offset-table encoding. Those
parse silently and return wrong values on old readers; a header bump turns that into
`IndexFormatTooNewException` at segment open. Id additions do not require a bump. See `FormatVersion`
Javadoc for full policy.

## Benchmarks & tests

Ship every format change with round-trip and range/bulk correctness coverage. JMH benchmarks live
in the `:benchmarks` module (`org.elasticsearch.benchmark.index.codec.columnar`) and compare ColumNAR
against the TSDB codecs; no results are committed. See `docs/BENCHMARKS.md`.

## Build & verify

- `./gradlew :libs:columnar:spotlessApply` — format (no wildcard imports; don't reorder untouched lines).
- `./gradlew :libs:columnar:test` — tests.
- Dependencies stay minimal (`lucene-core`, `libs:simdvec`, `libs:zstd`, `libs:lucene-store`, `libs:core`)
  and need justification. Never depend on
  `server` — `server` will depend on this library.

The repo-wide top-level `AGENTS.md` governs formatting, logging, Javadoc, and license headers.
