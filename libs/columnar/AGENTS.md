# ColumNAR — contributor & agent guide

Read `README.md` for the architecture first, then this. It covers what is expensive to get wrong.

## Non-negotiable rules

1. **Binary only.** ColumNAR stores and serves fields at the `BINARY` surface (`addBinaryField` /
   `getBinary`); the typed shapes (`Numeric`/`SortedNumeric`/`Sorted`/`SortedSet`) throw. There is no
   delegate format — an unsupported type is an error, not a fallback.

2. **Type-tagged and open.** Every field carries a `ColumnarFieldType` (`columnar.type` attribute).
   `LONG`/`DOUBLE` are the numeric column today; new types (`STRING`, …) slot in by extending
   the write dispatch (consumer) and read dispatch (producer) — the field framing is generic.

3. **The integration chooses the encoding.** Encoding is a per-field decision driven by what the
   integration knows (type, sorted, metric role). Keep that seam open; don't hard-wire one pipeline.

4. **Insertion order is preserved.** The numeric column never sorts or deduplicates; value ordinals
   stay internal to the presence layer.

5. **Never hold a column on the heap.** Read, write and merge stream one block at a time. Offset
   tables use `DirectMonotonic` (temp file on write, mapped slice on read); presence uses
   `IndexedDISI`. Only bounded metadata and one decode block stay in memory.

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
- Dependencies stay minimal (`lucene-core`, `libs:simdvec`) and need justification. Never depend on
  `server` — `server` will depend on this library.

The repo-wide top-level `AGENTS.md` governs formatting, logging, Javadoc, and license headers.
