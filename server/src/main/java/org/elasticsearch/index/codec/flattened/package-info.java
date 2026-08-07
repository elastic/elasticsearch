/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * Columnar doc values format for flattened fields.
 *
 * <h2>Goal</h2>
 *
 * <p>Stores the content of a flattened field's {@code ._keyed} binary field in a columnar layout
 * on disk. The {@code ._keyed} blob for each document contains an enumeration of
 * {@code (sub-field name, value)} pairs encoded as
 * {@code [vint prefix][key bytes][0x00][value bytes]}, where the prefix encodes {@code isNull} and
 * value length. Instead of storing all pairs for a document as one contiguous blob (row layout),
 * the columnar format stores all values for a given sub-field adjacent to one another (column
 * layout). This makes single-sub-field access cheap: only that column's blocks are decompressed;
 * the rest are never touched.
 *
 * <p>The format is selected by {@code PerFieldFormatSupplier} for {@code ._keyed} fields of
 * {@code flattened} fields that have {@code layout: columnar} in their mapping. The row layout
 * remains the default.
 *
 * <h2>On-disk layout</h2>
 *
 * <p>Two files per segment (extensions {@code .fdvd} / {@code .fdvm}):
 *
 * <pre>
 * .fdvd  (data file)
 *   [Lucene index header]
 *   per field, in field-number order:
 *     [column 0][column 1] ... [column K-1]   — one column per unique sub-field, lex order
 *     [DISI bitset + jump table]              — only for sparse fields; omitted when dense
 *     [key dictionary]
 *     [column address table]
 *   [Lucene footer with checksum]
 *
 * .fdvm  (meta file)
 *   [Lucene index header]
 *   per field:
 *     int   fieldNumber
 *     byte  FLATTENED_COLUMNAR_BINARY (0x00)
 *     long  dataOffset
 *     long  docsWithFieldOffset        (-2 = empty, -1 = dense, else DISI offset)
 *     long  docsWithFieldLength
 *     short jumpTableEntryCount        (-1 for dense/empty)
 *     byte  denseRankPower             (-1 for dense/empty)
 *     int   numDocsWithField
 *     int   numKeys
 *     long  keyDictOffset
 *     long  keyDictLength
 *     long  columnAddressTableOffset
 *     vint  maxUncompressedBlockLen    — for reader buffer pre-sizing
 *     vint  maxDocsPerBlock            — for reader buffer pre-sizing
 *     long  dataLength
 *   int -1  (FIELD_EOF sentinel)
 *   [Lucene footer with checksum]
 * </pre>
 *
 * <h2>Column layout</h2>
 *
 * <p>Each column stores one sub-field's values for all documents in the segment that have that
 * sub-field. Columns are written in ascending lex ordinal (= lex rank) order.
 *
 * <pre>
 * [block 0][block 1] ... [block N-1]
 * [int firstDocId, int blockStartRelative] × N   — block index, 8 bytes/entry
 * </pre>
 *
 * <p>{@code blockStartRelative} is the byte offset of the block from the column's own start in the
 * data file. The block index is appended after all blocks by {@link
 * org.elasticsearch.index.codec.flattened.FieldBlockWriter#finish()}.
 *
 * <h2>Block layout</h2>
 *
 * <pre>
 * byte  flags
 *         bit 0 = FLAG_VALUES_COMPRESSED    value region is ZSTD-compressed
 *         bit 1 = FLAG_DOCS_CONTIGUOUS      docIds are consecutive; delta array omitted
 *         bit 2 = FLAG_ALL_SINGLE_SLOT      every doc has exactly one slot; count array omitted
 *         bit 3 = FLAG_NO_NULL_VALUES       no slot in this block is null
 *         bit 4 = FLAG_META_COMPRESSED      metadata region is ZSTD-compressed
 * vint  numDocs
 * byte  bitsPerDelta                        absent when FLAG_DOCS_CONTIGUOUS
 * bit-packed (gap-1) × (numDocs-1)          absent when FLAG_DOCS_CONTIGUOUS; MSB-first, bitsPerDelta bits each
 * vint  metaLen                             uncompressed byte length of the metadata region
 * metadata region:
 *   if FLAG_META_COMPRESSED:
 *     vint compressedLen
 *     compressedLen bytes                   written by ZstdCompressionMode.ZstdCompressor
 *   else:
 *     metaLen raw bytes
 *   decoded content (exactly metaLen bytes):
 *     byte  bitsPerSlotCount                absent when FLAG_ALL_SINGLE_SLOT
 *     bit-packed slotCount × numDocs        absent when FLAG_ALL_SINGLE_SLOT; MSB-first
 *     byte  bitsPerValueLen                 always present
 *     bit-packed encodedLen × numSlots      MSB-first, bitsPerValueLen bits each;
 *                                           FLAG_NO_NULL_VALUES set:   encodedLen = valueLen
 *                                           FLAG_NO_NULL_VALUES clear: encodedLen = 0 for null, valueLen+1 otherwise
 * value region (raw value bytes concatenated; total = sum(valueLen)):
 *   if FLAG_VALUES_COMPRESSED:
 *     vint compressedLen
 *     compressedLen bytes                   written by ZstdCompressionMode.ZstdCompressor
 *   else:
 *     raw value bytes
 * </pre>
 *
 * <p>The docId-delta array stays outside any compressed region so that block skipping and
 * doc-presence checks never decompress anything. The slot-count and value-length arrays live in
 * a separate small ZSTD frame (the metadata region) ahead of the value region: they are only
 * needed once a doc is known to be present, and bit-packed lengths sized by the block's longest
 * value are near-incompressible when stored raw — ZSTD recovers that cost almost entirely. The
 * two regions are kept separate so that a slot-count query does not force decompression of the
 * much larger value region. The value region contains only concatenated raw value bytes — no
 * per-slot framing.
 *
 * <p>Flush triggers: a new block is started when {@code numDocs >= MAX_DOCS_PER_BLOCK} (default
 * 8192) or {@code blockValuesLen >= TARGET_BLOCK_BYTES} (default 64 KiB). The check fires at the
 * start of each new document so a single document's slots are never split across blocks.
 *
 * <h2>Key dictionary</h2>
 *
 * <pre>
 * vint numKeys
 * per key in lex order:
 *   vint keyLen
 *   keyLen bytes
 * </pre>
 *
 * <h2>Column address table</h2>
 *
 * <p>One fixed-width entry per key, in lex ordinal order:
 *
 * <pre>
 * long columnStartOffset        — absolute data-file position of the column's first block
 * int  blockIndexRelativeOffset — offset of the block index from columnStartOffset
 * int  numBlocks
 * </pre>
 *
 * <p>Entry size: 16 bytes ({@code COLUMN_ADDRESS_ENTRY_BYTES}). The fixed width enables O(1)
 * lookup by lex ordinal without a separate index.
 *
 * <h2>Write path</h2>
 *
 * <h3>Flush ({@link org.elasticsearch.index.codec.flattened.FlattenedDocValuesConsumer#addBinaryField addBinaryField})</h3>
 *
 * <ol>
 *   <li>Walk {@code BinaryDocValues} once. For each document, parse the {@code ._keyed} blob to
 *       extract {@code (keyString, valueBytes)} pairs. Intern key strings via {@code BytesRefHash}
 *       to get a compact {@code keyOrd}. Feed {@code (keyOrd, docId, preEncodedSlotBytes)} triples
 *       to a {@link org.elasticsearch.index.codec.flattened.SortedSlotAccumulator}.</li>
 *   <li>After all documents are scanned, call {@code BytesRefHash.sort()} to obtain the
 *       {@code lexRankOf} mapping.</li>
 *   <li>Call {@link org.elasticsearch.index.codec.flattened.SortedSlotAccumulator#sortedCursor sortedCursor(lexRankOf)}:
 *       returns records sorted by {@code (lexRank, docId)}.
 *       <ul>
 *         <li>If the accumulated data fits in {@code MAX_BUFFERED_BYTES} (default 32 MiB): sort
 *             an index array in memory and return an {@code InMemoryCursor}. No temp files
 *             created.</li>
 *         <li>Otherwise: partition the buffer into sorted-run temp files (each
 *             {@code ≤ MAX_BUFFERED_BYTES}, sorted by {@code (lexRankOf[keyOrd], docId)} using
 *             the now-known mapping) and return a k-way merge cursor ({@code MergeCursor}) backed
 *             by a {@code PriorityQueue}.</li>
 *       </ul>
 *   </li>
 *   <li>Stream the sorted cursor to a {@link org.elasticsearch.index.codec.flattened.FieldBlockWriter}
 *       per sub-field. {@code FieldBlockWriter} writes blocks directly into the shared data
 *       {@code IndexOutput} (no temp files, no splice step).</li>
 *   <li>Write the DISI bitset, key dictionary, and column address table; then write the meta
 *       record.</li>
 * </ol>
 *
 * <h3>Merge ({@link org.elasticsearch.index.codec.flattened.FlattenedDocValuesConsumer#mergeBinaryField mergeBinaryField})</h3>
 *
 * <p>When all source segments are columnar (same {@code VERSION_CURRENT}), a column-wise merge
 * is used:
 *
 * <ol>
 *   <li>K-way merge of per-segment key dictionaries (already lex sorted) yields the merged lex
 *       order.</li>
 *   <li>A presence pass walks {@code getMergedBinaryDocValues} calling only {@code nextDoc()}
 *       (never {@code binaryValue()}) to populate the DISI accumulator — deletes and index sorting
 *       are handled for free.</li>
 *   <li>For each merged key in lex order, a {@code DocIDMerger} over per-segment
 *       {@code SequentialColumnReader}s drives the block writer. Slot bytes are bulk-copied from
 *       the decompressed source block into the writer's buffer without per-value parsing.</li>
 * </ol>
 *
 * <p>When any source segment is not columnar, or when {@code es.flattened.mergeColumnWise=false}
 * is set, the inherited default from {@code DocValuesConsumer} is used (blob round-trip via
 * {@code addBinaryField}).
 *
 * <h2>Read path</h2>
 *
 * <p>{@link org.elasticsearch.index.codec.flattened.FlattenedDocValuesProducer} opens the data
 * and meta files, parses per-field metadata, and returns
 * {@link org.elasticsearch.index.codec.flattened.ColumnarKeyedBinaryDocValues} for
 * {@code getBinary}. The producer keeps one {@code IndexInput} clone per column open on demand.
 *
 * <p>{@code ColumnarKeyedBinaryDocValues.binaryValue()} reconstructs the per-doc blob by walking
 * columns in lex ordinal order and concatenating each sub-field's slot bytes. Blobs are therefore
 * not byte-identical to the row format when the original JSON key order differs from lex order;
 * this is safe for all current consumers ({@code DV queries},
 * {@code FlattenedDocValuesSyntheticFieldLoader}, and
 * {@code KeyFilteredSortingArrayOrderBinaryDocValues} which sorts and deduplicates).
 *
 * <p>{@link org.elasticsearch.index.mapper.flattened.KeyedFlattenedDocValuesBlockLoader} resolves
 * a single column by binary-searching the key dictionary, then reads only that column's blocks.
 * For ES|QL block loading, the loader tries a batch path first:
 *
 * <ol>
 *   <li>{@link org.elasticsearch.index.codec.flattened.ColumnarKeyedBinaryDocValues#keyColumnReader}
 *       resolves the key ordinal once and returns a
 *       {@link org.elasticsearch.index.mapper.BlockLoader.OptionalColumnAtATimeReader} bound to
 *       that column — implemented by
 *       {@link org.elasticsearch.index.codec.flattened.KeyColumnBatchReader}.</li>
 *   <li>For each page of documents, {@code KeyColumnBatchReader.tryRead} drives a single
 *       {@link org.elasticsearch.index.codec.flattened.SequentialColumnReader} forward across
 *       the page. Whole blocks can be skipped without decompression when the target document is
 *       past the block's last doc-id (the doc-id arrays live outside any compressed region).
 *       Within the target block, the small metadata region is decompressed once (lazily) to build
 *       slot-count and value-offset tables; the value region is also decompressed once. Maximal
 *       consecutive runs of single-valued, non-null documents are then copied with a single
 *       {@link System#arraycopy} — no per-doc vint decoding at all.</li>
 *   <li>If the batch reader is unavailable (ordinal absent, or a non-columnar segment), the loader
 *       falls back to the per-doc path via
 *       {@link org.elasticsearch.index.fielddata.KeyLookupArrayOrderBinaryDocValues}.</li>
 * </ol>
 */
package org.elasticsearch.index.codec.flattened;
