/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.flattened;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;

/**
 * Columnar doc values format for flattened {@code ._keyed} fields.
 *
 * <p>This format stores each sub-field as an independent column in the segment data file, with all
 * values for that sub-field adjacent and compressed together. Reading one sub-field pays I/O only
 * for that column's blocks; the other sub-fields are not touched.
 *
 * <p>The format is only selected (via {@link org.elasticsearch.index.codec.PerFieldFormatSupplier})
 * for {@code ._keyed} fields of flattened fields that have {@code layout: columnar} set in their
 * mapping and are on a strictly columnar index with {@code preserve_leaf_arrays: exact}.
 *
 * <h2>File layout</h2>
 *
 * <p>Two files per segment suffix:
 * <ul>
 *   <li>{@value #DATA_EXTENSION} — per-field column data (all sub-field blocks concatenated in
 *       lex ordinal order), the DISI bitset for sparse fields, the key dictionary, and the column
 *       address table.</li>
 *   <li>{@value #META_EXTENSION} — per-field metadata: DV type, doc counts, DISI pointers,
 *       key-dictionary pointer, column-address-table pointer, and buffer sizing hints.</li>
 * </ul>
 *
 * <h2>Data layout per field</h2>
 *
 * <pre>
 * [ column 0 ][ column 1 ] ... [ column K-1 ]   in ascending key-ordinal (= lex) order
 * [ DISI bitset + jump table ]                   only when the field is sparse
 * [ key dictionary ]
 * [ column address table ]
 * </pre>
 *
 * <h2>Column layout</h2>
 *
 * <pre>
 * [ block 0 ][ block 1 ] ... [ block N-1 ]
 * [int firstDocId, int blockStartRelative] x N   — block index (fixed-width, binary-searchable)
 * </pre>
 *
 * <p>{@code blockStartRelative} is the byte offset from the start of this column (i.e. relative to
 * the column's own start in the data file). The block index is written after all blocks, appended
 * at {@link FieldBlockWriter#finish} time.
 *
 * <h2>Block layout</h2>
 *
 * <pre>
 * [byte  flags]
 *       bit0 = FLAG_VALUES_COMPRESSED    value payload is ZSTD-compressed; otherwise stored raw
 *       bit1 = FLAG_DOCS_CONTIGUOUS      docIds are consecutive; delta array omitted
 *       bit2 = FLAG_ALL_SINGLE_SLOT      every doc has exactly one slot; count array omitted
 * [vint  numDocs]
 * [byte  bitsPerDelta]                   absent when FLAG_DOCS_CONTIGUOUS
 * [bit-packed (gap-1) x (numDocs-1)]     absent when FLAG_DOCS_CONTIGUOUS; MSB-first, bitsPerDelta bits each
 * [vint  uncompressedLen]
 * -- if FLAG_VALUES_COMPRESSED:
 *    [vint compressedLen][compressedLen bytes]   written by ZstdCompressionMode.ZstdCompressor
 * -- otherwise:
 *    [uncompressedLen bytes]                     raw
 * The (un)compressed payload contains:
 * [byte  bitsPerSlot]                    absent when FLAG_ALL_SINGLE_SLOT
 * [bit-packed slotCount x numDocs]       absent when FLAG_ALL_SINGLE_SLOT; MSB-first, bitsPerSlot bits each
 * [vint  valueLen+1][value bytes] x ...  per doc in ascending docId order, per slot
 * </pre>
 *
 * <p>The docId-delta array is stored outside the compressed payload so that
 * {@link FlattenedDocValuesProducer.ColumnCursor#advanceToDoc} can binary-search docIds without
 * decompressing the block — absent docs cost nothing. The slot-count array lives inside the
 * compressed region because it is only needed after a doc is confirmed present; ZSTD can exploit
 * its redundancy alongside the value bytes. Per doc in ascending docId order, per slot in document
 * order: {@code [vint valueLen+1][value bytes]}, where a {@code 0} prefix denotes a null slot
 * (no bytes follow).
 *
 * <h2>Key dictionary</h2>
 *
 * <pre>
 * [vint numKeys]
 * per key in lex order (ordinal = lex rank):
 *   [vint keyLen][keyLen bytes]
 * </pre>
 *
 * <h2>Column address table</h2>
 *
 * <p>One entry per key, in lex ordinal order, fixed width ({@value #COLUMN_ADDRESS_ENTRY_BYTES}
 * bytes each):
 * <pre>
 * [long columnStartOffset][int blockIndexRelativeOffset][int numBlocks]
 * </pre>
 * <p>{@code columnStartOffset} is the absolute data-file position of the first block for this key.
 * {@code blockIndexRelativeOffset} is the offset of the block index from {@code columnStartOffset}.
 * {@code numBlocks} is the number of blocks.
 *
 * <h2>binaryValue() and slot order</h2>
 *
 * <p>{@link ColumnarKeyedBinaryDocValues#binaryValue()} reconstructs the per-doc blob by walking
 * columns in key-ordinal (lexicographic) order. Blobs are therefore <em>not</em> byte-identical
 * to the row format when the original JSON key order differs from lex order. This is safe for all
 * current consumers: {@code KeyFilteredSortingArrayOrderBinaryDocValues} sorts and deduplicates,
 * the DV queries are pure predicate scans, and {@code FlattenedDocValuesSyntheticFieldLoader}
 * re-groups into a {@code TreeMap}.
 */
public final class FlattenedDocValuesFormat extends DocValuesFormat {

    static final String CODEC_NAME = "ESFlattenedColumnar";
    static final String DATA_CODEC = "ESFlattenedColumnarData";
    static final String DATA_EXTENSION = "fdvd";
    static final String META_CODEC = "ESFlattenedColumnarMeta";
    static final String META_EXTENSION = "fdvm";
    static final int VERSION_START = 0;
    static final int VERSION_CURRENT = 0;

    // Block flag bits
    /** Bit 0: block payload is ZSTD-compressed; otherwise stored raw. */
    static final int FLAG_VALUES_COMPRESSED = 0x01;
    /** Bit 1: docIds are consecutive (delta array omitted). */
    static final int FLAG_DOCS_CONTIGUOUS = 0x02;
    /** Bit 2: every doc has exactly one slot (slot-count array omitted). */
    static final int FLAG_ALL_SINGLE_SLOT = 0x04;

    /**
     * Flush a new block when the uncompressed payload reaches this size.
     * Balances I/O granularity against per-block-index overhead.
     */
    public static final int TARGET_BLOCK_BYTES_DEFAULT = 64 * 1024;
    /** Flush a new block when it contains this many documents. */
    public static final int MAX_DOCS_PER_BLOCK_DEFAULT = 8192;
    /**
     * Minimum uncompressed payload length to bother applying ZSTD compression.
     * Below this threshold the frame overhead exceeds the savings.
     */
    public static final int MIN_COMPRESS_BYTES_DEFAULT = 64;
    /**
     * Maximum bytes buffered in the {@link SortedSlotAccumulator} before spilling to an external
     * merge sort. Larger values reduce I/O at the cost of heap.
     */
    public static final int MAX_BUFFERED_BYTES_DEFAULT = 32 * 1024 * 1024;

    /** Fixed byte size of one entry in the column address table. */
    static final int COLUMN_ADDRESS_ENTRY_BYTES = 16; // long + int + int

    /**
     * Whether to use the column-wise optimised merge path in
     * {@link FlattenedDocValuesConsumer#mergeBinaryField}. Set the system property
     * {@code es.flattened.mergeColumnWise=false} to fall back to the row-based merge via
     * {@code addBinaryField} (useful for debugging or comparison). Mirrors Lucene's own
     * {@code org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat.BULK_MERGE_ENABLED}.
     */
    static final boolean MERGE_COLUMN_WISE_ENABLED = "false".equals(System.getProperty("es.flattened.mergeColumnWise")) == false;

    private final int targetBlockBytes;
    private final int maxDocsPerBlock;
    private final int minCompressBytes;
    private final int maxBufferedBytes;

    /** Public no-arg constructor required for SPI. */
    public FlattenedDocValuesFormat() {
        this(TARGET_BLOCK_BYTES_DEFAULT, MAX_DOCS_PER_BLOCK_DEFAULT, MIN_COMPRESS_BYTES_DEFAULT, MAX_BUFFERED_BYTES_DEFAULT);
    }

    /** Public constructor with configurable thresholds for testing and benchmarking. */
    public FlattenedDocValuesFormat(int targetBlockBytes, int maxDocsPerBlock, int minCompressBytes, int maxBufferedBytes) {
        super(CODEC_NAME);
        if (targetBlockBytes < 1) throw new IllegalArgumentException("targetBlockBytes must be >= 1, got " + targetBlockBytes);
        if (maxDocsPerBlock < 1) throw new IllegalArgumentException("maxDocsPerBlock must be >= 1, got " + maxDocsPerBlock);
        if (minCompressBytes < 0) throw new IllegalArgumentException("minCompressBytes must be >= 0, got " + minCompressBytes);
        if (maxBufferedBytes < 1) throw new IllegalArgumentException("maxBufferedBytes must be >= 1, got " + maxBufferedBytes);
        this.targetBlockBytes = targetBlockBytes;
        this.maxDocsPerBlock = maxDocsPerBlock;
        this.minCompressBytes = minCompressBytes;
        this.maxBufferedBytes = maxBufferedBytes;
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new FlattenedDocValuesConsumer(
            state,
            DATA_CODEC,
            DATA_EXTENSION,
            META_CODEC,
            META_EXTENSION,
            targetBlockBytes,
            maxDocsPerBlock,
            minCompressBytes,
            maxBufferedBytes
        );
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new FlattenedDocValuesProducer(state, DATA_CODEC, DATA_EXTENSION, META_CODEC, META_EXTENSION);
    }

    /**
     * Writes {@code n} values from {@code arr[0..n-1]} to {@code out} as an MSB-first bit-packed
     * stream. Each value occupies exactly {@code bitsPerValue} bits. The last byte is zero-padded
     * on the right if {@code n * bitsPerValue} is not a multiple of 8.
     */
    static void packInts(IndexOutput out, int[] arr, int n, int bitsPerValue) throws IOException {
        long accumulator = 0;
        int bitsInAcc = 0;
        for (int i = 0; i < n; i++) {
            accumulator = (accumulator << bitsPerValue) | (arr[i] & ((1L << bitsPerValue) - 1));
            bitsInAcc += bitsPerValue;
            while (bitsInAcc >= 8) {
                bitsInAcc -= 8;
                out.writeByte((byte) (accumulator >>> bitsInAcc));
            }
        }
        if (bitsInAcc > 0) {
            out.writeByte((byte) (accumulator << (8 - bitsInAcc)));
        }
    }

    /**
     * Reads {@code n} values from {@code in} into {@code arr[arrOffset..arrOffset+n-1]} from an
     * MSB-first bit-packed stream written by {@link #packInts}. Partial trailing bits are consumed.
     */
    static void unpackInts(IndexInput in, int[] arr, int arrOffset, int n, int bitsPerValue) throws IOException {
        final long mask = (1L << bitsPerValue) - 1;
        long accumulator = 0;
        int bitsInAcc = 0;
        for (int i = 0; i < n; i++) {
            while (bitsInAcc < bitsPerValue) {
                accumulator = (accumulator << 8) | (in.readByte() & 0xFFL);
                bitsInAcc += 8;
            }
            bitsInAcc -= bitsPerValue;
            arr[arrOffset + i] = (int) ((accumulator >>> bitsInAcc) & mask);
        }
    }

    /**
     * Reads {@code n} values from {@code src[srcOff..]} into {@code arr[arrOffset..arrOffset+n-1]}
     * from an MSB-first bit-packed stream written by {@link #packInts}. Returns the new source
     * offset (past the last consumed byte, including any partial trailing byte).
     */
    static int unpackInts(byte[] src, int srcOff, int[] arr, int arrOffset, int n, int bitsPerValue) {
        final long mask = (1L << bitsPerValue) - 1;
        long accumulator = 0;
        int bitsInAcc = 0;
        for (int i = 0; i < n; i++) {
            while (bitsInAcc < bitsPerValue) {
                accumulator = (accumulator << 8) | (src[srcOff++] & 0xFFL);
                bitsInAcc += 8;
            }
            bitsInAcc -= bitsPerValue;
            arr[arrOffset + i] = (int) ((accumulator >>> bitsInAcc) & mask);
        }
        return srcOff;
    }
}
