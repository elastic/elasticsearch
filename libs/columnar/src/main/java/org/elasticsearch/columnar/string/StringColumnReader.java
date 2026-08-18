/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.elasticsearch.columnar.substrate.BlockBytesCodec;
import org.elasticsearch.columnar.substrate.ColumnIterator;
import org.elasticsearch.columnar.substrate.ColumnIteratorReader;
import org.elasticsearch.columnar.substrate.MonotonicReader;

import java.io.IOException;

/**
 * Reads a string column written by {@link StringColumnWriter}.
 *
 * <p>Values are addressed by <b>value address</b> — a value's 0-based position in the column's block-encoded
 * store, in {@code [0, numValues)}. A document maps to its value addresses through {@link #iterator()}: a
 * single-valued column maps a document's rank straight to its value address. A block is decoded whole into a
 * reusable buffer with a single-block cache; nothing column-proportional is held on the heap.
 */
public final class StringColumnReader {

    private final StringColumnMetadata meta;
    private final BlockBytesCodec blockBytesCodec;
    private final ColumnIteratorReader iteratorReader;
    private final IndexInput data;
    private final LongValues blockOffsets;
    private final long valuesOffset;

    /** The decoded block's concatenated value bytes and per-value offsets. */
    private final byte[] blockValueBytes;
    private final int[] blockValueOffsets;

    private final BytesRef value = new BytesRef();

    private long cachedBlock = -1;

    public StringColumnReader(StringColumnMetadata meta, IndexInput data) throws IOException {
        assert meta.multiValued() == false : "multi-valued string columns are not implemented yet";
        this.meta = meta;
        this.blockBytesCodec = BlockBytesCodec.forId(meta.blockBytesCodecId());
        this.iteratorReader = new ColumnIteratorReader(meta.iterator(), data);
        this.data = data.clone();
        if (meta.numDocsWithField() == 0) {
            this.blockOffsets = null;
            this.valuesOffset = 0;
            this.blockValueBytes = null;
            this.blockValueOffsets = null;
            return;
        }
        this.blockOffsets = MonotonicReader.open(
            data,
            meta.blockOffsetsMeta(),
            meta.numBlocks() + 1L,
            meta.blockOffsetsDataOffset(),
            meta.blockOffsetsDataLength()
        );
        this.valuesOffset = meta.valuesOffset();
        this.blockValueBytes = new byte[meta.maxBlockValueBytes()];
        this.blockValueOffsets = new int[meta.blockSize() + 1];
    }

    /** A fresh iterator over the documents that have a value; positioned by {@link ColumnIterator#rank()}. */
    public ColumnIterator iterator() throws IOException {
        return iteratorReader.iterator();
    }

    /**
     * The value address of a document's first value, given its rank. String columns are single-valued for now,
     * so a document's rank is its value address; the seam is kept so multi-valued support stays a localized
     * change (the numeric column resolves this through a value-address table).
     */
    public long firstValueAddress(int rank) {
        return rank;
    }

    /** The number of values a document has, given its rank — always one until multi-valued columns land. */
    public long valueCount(int rank) {
        return 1;
    }

    /**
     * The value at {@code valueAddress} in {@code [0, numValues)}. The returned {@link BytesRef} points into a
     * buffer this reader reuses, so it is only valid until the next call.
     */
    public BytesRef valueAt(long valueAddress) throws IOException {
        long block = valueAddress / meta.blockSize();
        ensureBlock(block);
        int position = (int) (valueAddress - block * meta.blockSize());
        value.bytes = blockValueBytes;
        value.offset = blockValueOffsets[position];
        value.length = blockValueOffsets[position + 1] - blockValueOffsets[position];
        return value;
    }

    /** Values per encoding block. */
    public int blockSize() {
        return meta.blockSize();
    }

    /** Total number of values across all documents. */
    public long numValues() {
        return meta.numValues();
    }

    private void ensureBlock(long block) throws IOException {
        if (block == cachedBlock) {
            return;
        }
        long blockStart = valuesOffset + blockOffsets.get(block);
        long blockEnd = valuesOffset + blockOffsets.get(block + 1);
        data.seek(blockStart);
        int length = (int) (blockEnd - blockStart);
        DataInput blockData = blockBytesCodec.read(data, length);
        // Full blocks hold blockSize values; the last block holds the remainder.
        int valueCount = (int) Math.min(meta.blockSize(), meta.numValues() - block * meta.blockSize());
        StringBlockEncoder.decode(blockData, valueCount, blockValueBytes, blockValueOffsets);
        cachedBlock = block;
    }
}
