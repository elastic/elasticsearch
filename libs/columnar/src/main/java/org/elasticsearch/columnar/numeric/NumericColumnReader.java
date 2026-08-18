/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.elasticsearch.columnar.substrate.BlockBytesCodec;
import org.elasticsearch.columnar.substrate.ColumnIterator;
import org.elasticsearch.columnar.substrate.ColumnIteratorReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Reads a numeric column written by {@link NumericColumnWriter}, single- or multi-valued.
 *
 * <p>Values are addressed by ordinal within one block-encoded store. A document maps to its value
 * ordinals through {@link #iterator()}: single-valued columns map a document's rank
 * straight to its ordinal, while multi-valued columns look the range up in the value-address table.
 * A block is decoded whole into a reusable buffer with a single-block cache; nothing
 * column-proportional is held on the heap (offset tables are read on demand from the mapped input).
 */
public final class NumericColumnReader {

    private final NumericColumnMetadata meta;
    private final BlockBytesCodec blockBytesCodec;
    private final ColumnIteratorReader iteratorReader;
    private final IndexInput data;
    private final LongValues blockOffsets;
    private final LongValues valueAddresses;
    private final long valuesOffset;
    private final NumericBlockEncoder encoder;
    private final long[] blockBuffer;

    private long cachedBlock = -1;

    public NumericColumnReader(NumericColumnMetadata meta, IndexInput data) throws IOException {
        this.meta = meta;
        this.blockBytesCodec = BlockBytesCodec.forId(meta.blockBytesCodecId());
        this.iteratorReader = new ColumnIteratorReader(meta.iterator(), data);
        this.data = data.clone();
        if (meta.numDocsWithField() == 0) {
            this.blockOffsets = null;
            this.valueAddresses = null;
            this.valuesOffset = 0;
            this.encoder = null;
            this.blockBuffer = new long[0];
            return;
        }
        this.blockOffsets = monotonic(
            data,
            meta.blockOffsetsMeta(),
            meta.numBlocks() + 1L,
            meta.blockOffsetsDataOffset(),
            meta.blockOffsetsDataLength()
        );
        this.valueAddresses = meta.multiValued()
            ? monotonic(
                data,
                meta.valueAddressesMeta(),
                meta.numDocsWithField() + 1L,
                meta.valueAddressesDataOffset(),
                meta.valueAddressesDataLength()
            )
            : null;
        this.valuesOffset = meta.valuesOffset();
        NumericPipeline pipeline = NumericPipeline.Registry.rebuild(meta.terminalId(), meta.transformIds(), meta.blockSize());
        this.encoder = new NumericBlockEncoder(pipeline, meta.blockSize());
        this.blockBuffer = new long[meta.blockSize()];
    }

    /** A fresh iterator over the documents that have a value; {@link ColumnIterator#index()} is the rank. */
    public ColumnIterator iterator() throws IOException {
        return iteratorReader.iterator();
    }

    /**
     * Whether any document holds more than one value. A single-valued column maps a rank straight to an
     * ordinal.
     */
    public boolean multiValued() {
        return valueAddresses != null;
    }

    /** The ordinal of a document's first value, given its rank. */
    public long firstOrdinal(int rank) {
        return valueAddresses == null ? rank : valueAddresses.get(rank);
    }

    /** The number of values a document has, given its rank. */
    public long valueCount(int rank) {
        return valueAddresses == null ? 1 : valueAddresses.get(rank + 1) - valueAddresses.get(rank);
    }

    /** The value at {@code ordinal} in {@code [0, numValues)}. */
    public long valueForOrdinal(long ordinal) throws IOException {
        long block = ordinal / meta.blockSize();
        ensureBlock(block);
        return blockBuffer[(int) (ordinal - block * meta.blockSize())];
    }

    /** Values per encoding block. */
    public int blockSize() {
        return meta.blockSize();
    }

    /** Total number of values across all documents. */
    public long numValues() {
        return meta.numValues();
    }

    /**
     * Decodes the block at {@code blockIndex} (single-block cache) and returns the shared buffer, valid
     * until the next call that touches a different block.
     */
    public long[] block(long blockIndex) throws IOException {
        ensureBlock(blockIndex);
        return blockBuffer;
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
        encoder.decode(blockData, valueCount, blockBuffer);
        cachedBlock = block;
    }

    private static LongValues monotonic(IndexInput data, byte[] metaBytes, long numEntries, long dataOffset, long dataLength)
        throws IOException {
        DirectMonotonicReader.Meta tableMeta;
        try (
            IndexInput metaInput = new ByteBuffersIndexInput(
                new ByteBuffersDataInput(List.of(ByteBuffer.wrap(metaBytes))),
                "monotonic-meta"
            )
        ) {
            tableMeta = DirectMonotonicReader.loadMeta(metaInput, numEntries, NumericColumnWriter.DIRECT_MONOTONIC_BLOCK_SHIFT);
        }
        return DirectMonotonicReader.getInstance(tableMeta, data.randomAccessSlice(dataOffset, dataLength));
    }
}
