/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.LongValues;
import org.elasticsearch.columnar.substrate.BlockBytesCodec;
import org.elasticsearch.columnar.substrate.ColumnIterator;
import org.elasticsearch.columnar.substrate.ColumnIteratorReader;
import org.elasticsearch.columnar.substrate.MonotonicReader;

import java.io.IOException;

/**
 * Reads a numeric column written by {@link NumericColumnWriter}, single- or multi-valued.
 *
 * <p>Values are addressed by <b>value address</b> — a value's 0-based position in the block-encoded store, in
 * {@code [0, numValues)}. A document maps to its value addresses through {@link #iterator()}: a single-valued
 * column maps a document's rank straight to its value address, while a multi-valued one looks the range up in
 * the value-address table. A block is decoded whole into a reusable buffer with a single-block cache; nothing
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
        this.blockOffsets = MonotonicReader.open(
            data,
            meta.blockOffsetsMeta(),
            meta.numBlocks() + 1L,
            meta.blockOffsetsDataOffset(),
            meta.blockOffsetsDataLength()
        );
        this.valueAddresses = meta.multiValued()
            ? MonotonicReader.open(
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

    /** A fresh iterator over the documents that have a value; positioned by {@link ColumnIterator#rank()}. */
    public ColumnIterator iterator() throws IOException {
        return iteratorReader.iterator();
    }

    /**
     * Whether any document holds more than one value. A single-valued column maps a rank straight to a value
     * address.
     */
    public boolean multiValued() {
        return valueAddresses != null;
    }

    /** The value address of a document's first value, given its rank. */
    public long firstValueAddress(int rank) {
        return valueAddresses == null ? rank : valueAddresses.get(rank);
    }

    /** The number of values a document has, given its rank. */
    public long valueCount(int rank) {
        return valueAddresses == null ? 1 : valueAddresses.get(rank + 1) - valueAddresses.get(rank);
    }

    /** The value at {@code valueAddress} in {@code [0, numValues)}. */
    public long valueAt(long valueAddress) throws IOException {
        long block = valueAddress / meta.blockSize();
        ensureBlock(block);
        return blockBuffer[(int) (valueAddress - block * meta.blockSize())];
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

}
