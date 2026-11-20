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
import org.elasticsearch.columnar.substrate.ColumnInputs;
import org.elasticsearch.columnar.substrate.ColumnIterator;
import org.elasticsearch.columnar.substrate.ColumnIteratorReader;
import org.elasticsearch.columnar.substrate.ColumnPrefetcher;
import org.elasticsearch.columnar.substrate.MonotonicReader;
import org.elasticsearch.lucene.store.DocRanks;

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
public final class NumericColumnReader implements ColumnPrefetcher.Column {

    private final NumericColumnMetadata meta;
    private final BlockBytesCodec blockBytesCodec;
    private final ColumnIteratorReader iteratorReader;
    private final IndexInput data;
    private final IndexInput addressing;
    private final LongValues blockOffsets;
    private final LongValues valueAddresses;
    private final long valuesOffset;
    private final NumericBlockEncoder encoder;
    private final long[] blockBuffer;

    /** A block holds a power of two values, so the block a value is in and where it sits are shifts. */
    private final int blockShift;
    private final int blockMask;

    private long cachedBlock = -1;

    public NumericColumnReader(NumericColumnMetadata meta, ColumnInputs inputs) throws IOException {
        final IndexInput data = inputs.data();
        final IndexInput addressing = inputs.addressing();
        final IndexInput navigation = inputs.navigation();
        this.meta = meta;
        assert (meta.blockSize() & (meta.blockSize() - 1)) == 0 : "values per block must be a power of two, got " + meta.blockSize();
        this.blockShift = Integer.numberOfTrailingZeros(meta.blockSize());
        this.blockMask = meta.blockSize() - 1;
        this.blockBytesCodec = BlockBytesCodec.forId(meta.blockBytesCodecId());
        this.iteratorReader = new ColumnIteratorReader(meta.iterator(), addressing);
        this.data = data.clone();
        this.addressing = addressing.clone();
        if (meta.numDocsWithField() == 0) {
            this.blockOffsets = null;
            this.valueAddresses = null;
            this.valuesOffset = 0;
            this.encoder = null;
            this.blockBuffer = new long[0];
            return;
        }
        this.blockOffsets = MonotonicReader.open(
            navigation,
            meta.blockOffsetsMeta(),
            meta.numBlocks() + 1L,
            meta.blockOffsetsDataOffset(),
            meta.blockOffsetsDataLength()
        );
        this.valueAddresses = meta.hasValueAddresses()
            ? MonotonicReader.open(
                addressing,
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

    @Override
    public DocRanks ranks() {
        return iteratorReader.ranks();
    }

    /** Prefetches the table of where each document's values begin, whole. */
    @Override
    public boolean prefetchTables() throws IOException {
        if (valueAddresses == null || meta.valueAddressesDataLength() == 0) {
            return false;
        }
        addressing.prefetch(meta.valueAddressesDataOffset(), meta.valueAddressesDataLength());
        return true;
    }

    /** {@link #prefetchTables} prefetched the whole table. */
    @Override
    public boolean prefetchValueAddresses(long[] ranks, int count) {
        return false;
    }

    /** Prefetches the blocks holding the values, in one stage. */
    @Override
    public boolean prefetchValues(int stage, long[] firsts, long[] counts, int count) throws IOException {
        long runStart = -1;
        long runEnd = -1;
        for (int i = 0; i < count; i++) {
            if (counts[i] == 0) {
                continue;
            }
            final long firstBlock = firsts[i] >>> blockShift;
            final long lastBlock = (firsts[i] + counts[i] - 1) >>> blockShift;
            if (runStart >= 0 && firstBlock <= runEnd + 1) {
                runEnd = Math.max(runEnd, lastBlock);
                continue;
            }
            prefetchBlocks(runStart, runEnd);
            runStart = firstBlock;
            runEnd = lastBlock;
        }
        prefetchBlocks(runStart, runEnd);
        return false;
    }

    private void prefetchBlocks(long first, long last) throws IOException {
        if (first >= 0) {
            final long start = blockOffsets.get(first);
            data.prefetch(valuesOffset + start, blockOffsets.get(last + 1) - start);
        }
    }

    /**
     * Whether the column tables where each document's values begin. Without that table a rank maps straight
     * to a value address.
     */
    public boolean hasValueAddresses() {
        return valueAddresses != null;
    }

    /** The value address of a document's first value, given its rank. */
    @Override
    public long firstValueAddress(int rank) {
        return valueAddresses == null ? rank : valueAddresses.get(rank);
    }

    /** The number of values a document has, given its rank. */
    @Override
    public long valueCount(int rank) {
        return valueAddresses == null ? 1 : valueAddresses.get(rank + 1) - valueAddresses.get(rank);
    }

    /** The value at {@code valueAddress} in {@code [0, numValues)}. */
    public long valueAt(long valueAddress) throws IOException {
        final long block = valueAddress >>> blockShift;
        ensureBlock(block);
        return blockBuffer[(int) (valueAddress & blockMask)];
    }

    /** Values per encoding block. */
    public int blockSize() {
        return meta.blockSize();
    }

    /** Total number of values across all documents. */
    public long numValues() {
        return meta.numValues();
    }

    /** How many documents have a value. */
    public int numDocsWithField() {
        return meta.numDocsWithField();
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
