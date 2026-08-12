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
import org.elasticsearch.columnar.numeric.NumericBlockEncoder;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.substrate.BlockBytesCodec;
import org.elasticsearch.columnar.substrate.ColumnIterator;
import org.elasticsearch.columnar.substrate.ColumnIteratorReader;
import org.elasticsearch.columnar.substrate.MonotonicWriter;

import java.io.IOException;

/**
 * Reads a string column written by {@link StringColumnWriter}, in either {@link StringColumnLayout}.
 *
 * <p>Values are addressed by ordinal within one block-encoded store. A document maps to its value ordinals
 * through {@link #iterator()}: a single-valued column maps a document's rank straight to its ordinal. A block
 * is decoded whole into a reusable buffer with a single-block cache; nothing column-proportional is held on
 * the heap, apart from the bounded terms dictionary a {@link StringColumnLayout#DICTIONARY} column carries.
 */
public final class StringColumnReader {

    private final StringColumnMetadata meta;
    private final BlockBytesCodec blockBytesCodec;
    private final ColumnIteratorReader iteratorReader;
    private final IndexInput data;
    private final LongValues blockOffsets;
    private final long valuesOffset;

    /** {@code PLAIN}: the decoded block's concatenated value bytes and per-value offsets. */
    private final byte[] blockValueBytes;
    private final int[] blockValueOffsets;
    /** {@code DICTIONARY}: the decoded block's ordinals, and the encoder that produced them. */
    private final NumericBlockEncoder ordinalEncoder;
    private final long[] blockOrdinals;

    private final BytesRef value = new BytesRef();

    private int cachedBlock = -1;

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
            this.ordinalEncoder = null;
            this.blockOrdinals = null;
            return;
        }
        this.blockOffsets = MonotonicWriter.open(
            data,
            meta.blockOffsetsMeta(),
            meta.numBlocks() + 1L,
            meta.blockOffsetsDataOffset(),
            meta.blockOffsetsDataLength()
        );
        this.valuesOffset = meta.valuesOffset();
        // if/else rather than a switch: the compiler does not treat an enum switch statement as exhaustive
        // for final-field definite assignment.
        if (meta.layout() == StringColumnLayout.DICTIONARY) {
            this.blockValueBytes = null;
            this.blockValueOffsets = null;
            NumericPipeline pipeline = NumericPipeline.Registry.rebuild(meta.terminalId(), meta.transformIds(), meta.blockSize());
            this.ordinalEncoder = new NumericBlockEncoder(pipeline, meta.blockSize());
            this.blockOrdinals = new long[meta.blockSize()];
        } else {
            this.blockValueBytes = new byte[meta.maxBlockValueBytes()];
            this.blockValueOffsets = new int[meta.blockSize() + 1];
            this.ordinalEncoder = null;
            this.blockOrdinals = null;
        }
    }

    /** A fresh iterator over the documents that have a value; {@link ColumnIterator#index()} is the rank. */
    public ColumnIterator iterator() throws IOException {
        return iteratorReader.iterator();
    }

    /**
     * The ordinal of a document's first value, given its rank. String columns are single-valued for now, so a
     * document's rank is its ordinal; the seam is kept so multi-valued support stays a localized change (the
     * numeric column resolves this through a value-address table).
     */
    public int firstOrdinal(int rank) {
        return rank;
    }

    /** The number of values a document has, given its rank — always one until multi-valued columns land. */
    public int valueCount(int rank) {
        return 1;
    }

    /**
     * The value at {@code ordinal} in {@code [0, numValues)}. The returned {@link BytesRef} points into a
     * buffer this reader reuses, so it is only valid until the next call.
     */
    public BytesRef valueForOrdinal(int ordinal) throws IOException {
        int block = ordinal / meta.blockSize();
        ensureBlock(block);
        int position = ordinal - block * meta.blockSize();
        return switch (meta.layout()) {
            case PLAIN -> {
                value.bytes = blockValueBytes;
                value.offset = blockValueOffsets[position];
                value.length = blockValueOffsets[position + 1] - blockValueOffsets[position];
                yield value;
            }
            case DICTIONARY -> meta.dictionary().term((int) blockOrdinals[position]);
        };
    }

    /** Values per encoding block. */
    public int blockSize() {
        return meta.blockSize();
    }

    /** Total number of values across all documents. */
    public int numValues() {
        return meta.numValues();
    }

    private void ensureBlock(int block) throws IOException {
        if (block == cachedBlock) {
            return;
        }
        long blockStart = valuesOffset + blockOffsets.get(block);
        long blockEnd = valuesOffset + blockOffsets.get(block + 1);
        data.seek(blockStart);
        int length = (int) (blockEnd - blockStart);
        DataInput blockData = blockBytesCodec.read(data, length);
        // Full blocks hold blockSize values; the last block holds the remainder.
        int valueCount = Math.min(meta.blockSize(), meta.numValues() - block * meta.blockSize());
        switch (meta.layout()) {
            case PLAIN -> StringBlockEncoder.decode(blockData, valueCount, blockValueBytes, blockValueOffsets);
            case DICTIONARY -> ordinalEncoder.decode(blockData, valueCount, blockOrdinals);
        }
        cachedBlock = block;
    }
}
