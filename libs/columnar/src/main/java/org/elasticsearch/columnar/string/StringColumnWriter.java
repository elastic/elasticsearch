/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOSupplier;
import org.elasticsearch.columnar.numeric.NumericBlockEncoder;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.substrate.BlockBytesCodec;
import org.elasticsearch.columnar.substrate.ColumnIteratorMetadata;
import org.elasticsearch.columnar.substrate.ColumnIteratorWriter;
import org.elasticsearch.columnar.substrate.MonotonicWriter;

import java.io.IOException;

/**
 * Writes a string column, in whichever {@link StringColumnLayout} the caller's cardinality probe selected.
 * Values are written in the order the {@link StringColumnValues} cursor yields them and are never reordered.
 *
 * <p>Nothing column-proportional is held on the heap: values are streamed one block at a time and the
 * per-block byte offsets are written through {@link MonotonicWriter} to a temporary file. The one heap
 * structure is the terms dictionary, capped at {@link StringDictionary#MAX_SIZE}.
 *
 * <p>A {@link StringColumnLayout#DICTIONARY} column encodes its ordinal stream through
 * {@link NumericPipeline#defaultPipeline} — the same delta / offset / GCD detection plus FOR bit-packing the
 * numeric column uses — so a run of repeated or sequential ordinals collapses without any string-specific
 * encoder. The pipeline's stage ids are recorded in the metadata, so the read side rebuilds it exactly.
 */
public final class StringColumnWriter {

    private StringColumnWriter() {}

    /**
     * Encodes a string column into {@code data}: iterator metadata then the block-encoded values; returns the
     * metadata needed to reconstruct the column at read time.
     *
     * @param maxDoc           documents in the segment
     * @param numDocsWithField documents that have at least one value
     * @param numValues        total number of values across all documents
     * @param cursors          supplies fresh forward cursors over the documents that have a value; called
     *                         once for the iterator and once for the values
     * @param dictionary       the segment's terms dictionary, selecting {@link StringColumnLayout#DICTIONARY},
     *                         or {@code null} to store values directly as {@link StringColumnLayout#PLAIN}
     * @param blockSize        values per encoded block
     * @param blockBytesCodec  terminal byte codec applied to each block
     * @param directory        directory used for the temporary table file
     * @param context          IO context for the temporary table file
     * @param data             data output (iterator, value blocks, and the offset table are appended)
     */
    public static StringColumnMetadata write(
        int maxDoc,
        int numDocsWithField,
        long numValues,
        IOSupplier<StringColumnValues> cursors,
        StringDictionary dictionary,
        int blockSize,
        BlockBytesCodec blockBytesCodec,
        Directory directory,
        IOContext context,
        IndexOutput data
    ) throws IOException {
        ColumnIteratorMetadata iterator = ColumnIteratorWriter.write(cursors.get(), numDocsWithField, maxDoc, data);
        if (numDocsWithField == 0) {
            return StringColumnMetadata.empty(iterator, blockBytesCodec.id());
        }

        StringColumnLayout layout = dictionary == null ? StringColumnLayout.PLAIN : StringColumnLayout.DICTIONARY;
        long numBlocks = (numValues + blockSize - 1) / blockSize;
        long valuesOffset = data.getFilePointer();

        try (
            MonotonicWriter blockOffsets = new MonotonicWriter(
                directory,
                context,
                data.getName(),
                numBlocks + 1L,
                MonotonicWriter.BLOCK_SHIFT
            )
        ) {
            BlockWriter blockWriter = layout == StringColumnLayout.DICTIONARY
                ? new DictionaryBlockWriter(dictionary, blockSize)
                : new PlainBlockWriter(blockSize);

            // count of values written to current block
            int inBlock = 0;
            StringColumnValues values = cursors.get();
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                int count = values.valueCount();
                for (int i = 0; i < count; i++) {
                    if (inBlock == 0) {
                        blockOffsets.add(data.getFilePointer() - valuesOffset);
                    }
                    blockWriter.append(values.nextValue(), inBlock++);
                    if (inBlock == blockSize) {
                        blockWriter.flush(inBlock, blockBytesCodec, data);
                        inBlock = 0;
                    }
                }
            }
            if (inBlock > 0) {
                // The final block holds fewer than blockSize values; the encoder is told the real count and
                // never sees padding.
                blockWriter.flush(inBlock, blockBytesCodec, data);
            }
            blockOffsets.add(data.getFilePointer() - valuesOffset);

            MonotonicWriter.Table blocks = blockOffsets.finish(data);

            return new StringColumnMetadata(
                iterator,
                numDocsWithField,
                numValues,
                blockSize,
                blockBytesCodec.id(),
                layout,
                valuesOffset,
                blocks.dataOffset(),
                blocks.dataLength(),
                blocks.meta(),
                blockWriter.maxBlockValueBytes(),
                blockWriter.terminalId(),
                blockWriter.transformIds(),
                dictionary
            );
        }
    }

    /** Accumulates a block's values and serializes it, in whichever shape the layout calls for. */
    private interface BlockWriter {

        /** Adds the value at position {@code position} within the block being built. */
        void append(BytesRef value, int position);

        /** Serializes the {@code valueCount} values accumulated so far and resets for the next block. */
        void flush(int valueCount, BlockBytesCodec blockBytesCodec, IndexOutput data) throws IOException;

        /** The largest total value-byte count any one block held; only meaningful for {@code PLAIN}. */
        default int maxBlockValueBytes() {
            return 0;
        }

        /** The ordinal pipeline's terminal id; only meaningful for {@code DICTIONARY}. */
        default byte terminalId() {
            return 0;
        }

        /** The ordinal pipeline's transform ids; only meaningful for {@code DICTIONARY}. */
        default byte[] transformIds() {
            return new byte[0];
        }
    }

    /** Stores each value's bytes directly, through {@link StringBlockEncoder}. */
    private static final class PlainBlockWriter implements BlockWriter {

        private final int[] valueOffsets;
        private byte[] valueBytes;
        private int valueBytesLength = 0;
        private int maxBlockValueBytes = 0;

        PlainBlockWriter(int blockSize) {
            this.valueOffsets = new int[blockSize + 1];
            this.valueOffsets[0] = 0;
            this.valueBytes = new byte[Math.min(blockSize, 1024) * 8];
        }

        @Override
        public void append(BytesRef value, int position) {
            valueBytes = ArrayUtil.grow(valueBytes, valueBytesLength + value.length);
            System.arraycopy(value.bytes, value.offset, valueBytes, valueBytesLength, value.length);
            valueBytesLength += value.length;
            valueOffsets[position + 1] = valueBytesLength;
        }

        @Override
        public void flush(int valueCount, BlockBytesCodec blockBytesCodec, IndexOutput data) throws IOException {
            int totalValueBytes = valueBytesLength;
            blockBytesCodec.write(out -> StringBlockEncoder.encode(valueBytes, valueOffsets, valueCount, out), data);
            if (totalValueBytes > maxBlockValueBytes) {
                maxBlockValueBytes = totalValueBytes;
            }
            valueBytesLength = 0;
        }

        @Override
        public int maxBlockValueBytes() {
            return maxBlockValueBytes;
        }
    }

    /** Replaces each value with its dictionary ordinal and runs the ordinal block through the numeric pipeline. */
    private static final class DictionaryBlockWriter implements BlockWriter {

        private final StringDictionary dictionary;
        private final NumericPipeline pipeline;
        private final NumericBlockEncoder encoder;
        private final long[] ordinals;

        DictionaryBlockWriter(StringDictionary dictionary, int blockSize) {
            this.dictionary = dictionary;
            this.pipeline = NumericPipeline.defaultPipeline(blockSize);
            this.encoder = new NumericBlockEncoder(pipeline, blockSize);
            this.ordinals = new long[blockSize];
        }

        @Override
        public void append(BytesRef value, int position) {
            ordinals[position] = dictionary.ordinal(value);
        }

        @Override
        public void flush(int valueCount, BlockBytesCodec blockBytesCodec, IndexOutput data) throws IOException {
            blockBytesCodec.write(out -> encoder.encode(ordinals, valueCount, out), data);
        }

        @Override
        public byte terminalId() {
            return pipeline.terminalId();
        }

        @Override
        public byte[] transformIds() {
            return pipeline.transformIds();
        }
    }
}
