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
import org.elasticsearch.columnar.substrate.BlockBytesCodec;
import org.elasticsearch.columnar.substrate.ColumnIteratorMetadata;
import org.elasticsearch.columnar.substrate.ColumnIteratorWriter;
import org.elasticsearch.columnar.substrate.MonotonicWriter;

import java.io.IOException;

/**
 * Writes a string column. Values are written in the order the {@link StringColumnValues} cursor yields them and
 * are never reordered.
 *
 * <p>Nothing column-proportional is held on the heap: values are streamed one block at a time and the per-block
 * byte offsets are written through {@link MonotonicWriter} to a temporary file. The one buffer that grows with
 * the data is a single block's worth of value bytes.
 *
 * <p>Only {@link StringColumnLayout#PLAIN} is written today; the layout is recorded so an ordinal layout can
 * arrive as a new id. See {@code docs/PLAN.md} for how that decision is meant to be reached.
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

        long numBlocks = (numValues + blockSize - 1) / blockSize;
        long valuesOffset = data.getFilePointer();

        // The block being accumulated, in the flat shape StringBlockEncoder consumes: concatenated value bytes
        // plus the offset of each value within them.
        final int[] valueOffsets = new int[blockSize + 1];
        byte[] valueBytes = new byte[Math.min(blockSize, 1024) * 8];
        int valueBytesLength = 0;
        int maxBlockValueBytes = 0;

        try (MonotonicWriter blockOffsets = new MonotonicWriter(directory, context, data.getName(), numBlocks + 1L)) {
            int inBlock = 0;
            StringColumnValues values = cursors.get();
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                int count = values.valueCount();
                for (int i = 0; i < count; i++) {
                    if (inBlock == 0) {
                        blockOffsets.add(data.getFilePointer() - valuesOffset);
                    }
                    BytesRef value = values.nextValue();
                    valueBytes = ArrayUtil.grow(valueBytes, valueBytesLength + value.length);
                    System.arraycopy(value.bytes, value.offset, valueBytes, valueBytesLength, value.length);
                    valueBytesLength += value.length;
                    valueOffsets[++inBlock] = valueBytesLength;
                    if (inBlock == blockSize) {
                        maxBlockValueBytes = Math.max(maxBlockValueBytes, valueBytesLength);
                        writeBlock(valueBytes, valueOffsets, inBlock, blockBytesCodec, data);
                        inBlock = 0;
                        valueBytesLength = 0;
                    }
                }
            }
            if (inBlock > 0) {
                // The final block holds fewer than blockSize values; the encoder is told the real count and
                // never sees padding.
                maxBlockValueBytes = Math.max(maxBlockValueBytes, valueBytesLength);
                writeBlock(valueBytes, valueOffsets, inBlock, blockBytesCodec, data);
            }
            blockOffsets.add(data.getFilePointer() - valuesOffset);

            MonotonicWriter.Table blocks = blockOffsets.finish(data);

            return new StringColumnMetadata(
                iterator,
                numDocsWithField,
                numValues,
                blockSize,
                blockBytesCodec.id(),
                StringColumnLayout.PLAIN,
                valuesOffset,
                blocks.dataOffset(),
                blocks.dataLength(),
                blocks.meta(),
                maxBlockValueBytes
            );
        }
    }

    private static void writeBlock(byte[] valueBytes, int[] valueOffsets, int valueCount, BlockBytesCodec blockBytesCodec, IndexOutput data)
        throws IOException {
        blockBytesCodec.write(out -> StringBlockEncoder.encode(valueBytes, valueOffsets, valueCount, out), data);
    }
}
