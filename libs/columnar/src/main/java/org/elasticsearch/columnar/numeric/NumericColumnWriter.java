/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.columnar.substrate.BlockBytesCodec;
import org.elasticsearch.columnar.substrate.ColumnIteratorMetadata;
import org.elasticsearch.columnar.substrate.ColumnIteratorWriter;

import java.io.IOException;

/**
 * Writes a numeric column — single- or multi-valued, one format. It takes a
 * {@link NumericColumnValues} cursor; values are written in the order the cursor yields them and are never
 * reordered.
 *
 * <p>Nothing column-proportional is held on the heap: values are streamed one block at a time, and
 * both address tables — the per-block byte offsets and, when the column is multi-valued, the
 * per-document value addresses — are written through {@link MonotonicWriter} to temporary files. The
 * value-address table is written only when {@code numValues > numDocsWithField}; otherwise a
 * document's ordinal is its iterator rank.
 */
public final class NumericColumnWriter {

    /** Values per block. Small enough for fine-grained per-block adaptation. */
    public static final int BLOCK_SIZE = 128;

    /** Monotonic block shift for the offset tables. */
    static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

    private NumericColumnWriter() {}

    /**
     * @param maxDoc           documents in the segment
     * @param numDocsWithField documents that have at least one value
     * @param numValues        total number of values across all documents
     * @param cursors          supplies fresh forward cursors over the documents that have a value;
     *                         called once for iterator and once for the values
     * @param blockBytesCodec  terminal byte codec applied to each block
     * @param directory        directory used for the temporary table files
     * @param context          IO context for the temporary table files
     * @param data             data output (iterator, value blocks, and tables are appended)
     */
    public static NumericColumnMetadata write(
        int maxDoc,
        int numDocsWithField,
        int numValues,
        IOSupplier<NumericColumnValues> cursors,
        BlockBytesCodec blockBytesCodec,
        Directory directory,
        IOContext context,
        IndexOutput data
    ) throws IOException {
        ColumnIteratorMetadata iterator = ColumnIteratorWriter.write(cursors.get(), numDocsWithField, maxDoc, data);
        if (numDocsWithField == 0) {
            return NumericColumnMetadata.empty(iterator, blockBytesCodec.id());
        }

        boolean multiValued = numValues > numDocsWithField;
        int numBlocks = (numValues + BLOCK_SIZE - 1) / BLOCK_SIZE;
        long valuesOffset = data.getFilePointer();

        MonotonicWriter blockOffsets = new MonotonicWriter(
            directory,
            context,
            data.getName(),
            numBlocks + 1L,
            DIRECT_MONOTONIC_BLOCK_SHIFT
        );
        MonotonicWriter valueAddresses = null;
        try {
            if (multiValued) {
                valueAddresses = new MonotonicWriter(
                    directory,
                    context,
                    data.getName(),
                    numDocsWithField + 1L,
                    DIRECT_MONOTONIC_BLOCK_SHIFT
                );
            }

            // Seam: a non-default pipeline could be selected per field here (e.g. from a field attribute).
            NumericPipeline pipeline = NumericPipeline.defaultPipeline(BLOCK_SIZE);
            NumericBlockEncoder encoder = new NumericBlockEncoder(pipeline, BLOCK_SIZE);
            long[] buffer = new long[BLOCK_SIZE];
            int inBlock = 0;
            int ordinal = 0;
            NumericColumnValues values = cursors.get();
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                if (multiValued) {
                    valueAddresses.add(ordinal);
                }
                int count = values.valueCount();
                for (int i = 0; i < count; i++) {
                    if (inBlock == 0) {
                        blockOffsets.add(data.getFilePointer() - valuesOffset);
                    }
                    buffer[inBlock++] = values.nextValue();
                    ordinal++;
                    if (inBlock == BLOCK_SIZE) {
                        blockBytesCodec.write(out -> encoder.encode(buffer, out), data);
                        inBlock = 0;
                    }
                }
            }
            if (inBlock > 0) {
                for (int i = inBlock; i < BLOCK_SIZE; i++) {
                    buffer[i] = 0;
                }
                blockBytesCodec.write(out -> encoder.encode(buffer, out), data);
            }
            if (multiValued) {
                valueAddresses.add(ordinal);
            }
            blockOffsets.add(data.getFilePointer() - valuesOffset);

            MonotonicWriter.Table blocks = blockOffsets.finish(data);
            MonotonicWriter.Table addresses = multiValued ? valueAddresses.finish(data) : MonotonicWriter.Table.NONE;

            return new NumericColumnMetadata(
                iterator,
                numDocsWithField,
                numValues,
                BLOCK_SIZE,
                blockBytesCodec.id(),
                pipeline.terminalId(),
                pipeline.transformIds(),
                valuesOffset,
                blocks.dataOffset(),
                blocks.dataLength(),
                blocks.meta(),
                addresses.dataOffset(),
                addresses.dataLength(),
                addresses.meta(),
                null // the skip index, when present, is attached by the consumer after the values are written
            );
        } finally {
            IOUtils.close(blockOffsets, valueAddresses);
        }
    }
}
