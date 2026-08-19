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

    /** Monotonic block shift for the offset tables. */
    static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

    private NumericColumnWriter() {}

    /**
     * Encodes a numeric column into {@code data}: iterator metadata, block-encoded values, and an
     * optional skip index; returns the column metadata needed to reconstruct the column at read time.
     *
     * @param maxDoc           documents in the segment
     * @param numDocsWithField documents that have at least one value
     * @param numValues        total number of values across all documents
     * @param cursors          supplies fresh forward cursors over the documents that have a value;
     *                         called once for iterator and once for the values
     * @param pipeline         the encoding pipeline; obtain via {@link NumericPipelineSelector} or
     *                         a {@link NumericPipeline} named factory; the pipeline carries its own
     *                         block size via {@link NumericPipeline#blockSize()}
     * @param blockBytesCodec  terminal byte codec applied to each block
     * @param skipCodec        skip-index codec fed inline during the value-encode pass, or {@code null}
     *                         to write no skip index
     * @param directory        directory used for the temporary table files
     * @param context          IO context for the temporary table files
     * @param data             data output (iterator, value blocks, and tables are appended)
     * @param skipIndex        skip-index output (the skip region is appended)
     */
    public static NumericColumnMetadata write(
        int maxDoc,
        int numDocsWithField,
        long numValues,
        IOSupplier<NumericColumnValues> cursors,
        NumericPipeline pipeline,
        BlockBytesCodec blockBytesCodec,
        SkipIndexCodec skipCodec,
        Directory directory,
        IOContext context,
        IndexOutput data,
        IndexOutput skipIndex
    ) throws IOException {
        ColumnIteratorMetadata iterator = ColumnIteratorWriter.write(cursors.get(), numDocsWithField, maxDoc, data);
        if (numDocsWithField == 0) {
            return NumericColumnMetadata.empty(iterator, blockBytesCodec.id());
        }

        int blockSize = pipeline.blockSize();
        boolean multiValued = numValues > numDocsWithField;
        long numBlocks = (numValues + blockSize - 1) / blockSize;
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

            NumericBlockEncoder encoder = new NumericBlockEncoder(pipeline, blockSize);
            long[] buffer = new long[blockSize];
            // One reusable encoder closure over the buffer, so no lambda is allocated per block flush;
            // blockValueCount carries the count of the block currently being written.
            int[] blockValueCount = new int[1];
            BlockBytesCodec.BlockEncoder blockEncoder = out -> encoder.encode(buffer, blockValueCount[0], out);
            int inBlock = 0;
            long ordinal = 0;
            SkipIndexCodec.Writer skip = skipCodec == null ? null : skipCodec.writer();
            NumericColumnValues values = cursors.get();
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                if (multiValued) {
                    valueAddresses.add(ordinal);
                }
                int count = values.valueCount();
                if (skip != null) {
                    skip.startDoc(doc, count);
                }
                for (int i = 0; i < count; i++) {
                    if (inBlock == 0) {
                        blockOffsets.add(data.getFilePointer() - valuesOffset);
                    }
                    long value = values.nextValue();
                    if (skip != null) {
                        skip.add(value);
                    }
                    buffer[inBlock++] = value;
                    ordinal++;
                    if (inBlock == blockSize) {
                        blockValueCount[0] = blockSize;
                        blockBytesCodec.write(blockEncoder, data);
                        inBlock = 0;
                    }
                }
            }
            if (inBlock > 0) {
                // The final block holds fewer than blockSize values; the encoder is told the real count
                // and never sees padding, so each stage fits only the real data.
                blockValueCount[0] = inBlock;
                blockBytesCodec.write(blockEncoder, data);
            }
            if (multiValued) {
                valueAddresses.add(ordinal);
            }
            blockOffsets.add(data.getFilePointer() - valuesOffset);

            MonotonicWriter.Table blocks = blockOffsets.finish(data);
            MonotonicWriter.Table addresses = multiValued ? valueAddresses.finish(data) : MonotonicWriter.Table.NONE;

            // The writer buffered the skip bytes while being fed inline; they are flushed here, so the
            // recorded offset is the skip-index file's pointer.
            NumericColumnMetadata.Skipper skipper = skip == null ? null : skip.finish(skipIndex);

            return new NumericColumnMetadata(
                iterator,
                numDocsWithField,
                numValues,
                blockSize,
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
                skipper
            );
        } finally {
            IOUtils.close(blockOffsets, valueAddresses);
        }
    }
}
