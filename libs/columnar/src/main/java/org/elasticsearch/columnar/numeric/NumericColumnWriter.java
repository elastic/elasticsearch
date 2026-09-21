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
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOSupplier;
import org.elasticsearch.columnar.substrate.BlockBytesCodec;
import org.elasticsearch.columnar.substrate.ColumnIteratorMetadata;
import org.elasticsearch.columnar.substrate.ColumnIteratorWriter;
import org.elasticsearch.columnar.substrate.ColumnOutputs;
import org.elasticsearch.columnar.substrate.MonotonicWriter;

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
 * document's value address is its iterator rank.
 */
public final class NumericColumnWriter {

    private NumericColumnWriter() {}

    /**
     * Encodes a numeric column into {@code data}: iterator metadata, block-encoded values, and an
     * optional skip index; returns the column metadata needed to reconstruct the column at read time.
     *
     * @param maxDoc           documents in the segment
     * @param numDocsWithField documents that have at least one value
     * @param numValues        total number of values across all documents
     * @param valueAddressed   whether to table where each document's values begin, which a caller reading
     *                         the column by value address rather than by document does not need
     * @param cursors          supplies fresh forward cursors over the documents that have a value;
     *                         called once for iterator and once for the values
     * @param pipeline         the encoding pipeline; obtain via {@link NumericPipelineSelector} or
     *                         a {@link NumericPipeline} named factory; the pipeline carries its own
     *                         block size via {@link NumericPipeline#blockSize()}
     * @param blockBytesCodec  terminal byte codec applied to each block
     * @param skipCodec        skip-index codec fed inline during the value-encode pass, or {@code null}
     *                         to write no skip index
     * @param outputs          value blocks to its data, the iterator and value addresses to its addressing,
     *                         block offsets to its navigation
     * @param skipIndex        skip-index output (the skip region is appended)
     */
    public static NumericColumnMetadata write(
        int maxDoc,
        int numDocsWithField,
        long numValues,
        boolean valueAddressed,
        IOSupplier<NumericColumnValues> cursors,
        NumericPipeline pipeline,
        BlockBytesCodec blockBytesCodec,
        SkipIndexCodec skipCodec,
        ColumnOutputs outputs,
        IndexOutput skipIndex
    ) throws IOException {
        ColumnIteratorMetadata iterator = ColumnIteratorWriter.write(cursors.get(), numDocsWithField, maxDoc, outputs.addressing());
        if (numDocsWithField == 0) {
            return NumericColumnMetadata.empty(iterator, blockBytesCodec.id());
        }

        boolean tableAddresses = valueAddressed && numValues > numDocsWithField;

        // The values go straight into the data, and a document's first value address, when tabled, into the
        // addressing: nothing else writes either file while this column is being written.
        final LongBlocks.Writer blocks = new LongBlocks.Writer(pipeline, blockBytesCodec, outputs.data(), outputs.navigation());
        final MonotonicWriter valueAddresses = tableAddresses ? new MonotonicWriter(outputs.addressing()) : null;

        long valueAddress = 0;
        SkipIndexCodec.Writer skip = skipCodec == null ? null : skipCodec.writer();
        NumericColumnValues values = cursors.get();
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            if (tableAddresses) {
                valueAddresses.add(valueAddress);
            }
            int count = values.valueCount();
            if (skip != null) {
                skip.startDoc(doc, count);
            }
            for (int i = 0; i < count; i++) {
                long value = values.nextValue();
                if (skip != null) {
                    skip.add(value);
                }
                blocks.add(value);
                valueAddress++;
            }
        }
        if (tableAddresses) {
            valueAddresses.add(valueAddress);
        }
        final LongBlocks.Metadata written = blocks.finish();
        MonotonicWriter.Table addresses = tableAddresses ? valueAddresses.finish() : MonotonicWriter.Table.NONE;

        // The writer buffered the skip bytes while being fed inline; they are flushed here, so the
        // recorded offset is the skip-index file's pointer.
        NumericColumnMetadata.Skipper skipper = skip == null ? null : skip.finish(skipIndex);

        return new NumericColumnMetadata(
            iterator,
            numDocsWithField,
            numValues,
            written.blockSize(),
            written.blockBytesCodecId(),
            written.terminalId(),
            written.transformIds(),
            written.valuesOffset(),
            written.blockOffsets().dataOffset(),
            written.blockOffsets().dataLength(),
            written.blockOffsets().meta(),
            addresses.dataOffset(),
            addresses.dataLength(),
            addresses.meta(),
            skipper
        );
    }
}
