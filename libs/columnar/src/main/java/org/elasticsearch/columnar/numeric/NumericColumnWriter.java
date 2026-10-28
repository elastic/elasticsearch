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
 * per-document value addresses — are written through {@link MonotonicWriter} straight into their files. The
 * value-address table is written only when {@code numValues > numDocsWithField}; otherwise a
 * document's value address is its iterator rank.
 */
public final class NumericColumnWriter {

    private NumericColumnWriter() {}

    /**
     * Encodes a numeric column into {@code outputs}: iterator metadata, block-encoded values, and an
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
        ColumnIteratorMetadata iterator = ColumnIteratorWriter.write(cursors, numDocsWithField, maxDoc, outputs.addressing());
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
        final long[] consecutive = new long[pipeline.blockSize()];
        int read = values.nextValuesOfConsecutiveDocuments(consecutive);
        if (read >= 0) {
            // A value a document: each document's first value address is its rank, so there is nothing to table.
            assert tableAddresses == false : "a column of a value a document tables no value addresses";
            for (; read > 0; read = values.nextValuesOfConsecutiveDocuments(consecutive)) {
                if (skip != null) {
                    final int first = values.docID() - read + 1;
                    for (int i = 0; i < read; i++) {
                        skip.startDoc(first + i, 1);
                        skip.add(consecutive[i]);
                    }
                }
                blocks.add(consecutive, read);
                valueAddress += read;
            }
        } else {
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
        }
        if (tableAddresses) {
            valueAddresses.add(valueAddress);
        }
        final LongBlocks.Metadata written = blocks.finish();
        MonotonicWriter.Table addresses = tableAddresses ? valueAddresses.finish() : MonotonicWriter.Table.NONE;

        // The writer buffered the skip bytes while being fed inline; they are flushed here, so the
        // recorded offset is the skip-index file's pointer.
        NumericColumnMetadata.Skipper skipper = skip == null ? null : skip.finish(skipIndex);

        return metadata(iterator, numDocsWithField, numValues, written, addresses, skipper);
    }

    /** The column's metadata from the parts that were written, so every writer of one describes it the same way. */
    private static NumericColumnMetadata metadata(
        ColumnIteratorMetadata iterator,
        int numDocsWithField,
        long numValues,
        LongBlocks.Metadata written,
        MonotonicWriter.Table addresses,
        NumericColumnMetadata.Skipper skipper
    ) {
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

    /**
     * Writes a numeric column pushed to it a document at a time, for a caller that produces the values while
     * walking something else and cannot hand over a cursor to be pulled twice. What it writes is what
     * {@link #write} writes for a column every document of which has a value and nothing reads by document:
     * its presence is {@link ColumnIteratorMetadata#dense dense} over the documents it was fed, and it tables
     * no value addresses, so a value is reached by its address alone.
     *
     * <p>The blocks go straight into the data as they fill, so nothing else may write the data between the
     * first {@link #add} and {@link #finish}. The skip index, when a codec is given, is fed inline and buffered,
     * and whether it is kept is decided at {@link #finish}: a caller that only learns at the end of its pass
     * whether the column is one a reader will skip over does not have to know up front.
     */
    public static final class Stream {

        private final LongBlocks.Writer blocks;
        private final SkipIndexCodec.Writer skip;
        private int numDocs;
        private long numValues;

        /**
         * @param pipeline        the encoding pipeline, which carries its block size
         * @param blockBytesCodec terminal byte codec applied to each block
         * @param skipCodec       skip-index codec fed inline, or {@code null} to build no skip index; feeding
         *                        one costs its accumulators, so pass {@code null} where no skipper could be
         *                        wanted
         * @param outputs         value blocks to its data, block offsets to its navigation
         */
        public Stream(NumericPipeline pipeline, BlockBytesCodec blockBytesCodec, SkipIndexCodec skipCodec, ColumnOutputs outputs) {
            this.blocks = new LongBlocks.Writer(pipeline, blockBytesCodec, outputs.data(), outputs.navigation());
            this.skip = skipCodec == null ? null : skipCodec.writer();
        }

        /** Begins document {@code doc}, whose {@code valueCount} values follow through {@link #add}. */
        public void startDoc(int doc, int valueCount) {
            numDocs++;
            if (skip != null) {
                skip.startDoc(doc, valueCount);
            }
        }

        /** The next value of the current document. */
        public void add(long value) throws IOException {
            if (skip != null) {
                skip.add(value);
            }
            blocks.add(value);
            numValues++;
        }

        /**
         * Writes the last block and answers the column's metadata.
         *
         * @param writeSkipper whether to append the skip index this stream was fed to {@code skipIndex} and
         *                     record it; when false the buffered skip bytes are dropped and nothing is written
         *                     there. Must be false when the stream was given no skip codec.
         * @param skipIndex    where the skip index goes when it is kept
         */
        public NumericColumnMetadata finish(boolean writeSkipper, IndexOutput skipIndex) throws IOException {
            if (writeSkipper && skip == null) {
                throw new IllegalStateException("asked to write a skip index the stream was not fed");
            }
            final LongBlocks.Metadata written = blocks.finish();
            // Flushed here, after the values, so the recorded offset is the skip-index file's pointer. A
            // skipper that is dropped was buffered and never reached a file, so dropping it leaves no trace.
            final NumericColumnMetadata.Skipper skipper = writeSkipper ? skip.finish(skipIndex) : null;
            return metadata(ColumnIteratorMetadata.dense(numDocs), numDocs, numValues, written, MonotonicWriter.Table.NONE, skipper);
        }
    }
}
