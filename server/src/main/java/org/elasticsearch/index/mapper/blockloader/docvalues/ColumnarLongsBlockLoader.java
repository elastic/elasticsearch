/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.columnar.numeric.ColumnarNumericBinaryDocValues;
import org.elasticsearch.columnar.numeric.NumericBinaryPayload;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingBinaryDocValues;

import java.io.IOException;

/**
 * Loads {@code long}s from a field whose doc values the ColumNAR codec stores: binary doc values holding a
 * {@link NumericBinaryPayload} per document. A document's values come out in the order they were written, which
 * is the array order a strict columnar index keeps. A page of a single-valued column is read straight out of the
 * column's decoded blocks.
 */
public class ColumnarLongsBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {

    private final String fieldName;

    public ColumnarLongsBlockLoader(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public LongBuilder builder(BlockFactory factory, int expectedCount) {
        return factory.longs(expectedCount);
    }

    @Override
    public ColumnAtATimeReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        TrackingBinaryDocValues dv = TrackingBinaryDocValues.get(breaker, context, fieldName);
        if (dv == null) {
            return ConstantNull.COLUMN_READER;
        }
        return new Reader(dv);
    }

    @Override
    public String toString() {
        return "ColumnarLongsFromDocValues[" + fieldName + "]";
    }

    private static final class Reader extends BlockDocValuesReader {
        private final TrackingBinaryDocValues docValues;
        private final NumericBinaryPayload.Decoder decoder = new NumericBinaryPayload.Decoder();
        private int[] docIds = new int[0];

        Reader(TrackingBinaryDocValues docValues) {
            super(null);
            this.docValues = docValues;
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            final int count = docs.count() - offset;
            try (LongBuilder builder = factory.longsFromDocValues(count)) {
                if (docValues.docValues() instanceof ColumnarNumericBinaryDocValues columnar) {
                    if (readPage(columnar, docs, offset, count, builder) == false) {
                        for (int i = offset; i < docs.count(); i++) {
                            readDoc(columnar, docs.get(i), builder);
                        }
                    }
                } else {
                    // Payloads that haven't been through the codec.
                    for (int i = offset; i < docs.count(); i++) {
                        readPayload(docs.get(i), builder);
                    }
                }
                return builder.build();
            }
        }

        /**
         * Reads a page of a dense single-valued column in bulk. Returns {@code false} without appending anything when the
         * column declines the page.
         *
         * <p>Only a dense column is read this way: the column resolves the page's documents before it finds one without a
         * value, which moves the doc values past documents the per-document fallback would then have to go back to.
         */
        // TODO: read sparse columns in bulk too, once declining a page leaves the doc values where they were.
        private boolean readPage(ColumnarNumericBinaryDocValues columnar, Docs docs, int offset, int count, LongBuilder builder)
            throws IOException {
            if (columnar.isSingleValued() == false || columnar.isDense() == false || count == 0) {
                return false;
            }
            if (docIds.length < count) {
                docIds = new int[ArrayUtil.oversize(count, Integer.BYTES)];
            }
            for (int i = 0; i < count; i++) {
                docIds[i] = docs.get(offset + i);
            }
            return columnar.bulkLongs(docIds, 0, count, docs.mayContainDuplicates(), (values, from, length) -> {
                for (int i = from; i < from + length; i++) {
                    builder.appendLong(values[i]);
                }
            });
        }

        private static void readDoc(ColumnarNumericBinaryDocValues columnar, int doc, LongBuilder builder) throws IOException {
            if (columnar.advanceExact(doc) == false) {
                builder.appendNull();
                return;
            }
            final int count = columnar.docValueCount();
            if (count == 1) {
                builder.appendLong(columnar.valueAt(0));
                return;
            }
            builder.beginPositionEntry();
            for (int i = 0; i < count; i++) {
                builder.appendLong(columnar.valueAt(i));
            }
            builder.endPositionEntry();
        }

        private void readPayload(int doc, LongBuilder builder) throws IOException {
            final BinaryDocValues binary = docValues.docValues();
            if (binary.advanceExact(doc) == false) {
                builder.appendNull();
                return;
            }
            decoder.reset(binary.binaryValue());
            final long[] values = decoder.values();
            final int count = decoder.valueCount();
            if (count == 0) {
                // Only null slots, or an empty array.
                builder.appendNull();
            } else if (count == 1) {
                builder.appendLong(values[0]);
            } else {
                builder.beginPositionEntry();
                for (int i = 0; i < count; i++) {
                    builder.appendLong(values[i]);
                }
                builder.endPositionEntry();
            }
        }

        @Override
        protected int docId() {
            return docValues.docValues().docID();
        }

        @Override
        public void close() {
            docValues.close();
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.ColumnarLongs";
        }
    }
}
