/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License, v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.mapper.BinaryRangeUtil;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingBinaryDocValues;

import java.io.IOException;

/**
 * Loads double range binary doc values into paired double blocks, converting Lucene's inclusive
 * upper bound to the half-open representation used by ES|QL.
 */
public class DoubleRangeDocValuesLoader extends BlockDocValuesReader.DocValuesBlockLoader {
    private final String fieldName;

    public DoubleRangeDocValuesLoader(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.doubleRangeBuilder(expectedCount);
    }

    @Override
    public ColumnAtATimeReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        TrackingBinaryDocValues dv = TrackingBinaryDocValues.get(breaker, context, fieldName);
        if (dv == null) {
            return ConstantNull.COLUMN_READER;
        }
        return new DoubleRangeDocValuesReader(dv);
    }

    private class DoubleRangeDocValuesReader extends BlockDocValuesReader {
        private final TrackingBinaryDocValues docValues;
        private int docId = -1;

        DoubleRangeDocValuesReader(TrackingBinaryDocValues docValues) {
            super(null);
            this.docValues = docValues;
        }

        @Override
        protected int docId() {
            return docId;
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.DoubleRangeDocValuesReader";
        }

        @Override
        public BlockLoader.Block read(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset, boolean nullsFiltered)
            throws IOException {
            try (BlockLoader.DoubleRangeBuilder builder = factory.doubleRangeBuilder(docs.count() - offset)) {
                int lastDoc = -1;
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (doc < lastDoc) {
                        throw new IllegalStateException("docs within same block must be in order");
                    }
                    lastDoc = doc;
                    docId = doc;
                    if (docValues.docValues().advanceExact(doc) == false) {
                        builder.appendNull();
                        continue;
                    }
                    BytesRef ref = docValues.docValues().binaryValue();
                    var ranges = BinaryRangeUtil.decodeDoubleRanges(ref);
                    if (ranges.isEmpty()) {
                        builder.appendNull();
                    } else if (ranges.size() == 1) {
                        var range = ranges.get(0);
                        builder.from().appendDouble((double) range.getFrom());
                        // convert inclusive to exclusive bound
                        builder.to().appendDouble(Math.nextUp((double) range.getTo()));
                    } else {
                        builder.from().beginPositionEntry();
                        builder.to().beginPositionEntry();
                        for (var range : ranges) {
                            builder.from().appendDouble((double) range.getFrom());
                            // convert inclusive to exclusive bound
                            builder.to().appendDouble(Math.nextUp((double) range.getTo()));
                        }
                        builder.from().endPositionEntry();
                        builder.to().endPositionEntry();
                    }
                }
                return builder.build();
            }
        }

        @Override
        public void close() {
            docValues.close();
        }
    }
}
