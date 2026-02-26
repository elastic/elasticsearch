/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
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

public class DateRangeDocValuesLoader extends BlockDocValuesReader.DocValuesBlockLoader {
    private final String fieldName;

    public DateRangeDocValuesLoader(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.longRangeBuilder(expectedCount);
    }

    @Override
    public AllReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        TrackingBinaryDocValues dv = TrackingBinaryDocValues.get(breaker, context, fieldName);
        if (dv == null) {
            return ConstantNull.READER;
        }
        return new DateRangeDocValuesReader(dv);
    }

    private class DateRangeDocValuesReader extends BlockDocValuesReader {
        private final TrackingBinaryDocValues docValues;

        DateRangeDocValuesReader(TrackingBinaryDocValues docValues) {
            super(null);
            this.docValues = docValues;
        }

        private int docId = -1;

        @Override
        protected int docId() {
            return docId;
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.DateRangeDocValuesReader";
        }

        @Override
        public BlockLoader.Block read(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset, boolean nullsFiltered)
            throws IOException {
            try (BlockLoader.LongRangeBuilder builder = factory.longRangeBuilder(docs.count() - offset)) {
                int lastDoc = -1;
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (doc < lastDoc) {
                        throw new IllegalStateException("docs within same block must be in order");
                    }
                    if (false == docValues.docValues().advanceExact(doc)) {
                        builder.appendNull();
                    } else {
                        BytesRef ref = docValues.docValues().binaryValue();
                        var ranges = BinaryRangeUtil.decodeLongRanges(ref);
                        for (var range : ranges) {
                            lastDoc = doc;
                            this.docId = doc;
                            builder.from().appendLong((long) range.getFrom());
                            builder.to().appendLong((long) range.getTo());
                        }
                    }
                }
                return builder.build();
            }
        }

        @Override
        public void read(int doc, BlockLoader.StoredFields storedFields, BlockLoader.Builder builder) throws IOException {
            var blockBuilder = (BlockLoader.LongRangeBuilder) builder;
            this.docId = doc;
            if (false == docValues.docValues().advanceExact(doc)) {
                blockBuilder.appendNull();
            } else {
                var range = BinaryRangeUtil.decodeLongRanges(docValues.docValues().binaryValue());
                assert range.size() == 1 : "stored fields should only have a single range";
                blockBuilder.from().appendLong((long) range.getFirst().getFrom());
                blockBuilder.to().appendLong((long) range.getFirst().getTo());
            }
        }

        @Override
        public void close() {
            docValues.close();
        }
    }
}
