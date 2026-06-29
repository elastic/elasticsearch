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
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.NumericDvSingletonOrSorted;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingSortedNumericDocValues;

import java.io.IOException;
import java.util.function.LongFunction;

/**
 * Block loader for point-encoded fields (e.g. {@code geo_point}, {@code point}) that handles an edge case where doc
 * values are available yet {@code FieldExtractPreference = NONE}. When this happens, the BlockLoader sanity checker (see
 * {@code PlannerUtils.toElementType}) expects a {@code BytesRef}, which implies loading the value from {@code _source} -
 * very slow, especially with synthetic source. We are better off reading the encoded long from doc values and converting
 * it to a {@code BytesRef} (WKB) to satisfy the checker. The caller supplies the type-specific {@code long -> BytesRef}
 * decoding.
 */
public final class LongToBytesRefBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {

    private final String fieldName;
    private final LongFunction<BytesRef> longToBytesRef;

    public LongToBytesRefBlockLoader(String fieldName, LongFunction<BytesRef> longToBytesRef) {
        this.fieldName = fieldName;
        this.longToBytesRef = longToBytesRef;
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.bytesRefs(expectedCount);
    }

    @Override
    public ColumnAtATimeReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        NumericDvSingletonOrSorted dv = NumericDvSingletonOrSorted.get(breaker, context, fieldName);
        if (dv == null) {
            return ConstantNull.COLUMN_READER;
        }
        TrackingSortedNumericDocValues sorted = dv.forceSorted();
        return new BytesRefsFromLong(sorted, longToBytesRef);
    }

    private static final class BytesRefsFromLong extends BlockDocValuesReader {

        private final TrackingSortedNumericDocValues numericDocValues;
        private final LongFunction<BytesRef> longToBytesRef;

        BytesRefsFromLong(TrackingSortedNumericDocValues numericDocValues, LongFunction<BytesRef> longToBytesRef) {
            super(null);
            this.numericDocValues = numericDocValues;
            this.longToBytesRef = longToBytesRef;
        }

        @Override
        protected int docId() {
            return numericDocValues.docValues().docID();
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.BytesRefsFromLong";
        }

        @Override
        public BlockLoader.Block read(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset, boolean nullsFiltered)
            throws IOException {

            try (BlockLoader.BytesRefBuilder builder = factory.bytesRefs(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    read(doc, builder);
                }
                return builder.build();
            }
        }

        private void read(int doc, BlockLoader.BytesRefBuilder builder) throws IOException {
            if (numericDocValues.docValues().advanceExact(doc) == false) {
                builder.appendNull();
                return;
            }
            int count = numericDocValues.docValues().docValueCount();
            if (count == 1) {
                BytesRef bytesRefValue = longToBytesRef.apply(numericDocValues.docValues().nextValue());
                builder.appendBytesRef(bytesRefValue);
                return;
            }
            builder.beginPositionEntry();
            for (int v = 0; v < count; v++) {
                BytesRef bytesRefValue = longToBytesRef.apply(numericDocValues.docValues().nextValue());
                builder.appendBytesRef(bytesRefValue);
            }
            builder.endPositionEntry();
        }

        @Override
        public void close() {
            numericDocValues.close();
        }
    }
}
