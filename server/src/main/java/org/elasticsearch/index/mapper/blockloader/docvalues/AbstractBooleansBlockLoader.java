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
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.NumericDvSingletonOrSorted;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingNumericDocValues;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingSortedNumericDocValues;

import java.io.IOException;

/**
 * Loads {@code boolean}s from doc values.
 */
public abstract class AbstractBooleansBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {
    protected final String fieldName;

    public AbstractBooleansBlockLoader(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public final BooleanBuilder builder(BlockFactory factory, int expectedCount) {
        return factory.booleans(expectedCount);
    }

    @Override
    public final ColumnAtATimeReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        NumericDvSingletonOrSorted dv = NumericDvSingletonOrSorted.get(breaker, context, fieldName);
        if (dv == null) {
            return ConstantNull.COLUMN_READER;
        }
        if (dv.singleton() != null) {
            return singletonReader(dv.singleton());
        }
        return sortedReader(dv.sorted());
    }

    protected abstract ColumnAtATimeReader singletonReader(TrackingNumericDocValues docValues);

    protected abstract ColumnAtATimeReader sortedReader(TrackingSortedNumericDocValues docValues);

    public static class Singleton extends BlockDocValuesReader {
        private final TrackingNumericDocValues numericDocValues;

        public Singleton(TrackingNumericDocValues numericDocValues) {
            super(null);
            this.numericDocValues = numericDocValues;
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            try (BooleanBuilder builder = factory.booleansFromDocValues(docs.count() - offset)) {
                int lastDoc = -1;
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (doc < lastDoc) {
                        throw new IllegalStateException("docs within same block must be in order");
                    }
                    if (numericDocValues.docValues().advanceExact(doc)) {
                        builder.appendBoolean(numericDocValues.docValues().longValue() != 0);
                    } else {
                        builder.appendNull();
                    }
                    lastDoc = doc;
                }
                return builder.build();
            }
        }

        @Override
        public int docId() {
            return numericDocValues.docValues().docID();
        }

        @Override
        public String toString() {
            return "BooleansFromDocValues.Singleton";
        }

        @Override
        public void close() {
            numericDocValues.close();
        }
    }

    static class Sorted extends BlockDocValuesReader {
        private final TrackingSortedNumericDocValues numericDocValues;

        Sorted(TrackingSortedNumericDocValues numericDocValues) {
            super(null);
            this.numericDocValues = numericDocValues;
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            try (BooleanBuilder builder = factory.booleansFromDocValues(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    read(doc, builder);
                }
                return builder.build();
            }
        }

        private void read(int doc, BooleanBuilder builder) throws IOException {
            if (false == numericDocValues.docValues().advanceExact(doc)) {
                builder.appendNull();
                return;
            }
            int count = numericDocValues.docValues().docValueCount();
            if (count == 1) {
                builder.appendBoolean(numericDocValues.docValues().nextValue() != 0);
                return;
            }
            builder.beginPositionEntry();
            for (int v = 0; v < count; v++) {
                builder.appendBoolean(numericDocValues.docValues().nextValue() != 0);
            }
            builder.endPositionEntry();
        }

        @Override
        public int docId() {
            return numericDocValues.docValues().docID();
        }

        @Override
        public String toString() {
            return "BooleansFromDocValues.Sorted";
        }

        @Override
        public void close() {
            numericDocValues.close();
        }
    }
}
