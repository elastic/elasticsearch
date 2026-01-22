/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;

import java.io.IOException;

import static org.elasticsearch.index.mapper.blockloader.docvalues.AbstractLongsFromDocValuesBlockLoader.ESTIMATED_SIZE;

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
    public AllReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        breaker.addEstimateBytesAndMaybeBreak(ESTIMATED_SIZE, "load blocks");
        boolean release = true;
        try {
            SortedNumericDocValues docValues = context.reader().getSortedNumericDocValues(fieldName);
            if (docValues != null) {
                release = false;
                NumericDocValues singleton = DocValues.unwrapSingleton(docValues);
                if (singleton != null) {
                    return singletonReader(breaker, singleton);
                }
                return sortedReader(breaker, docValues);
            }
            NumericDocValues singleton = context.reader().getNumericDocValues(fieldName);
            if (singleton != null) {
                release = false;
                return singletonReader(breaker, singleton);
            }
            return ConstantNull.READER;
        } finally {
            if (release) {
                breaker.addWithoutBreaking(-ESTIMATED_SIZE);
            }
        }
    }

    protected abstract AllReader singletonReader(CircuitBreaker breaker, NumericDocValues docValues);

    protected abstract AllReader sortedReader(CircuitBreaker breaker, SortedNumericDocValues docValues);

    protected abstract static class BooleansBlockDocValuesReader extends BlockDocValuesReader {
        public BooleansBlockDocValuesReader(CircuitBreaker breaker) {
            super(breaker);
        }

        @Override
        public final void close() {
            breaker.addWithoutBreaking(-ESTIMATED_SIZE);
        }
    }

    public static class Singleton extends BooleansBlockDocValuesReader {
        private final NumericDocValues numericDocValues;

        public Singleton(CircuitBreaker breaker, NumericDocValues numericDocValues) {
            super(breaker);
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
                    if (numericDocValues.advanceExact(doc)) {
                        builder.appendBoolean(numericDocValues.longValue() != 0);
                    } else {
                        builder.appendNull();
                    }
                    lastDoc = doc;
                }
                return builder.build();
            }
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            BooleanBuilder blockBuilder = (BooleanBuilder) builder;
            if (numericDocValues.advanceExact(docId)) {
                blockBuilder.appendBoolean(numericDocValues.longValue() != 0);
            } else {
                blockBuilder.appendNull();
            }
        }

        @Override
        public int docId() {
            return numericDocValues.docID();
        }

        @Override
        public String toString() {
            return "BooleansFromDocValues.Singleton";
        }
    }

    static class Sorted extends BooleansBlockDocValuesReader {
        private final SortedNumericDocValues numericDocValues;

        Sorted(CircuitBreaker breaker, SortedNumericDocValues numericDocValues) {
            super(breaker);
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

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            read(docId, (BooleanBuilder) builder);
        }

        private void read(int doc, BooleanBuilder builder) throws IOException {
            if (false == numericDocValues.advanceExact(doc)) {
                builder.appendNull();
                return;
            }
            int count = numericDocValues.docValueCount();
            if (count == 1) {
                builder.appendBoolean(numericDocValues.nextValue() != 0);
                return;
            }
            builder.beginPositionEntry();
            for (int v = 0; v < count; v++) {
                builder.appendBoolean(numericDocValues.nextValue() != 0);
            }
            builder.endPositionEntry();
        }

        @Override
        public int docId() {
            return numericDocValues.docID();
        }

        @Override
        public String toString() {
            return "BooleansFromDocValues.Sorted";
        }
    }
}
