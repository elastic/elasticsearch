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
 * Loads {@code double}s from doc values.
 */
public abstract class AbstractDoublesFromDocValuesBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {
    protected final String fieldName;
    private final BlockDocValuesReader.ToDouble toDouble;

    public AbstractDoublesFromDocValuesBlockLoader(String fieldName, BlockDocValuesReader.ToDouble toDouble) {
        this.fieldName = fieldName;
        this.toDouble = toDouble;
    }

    @Override
    public final Builder builder(BlockFactory factory, int expectedCount) {
        return factory.doubles(expectedCount);
    }

    @Override
    public final AllReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        breaker.addEstimateBytesAndMaybeBreak(ESTIMATED_SIZE, "load blocks");
        boolean release = true;
        try {
            SortedNumericDocValues docValues = context.reader().getSortedNumericDocValues(fieldName);
            if (docValues != null) {
                release = false;
                NumericDocValues singleton = DocValues.unwrapSingleton(docValues);
                if (singleton != null) {
                    return singletonReader(breaker, singleton, toDouble);
                }
                return sortedReader(breaker, docValues, toDouble);
            }
            NumericDocValues singleton = context.reader().getNumericDocValues(fieldName);
            if (singleton != null) {
                release = false;
                return singletonReader(breaker, singleton, toDouble);
            }
            return ConstantNull.READER;
        } finally {
            if (release) {
                breaker.addWithoutBreaking(-ESTIMATED_SIZE);
            }
        }
    }

    protected abstract AllReader singletonReader(
        CircuitBreaker breaker,
        NumericDocValues docValues,
        BlockDocValuesReader.ToDouble toDouble
    );

    protected abstract AllReader sortedReader(
        CircuitBreaker breaker,
        SortedNumericDocValues docValues,
        BlockDocValuesReader.ToDouble toDouble
    );

    protected abstract static class DoublesBlockDocValuesReader extends BlockDocValuesReader {
        public DoublesBlockDocValuesReader(CircuitBreaker breaker) {
            super(breaker);
        }

        @Override
        public final void close() {
            breaker.addWithoutBreaking(-ESTIMATED_SIZE);
        }
    }

    public static class Singleton extends DoublesBlockDocValuesReader implements BlockDocValuesReader.NumericDocValuesAccessor {
        private final NumericDocValues docValues;
        private final ToDouble toDouble;

        public Singleton(CircuitBreaker breaker, NumericDocValues docValues, ToDouble toDouble) {
            super(breaker);
            this.docValues = docValues;
            this.toDouble = toDouble;
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            if (docValues instanceof OptionalColumnAtATimeReader direct) {
                Block result = direct.tryRead(factory, docs, offset, nullsFiltered, toDouble, false, false);
                if (result != null) {
                    return result;
                }
            }
            try (DoubleBuilder builder = factory.doublesFromDocValues(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (docValues.advanceExact(doc)) {
                        builder.appendDouble(toDouble.convert(docValues.longValue()));
                    } else {
                        builder.appendNull();
                    }
                }
                return builder.build();
            }
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            DoubleBuilder blockBuilder = (DoubleBuilder) builder;
            if (docValues.advanceExact(docId)) {
                blockBuilder.appendDouble(toDouble.convert(docValues.longValue()));
            } else {
                blockBuilder.appendNull();
            }
        }

        @Override
        public int docId() {
            return docValues.docID();
        }

        @Override
        public String toString() {
            return "DoublesFromDocValues.Singleton";
        }

        @Override
        public NumericDocValues numericDocValues() {
            return docValues;
        }
    }

    public static class Sorted extends DoublesBlockDocValuesReader {
        private final SortedNumericDocValues docValues;
        private final ToDouble toDouble;

        Sorted(CircuitBreaker breaker, SortedNumericDocValues docValues, ToDouble toDouble) {
            super(breaker);
            this.docValues = docValues;
            this.toDouble = toDouble;
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            try (DoubleBuilder builder = factory.doublesFromDocValues(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    read(doc, builder);
                }
                return builder.build();
            }
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            read(docId, (DoubleBuilder) builder);
        }

        private void read(int doc, DoubleBuilder builder) throws IOException {
            if (false == docValues.advanceExact(doc)) {
                builder.appendNull();
                return;
            }
            int count = docValues.docValueCount();
            if (count == 1) {
                builder.appendDouble(toDouble.convert(docValues.nextValue()));
                return;
            }
            builder.beginPositionEntry();
            for (int v = 0; v < count; v++) {
                builder.appendDouble(toDouble.convert(docValues.nextValue()));
            }
            builder.endPositionEntry();
        }

        @Override
        public int docId() {
            return docValues.docID();
        }

        @Override
        public String toString() {
            return "DoublesFromDocValues.Sorted";
        }
    }
}
