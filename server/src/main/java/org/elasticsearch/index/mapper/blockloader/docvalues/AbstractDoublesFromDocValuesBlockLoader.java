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
import org.apache.lucene.index.NumericDocValues;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.NumericDvSingletonOrSorted;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingNumericDocValues;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingSortedNumericDocValues;

import java.io.IOException;

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
        NumericDvSingletonOrSorted dv = NumericDvSingletonOrSorted.get(breaker, context, fieldName);
        if (dv == null) {
            return ConstantNull.READER;
        }
        if (dv.singleton() != null) {
            return singletonReader(dv.singleton(), toDouble);
        }
        return sortedReader(dv.sorted(), toDouble);
    }

    protected abstract AllReader singletonReader(TrackingNumericDocValues docValues, BlockDocValuesReader.ToDouble toDouble);

    protected abstract AllReader sortedReader(TrackingSortedNumericDocValues docValues, BlockDocValuesReader.ToDouble toDouble);

    public static class Singleton extends BlockDocValuesReader implements BlockDocValuesReader.NumericDocValuesAccessor {
        private final TrackingNumericDocValues docValues;
        private final ToDouble toDouble;

        public Singleton(TrackingNumericDocValues docValues, ToDouble toDouble) {
            super(null);
            this.docValues = docValues;
            this.toDouble = toDouble;
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            if (docValues.docValues() instanceof OptionalColumnAtATimeReader direct) {
                Block result = direct.tryRead(factory, docs, offset, nullsFiltered, toDouble, false, false);
                if (result != null) {
                    return result;
                }
            }
            try (DoubleBuilder builder = factory.doublesFromDocValues(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (docValues.docValues().advanceExact(doc)) {
                        builder.appendDouble(toDouble.convert(docValues.docValues().longValue()));
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
            if (docValues.docValues().advanceExact(docId)) {
                blockBuilder.appendDouble(toDouble.convert(docValues.docValues().longValue()));
            } else {
                blockBuilder.appendNull();
            }
        }

        @Override
        public int docId() {
            return docValues.docValues().docID();
        }

        @Override
        public String toString() {
            return "DoublesFromDocValues.Singleton";
        }

        @Override
        public NumericDocValues numericDocValues() {
            return docValues.docValues();
        }

        @Override
        public void close() {
            docValues.close();
        }
    }

    public static class Sorted extends BlockDocValuesReader {
        private final TrackingSortedNumericDocValues docValues;
        private final ToDouble toDouble;

        Sorted(TrackingSortedNumericDocValues docValues, ToDouble toDouble) {
            super(null);
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
            if (false == docValues.docValues().advanceExact(doc)) {
                builder.appendNull();
                return;
            }
            int count = docValues.docValues().docValueCount();
            if (count == 1) {
                builder.appendDouble(toDouble.convert(docValues.docValues().nextValue()));
                return;
            }
            builder.beginPositionEntry();
            for (int v = 0; v < count; v++) {
                builder.appendDouble(toDouble.convert(docValues.docValues().nextValue()));
            }
            builder.endPositionEntry();
        }

        @Override
        public int docId() {
            return docValues.docValues().docID();
        }

        @Override
        public String toString() {
            return "DoublesFromDocValues.Sorted";
        }

        @Override
        public void close() {
            docValues.close();
        }
    }
}
