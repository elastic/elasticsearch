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

import java.io.IOException;

/**
 * Loads {@code long}s from doc values.
 */
public abstract class AbstractLongsFromDocValuesBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {
    protected final String fieldName;

    public AbstractLongsFromDocValuesBlockLoader(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public final Builder builder(BlockFactory factory, int expectedCount) {
        return factory.longs(expectedCount);
    }

    @Override
    public final AllReader reader(LeafReaderContext context) throws IOException {
        SortedNumericDocValues docValues = context.reader().getSortedNumericDocValues(fieldName);
        if (docValues != null) {
            NumericDocValues singleton = DocValues.unwrapSingleton(docValues);
            if (singleton != null) {
                return singletonReader(singleton);
            }
            return sortedReader(docValues);
        }
        NumericDocValues singleton = context.reader().getNumericDocValues(fieldName);
        if (singleton != null) {
            return singletonReader(singleton);
        }
        return new ConstantNullsReader();
    }

    protected abstract AllReader singletonReader(NumericDocValues docValues);

    protected abstract AllReader sortedReader(SortedNumericDocValues docValues);

    public static class Singleton extends BlockDocValuesReader implements BlockDocValuesReader.NumericDocValuesAccessor {
        final NumericDocValues numericDocValues;

        public Singleton(NumericDocValues numericDocValues) {
            this.numericDocValues = numericDocValues;
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            if (numericDocValues instanceof OptionalColumnAtATimeReader direct) {
                Block result = direct.tryRead(factory, docs, offset, nullsFiltered, null, false);
                if (result != null) {
                    return result;
                }
            }
            try (LongBuilder builder = factory.longsFromDocValues(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (numericDocValues.advanceExact(doc)) {
                        builder.appendLong(numericDocValues.longValue());
                    } else {
                        builder.appendNull();
                    }
                }
                return builder.build();
            }
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            LongBuilder blockBuilder = (LongBuilder) builder;
            if (numericDocValues.advanceExact(docId)) {
                blockBuilder.appendLong(numericDocValues.longValue());
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
            return "LongsFromDocValues.Singleton";
        }

        @Override
        public NumericDocValues numericDocValues() {
            return numericDocValues;
        }
    }

    public static class Sorted extends BlockDocValuesReader {
        private final SortedNumericDocValues numericDocValues;

        Sorted(SortedNumericDocValues numericDocValues) {
            this.numericDocValues = numericDocValues;
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            try (LongBuilder builder = factory.longsFromDocValues(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    read(doc, builder);
                }
                return builder.build();
            }
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            read(docId, (LongBuilder) builder);
        }

        private void read(int doc, LongBuilder builder) throws IOException {
            if (false == numericDocValues.advanceExact(doc)) {
                builder.appendNull();
                return;
            }
            int count = numericDocValues.docValueCount();
            if (count == 1) {
                builder.appendLong(numericDocValues.nextValue());
                return;
            }
            builder.beginPositionEntry();
            for (int v = 0; v < count; v++) {
                builder.appendLong(numericDocValues.nextValue());
            }
            builder.endPositionEntry();
        }

        @Override
        public int docId() {
            return numericDocValues.docID();
        }

        @Override
        public String toString() {
            return "LongsFromDocValues.Sorted";
        }
    }
}
