/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.index.mapper.FieldArrayContext;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingNumericDocValues;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingSortedDocValues;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingSortedNumericDocValues;

import java.io.IOException;

/**
 * Loads {@code int}s from doc values.
 */
public class IntsBlockLoader extends AbstractNumericBlockLoader {
    public IntsBlockLoader(String fieldName) {
        this(fieldName, false);
    }

    public IntsBlockLoader(String fieldName, boolean readInArrayOrder) {
        super(fieldName, "IntsFromDocValues", readInArrayOrder);
    }

    @Override
    public IntBuilder builder(BlockFactory factory, int expectedCount) {
        return factory.ints(expectedCount);
    }

    @Override
    protected ColumnAtATimeReader singletonReader(TrackingNumericDocValues docValues) {
        return new Singleton(readerName, docValues);
    }

    @Override
    protected ColumnAtATimeReader sortedReader(TrackingSortedNumericDocValues docValues) {
        return new Sorted(readerName, docValues);
    }

    @Override
    protected ColumnAtATimeReader arrayOrderReader(TrackingSortedNumericDocValues values, TrackingSortedDocValues offsets) {
        return new ArrayOrder(readerName, values, offsets);
    }

    static final class Singleton extends AbstractNumericBlockLoader.Singleton {
        Singleton(String name, TrackingNumericDocValues numericDocValues) {
            super(name, numericDocValues);
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            NumericDocValues docValues = numericDocValues.docValues();
            // Attempt a fast path through OptionalColumnAtATimeReader
            if (docValues instanceof OptionalColumnAtATimeReader direct) {
                Block result = direct.tryRead(factory, docs, offset, nullsFiltered, null, true, false);
                if (result != null) {
                    return result;
                }
            }
            try (IntBuilder builder = factory.intsFromDocValues(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (docValues.advanceExact(doc)) {
                        builder.appendInt(Math.toIntExact(docValues.longValue()));
                    } else {
                        builder.appendNull();
                    }
                }
                return builder.build();
            }
        }
    }

    static final class Sorted extends AbstractNumericBlockLoader.Sorted {
        Sorted(String name, TrackingSortedNumericDocValues values) {
            super(name, values);
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            try (IntBuilder builder = factory.intsFromDocValues(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    readSortedDoc(docs.get(i), builder);
                }
                return builder.build();
            }
        }

        void readSortedDoc(int doc, IntBuilder builder) throws IOException {
            SortedNumericDocValues docValues = values.docValues();
            if (docValues.advanceExact(doc) == false) {
                builder.appendNull();
                return;
            }
            int count = docValues.docValueCount();
            if (count == 1) {
                builder.appendInt(Math.toIntExact(docValues.nextValue()));
                return;
            }
            builder.beginPositionEntry();
            for (int v = 0; v < count; v++) {
                builder.appendInt(Math.toIntExact(docValues.nextValue()));
            }
            builder.endPositionEntry();
        }
    }

    static final class ArrayOrder extends AbstractNumericBlockLoader.ArrayOrder<IntBuilder> {
        private final Sorted sortedFallback;

        ArrayOrder(String name, TrackingSortedNumericDocValues values, TrackingSortedDocValues offsets) {
            super(name, values, offsets);
            this.sortedFallback = new Sorted(name, values);
        }

        @Override
        protected IntBuilder newBuilder(BlockFactory factory, int count) {
            return factory.intsFromDocValues(count);
        }

        @Override
        protected void readSortedFallback(int docId, IntBuilder builder) throws IOException {
            sortedFallback.readSortedDoc(docId, builder);
        }

        @Override
        protected void emit(int[] offsetToOrd, IntBuilder builder, long[] materialized) {
            int nonNullCount = OffsetsAwareBlockLoaderHelper.countNonNull(offsetToOrd);
            if (nonNullCount == 0) {
                builder.appendNull();
                return;
            }
            if (nonNullCount == 1) {
                for (int ord : offsetToOrd) {
                    if (ord != FieldArrayContext.NULL_ORD) {
                        builder.appendInt(Math.toIntExact(materialized[ord]));
                        return;
                    }
                }
            }
            builder.beginPositionEntry();
            for (int ord : offsetToOrd) {
                if (ord != FieldArrayContext.NULL_ORD) {
                    builder.appendInt(Math.toIntExact(materialized[ord]));
                }
            }
            builder.endPositionEntry();
        }
    }
}
