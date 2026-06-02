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

public class BooleansBlockLoader extends AbstractNumericBlockLoader {

    public BooleansBlockLoader(String fieldName) {
        this(fieldName, false);
    }

    public BooleansBlockLoader(String fieldName, boolean readInArrayOrder) {
        super(fieldName, "BooleansFromDocValues", readInArrayOrder);
    }

    @Override
    public BooleanBuilder builder(BlockFactory factory, int expectedCount) {
        return factory.booleans(expectedCount);
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
            try (BooleanBuilder builder = factory.booleansFromDocValues(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (docValues.advanceExact(doc)) {
                        builder.appendBoolean(docValues.longValue() != 0);
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
            try (BooleanBuilder builder = factory.booleansFromDocValues(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    readSortedDoc(docs.get(i), builder);
                }
                return builder.build();
            }
        }

        void readSortedDoc(int doc, BooleanBuilder builder) throws IOException {
            SortedNumericDocValues docValues = values.docValues();
            if (docValues.advanceExact(doc) == false) {
                builder.appendNull();
                return;
            }
            int count = docValues.docValueCount();
            if (count == 1) {
                builder.appendBoolean(docValues.nextValue() != 0);
                return;
            }
            builder.beginPositionEntry();
            for (int v = 0; v < count; v++) {
                builder.appendBoolean(docValues.nextValue() != 0);
            }
            builder.endPositionEntry();
        }
    }

    static final class ArrayOrder extends AbstractNumericBlockLoader.ArrayOrder<BooleanBuilder> {
        private final Sorted sortedFallback;

        ArrayOrder(String name, TrackingSortedNumericDocValues values, TrackingSortedDocValues offsets) {
            super(name, values, offsets);
            this.sortedFallback = new Sorted(name, values);
        }

        @Override
        protected BooleanBuilder newBuilder(BlockFactory factory, int count) {
            return factory.booleansFromDocValues(count);
        }

        @Override
        protected void readSortedFallback(int docId, BooleanBuilder builder) throws IOException {
            sortedFallback.readSortedDoc(docId, builder);
        }

        @Override
        protected void emit(int[] offsetToOrd, BooleanBuilder builder, long[] materialized) {
            int nonNullCount = OffsetsAwareBlockLoaderHelper.countNonNull(offsetToOrd);
            if (nonNullCount == 0) {
                builder.appendNull();
                return;
            }
            if (nonNullCount == 1) {
                for (int ord : offsetToOrd) {
                    if (ord != FieldArrayContext.NULL_ORD) {
                        builder.appendBoolean(materialized[ord] != 0);
                        return;
                    }
                }
            }
            builder.beginPositionEntry();
            for (int ord : offsetToOrd) {
                if (ord != FieldArrayContext.NULL_ORD) {
                    builder.appendBoolean(materialized[ord] != 0);
                }
            }
            builder.endPositionEntry();
        }
    }
}
