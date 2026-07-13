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

public class DoublesBlockLoader extends AbstractNumericBlockLoader {
    protected final BlockDocValuesReader.ToDouble toDouble;

    public DoublesBlockLoader(String fieldName, BlockDocValuesReader.ToDouble toDouble) {
        this(fieldName, toDouble, false);
    }

    public DoublesBlockLoader(String fieldName, BlockDocValuesReader.ToDouble toDouble, boolean readInArrayOrder) {
        super(fieldName, "DoublesFromDocValues", readInArrayOrder);
        this.toDouble = toDouble;
    }

    @Override
    public DoubleBuilder builder(BlockFactory factory, int expectedCount) {
        return factory.doubles(expectedCount);
    }

    @Override
    protected ColumnAtATimeReader singletonReader(TrackingNumericDocValues docValues) {
        return new Singleton(readerName, docValues, toDouble);
    }

    @Override
    protected ColumnAtATimeReader sortedReader(TrackingSortedNumericDocValues docValues) {
        return new Sorted(readerName, docValues, toDouble);
    }

    @Override
    protected ColumnAtATimeReader arrayOrderReader(TrackingSortedNumericDocValues values, TrackingSortedDocValues offsets) {
        return new ArrayOrder(readerName, values, offsets, toDouble);
    }

    static final class Singleton extends AbstractNumericBlockLoader.Singleton {
        private final BlockDocValuesReader.ToDouble toDouble;

        Singleton(String name, TrackingNumericDocValues numericDocValues, BlockDocValuesReader.ToDouble toDouble) {
            super(name, numericDocValues);
            this.toDouble = toDouble;
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            NumericDocValues docValues = numericDocValues.docValues();
            // Attempt a fast path through OptionalColumnAtATimeReader
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
    }

    static final class Sorted extends AbstractNumericBlockLoader.Sorted {
        private final BlockDocValuesReader.ToDouble toDouble;

        Sorted(String name, TrackingSortedNumericDocValues values, BlockDocValuesReader.ToDouble toDouble) {
            super(name, values);
            this.toDouble = toDouble;
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            try (DoubleBuilder builder = factory.doublesFromDocValues(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    readSortedDoc(docs.get(i), builder);
                }
                return builder.build();
            }
        }

        void readSortedDoc(int doc, DoubleBuilder builder) throws IOException {
            SortedNumericDocValues docValues = values.docValues();
            if (docValues.advanceExact(doc) == false) {
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
    }

    static final class ArrayOrder extends AbstractNumericBlockLoader.ArrayOrder<DoubleBuilder> {
        private final BlockDocValuesReader.ToDouble toDouble;
        private final Sorted sortedFallback;

        ArrayOrder(
            String name,
            TrackingSortedNumericDocValues values,
            TrackingSortedDocValues offsets,
            BlockDocValuesReader.ToDouble toDouble
        ) {
            super(name, values, offsets);
            this.toDouble = toDouble;
            this.sortedFallback = new Sorted(name, values, toDouble);
        }

        @Override
        protected DoubleBuilder newBuilder(BlockFactory factory, int count) {
            return factory.doublesFromDocValues(count);
        }

        @Override
        protected void readSortedFallback(int docId, DoubleBuilder builder) throws IOException {
            sortedFallback.readSortedDoc(docId, builder);
        }

        @Override
        protected void emit(int[] offsetToOrd, DoubleBuilder builder, long[] materialized) {
            int nonNullCount = OffsetsAwareBlockLoaderHelper.countNonNull(offsetToOrd);
            if (nonNullCount == 0) {
                builder.appendNull();
                return;
            }
            if (nonNullCount == 1) {
                for (int ord : offsetToOrd) {
                    if (ord != FieldArrayContext.NULL_ORD) {
                        builder.appendDouble(toDouble.convert(materialized[ord]));
                        return;
                    }
                }
            }
            builder.beginPositionEntry();
            for (int ord : offsetToOrd) {
                if (ord != FieldArrayContext.NULL_ORD) {
                    builder.appendDouble(toDouble.convert(materialized[ord]));
                }
            }
            builder.endPositionEntry();
        }
    }
}
