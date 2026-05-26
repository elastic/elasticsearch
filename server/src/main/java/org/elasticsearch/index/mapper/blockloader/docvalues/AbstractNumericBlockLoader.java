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
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.FieldArrayContext;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.NumericDvSingletonOrSorted;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingNumericDocValues;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingSortedDocValues;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingSortedNumericDocValues;

import java.io.IOException;

/**
 * Shared base for the numeric-family block loaders.
 * <p>
 * Exposes three nested reader shapes produced by {@link #reader}:
 * <ul>
 *     <li>{@link Singleton} — single-valued numeric doc values; can use the {@code tryDirectRead} fast path</li>
 *     <li>{@link Sorted} — multi-valued in natural (sorted) order</li>
 *     <li>{@link ArrayOrder} — multi-valued in index-time arrival order via a companion offsets field</li>
 * </ul>
 */
public abstract class AbstractNumericBlockLoader<B extends BlockLoader.Builder> extends BlockDocValuesReader.DocValuesBlockLoader {

    protected final String fieldName;
    protected final String readerName;
    private final boolean readInArrayOrder;   // whether to emit the values in arrival order at index time

    protected AbstractNumericBlockLoader(String fieldName, String readerName) {
        this(fieldName, readerName, false);
    }

    protected AbstractNumericBlockLoader(String fieldName, String readerName, boolean readInArrayOrder) {
        this.fieldName = fieldName;
        this.readerName = readerName;
        this.readInArrayOrder = readInArrayOrder;
    }

    @Override
    public final ColumnAtATimeReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        NumericDvSingletonOrSorted dv = NumericDvSingletonOrSorted.get(breaker, context, fieldName);
        if (dv == null) {
            return ConstantNull.COLUMN_READER;
        }

        // If the value is singleton, then we can skip reading offsets altogether
        if (readInArrayOrder && dv.singleton() == null) {
            TrackingSortedDocValues offsets = TrackingSortedDocValues.get(breaker, context, FieldArrayContext.offsetsFieldName(fieldName));
            if (offsets != null) {
                return new ArrayOrder<>(this, readerName, dv.sorted(), offsets);
            }
        }

        // Doc values are single-valued
        if (dv.singleton() != null) {
            return new Singleton<>(this, readerName, dv.singleton());
        }

        // Otherwise, doc values are multi-valued but without offsets
        return sortedReader(dv.sorted());
    }

    /**
     * Builds the reader for multi-valued, sorted-order doc values. The default returns a vanilla {@link Sorted}; multi-value reduction
     * loaders (Mv max/min) override this hook to inject a custom {@link Sorted} subclass with reduction logic.
     */
    protected ColumnAtATimeReader sortedReader(TrackingSortedNumericDocValues docValues) {
        return new Sorted<>(this, readerName, docValues);
    }

    /** Allocates the typed reader-side builder. */
    protected abstract B newBuilder(BlockFactory factory, int expectedCount);

    /** Appends a single non-null value, converting from the raw {@code long} bits stored in the doc values. */
    protected abstract void appendValue(B builder, long rawValue);

    /**
     * Optional fast path through {@link BlockLoader.OptionalColumnAtATimeReader#tryRead} for single-valued reads. Default returns
     * {@code null} to skip the fast path.
     */
    protected BlockLoader.Block tryDirectRead(
        BlockLoader.OptionalColumnAtATimeReader direct,
        BlockFactory factory,
        Docs docs,
        int offset,
        boolean nullsFiltered
    ) throws IOException {
        return null;
    }

    public static final class Singleton<B extends BlockLoader.Builder> extends BlockDocValuesReader
        implements
            BlockDocValuesReader.NumericDocValuesAccessor {

        private final AbstractNumericBlockLoader<B> loader;
        private final String name;
        private final TrackingNumericDocValues numericDocValues;

        Singleton(AbstractNumericBlockLoader<B> loader, String name, TrackingNumericDocValues numericDocValues) {
            super(null);
            this.loader = loader;
            this.name = name + ".Singleton";
            this.numericDocValues = numericDocValues;
        }

        @Override
        public BlockLoader.Block read(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset, boolean nullsFiltered)
            throws IOException {
            // Attempt a fast path through OptionalColumnAtATimeReader
            if (numericDocValues.docValues() instanceof BlockLoader.OptionalColumnAtATimeReader direct) {
                BlockLoader.Block result = loader.tryDirectRead(direct, factory, docs, offset, nullsFiltered);
                if (result != null) {
                    return result;
                }
            }

            try (B builder = loader.newBuilder(factory, docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (numericDocValues.docValues().advanceExact(doc)) {
                        loader.appendValue(builder, numericDocValues.docValues().longValue());
                    } else {
                        builder.appendNull();
                    }
                }
                return builder.build();
            }
        }

        @Override
        public int docId() {
            return numericDocValues.docValues().docID();
        }

        @Override
        public NumericDocValues numericDocValues() {
            return numericDocValues.docValues();
        }

        @Override
        public String toString() {
            return name;
        }

        @Override
        public void close() {
            numericDocValues.close();
        }
    }

    public static class Sorted<B extends BlockLoader.Builder> extends BlockDocValuesReader {

        private final AbstractNumericBlockLoader<B> loader;
        private final String name;
        protected final TrackingSortedNumericDocValues values;

        protected Sorted(AbstractNumericBlockLoader<B> loader, String name, TrackingSortedNumericDocValues values) {
            super(null);
            this.loader = loader;
            this.name = name + ".Sorted";
            this.values = values;
        }

        @Override
        public BlockLoader.Block read(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset, boolean nullsFiltered)
            throws IOException {
            try (B builder = loader.newBuilder(factory, docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    readSortedDoc(docs.get(i), builder);
                }
                return builder.build();
            }
        }

        protected void readSortedDoc(int doc, B builder) throws IOException {
            if (values.docValues().advanceExact(doc) == false) {
                builder.appendNull();
                return;
            }
            int count = values.docValues().docValueCount();
            if (count == 1) {
                loader.appendValue(builder, values.docValues().nextValue());
                return;
            }
            builder.beginPositionEntry();
            for (int v = 0; v < count; v++) {
                loader.appendValue(builder, values.docValues().nextValue());
            }
            builder.endPositionEntry();
        }

        /** Convenience pass-through used by Mv-fused subclasses that override {@link #readSortedDoc}. */
        protected final void appendValue(B builder, long rawValue) {
            loader.appendValue(builder, rawValue);
        }

        @Override
        public final int docId() {
            return values.docValues().docID();
        }

        @Override
        public final String toString() {
            return name;
        }

        @Override
        public void close() {
            values.close();
        }
    }

    static final class ArrayOrder<B extends BlockLoader.Builder> extends BlockDocValuesReader {

        private final AbstractNumericBlockLoader<B> loader;
        private final String name;
        private final TrackingSortedNumericDocValues values;
        private final TrackingSortedDocValues offsets;
        private final Sorted<B> sortedFallback;

        private final ByteArrayStreamInput scratch = new ByteArrayStreamInput();

        ArrayOrder(
            AbstractNumericBlockLoader<B> loader,
            String name,
            TrackingSortedNumericDocValues values,
            TrackingSortedDocValues offsets
        ) {
            super(null);
            this.loader = loader;
            this.name = name + ".ArrayOrder";
            this.values = values;
            this.offsets = offsets;
            this.sortedFallback = new Sorted<>(loader, name, values);
        }

        @Override
        public BlockLoader.Block read(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset, boolean nullsFiltered)
            throws IOException {
            try (B builder = loader.newBuilder(factory, docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    readDoc(docs.get(i), builder);
                }
                return builder.build();
            }
        }

        private void readDoc(int docId, B builder) throws IOException {
            int[] offsetToOrd = OffsetsAwareBlockLoaderHelper.readOffsets(offsets.docValues(), scratch, docId);

            // no offsets were recorded - fallback to the sorted path
            if (offsetToOrd == null) {
                sortedFallback.readSortedDoc(docId, builder);
                return;
            }

            // no values arrived (all slots null) — emit a single null position
            if (values.docValues().advanceExact(docId) == false) {
                assert OffsetsAwareBlockLoaderHelper.allNulls(offsetToOrd);
                builder.appendNull();
                return;
            }

            // materialize the per-doc values once so we can index into them by ord
            int count = values.docValues().docValueCount();
            long[] materialized = new long[count];
            for (int i = 0; i < count; i++) {
                materialized[i] = values.docValues().nextValue();
            }

            OffsetsAwareBlockLoaderHelper.emit(offsetToOrd, builder, ord -> loader.appendValue(builder, materialized[ord]));
        }

        @Override
        public int docId() {
            // it doesn't matter whose docId we grab, as long as we advanced everything in unison and we do
            return offsets.docValues().docID();
        }

        @Override
        public String toString() {
            return name;
        }

        @Override
        public void close() {
            Releasables.close(values, offsets, sortedFallback);
        }
    }
}
