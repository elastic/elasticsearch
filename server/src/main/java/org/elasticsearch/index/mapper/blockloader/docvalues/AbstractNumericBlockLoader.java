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
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.core.Releasables;
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
 * {@link #reader} picks one of three reader shapes for the segment. Each shape is supplied by the concrete loader through an abstract
 * hook so that the per-document loop lives in a type-specific {@code read} method that the JIT compiles monomorphically; a single shared
 * loop would see every numeric type and go megamorphic.
 * <ul>
 *     <li>{@link Singleton} — single-valued numeric doc values/li>
 *     <li>{@link Sorted} — multi-valued in natural (sorted) order</li>
 *     <li>{@link ArrayOrder} — multi-valued in index-time arrival order via a companion offsets field</li>
 * </ul>
 */
public abstract class AbstractNumericBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {

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
            TrackingSortedDocValues offsets;
            try {
                offsets = TrackingSortedDocValues.get(breaker, context, FieldArrayContext.offsetsFieldName(fieldName));
            } catch (Exception e) {
                // We already reserved breaker space for the doc values above. If acquiring the offsets companion fails (ex. circuit
                // breaker) we must release that reservation here, otherwise it leaks.
                Releasables.close(dv.sorted());
                throw e;
            }
            if (offsets != null) {
                return arrayOrderReader(dv.sorted(), offsets);
            }
        }

        // Doc values are single-valued
        if (dv.singleton() != null) {
            return singletonReader(dv.singleton());
        }

        // Otherwise, doc values are multi-valued but without offsets
        return sortedReader(dv.sorted());
    }

    protected abstract ColumnAtATimeReader singletonReader(TrackingNumericDocValues docValues);

    protected abstract ColumnAtATimeReader sortedReader(TrackingSortedNumericDocValues docValues);

    protected abstract ColumnAtATimeReader arrayOrderReader(TrackingSortedNumericDocValues values, TrackingSortedDocValues offsets);

    @Override
    public String toString() {
        return readerName + "[" + fieldName + "]";
    }

    /**
     * Each subclass overrides {@code read} in full: the per-doc append is the hot path, so a shared loop would see every type and go
     * megamorphic. Unlike {@link ArrayOrder} there is no type-independent work to hoist into a base {@code read}.
     */
    public abstract static class Singleton extends BlockDocValuesReader implements BlockDocValuesReader.NumericDocValuesAccessor {

        private final String name;
        protected final TrackingNumericDocValues numericDocValues;

        protected Singleton(String name, TrackingNumericDocValues numericDocValues) {
            super(null);
            this.name = name + ".Singleton";
            this.numericDocValues = numericDocValues;
        }

        @Override
        public final int docId() {
            return numericDocValues.docValues().docID();
        }

        @Override
        public final NumericDocValues numericDocValues() {
            return numericDocValues.docValues();
        }

        @Override
        public final String toString() {
            return name;
        }

        @Override
        public void close() {
            numericDocValues.close();
        }
    }

    /**
     * Each subclass overrides {@code read} in full rather than sharing a base loop: the {@code MvMin}/{@code MvMax} reducers extend this
     * with their own {@code read}, and like {@link Singleton} there is little type-independent work to hoist.
     */
    public abstract static class Sorted extends BlockDocValuesReader {

        private final String name;
        protected final TrackingSortedNumericDocValues values;

        protected Sorted(String name, TrackingSortedNumericDocValues values) {
            super(null);
            this.name = name + ".Sorted";
            this.values = values;
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

    abstract static class ArrayOrder<B extends Builder> extends BlockDocValuesReader {

        private final String name;
        protected final TrackingSortedNumericDocValues values;
        protected final TrackingSortedDocValues offsets;
        private final ByteArrayStreamInput scratch = new ByteArrayStreamInput();

        protected ArrayOrder(String name, TrackingSortedNumericDocValues values, TrackingSortedDocValues offsets) {
            super(null);
            this.name = name + ".ArrayOrder";
            this.values = values;
            this.offsets = offsets;
        }

        /**
         * The shared {@code read} loop, the offsets decode, the all-null handling, and the per-doc value materialization live here; the
         * per-type subclass supplies only the builder, the sorted fallback, and the {@link #emit} loop so the value-append stays in
         * type-specific code and compiles monomorphically rather than going through a megamorphic callback.
         */
        @Override
        public final Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            try (B builder = newBuilder(factory, docs.count() - offset)) {
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
                readSortedFallback(docId, builder);
                return;
            }

            SortedNumericDocValues docValues = values.docValues();
            // no values arrived (all slots null) — emit a single null position
            if (docValues.advanceExact(docId) == false) {
                assert OffsetsAwareBlockLoaderHelper.allNulls(offsetToOrd);
                builder.appendNull();
                return;
            }

            // Offsets are encoded against a unique collection of entries, but for numerics, the underlying doc values
            // (SortedNumericDocValuesField) does not dedupe, it just sorts. Hence, we must dedupe ourselves.
            int count = values.docValues().docValueCount();
            long[] materialized = new long[count];
            int duplicates = 0;
            for (int i = 0; i < count; i++) {
                long value = values.docValues().nextValue();
                // since the values are in sorted order, to detect a duplicate compare the current value against the previous one
                if (i > 0 && value == materialized[i - duplicates - 1]) {
                    duplicates++;
                    continue;
                }
                materialized[i - duplicates] = value;
            }

            emit(offsetToOrd, builder, materialized);
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
            // sortedFallback wraps `values` and owns no other resources, so closing it would double-release `values`.
            Releasables.close(values, offsets);
        }

        /**
         * Creates the type-specific builder sized for the block.
         */
        protected abstract B newBuilder(BlockFactory factory, int count);

        /**
         * Emits the doc in natural (sorted) order when no offsets were recorded for it, delegating to the type's sorted reader.
         */
        protected abstract void readSortedFallback(int docId, B builder) throws IOException;

        /**
         * Appends the doc's values in arrival order, indexing into {@code materialized} by ord. Kept in the subclass so the append is
         * monomorphic; use {@link OffsetsAwareBlockLoaderHelper#countNonNull} to pick the position shape before appending.
         */
        protected abstract void emit(int[] offsetToOrd, B builder, long[] materialized) throws IOException;
    }
}
