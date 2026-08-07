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
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.FieldArrayContext;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.SortedDvSingletonOrSet;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingSortedDocValues;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingSortedSetDocValues;

import java.io.IOException;

/**
 * Loads {@code keyword}-style fields stored as a sorted ordinal lookup table.
 * <p>
 * Exposes three nested reader shapes:
 * <ul>
 *     <li>{@link Singleton} — single-valued sorted-set doc values</li>
 *     <li>{@link SortedSet} — multi-valued in natural (sorted) order</li>
 *     <li>{@link ArrayOrder} — multi-valued in index-time arrival order via a companion offsets field</li>
 * </ul>
 */
public class BytesRefsFromOrdsBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {

    protected final String fieldName;
    protected final ByteSizeValue size;
    private final boolean readInArrayOrder;   // whether to emit the values in arrival order at index time

    public BytesRefsFromOrdsBlockLoader(String fieldName, ByteSizeValue size) {
        this(fieldName, size, false);
    }

    public BytesRefsFromOrdsBlockLoader(String fieldName, ByteSizeValue size, boolean readInArrayOrder) {
        this.fieldName = fieldName;
        this.size = size;
        this.readInArrayOrder = readInArrayOrder;
    }

    @Override
    public BytesRefBuilder builder(BlockFactory factory, int expectedCount) {
        return factory.bytesRefs(expectedCount);
    }

    @Override
    public ColumnAtATimeReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        SortedDvSingletonOrSet dv = SortedDvSingletonOrSet.get(breaker, size, context, fieldName);
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
                Releasables.close(dv.set());
                throw e;
            }
            if (offsets != null) {
                return new ArrayOrder(dv.set(), offsets);
            }
        }

        // Doc values are single-valued
        if (dv.singleton() != null) {
            return new Singleton(dv.singleton());
        }

        // Otherwise, doc values are multi-valued but without offsets
        return new SortedSet(dv.set());
    }

    @Override
    public boolean supportsOrdinals() {
        return true;
    }

    @Override
    public SortedSetDocValues ordinals(LeafReaderContext context) throws IOException {
        return DocValues.getSortedSet(context.reader(), fieldName);
    }

    @Override
    public String toString() {
        return "BytesRefsFromOrds[" + fieldName + "]";
    }

    @Override
    public RowStrideReader rowStrideReader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        SortedDvSingletonOrSet dv = SortedDvSingletonOrSet.get(breaker, size, context, fieldName);
        if (dv == null) {
            return ConstantNull.ROW_READER;
        }
        return new RowStride(dv.forceSet(), Thread.currentThread());
    }

    private record RowStride(TrackingSortedSetDocValues ordinals, Thread creationThread) implements RowStrideReader {
        @Override
        public void read(int docId, StoredFields storedFields, Builder b) throws IOException {
            BytesRefBuilder builder = (BytesRefBuilder) b;
            if (ordinals.docValues().advanceExact(docId) == false) {
                builder.appendNull();
                return;
            }
            int count = ordinals.docValues().docValueCount();
            if (count == 1) {
                builder.appendBytesRef(ordinals.docValues().lookupOrd(ordinals.docValues().nextOrd()));
                return;
            }
            builder.beginPositionEntry();
            for (int v = 0; v < count; v++) {
                builder.appendBytesRef(ordinals.docValues().lookupOrd(ordinals.docValues().nextOrd()));
            }
            builder.endPositionEntry();
        }

        @Override
        public boolean canReuse(int startingDocID) {
            return creationThread == Thread.currentThread() && ordinals.docValues().docID() <= startingDocID;
        }

        @Override
        public void close() {
            ordinals.close();
        }

        @Override
        public String toString() {
            return "BytesRefsFromOrds.RowStride";
        }
    }

    public static class Singleton extends BlockDocValuesReader {
        private final TrackingSortedDocValues ordinals;

        public Singleton(TrackingSortedDocValues ordinals) {
            super(null);
            this.ordinals = ordinals;
        }

        private Block readSingleDoc(BlockFactory factory, int docId) throws IOException {
            if (ordinals.docValues().advanceExact(docId)) {
                BytesRef v = ordinals.docValues().lookupOrd(ordinals.docValues().ordValue());
                // the returned BytesRef can be reused
                return factory.constantBytes(BytesRef.deepCopyOf(v), 1);
            } else {
                return factory.constantNulls(1);
            }
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            if (docs.count() - offset == 1) {
                return readSingleDoc(factory, docs.get(offset));
            }
            if (ordinals.docValues() instanceof OptionalColumnAtATimeReader direct) {
                Block block = direct.tryRead(factory, docs, offset, nullsFiltered, null, false, false);
                if (block != null) {
                    return block;
                }
            }
            try (var builder = factory.singletonOrdinalsBuilder(ordinals.docValues(), docs.count() - offset, false)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (ordinals.docValues().advanceExact(doc)) {
                        builder.appendOrd(ordinals.docValues().ordValue());
                    } else {
                        builder.appendNull();
                    }
                }
                return builder.build();
            }
        }

        @Override
        public int docId() {
            return ordinals.docValues().docID();
        }

        @Override
        public String toString() {
            return "BytesRefsFromOrds.Singleton";
        }

        @Override
        public void close() {
            ordinals.close();
        }
    }

    static class SortedSet extends BlockDocValuesReader {

        private final TrackingSortedSetDocValues ordinals;

        SortedSet(TrackingSortedSetDocValues ordinals) {
            super(null);
            this.ordinals = ordinals;
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            if (docs.count() - offset == 1) {
                return readSingleDoc(factory, docs.get(offset));
            }
            try (var builder = factory.sortedSetOrdinalsBuilder(ordinals.docValues(), docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (doc < ordinals.docValues().docID()) {
                        throw new IllegalStateException("docs within same block must be in order");
                    }
                    readSortedSetDoc(doc, builder);
                }
                return builder.build();
            }
        }

        /** Emits one doc using the natural sorted-set order (null when no value, lone value flat, multi-value as a position entry). */
        private void readSortedSetDoc(int doc, SortedSetOrdinalsBuilder builder) throws IOException {
            if (ordinals.docValues().advanceExact(doc) == false) {
                builder.appendNull();
                return;
            }
            int count = ordinals.docValues().docValueCount();
            if (count == 1) {
                builder.appendOrd(Math.toIntExact(ordinals.docValues().nextOrd()));
                return;
            }
            builder.beginPositionEntry();
            for (int v = 0; v < count; v++) {
                builder.appendOrd(Math.toIntExact(ordinals.docValues().nextOrd()));
            }
            builder.endPositionEntry();
        }

        private Block readSingleDoc(BlockFactory factory, int docId) throws IOException {
            if (ordinals.docValues().advanceExact(docId) == false) {
                return factory.constantNulls(1);
            }
            int count = ordinals.docValues().docValueCount();
            if (count == 1) {
                BytesRef v = ordinals.docValues().lookupOrd(ordinals.docValues().nextOrd());
                return factory.constantBytes(BytesRef.deepCopyOf(v), 1);
            }
            try (var builder = factory.bytesRefsFromDocValues(count)) {
                builder.beginPositionEntry();
                for (int c = 0; c < count; c++) {
                    BytesRef v = ordinals.docValues().lookupOrd(ordinals.docValues().nextOrd());
                    builder.appendBytesRef(v);
                }
                builder.endPositionEntry();
                return builder.build();
            }
        }

        @Override
        public int docId() {
            return ordinals.docValues().docID();
        }

        @Override
        public String toString() {
            return "BytesRefsFromOrds.SortedSet";
        }

        @Override
        public void close() {
            ordinals.close();
        }
    }

    static class ArrayOrder extends BlockDocValuesReader {

        private final SortedSet sortedSetFallback;
        private final TrackingSortedDocValues offsets;
        private final TrackingSortedSetDocValues ordinals;
        private final ByteArrayStreamInput scratch = new ByteArrayStreamInput();

        ArrayOrder(TrackingSortedSetDocValues ordinals, TrackingSortedDocValues offsets) {
            super(null);
            this.ordinals = ordinals;
            this.offsets = offsets;
            this.sortedSetFallback = new SortedSet(ordinals);
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            try (var builder = factory.arrayOrderOrdinalsBuilder(ordinals.docValues(), docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    readDoc(docs.get(i), builder);
                }
                return builder.build();
            }
        }

        private void readDoc(int docId, SortedSetOrdinalsBuilder builder) throws IOException {
            int[] offsetToOrd = OffsetsAwareBlockLoaderHelper.readOffsets(offsets.docValues(), scratch, docId);
            if (offsetToOrd == null) {
                sortedSetFallback.readSortedSetDoc(docId, builder);
                return;
            }
            if (ordinals.docValues().advanceExact(docId) == false) {
                assert OffsetsAwareBlockLoaderHelper.allNulls(offsetToOrd);
                builder.appendNull();
                return;
            }
            int count = ordinals.docValues().docValueCount();
            long[] ords = new long[count];
            for (int i = 0; i < count; i++) {
                ords[i] = ordinals.docValues().nextOrd();
            }
            OffsetsAwareBlockLoaderHelper.emit(offsetToOrd, builder, ord -> builder.appendOrd(Math.toIntExact(ords[ord])));
        }

        @Override
        protected int docId() {
            return ordinals.docValues().docID();
        }

        @Override
        public String toString() {
            return "BytesRefsFromOrds.ArrayOrder";
        }

        @Override
        public void close() {
            Releasables.close(ordinals, offsets);
        }
    }
}
