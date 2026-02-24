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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.SortedDvSingletonOrSet;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingSortedDocValues;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingSortedSetDocValues;

import java.io.IOException;

/**
 * Loads {@code keyword} style fields that are stored as a lookup table.
 */
public abstract class AbstractBytesRefsFromOrdsBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {
    protected final String fieldName;
    private final ByteSizeValue size;

    public AbstractBytesRefsFromOrdsBlockLoader(String fieldName, ByteSizeValue size) {
        this.fieldName = fieldName;
        this.size = size;
    }

    @Override
    public final BytesRefBuilder builder(BlockFactory factory, int expectedCount) {
        return factory.bytesRefs(expectedCount);
    }

    @Override
    public final AllReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        SortedDvSingletonOrSet dv = SortedDvSingletonOrSet.get(breaker, size, context, fieldName);
        if (dv == null) {
            return ConstantNull.READER;
        }
        if (dv.singleton() != null) {
            return singletonReader(dv.singleton());
        }
        return sortedSetReader(dv.set());
    }

    protected abstract AllReader singletonReader(TrackingSortedDocValues docValues);

    protected abstract AllReader sortedSetReader(TrackingSortedSetDocValues docValues);

    protected class Singleton extends BlockDocValuesReader {
        private final TrackingSortedDocValues ordinals;

        public Singleton(TrackingSortedDocValues ordinals) {
            super(null); // TODO remove this entirely
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
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            if (ordinals.docValues().advanceExact(docId)) {
                ((BytesRefBuilder) builder).appendBytesRef(ordinals.docValues().lookupOrd(ordinals.docValues().ordValue()));
            } else {
                builder.appendNull();
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

    protected class SortedSet extends BlockDocValuesReader {
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
                    if (ordinals.docValues().advanceExact(doc) == false) {
                        builder.appendNull();
                        continue;
                    }
                    int count = ordinals.docValues().docValueCount();
                    if (count == 1) {
                        builder.appendOrd(Math.toIntExact(ordinals.docValues().nextOrd()));
                    } else {
                        builder.beginPositionEntry();
                        for (int c = 0; c < count; c++) {
                            builder.appendOrd(Math.toIntExact(ordinals.docValues().nextOrd()));
                        }
                        builder.endPositionEntry();
                    }
                }
                return builder.build();
            }
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            read(docId, (BytesRefBuilder) builder);
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

        private void read(int docId, BytesRefBuilder builder) throws IOException {
            if (false == ordinals.docValues().advanceExact(docId)) {
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
}
