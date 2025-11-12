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
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * Loads {@code keyword} style fields that are stored as a lookup table.
 */
public abstract class AbstractBytesRefsFromOrdsBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {
    protected final String fieldName;

    public AbstractBytesRefsFromOrdsBlockLoader(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public final BytesRefBuilder builder(BlockFactory factory, int expectedCount) {
        return factory.bytesRefs(expectedCount);
    }

    @Override
    public final AllReader reader(LeafReaderContext context) throws IOException {
        SortedSetDocValues docValues = context.reader().getSortedSetDocValues(fieldName);
        if (docValues != null) {
            SortedDocValues singleton = DocValues.unwrapSingleton(docValues);
            if (singleton != null) {
                return singletonReader(singleton);
            }
            return sortedSetReader(docValues);
        }
        SortedDocValues singleton = context.reader().getSortedDocValues(fieldName);
        if (singleton != null) {
            return singletonReader(singleton);
        }
        return new ConstantNullsReader();
    }

    protected abstract AllReader singletonReader(SortedDocValues docValues);

    protected abstract AllReader sortedSetReader(SortedSetDocValues docValues);

    protected static class Singleton extends BlockDocValuesReader {
        private final SortedDocValues ordinals;

        public Singleton(SortedDocValues ordinals) {
            this.ordinals = ordinals;
        }

        private Block readSingleDoc(BlockFactory factory, int docId) throws IOException {
            if (ordinals.advanceExact(docId)) {
                BytesRef v = ordinals.lookupOrd(ordinals.ordValue());
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
            if (ordinals instanceof OptionalColumnAtATimeReader direct) {
                Block block = direct.tryRead(factory, docs, offset, nullsFiltered, null, false);
                if (block != null) {
                    return block;
                }
            }
            try (var builder = factory.singletonOrdinalsBuilder(ordinals, docs.count() - offset, false)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (ordinals.advanceExact(doc)) {
                        builder.appendOrd(ordinals.ordValue());
                    } else {
                        builder.appendNull();
                    }
                }
                return builder.build();
            }
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            if (ordinals.advanceExact(docId)) {
                ((BytesRefBuilder) builder).appendBytesRef(ordinals.lookupOrd(ordinals.ordValue()));
            } else {
                builder.appendNull();
            }
        }

        @Override
        public int docId() {
            return ordinals.docID();
        }

        @Override
        public String toString() {
            return "BytesRefsFromOrds.Singleton";
        }
    }

    protected static class SortedSet extends BlockDocValuesReader {
        private final SortedSetDocValues ordinals;

        SortedSet(SortedSetDocValues ordinals) {
            this.ordinals = ordinals;
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            if (docs.count() - offset == 1) {
                return readSingleDoc(factory, docs.get(offset));
            }
            try (var builder = factory.sortedSetOrdinalsBuilder(ordinals, docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (doc < ordinals.docID()) {
                        throw new IllegalStateException("docs within same block must be in order");
                    }
                    if (ordinals.advanceExact(doc) == false) {
                        builder.appendNull();
                        continue;
                    }
                    int count = ordinals.docValueCount();
                    if (count == 1) {
                        builder.appendOrd(Math.toIntExact(ordinals.nextOrd()));
                    } else {
                        builder.beginPositionEntry();
                        for (int c = 0; c < count; c++) {
                            builder.appendOrd(Math.toIntExact(ordinals.nextOrd()));
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
            if (ordinals.advanceExact(docId) == false) {
                return factory.constantNulls(1);
            }
            int count = ordinals.docValueCount();
            if (count == 1) {
                BytesRef v = ordinals.lookupOrd(ordinals.nextOrd());
                return factory.constantBytes(BytesRef.deepCopyOf(v), 1);
            }
            try (var builder = factory.bytesRefsFromDocValues(count)) {
                builder.beginPositionEntry();
                for (int c = 0; c < count; c++) {
                    BytesRef v = ordinals.lookupOrd(ordinals.nextOrd());
                    builder.appendBytesRef(v);
                }
                builder.endPositionEntry();
                return builder.build();
            }
        }

        private void read(int docId, BytesRefBuilder builder) throws IOException {
            if (false == ordinals.advanceExact(docId)) {
                builder.appendNull();
                return;
            }
            int count = ordinals.docValueCount();
            if (count == 1) {
                builder.appendBytesRef(ordinals.lookupOrd(ordinals.nextOrd()));
                return;
            }
            builder.beginPositionEntry();
            for (int v = 0; v < count; v++) {
                builder.appendBytesRef(ordinals.lookupOrd(ordinals.nextOrd()));
            }
            builder.endPositionEntry();
        }

        @Override
        public int docId() {
            return ordinals.docID();
        }

        @Override
        public String toString() {
            return "BytesRefsFromOrds.SortedSet";
        }
    }
}
