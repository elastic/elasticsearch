/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.fn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.mapper.blockloader.docvalues.AbstractBytesRefsFromOrdsBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingSortedDocValues;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingSortedSetDocValues;

import java.io.IOException;

/**
 * Loads the MAX {@code keyword} in each doc.
 */
public class MvMaxBytesRefsFromOrdsBlockLoader extends AbstractBytesRefsFromOrdsBlockLoader {
    private final String fieldName;

    public MvMaxBytesRefsFromOrdsBlockLoader(String fieldName, ByteSizeValue byteSize) {
        super(fieldName, byteSize);
        this.fieldName = fieldName;
    }

    @Override
    protected AllReader singletonReader(TrackingSortedDocValues docValues) {
        return new Singleton(docValues);
    }

    @Override
    protected AllReader sortedSetReader(TrackingSortedSetDocValues docValues) {
        return new MvMaxSortedSet(docValues);
    }

    @Override
    public String toString() {
        return "MvMaxBytesRefsFromOrds[" + fieldName + "]";
    }

    private class MvMaxSortedSet extends BlockDocValuesReader {
        private final TrackingSortedSetDocValues ordinals;

        MvMaxSortedSet(TrackingSortedSetDocValues ordinals) {
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
                    discardAllButLast();
                    builder.appendOrd(Math.toIntExact(ordinals.docValues().nextOrd()));
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
            discardAllButLast();
            BytesRef v = ordinals.docValues().lookupOrd(ordinals.docValues().nextOrd());
            return factory.constantBytes(BytesRef.deepCopyOf(v), 1);
        }

        private void read(int docId, BytesRefBuilder builder) throws IOException {
            if (false == ordinals.docValues().advanceExact(docId)) {
                builder.appendNull();
                return;
            }
            discardAllButLast();
            builder.appendBytesRef(ordinals.docValues().lookupOrd(ordinals.docValues().nextOrd()));
        }

        private void discardAllButLast() throws IOException {
            int count = ordinals.docValues().docValueCount();
            for (int i = 0; i < count - 1; i++) {
                ordinals.docValues().nextOrd();
            }
        }

        @Override
        public int docId() {
            return ordinals.docValues().docID();
        }

        @Override
        public String toString() {
            return "MvMaxBytesRefsFromOrds.SortedSet";
        }

        @Override
        public void close() {
            ordinals.close();
        }
    }
}
