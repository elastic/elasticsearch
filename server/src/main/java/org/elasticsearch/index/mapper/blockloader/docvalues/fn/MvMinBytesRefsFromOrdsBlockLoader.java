/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.fn;

import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.blockloader.docvalues.AbstractBytesRefsFromOrdsBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;

import java.io.IOException;

/**
 * Loads the MIN {@code keyword} in each doc.
 */
public class MvMinBytesRefsFromOrdsBlockLoader extends AbstractBytesRefsFromOrdsBlockLoader {
    private final String fieldName;

    public MvMinBytesRefsFromOrdsBlockLoader(String fieldName) {
        super(fieldName);
        this.fieldName = fieldName;
    }

    @Override
    protected AllReader singletonReader(SortedDocValues docValues) {
        return new Singleton(docValues);
    }

    @Override
    protected AllReader sortedSetReader(SortedSetDocValues docValues) {
        return new MvMinSortedSet(docValues);
    }

    @Override
    public String toString() {
        return "MvMinBytesRefsFromOrds[" + fieldName + "]";
    }

    private static class MvMinSortedSet extends BlockDocValuesReader {
        private final SortedSetDocValues ordinals;

        MvMinSortedSet(SortedSetDocValues ordinals) {
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
                    builder.appendOrd(Math.toIntExact(ordinals.nextOrd()));
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
            BytesRef v = ordinals.lookupOrd(ordinals.nextOrd());
            return factory.constantBytes(BytesRef.deepCopyOf(v), 1);
        }

        private void read(int docId, BytesRefBuilder builder) throws IOException {
            if (false == ordinals.advanceExact(docId)) {
                builder.appendNull();
                return;
            }
            builder.appendBytesRef(ordinals.lookupOrd(ordinals.nextOrd()));
        }

        @Override
        public int docId() {
            return ordinals.docID();
        }

        @Override
        public String toString() {
            return "MvMinBytesRefsFromOrds.SortedSet";
        }
    }
}
