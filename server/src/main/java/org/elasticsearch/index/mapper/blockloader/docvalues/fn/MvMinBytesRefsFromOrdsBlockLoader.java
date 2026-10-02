/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.fn;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromOrdsBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.SortedDvSingletonOrSet;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingSortedSetDocValues;

import java.io.IOException;

/**
 * Loads the MIN {@code keyword} in each doc.
 */
public class MvMinBytesRefsFromOrdsBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {
    private final String fieldName;
    private final ByteSizeValue size;

    public MvMinBytesRefsFromOrdsBlockLoader(String fieldName, ByteSizeValue byteSize) {
        this.fieldName = fieldName;
        this.size = byteSize;
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
        if (dv.singleton() != null) {
            return new BytesRefsFromOrdsBlockLoader.Singleton(dv.singleton());
        }
        return new MvMinSortedSet(dv.set());
    }

    @Override
    public String toString() {
        return "MvMinBytesRefsFromOrds[" + fieldName + "]";
    }

    private static class MvMinSortedSet extends BlockDocValuesReader {
        private final TrackingSortedSetDocValues ordinals;

        MvMinSortedSet(TrackingSortedSetDocValues ordinals) {
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
                    builder.appendOrd(Math.toIntExact(ordinals.docValues().nextOrd()));
                }
                return builder.build();
            }
        }

        private Block readSingleDoc(BlockFactory factory, int docId) throws IOException {
            if (ordinals.docValues().advanceExact(docId) == false) {
                return factory.constantNulls(1);
            }
            BytesRef v = ordinals.docValues().lookupOrd(ordinals.docValues().nextOrd());
            return factory.constantBytes(BytesRef.deepCopyOf(v), 1);
        }

        @Override
        public int docId() {
            return ordinals.docValues().docID();
        }

        @Override
        public String toString() {
            return "MvMinBytesRefsFromOrds.SortedSet";
        }

        @Override
        public void close() {
            ordinals.close();
        }
    }
}
