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
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.SortedDvSingletonOrSet;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingSortedDocValues;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingSortedSetDocValues;

import java.io.IOException;

/**
 * Loads {@code keyword} style fields that are stored as a lookup table and ordinals.  See also {@link BytesRefsFromCustomBinaryBlockLoader}
 * for {@code wildcard} style (i.e. non-ordinal encoded multivalued) and {@link BytesRefsFromBinaryBlockLoader} for {@code histogram}
 * style (i.e. non-ordinal single valued).
 */
public class BytesRefsFromOrdsBlockLoader extends AbstractBytesRefsFromOrdsBlockLoader {
    public BytesRefsFromOrdsBlockLoader(String fieldName, ByteSizeValue size) {
        super(fieldName, size);
    }

    @Override
    protected ColumnAtATimeReader singletonReader(TrackingSortedDocValues docValues) {
        return new Singleton(docValues);
    }

    @Override
    protected ColumnAtATimeReader sortedSetReader(TrackingSortedSetDocValues docValues) {
        return new SortedSet(docValues);
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
    }
}
