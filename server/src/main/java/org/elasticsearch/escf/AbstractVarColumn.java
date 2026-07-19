/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IntsRef;
import org.elasticsearch.common.bytes.BytesReference;

/**
 * Shared base for the variable-length columns (STRING and BINARY), whose values are a contiguous
 * {@code data} payload delimited by a {@code (docCount + 1)}-entry offset vector
 * ({@code [offsets[d], offsets[d + 1])} within {@code data}).
 */
abstract class AbstractVarColumn extends EscfColumn {

    final BytesReference data;
    final IntsRef offsets;

    AbstractVarColumn(int docCount, FixedBitSet absent, BytesReference data, IntsRef offsets) {
        super(docCount, absent);
        this.data = data;
        this.offsets = offsets;
        assert offsets.length == docCount + 1;
    }

    abstract AbstractVarColumn newSlice(int count, FixedBitSet sliceAbsent, BytesReference sliceData, IntsRef sliceOffsets);

    /**
     * Returns a forward-only dense {@link ObjectTupleCursor}{@code <BytesRef>} positioned before the first row of this
     * column's window. Dense-only: every row in {@code [0, docCount)} is yielded in order; the
     * absent bitset is not consulted.
     */
    @Override
    final ObjectTupleCursor<BytesRef> bytesRefCursor() {
        return new ObjectTupleCursor<>() {
            private int row = -1;

            @Override
            public int nextDoc() {
                // TODO: does not support sparse yet. Need to iterate bitset too.
                return ++row < docCount ? row : DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public BytesRef value() {
                return getBinaryValue(row);
            }
        };
    }

    @Override
    final BytesRef getBinaryValue(int row) {
        int off = intAt(offsets, row);
        return data.slice(off, intAt(offsets, row + 1) - off).toBytesRef();
    }

    @Override
    final EscfColumn sliceInternal(int from, int count) {
        // data is kept full/shared; the slice is expressed by adjusting dataOffsets.offset.
        return newSlice(count, windowBitSet(absent, from, count), data, sliceOffsets(offsets, from, count));
    }

    @Override
    final EscfColumnData toColumnData() {
        BytesReference newData = sliceData(offsets, data, docCount);
        int[] newOffsets = rebasedOffsets(offsets, docCount);
        return EscfColumnData.ofVarWidth(kind(), docCount, absent, newOffsets, newData);
    }
}
