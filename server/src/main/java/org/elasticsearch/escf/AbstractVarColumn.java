/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.document.column.BytesRefValuesCursor;
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

    AbstractVarColumn(int docCount, FixedBitSet validity, BytesReference data, IntsRef offsets) {
        super(docCount, validity);
        this.data = data;
        this.offsets = offsets;
        assert offsets.length == docCount + 1;
    }

    abstract AbstractVarColumn newSlice(int count, FixedBitSet sliceValidity, BytesReference sliceData, IntsRef sliceOffsets);

    /**
     * Returns a forward-only {@link ObjectTupleCursor}{@code <BytesRef>} positioned before the first
     * row of this column's window. Absent rows (clear bits in the {@link #validity} bitset) are skipped;
     * present rows are yielded in ascending order. The returned {@link BytesRef} is valid only until
     * the next {@link ObjectTupleCursor#nextDoc()} call.
     */
    @Override
    final ObjectTupleCursor<BytesRef> bytesRefCursor() {
        return new BytesRefTupleCursor(this);
    }

    /**
     * Returns a dense {@link BytesRefValuesCursor} positioned before the first row of this column's
     * window. The column must be fully present ({@link #validity} {@code == null}); call this only on
     * dense columns. The returned {@link BytesRef} per {@link BytesRefValuesCursor#nextValue()} is
     * valid only until the next call to {@code nextValue()}.
     */
    final BytesRefValuesCursor bytesRefValuesCursor() {
        assert validity == null : "values cursor is only valid for dense (fully-present) columns";
        return new DenseBytesRefValuesCursor(docCount, this);
    }

    @Override
    final BytesRef getBinaryValue(int row) {
        int off = intAt(offsets, row);
        return data.slice(off, intAt(offsets, row + 1) - off).toBytesRef();
    }

    @Override
    final EscfColumn sliceInternal(int from, int count) {
        // data is kept full/shared; the slice is expressed by adjusting dataOffsets.offset.
        return newSlice(count, windowValidity(validity, from, count), data, sliceOffsets(offsets, from, count));
    }

    @Override
    final EscfColumnData toColumnData() {
        BytesReference newData = sliceData(offsets, data, docCount);
        int[] newOffsets = rebasedOffsets(offsets, docCount);
        return EscfColumnData.ofVarWidth(kind(), docCount, validity, newOffsets, newData);
    }

    private static final class BytesRefTupleCursor extends ObjectTupleCursor<BytesRef> {
        private final AbstractVarColumn column;
        private int row = -1;

        BytesRefTupleCursor(AbstractVarColumn column) {
            this.column = column;
        }

        @Override
        public int nextDoc() {
            while (++row < column.docCount) {
                if (column.isAbsent(row) == false) {
                    return row;
                }
            }
            return DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public BytesRef value() {
            return column.getBinaryValue(row);
        }
    }

    private static final class DenseBytesRefValuesCursor extends BytesRefValuesCursor {
        private final AbstractVarColumn column;
        private int pos;

        DenseBytesRefValuesCursor(int count, AbstractVarColumn column) {
            super(count);
            this.column = column;
        }

        @Override
        public BytesRef nextValue() {
            if (pos >= size()) {
                throw new IllegalStateException("nextValue() called more than size()=" + size() + " times");
            }
            return column.getBinaryValue(pos++);
        }
    }
}
