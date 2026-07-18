/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.sourcebatch.SourceValueType;

/** An ESCF column whose values are all {@code long}s (JSON ints and longs upcast to 64-bit). */
final class EscfLongColumn extends AbstractFixed64Column {

    /**
     * A forward-only cursor over this column's long values, in row order. Dense-only: every row in
     * {@code [0, docCount)} has a value; no absent-set check is performed.
     */
    interface LongCursor {
        /**
         * Advances to the next row and returns its 0-based row-id, or
         * {@link DocIdSetIterator#NO_MORE_DOCS} when the column is exhausted.
         */
        int nextRow();

        /** Returns the long value for the current row. Valid only after a successful {@link #nextRow()}. */
        long longValue();
    }

    EscfLongColumn(int docCount, FixedBitSet absent, BytesReference data) {
        super(docCount, absent, data);
    }

    @Override
    byte kind() {
        return EscfColumnKind.LONG;
    }

    @Override
    byte typeByteForPresent(int row) {
        return SourceValueType.LONG;
    }

    @Override
    long getLongValue(int row) {
        return rawLong(row);
    }

    /** Returns a new dense {@link LongCursor} positioned before the first row of this column's window. */
    LongCursor longCursor() {
        final int rowCount = docCount;
        return new LongCursor() {
            private int row = -1;

            @Override
            public int nextRow() {
                return ++row < rowCount ? row : DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public long longValue() {
                return getLongValue(row);
            }
        };
    }

    @Override
    EscfColumn sliceInternal(int from, int count) {
        return new EscfLongColumn(count, windowBitSet(absent, from, count), data.slice(from * 8, count * 8));
    }

    @Override
    EscfColumnData toColumnData() {
        return EscfColumnData.ofFixed64(kind(), docCount, absent, data);
    }
}
