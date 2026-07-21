/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.document.column.LongTupleCursor;
import org.apache.lucene.document.column.LongValuesCursor;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.sourcebatch.SourceValueType;

import java.util.Objects;

/** An ESCF column whose values are all {@code long}s (JSON ints and longs upcast to 64-bit). */
final class EscfLongColumn extends AbstractFixed64Column {

    EscfLongColumn(int docCount, FixedBitSet validity, BytesReference data) {
        super(docCount, validity, data);
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
        return new LongCursor(docCount, this);
    }

    LongValuesCursor longValuesCursor() {
        return new DenseLongValuesCursor(docCount, this);
    }

    @Override
    EscfColumn sliceInternal(int from, int count) {
        return new EscfLongColumn(count, windowValidity(validity, from, count), data.slice(from * 8, count * 8));
    }

    @Override
    EscfColumnData toColumnData() {
        return EscfColumnData.ofFixed64(kind(), docCount, validity, data);
    }

    private static final class DenseLongValuesCursor extends LongValuesCursor {
        private final EscfLongColumn column;
        private int pos;

        DenseLongValuesCursor(int count, EscfLongColumn column) {
            super(count);
            this.column = column;
        }

        @Override
        public long nextLong() {
            Objects.checkIndex(pos, size());
            return column.getLongValue(pos++);
        }

        @Override
        public void fillDocValues(long[] dst, int offset, int length) {
            Objects.checkFromIndexSize(pos, length, size());
            // TODO: implement based on the BytesRefIterator to remove most bounds checks
            for (int i = 0; i < length; i++) {
                dst[offset + i] = column.getLongValue(pos++);
            }
        }
    }

    private static final class LongCursor extends LongTupleCursor {
        private final int rowCount;
        private final EscfLongColumn column;
        private int row = -1;

        LongCursor(int rowCount, EscfLongColumn column) {
            this.rowCount = rowCount;
            this.column = column;
        }

        @Override
        public int nextDoc() {
            // TODO: does not support sparse yet. Need to iterate bitset too.
            return ++row < rowCount ? row : DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public long longValue() {
            return column.getLongValue(row);
        }
    }
}
