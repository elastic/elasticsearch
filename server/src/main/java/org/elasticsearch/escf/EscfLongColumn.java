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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.sourcebatch.SourceValueType;

import java.io.IOException;
import java.io.UncheckedIOException;

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

    /**
     * Returns a new {@link LongTupleCursor} positioned before the first row of this column's window.
     * Absent rows (tracked by the {@link #validity} bitset) are skipped; present rows are yielded in
     * ascending order. Dense columns (no absent rows) iterate every row without any bitset overhead.
     */
    @Override
    LongTupleCursor longCursor() {
        return new LongCursor(this);
    }

    /**
     * Returns a new dense {@link LongValuesCursor} positioned before the first row of this column's
     * window. The column must be fully present ({@link #validity} {@code == null}); call this only on
     * dense columns.
     */
    LongValuesCursor longValuesCursor() {
        assert validity == null : "values cursor is only valid for dense (fully-present) columns";
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

    private static final class LongCursor extends LongTupleCursor {
        private final EscfLongColumn column;
        private final DenseLongValuesCursor values;
        private int row = -1;
        private long currentValue;

        LongCursor(EscfLongColumn column) {
            this.column = column;
            this.values = new DenseLongValuesCursor(column.docCount, column);
        }

        @Override
        public int nextDoc() {
            int toSkip = 0;
            while (++row < column.docCount) {
                if (column.isAbsent(row)) {
                    toSkip++;
                } else {
                    if (toSkip > 0) {
                        values.skip(toSkip);
                    }
                    currentValue = values.nextLong();
                    return row;
                }
            }
            return DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public long longValue() {
            return currentValue;
        }
    }

    private static final class DenseLongValuesCursor extends LongValuesCursor {
        private final BytesRefIterator iter;
        private byte[] currentBytes = BytesRef.EMPTY_BYTES;
        private int currentBytesOffset;
        private int currentBytesEnd;
        private int pos;

        DenseLongValuesCursor(int count, EscfLongColumn column) {
            super(count);
            this.iter = column.data.iterator();
            if (count > 0) {
                nextChunk();
            }
        }

        private void nextChunk() {
            try {
                BytesRef chunk = iter.next();
                if ((chunk.length & 7) != 0) {
                    throw new IllegalStateException("long column chunk length " + chunk.length + " is not a multiple of 8");
                }
                currentBytes = chunk.bytes;
                currentBytesOffset = chunk.offset;
                currentBytesEnd = chunk.offset + chunk.length;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private long readNextLong() {
            if (currentBytesOffset >= currentBytesEnd) {
                nextChunk();
            }
            long val = ByteUtils.readLongLE(currentBytes, currentBytesOffset);
            currentBytesOffset += 8;
            return val;
        }

        /** Advances past {@code n} values without reading them. */
        private void skip(int n) {
            pos += n;
            while (n > 0) {
                if (currentBytesOffset >= currentBytesEnd) {
                    nextChunk();
                }
                int longsAvailable = (currentBytesEnd - currentBytesOffset) >> 3;
                int toSkip = Math.min(n, longsAvailable);
                currentBytesOffset += toSkip << 3;
                n -= toSkip;
            }
        }

        @Override
        public long nextLong() {
            if (pos >= size()) {
                throw new IllegalStateException("nextLong() called more than size()=" + size() + " times");
            }
            pos++;
            return readNextLong();
        }

        @Override
        public void fillDocValues(long[] dst, int offset, int length) {
            if (pos + length > size()) {
                throw new IllegalStateException("fill of " + length + " from pos " + pos + " exceeds size()=" + size());
            }
            pos += length;
            int end = offset + length;
            while (offset < end) {
                if (currentBytesOffset >= currentBytesEnd) {
                    nextChunk();
                }
                int longsAvailable = (currentBytesEnd - currentBytesOffset) >> 3;
                int toRead = Math.min(end - offset, longsAvailable);
                for (int i = 0; i < toRead; i++) {
                    dst[offset++] = ByteUtils.readLongLE(currentBytes, currentBytesOffset);
                    currentBytesOffset += 8;
                }
            }
        }
    }
}
