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
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.util.ByteUtils;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Shared base for the fixed-width 64-bit columns (LONG and DOUBLE), whose values are contiguous
 * little-endian 8-byte slots ({@code data.getLongLE(d * 8)}).
 */
abstract class AbstractFixed64Column extends EscfColumn {

    protected final BytesReference data;

    AbstractFixed64Column(int docCount, FixedBitSet validity, BytesReference data) {
        super(docCount, validity);
        assert assertDataValid(docCount, data);
        // We do not handle lifecycle here. Unwrap to reduce indirection.
        this.data = ReleasableBytesReference.unwrap(data);
    }

    /**
     * Returns a new dense {@link DenseLongValuesCursor} positioned before the first row of this
     * column's window. The column must be fully present ({@link #validity} {@code == null}); call
     * this only on dense columns (e.g. array children).
     */
    DenseLongValuesCursor longValuesCursor() {
        assert validity == null : "values cursor is only valid for dense (fully-present) columns";
        return new DenseLongValuesCursor(docCount, this);
    }

    /** The raw little-endian 8-byte slot for document {@code d}. */
    final long rawLong(int row) {
        return data.getLongLE(row * 8);
    }

    /**
     * Returns a new {@link LongTupleCursor} positioned before the first row of this column's window.
     * Absent rows (tracked by the {@link #validity} bitset) are skipped; present rows are yielded in
     * ascending order. Dense columns (no absent rows) iterate every row without any bitset overhead.
     *
     * <p>The yielded {@code longValue()} is the <em>raw 64-bit stored word</em>: for a
     * {@link EscfLongColumn} that is the long value; for a {@link EscfDoubleColumn} that is
     * {@code Double.doubleToRawLongBits(d)}.
     */
    @Override
    public LongTupleCursor longCursor() {
        return new LongCursor(this);
    }

    private static boolean assertDataValid(int docCount, BytesReference data) {
        assert data.length() == (long) docCount * 8
            : "fixed-64 column data length " + data.length() + " != docCount * 8 = " + ((long) docCount * 8);
        try {
            BytesRefIterator iter = data.iterator();
            BytesRef chunk;
            while ((chunk = iter.next()) != null) {
                assert chunk.length % 8 == 0 : "chunk length " + chunk.length + " is not a multiple of 8";
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return true;
    }

    private static final class LongCursor extends LongTupleCursor {
        private final PresentDocIterator present;
        private final DenseLongValuesCursor values;
        private int lastRow = -1;
        private long currentValue;

        LongCursor(AbstractFixed64Column column) {
            this.present = column.presentDocs();
            this.values = new DenseLongValuesCursor(column.docCount, column);
        }

        @Override
        public int nextDoc() {
            int doc = present.nextDoc();
            if (doc == DocIdSetIterator.NO_MORE_DOCS) {
                return doc;
            }
            int toSkip = doc - lastRow - 1;
            if (toSkip > 0) {
                values.skip(toSkip);
            }
            currentValue = values.nextLong();
            lastRow = doc;
            return doc;
        }

        @Override
        public long longValue() {
            return currentValue;
        }
    }

    /**
     * A forward-only, dense (no absent-row skipping) cursor over the raw 64-bit words of a
     * {@link AbstractFixed64Column}. The caller is responsible for skipping absent rows; this cursor
     * always advances exactly one word per call. See {@link LongCursor} for the sparse wrapper.
     */
    static final class DenseLongValuesCursor extends LongValuesCursor {
        private final BytesRefIterator iter;
        private byte[] currentBytes = BytesRef.EMPTY_BYTES;
        private int currentBytesOffset;
        private int currentBytesEnd;
        private int pos;

        DenseLongValuesCursor(int count, AbstractFixed64Column column) {
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
        void skip(int n) {
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
