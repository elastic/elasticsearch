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
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IntsRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;

import java.io.IOException;
import java.io.UncheckedIOException;

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
        // We do not handle lifecycle here. Unwrap to reduce indirection.
        this.data = ReleasableBytesReference.unwrap(data);
        this.offsets = offsets;
        assert offsets.length == docCount + 1;
    }

    abstract AbstractVarColumn newSlice(int count, FixedBitSet sliceValidity, BytesReference sliceData, IntsRef sliceOffsets);

    /**
     * Returns a forward-only {@link ObjectTupleCursor}{@code <BytesRef>} positioned before the first
     * row of this column's window. Absent rows (clear bits in the {@link #validity} bitset) are skipped;
     * present rows are yielded in ascending order.
     *
     * @param retainValues when {@code false} every {@link ObjectTupleCursor#value()} returns the cursor's
     *                     single reusable {@link BytesRef}, valid only until the next
     *                     {@link ObjectTupleCursor#nextDoc()} call — the plain {@code ObjectTupleCursor}
     *                     contract, and allocation-free. When {@code true} each value is a fresh
     *                     {@link BytesRef} that stays valid indefinitely; pass {@code true} only when the
     *                     values must outlive the cursor position.
     */
    @Override
    public final ObjectTupleCursor<BytesRef> bytesRefCursor(boolean retainValues) {
        return new BytesRefTupleCursor(presentDocs(), new DenseBytesRefValuesCursor(docCount, this, retainValues));
    }

    /**
     * Returns a dense {@link BytesRefValuesCursor} positioned before the first row of this column's
     * window. This cursor is purely positional — it advances one slot per call and is unaware of the
     * validity bitset. The caller is responsible for consulting the validity (or child bitset) to
     * determine whether each slot is meaningful. For null elements in an array child, a null slot has
     * a zero-length offset range and therefore returns an empty {@link BytesRef}; the child validity
     * bitset distinguishes null from an empty string.
     *
     * @param retainValues when {@code false} every {@link BytesRefValuesCursor#nextValue()} returns the
     *                     cursor's single reusable {@link BytesRef}, valid only until the next
     *                     {@code nextValue()} call — the {@code BytesRefValuesCursor} contract, and
     *                     allocation-free. When {@code true} each value is a fresh {@link BytesRef} that
     *                     stays valid indefinitely.
     */
    final DenseBytesRefValuesCursor bytesRefValuesCursor(boolean retainValues) {
        return new DenseBytesRefValuesCursor(docCount, this, retainValues);
    }

    @Override
    public final BytesRef getBinaryValue(int row) {
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

    static final class BytesRefTupleCursor extends ObjectTupleCursor<BytesRef> {
        private final PresentDocIterator present;
        private final DenseBytesRefValuesCursor values;
        private int lastRow = -1;
        private BytesRef currentValue;

        BytesRefTupleCursor(PresentDocIterator present, DenseBytesRefValuesCursor values) {
            this.present = present;
            this.values = values;
        }

        @Override
        public int nextDoc() {
            int doc = present.nextDoc();
            if (doc == DocIdSetIterator.NO_MORE_DOCS) {
                return doc;
            }
            int toSkip = doc - lastRow - 1; // absent rows between the previous present row and this one
            if (toSkip > 0) {
                values.skip(toSkip);
            }
            currentValue = values.nextValue();
            lastRow = doc;
            return doc;
        }

        @Override
        public BytesRef value() {
            return currentValue;
        }
    }

    static final class DenseBytesRefValuesCursor extends BytesRefValuesCursor {

        private final BytesRefIterator iter;
        private final int[] offsets;
        private final BytesRef value = new BytesRef();
        private final boolean retainValues;
        private byte[] currentBytes = BytesRef.EMPTY_BYTES;
        private byte[] scratch = BytesRef.EMPTY_BYTES;
        private int currentBytesOffset;
        private int currentBytesEnd;
        private int nextOffsetIndex;
        private int valueOffset;
        private int pos;

        DenseBytesRefValuesCursor(int count, AbstractVarColumn column, boolean retainValues) {
            this(count, column.offsets, column.data, retainValues);
        }

        /** Constructs a cursor directly from raw offset and data buffers. */
        DenseBytesRefValuesCursor(int count, IntsRef offsets, BytesReference data, boolean retainValues) {
            super(count);
            this.iter = sliceData(offsets, data, count).iterator();
            this.offsets = offsets.ints;
            this.nextOffsetIndex = offsets.offset + 1;
            this.valueOffset = this.offsets[offsets.offset];
            this.retainValues = retainValues;
        }

        private void nextChunk() {
            try {
                BytesRef chunk = iter.next();
                if (chunk == null) {
                    throw new IllegalStateException("variable-width column data exhausted before all values were read");
                }
                currentBytes = chunk.bytes;
                currentBytesOffset = chunk.offset;
                currentBytesEnd = chunk.offset + chunk.length;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private BytesRef readNextValue(int valueSize) {
            if (valueSize == 0) {
                value.bytes = BytesRef.EMPTY_BYTES;
                value.offset = 0;
                value.length = 0;
                return value;
            }
            if (currentBytesOffset >= currentBytesEnd) {
                nextChunk();
            }
            int remaining = currentBytesEnd - currentBytesOffset;
            if (valueSize > remaining) {
                return readStraddlingValue(valueSize);
            }
            value.bytes = currentBytes;
            value.offset = currentBytesOffset;
            value.length = valueSize;
            currentBytesOffset += valueSize;
            return value;
        }

        private BytesRef readStraddlingValue(int valueSize) {
            scratch = ArrayUtil.growNoCopy(scratch, valueSize);
            int copied = 0;
            while (copied < valueSize) {
                if (currentBytesOffset >= currentBytesEnd) {
                    nextChunk();
                }
                int toCopy = Math.min(valueSize - copied, currentBytesEnd - currentBytesOffset);
                System.arraycopy(currentBytes, currentBytesOffset, scratch, copied, toCopy);
                currentBytesOffset += toCopy;
                copied += toCopy;
            }
            value.bytes = scratch;
            value.offset = 0;
            value.length = valueSize;
            return value;
        }

        /** Advances past {@code n} values without reading them. */
        void skip(int n) {
            pos += n;
            nextOffsetIndex += n;
            // end offset of the last skipped row = start of the next row to read
            int newOffset = offsets[nextOffsetIndex - 1];
            int byteDelta = newOffset - valueOffset; // 0 when every skipped row is absent (zero-width offsets)
            valueOffset = newOffset;
            while (byteDelta > 0) {
                if (currentBytesOffset >= currentBytesEnd) {
                    nextChunk();
                }
                int toSkip = Math.min(byteDelta, currentBytesEnd - currentBytesOffset);
                currentBytesOffset += toSkip;
                byteDelta -= toSkip;
            }
        }

        @Override
        public BytesRef nextValue() {
            if (pos >= size()) {
                throw new IllegalStateException("nextValue() called more than size()=" + size() + " times");
            }
            int nextOffset = offsets[nextOffsetIndex++];
            int valueSize = nextOffset - valueOffset;
            valueOffset = nextOffset;
            pos++;
            BytesRef v = readNextValue(valueSize);
            if (retainValues == false) {
                return v;
            }
            // The contiguous fast path points straight into a data chunk, which never changes, so a new
            // header is enough. A straddling value lives in the reusable scratch buffer and must be copied.
            return v.bytes == scratch ? BytesRef.deepCopyOf(v) : new BytesRef(v.bytes, v.offset, v.length);
        }
    }
}
