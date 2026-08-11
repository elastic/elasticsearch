/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.bitmapterms;

import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * {@link BitmapValues} over a 32-bit {@link RoaringBitmap}, for {@code integer} fields.
 * <p>
 * Values are reported as {@code long}, which is lossless and order-preserving for {@code int}. The
 * reverse never happens: {@link #encodeFirst}, {@link #encodeLast} and
 * {@link BitmapValues.PeekableIterator#encodePeek} all encode from the {@code int} the bitmap already holds.
 */
public final class IntBitmap implements BitmapValues {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(IntBitmap.class);

    private final RoaringBitmap bitmap;

    IntBitmap(RoaringBitmap bitmap) {
        this.bitmap = bitmap;
    }

    /**
     * Decodes the standard 32-bit RoaringBitmap serialization, in little-endian byte order &mdash; what
     * {@code RoaringBitmap#serialize} and pyroaring's {@code BitMap#serialize} produce.
     *
     * @throws IOException if the bytes are not a valid serialized bitmap
     * @throws IllegalArgumentException if the bitmap's internal structure does not validate
     */
    public static IntBitmap deserialize(byte[] bytes) throws IOException {
        RoaringBitmap bitmap = new RoaringBitmap();
        bitmap.deserialize(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN));
        // validate() reports its verdict by return value rather than by throwing.
        if (bitmap.validate() == false) {
            throw new IllegalArgumentException("the bitmap's internal structure is not valid");
        }
        return new IntBitmap(bitmap);
    }

    public static IntBitmap bitmapOf(int... values) {
        return new IntBitmap(RoaringBitmap.bitmapOf(values));
    }

    /**
     * Whether any value has its sign bit set. The bitmap orders values as unsigned, so {@link #last()}
     * is the unsigned maximum; a negative result there means at least one value would sort differently
     * as a signed int than the bitmap iterates it.
     */
    boolean hasNegativeValues() {
        return isEmpty() == false && bitmap.last() < 0;
    }

    @Override
    public boolean isEmpty() {
        return bitmap.isEmpty();
    }

    @Override
    public long cardinality() {
        return bitmap.getLongCardinality();
    }

    @Override
    public long first() {
        return bitmap.first();
    }

    @Override
    public long last() {
        return bitmap.last();
    }

    @Override
    public int bytesPerValue() {
        return Integer.BYTES;
    }

    @Override
    public long decode(byte[] src, int offset) {
        return NumericUtils.sortableBytesToInt(src, offset);
    }

    @Override
    public void encodeFirst(byte[] dest) {
        NumericUtils.intToSortableBytes(bitmap.first(), dest, 0);
    }

    @Override
    public void encodeLast(byte[] dest) {
        NumericUtils.intToSortableBytes(bitmap.last(), dest, 0);
    }

    @Override
    public PeekableIterator iterator() {
        return new IntValuesIterator(bitmap.getIntIterator());
    }

    /**
     * Delegates straight to {@link PeekableIntIterator}, which is natively peekable and whose
     * {@code advanceIfNeeded} skips whole containers rather than walking values.
     */
    private static final class IntValuesIterator implements PeekableIterator {

        private final PeekableIntIterator values;
        private boolean exhausted;

        IntValuesIterator(PeekableIntIterator values) {
            this.values = values;
        }

        @Override
        public boolean hasNext() {
            return exhausted == false && values.hasNext();
        }

        @Override
        public long peek() {
            return values.peekNext();
        }

        @Override
        public void encodePeek(byte[] dest) {
            NumericUtils.intToSortableBytes(values.peekNext(), dest, 0);
        }

        @Override
        public long next() {
            return values.next();
        }

        @Override
        public void advanceTo(long target) {
            if (target > Integer.MAX_VALUE) {
                // No int can reach the target, so nothing remaining can match. Flag it rather than
                // narrowing the target, which would wrap round into a much smaller value.
                exhausted = true;
            } else if (target > 0) {
                values.advanceIfNeeded((int) target);
            }
            // target <= 0 is at or below every value, so there is nothing to skip
        }
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + bitmap.getLongSizeInBytes();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o instanceof IntBitmap other && bitmap.equals(other.bitmap);
    }

    @Override
    public int hashCode() {
        return bitmap.hashCode();
    }

    @Override
    public String toString() {
        if (isEmpty()) {
            return "IntBitmap(cardinality=0)";
        }
        return "IntBitmap(cardinality=" + cardinality() + ", first=" + first() + ", last=" + last() + ")";
    }
}
