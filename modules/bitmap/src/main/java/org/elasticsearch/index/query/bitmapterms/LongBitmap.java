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
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * This wraps {@link Roaring64NavigableMap} to make a 64-bit roaring bitmap safe to hand to a
 * {@link org.apache.lucene.search.Query}, which must be immutable because it is shared across
 * concurrent per-segment search threads and cached. Only read methods are exposed, and
 * {@link Roaring64NavigableMap#getLongCardinality()} — documented as a mutator, since it caches
 * cumulative cardinalities and structurally removes empty buckets — is called once during
 * construction, along with the other derived values.
 * <p>
 * Values are ordered as unsigned longs, matching the wire format read by
 * {@link #deserializePortable}. Callers comparing them against signed longs from the index must
 * therefore reject bitmaps containing negative values; see {@link #hasNegativeValues()}.
 */
public final class LongBitmap implements BitmapValues {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(LongBitmap.class);

    private final Roaring64NavigableMap bitmap;

    /*
     * Derived values are frozen at construction: getLongCardinality() mutates the bitmap, and the
     * others are O(buckets) per call on a structure the query cache probes repeatedly.
     */
    private final long cardinality;
    private final long first;
    private final long last;
    private final long ramBytesUsed;
    private final int hashCode;

    private LongBitmap(Roaring64NavigableMap bitmap) {
        this.bitmap = bitmap;
        // Read cardinality first, and keep it first: the portable bytes may legally contain a bucket
        // whose 32-bit bitmap is empty, and this call is what prunes those. Reading first()/last()
        // ahead of it would hit such a bucket and throw.
        this.cardinality = bitmap.getLongCardinality();
        if (cardinality == 0) {
            this.first = 0;
            this.last = 0;
        } else {
            this.first = bitmap.first();
            this.last = bitmap.last();
        }
        this.ramBytesUsed = SHALLOW_SIZE + bitmap.getLongSizeInBytes();
        // Delegates to the backing TreeMap, which hashes every bucket's bitmap. The query cache hashes
        // its keys on every probe, so pay this once rather than per lookup.
        this.hashCode = bitmap.hashCode();
    }

    /**
     * Decodes the cross-language "portable" 64-bit format
     * (<a href="https://github.com/RoaringBitmap/RoaringFormatSpec#extention-for-64-bit-implementations">spec</a>):
     * an 8-byte little-endian bucket count, then per bucket a 4-byte little-endian key holding the
     * high 32 bits followed by a standard 32-bit RoaringBitmap serialization of the low 32 bits.
     * <p>
     * {@code Roaring64NavigableMap#serializePortable}, CRoaring's
     * {@code roaring64_bitmap_portable_serialize}, Go's {@code roaring64.Bitmap#WriteTo} and
     * pyroaring's {@code BitMap64#serialize} all emit these bytes. Java's plain
     * {@code Roaring64NavigableMap#serialize} does <em>not</em>: it writes a legacy layout unless the
     * class-wide static {@code SERIALIZATION_MODE} has been flipped, so Java clients must call
     * {@code serializePortable}.
     *
     * @throws IOException if the bytes are truncated or are not a valid serialized bitmap
     * @throws IllegalArgumentException if a valid bitmap is followed by trailing bytes
     */
    public static LongBitmap deserializePortable(byte[] bytes) throws IOException {
        Roaring64NavigableMap bitmap = new Roaring64NavigableMap();
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        // deserializePortable, never deserialize: the latter dispatches on the class-wide static
        // SERIALIZATION_MODE, which defaults to the legacy layout.
        bitmap.deserializePortable(new DataInputStream(in));
        // The reader stops at the end of the bitmap and ignores anything after it. These bytes come
        // from a search request, so reject leftovers instead of silently accepting bitmap + garbage.
        if (in.available() != 0) {
            throw new IllegalArgumentException(
                "found " + in.available() + " trailing byte(s) after a valid 64-bit RoaringBitmap; the value is malformed"
            );
        }
        return new LongBitmap(bitmap);
    }

    /** Builds a bitmap holding {@code values}. */
    public static LongBitmap bitmapOf(long... values) {
        Roaring64NavigableMap bitmap = new Roaring64NavigableMap();
        bitmap.add(values);
        return new LongBitmap(bitmap);
    }

    @Override
    public boolean isEmpty() {
        return cardinality == 0;
    }

    @Override
    public long cardinality() {
        return cardinality;
    }

    /** The smallest value, in unsigned order. Undefined when the bitmap is empty. */
    @Override
    public long first() {
        return first;
    }

    /** The largest value, in unsigned order. Undefined when the bitmap is empty. */
    @Override
    public long last() {
        return last;
    }

    @Override
    public int bytesPerValue() {
        return Long.BYTES;
    }

    @Override
    public long decode(byte[] src, int offset) {
        return NumericUtils.sortableBytesToLong(src, offset);
    }

    @Override
    public void encodeFirst(byte[] dest) {
        NumericUtils.longToSortableBytes(first, dest, 0);
    }

    @Override
    public void encodeLast(byte[] dest) {
        NumericUtils.longToSortableBytes(last, dest, 0);
    }

    /**
     * Whether any value has its sign bit set. Since the bitmap orders values as unsigned,
     * {@link #last()} is the unsigned maximum, so a negative result there means at least one value
     * would sort differently as a signed long than the bitmap iterates it.
     */
    boolean hasNegativeValues() {
        return isEmpty() == false && last < 0;
    }

    /**
     * Always declines, which {@link BitmapValues#coversRange} permits, because the current implementation
     * {@link Roaring64NavigableMap} exposes no range-containment method.
     */
    @Override
    public boolean coversRange(long min, long max) {
        return false;
    }

    /** @return a iterator over the values, in ascending unsigned order */
    @Override
    public PeekableIterator iterator() {
        return new LongValuesIterator(bitmap.getLongIterator());
    }

    /**
     * A forward-only iterator over a {@link LongBitmap}'s values in ascending unsigned order.
     * <p>
     * A iterator never rewinds, so total work stays bounded by the cardinality. What it costs is the
     * per-seek complexity: {@link #advanceTo} is O(values skipped) where a bucket-indexed
     * representation would be O(log buckets). That shows up when a large bitmap is matched against a
     * segment holding a narrow, high slice of the value space — the shape monotonically increasing
     * ids produce.
     */
    private static final class LongValuesIterator implements PeekableIterator {

        private final LongIterator values;
        private long next;
        private boolean hasNext;

        LongValuesIterator(LongIterator values) {
            this.values = values;
            fetch();
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public long peek() {
            return next;
        }

        @Override
        public void encodePeek(byte[] dest) {
            NumericUtils.longToSortableBytes(next, dest, 0);
        }

        @Override
        public long next() {
            long value = next;
            fetch();
            return value;
        }

        /**
         * Compares as signed, which is only equivalent to the bitmap's unsigned order when every
         * value is non-negative — callers must have rejected bitmaps failing
         * {@link LongBitmap#hasNegativeValues()}. Under that precondition a negative {@code target}
         * is below every remaining value and correctly advances nothing.
         * <p>
         * This walks values one at a time because nothing in {@link Roaring64NavigableMap}'s public API
         * can position an iterator: {@link LongIterator} has no {@code advanceIfNeeded}, there is no
         * {@code getLongIteratorFrom}, and the per-bucket 32-bit bitmaps that could seek are behind a
         * package-private accessor. Note that {@code rankLong}/{@code select} <em>do</em> binary-search
         * the buckets, and are pure reads once the constructor has populated the cumulative caches —
         * but they only locate a value, so driving iteration through them would make every sequential
         * step O(log buckets), which costs a merge scan more than the walk it replaces.
         * <p>
         * <b>If a future RoaringBitmap release gives {@link Roaring64NavigableMap} a peekable iterator
         * or a {@code getLongIteratorFrom(long)}, or exposes its buckets, rewrite this method to use
         * it</b> — everything else here can stay as it is, and {@link IntBitmap}'s iterator shows the
         * shape to copy.
         */
        @Override
        public void advanceTo(long target) {
            while (hasNext && next < target) {
                fetch();
            }
        }

        private void fetch() {
            hasNext = values.hasNext();
            if (hasNext) {
                next = values.next();
            }
        }
    }

    /**
     * The bitmap dominates this object's footprint and can reach tens of megabytes. Reporting it lets
     * the query cache account for a cached entry's true size instead of treating it as negligible.
     */
    @Override
    public long ramBytesUsed() {
        return ramBytesUsed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof LongBitmap other) {
            // Comparing the bitmaps walks every bucket, so rule out the common unequal case first
            // using the values already computed at construction.
            if (cardinality != other.cardinality || first != other.first || last != other.last) {
                return false;
            }
            return bitmap.equals(other.bitmap);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        if (isEmpty()) {
            return "LongBitmap(cardinality=0)";
        }
        return "LongBitmap(cardinality=" + cardinality + ", first=" + first + ", last=" + last + ")";
    }
}
