/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.bitmapterms;

import org.elasticsearch.test.ESTestCase;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;

public class LongBitmapTests extends ESTestCase {

    /**
     * Produces the wire bytes with the library's own portable writer rather than a hand-rolled
     * encoder, so every test here doubles as an interop check against the reference implementation
     * of the format that CRoaring, GoRoaring and pyroaring also speak.
     */
    private static byte[] serializePortable(long... values) throws IOException {
        Roaring64NavigableMap bitmap = new Roaring64NavigableMap();
        bitmap.add(values);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            bitmap.serializePortable(out);
        }
        return bytes.toByteArray();
    }

    /**
     * Writes the portable envelope by hand so a test can supply bucket contents the library's own
     * writer never produces &mdash; specifically an empty bucket.
     */
    private static byte[] portableBytes(int[] highKeys, RoaringBitmap[] lowBitmaps) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeLong(Long.reverseBytes(highKeys.length));
            for (int i = 0; i < highKeys.length; i++) {
                out.writeInt(Integer.reverseBytes(highKeys[i]));
                lowBitmaps[i].serialize(out);
            }
        }
        return bytes.toByteArray();
    }

    private static List<Long> drain(LongBitmap.PeekableIterator iterator) {
        List<Long> values = new ArrayList<>();
        while (iterator.hasNext()) {
            values.add(iterator.next());
        }
        return values;
    }

    public void testDeserializeRoundTrip() throws IOException {
        // Spans several high-32-bit buckets, including values well beyond the 32-bit range
        long[] values = { 0L, 1L, 42L, Integer.MAX_VALUE, 1L << 32, (1L << 32) + 7, 1L << 33, Long.MAX_VALUE };
        LongBitmap bitmap = LongBitmap.deserializePortable(serializePortable(values));

        assertThat(bitmap.isEmpty(), equalTo(false));
        assertThat(bitmap.cardinality(), equalTo((long) values.length));
        assertThat(bitmap.first(), equalTo(0L));
        assertThat(bitmap.last(), equalTo(Long.MAX_VALUE));
        assertThat(bitmap.hasNegativeValues(), equalTo(false));
        assertThat(drain(bitmap.iterator()), equalTo(Arrays.stream(values).boxed().toList()));
    }

    public void testDeserializeEmpty() throws IOException {
        LongBitmap bitmap = LongBitmap.deserializePortable(serializePortable());
        assertThat(bitmap.isEmpty(), equalTo(true));
        assertThat(bitmap.cardinality(), equalTo(0L));
        assertThat(bitmap.hasNegativeValues(), equalTo(false));
        assertThat(bitmap.iterator().hasNext(), equalTo(false));
    }

    public void testDeserializeRandomValues() throws IOException {
        long[] values = randomLongsOfLength(randomIntBetween(1, 500));
        LongBitmap bitmap = LongBitmap.deserializePortable(serializePortable(values));
        long[] expected = Arrays.stream(values).distinct().sorted().toArray();
        assertThat(bitmap.cardinality(), equalTo((long) expected.length));
        assertThat(drain(bitmap.iterator()), equalTo(Arrays.stream(expected).boxed().toList()));
    }

    private static long[] randomLongsOfLength(int length) {
        long[] values = new long[length];
        for (int i = 0; i < length; i++) {
            // Non-negative, but deliberately spread across many high-32-bit buckets
            values[i] = randomLongBetween(0, Long.MAX_VALUE);
        }
        return values;
    }

    /** Bytes arrive from a search request, so a valid bitmap followed by junk must be rejected. */
    public void testTrailingBytesRejected() throws IOException {
        byte[] valid = serializePortable(1L, 2L, 3L);
        byte[] withJunk = Arrays.copyOf(valid, valid.length + 3);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> LongBitmap.deserializePortable(withJunk));
        assertThat(e.getMessage(), containsString("trailing byte"));
    }

    public void testTruncatedBytesRejected() throws IOException {
        byte[] valid = serializePortable(1L, 1L << 32);
        byte[] truncated = Arrays.copyOf(valid, valid.length - 4);
        expectThrows(IOException.class, () -> LongBitmap.deserializePortable(truncated));
    }

    /**
     * The portable format orders values as unsigned, so a value with its sign bit set sorts last
     * even though it is a negative signed long. The merge-scan queries compare against signed
     * values from the index, so they must be able to detect this and refuse.
     */
    public void testHasNegativeValues() throws IOException {
        LongBitmap negative = LongBitmap.deserializePortable(serializePortable(5L, -1L));
        assertThat(negative.hasNegativeValues(), equalTo(true));
        assertThat(negative.last(), equalTo(-1L));

        LongBitmap nonNegative = LongBitmap.deserializePortable(serializePortable(5L, Long.MAX_VALUE));
        assertThat(nonNegative.hasNegativeValues(), equalTo(false));
    }

    public void testIteratorPeekDoesNotConsume() {
        LongBitmap.PeekableIterator iterator = LongBitmap.bitmapOf(7L, 9L).iterator();
        assertThat(iterator.peek(), equalTo(7L));
        assertThat(iterator.peek(), equalTo(7L));
        assertThat(iterator.next(), equalTo(7L));
        assertThat(iterator.peek(), equalTo(9L));
        assertThat(iterator.next(), equalTo(9L));
        assertThat(iterator.hasNext(), equalTo(false));
    }

    public void testIteratorAdvanceTo() {
        // Deliberately straddles bucket boundaries so advanceTo has to cross them
        LongBitmap bitmap = LongBitmap.bitmapOf(1L, 5L, 1L << 32, (1L << 32) + 10, 1L << 34);

        LongBitmap.PeekableIterator iterator = bitmap.iterator();
        iterator.advanceTo(5L);
        assertThat(iterator.next(), equalTo(5L));

        iterator = bitmap.iterator();
        iterator.advanceTo(1L << 32);
        assertThat(iterator.next(), equalTo(1L << 32));

        // Target between two values lands on the next one up
        iterator = bitmap.iterator();
        iterator.advanceTo((1L << 32) + 1);
        assertThat(iterator.next(), equalTo((1L << 32) + 10));

        // Target past the last value exhausts the iterator
        iterator = bitmap.iterator();
        iterator.advanceTo((1L << 34) + 1);
        assertThat(iterator.hasNext(), equalTo(false));
    }

    /** advanceTo must never rewind, and must be a no-op for a target at or below the current value. */
    public void testIteratorAdvanceToIsMonotonic() {
        LongBitmap.PeekableIterator iterator = LongBitmap.bitmapOf(10L, 20L, 30L).iterator();
        iterator.advanceTo(20L);
        assertThat(iterator.peek(), equalTo(20L));
        iterator.advanceTo(5L);
        assertThat(iterator.peek(), equalTo(20L));
        iterator.advanceTo(Long.MIN_VALUE);
        assertThat(iterator.peek(), equalTo(20L));
        assertThat(iterator.next(), equalTo(20L));
    }

    public void testIteratorsAreIndependent() {
        LongBitmap bitmap = LongBitmap.bitmapOf(1L, 2L, 3L);
        LongBitmap.PeekableIterator first = bitmap.iterator();
        LongBitmap.PeekableIterator second = bitmap.iterator();
        assertThat(first.next(), equalTo(1L));
        assertThat(second.next(), equalTo(1L));
        first.advanceTo(3L);
        assertThat(second.peek(), equalTo(2L));
    }

    /**
     * {@code Roaring64NavigableMap#getLongCardinality} is a documented mutator, so the wrapper reads
     * it once during construction. Repeated reads of the derived values must stay stable, and iterators
     * taken after them must still see every value.
     */
    public void testDerivedValuesAreStable() throws IOException {
        LongBitmap bitmap = LongBitmap.deserializePortable(serializePortable(3L, 1L << 32, Long.MAX_VALUE));
        for (int i = 0; i < 3; i++) {
            assertThat(bitmap.cardinality(), equalTo(3L));
            assertThat(bitmap.first(), equalTo(3L));
            assertThat(bitmap.last(), equalTo(Long.MAX_VALUE));
            assertThat(bitmap.ramBytesUsed(), greaterThan(0L));
        }
        assertThat(drain(bitmap.iterator()), equalTo(List.of(3L, 1L << 32, Long.MAX_VALUE)));
    }

    /**
     * The portable format permits a bucket holding an empty 32-bit bitmap. Reading the cardinality is
     * what prunes those, so it has to happen before {@code first()}/{@code last()} &mdash; otherwise the
     * leading empty bucket is what those read, and it has no first or last value. Guards the ordering
     * inside the constructor.
     */
    public void testEmptyBucketAheadOfNonEmptyBucket() throws IOException {
        byte[] bytes = portableBytes(new int[] { 0, 1 }, new RoaringBitmap[] { new RoaringBitmap(), RoaringBitmap.bitmapOf(5) });

        LongBitmap bitmap = LongBitmap.deserializePortable(bytes);
        long expected = (1L << 32) + 5;
        assertThat(bitmap.cardinality(), equalTo(1L));
        assertThat(bitmap.isEmpty(), equalTo(false));
        assertThat(bitmap.first(), equalTo(expected));
        assertThat(bitmap.last(), equalTo(expected));
        assertThat(drain(bitmap.iterator()), equalTo(List.of(expected)));
    }

    public void testEqualsAndHashCode() {
        LongBitmap a = LongBitmap.bitmapOf(1L, 1L << 32);
        LongBitmap b = LongBitmap.bitmapOf(1L, 1L << 32);
        LongBitmap c = LongBitmap.bitmapOf(1L);
        assertThat(a, equalTo(b));
        assertThat(a.hashCode(), equalTo(b.hashCode()));
        assertThat(a, not(equalTo(c)));
    }

    public void testRamBytesUsedGrowsWithBitmap() {
        long[] many = new long[100_000];
        for (int i = 0; i < many.length; i++) {
            many[i] = i;
        }
        long smallBytes = LongBitmap.bitmapOf(1L, 2L, 3L).ramBytesUsed();
        long largeBytes = LongBitmap.bitmapOf(many).ramBytesUsed();
        assertThat(smallBytes, greaterThan(0L));
        assertThat(largeBytes, greaterThan(smallBytes));
    }

    public void testToString() {
        assertThat(LongBitmap.bitmapOf().toString(), containsString("cardinality=0"));
        String description = LongBitmap.bitmapOf(1L, 1L << 33).toString();
        assertThat(description, containsString("cardinality=2"));
        assertThat(description, containsString("first=1"));
        assertThat(description, containsString("last=" + (1L << 33)));
    }

}
