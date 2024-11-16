/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.blobcache;

import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;

import java.io.EOFException;
import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.equalTo;

public class BlobCacheUtilsTests extends ESTestCase {

    public void testReadSafeThrows() {
        final ByteBuffer buffer = ByteBuffer.allocate(randomIntBetween(1, 1025));
        final int remaining = randomIntBetween(1, 1025);
        expectThrows(EOFException.class, () -> BlobCacheUtils.readSafe(BytesArray.EMPTY.streamInput(), buffer, 0, remaining));
    }

    public void testToPageAlignedSize() {
        long value = randomLongBetween(0, Long.MAX_VALUE - SharedBytes.PAGE_SIZE);
        long expected = ((value - 1) / SharedBytes.PAGE_SIZE + 1) * SharedBytes.PAGE_SIZE;
        assertThat(BlobCacheUtils.toPageAlignedSize(value), equalTo(expected));
        assertThat(BlobCacheUtils.toPageAlignedSize(value), equalTo(roundUpUsingRemainder(value, SharedBytes.PAGE_SIZE)));
    }

    public void testRoundUpToAlignment() {
        assertThat(BlobCacheUtils.roundUpToAlignedSize(8, 4), equalTo(8L));
        assertThat(BlobCacheUtils.roundUpToAlignedSize(9, 4), equalTo(12L));
        assertThat(BlobCacheUtils.roundUpToAlignedSize(between(1, 4), 4), equalTo(4L));
        long alignment = randomLongBetween(1, Long.MAX_VALUE / 2);
        assertThat(BlobCacheUtils.roundUpToAlignedSize(0, alignment), equalTo(0L));
        long value = randomLongBetween(0, Long.MAX_VALUE - alignment);
        assertThat(BlobCacheUtils.roundUpToAlignedSize(value, alignment), equalTo(roundUpUsingRemainder(value, alignment)));
    }

    public void testRoundDownToAlignment() {
        assertThat(BlobCacheUtils.roundDownToAlignedSize(8, 4), equalTo(8L));
        assertThat(BlobCacheUtils.roundDownToAlignedSize(9, 4), equalTo(8L));
        assertThat(BlobCacheUtils.roundDownToAlignedSize(between(0, 3), 4), equalTo(0L));
        long alignment = randomLongBetween(1, Long.MAX_VALUE / 2);
        long value = randomLongBetween(0, Long.MAX_VALUE);
        assertThat(BlobCacheUtils.roundDownToAlignedSize(value, alignment), equalTo(roundDownUsingRemainder(value, alignment)));
    }

    public void testComputeRange() {
        assertThat(BlobCacheUtils.computeRange(8, 8, 8), equalTo(ByteRange.of(8, 16)));
        assertThat(BlobCacheUtils.computeRange(8, 9, 8), equalTo(ByteRange.of(8, 24)));
        assertThat(BlobCacheUtils.computeRange(8, 8, 9), equalTo(ByteRange.of(8, 24)));

        long large = randomLongBetween(24, 64);
        assertThat(BlobCacheUtils.computeRange(8, 8, 8, large), equalTo(ByteRange.of(8, 16)));
        assertThat(BlobCacheUtils.computeRange(8, 9, 8, large), equalTo(ByteRange.of(8, 24)));
        assertThat(BlobCacheUtils.computeRange(8, 8, 9, large), equalTo(ByteRange.of(8, 24)));

        long small = randomLongBetween(8, 16);
        assertThat(BlobCacheUtils.computeRange(8, 8, 8, small), equalTo(ByteRange.of(8, small)));
        assertThat(BlobCacheUtils.computeRange(8, 9, 8, small), equalTo(ByteRange.of(8, small)));
        assertThat(BlobCacheUtils.computeRange(8, 8, 9, small), equalTo(ByteRange.of(8, small)));
    }

    private static long roundUpUsingRemainder(long value, long alignment) {
        long remainder = value % alignment;
        if (remainder > 0L) {
            return value + (alignment - remainder);
        }
        return value;
    }

    private static long roundDownUsingRemainder(long value, long alignment) {
        long remainder = value % alignment;
        if (remainder > 0L) {
            return value - remainder;
        }
        return value;
    }
}
