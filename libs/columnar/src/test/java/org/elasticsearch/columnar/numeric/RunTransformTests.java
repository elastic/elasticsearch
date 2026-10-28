/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

/**
 * Describing a block by the runs it is made of. Whatever is decided, the values have to come back exactly,
 * so every case here checks the round trip.
 */
public class RunTransformTests extends ESTestCase {

    private static final int BLOCK = 128;
    private final RunTransform transform = new RunTransform(BLOCK);

    /** The shape this exists for: a column ordered by its own values, seen a block at a time. */
    public void testTwoRuns() throws IOException {
        final long[] block = new long[BLOCK];
        Arrays.fill(block, 0, 100, 7L);
        Arrays.fill(block, 100, BLOCK, 8L);
        assertDescribed(block, BLOCK);
    }

    /** Run values far apart, which bit-packing would spend the whole width on. */
    public void testTwoDistantRuns() throws IOException {
        final long[] block = new long[BLOCK];
        Arrays.fill(block, 0, 100, 7L);
        Arrays.fill(block, 100, BLOCK, 1L << 40);
        assertDescribed(block, BLOCK);
    }

    public void testThreeRuns() throws IOException {
        final long[] block = new long[BLOCK];
        Arrays.fill(block, 0, 40, 3L);
        Arrays.fill(block, 40, 90, 250L);
        Arrays.fill(block, 90, BLOCK, 17L);
        assertDescribed(block, BLOCK);
    }

    /** One run is what the offset already reduces to nothing, so this leaves it alone. */
    public void testSingleRunIsLeftAlone() throws IOException {
        final long[] block = new long[BLOCK];
        Arrays.fill(block, 42L);
        assertLeftAlone(block, BLOCK);
    }

    /** Values that never repeat are not runs. */
    public void testAllDistinctIsLeftAlone() throws IOException {
        final long[] block = new long[BLOCK];
        for (int i = 0; i < BLOCK; i++) {
            block[i] = i;
        }
        assertLeftAlone(block, BLOCK);
    }

    /** The premise: runs must cover enough values on average for the block to be made of them. */
    public void testTheBoundOnHowShortRunsMayBe() throws IOException {
        for (int runs : new int[] { BLOCK / RunTransform.MIN_RUN, BLOCK / RunTransform.MIN_RUN + 1 }) {
            final long[] block = new long[BLOCK];
            final int length = BLOCK / runs;
            for (int r = 0; r < runs; r++) {
                // Values far apart, so packing is expensive and only the premise can refuse the block.
                Arrays.fill(block, r * length, r == runs - 1 ? BLOCK : (r + 1) * length, (long) r << 40);
            }
            final long[] copy = block.clone();
            final MetadataBuffer params = new MetadataBuffer();
            final boolean fired = transform.tryEncode(block, BLOCK, params);
            assertEquals("runs=" + runs, runs <= BLOCK / RunTransform.MIN_RUN, fired);
            if (fired) {
                assertRoundTrip(copy, block, BLOCK, params);
            }
        }
    }

    /** Few runs, but so cheap to pack that describing them costs more. */
    public void testRunsThatCostMoreThanPackingAreLeftAlone() throws IOException {
        final long[] block = new long[BLOCK];
        for (int i = 0; i < BLOCK; i++) {
            // Thirty-two runs of one bit each: the terminal packs the block for less than the runs cost.
            block[i] = (i / 4) % 2;
        }
        assertLeftAlone(block, BLOCK);
    }

    /** Runs of negative values, which this describes as readily as any other. */
    public void testNegativeRuns() throws IOException {
        final long[] block = new long[BLOCK];
        Arrays.fill(block, 0, 64, -5_000_000_000L);
        Arrays.fill(block, 64, BLOCK, 7L);
        assertDescribed(block, BLOCK);
    }

    /** The extremes, where the delta between two run values overflows a signed long. */
    public void testExtremeRunValues() throws IOException {
        final long[] block = new long[BLOCK];
        Arrays.fill(block, 0, 64, Long.MIN_VALUE);
        Arrays.fill(block, 64, BLOCK, Long.MAX_VALUE);
        assertDescribed(block, BLOCK);
    }

    /** A partial last block, which the encoder never pads. */
    public void testPartialBlock() throws IOException {
        final long[] block = new long[BLOCK];
        Arrays.fill(block, 0, 5, 3L);
        Arrays.fill(block, 5, 11, 900L);
        assertDescribed(block, 11);
    }

    /** Values beyond {@code valueCount} are none of the transform's business. */
    public void testTailBeyondValueCountIsUntouched() throws IOException {
        final long[] block = new long[BLOCK];
        Arrays.fill(block, 0, 20, 3L);
        Arrays.fill(block, 20, 40, 900L);
        Arrays.fill(block, 40, BLOCK, 777L);
        final long[] copy = block.clone();
        final MetadataBuffer params = new MetadataBuffer();
        assertTrue(transform.tryEncode(block, 40, params));
        for (int i = 40; i < BLOCK; i++) {
            assertEquals("tail at " + i, 777L, block[i]);
        }
        transform.decode(block, 40, DataInputMetadataReader.wrap(params));
        assertArrayEquals(copy, block);
    }

    public void testSingleValueBlockIsLeftAlone() throws IOException {
        assertLeftAlone(new long[] { 5L }, 1);
    }

    /** Whatever the block holds, it comes back. */
    public void testRandomBlocks() throws IOException {
        for (int iter = 0; iter < 500; iter++) {
            final long[] block = new long[BLOCK];
            final int count = between(1, BLOCK);
            int at = 0;
            while (at < count) {
                final long value = randomBoolean() ? randomLongBetween(0, 8) : randomLong();
                final int length = Math.min(between(1, randomBoolean() ? 3 : 40), count - at);
                Arrays.fill(block, at, at + length, value);
                at += length;
            }
            final long[] copy = block.clone();
            final MetadataBuffer params = new MetadataBuffer();
            if (transform.tryEncode(block, count, params)) {
                assertRoundTrip(copy, block, count, params);
            } else {
                assertArrayEquals("an untouched block", copy, block);
            }
        }
    }

    /** Params are round-tripped through bytes, so a mis-sized read cannot pass by reading its own leftovers. */
    private void assertRoundTrip(long[] original, long[] encoded, int valueCount, MetadataBuffer params) throws IOException {
        for (int i = 0; i < valueCount; i++) {
            assertEquals("a described block is packed at no width, position " + i, 0L, encoded[i]);
        }
        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        params.writeTo(out);
        final ByteArrayDataInput in = new ByteArrayDataInput(out.toArrayCopy());
        final DataInputMetadataReader onDisk = new DataInputMetadataReader();
        onDisk.reset(in);
        transform.decode(encoded, valueCount, onDisk);
        for (int i = 0; i < valueCount; i++) {
            assertEquals("value " + i, original[i], encoded[i]);
        }
        assertTrue("every param byte was read", in.eof());
    }

    private void assertDescribed(long[] block, int valueCount) throws IOException {
        final long[] copy = block.clone();
        final MetadataBuffer params = new MetadataBuffer();
        assertTrue("expected the runs to be described", transform.tryEncode(block, valueCount, params));
        assertRoundTrip(copy, block, valueCount, params);
    }

    private void assertLeftAlone(long[] block, int valueCount) throws IOException {
        final long[] copy = block.clone();
        final MetadataBuffer params = new MetadataBuffer();
        assertFalse("expected the block to be left alone", transform.tryEncode(block, valueCount, params));
        assertArrayEquals("an untouched block", copy, block);
        assertEquals("nothing written", 0, params.size());
    }
}
