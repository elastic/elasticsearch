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
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.Matchers.lessThan;

/**
 * Setting a block's widest values aside so the terminal packs at the width the rest need. Whatever is
 * decided, the values have to come back exactly, so every case here checks the round trip.
 */
public class PatchedTransformTests extends ESTestCase {

    private static final int BLOCK = 128;
    private final PatchedTransform transform = new PatchedTransform();

    /** The shape this exists for: one value repeated, and a single much wider one beside it. */
    public void testOneWideValueAmongMany() throws IOException {
        final long[] block = filled(1L);
        block[57] = 399;
        assertKept(block, BLOCK);
    }

    /** Several set aside, which is still cheaper than widening every value. */
    public void testSeveralWideValues() throws IOException {
        final long[] block = filled(3L);
        for (int i = 0; i < 8; i++) {
            block[i * 7] = 1L << 40;
        }
        assertKept(block, BLOCK);
    }

    /** Values all of a size have no common case to pack for. */
    public void testUniformBlockIsLeftAlone() throws IOException {
        final long[] block = new long[BLOCK];
        for (int i = 0; i < BLOCK; i++) {
            block[i] = 1000 + i;
        }
        assertLeftAlone(block, BLOCK);
    }

    /** Half the block wide is two populations, not a common case and its exceptions. */
    public void testHalfWideIsLeftAlone() throws IOException {
        final long[] block = new long[BLOCK];
        for (int i = 0; i < BLOCK; i++) {
            block[i] = i % 2 == 0 ? 1 : 1L << 30;
        }
        assertLeftAlone(block, BLOCK);
    }

    /** One short of half is exceptional, one over is not, whatever the saving would have been. */
    public void testTheBoundOnWhatCountsAsExceptional() throws IOException {
        for (int wide : new int[] { BLOCK / 2 - 1, BLOCK / 2 }) {
            final long[] block = filled(0L);
            for (int i = 0; i < wide; i++) {
                block[i] = 1L << 20;
            }
            final long[] copy = block.clone();
            final MetadataBuffer params = new MetadataBuffer();
            assertEquals("wide=" + wide, wide < BLOCK / 2, transform.tryEncode(block, BLOCK, params));
            if (wide < BLOCK / 2) {
                assertRoundTrip(copy, block, BLOCK, params);
            }
        }
    }

    /** A block the pipeline has not offset can hold negatives, which no narrower width holds. */
    public void testNegativesAreLeftAlone() throws IOException {
        final long[] block = filled(1L);
        block[3] = -5;
        assertLeftAlone(block, BLOCK);
    }

    public void testAllZerosIsLeftAlone() throws IOException {
        assertLeftAlone(new long[BLOCK], BLOCK);
    }

    /** The widest value there is, which is the vlong the exception costs most to record. */
    public void testMaxLongIsSetAside() throws IOException {
        final long[] block = filled(1L);
        block[9] = Long.MAX_VALUE;
        assertKept(block, BLOCK);
    }

    /** A partial last block, which the encoder never pads. */
    public void testPartialBlock() throws IOException {
        final long[] block = filled(2L);
        block[5] = 1L << 33;
        assertKept(block, 11);
    }

    /** A block of one value, where nothing can be exceptional. */
    public void testSingleValueBlock() throws IOException {
        final long[] block = filled(0L);
        block[0] = 1L << 40;
        assertLeftAlone(block, 1);
    }

    /**
     * Above 24 bits the terminal rounds the width it packs at, so narrowing within a rounding step buys
     * nothing and must not be paid for. A block of 40-bit values with wider ones beside them still packs at
     * 40 bits either way.
     */
    public void testNarrowingWithinARoundingStepIsNotWorthIt() throws IOException {
        final long[] block = filled((1L << 33) - 1);
        block[1] = (1L << 39) - 1;
        final long[] copy = block.clone();
        final MetadataBuffer params = new MetadataBuffer();
        if (transform.tryEncode(block, BLOCK, params)) {
            // Firing is allowed, but only if it really reached a narrower rounded width.
            long max = 0;
            for (int i = 0; i < BLOCK; i++) {
                max = Math.max(max, block[i]);
            }
            assertThat(
                "fired without reaching a narrower packed width",
                DocValuesForUtil.roundBits(PackedInts.unsignedBitsRequired(max)),
                lessThan(DocValuesForUtil.roundBits(PackedInts.unsignedBitsRequired((1L << 39) - 1)))
            );
            assertRoundTrip(copy, block, BLOCK, params);
        }
    }

    /** Values beyond {@code valueCount} are none of the transform's business and must not be touched. */
    public void testTailBeyondValueCountIsUntouched() throws IOException {
        final long[] block = filled(1L);
        block[3] = 1L << 40;
        Arrays.fill(block, 40, BLOCK, 777L);
        final long[] copy = block.clone();
        final MetadataBuffer params = new MetadataBuffer();
        assertTrue(transform.tryEncode(block, 40, params));
        for (int i = 40; i < BLOCK; i++) {
            assertEquals("tail at " + i, 777L, block[i]);
        }
        transform.decode(block, 40, reader(params));
        assertArrayEquals(copy, block);
    }

    /** Whatever the block holds, it comes back. */
    public void testRandomBlocks() throws IOException {
        for (int iter = 0; iter < 500; iter++) {
            final long[] block = new long[BLOCK];
            final int count = between(1, BLOCK);
            final long common = randomBoolean() ? 0 : randomLongBetween(0, 1L << between(0, 20));
            for (int i = 0; i < count; i++) {
                block[i] = rarely() ? randomLongBetween(0, Long.MAX_VALUE) : common;
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

    /** Params written by one block must not be read by the next, so the buffer is round-tripped as bytes. */
    private void assertRoundTrip(long[] original, long[] encoded, int valueCount, MetadataBuffer params) throws IOException {
        for (int i = 0; i < valueCount; i++) {
            assertTrue("a value set aside is packed narrower at " + i, encoded[i] <= original[i]);
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

    private void assertKept(long[] block, int valueCount) throws IOException {
        final long[] copy = block.clone();
        final MetadataBuffer params = new MetadataBuffer();
        assertTrue("expected the block to be narrowed", transform.tryEncode(block, valueCount, params));
        assertRoundTrip(copy, block, valueCount, params);
    }

    private void assertLeftAlone(long[] block, int valueCount) throws IOException {
        final long[] copy = block.clone();
        final MetadataBuffer params = new MetadataBuffer();
        assertFalse("expected the block to be left alone", transform.tryEncode(block, valueCount, params));
        assertArrayEquals("an untouched block", copy, block);
        assertEquals("nothing written", 0, params.size());
    }

    private static MetadataReader reader(MetadataBuffer params) {
        return DataInputMetadataReader.wrap(params);
    }

    private static long[] filled(long value) {
        final long[] block = new long[BLOCK];
        Arrays.fill(block, value);
        return block;
    }
}
