/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

/**
 * Directly round-trips each {@link BlockTransform} stage ({@link DeltaTransform}, {@link OffsetTransform},
 * {@link GcdTransform}, {@link SplitDeltaTransform}, {@link AlpDoubleTransform}) over a range of block
 * shapes: {@code tryEncode} into a params buffer, then {@code decode} back, asserting the block is
 * restored exactly. When a stage declines ({@code tryEncode} returns {@code false}) the block and its
 * params must be left untouched.
 */
public class BlockTransformTests extends ESTestCase {

    private static final int BLOCK = 128;

    private static BlockTransform[] stages() {
        return new BlockTransform[] {
            DeltaTransform.INSTANCE,
            OffsetTransform.INSTANCE,
            GcdTransform.INSTANCE,
            new SplitDeltaTransform(),
            new AlpDoubleTransform(BLOCK) };
    }

    public void testConstant() throws IOException {
        assertAllStages(filled(0L));
        assertAllStages(filled(42L));
        assertAllStages(filled(-7L));
    }

    public void testMonotonicAscending() throws IOException {
        long[] block = new long[BLOCK];
        long v = randomLong() >> 2;
        for (int i = 0; i < BLOCK; i++) {
            block[i] = v;
            v += between(0, 1000);
        }
        assertAllStages(block);
    }

    public void testMonotonicDescending() throws IOException {
        long[] block = new long[BLOCK];
        long v = randomLong() >> 2;
        for (int i = 0; i < BLOCK; i++) {
            block[i] = v;
            v -= between(0, 1000);
        }
        assertAllStages(block);
    }

    public void testGcdFriendly() throws IOException {
        long[] block = new long[BLOCK];
        for (int i = 0; i < BLOCK; i++) {
            block[i] = 1024L * between(-500, 500); // power-of-two GCD exercises the shift fast path
        }
        assertAllStages(block);
        for (int i = 0; i < BLOCK; i++) {
            block[i] = 1000L * between(-500, 500); // non-power-of-two GCD exercises the divide path
        }
        assertAllStages(block);
    }

    public void testNegatives() throws IOException {
        long[] block = new long[BLOCK];
        for (int i = 0; i < BLOCK; i++) {
            block[i] = -between(1, 1_000_000);
        }
        assertAllStages(block);
        // negative, monotonic ascending toward zero
        long v = -1_000_000L;
        for (int i = 0; i < BLOCK; i++) {
            block[i] = v;
            v += between(0, 5000);
        }
        assertAllStages(block);
    }

    public void testRandom() throws IOException {
        for (int iter = 0; iter < 50; iter++) {
            long[] block = new long[BLOCK];
            for (int i = 0; i < BLOCK; i++) {
                block[i] = switch (between(0, 3)) {
                    case 0 -> randomLong();
                    case 1 -> between(-1000, 1000);
                    case 2 -> Long.MIN_VALUE;
                    default -> Long.MAX_VALUE;
                };
            }
            assertAllStages(block);
        }
    }

    public void testSortableDoubles() throws IOException {
        // Sortable longs encoding IEEE doubles with a one-decimal stride: ALP fires and round-trips.
        long[] block = new long[BLOCK];
        double base = 20.0 + randomDoubleBetween(0.0, 5.0, true);
        for (int i = 0; i < BLOCK; i++) {
            block[i] = NumericUtils.doubleToSortableLong(base + i * 0.1);
        }
        assertAllStages(block);
    }

    private void assertAllStages(long[] original) throws IOException {
        for (BlockTransform stage : stages()) {
            // Full block, then a partial count: the stage must fit and round-trip only the real prefix,
            // ignoring the padded tail.
            assertStageRoundTrip(stage, original, original.length);
            assertStageRoundTrip(stage, original, Math.max(1, original.length / 2));
        }
    }

    private static void assertStageRoundTrip(BlockTransform stage, long[] original, int valueCount) throws IOException {
        String name = stage.getClass().getSimpleName() + "[valueCount=" + valueCount + "]";
        long[] work = original.clone();
        MetadataBuffer params = new MetadataBuffer();
        boolean fired = stage.tryEncode(work, valueCount, params);
        if (fired) {
            long[] decoded = work.clone();
            stage.decode(decoded, valueCount, DataInputMetadataReader.wrap(params));
            for (int i = 0; i < valueCount; i++) {
                assertEquals(name + " must round-trip value " + i + " when it fires", original[i], decoded[i]);
            }
        } else {
            // A stage that declines leaves the real prefix and the params buffer untouched.
            for (int i = 0; i < valueCount; i++) {
                assertEquals(name + " must not mutate value " + i + " when it declines", original[i], work[i]);
            }
            assertEquals(name + " must not write params when it declines", 0L, params.size());
        }
    }

    private static long[] filled(long value) {
        long[] block = new long[BLOCK];
        Arrays.fill(block, value);
        return block;
    }
}
