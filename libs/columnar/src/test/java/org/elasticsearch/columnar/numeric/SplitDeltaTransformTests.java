/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

/**
 * Round-trip tests for {@link SplitDeltaTransform} over a range of block shapes including
 * piecewise-monotonic sequences typical of TSDB timestamp blocks crossing {@code _tsid} boundaries.
 */
public class SplitDeltaTransformTests extends ESTestCase {

    private static final int BLOCK = 128;

    private final SplitDeltaTransform stage = new SplitDeltaTransform();

    public void testFullyMonotonicDeclines() throws IOException {
        // A strictly monotonic block must be left to DeltaTransform, not consumed by SplitDelta.
        long[] block = new long[BLOCK];
        long v = 1_700_000_000_000L;
        for (int i = 0; i < BLOCK; i++) {
            block[i] = v;
            v += between(1, 1000);
        }
        assertDeclines(stage, block, BLOCK);
    }

    public void testConstantDeclines() throws IOException {
        assertDeclines(stage, filled(42L), BLOCK);
    }

    public void testOneSplit() throws IOException {
        // Classic TSDB boundary: one descending run followed by one ascending run.
        long[] block = new long[BLOCK];
        long ts = 1_700_000_100_000L;
        int boundary = between(4, BLOCK - 4);
        // first sub-run: descending (timestamps within a _tsid go desc in this sort order)
        for (int i = 0; i < boundary; i++) {
            block[i] = ts;
            ts -= between(1, 500);
        }
        // second sub-run: ascending (new _tsid, higher timestamp start)
        ts += between(10_000, 1_000_000);
        for (int i = boundary; i < BLOCK; i++) {
            block[i] = ts;
            ts += between(1, 500);
        }
        assertRoundTrip(stage, block, BLOCK);
    }

    public void testMultipleSplits() throws IOException {
        // Three monotonic sub-runs.
        long[] block = new long[BLOCK];
        long v = 1_000_000L;
        int cut1 = 32;
        int cut2 = 80;
        for (int i = 0; i < cut1; i++) {
            block[i] = v;
            v -= between(1, 100);
        }
        v += 1_000_000L;
        for (int i = cut1; i < cut2; i++) {
            block[i] = v;
            v -= between(1, 100);
        }
        v += 1_000_000L;
        for (int i = cut2; i < BLOCK; i++) {
            block[i] = v;
            v -= between(1, 100);
        }
        assertRoundTrip(stage, block, BLOCK);
    }

    public void testPartialBlock() throws IOException {
        // tryEncode must operate only on block[0..valueCount) and ignore padding.
        long[] block = new long[BLOCK];
        int boundary = 30;
        long ts = 1_700_000_100_000L;
        for (int i = 0; i < boundary; i++) {
            block[i] = ts;
            ts -= between(1, 200);
        }
        ts += 500_000L;
        int valueCount = 60;
        for (int i = boundary; i < valueCount; i++) {
            block[i] = ts;
            ts -= between(1, 200);
        }
        // Poison the padding to confirm the stage ignores it.
        Arrays.fill(block, valueCount, BLOCK, Long.MIN_VALUE);
        assertRoundTrip(stage, block, valueCount);
    }

    public void testTooFewValuesToDecode() throws IOException {
        // Blocks shorter than 4 must be declined.
        for (int n = 1; n < 4; n++) {
            long[] block = new long[BLOCK];
            block[0] = 100;
            block[1] = 90;
            block[2] = 200;
            assertDeclines(stage, block, n);
        }
    }

    public void testTsdbTimestampShape() throws IOException {
        // TSDB sort order: [_tsid asc, @timestamp desc]. Within each _tsid, timestamps decrease.
        // Between _tsid boundaries, timestamps jump UP (the new series starts at a higher base).
        // The block therefore looks like: DOWN, UP, DOWN, UP, ... — globally non-monotonic.
        // SplitDelta must fire; DeltaTransform alone would not because the block has mixed directions.
        int numSeries = between(2, 4);
        int docsPerSeries = BLOCK / numSeries;
        long[] block = new long[BLOCK];
        int pos = 0;
        for (int s = 0; s < numSeries && pos < BLOCK; s++) {
            // Each new _tsid starts at a base 100s ms higher than the previous.
            long ts = 1_700_000_000_000L + (long) s * 100_000_000L;
            int end = Math.min(pos + docsPerSeries, BLOCK);
            for (int i = pos; i < end; i++) {
                block[i] = ts;
                ts -= between(500, 60_000); // descending within this _tsid
            }
            pos = end;
        }
        // Fill any remainder with the same descending pattern.
        while (pos < BLOCK) {
            block[pos] = block[pos - 1] - between(500, 60_000);
            pos++;
        }

        // Verify DeltaTransform declines: the up-jump at each boundary breaks monotonicity.
        long[] copy = block.clone();
        MetadataBuffer deltaParams = new MetadataBuffer();
        boolean deltaFired = DeltaTransform.INSTANCE.tryEncode(copy, BLOCK, deltaParams);
        assertFalse("DeltaTransform should decline piecewise-monotonic block with _tsid jumps", deltaFired);

        // SplitDelta must fire on this shape.
        assertRoundTrip(stage, block, BLOCK);
    }

    public void testRandom() throws IOException {
        for (int iter = 0; iter < 30; iter++) {
            long[] block = new long[BLOCK];
            for (int i = 0; i < BLOCK; i++) {
                block[i] = randomLong();
            }
            // Random blocks may or may not fire; either way the round-trip contract must hold.
            assertRoundTripOrUnchanged(stage, block, BLOCK);
        }
    }

    private static void assertRoundTrip(SplitDeltaTransform t, long[] original, int valueCount) throws IOException {
        long[] work = original.clone();
        MetadataBuffer params = new MetadataBuffer();
        boolean fired = t.tryEncode(work, valueCount, params);
        assertTrue("SplitDelta must fire on this input", fired);

        long[] decoded = work.clone();
        t.decode(decoded, valueCount, DataInputMetadataReader.wrap(params));
        for (int i = 0; i < valueCount; i++) {
            assertEquals("round-trip failure at position " + i, original[i], decoded[i]);
        }
        // Padding must be untouched.
        for (int i = valueCount; i < original.length; i++) {
            assertEquals("padding must be untouched at position " + i, original[i], work[i]);
        }
    }

    private static void assertDeclines(SplitDeltaTransform t, long[] original, int valueCount) throws IOException {
        long[] work = original.clone();
        MetadataBuffer params = new MetadataBuffer();
        boolean fired = t.tryEncode(work, valueCount, params);
        assertFalse("SplitDelta must decline this input", fired);
        for (int i = 0; i < valueCount; i++) {
            assertEquals("declined stage must not mutate value " + i, original[i], work[i]);
        }
        assertEquals("declined stage must write no params", 0L, params.size());
    }

    private static void assertRoundTripOrUnchanged(SplitDeltaTransform t, long[] original, int valueCount) throws IOException {
        long[] work = original.clone();
        MetadataBuffer params = new MetadataBuffer();
        boolean fired = t.tryEncode(work, valueCount, params);
        if (fired) {
            long[] decoded = work.clone();
            t.decode(decoded, valueCount, DataInputMetadataReader.wrap(params));
            for (int i = 0; i < valueCount; i++) {
                assertEquals("round-trip failure at position " + i, original[i], decoded[i]);
            }
        } else {
            for (int i = 0; i < valueCount; i++) {
                assertEquals("declined stage must not mutate value " + i, original[i], work[i]);
            }
            assertEquals("declined stage must write no params", 0L, params.size());
        }
    }

    private static long[] filled(long value) {
        long[] block = new long[BLOCK];
        Arrays.fill(block, value);
        return block;
    }
}
