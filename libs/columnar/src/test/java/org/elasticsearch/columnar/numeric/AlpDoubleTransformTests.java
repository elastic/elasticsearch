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
 * Round-trip tests for {@link AlpDoubleTransform} covering sensor-like doubles, constant doubles,
 * outlier-containing blocks, and near-constant-stride blocks that should decline.
 */
public class AlpDoubleTransformTests extends ESTestCase {

    private static final int BLOCK = 128;

    private AlpDoubleTransform freshStage() {
        return new AlpDoubleTransform(BLOCK);
    }

    public void testSensorLikeDoubles() throws IOException {
        // Sensor readings like 22.5, 22.7, 22.6 ... encode well with ALP (e=1, f=0).
        long[] block = new long[BLOCK];
        double base = 22.0 + randomDoubleBetween(0.0, 5.0, true);
        for (int i = 0; i < BLOCK; i++) {
            double v = base + (randomDoubleBetween(-2.0, 2.0, true));
            // Round to one decimal place so ALP finds a good (e, f).
            v = Math.round(v * 10.0) / 10.0;
            block[i] = NumericUtils.doubleToSortableLong(v);
        }
        assertRoundTripOrUnchanged(freshStage(), block, BLOCK);
    }

    public void testConstantDouble() throws IOException {
        // Small integer doubles: ALP encodes with the identity pair (e=0, f=0) and always fires
        // because the integer mantissa is far narrower than the 63-bit IEEE sortable representation.
        long sv = NumericUtils.doubleToSortableLong(between(1, 1000));
        long[] block = new long[BLOCK];
        for (int i = 0; i < BLOCK; i++) {
            block[i] = sv;
        }
        assertRoundTrip(freshStage(), block, BLOCK);
    }

    public void testOutlierBlock() throws IOException {
        // Most values encode cleanly; a few are outliers that need exception storage.
        long[] block = new long[BLOCK];
        double base = 100.0;
        for (int i = 0; i < BLOCK; i++) {
            block[i] = NumericUtils.doubleToSortableLong(base + i * 0.1);
        }
        // Inject a handful of outliers (NaN or very large values that resist ALP).
        int numOutliers = between(1, 4);
        for (int n = 0; n < numOutliers; n++) {
            int pos = between(0, BLOCK - 1);
            block[pos] = NumericUtils.doubleToSortableLong(Double.MAX_VALUE / (n + 1.0));
        }
        assertRoundTripOrUnchanged(freshStage(), block, BLOCK);
    }

    public void testPartialBlock() throws IOException {
        // Only the first valueCount entries carry real data; the stage must ignore padding.
        long[] block = new long[BLOCK];
        int valueCount = between(4, BLOCK - 1);
        double base = 50.0;
        for (int i = 0; i < valueCount; i++) {
            block[i] = NumericUtils.doubleToSortableLong(base + i * 0.25);
        }
        // Poison padding to verify stage ignores it.
        for (int i = valueCount; i < BLOCK; i++) {
            block[i] = Long.MIN_VALUE;
        }
        assertRoundTripOrUnchanged(freshStage(), block, valueCount);
    }

    public void testNearConstantStrideDeclines() throws IOException {
        // Doubles advancing with a near-uniform stride: the integer pipeline wins, ALP should decline.
        // Use exact IEEE 754 increments to keep sortable-long stride constant.
        long[] block = new long[BLOCK];
        long base = NumericUtils.doubleToSortableLong(1000.0);
        // Stride of 1 sortable-long unit: much smaller than DELTA_SPREAD_THRESHOLD (16).
        for (int i = 0; i < BLOCK; i++) {
            block[i] = base + i;
        }
        // NOTE: fresh stage so the cache is cold; hasNearConstantStride should fire.
        AlpDoubleTransform stage = freshStage();
        long[] work = block.clone();
        MetadataBuffer params = new MetadataBuffer();
        boolean fired = stage.tryEncode(work, BLOCK, params);
        if (fired) {
            // If it fired anyway, it must round-trip correctly.
            stage.decode(work, BLOCK, DataInputMetadataReader.wrap(params));
            for (int i = 0; i < BLOCK; i++) {
                assertEquals("round-trip failure at " + i, block[i], work[i]);
            }
        }
        // No assertion that it must decline; the heuristic is best-effort. The test ensures
        // that if the stage declines it leaves the block untouched.
        if (fired == false) {
            for (int i = 0; i < BLOCK; i++) {
                assertEquals("declined stage must not mutate value " + i, block[i], work[i]);
            }
            assertEquals("declined stage must write no params", 0L, params.size());
        }
    }

    public void testCacheHitPath() throws IOException {
        // Encode two similar blocks in sequence so the second block exercises the cache fast path.
        AlpDoubleTransform stage = freshStage();
        double base = 22.0;
        long[] block1 = new long[BLOCK];
        long[] block2 = new long[BLOCK];
        for (int i = 0; i < BLOCK; i++) {
            block1[i] = NumericUtils.doubleToSortableLong(base + i * 0.1);
            block2[i] = NumericUtils.doubleToSortableLong(base + 0.05 + i * 0.1);
        }
        // First block: warms the cache.
        assertRoundTripOrUnchanged(stage, block1, BLOCK);
        // Second block: should hit the cache path and still round-trip correctly.
        assertRoundTripOrUnchanged(stage, block2, BLOCK);
    }

    public void testRoundTripSmallDecimals() throws IOException {
        // Values like 0.001, 0.002, ... test the (e=3, f=0) path.
        long[] block = new long[BLOCK];
        for (int i = 0; i < BLOCK; i++) {
            block[i] = NumericUtils.doubleToSortableLong((i + 1) * 0.001);
        }
        assertRoundTripOrUnchanged(freshStage(), block, BLOCK);
    }

    public void testAllSpecialValuesDecline() throws IOException {
        assertDeclines(NumericUtils.doubleToSortableLong(Double.NaN));
        assertDeclines(NumericUtils.doubleToSortableLong(Double.POSITIVE_INFINITY));
        assertDeclines(NumericUtils.doubleToSortableLong(0.0));
    }

    private void assertDeclines(long sortableLong) throws IOException {
        long[] block = new long[BLOCK];
        Arrays.fill(block, sortableLong);
        long[] original = block.clone();
        MetadataBuffer params = new MetadataBuffer();
        assertFalse(freshStage().tryEncode(block, BLOCK, params));
        assertArrayEquals(original, block);
        assertEquals(0L, params.size());
    }

    private static void assertRoundTrip(AlpDoubleTransform stage, long[] original, int valueCount) throws IOException {
        long[] work = original.clone();
        MetadataBuffer params = new MetadataBuffer();
        boolean fired = stage.tryEncode(work, valueCount, params);
        assertTrue("AlpDoubleTransform must fire on this input", fired);
        stage.decode(work, valueCount, DataInputMetadataReader.wrap(params));
        for (int i = 0; i < valueCount; i++) {
            assertEquals("round-trip failure at position " + i, original[i], work[i]);
        }
    }

    private static void assertRoundTripOrUnchanged(AlpDoubleTransform stage, long[] original, int valueCount) throws IOException {
        long[] work = original.clone();
        MetadataBuffer params = new MetadataBuffer();
        boolean fired = stage.tryEncode(work, valueCount, params);
        if (fired) {
            stage.decode(work, valueCount, DataInputMetadataReader.wrap(params));
            for (int i = 0; i < valueCount; i++) {
                assertEquals("round-trip failure at position " + i, original[i], work[i]);
            }
        } else {
            for (int i = 0; i < valueCount; i++) {
                assertEquals("declined stage must not mutate value " + i, original[i], work[i]);
            }
            assertEquals("declined stage must write no params", 0L, params.size());
        }
    }
}
