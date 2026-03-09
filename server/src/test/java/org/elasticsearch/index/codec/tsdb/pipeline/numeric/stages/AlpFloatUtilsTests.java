/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

public class AlpFloatUtilsTests extends ESTestCase {

    public void testTopKFindsLowExceptionsDecimal2dp() {
        final int blockSize = 128;
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.floatToSortableInt(10.0f + i * 0.01f);
        }

        final int[] efOut = new int[2];
        final int[] candE = new int[AlpFloatUtils.CAND_POOL_SIZE];
        final int[] candF = new int[AlpFloatUtils.CAND_POOL_SIZE];
        final int[] candCount = new int[AlpFloatUtils.CAND_POOL_SIZE];
        final int topKExceptions = AlpFloatUtils.findBestEFFloatTopK(
            values,
            blockSize,
            AlpFloatUtils.MAX_EXPONENT,
            efOut,
            candE,
            candF,
            candCount
        );

        assertTrue("top-K should find a valid candidate", efOut[0] >= 0);
        assertTrue("top-K exceptions (" + topKExceptions + ") should be reasonable for 2dp data", topKExceptions <= blockSize / 2);
    }

    public void testTopKFindsZeroExceptionsMonotonicInteger() {
        final int blockSize = 256;
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.floatToSortableInt((float) (1000 + i));
        }

        final int[] efOut = new int[2];
        final int[] candE = new int[AlpFloatUtils.CAND_POOL_SIZE];
        final int[] candF = new int[AlpFloatUtils.CAND_POOL_SIZE];
        final int[] candCount = new int[AlpFloatUtils.CAND_POOL_SIZE];
        final int topKExceptions = AlpFloatUtils.findBestEFFloatTopK(
            values,
            blockSize,
            AlpFloatUtils.MAX_EXPONENT,
            efOut,
            candE,
            candF,
            candCount
        );

        assertTrue("top-K should find a valid candidate", efOut[0] >= 0);
        assertEquals("integer-like data should have 0 exceptions", 0, topKExceptions);
    }

    public void testTopKFallbackSignalOnEmptyBlock() {
        final long[] values = new long[0];
        final int[] efOut = new int[2];
        final int[] candE = new int[AlpFloatUtils.CAND_POOL_SIZE];
        final int[] candF = new int[AlpFloatUtils.CAND_POOL_SIZE];
        final int[] candCount = new int[AlpFloatUtils.CAND_POOL_SIZE];

        AlpFloatUtils.findBestEFFloatTopK(values, 0, AlpFloatUtils.MAX_EXPONENT, efOut, candE, candF, candCount);
        assertEquals("empty block should signal fallback", -1, efOut[0]);
    }

    public void testPoolResetBetweenCalls() {
        final int blockSize = 64;
        final int[] efOut = new int[2];
        final int[] candE = new int[AlpFloatUtils.CAND_POOL_SIZE];
        final int[] candF = new int[AlpFloatUtils.CAND_POOL_SIZE];
        final int[] candCount = new int[AlpFloatUtils.CAND_POOL_SIZE];

        final long[] values1 = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values1[i] = NumericUtils.floatToSortableInt(10.0f + i * 0.01f);
        }
        AlpFloatUtils.findBestEFFloatTopK(values1, blockSize, AlpFloatUtils.MAX_EXPONENT, efOut, candE, candF, candCount);
        final int e1 = efOut[0];

        final long[] values2 = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values2[i] = NumericUtils.floatToSortableInt((float) (500 + i));
        }
        AlpFloatUtils.findBestEFFloatTopK(values2, blockSize, AlpFloatUtils.MAX_EXPONENT, efOut, candE, candF, candCount);
        final int e2 = efOut[0];

        assertTrue("first call should find e >= 1 for decimal data", e1 >= 1);
        assertEquals("second call should find e=0 for integer data", 0, e2);
    }

    public void testBestEFForSingleFloat() {
        int packed = AlpFloatUtils.bestEFForSingleFloat(3.14f, AlpFloatUtils.MAX_EXPONENT);
        int e = packed >>> 16;
        // NOTE: Float precision requires e=3 for 3.14f (e=2 loses a bit).
        assertEquals(3, e);

        packed = AlpFloatUtils.bestEFForSingleFloat(42.0f, AlpFloatUtils.MAX_EXPONENT);
        e = packed >>> 16;
        assertEquals(0, e);

        assertEquals(0, AlpFloatUtils.bestEFForSingleFloat(Float.NaN, AlpFloatUtils.MAX_EXPONENT));
        assertEquals(0, AlpFloatUtils.bestEFForSingleFloat(0.0f, AlpFloatUtils.MAX_EXPONENT));
    }

    public void testTopKHandlesAllZeroData() {
        final int blockSize = 64;
        final long[] values = new long[blockSize];
        Arrays.fill(values, NumericUtils.floatToSortableInt(0.0f));

        final int[] efOut = new int[2];
        final int[] candE = new int[AlpFloatUtils.CAND_POOL_SIZE];
        final int[] candF = new int[AlpFloatUtils.CAND_POOL_SIZE];
        final int[] candCount = new int[AlpFloatUtils.CAND_POOL_SIZE];
        final int exceptions = AlpFloatUtils.findBestEFFloatTopK(
            values,
            blockSize,
            AlpFloatUtils.MAX_EXPONENT,
            efOut,
            candE,
            candF,
            candCount
        );

        assertTrue("should find a valid candidate for all-zero data", efOut[0] >= 0);
        assertEquals("all-zero data should have 0 exceptions", 0, exceptions);
    }

    public void testAlpTransformBlockBasic() {
        final int blockSize = 16;
        final long[] values = new long[blockSize];
        final long[] original = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.floatToSortableInt(10.0f + i * 0.01f);
            original[i] = values[i];
        }

        final int[] excPositions = new int[blockSize];
        final int[] excValues = new int[blockSize];
        final int excCount = AlpFloatUtils.alpTransformBlock(values, blockSize, 2, 0, excPositions, excValues);

        assertTrue("exception count should be non-negative", excCount >= 0);
        assertTrue("exception count should not exceed block size", excCount <= blockSize);

        for (int i = 0; i < excCount; i++) {
            assertTrue("exception position should be in range", excPositions[i] >= 0 && excPositions[i] < blockSize);
            if (i > 0) {
                assertTrue("exception positions should be ordered", excPositions[i] > excPositions[i - 1]);
            }
            assertEquals("exception value should be original", (int) original[excPositions[i]], excValues[i]);
        }
    }

    public void testAlpTransformBlockRoundTrip() {
        final int blockSize = 32;
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.floatToSortableInt(100.0f + i * 0.25f);
        }

        final int expectedExceptions = AlpFloatUtils.countExceptionsFloat(values, blockSize, 2, 0);

        final long[] copy = values.clone();
        final int[] excPositions = new int[blockSize];
        final int[] excValues = new int[blockSize];
        final int excCount = AlpFloatUtils.alpTransformBlock(copy, blockSize, 2, 0, excPositions, excValues);

        assertEquals("alpTransformBlockFloat exception count should match countExceptionsFloat", expectedExceptions, excCount);
    }

    public void testFastRoundBasic() {
        assertEquals(0, AlpFloatUtils.fastRound(0.0f));
        assertEquals(1, AlpFloatUtils.fastRound(0.6f));
        assertEquals(-1, AlpFloatUtils.fastRound(-0.6f));
        assertEquals(3, AlpFloatUtils.fastRound(3.14f));
        assertEquals(-3, AlpFloatUtils.fastRound(-3.14f));
    }

    public void testFastRoundBoundary() {
        assertEquals(1000000, AlpFloatUtils.fastRound(1000000.0f));
        assertEquals(-1000000, AlpFloatUtils.fastRound(-1000000.0f));
    }

    public void testAlpRoundGuardedFallback() {
        final float atLimit = (float) (1 << 23);
        assertEquals(Math.round(atLimit), AlpFloatUtils.alpRound(atLimit));
        final float aboveLimit = (float) ((1 << 23) + 64);
        assertEquals(Math.round(aboveLimit), AlpFloatUtils.alpRound(aboveLimit));
    }

    public void testAlpRoundFloatMatchesMathRoundForNonHalfValues() {
        final float[] values = { 0.0f, 3.14f, -3.14f, 100.0f, 99.999f, -0.001f, 42.7f, -42.3f };
        for (final float v : values) {
            assertEquals("mismatch for " + v, Math.round(v), AlpFloatUtils.alpRound(v));
        }
    }

    public void testEstimatePrecisionFloat() {
        assertEquals(0, AlpFloatUtils.estimatePrecisionFloat(100.0f, AlpFloatUtils.MAX_EXPONENT));
        assertEquals(0, AlpFloatUtils.estimatePrecisionFloat(0.0f, AlpFloatUtils.MAX_EXPONENT));
        assertEquals(0, AlpFloatUtils.estimatePrecisionFloat(Float.NaN, AlpFloatUtils.MAX_EXPONENT));
        assertEquals(1, AlpFloatUtils.estimatePrecisionFloat(1.5f, AlpFloatUtils.MAX_EXPONENT));
        assertTrue("0.001f should have precision >= 2", AlpFloatUtils.estimatePrecisionFloat(0.001f, AlpFloatUtils.MAX_EXPONENT) >= 2);
    }
}
