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

public class AlpDoubleUtilsTests extends ESTestCase {

    public void testTopKFindsLowExceptionsDecimal2dp() {
        final int blockSize = 128;
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(10.0 + i * 0.01);
        }

        final int[] efOut = new int[2];
        final int[] candE = new int[AlpDoubleUtils.CAND_POOL_SIZE];
        final int[] candF = new int[AlpDoubleUtils.CAND_POOL_SIZE];
        final int[] candCount = new int[AlpDoubleUtils.CAND_POOL_SIZE];
        final int topKExceptions = AlpDoubleUtils.findBestEFDoubleTopK(
            values,
            blockSize,
            AlpDoubleUtils.MAX_EXPONENT,
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
            values[i] = NumericUtils.doubleToSortableLong((double) (1000 + i));
        }

        final int[] efOut = new int[2];
        final int[] candE = new int[AlpDoubleUtils.CAND_POOL_SIZE];
        final int[] candF = new int[AlpDoubleUtils.CAND_POOL_SIZE];
        final int[] candCount = new int[AlpDoubleUtils.CAND_POOL_SIZE];
        final int topKExceptions = AlpDoubleUtils.findBestEFDoubleTopK(
            values,
            blockSize,
            AlpDoubleUtils.MAX_EXPONENT,
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
        final int[] candE = new int[AlpDoubleUtils.CAND_POOL_SIZE];
        final int[] candF = new int[AlpDoubleUtils.CAND_POOL_SIZE];
        final int[] candCount = new int[AlpDoubleUtils.CAND_POOL_SIZE];

        AlpDoubleUtils.findBestEFDoubleTopK(values, 0, AlpDoubleUtils.MAX_EXPONENT, efOut, candE, candF, candCount);
        assertEquals("empty block should signal fallback", -1, efOut[0]);
    }

    public void testPoolResetBetweenCalls() {
        final int blockSize = 64;
        final int[] efOut = new int[2];
        final int[] candE = new int[AlpDoubleUtils.CAND_POOL_SIZE];
        final int[] candF = new int[AlpDoubleUtils.CAND_POOL_SIZE];
        final int[] candCount = new int[AlpDoubleUtils.CAND_POOL_SIZE];

        final long[] values1 = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values1[i] = NumericUtils.doubleToSortableLong(10.0 + i * 0.01);
        }
        AlpDoubleUtils.findBestEFDoubleTopK(values1, blockSize, AlpDoubleUtils.MAX_EXPONENT, efOut, candE, candF, candCount);
        final int e1 = efOut[0];

        final long[] values2 = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values2[i] = NumericUtils.doubleToSortableLong((double) (500 + i));
        }
        AlpDoubleUtils.findBestEFDoubleTopK(values2, blockSize, AlpDoubleUtils.MAX_EXPONENT, efOut, candE, candF, candCount);
        final int e2 = efOut[0];

        assertTrue("first call should find e >= 1 for decimal data", e1 >= 1);
        assertEquals("second call should find e=0 for integer data", 0, e2);
    }

    public void testBestEFForSingleDouble() {
        int packed = AlpDoubleUtils.bestEFForSingleDouble(3.14, AlpDoubleUtils.MAX_EXPONENT);
        int e = packed >>> 16;
        assertEquals(2, e);

        packed = AlpDoubleUtils.bestEFForSingleDouble(42.0, AlpDoubleUtils.MAX_EXPONENT);
        e = packed >>> 16;
        assertEquals(0, e);

        assertEquals(0, AlpDoubleUtils.bestEFForSingleDouble(Double.NaN, AlpDoubleUtils.MAX_EXPONENT));
        assertEquals(0, AlpDoubleUtils.bestEFForSingleDouble(0.0, AlpDoubleUtils.MAX_EXPONENT));
    }

    public void testTopKHandlesAllZeroData() {
        final int blockSize = 64;
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(0.0);
        }

        final int[] efOut = new int[2];
        final int[] candE = new int[AlpDoubleUtils.CAND_POOL_SIZE];
        final int[] candF = new int[AlpDoubleUtils.CAND_POOL_SIZE];
        final int[] candCount = new int[AlpDoubleUtils.CAND_POOL_SIZE];
        final int exceptions = AlpDoubleUtils.findBestEFDoubleTopK(
            values,
            blockSize,
            AlpDoubleUtils.MAX_EXPONENT,
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
            values[i] = NumericUtils.doubleToSortableLong(10.0 + i * 0.01);
            original[i] = values[i];
        }

        final int[] excPositions = new int[blockSize];
        final long[] excValues = new long[blockSize];
        final int excCount = AlpDoubleUtils.alpTransformBlock(values, blockSize, 2, 0, excPositions, excValues);

        assertTrue("exception count should be non-negative", excCount >= 0);
        assertTrue("exception count should not exceed block size", excCount <= blockSize);

        for (int i = 0; i < excCount; i++) {
            assertTrue("exception position should be in range", excPositions[i] >= 0 && excPositions[i] < blockSize);
            if (i > 0) {
                assertTrue("exception positions should be ordered", excPositions[i] > excPositions[i - 1]);
            }
            assertEquals("exception value should be original", original[excPositions[i]], excValues[i]);
        }

        for (int i = 0; i < blockSize; i++) {
            boolean isException = false;
            for (int j = 0; j < excCount; j++) {
                if (excPositions[j] == i) {
                    isException = true;
                    break;
                }
            }
            if (isException == false) {
                assertNotEquals("non-exception value should be transformed", original[i], values[i]);
            }
        }
    }

    public void testAlpTransformBlockRoundTrip() {
        final int blockSize = 32;
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(100.0 + i * 0.25);
        }

        final int expectedExceptions = AlpDoubleUtils.countExceptions(values, blockSize, 2, 0);

        final long[] copy = values.clone();
        final int[] excPositions = new int[blockSize];
        final long[] excValues = new long[blockSize];
        final int excCount = AlpDoubleUtils.alpTransformBlock(copy, blockSize, 2, 0, excPositions, excValues);

        assertEquals("alpTransformBlock exception count should match countExceptionsDouble", expectedExceptions, excCount);
    }

    public void testEstimatePrecisionCapsAtMaxExponent() {
        assertEquals(6, AlpDoubleUtils.estimatePrecision(3.141592, 6));
        assertEquals(6, AlpDoubleUtils.estimatePrecision(3.141592, AlpDoubleUtils.MAX_EXPONENT));
        assertEquals(2, AlpDoubleUtils.estimatePrecision(3.14, 6));
        assertEquals(0, AlpDoubleUtils.estimatePrecision(42.0, 3));
    }

    public void testBestEFForSingleDoubleWithReducedMaxExponent() {
        int packed = AlpDoubleUtils.bestEFForSingleDouble(3.14, 2);
        int e = packed >>> 16;
        assertEquals(2, e);

        packed = AlpDoubleUtils.bestEFForSingleDouble(3.14, 1);
        e = packed >>> 16;
        assertEquals(0, e);
    }

    public void testTopKWithReducedMaxExponentQuantized2dp() {
        final int blockSize = 128;
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(10.0 + i * 0.01);
        }

        final int[] efOut = new int[2];
        final int[] candE = new int[AlpDoubleUtils.CAND_POOL_SIZE];
        final int[] candF = new int[AlpDoubleUtils.CAND_POOL_SIZE];
        final int[] candCount = new int[AlpDoubleUtils.CAND_POOL_SIZE];
        final int exceptions = AlpDoubleUtils.findBestEFDoubleTopK(values, blockSize, 2, efOut, candE, candF, candCount);

        assertTrue("should find valid candidate with maxExponent=2", efOut[0] >= 0);
        assertTrue("e should be bounded by maxExponent", efOut[0] <= 2);
        assertTrue("2dp data with maxExponent=2 should have few exceptions", exceptions <= blockSize / 2);
    }

    public void testTopKWithReducedMaxExponentQuantized6dp() {
        final int blockSize = 128;
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(10.0 + i * 0.000001);
        }

        final int[] efOut = new int[2];
        final int[] candE = new int[AlpDoubleUtils.CAND_POOL_SIZE];
        final int[] candF = new int[AlpDoubleUtils.CAND_POOL_SIZE];
        final int[] candCount = new int[AlpDoubleUtils.CAND_POOL_SIZE];
        final int exceptions = AlpDoubleUtils.findBestEFDoubleTopK(values, blockSize, 6, efOut, candE, candF, candCount);

        assertTrue("should find valid candidate with maxExponent=6", efOut[0] >= 0);
        assertTrue("e should be bounded by maxExponent", efOut[0] <= 6);
    }

    public void testFastRoundBasic() {
        assertEquals(0L, AlpDoubleUtils.fastRound(0.0));
        // NOTE: Bias rounding rounds half-integers to even, unlike Math.round
        // which rounds 0.5 up. For ALP this is fine -- encode and decode use
        // the same rounding so round-trip is preserved.
        assertEquals(2L, AlpDoubleUtils.fastRound(1.5));
        assertEquals(1L, AlpDoubleUtils.fastRound(0.6));
        assertEquals(-1L, AlpDoubleUtils.fastRound(-0.6));
        assertEquals(3L, AlpDoubleUtils.fastRound(3.14));
        assertEquals(-3L, AlpDoubleUtils.fastRound(-3.14));
    }

    public void testFastRoundBoundary() {
        // NOTE: Values well within the 2^52 safe range
        assertEquals(1000000L, AlpDoubleUtils.fastRound(1000000.0));
        assertEquals(1000000L, AlpDoubleUtils.fastRound(999999.5));
        assertEquals(-1000000L, AlpDoubleUtils.fastRound(-1000000.0));
    }

    public void testAlpRoundGuardedFallback() {
        // NOTE: Values at or above 2^52 fall back to Math.round
        final double atLimit = (double) (1L << 52);
        assertEquals(Math.round(atLimit), AlpDoubleUtils.alpRound(atLimit));
        final double aboveLimit = (double) ((1L << 52) + 1024);
        assertEquals(Math.round(aboveLimit), AlpDoubleUtils.alpRound(aboveLimit));
    }

    public void testAlpRoundDoubleMatchesMathRoundForNonHalfValues() {
        // NOTE: For non-half values within safe range, alpRoundDouble matches Math.round.
        // Half-integers (x.5) may differ due to ties-to-even vs ties-to-up.
        final double[] values = { 0.0, 3.14, -3.14, 100.0, 99999.999, -0.001, 42.7, -42.3 };
        for (final double v : values) {
            assertEquals("mismatch for " + v, Math.round(v), AlpDoubleUtils.alpRound(v));
        }
    }
}
