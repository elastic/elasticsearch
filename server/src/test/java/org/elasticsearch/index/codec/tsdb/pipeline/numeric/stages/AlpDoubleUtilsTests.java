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

    public void testFastRoundBasic() {
        assertEquals(0L, AlpDoubleUtils.fastRound(0.0));
        // Bias rounding uses ties-to-even, so 1.5 rounds to 2 and 2.5 rounds to 2.
        assertEquals(2L, AlpDoubleUtils.fastRound(1.5));
        assertEquals(2L, AlpDoubleUtils.fastRound(2.5));
        assertEquals(1L, AlpDoubleUtils.fastRound(0.6));
        assertEquals(-1L, AlpDoubleUtils.fastRound(-0.6));
        assertEquals(3L, AlpDoubleUtils.fastRound(3.14));
        assertEquals(-3L, AlpDoubleUtils.fastRound(-3.14));
    }

    public void testFastRoundLargeMagnitudeStillCorrect() {
        assertEquals(1_000_000L, AlpDoubleUtils.fastRound(1_000_000.0));
        assertEquals(1_000_000L, AlpDoubleUtils.fastRound(999_999.5));
        assertEquals(-1_000_000L, AlpDoubleUtils.fastRound(-1_000_000.0));
    }

    public void testAlpRoundFallsBackOutsideSafeRange() {
        final double atLimit = (double) (1L << 52);
        assertEquals(Math.round(atLimit), AlpDoubleUtils.alpRound(atLimit));
        final double aboveLimit = (double) ((1L << 52) + 1024);
        assertEquals(Math.round(aboveLimit), AlpDoubleUtils.alpRound(aboveLimit));
    }

    public void testAlpRoundMatchesMathRoundForNonHalfValues() {
        final double[] values = { 0.0, 3.14, -3.14, 100.0, 99_999.999, -0.001, 42.7, -42.3 };
        for (final double v : values) {
            assertEquals("mismatch for " + v, Math.round(v), AlpDoubleUtils.alpRound(v));
        }
    }

    public void testEstimatePrecision() {
        assertEquals(6, AlpDoubleUtils.estimatePrecision(3.141592));
        assertEquals(2, AlpDoubleUtils.estimatePrecision(3.14));
        assertEquals(0, AlpDoubleUtils.estimatePrecision(42.0));
        assertEquals(0, AlpDoubleUtils.estimatePrecision(Double.NaN));
        assertEquals(0, AlpDoubleUtils.estimatePrecision(Double.POSITIVE_INFINITY));
        assertEquals(0, AlpDoubleUtils.estimatePrecision(0.0));
    }

    public void testBestEFForValue() {
        assertEquals(2, AlpDoubleUtils.bestEFForValue(3.14) >>> 16);
        assertEquals(0, AlpDoubleUtils.bestEFForValue(42.0) >>> 16);
        assertEquals(0, AlpDoubleUtils.bestEFForValue(Double.NaN));
        assertEquals(0, AlpDoubleUtils.bestEFForValue(0.0));
    }

    public void testCountExceptionsZeroForIntegerDoubles() {
        // Integer doubles round-trip exactly through identity (e=0, f=0) because the
        // multiply-and-divide path uses 1.0 on both sides.
        final int blockSize = 64;
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong((double) (1000 + i));
        }
        assertEquals(0, AlpDoubleUtils.countExceptions(values, blockSize, 0, 0, Integer.MAX_VALUE));
    }

    public void testCountExceptionsAllOnIdentityForIrrationals() {
        final int blockSize = 32;
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(Math.sqrt(i + 2) * Math.PI);
        }
        assertEquals(blockSize, AlpDoubleUtils.countExceptions(values, blockSize, 0, 0, Integer.MAX_VALUE));
    }

    public void testComputeBitSavingsPositiveForIntegerDoubles() {
        // Sortable-long encoding of an integer double like 1000.0 has ~63 bits set; the
        // ALP mantissa is the integer itself (10 bits). Identity (e=0, f=0) therefore still
        // produces a large bit-width reduction in the mantissa stream.
        final int blockSize = 64;
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong((double) (1000 + i));
        }
        assertTrue("identity must save bits versus sortable-long encoding", AlpDoubleUtils.computeBitSavings(values, blockSize, 0, 0) > 0);
    }

    public void testComputeBitSavingsZeroForAllZeroBlock() {
        final int blockSize = 64;
        final long[] values = new long[blockSize];
        assertEquals(0, AlpDoubleUtils.computeBitSavings(values, blockSize, 0, 0));
    }

    public void testMaxExceptionsReturnsZeroWhenNoSavings() {
        assertEquals(0, AlpDoubleUtils.maxExceptions(0, 512, AlpDoubleUtils.DOUBLE_EXCEPTION_COST));
        assertEquals(0, AlpDoubleUtils.maxExceptions(-1, 512, AlpDoubleUtils.DOUBLE_EXCEPTION_COST));
    }

    public void testMaxExceptionsScalesWithBitsSaved() {
        final int small = AlpDoubleUtils.maxExceptions(8, 512, AlpDoubleUtils.DOUBLE_EXCEPTION_COST);
        final int large = AlpDoubleUtils.maxExceptions(32, 512, AlpDoubleUtils.DOUBLE_EXCEPTION_COST);
        assertTrue("more bits saved must tolerate more exceptions", large > small);
    }

    public void testMaxExceptionsScalesWithBlockSize() {
        final int small = AlpDoubleUtils.maxExceptions(8, 128, AlpDoubleUtils.DOUBLE_EXCEPTION_COST);
        final int large = AlpDoubleUtils.maxExceptions(8, 2048, AlpDoubleUtils.DOUBLE_EXCEPTION_COST);
        assertTrue("larger blocks must tolerate more exceptions for the same bit saving", large > small);
    }

    public void testFindBestEFKeepsExceptionsBoundedFor2dpDecimals() {
        // 10.0 + i*0.01 is not exactly representable for most i, so a non-trivial number of
        // exceptions is expected. The selection should still pick a pair that bounds them.
        final int blockSize = 128;
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(10.0 + i * 0.01);
        }
        final int[] efOut = new int[2];
        final int[] candCounts = new int[AlpDoubleUtils.CAND_POOL_SIZE];
        final int exceptions = AlpDoubleUtils.findBestEFForBlock(values, blockSize, efOut, candCounts);

        assertTrue("top-K must produce a non-negative e", efOut[0] >= 0);
        assertTrue("2dp exceptions must stay below the full block", exceptions < blockSize);
    }

    public void testFindBestEFForIntegerLikeDoubles() {
        final int blockSize = 256;
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong((double) (1000 + i));
        }
        final int[] efOut = new int[2];
        final int[] candCounts = new int[AlpDoubleUtils.CAND_POOL_SIZE];
        final int exceptions = AlpDoubleUtils.findBestEFForBlock(values, blockSize, efOut, candCounts);

        assertEquals(0, efOut[0]);
        assertEquals(0, efOut[1]);
        assertEquals(0, exceptions);
    }

    public void testFindBestEFSignalsFallbackOnEmptyBlock() {
        final long[] values = new long[0];
        final int[] efOut = new int[2];
        final int[] candCounts = new int[AlpDoubleUtils.CAND_POOL_SIZE];
        AlpDoubleUtils.findBestEFForBlock(values, 0, efOut, candCounts);
        assertEquals(-1, efOut[0]);
        assertEquals(-1, efOut[1]);
    }

    public void testFindBestEFAllZeroData() {
        final int blockSize = 64;
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(0.0);
        }
        final int[] efOut = new int[2];
        final int[] candCounts = new int[AlpDoubleUtils.CAND_POOL_SIZE];
        final int exceptions = AlpDoubleUtils.findBestEFForBlock(values, blockSize, efOut, candCounts);
        assertTrue(efOut[0] >= 0);
        assertEquals(0, exceptions);
    }

    public void testFindBestEFPoolResetBetweenCalls() {
        final int blockSize = 64;
        final int[] efOut = new int[2];
        final int[] candCounts = new int[AlpDoubleUtils.CAND_POOL_SIZE];

        final long[] decimals = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            decimals[i] = NumericUtils.doubleToSortableLong((double) (1000 + i) / 100.0);
        }
        AlpDoubleUtils.findBestEFForBlock(decimals, blockSize, efOut, candCounts);
        assertTrue("decimal data should select e >= 1", efOut[0] >= 1);

        final long[] integers = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            integers[i] = NumericUtils.doubleToSortableLong((double) (500 + i));
        }
        AlpDoubleUtils.findBestEFForBlock(integers, blockSize, efOut, candCounts);
        assertEquals("integers should select identity (e=0)", 0, efOut[0]);
    }

    public void testAlpTransformBlockStoresRoundedMantissaForException() {
        // Integer doubles round-trip exactly through identity (e=0, f=0); inject one
        // irrational to force a single exception at a known position.
        final int blockSize = 16;
        final long[] values = new long[blockSize];
        final long[] original = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            final double v = i == 7 ? Math.PI : (double) (1000 + i);
            values[i] = NumericUtils.doubleToSortableLong(v);
            original[i] = values[i];
        }

        final int[] excPositions = new int[blockSize];
        final long[] excValues = new long[blockSize];
        final int excCount = AlpDoubleUtils.alpTransformBlock(values, blockSize, 0, 0, excPositions, excValues, null);

        assertEquals(1, excCount);
        assertEquals(7, excPositions[0]);
        assertEquals(original[7], excValues[0]);
        // NOTE: the exception slot now carries the rounded mantissa (round(PI)=3 at e=0)
        // rather than the predecessor mantissa. The decoder patches the slot back to
        // the original sortable long from the exception list, so the in-array value
        // only matters for downstream stages, where a rounded mantissa keeps the
        // block's natural shape better than a copy of the predecessor.
        assertEquals("exception slot carries the rounded mantissa", 3L, values[7]);
    }

    public void testAlpTransformBlockStoresRoundedMantissaForLeadingException() {
        final int blockSize = 8;
        final long[] values = new long[blockSize];
        values[0] = NumericUtils.doubleToSortableLong(Math.PI);
        for (int i = 1; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong((double) (1000 + i));
        }

        final int[] excPositions = new int[blockSize];
        final long[] excValues = new long[blockSize];
        final int excCount = AlpDoubleUtils.alpTransformBlock(values, blockSize, 0, 0, excPositions, excValues, null);

        assertEquals(1, excCount);
        assertEquals(0, excPositions[0]);
        // NOTE: leading exception at position 0 used to fall back to 0, which created
        // an artificial spike below the block's actual scale. The rounded mantissa
        // (round(PI)=3 at e=0) keeps the slot at the right magnitude.
        assertEquals(3L, values[0]);
    }

    public void testAlpTransformBlockObservesConstantStride() {
        final int blockSize = 16;
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong((double) ((1L << 52) + i));
        }
        final long[] snapshot = values.clone();

        final int[] excPositions = new int[blockSize];
        final long[] excValues = new long[blockSize];
        final boolean[] observation = new boolean[1];
        AlpDoubleUtils.alpTransformBlock(values, blockSize, 0, 0, excPositions, excValues, observation);

        assertTrue(observation[0]);
        assertEquals(AlpDoubleUtils.hasNearConstantStride(snapshot, blockSize), observation[0]);
    }

    public void testAlpTransformBlockObservesVariableStrideAsNotNearConstant() {
        final long[] values = new long[] {
            NumericUtils.doubleToSortableLong(1.5),
            NumericUtils.doubleToSortableLong(1000.0),
            NumericUtils.doubleToSortableLong(-50.0),
            NumericUtils.doubleToSortableLong(123.456),
            NumericUtils.doubleToSortableLong(0.001),
            NumericUtils.doubleToSortableLong(9999.0) };
        final long[] snapshot = values.clone();

        final int[] excPositions = new int[values.length];
        final long[] excValues = new long[values.length];
        final boolean[] observation = new boolean[1];
        AlpDoubleUtils.alpTransformBlock(values, values.length, 0, 0, excPositions, excValues, observation);

        assertFalse(observation[0]);
        assertEquals(AlpDoubleUtils.hasNearConstantStride(snapshot, values.length), observation[0]);
    }

    public void testAlpTransformBlockObservesConstantBlockAsNotNearConstant() {
        final int blockSize = 8;
        final long[] values = new long[blockSize];
        java.util.Arrays.fill(values, NumericUtils.doubleToSortableLong(42.0));
        final long[] snapshot = values.clone();

        final int[] excPositions = new int[blockSize];
        final long[] excValues = new long[blockSize];
        final boolean[] observation = new boolean[1];
        AlpDoubleUtils.alpTransformBlock(values, blockSize, 0, 0, excPositions, excValues, observation);

        assertFalse(observation[0]);
        assertEquals(AlpDoubleUtils.hasNearConstantStride(snapshot, blockSize), observation[0]);
    }
}
