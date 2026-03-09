/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.profiler;

import org.elasticsearch.index.codec.tsdb.pipeline.NumericDataGenerators;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

public class BlockProfilerTests extends ESTestCase {

    private final BlockProfiler profiler = new BlockProfiler();

    public void testConstantDataProfile() {
        final long[] values = new long[512];
        Arrays.fill(values, 42L);
        final BlockProfile profile = profiler.profile(values, values.length);

        assertEquals(512, profile.valueCount());
        assertEquals(42L, profile.min());
        assertEquals(42L, profile.max());
        assertEquals(0L, profile.range());
        assertEquals(42L, profile.rawGcd());
        assertEquals(0L, profile.shiftedGcd());
        assertEquals(0, profile.rawMaxBits());
        assertEquals(0, profile.xorMaxBits());
        assertEquals(511, profile.xorZeroCount());
        assertEquals(0, profile.deltaDeltaMaxBits());
    }

    public void testMonotonicallyIncreasingProfile() {
        final long[] values = new long[512];
        for (int i = 0; i < 512; i++)
            values[i] = i * 10L;
        final BlockProfile profile = profiler.profile(values, values.length);

        assertEquals(0L, profile.min());
        assertEquals(5110L, profile.max());
        assertTrue(profile.isMonotonicallyIncreasing());
        assertFalse(profile.isMonotonicallyDecreasing());
        assertEquals(10L, profile.rawGcd());
        assertEquals(10L, profile.shiftedGcd());
        assertEquals(0, profile.deltaDeltaMaxBits());
    }

    public void testGcdFriendlyProfile() {
        final long[] values = new long[512];
        for (int i = 0; i < 512; i++)
            values[i] = (i % 100) * 50L;
        final BlockProfile profile = profiler.profile(values, values.length);

        assertEquals(50L, profile.rawGcd());
        assertEquals(50L, profile.shiftedGcd());
    }

    public void testRunFriendlyProfile() {
        final long[] values = new long[512];
        for (int i = 0; i < 512; i++)
            values[i] = i / 100;
        final BlockProfile profile = profiler.profile(values, values.length);

        assertTrue(profile.runCount() < (int) (0.05 * profile.valueCount()));
        assertTrue(profile.range() < 10);
    }

    public void testRandomDataProfile() {
        final long[] values = NumericDataGenerators.randomLongs(512, 0x5DEECE66DL);
        final BlockProfile profile = profiler.profile(values, values.length);

        assertFalse(profile.isMonotonicallyIncreasing());
        assertTrue(profile.range() > 0);
        assertEquals(1L, profile.rawGcd());
        assertEquals(1L, profile.shiftedGcd());
        assertTrue(profile.runCount() > (int) (0.9 * profile.valueCount()));
    }

    public void testDoubleGaugeProfile() {
        final double[] doubles = NumericDataGenerators.gaugeDoubles(512);
        final long[] values = NumericDataGenerators.doublesToSortableLongs(doubles);
        final BlockProfile profile = profiler.profile(values, values.length);

        assertFalse(profile.isMonotonicallyIncreasing());
        assertTrue(profile.range() > 0);
    }

    public void testXorZeroCountForSmoothDoubles() {
        final double[] doubles = NumericDataGenerators.gaugeDoubles(512);
        final long[] values = NumericDataGenerators.doublesToSortableLongs(doubles);
        final BlockProfile profile = profiler.profile(values, values.length);

        assertTrue(profile.xorMaxBits() > 0);
        assertTrue(profile.xorZeroCount() < profile.valueCount() - 1);
    }

    public void testXorZeroCountForConstantData() {
        final long[] values = new long[512];
        Arrays.fill(values, 42L);
        final BlockProfile profile = profiler.profile(values, values.length);

        assertEquals(0, profile.xorMaxBits());
        assertEquals(511, profile.xorZeroCount());
    }

    public void testTimestampLikeDeltaDeltaMaxBits() {
        final long[] values = new long[512];
        final long base = 1700000000000L;
        for (int i = 0; i < 512; i++)
            values[i] = base + i * 1000L;
        final BlockProfile profile = profiler.profile(values, values.length);

        assertEquals(0, profile.deltaDeltaMaxBits());
        assertTrue(profile.isMonotonicallyIncreasing());
    }

    public void testEmptyBlock() {
        final BlockProfile profile = profiler.profile(new long[0], 0);

        assertEquals(0, profile.valueCount());
        assertEquals(0L, profile.min());
        assertEquals(0L, profile.max());
        assertEquals(0L, profile.range());
        assertEquals(0, profile.rawMaxBits());
    }

    public void testSingleValue() {
        final long[] values = { 99L };
        final BlockProfile profile = profiler.profile(values, 1);

        assertEquals(1, profile.valueCount());
        assertEquals(99L, profile.min());
        assertEquals(99L, profile.max());
        assertEquals(0L, profile.range());
        assertEquals(0, profile.rawMaxBits());
        assertEquals(1, profile.runCount());
        assertEquals(0, profile.xorZeroCount());
    }

    public void testDecreasingValuesProfile() {
        final long[] values = new long[512];
        for (int i = 0; i < 512; i++)
            values[i] = 5110L - i * 10L;
        final BlockProfile profile = profiler.profile(values, values.length);

        assertFalse(profile.isMonotonicallyIncreasing());
        assertTrue(profile.isMonotonicallyDecreasing());
        assertEquals(10L, profile.rawGcd());
        assertEquals(10L, profile.shiftedGcd());
        assertEquals(0, profile.deltaDeltaMaxBits());
    }

    public void testXorZeroCountEqualsValueCountMinusRunCount() {
        final long[] values = new long[512];
        for (int i = 0; i < 512; i++) {
            values[i] = i / 50;
        }
        final BlockProfile profile = profiler.profile(values, values.length);

        assertEquals(profile.valueCount() - profile.runCount(), profile.xorZeroCount());
    }

    public void testNonZeroDeltaDeltaMaxBits() {
        final long[] values = new long[512];
        for (int i = 0; i < 512; i++) {
            values[i] = (long) i * i;
        }
        final BlockProfile profile = profiler.profile(values, values.length);

        assertTrue(profile.deltaDeltaMaxBits() > 0);
        assertTrue(profile.isMonotonicallyIncreasing());
    }

    public void testTwoValuesProfile() {
        final long[] values = { 10L, 20L };
        final BlockProfile profile = profiler.profile(values, 2);

        assertEquals(2, profile.valueCount());
        assertEquals(10L, profile.min());
        assertEquals(20L, profile.max());
        assertEquals(10L, profile.range());
        assertEquals(10L, profile.rawGcd());
        assertEquals(10L, profile.shiftedGcd());
        // NOTE: 2-value block has gts=1 which is below MIN_DIRECTIONAL_CHANGES=2,
        // so it's not classified as monotonic — aligned with DeltaCodecStage behavior
        assertFalse(profile.isMonotonicallyIncreasing());
        assertFalse(profile.isMonotonicallyDecreasing());
        assertEquals(2, profile.runCount());
        assertEquals(0, profile.xorZeroCount());
        assertEquals(0, profile.deltaDeltaMaxBits());
    }

    public void testGcdOneForCoprimeValues() {
        final long[] values = new long[512];
        for (int i = 0; i < 512; i++) {
            values[i] = 2 * i + 1;
        }
        values[0] = 2;
        final BlockProfile profile = profiler.profile(values, values.length);

        assertEquals(1L, profile.rawGcd());
        assertEquals(1L, profile.shiftedGcd());
    }

    public void testShiftedGcdDiscovery() {
        final long[] values = new long[512];
        for (int i = 0; i < 512; i++) {
            values[i] = 1001L + i * 10L;
        }
        final BlockProfile profile = profiler.profile(values, values.length);

        // NOTE: raw GCD is 1 because 1001 is coprime with 10,
        // but shifted GCD is 10 — the offset stage reveals the common divisor
        assertEquals(1L, profile.rawGcd());
        assertEquals(10L, profile.shiftedGcd());
    }
}
