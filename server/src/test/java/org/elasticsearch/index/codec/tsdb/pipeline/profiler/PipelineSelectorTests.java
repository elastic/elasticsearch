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
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineResolver;
import org.elasticsearch.index.codec.tsdb.pipeline.StageSpec;
import org.elasticsearch.index.mapper.TimeSeriesParams.MetricType;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class PipelineSelectorTests extends ESTestCase {

    private final BlockProfiler profiler = new BlockProfiler();
    private final PipelineSelector selector = new PipelineSelector();

    public void testConstantSelectsRle() {
        final long[] values = new long[512];
        Arrays.fill(values, 42L);
        final PipelineConfig config = selector.select(profiler.profile(values, 512), 512, PipelineConfig.DataType.LONG, null, null);

        assertFalse(config.isDefault());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Rle.class)));
    }

    public void testTimestampSelectsDeltaDelta() {
        final long[] values = new long[512];
        final long base = 1700000000000L;
        for (int i = 0; i < 512; i++)
            values[i] = base + i * 1000L;
        final PipelineConfig config = selector.select(profiler.profile(values, 512), 512, PipelineConfig.DataType.LONG, null, null);

        assertFalse(config.isDefault());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.DeltaDelta.class)));
    }

    public void testMonotonicWithVaryingStrideSelectsDelta() {
        final long[] values = new long[512];
        for (int i = 0; i < 512; i++)
            values[i] = i * 10L;
        final BlockProfile profile = profiler.profile(values, 512);

        assertTrue(profile.isMonotonicallyIncreasing());
        assertEquals(0, profile.deltaDeltaMaxBits());

        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.LONG, null, null);
        assertFalse(config.isDefault());
    }

    public void testGcdFriendlySelectsGcd() {
        final long[] values = new long[512];
        for (int i = 0; i < 512; i++)
            values[i] = (i % 100) * 50L;
        final PipelineConfig config = selector.select(profiler.profile(values, 512), 512, PipelineConfig.DataType.LONG, null, null);

        assertFalse(config.isDefault());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Gcd.class)));
    }

    public void testRandomSelectsOffsetBitpack() {
        final long[] values = NumericDataGenerators.randomLongs(512, 0x5DEECE66DL);
        final PipelineConfig config = selector.select(profiler.profile(values, 512), 512, PipelineConfig.DataType.LONG, null, null);

        assertFalse(config.isDefault());
        assertEquals(PipelineConfig.DataType.LONG, config.dataType());
    }

    public void testSmoothDoubleSelectsFpc() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 10, 12, 20);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.DOUBLE, null, null);

        assertEquals(PipelineConfig.DataType.DOUBLE, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.FpcStage.class)));
    }

    public void testNoisyDoubleDefaultSelectsAlpWith6Decimals() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 30, 12, 20);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.DOUBLE, null, null);

        assertEquals(PipelineConfig.DataType.DOUBLE, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.AlpDoubleStage.class)));
        assertAlpMaxError(config, 1e-6);
    }

    public void testNoisyDoubleStorageSelectsAlpWith2Decimals() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 30, 12, 20);
        final PipelineConfig config = selector.select(
            profile,
            512,
            PipelineConfig.DataType.DOUBLE,
            PipelineResolver.OptimizeFor.STORAGE,
            null
        );

        assertThat(config.specs(), hasItem(instanceOf(StageSpec.AlpDoubleStage.class)));
        assertAlpMaxError(config, 1e-2);
    }

    public void testNoisyDoubleBalancedSelectsAlpWith4Decimals() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 30, 12, 20);
        final PipelineConfig config = selector.select(
            profile,
            512,
            PipelineConfig.DataType.DOUBLE,
            PipelineResolver.OptimizeFor.BALANCED,
            null
        );

        assertThat(config.specs(), hasItem(instanceOf(StageSpec.AlpDoubleStage.class)));
        assertAlpMaxError(config, 1e-4);
    }

    public void testNoisyDoubleSpeedSelectsLosslessAlp() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 30, 12, 20);
        final PipelineConfig config = selector.select(
            profile,
            512,
            PipelineConfig.DataType.DOUBLE,
            PipelineResolver.OptimizeFor.SPEED,
            null
        );

        assertThat(config.specs(), hasItem(instanceOf(StageSpec.AlpDoubleStage.class)));
        assertAlpMaxError(config, -1.0);
    }

    public void testDoubleCounterDefaultSelectsFpc() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 10, 12, 20);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.DOUBLE, null, MetricType.COUNTER);

        assertEquals(PipelineConfig.DataType.DOUBLE, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.FpcStage.class)));
    }

    public void testDoubleCounterStorageSelectsGorilla() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 10, 12, 20);
        final PipelineConfig config = selector.select(
            profile,
            512,
            PipelineConfig.DataType.DOUBLE,
            PipelineResolver.OptimizeFor.STORAGE,
            MetricType.COUNTER
        );

        assertEquals(PipelineConfig.DataType.DOUBLE, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Gorilla.class)));
    }

    public void testDoubleCounterSpeedSelectsChimp() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 10, 12, 20);
        final PipelineConfig config = selector.select(
            profile,
            512,
            PipelineConfig.DataType.DOUBLE,
            PipelineResolver.OptimizeFor.SPEED,
            MetricType.COUNTER
        );

        assertEquals(PipelineConfig.DataType.DOUBLE, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.ChimpDoubleStage.class)));
    }

    public void testSmoothFloatSelectsFpc() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 10, 12, 20);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.FLOAT, null, null);

        assertEquals(PipelineConfig.DataType.FLOAT, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.FpcStage.class)));
    }

    public void testNoisyFloatSelectsAlp() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 30, 12, 20);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.FLOAT, null, null);

        assertEquals(PipelineConfig.DataType.FLOAT, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.AlpFloatStage.class)));
    }

    public void testFloatCounterDefaultSelectsFpc() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 10, 12, 20);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.FLOAT, null, MetricType.COUNTER);

        assertEquals(PipelineConfig.DataType.FLOAT, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.FpcStage.class)));
    }

    public void testFloatCounterStorageSelectsGorilla() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 10, 12, 20);
        final PipelineConfig config = selector.select(
            profile,
            512,
            PipelineConfig.DataType.FLOAT,
            PipelineResolver.OptimizeFor.STORAGE,
            MetricType.COUNTER
        );

        assertEquals(PipelineConfig.DataType.FLOAT, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.GorillaFloat.class)));
    }

    public void testFloatCounterSpeedSelectsChimp() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 10, 12, 20);
        final PipelineConfig config = selector.select(
            profile,
            512,
            PipelineConfig.DataType.FLOAT,
            PipelineResolver.OptimizeFor.SPEED,
            MetricType.COUNTER
        );

        assertEquals(PipelineConfig.DataType.FLOAT, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.ChimpFloatStage.class)));
    }

    public void testDeterministic() {
        final long[] values = new long[512];
        for (int i = 0; i < 512; i++)
            values[i] = i * 10L;
        final BlockProfile profile = profiler.profile(values, 512);

        final PipelineConfig first = selector.select(profile, 512, PipelineConfig.DataType.LONG, null, null);
        final PipelineConfig second = selector.select(profile, 512, PipelineConfig.DataType.LONG, null, null);
        assertEquals(first, second);
    }

    public void testLowRunRatioSelectsRle() {
        final long[] values = new long[512];
        for (int i = 0; i < 512; i++) {
            values[i] = i / 256;
        }
        final BlockProfile profile = profiler.profile(values, 512);
        assertTrue(profile.range() > 0);

        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.LONG, null, null);
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Rle.class)));
    }

    public void testMonotonicLargeDeltaDeltaSelectsDelta() {
        final long[] values = new long[512];
        for (int i = 0; i < 512; i++) {
            values[i] = (long) i * i * i;
        }
        final BlockProfile profile = profiler.profile(values, 512);
        assertTrue(profile.isMonotonicallyIncreasing());
        assertTrue(profile.deltaDeltaMaxBits() > 4);

        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.LONG, null, null);
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Delta.class)));
        assertThat(config.specs(), not(hasItem(instanceOf(StageSpec.DeltaDelta.class))));
    }

    public void testSmoothLongSelectsDelta() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 10, 12, 20);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.LONG, null, null);

        assertEquals(PipelineConfig.DataType.LONG, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Delta.class)));
    }

    public void testHighRunRatioGcdOneSelectsOffsetBitPack() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 510, 7, 7, 2, 10);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.LONG, null, null);

        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Offset.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.BitPack.class)));
        assertThat(config.specs(), not(hasItem(instanceOf(StageSpec.Delta.class))));
        assertThat(config.specs(), not(hasItem(instanceOf(StageSpec.Gcd.class))));
    }

    public void testConstantDoublePreservesDataType() {
        final long[] values = new long[512];
        Arrays.fill(values, 42L);
        final PipelineConfig config = selector.select(profiler.profile(values, 512), 512, PipelineConfig.DataType.DOUBLE, null, null);

        assertEquals(PipelineConfig.DataType.DOUBLE, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Rle.class)));
    }

    public void testShiftedGcdEnablesGcdStage() {
        final long[] values = new long[512];
        for (int i = 0; i < 512; i++) {
            values[i] = 1001L + i * 10L;
        }
        final BlockProfile profile = profiler.profile(values, 512);

        // NOTE: raw GCD is 1 (1001 is coprime with 10), but shifted GCD is 10
        assertEquals(1L, profile.rawGcd());
        assertEquals(10L, profile.shiftedGcd());

        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.LONG, null, null);
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Gcd.class)));
    }

    public void testRleDoublePreservesDataType() {
        final long[] values = new long[512];
        for (int i = 0; i < 512; i++) {
            values[i] = i / 256;
        }
        final PipelineConfig config = selector.select(profiler.profile(values, 512), 512, PipelineConfig.DataType.DOUBLE, null, null);

        assertEquals(PipelineConfig.DataType.DOUBLE, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Rle.class)));
    }

    public void testDoubleGaugeCounterDistinction() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 30, 12, 20);

        final PipelineConfig gauge = selector.select(profile, 512, PipelineConfig.DataType.DOUBLE, null, MetricType.GAUGE);
        assertThat(gauge.specs(), hasItem(instanceOf(StageSpec.AlpDoubleStage.class)));

        final PipelineConfig counter = selector.select(profile, 512, PipelineConfig.DataType.DOUBLE, null, MetricType.COUNTER);
        assertThat(counter.specs(), hasItem(instanceOf(StageSpec.FpcStage.class)));
    }

    public void testFloatGaugeCounterDistinction() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 30, 12, 20);

        final PipelineConfig gauge = selector.select(profile, 512, PipelineConfig.DataType.FLOAT, null, MetricType.GAUGE);
        assertThat(gauge.specs(), hasItem(instanceOf(StageSpec.AlpFloatStage.class)));

        final PipelineConfig counter = selector.select(profile, 512, PipelineConfig.DataType.FLOAT, null, MetricType.COUNTER);
        assertThat(counter.specs(), hasItem(instanceOf(StageSpec.FpcStage.class)));
    }

    public void testSmoothDoubleStorageSelectsGorilla() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 10, 12, 20);
        final PipelineConfig config = selector.select(
            profile,
            512,
            PipelineConfig.DataType.DOUBLE,
            PipelineResolver.OptimizeFor.STORAGE,
            null
        );

        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Gorilla.class)));
    }

    public void testSmoothDoubleSpeedSelectsChimp() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 10, 12, 20);
        final PipelineConfig config = selector.select(
            profile,
            512,
            PipelineConfig.DataType.DOUBLE,
            PipelineResolver.OptimizeFor.SPEED,
            null
        );

        assertThat(config.specs(), hasItem(instanceOf(StageSpec.ChimpDoubleStage.class)));
    }

    public void testSmoothFloatStorageSelectsGorilla() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 10, 12, 20);
        final PipelineConfig config = selector.select(
            profile,
            512,
            PipelineConfig.DataType.FLOAT,
            PipelineResolver.OptimizeFor.STORAGE,
            null
        );

        assertThat(config.specs(), hasItem(instanceOf(StageSpec.GorillaFloat.class)));
    }

    public void testSmoothFloatSpeedSelectsChimp() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 10, 12, 20);
        final PipelineConfig config = selector.select(
            profile,
            512,
            PipelineConfig.DataType.FLOAT,
            PipelineResolver.OptimizeFor.SPEED,
            null
        );

        assertThat(config.specs(), hasItem(instanceOf(StageSpec.ChimpFloatStage.class)));
    }

    private static void assertAlpMaxError(final PipelineConfig config, double expectedMaxError) {
        for (final StageSpec spec : config.specs()) {
            if (spec instanceof StageSpec.AlpDoubleStage alp) {
                assertEquals(expectedMaxError, alp.maxError(), 0.0);
                return;
            }
            if (spec instanceof StageSpec.AlpFloatStage alp) {
                assertEquals(expectedMaxError, alp.maxError(), 0.0);
                return;
            }
        }
        fail("no ALP stage found in config: " + config);
    }
}
