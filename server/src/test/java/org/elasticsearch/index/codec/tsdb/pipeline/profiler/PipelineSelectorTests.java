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
        final PipelineConfig config = selector.select(profiler.profile(values, 512), 512, PipelineConfig.DataType.LONG, null);

        assertFalse(config.isDefault());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Rle.class)));
    }

    public void testTimestampSelectsDeltaDelta() {
        final long[] values = new long[512];
        final long base = 1700000000000L;
        for (int i = 0; i < 512; i++)
            values[i] = base + i * 1000L;
        final PipelineConfig config = selector.select(profiler.profile(values, 512), 512, PipelineConfig.DataType.LONG, null);

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

        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.LONG, null);
        assertFalse(config.isDefault());
    }

    public void testGcdFriendlySelectsGcd() {
        final long[] values = new long[512];
        for (int i = 0; i < 512; i++)
            values[i] = (i % 100) * 50L;
        final PipelineConfig config = selector.select(profiler.profile(values, 512), 512, PipelineConfig.DataType.LONG, null);

        assertFalse(config.isDefault());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Gcd.class)));
    }

    public void testRandomSelectsOffsetBitpack() {
        final long[] values = NumericDataGenerators.randomLongs(512, 0x5DEECE66DL);
        final PipelineConfig config = selector.select(profiler.profile(values, 512), 512, PipelineConfig.DataType.LONG, null);

        assertFalse(config.isDefault());
        assertEquals(PipelineConfig.DataType.LONG, config.dataType());
    }

    public void testSmoothDoubleSelectsLosslessAlp() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 10, 12, 20);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.DOUBLE, null);

        assertEquals(PipelineConfig.DataType.DOUBLE, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.AlpDoubleStage.class)));
    }

    public void testSmoothDoubleWithStorageHintSelectsQuantizedAlp() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 10, 12, 20);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.DOUBLE, PipelineResolver.OptimizeFor.STORAGE);

        assertEquals(PipelineConfig.DataType.DOUBLE, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.AlpDoubleStage.class)));
    }

    public void testSmoothDoubleWithSpeedHintSelectsXor() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 10, 12, 20);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.DOUBLE, PipelineResolver.OptimizeFor.SPEED);

        assertEquals(PipelineConfig.DataType.DOUBLE, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Xor.class)));
    }

    public void testDeterministic() {
        final long[] values = new long[512];
        for (int i = 0; i < 512; i++)
            values[i] = i * 10L;
        final BlockProfile profile = profiler.profile(values, 512);

        final PipelineConfig first = selector.select(profile, 512, PipelineConfig.DataType.LONG, null);
        final PipelineConfig second = selector.select(profile, 512, PipelineConfig.DataType.LONG, null);
        assertEquals(first, second);
    }

    public void testLowRunRatioSelectsRle() {
        final long[] values = new long[512];
        for (int i = 0; i < 512; i++) {
            values[i] = i / 256;
        }
        final BlockProfile profile = profiler.profile(values, 512);
        assertTrue(profile.range() > 0);

        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.LONG, null);
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

        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.LONG, null);
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Delta.class)));
        assertThat(config.specs(), not(hasItem(instanceOf(StageSpec.DeltaDelta.class))));
    }

    public void testSmoothFloatSelectsAlpFloat() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 10, 12, 20);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.FLOAT, null);

        assertEquals(PipelineConfig.DataType.FLOAT, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.AlpFloatStage.class)));
    }

    public void testSmoothFloatWithSpeedHintSelectsXor() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 10, 12, 20);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.FLOAT, PipelineResolver.OptimizeFor.SPEED);

        assertEquals(PipelineConfig.DataType.FLOAT, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Xor.class)));
    }

    public void testSmoothLongSelectsDelta() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 10, 12, 20);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.LONG, null);

        assertEquals(PipelineConfig.DataType.LONG, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Delta.class)));
    }

    public void testNoisyFloatSkipsXorAndFpc() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 30, 12, 20);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.FLOAT, null);

        assertEquals(PipelineConfig.DataType.FLOAT, config.dataType());
        assertThat(config.specs(), not(hasItem(instanceOf(StageSpec.Xor.class))));
        assertThat(config.specs(), not(hasItem(instanceOf(StageSpec.FpcStage.class))));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Offset.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.PatchedPFor.class)));
    }

    public void testNoisyDoubleSkipsXor() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 30, 12, 20);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.DOUBLE, null);

        assertEquals(PipelineConfig.DataType.DOUBLE, config.dataType());
        assertThat(config.specs(), not(hasItem(instanceOf(StageSpec.Xor.class))));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Offset.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.PatchedPFor.class)));
    }

    public void testHighRunRatioGcdOneSelectsOffsetBitPack() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 510, 7, 7, 2, 10);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.LONG, null);

        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Offset.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.BitPack.class)));
        assertThat(config.specs(), not(hasItem(instanceOf(StageSpec.Delta.class))));
        assertThat(config.specs(), not(hasItem(instanceOf(StageSpec.Gcd.class))));
    }

    public void testConstantDoublePreservesDataType() {
        final long[] values = new long[512];
        Arrays.fill(values, 42L);
        final PipelineConfig config = selector.select(profiler.profile(values, 512), 512, PipelineConfig.DataType.DOUBLE, null);

        assertEquals(PipelineConfig.DataType.DOUBLE, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Rle.class)));
    }

    public void testMonotonicDoubleCounterSelectsGorilla() {
        final long[] values = new long[512];
        final long base = NumericDataGenerators.doublesToSortableLongs(new double[] { 100.0 })[0];
        final long step = NumericDataGenerators.doublesToSortableLongs(new double[] { 100.001 })[0] - base;
        for (int i = 0; i < 512; i++) {
            values[i] = base + i * step;
        }
        final BlockProfile profile = profiler.profile(values, 512);
        assertTrue(profile.isMonotonicallyIncreasing());

        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.DOUBLE, null);

        assertEquals(PipelineConfig.DataType.DOUBLE, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Gorilla.class)));
    }

    public void testMonotonicFloatCounterSelectsGorilla() {
        final long[] values = new long[512];
        for (int i = 0; i < 512; i++) {
            values[i] = 1000L + i * 10L;
        }
        final BlockProfile profile = profiler.profile(values, 512);
        assertTrue(profile.isMonotonicallyIncreasing());

        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.FLOAT, null);

        assertEquals(PipelineConfig.DataType.FLOAT, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.GorillaFloat.class)));
    }

    public void testGcdDoublePreservesDataType() {
        final long[] values = new long[512];
        for (int i = 0; i < 512; i++) {
            values[i] = (i % 100) * 50L;
        }
        final PipelineConfig config = selector.select(profiler.profile(values, 512), 512, PipelineConfig.DataType.DOUBLE, null);

        assertEquals(PipelineConfig.DataType.DOUBLE, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Gcd.class)));
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

        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.LONG, null);
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Gcd.class)));
    }

    public void testRleDoublePreservesDataType() {
        final long[] values = new long[512];
        for (int i = 0; i < 512; i++) {
            values[i] = i / 256;
        }
        final PipelineConfig config = selector.select(profiler.profile(values, 512), 512, PipelineConfig.DataType.DOUBLE, null);

        assertEquals(PipelineConfig.DataType.DOUBLE, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Rle.class)));
    }
}
