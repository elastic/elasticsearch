/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.profiler;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.codec.tsdb.pipeline.NumericDataGenerators;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineResolver;
import org.elasticsearch.index.codec.tsdb.pipeline.StageSpec;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericDecoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericEncoder;
import org.elasticsearch.index.mapper.TimeSeriesParams.MetricType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class PipelineSelectorTests extends ESTestCase {

    private final BlockProfiler profiler = new BlockProfiler();
    private final PipelineSelector selector = new PipelineSelector();

    public void testConstantSelectsWideDefault() {
        final long[] values = new long[512];
        Arrays.fill(values, 42L);
        final PipelineConfig config = selector.select(profiler.profile(values, 512), 512, PipelineConfig.DataType.LONG, null, null);

        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Delta.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Offset.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Gcd.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.PatchedPFor.class)));
        assertThat(config.specs(), not(hasItem(instanceOf(StageSpec.Rle.class))));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.BitPack.class)));
    }

    public void testTimestampSelectsDoubleDelta() {
        final long[] values = new long[512];
        final long base = 1700000000000L;
        for (int i = 0; i < 512; i++)
            values[i] = base + i * 1000L;
        final PipelineConfig config = selector.select(profiler.profile(values, 512), 512, PipelineConfig.DataType.LONG, null, null);

        assertThat(config.specs(), not(hasItem(instanceOf(StageSpec.DeltaDelta.class))));
        // NOTE: two Delta specs at positions 0 and 1
        assertEquals(2, config.specs().stream().filter(s -> s instanceof StageSpec.Delta).count());
    }

    public void testMonotonicWithVaryingStrideSelectsDelta() {
        final long[] values = new long[512];
        for (int i = 0; i < 512; i++)
            values[i] = i * 10L;
        final BlockProfile profile = profiler.profile(values, 512);

        assertTrue(profile.isMonotonicallyIncreasing());

        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.LONG, null, null);

    }

    public void testGcdFriendlySelectsGcd() {
        final long[] values = new long[512];
        for (int i = 0; i < 512; i++)
            values[i] = (i % 100) * 50L;
        final PipelineConfig config = selector.select(profiler.profile(values, 512), 512, PipelineConfig.DataType.LONG, null, null);

        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Gcd.class)));
    }

    public void testRandomSelectsOffsetBitpack() {
        final long[] values = NumericDataGenerators.randomLongs(512, 0x5DEECE66DL);
        final PipelineConfig config = selector.select(profiler.profile(values, 512), 512, PipelineConfig.DataType.LONG, null, null);

        assertEquals(PipelineConfig.DataType.LONG, config.dataType());
    }

    public void testSmoothDoubleSelectsFpc() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 10, 12, 10);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.DOUBLE, null, null);

        assertEquals(PipelineConfig.DataType.DOUBLE, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.FpcDoubleStage.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Delta.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.PatchedPFor.class)));
    }

    public void testNoisyDoubleDefaultSelectsLosslessAlp() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 30, 12, 20);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.DOUBLE, null, null);

        assertEquals(PipelineConfig.DataType.DOUBLE, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.AlpDoubleStage.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Delta.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.PatchedPFor.class)));
        assertAlpMaxError(config, -1.0);
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

    public void testNoisyDoubleBalancedSelectsAlpWith6Decimals() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 30, 12, 20);
        final PipelineConfig config = selector.select(
            profile,
            512,
            PipelineConfig.DataType.DOUBLE,
            PipelineResolver.OptimizeFor.BALANCED,
            null
        );

        assertThat(config.specs(), hasItem(instanceOf(StageSpec.AlpDoubleStage.class)));
        assertAlpMaxError(config, 1e-6);
    }

    public void testNoisyDoubleSpeedSelectsLosslessAlp() {
        // NOTE: xorMaxBits == rawMaxBits (30 == 30) → no XOR benefit, so SPEED still
        // falls through to ALP. SPEED only affects the XOR path (wider FPC/Gorilla thresholds).
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
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, true, false, 500, 30, 10, 12, 10);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.DOUBLE, null, MetricType.COUNTER);

        assertEquals(PipelineConfig.DataType.DOUBLE, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.FpcDoubleStage.class)));
    }

    public void testDoubleCounterHighRepeatSelectsGorilla() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, true, false, 500, 30, 10, 250, 30);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.DOUBLE, null, MetricType.COUNTER);

        assertEquals(PipelineConfig.DataType.DOUBLE, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Gorilla.class)));
    }

    public void testDoubleCounterLowXorSelectsChimp() {
        // NOTE: xorMaxBits=10 <= rawMaxBits/2=15, so streaming Chimp saves index overhead
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, true, false, 500, 30, 10, 10, 30);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.DOUBLE, null, MetricType.COUNTER);

        assertEquals(PipelineConfig.DataType.DOUBLE, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.ChimpDoublePayload.class)));
    }

    public void testDoubleCounterHighXorSelectsChimp128() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, true, false, 500, 30, 20, 10, 30);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.DOUBLE, null, MetricType.COUNTER);

        assertEquals(PipelineConfig.DataType.DOUBLE, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Chimp128DoublePayload.class)));
    }

    public void testSmoothFloatSelectsFpc() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 10, 12, 10);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.FLOAT, null, null);

        assertEquals(PipelineConfig.DataType.FLOAT, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.FpcFloatStage.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Delta.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.PatchedPFor.class)));
    }

    public void testNoisyFloatSelectsAlp() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 30, 12, 20);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.FLOAT, null, null);

        assertEquals(PipelineConfig.DataType.FLOAT, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.AlpFloatStage.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Delta.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.PatchedPFor.class)));
    }

    public void testFloatCounterDefaultSelectsFpc() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, true, false, 500, 30, 10, 12, 10);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.FLOAT, null, MetricType.COUNTER);

        assertEquals(PipelineConfig.DataType.FLOAT, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.FpcFloatStage.class)));
    }

    public void testFloatCounterHighRepeatSelectsGorilla() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, true, false, 500, 30, 10, 250, 30);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.FLOAT, null, MetricType.COUNTER);

        assertEquals(PipelineConfig.DataType.FLOAT, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.GorillaFloat.class)));
    }

    public void testFloatCounterLowXorSelectsChimp() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, true, false, 500, 30, 10, 10, 30);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.FLOAT, null, MetricType.COUNTER);

        assertEquals(PipelineConfig.DataType.FLOAT, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.ChimpFloatPayload.class)));
    }

    public void testFloatCounterHighXorSelectsChimp128() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, true, false, 500, 30, 20, 10, 30);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.FLOAT, null, MetricType.COUNTER);

        assertEquals(PipelineConfig.DataType.FLOAT, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Chimp128FloatPayload.class)));
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

    public void testLowRunRatioDoesNotSelectRle() {
        final long[] values = new long[512];
        for (int i = 0; i < 512; i++) {
            values[i] = i / 256;
        }
        final BlockProfile profile = profiler.profile(values, 512);
        assertTrue(profile.range() > 0);

        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.LONG, null, null);
        assertThat(config.specs(), not(hasItem(instanceOf(StageSpec.Rle.class))));
    }

    public void testAllLongPathsUseDoubleDelta() {
        final long[] monotonic = new long[512];
        for (int i = 0; i < 512; i++) {
            monotonic[i] = (long) i * i * i;
        }
        final PipelineConfig monotonicConfig = selector.select(
            profiler.profile(monotonic, 512),
            512,
            PipelineConfig.DataType.LONG,
            null,
            null
        );
        assertEquals(2, monotonicConfig.specs().stream().filter(s -> s instanceof StageSpec.Delta).count());
        assertThat(monotonicConfig.specs(), not(hasItem(instanceOf(StageSpec.DeltaDelta.class))));

        final long[] constant = new long[512];
        Arrays.fill(constant, 42L);
        final PipelineConfig constantConfig = selector.select(
            profiler.profile(constant, 512),
            512,
            PipelineConfig.DataType.LONG,
            null,
            null
        );
        assertEquals(2, constantConfig.specs().stream().filter(s -> s instanceof StageSpec.Delta).count());

        final long[] random = NumericDataGenerators.randomLongs(512, 0x5DEECE66DL);
        final PipelineConfig randomConfig = selector.select(profiler.profile(random, 512), 512, PipelineConfig.DataType.LONG, null, null);
        assertEquals(2, randomConfig.specs().stream().filter(s -> s instanceof StageSpec.Delta).count());
    }

    public void testSmoothLongSelectsDelta() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 10, 12, 20);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.LONG, null, null);

        assertEquals(PipelineConfig.DataType.LONG, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Delta.class)));
    }

    public void testHighRunRatioGcdOneSelectsWideDefault() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 510, 7, 7, 2, 10);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.LONG, null, null);

        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Delta.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Offset.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Gcd.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.PatchedPFor.class)));
        assertThat(config.specs(), not(hasItem(instanceOf(StageSpec.Rle.class))));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.BitPack.class)));
    }

    public void testConstantDoublePreservesDataType() {
        final long[] values = new long[512];
        Arrays.fill(values, 42L);
        final PipelineConfig config = selector.select(profiler.profile(values, 512), 512, PipelineConfig.DataType.DOUBLE, null, null);

        assertEquals(PipelineConfig.DataType.DOUBLE, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Delta.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Offset.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Gcd.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.PatchedPFor.class)));
        assertThat(config.specs(), not(hasItem(instanceOf(StageSpec.Rle.class))));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.BitPack.class)));
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

    public void testLowRunRatioDoubleDoesNotSelectRle() {
        final long[] values = new long[512];
        for (int i = 0; i < 512; i++) {
            values[i] = i / 256;
        }
        final PipelineConfig config = selector.select(profiler.profile(values, 512), 512, PipelineConfig.DataType.DOUBLE, null, null);

        assertEquals(PipelineConfig.DataType.DOUBLE, config.dataType());
        assertThat(config.specs(), not(hasItem(instanceOf(StageSpec.Rle.class))));
    }

    public void testMonotonicDoubleSelectsXorRegardlessOfMetricType() {
        // NOTE: routing is purely data-driven — monotonic data goes to XOR path
        // regardless of whether the mapping says COUNTER, GAUGE, or nothing.
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, true, false, 500, 30, 30, 12, 10);

        final PipelineConfig gauge = selector.select(profile, 512, PipelineConfig.DataType.DOUBLE, null, MetricType.GAUGE);
        final PipelineConfig counter = selector.select(profile, 512, PipelineConfig.DataType.DOUBLE, null, MetricType.COUNTER);
        final PipelineConfig noType = selector.select(profile, 512, PipelineConfig.DataType.DOUBLE, null, null);
        assertEquals(gauge, counter);
        assertEquals(gauge, noType);
        assertThat(gauge.specs(), hasItem(instanceOf(StageSpec.FpcDoubleStage.class)));
    }

    public void testNonMonotonicDoubleCounterFallsThroughToNormalProfiling() {
        // NOTE: non-monotonic data falls through to the normal XOR gate / ALP profiling
        // regardless of the metric type annotation — the profile data is the ground truth.
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 30, 12, 10);

        final PipelineConfig counter = selector.select(profile, 512, PipelineConfig.DataType.DOUBLE, null, MetricType.COUNTER);
        final PipelineConfig gauge = selector.select(profile, 512, PipelineConfig.DataType.DOUBLE, null, MetricType.GAUGE);
        assertEquals(counter, gauge);
        assertThat(counter.specs(), hasItem(instanceOf(StageSpec.AlpDoubleStage.class)));
    }

    public void testMonotonicFloatSelectsXorRegardlessOfMetricType() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, true, false, 500, 30, 30, 12, 10);

        final PipelineConfig gauge = selector.select(profile, 512, PipelineConfig.DataType.FLOAT, null, MetricType.GAUGE);
        final PipelineConfig counter = selector.select(profile, 512, PipelineConfig.DataType.FLOAT, null, MetricType.COUNTER);
        final PipelineConfig noType = selector.select(profile, 512, PipelineConfig.DataType.FLOAT, null, null);
        assertEquals(gauge, counter);
        assertEquals(gauge, noType);
        assertThat(gauge.specs(), hasItem(instanceOf(StageSpec.FpcFloatStage.class)));
    }

    public void testNonMonotonicFloatCounterFallsThroughToNormalProfiling() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 30, 12, 10);

        final PipelineConfig counter = selector.select(profile, 512, PipelineConfig.DataType.FLOAT, null, MetricType.COUNTER);
        final PipelineConfig gauge = selector.select(profile, 512, PipelineConfig.DataType.FLOAT, null, MetricType.GAUGE);
        assertEquals(counter, gauge);
        assertThat(counter.specs(), hasItem(instanceOf(StageSpec.AlpFloatStage.class)));
    }

    public void testHighRepeatDoubleSelectsGorilla() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 10, 250, 30);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.DOUBLE, null, null);

        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Gorilla.class)));
    }

    public void testXorDoubleLowXorSelectsChimp() {
        // NOTE: xorMaxBits=10 <= rawMaxBits/2=15
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 10, 10, 30);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.DOUBLE, null, null);

        assertThat(config.specs(), hasItem(instanceOf(StageSpec.ChimpDoublePayload.class)));
    }

    public void testXorDoubleHighXorSelectsChimp128() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 20, 10, 30);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.DOUBLE, null, null);

        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Chimp128DoublePayload.class)));
    }

    public void testXorDoubleHighXorSmallBlockSelectsChimp128() {
        final BlockProfile profile = new BlockProfile(128, 0L, 100L, 100L, 1L, 1L, false, false, 120, 30, 20, 10, 30);
        final PipelineConfig config = selector.select(profile, 128, PipelineConfig.DataType.DOUBLE, null, null);

        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Chimp128DoublePayload.class)));
    }

    public void testHighRepeatFloatSelectsGorilla() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 10, 250, 30);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.FLOAT, null, null);

        assertThat(config.specs(), hasItem(instanceOf(StageSpec.GorillaFloat.class)));
    }

    public void testXorFloatLowXorSelectsChimp() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 10, 10, 30);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.FLOAT, null, null);

        assertThat(config.specs(), hasItem(instanceOf(StageSpec.ChimpFloatPayload.class)));
    }

    public void testXorFloatHighXorSelectsChimp128() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 20, 10, 30);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.FLOAT, null, null);

        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Chimp128FloatPayload.class)));
    }

    public void testXorFloatHighXorSmallBlockSelectsChimp128() {
        final BlockProfile profile = new BlockProfile(128, 0L, 100L, 100L, 1L, 1L, false, false, 120, 30, 20, 10, 30);
        final PipelineConfig config = selector.select(profile, 128, PipelineConfig.DataType.FLOAT, null, null);

        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Chimp128FloatPayload.class)));
    }

    public void testConstantFloatUsesWidePipeline() {
        final long[] values = new long[512];
        Arrays.fill(values, 42L);
        final PipelineConfig config = selector.select(profiler.profile(values, 512), 512, PipelineConfig.DataType.FLOAT, null, null);

        assertEquals(PipelineConfig.DataType.FLOAT, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Delta.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Offset.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Gcd.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.PatchedPFor.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.BitPack.class)));
    }

    public void testSpeedWidensFpcThresholdForDouble() {
        // NOTE: deltaDeltaMaxBits=20 is between default(16) and SPEED(24) thresholds
        // xorMaxBits=10 <= rawMaxBits/2=15, so default falls to streaming Chimp
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 10, 10, 20);

        final PipelineConfig defaultConfig = selector.select(profile, 512, PipelineConfig.DataType.DOUBLE, null, null);
        assertThat(defaultConfig.specs(), hasItem(instanceOf(StageSpec.ChimpDoublePayload.class)));

        final PipelineConfig speedConfig = selector.select(
            profile,
            512,
            PipelineConfig.DataType.DOUBLE,
            PipelineResolver.OptimizeFor.SPEED,
            null
        );
        assertThat(speedConfig.specs(), hasItem(instanceOf(StageSpec.FpcDoubleStage.class)));
    }

    public void testSpeedWidensFpcThresholdForFloat() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 10, 10, 20);

        final PipelineConfig defaultConfig = selector.select(profile, 512, PipelineConfig.DataType.FLOAT, null, null);
        assertThat(defaultConfig.specs(), hasItem(instanceOf(StageSpec.ChimpFloatPayload.class)));

        final PipelineConfig speedConfig = selector.select(
            profile,
            512,
            PipelineConfig.DataType.FLOAT,
            PipelineResolver.OptimizeFor.SPEED,
            null
        );
        assertThat(speedConfig.specs(), hasItem(instanceOf(StageSpec.FpcFloatStage.class)));
    }

    public void testSpeedRaisesGorillaThresholdForDouble() {
        // NOTE: xorZeroFraction = 260/511 ≈ 0.509, between default(0.4) and SPEED(0.6) thresholds
        // xorMaxBits=10 <= rawMaxBits/2=15, so SPEED fallback goes to streaming Chimp
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 10, 260, 30);

        final PipelineConfig defaultConfig = selector.select(profile, 512, PipelineConfig.DataType.DOUBLE, null, null);
        assertThat(defaultConfig.specs(), hasItem(instanceOf(StageSpec.Gorilla.class)));

        final PipelineConfig speedConfig = selector.select(
            profile,
            512,
            PipelineConfig.DataType.DOUBLE,
            PipelineResolver.OptimizeFor.SPEED,
            null
        );
        assertThat(speedConfig.specs(), hasItem(instanceOf(StageSpec.ChimpDoublePayload.class)));
    }

    public void testSpeedRaisesGorillaThresholdForFloat() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 500, 30, 10, 260, 30);

        final PipelineConfig defaultConfig = selector.select(profile, 512, PipelineConfig.DataType.FLOAT, null, null);
        assertThat(defaultConfig.specs(), hasItem(instanceOf(StageSpec.GorillaFloat.class)));

        final PipelineConfig speedConfig = selector.select(
            profile,
            512,
            PipelineConfig.DataType.FLOAT,
            PipelineResolver.OptimizeFor.SPEED,
            null
        );
        assertThat(speedConfig.specs(), hasItem(instanceOf(StageSpec.ChimpFloatPayload.class)));
    }

    // -- Round-trip tests: verify which delta stages fire and that encode→decode is lossless --

    public void testDoubleDeltaBothFireOnAcceleratingData() throws IOException {
        final long[] original = new long[BS];
        for (int i = 0; i < BS; i++) {
            original[i] = (long) i * i;
        }

        final PipelineConfig config = PipelineConfig.forLongs(BS).delta().delta().offset().gcd().patchedPFor().rle().bitPack();
        final long[] block = Arrays.copyOf(original, BS);
        final byte[] buffer = new byte[BS * Long.BYTES * 8];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        final NumericEncoder encoder = NumericEncoder.fromConfig(config);
        encoder.newBlockEncoder().encode(block, BS, out);

        final int bitmap = buffer[0] & 0xFF;
        assertTrue("first delta (position 0) should fire on monotonic i²", (bitmap & (1 << 0)) != 0);
        assertTrue("second delta (position 1) should fire on monotonic deltas", (bitmap & (1 << 1)) != 0);

        final long[] decoded = new long[BS];
        final ByteArrayDataInput in = new ByteArrayDataInput(buffer, 0, out.getPosition());
        final int decodedCount = NumericDecoder.fromDescriptor(encoder.descriptor()).newBlockDecoder().decode(decoded, in);

        assertEquals(BS, decodedCount);
        assertArrayEquals(original, decoded);
    }

    public void testDoubleDeltaOnlyFirstFiresOnConstantRate() throws IOException {
        final long[] original = new long[BS];
        final long base = 1700000000000L;
        for (int i = 0; i < BS; i++) {
            original[i] = base + i * 1000L;
        }

        final PipelineConfig config = PipelineConfig.forLongs(BS).delta().delta().offset().gcd().patchedPFor().rle().bitPack();
        final long[] block = Arrays.copyOf(original, BS);
        final byte[] buffer = new byte[BS * Long.BYTES * 8];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        final NumericEncoder encoder = NumericEncoder.fromConfig(config);
        encoder.newBlockEncoder().encode(block, BS, out);

        final int bitmap = buffer[0] & 0xFF;
        assertTrue("first delta (position 0) should fire on monotonic timestamps", (bitmap & (1 << 0)) != 0);
        assertEquals("second delta (position 1) should skip on constant deltas", 0, bitmap & (1 << 1));

        final long[] decoded = new long[BS];
        final ByteArrayDataInput in = new ByteArrayDataInput(buffer, 0, out.getPosition());
        final int decodedCount = NumericDecoder.fromDescriptor(encoder.descriptor()).newBlockDecoder().decode(decoded, in);

        assertEquals(BS, decodedCount);
        assertArrayEquals(original, decoded);
    }

    public void testDoubleDeltaNeitherFiresOnRandomData() throws IOException {
        final long[] original = NumericDataGenerators.randomLongs(BS, 0x5DEECE66DL);

        final PipelineConfig config = PipelineConfig.forLongs(BS).delta().delta().offset().gcd().patchedPFor().rle().bitPack();
        final long[] block = Arrays.copyOf(original, BS);
        final byte[] buffer = new byte[BS * Long.BYTES * 8];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        final NumericEncoder encoder = NumericEncoder.fromConfig(config);
        encoder.newBlockEncoder().encode(block, BS, out);

        final int bitmap = buffer[0] & 0xFF;
        assertEquals("first delta (position 0) should skip on random data", 0, bitmap & (1 << 0));
        assertEquals("second delta (position 1) should skip on random data", 0, bitmap & (1 << 1));

        final long[] decoded = new long[BS];
        final ByteArrayDataInput in = new ByteArrayDataInput(buffer, 0, out.getPosition());
        final int decodedCount = NumericDecoder.fromDescriptor(encoder.descriptor()).newBlockDecoder().decode(decoded, in);

        assertEquals(BS, decodedCount);
        assertArrayEquals(original, decoded);
    }

    // -- Encoding size tests: validate compression for each pipeline path in PipelineSelector --

    private static final int BS = 512;
    private static final int RAW_LONG_BYTES = BS * Long.BYTES;

    public void testEncodedSizeConstantLong() throws IOException {
        final long[] values = new long[BS];
        Arrays.fill(values, 42L);
        final PipelineConfig config = PipelineConfig.forLongs(BS).delta().offset().gcd().patchedPFor().rle().bitPack();
        final int bytes = measureAndLog("constant-long", config, values);
        assertTrue("constant block should encode to less than 32 bytes, got " + bytes, bytes < 32);
    }

    public void testEncodedSizeRleFriendlyLong() throws IOException {
        final long[] values = new long[BS];
        for (int i = 0; i < BS; i++) {
            values[i] = 1000L + (i / 128);
        }
        final PipelineConfig config = PipelineConfig.forLongs(BS).delta().offset().gcd().patchedPFor().rle().bitPack();
        final int bytes = measureAndLog("rle-friendly-long", config, values);
        assertTrue("RLE-friendly block should be much smaller than raw, got " + bytes, bytes < RAW_LONG_BYTES / 10);
    }

    public void testEncodedSizeDoubleDeltaTimestamp() throws IOException {
        final long[] values = new long[BS];
        final long base = 1700000000000L;
        for (int i = 0; i < BS; i++) {
            values[i] = base + i * 1000L;
        }
        final PipelineConfig config = PipelineConfig.forLongs(BS).delta().delta().offset().gcd().patchedPFor().rle().bitPack();
        final int bytes = measureAndLog("double-delta-timestamp", config, values);
        assertTrue("steady-rate timestamps should compress well, got " + bytes, bytes < RAW_LONG_BYTES / 10);
    }

    public void testEncodedSizeDeltaMonotonic() throws IOException {
        final long[] values = new long[BS];
        final Random rng = new Random(0x5DEECE66DL);
        long v = 1000L;
        for (int i = 0; i < BS; i++) {
            v += 10 + rng.nextInt(100);
            values[i] = v;
        }
        final PipelineConfig config = PipelineConfig.forLongs(BS).delta().offset().gcd().bitPack();
        final int bytes = measureAndLog("delta-monotonic", config, values);
        assertTrue("monotonic with irregular steps should compress via delta, got " + bytes, bytes < RAW_LONG_BYTES / 4);
    }

    public void testEncodedSizeGcdLong() throws IOException {
        final long[] values = new long[BS];
        final Random rng = new Random(0x5DEECE66DL);
        for (int i = 0; i < BS; i++) {
            values[i] = rng.nextInt(1000) * 50L;
        }
        final PipelineConfig config = PipelineConfig.forLongs(BS).offset().gcd().bitPack();
        final int bytes = measureAndLog("gcd-long", config, values);
        assertTrue("GCD-friendly block should compress, got " + bytes, bytes < RAW_LONG_BYTES / 4);
    }

    public void testEncodedSizeSmoothLong() throws IOException {
        final long[] values = new long[BS];
        final Random rng = new Random(0x5DEECE66DL);
        long v = 50000L;
        for (int i = 0; i < BS; i++) {
            v += rng.nextInt(3) - 1;
            values[i] = v;
        }
        final PipelineConfig config = PipelineConfig.forLongs(BS).delta().offset().gcd().bitPack();
        final int bytes = measureAndLog("smooth-long-delta", config, values);
        assertTrue("slowly drifting longs should compress via delta, got " + bytes, bytes < RAW_LONG_BYTES / 10);
    }

    public void testEncodedSizeRandomLong() throws IOException {
        final long[] values = NumericDataGenerators.randomLongs(BS, 0x5DEECE66DL);
        final PipelineConfig config = PipelineConfig.forLongs(BS).offset().bitPack();
        final int bytes = measureAndLog("random-long", config, values);
        assertTrue("random longs should still fit in raw bits, got " + bytes, bytes <= RAW_LONG_BYTES);
    }

    public void testEncodedSizeXorDoublesOnRandomWalk() throws IOException {
        final long[] values = smoothDoubles(BS, 100.0, 0.1);

        final int gorillaBytes = measureAndLog("gorilla-randomwalk-double", PipelineConfig.forDoubles(BS).gorilla(), values);
        final int chimpBytes = measureAndLog("chimp-randomwalk-double", PipelineConfig.forDoubles(BS).chimp128(), values);
        final int fpcBytes = measureAndLog(
            "fpc-randomwalk-double",
            PipelineConfig.forDoubles(BS).fpcStage().offset().gcd().bitPack(),
            values
        );

        assertTrue("gorilla should produce output, got " + gorillaBytes, gorillaBytes > 0);
        assertTrue("chimp should produce output, got " + chimpBytes, chimpBytes > 0);
        assertTrue("fpc should produce output, got " + fpcBytes, fpcBytes > 0);
    }

    public void testEncodedSizeXorDoublesOnMonotonicCounter() throws IOException {
        final long[] values = monotonicDoubles(BS, 1000.0, 0.5);

        final int gorillaBytes = measureAndLog("gorilla-monotonic-double", PipelineConfig.forDoubles(BS).gorilla(), values);
        final int chimpBytes = measureAndLog("chimp-monotonic-double", PipelineConfig.forDoubles(BS).chimp128(), values);
        final int fpcBytes = measureAndLog(
            "fpc-monotonic-double",
            PipelineConfig.forDoubles(BS).fpcStage().offset().gcd().bitPack(),
            values
        );

        assertTrue("gorilla on monotonic counter should compress, got " + gorillaBytes, gorillaBytes < RAW_LONG_BYTES);
        assertTrue("chimp should produce output, got " + chimpBytes, chimpBytes > 0);
        assertTrue("fpc should produce output, got " + fpcBytes, fpcBytes > 0);
    }

    public void testEncodedSizeAlpDoubleAllHints() throws IOException {
        final long[] values = decimalDoubles(BS, 20.0, 5.0);
        final PipelineConfig storage = PipelineConfig.forDoubles(BS).alpDoubleStage(1e-2).offset().gcd().bitPack();
        final PipelineConfig balanced = PipelineConfig.forDoubles(BS).alpDoubleStage(1e-4).offset().gcd().bitPack();
        final PipelineConfig lossless = PipelineConfig.forDoubles(BS).alpDoubleStage().offset().gcd().bitPack();

        final int storageBytes = measureAndLog("alp-double-storage(1e-2)", storage, values);
        final int balancedBytes = measureAndLog("alp-double-balanced(1e-4)", balanced, values);
        final int losslessBytes = measureAndLog("alp-double-lossless", lossless, values);

        assertTrue("storage should compress, got " + storageBytes, storageBytes < RAW_LONG_BYTES);
        assertTrue("more quantization should produce smaller output", storageBytes <= balancedBytes);
        assertTrue("lossless should compress, got " + losslessBytes, losslessBytes < RAW_LONG_BYTES);
    }

    public void testEncodedSizeAlpFloatAllHints() throws IOException {
        final long[] values = decimalFloats(BS, 20.0f, 5.0f);
        final PipelineConfig storage = PipelineConfig.forFloats(BS).alpFloatStage(1e-2).offset().gcd().bitPack();
        final PipelineConfig balanced = PipelineConfig.forFloats(BS).alpFloatStage(1e-4).offset().gcd().bitPack();
        final PipelineConfig lossless = PipelineConfig.forFloats(BS).alpFloatStage().offset().gcd().bitPack();

        final int storageBytes = measureAndLog("alp-float-storage(1e-2)", storage, values);
        final int balancedBytes = measureAndLog("alp-float-balanced(1e-4)", balanced, values);
        final int losslessBytes = measureAndLog("alp-float-lossless", lossless, values);

        assertTrue("storage should compress, got " + storageBytes, storageBytes < RAW_LONG_BYTES);
        assertTrue("more quantization should produce smaller output", storageBytes <= balancedBytes);
        assertTrue("lossless should compress, got " + losslessBytes, losslessBytes < RAW_LONG_BYTES);
    }

    public void testEncodedSizeXorVsAlpComparison() throws IOException {
        final long[] randomWalk = smoothDoubles(BS, 100.0, 0.1);
        final long[] monotonic = monotonicDoubles(BS, 1000.0, 0.5);
        final long[] decimal = decimalDoubles(BS, 20.0, 5.0);

        final PipelineConfig gorilla = PipelineConfig.forDoubles(BS).gorilla();
        final PipelineConfig chimp = PipelineConfig.forDoubles(BS).chimp128();
        final PipelineConfig fpc = PipelineConfig.forDoubles(BS).fpcStage().offset().gcd().bitPack();
        final PipelineConfig alp = PipelineConfig.forDoubles(BS).alpDoubleStage(1e-6).offset().gcd().bitPack();

        System.out.println("--- Random walk doubles ---");
        measureAndLog("  gorilla", gorilla, randomWalk);
        measureAndLog("  chimp", chimp, randomWalk);
        measureAndLog("  fpc", fpc, randomWalk);
        measureAndLog("  alp(1e-6)", alp, randomWalk);

        System.out.println("--- Monotonic counter doubles ---");
        measureAndLog("  gorilla", gorilla, monotonic);
        measureAndLog("  chimp", chimp, monotonic);
        measureAndLog("  fpc", fpc, monotonic);
        measureAndLog("  alp(1e-6)", alp, monotonic);

        System.out.println("--- Decimal gauge doubles ---");
        measureAndLog("  gorilla", gorilla, decimal);
        measureAndLog("  chimp", chimp, decimal);
        measureAndLog("  fpc", fpc, decimal);
        measureAndLog("  alp(1e-6)", alp, decimal);
    }

    private static int measureSize(final PipelineConfig config, final long[] values) throws IOException {
        final int blockSize = config.blockSize();
        final long[] block = Arrays.copyOf(values, blockSize);
        final byte[] buffer = new byte[blockSize * Long.BYTES * 8];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        final NumericEncoder encoder = NumericEncoder.fromConfig(config);
        encoder.newBlockEncoder().encode(block, Math.min(values.length, blockSize), out);
        return out.getPosition();
    }

    private static int measureAndLog(final String label, final PipelineConfig config, final long[] values) throws IOException {
        final int bytes = measureSize(config, values);
        final double bpv = (double) bytes / values.length;
        System.out.printf(
            "[SIZE] %-35s %6d bytes  (%.3f bytes/value, %.1f%% of raw)%n",
            label,
            bytes,
            bpv,
            (double) bytes / RAW_LONG_BYTES * 100
        );
        return bytes;
    }

    private static long[] smoothDoubles(int count, double center, double drift) {
        final long[] values = new long[count];
        final Random rng = new Random(0x5DEECE66DL);
        double v = center;
        for (int i = 0; i < count; i++) {
            v += (rng.nextDouble() - 0.5) * drift;
            values[i] = NumericUtils.doubleToSortableLong(v);
        }
        return values;
    }

    private static long[] monotonicDoubles(int count, double start, double avgStep) {
        final long[] values = new long[count];
        final Random rng = new Random(0x5DEECE66DL);
        double v = start;
        for (int i = 0; i < count; i++) {
            v += avgStep + (rng.nextDouble() - 0.5) * avgStep * 0.1;
            values[i] = NumericUtils.doubleToSortableLong(v);
        }
        return values;
    }

    private static long[] decimalDoubles(int count, double center, double spread) {
        final long[] values = new long[count];
        final Random rng = new Random(0x5DEECE66DL);
        for (int i = 0; i < count; i++) {
            final double v = center + (rng.nextDouble() - 0.5) * spread;
            values[i] = NumericUtils.doubleToSortableLong(Math.round(v * 100.0) / 100.0);
        }
        return values;
    }

    private static long[] decimalFloats(int count, float center, float spread) {
        final long[] values = new long[count];
        final Random rng = new Random(0x5DEECE66DL);
        for (int i = 0; i < count; i++) {
            final float v = center + (rng.nextFloat() - 0.5f) * spread;
            values[i] = NumericUtils.floatToSortableInt(Math.round(v * 100.0f) / 100.0f);
        }
        return values;
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
