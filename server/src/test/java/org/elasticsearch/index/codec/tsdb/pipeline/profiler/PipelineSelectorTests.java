/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.profiler;

import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.codec.tsdb.pipeline.NumericDataGenerators;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineResolver;
import org.elasticsearch.index.codec.tsdb.pipeline.StageSpec;
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

        assertFalse(config.isDefault());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Delta.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Offset.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Gcd.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.PatchedPFor.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Rle.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.BitPack.class)));
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

    public void testHighRunRatioGcdOneSelectsWideDefault() {
        final BlockProfile profile = new BlockProfile(512, 0L, 100L, 100L, 1L, 1L, false, false, 510, 7, 7, 2, 10);
        final PipelineConfig config = selector.select(profile, 512, PipelineConfig.DataType.LONG, null, null);

        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Delta.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Offset.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Gcd.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.PatchedPFor.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Rle.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.BitPack.class)));
    }

    public void testConstantDoublePreservesDataType() {
        final long[] values = new long[512];
        Arrays.fill(values, 42L);
        final PipelineConfig config = selector.select(profiler.profile(values, 512), 512, PipelineConfig.DataType.DOUBLE, null, null);

        assertEquals(PipelineConfig.DataType.DOUBLE, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Offset.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Gcd.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Rle.class)));
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

    public void testEncodedSizeDeltaDeltaTimestamp() throws IOException {
        final long[] values = new long[BS];
        final long base = 1700000000000L;
        for (int i = 0; i < BS; i++) {
            values[i] = base + i * 1000L;
        }
        final PipelineConfig config = PipelineConfig.forLongs(BS).deltaDelta().offset().gcd().patchedPFor().bitPack();
        final int bytes = measureAndLog("deltadelta-timestamp", config, values);
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
        final int chimpBytes = measureAndLog(
            "chimp-randomwalk-double",
            PipelineConfig.forDoubles(BS).chimpDoubleStage().offset().gcd().bitPack(),
            values
        );
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
        final int chimpBytes = measureAndLog(
            "chimp-monotonic-double",
            PipelineConfig.forDoubles(BS).chimpDoubleStage().offset().gcd().bitPack(),
            values
        );
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
        final PipelineConfig deflt = PipelineConfig.forDoubles(BS).alpDoubleStage(1e-6).offset().gcd().bitPack();
        final PipelineConfig speed = PipelineConfig.forDoubles(BS).alpDoubleStage().offset().gcd().bitPack();

        final int storageBytes = measureAndLog("alp-double-storage(1e-2)", storage, values);
        final int balancedBytes = measureAndLog("alp-double-balanced(1e-4)", balanced, values);
        final int defaultBytes = measureAndLog("alp-double-default(1e-6)", deflt, values);
        final int speedBytes = measureAndLog("alp-double-speed(lossless)", speed, values);

        assertTrue("storage should compress, got " + storageBytes, storageBytes < RAW_LONG_BYTES);
        assertTrue("more quantization should produce smaller output", storageBytes <= balancedBytes);
        assertTrue("more quantization should produce smaller output", balancedBytes <= defaultBytes);
        assertTrue("more quantization should produce smaller output", defaultBytes <= speedBytes);
    }

    public void testEncodedSizeAlpFloatAllHints() throws IOException {
        final long[] values = decimalFloats(BS, 20.0f, 5.0f);
        final PipelineConfig storage = PipelineConfig.forFloats(BS).alpFloatStage(1e-2).offset().gcd().bitPack();
        final PipelineConfig balanced = PipelineConfig.forFloats(BS).alpFloatStage(1e-4).offset().gcd().bitPack();
        final PipelineConfig deflt = PipelineConfig.forFloats(BS).alpFloatStage(1e-6).offset().gcd().bitPack();
        final PipelineConfig speed = PipelineConfig.forFloats(BS).alpFloatStage().offset().gcd().bitPack();

        final int storageBytes = measureAndLog("alp-float-storage(1e-2)", storage, values);
        final int balancedBytes = measureAndLog("alp-float-balanced(1e-4)", balanced, values);
        final int defaultBytes = measureAndLog("alp-float-default(1e-6)", deflt, values);
        final int speedBytes = measureAndLog("alp-float-speed(lossless)", speed, values);

        assertTrue("storage should compress, got " + storageBytes, storageBytes < RAW_LONG_BYTES);
        assertTrue("more quantization should produce smaller output", storageBytes <= balancedBytes);
        assertTrue("more quantization should produce smaller output", balancedBytes <= defaultBytes);
        assertTrue("more quantization should produce smaller output", defaultBytes <= speedBytes);
    }

    public void testEncodedSizeXorVsAlpComparison() throws IOException {
        final long[] randomWalk = smoothDoubles(BS, 100.0, 0.1);
        final long[] monotonic = monotonicDoubles(BS, 1000.0, 0.5);
        final long[] decimal = decimalDoubles(BS, 20.0, 5.0);

        final PipelineConfig gorilla = PipelineConfig.forDoubles(BS).gorilla();
        final PipelineConfig chimp = PipelineConfig.forDoubles(BS).chimpDoubleStage().offset().gcd().bitPack();
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
