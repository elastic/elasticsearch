/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import org.apache.lucene.util.NumericUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.IntFunction;

public final class NumericDataGenerators {

    private NumericDataGenerators() {}

    static final long DEFAULT_SEED = 0x5DEECE66DL;

    // NOTE: Deterministic, pseudo-random double value in [0, 1).
    public static double hash(int i, long seed) {
        long h = (i * 6364136223846793005L + seed) ^ 0x5DEECE66DL;
        h = h ^ (h >>> 33);
        h = h * 0xff51afd7ed558ccdL;
        h = h ^ (h >>> 33);
        return (double) (h & 0x7fffffffffffffL) / (double) 0x80000000000000L;
    }

    public static double[] longsToIntegerDoubles(long[] longs) {
        final double[] data = new double[longs.length];
        for (int i = 0; i < longs.length; i++) {
            data[i] = (double) longs[i];
        }
        return data;
    }

    // -- Conversion helpers --

    public static long[] doublesToSortableLongs(double[] data) {
        final long[] longs = new long[data.length];
        for (int i = 0; i < data.length; i++) {
            longs[i] = NumericUtils.doubleToSortableLong(data[i]);
        }
        return longs;
    }

    // -- DataSource records --

    public record DoubleDataSource(String name, IntFunction<double[]> generator) {}

    public record LongDataSource(String name, IntFunction<long[]> generator) {}

    // -- Seeded DataSource records --

    public record SeededDoubleDataSource(String name, BiFunction<Integer, Long, double[]> generator) {}

    public record SeededLongDataSource(String name, BiFunction<Integer, Long, long[]> generator) {}

    // ========================================================================
    // Double generators (21 patterns)
    // ========================================================================

    public static double[] constantDoubles(int size) {
        return constantDoubles(size, DEFAULT_SEED);
    }

    public static double[] constantDoubles(int size, long seed) {
        final double[] data = new double[size];
        Arrays.fill(data, 42.5);
        return data;
    }

    public static double[] percentageDoubles(int size) {
        return percentageDoubles(size, DEFAULT_SEED);
    }

    public static double[] percentageDoubles(int size, long seed) {
        final double[] data = new double[size];
        for (int i = 0; i < size; i++) {
            data[i] = hash(i, seed + 1) * 100.0;
        }
        return data;
    }

    public static double[] monotonicDoubles(int size) {
        return monotonicDoubles(size, DEFAULT_SEED);
    }

    public static double[] monotonicDoubles(int size, long seed) {
        final double[] data = new double[size];
        double value = 50.0;
        for (int i = 0; i < size; i++) {
            value += 0.1 + hash(i, seed + 2) * 9.9;
            data[i] = value;
        }
        return data;
    }

    public static double[] gaugeDoubles(int size) {
        return gaugeDoubles(size, DEFAULT_SEED);
    }

    public static double[] gaugeDoubles(int size, long seed) {
        final double[] data = new double[size];
        for (int i = 0; i < size; i++) {
            data[i] = 75.0 + (hash(i, seed + 3) - 0.5) * 20.0;
        }
        return data;
    }

    public static double[] realisticGaugeDoubles(int size) {
        return realisticGaugeDoubles(size, DEFAULT_SEED);
    }

    public static double[] realisticGaugeDoubles(int size, long seed) {
        final double[] data = new double[size];
        double value = 50.0;
        for (int i = 0; i < size; i++) {
            double drift = (50.0 - value) * 0.01;
            double noise = (hash(i, seed + 4) - 0.5);
            value = Math.max(0, Math.min(100, value + drift + noise));
            data[i] = value;
        }
        return data;
    }

    public static double[] sparseGaugeDoubles(int size) {
        return sparseGaugeDoubles(size, DEFAULT_SEED);
    }

    public static double[] sparseGaugeDoubles(int size, long seed) {
        final double[] data = new double[size];
        double value = 50.0;
        for (int i = 0; i < size; i++) {
            if (i > 0 && hash(i, seed + 5) < 0.7) {
                data[i] = data[i - 1];
            } else {
                value = value + (hash(i, seed + 6) - 0.5) * 2.0;
                value = Math.max(0, Math.min(100, value));
                data[i] = value;
            }
        }
        return data;
    }

    public static double[] randomDoubles(int size) {
        return randomDoubles(size, DEFAULT_SEED);
    }

    public static double[] randomDoubles(int size, long seed) {
        final double[] data = new double[size];
        for (int i = 0; i < size; i++) {
            data[i] = (hash(i, seed + 7) - 0.5) * 2_000_000.0;
        }
        return data;
    }

    public static double[] stableSensorDoubles(int size) {
        return stableSensorDoubles(size, DEFAULT_SEED);
    }

    public static double[] stableSensorDoubles(int size, long seed) {
        final double[] data = new double[size];
        double value = 25.0;
        for (int i = 0; i < size; i++) {
            data[i] = value;
            if (i % 5 == 4) {
                value += (hash(i, seed + 13) - 0.5) * 0.2;
            }
        }
        return data;
    }

    public static double[] tinyIncrementDoubles(int size) {
        return tinyIncrementDoubles(size, DEFAULT_SEED);
    }

    public static double[] tinyIncrementDoubles(int size, long seed) {
        final double[] data = new double[size];
        double value = 500.0;
        for (int i = 0; i < size; i++) {
            data[i] = value;
            value += 0.005 + (hash(i, seed + 14) - 0.5) * 0.001;
        }
        return data;
    }

    public static double[] steadyCounterDoubles(int size) {
        return steadyCounterDoubles(size, DEFAULT_SEED);
    }

    public static double[] steadyCounterDoubles(int size, long seed) {
        final double[] data = new double[size];
        double value = 5_000_000.0;
        for (int i = 0; i < size; i++) {
            data[i] = value;
            value += 500.0 + (hash(i, seed + 15) - 0.5) * 50.0;
        }
        return data;
    }

    public static double[] burstSpikeDoubles(int size) {
        return burstSpikeDoubles(size, DEFAULT_SEED);
    }

    public static double[] burstSpikeDoubles(int size, long seed) {
        final double[] data = new double[size];
        for (int i = 0; i < size; i++) {
            if (hash(i, seed + 16) < 0.1) {
                data[i] = 10.0 + hash(i, seed + 17) * 10.0;
            } else {
                data[i] = 10.0 + (hash(i, seed + 18) - 0.5) * 0.02;
            }
        }
        return data;
    }

    public static double[] zeroCrossingOscillationDoubles(int size) {
        return zeroCrossingOscillationDoubles(size, DEFAULT_SEED);
    }

    public static double[] zeroCrossingOscillationDoubles(int size, long seed) {
        final double[] data = new double[size];
        for (int i = 0; i < size; i++) {
            data[i] = 5.0 * Math.sin(2.0 * Math.PI * i / 32.0) + (hash(i, seed + 19) - 0.5) * 0.02;
        }
        return data;
    }

    public static double[] stepWithSpikesDoubles(int size) {
        return stepWithSpikesDoubles(size, DEFAULT_SEED);
    }

    public static double[] stepWithSpikesDoubles(int size, long seed) {
        final double[] data = new double[size];
        double level = 30.0;
        for (int i = 0; i < size; i++) {
            if (hash(i, seed + 21) < 0.03) {
                data[i] = level + 100.0 + hash(i, seed + 22) * 100.0;
            } else {
                data[i] = level + (hash(i, seed + 23) - 0.5) * 0.02;
            }
            if (i % 15 == 14) {
                level = 10.0 + hash(i, seed + 24) * 40.0;
            }
        }
        return data;
    }

    public static double[] counterWithResetsDoubles(int size) {
        return counterWithResetsDoubles(size, DEFAULT_SEED);
    }

    public static double[] counterWithResetsDoubles(int size, long seed) {
        final double[] data = new double[size];
        double value = 0.0;
        for (int i = 0; i < size; i++) {
            if (i > 0 && hash(i, seed + 25) < 0.05) {
                value = 0.0;
            } else {
                value += 50.0 + (hash(i, seed + 26) - 0.5) * 5.0;
            }
            data[i] = value;
        }
        return data;
    }

    public static double[] quantizedDoubles(int size) {
        return quantizedDoubles(size, DEFAULT_SEED);
    }

    public static double[] quantizedDoubles(int size, long seed) {
        final double[] data = new double[size];
        double value = 30.0;
        for (int i = 0; i < size; i++) {
            data[i] = value;
            double delta = (hash(i, seed + 27) - 0.5) * 4.0;
            value = Math.round((value + delta) / 0.1) * 0.1;
        }
        return data;
    }

    // Decimal-origin double generators

    public static double[] sensor2dpDoubles(int size) {
        return sensor2dpDoubles(size, DEFAULT_SEED);
    }

    public static double[] sensor2dpDoubles(int size, long seed) {
        final double[] data = new double[size];
        double value = 25.0;
        for (int i = 0; i < size; i++) {
            value += (hash(i, seed + 28) - 0.5);
            data[i] = Math.round(value * 100.0) / 100.0;
        }
        return data;
    }

    public static double[] temperature1dpDoubles(int size) {
        return temperature1dpDoubles(size, DEFAULT_SEED);
    }

    public static double[] temperature1dpDoubles(int size, long seed) {
        final double[] data = new double[size];
        double value = 25.0;
        for (int i = 0; i < size; i++) {
            value += (hash(i, seed + 29) - 0.5) * 2.0;
            data[i] = Math.round(value * 10.0) / 10.0;
        }
        return data;
    }

    public static double[] financial2dpDoubles(int size) {
        return financial2dpDoubles(size, DEFAULT_SEED);
    }

    public static double[] financial2dpDoubles(int size, long seed) {
        final double[] data = new double[size];
        double value = 500.0;
        for (int i = 0; i < size; i++) {
            value += (hash(i, seed + 30) - 0.5) * 10.0;
            data[i] = Math.round(value * 100.0) / 100.0;
        }
        return data;
    }

    public static double[] percentageRounded1dpDoubles(int size) {
        return percentageRounded1dpDoubles(size, DEFAULT_SEED);
    }

    public static double[] percentageRounded1dpDoubles(int size, long seed) {
        final double[] data = new double[size];
        for (int i = 0; i < size; i++) {
            data[i] = Math.round(hash(i, seed + 31) * 100.0 * 10.0) / 10.0;
        }
        return data;
    }

    public static double[] mixedSignDoubles(int size) {
        return mixedSignDoubles(size, DEFAULT_SEED);
    }

    public static double[] mixedSignDoubles(int size, long seed) {
        final double[] data = new double[size];
        for (int i = 0; i < size; i++) {
            double magnitude = 1000.0 + hash(i, seed + 32) * 500.0;
            data[i] = (i % 2 == 0) ? magnitude : -magnitude;
        }
        return data;
    }

    public static double[] stepHoldDoubles(int size) {
        return stepHoldDoubles(size, DEFAULT_SEED);
    }

    public static double[] stepHoldDoubles(int size, long seed) {
        final double[] data = new double[size];
        double level = 100.0;
        for (int i = 0; i < size; i++) {
            data[i] = level;
            if (i % 16 == 15) {
                level = 50.0 + hash(i, seed + 33) * 200.0;
            }
        }
        return data;
    }

    // ========================================================================
    // Long generators (11 patterns)
    // ========================================================================

    public static long[] timestampLongs(int size) {
        return timestampLongs(size, DEFAULT_SEED);
    }

    public static long[] timestampLongs(int size, long seed) {
        final long[] data = new long[size];
        long baseTimestamp = 1500000L;
        long interval = 150L;
        for (int i = 0; i < size; i++) {
            data[i] = baseTimestamp + (i * interval);
        }
        return data;
    }

    public static long[] counterLongs(int size) {
        return counterLongs(size, DEFAULT_SEED);
    }

    public static long[] counterLongs(int size, long seed) {
        final long[] data = new long[size];
        long counter = 1500000L;
        for (int i = 0; i < size; i++) {
            counter += 1 + (long) (hash(i, seed + 40) * 99);
            data[i] = counter;
        }
        return data;
    }

    public static long[] gaugeLongs(int size) {
        return gaugeLongs(size, DEFAULT_SEED);
    }

    public static long[] gaugeLongs(int size, long seed) {
        final long[] data = new long[size];
        long baseline = 50000L;
        for (int i = 0; i < size; i++) {
            data[i] = baseline + (long) ((hash(i, seed + 41) - 0.5) * 200);
        }
        return data;
    }

    public static long[] gcdLongs(int size) {
        return gcdLongs(size, DEFAULT_SEED);
    }

    public static long[] gcdLongs(int size, long seed) {
        final long[] data = new long[size];
        long gcd = 50L;
        for (int i = 0; i < size; i++) {
            data[i] = gcd * (1 + (long) (hash(i, seed + 42) * 999));
        }
        return data;
    }

    public static long[] constantLongs(int size) {
        return constantLongs(size, DEFAULT_SEED);
    }

    public static long[] constantLongs(int size, long seed) {
        final long[] data = new long[size];
        Arrays.fill(data, 500L);
        return data;
    }

    public static long[] randomLongs(int size) {
        return randomLongs(size, DEFAULT_SEED);
    }

    public static long[] randomLongs(int size, long seed) {
        final long[] data = new long[size];
        for (int i = 0; i < size; i++) {
            data[i] = (long) (hash(i, seed + 43) * 1_000_000L);
        }
        return data;
    }

    public static long[] decreasingTimestampLongs(int size) {
        return decreasingTimestampLongs(size, DEFAULT_SEED);
    }

    public static long[] decreasingTimestampLongs(int size, long seed) {
        final long[] data = new long[size];
        long baseTimestamp = 1500000L;
        long interval = 150L;
        for (int i = 0; i < size; i++) {
            data[i] = baseTimestamp - (i * interval);
        }
        return data;
    }

    public static long[] boundaryLongs(int size) {
        return boundaryLongs(size, DEFAULT_SEED);
    }

    public static long[] boundaryLongs(int size, long seed) {
        final long[] data = new long[size];
        data[0] = 0L;
        if (size > 1) data[1] = 1L;
        if (size > 2) data[2] = Long.MAX_VALUE / 2;
        for (int i = 3; i < size; i++) {
            data[i] = (long) (hash(i, seed + 44) * (Long.MAX_VALUE / 2.0));
        }
        return data;
    }

    public static long[] smallLongs(int size) {
        return smallLongs(size, DEFAULT_SEED);
    }

    public static long[] smallLongs(int size, long seed) {
        final long[] data = new long[size];
        for (int i = 0; i < size; i++) {
            data[i] = (long) (hash(i, seed + 45) * 255);
        }
        return data;
    }

    public static long[] timestampWithJitterLongs(int size) {
        return timestampWithJitterLongs(size, DEFAULT_SEED);
    }

    public static long[] timestampWithJitterLongs(int size, long seed) {
        final long[] data = new long[size];
        final long baseDelta = 1000L;
        final long maxJitter = 50L;
        long current = 500000L;
        for (int i = 0; i < size; i++) {
            data[i] = current;
            long jitter = 0;
            if (hash(i, seed + 46) < 0.1) {
                jitter = (long) ((hash(i, seed + 47) - 0.5) * 2 * maxJitter);
            }
            current += baseDelta + jitter;
        }
        return data;
    }

    // ========================================================================
    // Typed registries
    // ========================================================================

    public static List<DoubleDataSource> doubleDataSources() {
        final List<DoubleDataSource> sources = new ArrayList<>();
        sources.add(new DoubleDataSource("constant-double", NumericDataGenerators::constantDoubles));
        sources.add(new DoubleDataSource("percentage-double", NumericDataGenerators::percentageDoubles));
        sources.add(new DoubleDataSource("monotonic-double", NumericDataGenerators::monotonicDoubles));
        sources.add(new DoubleDataSource("gauge-double", NumericDataGenerators::gaugeDoubles));
        sources.add(new DoubleDataSource("realistic-gauge-double", NumericDataGenerators::realisticGaugeDoubles));
        sources.add(new DoubleDataSource("sparse-gauge-double", NumericDataGenerators::sparseGaugeDoubles));
        sources.add(new DoubleDataSource("random-double", NumericDataGenerators::randomDoubles));
        sources.add(new DoubleDataSource("stable-sensor-double", NumericDataGenerators::stableSensorDoubles));
        sources.add(new DoubleDataSource("tiny-increment-double", NumericDataGenerators::tinyIncrementDoubles));
        sources.add(new DoubleDataSource("steady-counter-double", NumericDataGenerators::steadyCounterDoubles));
        sources.add(new DoubleDataSource("burst-spike-double", NumericDataGenerators::burstSpikeDoubles));
        sources.add(new DoubleDataSource("zero-crossing-oscillation-double", NumericDataGenerators::zeroCrossingOscillationDoubles));
        sources.add(new DoubleDataSource("step-with-spikes-double", NumericDataGenerators::stepWithSpikesDoubles));
        sources.add(new DoubleDataSource("counter-with-resets-double", NumericDataGenerators::counterWithResetsDoubles));
        sources.add(new DoubleDataSource("quantized-double", NumericDataGenerators::quantizedDoubles));
        sources.add(new DoubleDataSource("sensor-2dp-double", NumericDataGenerators::sensor2dpDoubles));
        sources.add(new DoubleDataSource("temperature-1dp-double", NumericDataGenerators::temperature1dpDoubles));
        sources.add(new DoubleDataSource("financial-2dp-double", NumericDataGenerators::financial2dpDoubles));
        sources.add(new DoubleDataSource("percentage-rounded-1dp-double", NumericDataGenerators::percentageRounded1dpDoubles));
        sources.add(new DoubleDataSource("mixed-sign-double", NumericDataGenerators::mixedSignDoubles));
        sources.add(new DoubleDataSource("step-hold-double", NumericDataGenerators::stepHoldDoubles));
        // NOTE: boundary excluded -- Long.MAX_VALUE/2 exceeds lossy-quantize tolerance.
        for (final LongDataSource ls : longDataSources()) {
            if ("boundary".equals(ls.name()) == false) {
                sources.add(new DoubleDataSource(ls.name() + "-as-double", size -> longsToIntegerDoubles(ls.generator().apply(size))));
            }
        }
        return sources;
    }

    public static List<LongDataSource> longDataSources() {
        final List<LongDataSource> sources = new ArrayList<>();
        sources.add(new LongDataSource("timestamp", NumericDataGenerators::timestampLongs));
        sources.add(new LongDataSource("counter", NumericDataGenerators::counterLongs));
        sources.add(new LongDataSource("gauge", NumericDataGenerators::gaugeLongs));
        sources.add(new LongDataSource("gcd", NumericDataGenerators::gcdLongs));
        sources.add(new LongDataSource("constant", NumericDataGenerators::constantLongs));
        sources.add(new LongDataSource("random", NumericDataGenerators::randomLongs));
        sources.add(new LongDataSource("decreasing-timestamp", NumericDataGenerators::decreasingTimestampLongs));
        sources.add(new LongDataSource("boundary", NumericDataGenerators::boundaryLongs));
        sources.add(new LongDataSource("small", NumericDataGenerators::smallLongs));
        sources.add(new LongDataSource("timestamp-with-jitter", NumericDataGenerators::timestampWithJitterLongs));
        return sources;
    }

    // ========================================================================
    // Seeded registries
    // ========================================================================

    public static List<SeededDoubleDataSource> seededDoubleDataSources() {
        final List<SeededDoubleDataSource> sources = new ArrayList<>();
        sources.add(new SeededDoubleDataSource("constant-double", NumericDataGenerators::constantDoubles));
        sources.add(new SeededDoubleDataSource("percentage-double", NumericDataGenerators::percentageDoubles));
        sources.add(new SeededDoubleDataSource("monotonic-double", NumericDataGenerators::monotonicDoubles));
        sources.add(new SeededDoubleDataSource("gauge-double", NumericDataGenerators::gaugeDoubles));
        sources.add(new SeededDoubleDataSource("realistic-gauge-double", NumericDataGenerators::realisticGaugeDoubles));
        sources.add(new SeededDoubleDataSource("sparse-gauge-double", NumericDataGenerators::sparseGaugeDoubles));
        sources.add(new SeededDoubleDataSource("random-double", NumericDataGenerators::randomDoubles));
        sources.add(new SeededDoubleDataSource("stable-sensor-double", NumericDataGenerators::stableSensorDoubles));
        sources.add(new SeededDoubleDataSource("tiny-increment-double", NumericDataGenerators::tinyIncrementDoubles));
        sources.add(new SeededDoubleDataSource("steady-counter-double", NumericDataGenerators::steadyCounterDoubles));
        sources.add(new SeededDoubleDataSource("burst-spike-double", NumericDataGenerators::burstSpikeDoubles));
        sources.add(new SeededDoubleDataSource("zero-crossing-oscillation-double", NumericDataGenerators::zeroCrossingOscillationDoubles));
        sources.add(new SeededDoubleDataSource("step-with-spikes-double", NumericDataGenerators::stepWithSpikesDoubles));
        sources.add(new SeededDoubleDataSource("counter-with-resets-double", NumericDataGenerators::counterWithResetsDoubles));
        sources.add(new SeededDoubleDataSource("quantized-double", NumericDataGenerators::quantizedDoubles));
        sources.add(new SeededDoubleDataSource("sensor-2dp-double", NumericDataGenerators::sensor2dpDoubles));
        sources.add(new SeededDoubleDataSource("temperature-1dp-double", NumericDataGenerators::temperature1dpDoubles));
        sources.add(new SeededDoubleDataSource("financial-2dp-double", NumericDataGenerators::financial2dpDoubles));
        sources.add(new SeededDoubleDataSource("percentage-rounded-1dp-double", NumericDataGenerators::percentageRounded1dpDoubles));
        sources.add(new SeededDoubleDataSource("mixed-sign-double", NumericDataGenerators::mixedSignDoubles));
        sources.add(new SeededDoubleDataSource("step-hold-double", NumericDataGenerators::stepHoldDoubles));
        // NOTE: boundary excluded -- Long.MAX_VALUE/2 exceeds lossy-quantize tolerance.
        for (final SeededLongDataSource ls : seededLongDataSources()) {
            if ("boundary".equals(ls.name()) == false) {
                sources.add(
                    new SeededDoubleDataSource(
                        ls.name() + "-as-double",
                        (size, seed) -> longsToIntegerDoubles(ls.generator().apply(size, seed))
                    )
                );
            }
        }
        return sources;
    }

    public static List<SeededLongDataSource> seededLongDataSources() {
        final List<SeededLongDataSource> sources = new ArrayList<>();
        sources.add(new SeededLongDataSource("timestamp", NumericDataGenerators::timestampLongs));
        sources.add(new SeededLongDataSource("counter", NumericDataGenerators::counterLongs));
        sources.add(new SeededLongDataSource("gauge", NumericDataGenerators::gaugeLongs));
        sources.add(new SeededLongDataSource("gcd", NumericDataGenerators::gcdLongs));
        sources.add(new SeededLongDataSource("constant", NumericDataGenerators::constantLongs));
        sources.add(new SeededLongDataSource("random", NumericDataGenerators::randomLongs));
        sources.add(new SeededLongDataSource("decreasing-timestamp", NumericDataGenerators::decreasingTimestampLongs));
        sources.add(new SeededLongDataSource("boundary", NumericDataGenerators::boundaryLongs));
        sources.add(new SeededLongDataSource("small", NumericDataGenerators::smallLongs));
        sources.add(new SeededLongDataSource("timestamp-with-jitter", NumericDataGenerators::timestampWithJitterLongs));
        return sources;
    }
}
