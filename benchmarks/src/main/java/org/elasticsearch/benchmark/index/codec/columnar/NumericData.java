/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.columnar;

import org.apache.lucene.util.NumericUtils;

/**
 * Deterministic numeric data shapes for the ColumNAR benchmarks. A given workload always produces
 * the same values, so every format sees identical input.
 */
final class NumericData {

    private NumericData() {}

    private static final long[] LOW_CARDINALITY_CODES = { 3, 7, 11, 42, 99, 128, 256, 999, 4096, 5000, 65535, 100000, 1, 2, 8, 16 };

    static long[] generate(String workload, int count) {
        Rng rng = new Rng(workloadSeed(workload));
        long[] values = new long[count];
        long timestamp = 1_700_000_000_000L;
        double counterValue = 0.0;
        for (int i = 0; i < count; i++) {
            values[i] = switch (workload) {
                case "MONOTONIC_TIMESTAMPS" -> {
                    long current = timestamp;
                    timestamp += rng.nextInt(1000);
                    yield current;
                }
                case "COUNTER_STEADY" -> 1000L * i;
                case "GAUGE" -> 50_000_000L + rng.nextInt(201) - 100;
                case "DOUBLE_GAUGE" -> NumericUtils.doubleToSortableLong(50.0 + (rng.nextInt(201) - 100) * 0.01);
                case "DOUBLE_COUNTER" -> {
                    counterValue += 1.5 + rng.nextInt(100) * 0.001;
                    yield NumericUtils.doubleToSortableLong(counterValue);
                }
                case "LOW_CARDINALITY" -> LOW_CARDINALITY_CODES[rng.nextInt(LOW_CARDINALITY_CODES.length)];
                case "SMALL_INTS" -> rng.nextInt(256);
                case "RANDOM_FULL" -> rng.nextLong();
                case "TSDB_SPLIT" -> {
                    int runsOf = Math.max(1, count / 4);
                    int run = i / runsOf;
                    int posInRun = i % runsOf;
                    yield 1_700_000_000_000L + (long) run * 100_000L - (long) posInRun * 1_000L;
                }
                case "SENSOR_DOUBLES" -> NumericUtils.doubleToSortableLong(20.0 + (i % 1000) * 0.1);
                case "CONSTANT" -> 1_700_000_000_000L;
                case "DECREASING" -> 1_700_000_000_000L - (long) i * 1_000L;
                case "GCD_FRIENDLY" -> ((long) rng.nextInt(1000) + 1) * 1_000_000L;
                case "NEAR_CONSTANT_OUTLIERS" -> rng.nextInt(20) == 0 ? rng.nextInt(1_000_000_000) : 1_700_000_000_000L;
                default -> throw new IllegalArgumentException("Unknown workload: " + workload);
            };
        }
        return values;
    }

    private static long workloadSeed(String workload) {
        return switch (workload) {
            case "MONOTONIC_TIMESTAMPS" -> 1L;
            case "COUNTER_STEADY" -> 2L;
            case "GAUGE" -> 3L;
            case "LOW_CARDINALITY" -> 4L;
            case "SMALL_INTS" -> 5L;
            case "RANDOM_FULL" -> 6L;
            case "TSDB_SPLIT" -> 7L;
            case "SENSOR_DOUBLES" -> 8L;
            case "CONSTANT" -> 9L;
            case "DECREASING" -> 10L;
            case "GCD_FRIENDLY" -> 11L;
            case "NEAR_CONSTANT_OUTLIERS" -> 12L;
            case "DOUBLE_GAUGE" -> 13L;
            case "DOUBLE_COUNTER" -> 14L;
            default -> throw new IllegalArgumentException("Unknown workload: " + workload);
        };
    }

    /** A tiny deterministic SplitMix64 generator, reproducible and dependency-free. */
    static final class Rng {
        private long state;

        Rng(long seed) {
            this.state = seed;
        }

        long nextLong() {
            long z = (state += 0x9E3779B97F4A7C15L);
            z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
            z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
            return z ^ (z >>> 31);
        }

        int nextInt(int bound) {
            return Math.floorMod(nextLong(), bound);
        }
    }
}
