/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.search.aggregations;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.search.aggregations.metrics.HyperLogLogPlusPlus;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks {@link HyperLogLogPlusPlus} across scenarios covering every operating mode.
 *
 * <p>{@code scenario} encodes {@code "profile:numGroups"}; {@code precision} is a separate axis
 * because it shifts the LC→HLL upgrade threshold and register-array size. Each {@link #collect()}
 * invocation creates a fresh instance, replays all pre-generated pairs, and sums cardinalities to
 * prevent dead-code elimination.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
@State(Scope.Thread)
public class HyperLogLogPlusPlusBenchmark {

    /** HLL precision. 14 is the default; 18 is the current maximum. */
    @Param({ "14", "18" })
    int precision;

    /**
     * Compound {@code "profile:numGroups"} parameter. Each value is an independently
     * meaningful scenario; the combinations are chosen so that total pre-generated pairs
     * never exceed 10 M, making runtime truncation unnecessary.
     *
     * <table>
     *   <caption>Scenario descriptions</caption>
     *   <tr><th>Scenario</th><th>Total pairs</th><th>Mode at p=14</th><th>Mode at p=18</th></tr>
     *   <tr><td>{@code single:100000}</td><td>100k</td><td>LC minimal</td><td>LC minimal</td></tr>
     *   <tr><td>{@code uniform8:100000}</td><td>800k</td><td>LC steady-state</td><td>LC steady-state</td></tr>
     *   <tr><td>{@code uniform_5k:1}</td><td>5k</td><td>LC→HLL upgrade</td><td>LC, near limit</td></tr>
     *   <tr><td>{@code uniform_5k:1000}</td><td>5M</td><td>LC→HLL upgrade × 1k groups</td><td>LC, near limit × 1k groups</td></tr>
     *   <tr><td>{@code uniform_5k:2000}</td><td>10M</td><td>LC→HLL upgrade × 2k groups</td><td>LC, near limit × 2k groups</td></tr>
     *   <tr><td>{@code uniform_50k:1}</td><td>50k</td><td>HLL deep</td><td>HLL (just past upgrade)</td></tr>
     *   <tr><td>{@code uniform_50k:200}</td><td>10M</td><td>HLL deep × 200 groups</td><td>HLL × 200 groups</td></tr>
     *   <tr><td>{@code uniform_1M:1}</td><td>1M</td><td>HLL very deep (&lt;0.3% LC overhead)</td><td>HLL very deep</td></tr>
     *   <tr><td>{@code uniform_1M:10}</td><td>10M</td><td>HLL very deep × 10 groups</td><td>HLL very deep × 10 groups</td></tr>
     *   <tr><td>{@code skewed:1000}</td><td>≈1.8M avg, ≤3M worst-case</td><td>mixed LC+HLL</td><td>mixed LC+HLL</td></tr>
     * </table>
     */
    @Param(
        {
            // LC mode: many groups, very few distinct values each
            "single:100000",    // 100 000 groups × 1 distinct = 100k pairs
            "uniform8:100000",  // 100 000 groups × 8 distinct = 800k pairs

            // LC→HLL upgrade at p=14 (≈3 072 distinct); stays LC at p=18 (≈49 152)
            "uniform_5k:1",     // 1 group × 5 000 distinct = 5k pairs (ungrouped case)
            "uniform_5k:1000",  // 1k groups × 5 000 distinct = 5M pairs
            "uniform_5k:2000",  // 2k groups × 5 000 distinct = 10M pairs

            // HLL steady-state: all groups well past upgrade at both precisions
            "uniform_50k:1",    // 1 group × 50 000 distinct = 50k pairs (ungrouped case)
            "uniform_50k:200",  // 200 groups × 50 000 distinct = 10M pairs

            // Very deep HLL: millions of distinct values, register-update hot path
            "uniform_1M:1",     // 1 group × 1 000 000 distinct = 1M pairs (ungrouped case)
            "uniform_1M:10",    // 10 groups × 1 000 000 distinct = 10M pairs

            // Power-law mix: most groups in LC, a tail in HLL — typical real aggregation
            // Tail capped at 100k (not 1M) to keep totals bounded; 1M-value depth is
            // covered by the uniform_1M scenarios above.
            "skewed:1000",      // 1k groups, ≈1.8M total pairs avg, ≤3M worst-case
        }
    )
    String scenario;

    /** Pre-generated hashes, one per pair. */
    private long[] hashes;

    /** Pre-generated group ordinals, parallel to {@link #hashes}. */
    private int[] groupIds;

    /** Parsed from {@link #scenario}; used to initialize and iterate over HLL buckets. */
    private int numGroups;

    @Setup
    public void setUp() {
        int colon = scenario.indexOf(':');
        String profile = scenario.substring(0, colon);
        numGroups = Integer.parseInt(scenario.substring(colon + 1));

        Random random = new Random(42);
        int[] cardinalities = new int[numGroups];
        int totalPairs = 0;
        for (int g = 0; g < numGroups; g++) {
            cardinalities[g] = groupCardinality(profile, random);
            totalPairs += cardinalities[g];
        }

        hashes = new long[totalPairs];
        groupIds = new int[totalPairs];
        int pos = 0;
        for (int g = 0; g < numGroups; g++) {
            for (int i = 0; i < cardinalities[g]; i++) {
                hashes[pos] = random.nextLong();
                groupIds[pos] = g;
                pos++;
            }
        }
        fisherYates(random, hashes, groupIds, totalPairs);
    }

    private static int groupCardinality(String profile, Random random) {
        return switch (profile) {
            case "single" -> 1;
            case "uniform8" -> 8;
            case "uniform_5k" -> 5_000;
            case "uniform_50k" -> 50_000;
            case "uniform_1M" -> 1_000_000;
            case "skewed" -> {
                // Power-law buckets (cumulative probability):
                // 70% → 1 (LC minimal)
                // 88% → 2–50 (LC low)
                // 96% → 51–5 000 (LC high / HLL entry for p=14)
                // 99% → 5 001–50 000 (HLL for p=14, LC/entry for p=18)
                // 100% → 50 001–100 000 (HLL deep for both precisions)
                int r = random.nextInt(100);
                if (r < 70) yield 1;
                else if (r < 88) yield 2 + random.nextInt(49);
                else if (r < 96) yield 51 + random.nextInt(4950);
                else if (r < 99) yield 5001 + random.nextInt(45000);
                else yield 50001 + random.nextInt(50000);
            }
            default -> throw new IllegalArgumentException("Unknown profile: " + profile);
        };
    }

    /** In-place Fisher-Yates shuffle of two parallel primitive arrays of length {@code n}. */
    private static void fisherYates(Random random, long[] h, int[] g, int n) {
        for (int i = n - 1; i > 0; i--) {
            int j = random.nextInt(i + 1);
            long tmpH = h[i];
            h[i] = h[j];
            h[j] = tmpH;
            int tmpG = g[i];
            g[i] = g[j];
            g[j] = tmpG;
        }
    }

    /**
     * Collects all pre-generated (hash, groupId) pairs into a fresh {@link HyperLogLogPlusPlus},
     * then sums cardinalities across all groups. The sum is returned to prevent dead-code
     * elimination by the JIT.
     */
    @Benchmark
    public long collect() {
        try (HyperLogLogPlusPlus hll = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, numGroups)) {
            long[] h = hashes;
            int[] g = groupIds;
            int n = h.length;
            for (int i = 0; i < n; i++) {
                hll.collect(g[i], h[i]);
            }
            long sum = 0;
            for (int group = 0; group < numGroups; group++) {
                sum += hll.cardinality(group);
            }
            return sum;
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder().include(".*" + HyperLogLogPlusPlusBenchmark.class.getSimpleName() + ".*")
            .warmupIterations(3)
            .measurementIterations(5)
            .build();
        new Runner(opt).run();
    }
}
