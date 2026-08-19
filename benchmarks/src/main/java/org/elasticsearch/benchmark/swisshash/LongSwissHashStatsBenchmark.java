/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.swisshash;

import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.swisshash.LongSwissHash;
import org.elasticsearch.swisshash.SwissHashFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Production-like benchmark for {@link LongSwissHash} for a
 * {@code STATS COUNT(*) BY <long>} query
 *
 * <p> Unlike {@link LongSwissHashBenchmark} (which feeds {@code cardinality}
 * distinct keys, i.e. an insert-only workload), this feeds a stream of
 * {@code cardinality * DUP_FACTOR} rows drawn from a fixed universe of
 * {@code cardinality} groups, so most {@code add} calls are <b>hits</b> on an
 * existing group. The hit path walks the cold {@code control -> id -> key} chain
 * that prefetch targets, which the insert-only benchmark barely exercises.
 *
 * <p> The row->group frequency is power-law skewed ({@link #SKEW}) to approximate
 * real UserID traffic: a few hot groups take most rows, a long tail appears only
 * a few times. The stream is seeded with one occurrence of every group (so the
 * cardinality is exactly {@code cardinality}) and then shuffled so inserts and
 * hits interleave the way a streaming aggregation sees them.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector", "-Xms4g", "-Xmx4g" })
@State(Scope.Thread)
public class LongSwissHashStatsBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    /** Distinct groups, i.e. the final table size. {@code rows = cardinality * DUP_FACTOR}. */
    @Param({ "1000000", "10000000", "17600000" })
    int cardinality;

    /** Average rows per group, e.g. ~100M rows / ~17.6M distinct UserIDs. */
    private static final double DUP_FACTOR = 5.68;

    /**
     * Power-law skew exponent for the row->group frequency. {@code rank = C * U^SKEW}
     * for {@code U} uniform in {@code [0,1)}: {@code 1.0} is uniform, {@code >1}
     * concentrates rows on hot (low-rank) groups. This approximates the heavy tail
     * of real UserID traffic; it is not a fitted Zipf.
     */
    private static final double SKEW = 2.0;

    private static final int PREFETCH_BATCH = 128;

    long[] stream;
    PageCacheRecycler recycler;
    NoopCircuitBreaker breaker;

    @Setup(Level.Trial)
    public void setup() {
        stream = generate(cardinality, (int) (cardinality * DUP_FACTOR), SKEW);
        recycler = PageCacheRecycler.NON_RECYCLING_INSTANCE;
        breaker = new NoopCircuitBreaker("dummy");
    }

    private LongSwissHash newSwiss() {
        return SwissHashFactory.getInstance().newLongSwissHash(recycler, breaker);
    }

    /** Scalar build of the hit-heavy stream: the current production path. */
    @Benchmark
    public long statsScalar() {
        try (LongSwissHash swiss = newSwiss()) {
            long acc = 0;
            for (long v : stream) {
                acc += swiss.add(v);
            }
            return acc;
        }
    }

    /** Hit-heavy build with the two-level prefetch (warm control + id). */
    @Benchmark
    public long statsPrefetch2() {
        return prefetchStats();
    }

    private long prefetchStats() {
        final int[] hashes = new int[PREFETCH_BATCH];
        final int n = stream.length;
        try (LongSwissHash swiss = newSwiss()) {
            long acc = 0;
            int dummy = 0;
            for (int off = 0; off < n; off += PREFETCH_BATCH) {
                final int batch = Math.min(PREFETCH_BATCH, n - off);
                if (swiss.shouldPrefetch()) {
                    for (int i = 0; i < batch; i++) {
                        final int h = LongSwissHash.hash(stream[off + i]);
                        hashes[i] = h;
                        dummy ^= swiss.prefetch(h);
                    }
                    for (int i = 0; i < batch; i++) {
                        acc += swiss.addWithHash(stream[off + i], hashes[i]);
                    }
                } else {
                    for (int i = 0; i < batch; i++) {
                        acc += swiss.add(stream[off + i]);
                    }
                }
            }
            return acc + (long) dummy;
        }
    }

    /** Same hit-heavy stream through the legacy {@link LongHash}, as a reference point. */
    @Benchmark
    public long legacyStats() {
        try (LongHash legacy = new LongHash(1, BigArrays.NON_RECYCLING_INSTANCE)) {
            long acc = 0;
            for (long v : stream) {
                acc += legacy.add(v);
            }
            return acc;
        }
    }

    /**
     * Builds a stream of {@code rows} keys over exactly {@code cardinality} distinct
     * groups with a power-law row->group frequency, every group present at least once,
     * shuffled so inserts and hits interleave.
     */
    private static long[] generate(int cardinality, int rows, double skew) {
        ThreadLocalRandom r = ThreadLocalRandom.current();

        // Universe of distinct group keys; mix64 is a bijection so all are distinct.
        long[] universe = new long[cardinality];
        for (int i = 0; i < cardinality; i++) {
            universe[i] = mix64(i);
        }

        long[] stream = new long[rows];
        // Seed one of each group so the realized cardinality is exactly `cardinality`.
        System.arraycopy(universe, 0, stream, 0, cardinality);
        // Fill the remaining rows by sampling groups with the power-law skew.
        for (int j = cardinality; j < rows; j++) {
            int rank = (int) (cardinality * Math.pow(r.nextDouble(), skew));
            if (rank >= cardinality) {
                rank = cardinality - 1; // guard against pow() rounding to 1.0
            }
            stream[j] = universe[rank];
        }
        // Fisher-Yates shuffle so first-occurrences (inserts) spread through the stream.
        for (int i = rows - 1; i > 0; i--) {
            int j = r.nextInt(i + 1);
            long tmp = stream[i];
            stream[i] = stream[j];
            stream[j] = tmp;
        }
        return stream;
    }

    /** SplitMix64 finalizer: a bijection on 64 bits, so distinct inputs map to distinct keys. */
    private static long mix64(long z) {
        z = z + 0x9E3779B97F4A7C15L;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }
}
