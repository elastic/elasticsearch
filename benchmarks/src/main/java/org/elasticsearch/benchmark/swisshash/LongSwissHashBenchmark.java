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
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;

/**
 * Benchmarks {@link LongSwissHash} build throughput, the hot path behind
 * {@code STATS ... BY <long>} (issue #798).
 *
 * <p> Each benchmark method builds a <b>fresh</b> table per invocation so it
 * measures {@code addAll} of {@code cardinality} keys (inserts plus the probes
 * for duplicate keys), not a re-add into an already-full table. The keys are
 * generated once per trial; the table is created and released inside the method.
 * A large fixed heap keeps GC out of the measurement.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector", "-Xms4g", "-Xmx4g" })
@State(Scope.Thread)
public class LongSwissHashBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    @Param({ "1000", "10000", "100000", "1000000", "10000000", "17600000" })
    int cardinality;

    @Param({ "uniform", "duplicates", "collision" })
    String distribution;

    long[] keys;

    BigArrays bigArrays;
    PageCacheRecycler recycler;
    NoopCircuitBreaker breaker;

    @Setup(Level.Trial)
    public void setup() {
        keys = generate(distribution, cardinality);
        bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        recycler = PageCacheRecycler.NON_RECYCLING_INSTANCE;
        breaker = new NoopCircuitBreaker("dummy");
    }

    private LongSwissHash newSwiss() {
        return SwissHashFactory.getInstance().newLongSwissHash(recycler, breaker);
    }

    /**
     * Build the Swiss table only (no output iteration). Isolates the {@code add}
     * probe path, which is the hot path #798 targets
     * ({@code addAll(17_630_976 random longs)}).
     */
    @Benchmark
    public long swissBuild() {
        try (LongSwissHash swiss = newSwiss()) {
            long acc = 0;
            for (long v : keys) {
                acc += swiss.add(v);
            }
            return acc;
        }
    }

    private static final int PREFETCH_BATCH = 128;

    /**
     * Build via the batched two-level-prefetch loop the block hash will use
     * (warm control + id), falling back to scalar add until the table is large
     * enough to prefetch.
     */
    @Benchmark
    public long swissBuildPrefetch2() {
        return prefetchBuild();
    }

    private long prefetchBuild() {
        final int[] hashes = new int[PREFETCH_BATCH];
        final int n = keys.length;
        try (LongSwissHash swiss = newSwiss()) {
            long acc = 0;
            int dummy = 0;
            for (int off = 0; off < n; off += PREFETCH_BATCH) {
                final int batch = Math.min(PREFETCH_BATCH, n - off);
                if (swiss.shouldPrefetch()) {
                    for (int i = 0; i < batch; i++) {
                        final int h = LongSwissHash.hash(keys[off + i]);
                        hashes[i] = h;
                        dummy ^= swiss.prefetch(h);
                    }
                    for (int i = 0; i < batch; i++) {
                        acc += swiss.addWithHash(keys[off + i], hashes[i]);
                    }
                } else {
                    for (int i = 0; i < batch; i++) {
                        acc += swiss.add(keys[off + i]);
                    }
                }
            }
            return acc + (long) dummy;
        }
    }

    /**
     * Build the Swiss table completely, then iterate. Mirrors STATS build ->
     * finalize -> output.
     */
    @Benchmark
    public long swissBuildThenIterate(Blackhole bh) {
        try (LongSwissHash swiss = newSwiss()) {
            return swissBuildThenIterateImpl(swiss, bh::consume);
        }
    }

    long swissBuildThenIterateImpl(LongSwissHash swiss, LongConsumer bh) {
        for (long v : keys) {
            swiss.add(v);
        }
        for (int i = 0; i < swiss.size(); i++) {
            bh.accept(swiss.get(i));
        }
        return swiss.size();
    }

    /**
     * Same build-then-iterate for the legacy hash table, as a reference point.
     */
    @Benchmark
    public long legacyBuildThenIterate(Blackhole bh) {
        try (LongHash legacy = new LongHash(1, bigArrays)) {
            for (long v : keys) {
                legacy.add(v);
            }
            for (int i = 0; i < legacy.size(); i++) {
                bh.consume(legacy.get(i));
            }
            return legacy.size();
        }
    }

    private long[] generate(String dist, int size) {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        long[] out = new long[size];

        switch (dist) {
            case "uniform":
                for (int i = 0; i < size; i++) {
                    out[i] = r.nextLong();
                }
                break;
            case "duplicates":
                // 80% of keys come from a small "hot" set
                int hotSet = Math.max(32, Math.min(1000, size / 50)); // ~2% of cardinality
                long[] hot = new long[hotSet];
                for (int i = 0; i < hotSet; i++) {
                    hot[i] = r.nextLong();
                }
                for (int i = 0; i < size; i++) {
                    if (r.nextInt(10) < 8) {        // 80% duplicates
                        out[i] = hot[r.nextInt(hotSet)];
                    } else {                               // 20% random noise
                        out[i] = r.nextLong();
                    }
                }
                break;
            case "collision":
                // Force collisions by clamping top bits so BitMixer mixes poorly
                final long seed = 0xABCDEFL;
                for (int i = 0; i < size; i++) {
                    out[i] = seed | ((long) i & 0xFFFF); // all share same high bits
                }
                break;
            default:
                throw new IllegalArgumentException("unknown distribution: " + dist);
        }
        return out;
    }
}
