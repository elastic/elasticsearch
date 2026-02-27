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

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@State(Scope.Thread)
public class LongSwissHashBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    @Param({ "1000", "10000", "100000", "1000000", "10000000" })
    int cardinality;

    @Param({ "uniform", "duplicates", "collision" })
    String distribution;

    long[] keys;

    LongSwissHash swiss;
    LongHash legacy;

    @Setup(Level.Iteration)
    public void setup() {
        keys = null;
        keys = generate(distribution, cardinality);

        BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        PageCacheRecycler recycler = PageCacheRecycler.NON_RECYCLING_INSTANCE;
        NoopCircuitBreaker breaker = new NoopCircuitBreaker("dummy");
        swiss = SwissHashFactory.getInstance().newLongSwissHash(recycler, breaker);
        legacy = new LongHash(1, bigArrays);
    }

    /**
     * Build Swiss table completely, then iterate.
     * Mirrors STATS build -> finalize -> output.
     */
    @Benchmark
    public long swissBuildThenIterate(Blackhole bh) {
        return swissBuildThenIterateImpl(bh::consume);
    }

    long swissBuildThenIterateImpl(LongConsumer bh) {
        for (long v : keys) {
            swiss.add(v);
        }
        for (int i = 0; i < swiss.size(); i++) {
            bh.accept(swiss.get(i));
        }
        return swiss.size();
    }

    /**
     * Same for legacy hash table.
     */
    @Benchmark
    public long legacyBuildThenIterate(Blackhole bh) {
        return legacyBuildThenIterateImpl(bh::consume);
    }

    long legacyBuildThenIterateImpl(LongConsumer bh) {
        for (long v : keys) {
            legacy.add(v);
        }
        for (int i = 0; i < legacy.size(); i++) {
            bh.accept(legacy.get(i));
        }
        return legacy.size();
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
