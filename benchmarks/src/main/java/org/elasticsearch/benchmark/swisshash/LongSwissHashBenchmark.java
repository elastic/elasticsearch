/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.swisshash;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.swisshash.LongSwissHash;
import org.elasticsearch.swisshash.SwissHashFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.ThreadLocalRandom;

@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
public class LongSwissHashBenchmark {

    static {
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    // -----------------------
    // Benchmark parameters
    // -----------------------
    @Param({ "uniform", "zipf", "hot", "collision" })
    public String distribution;

    @Param({ "1000", "10000", "100000", "1000000" })
    public int uniqueKeys;

    @Param({ "insert", "lookup", "mixed" })
    public String mode;

    // -----------------------
    // Bench state
    // -----------------------
    long[] keys;
    long[] lookupKeys;
    int[] ids;

    LongSwissHash hash;
    LongHash longHash;

    @Setup(Level.Trial)
    public void setup() {
        PageCacheRecycler recycler = PageCacheRecycler.NON_RECYCLING_INSTANCE;
        NoopCircuitBreaker breaker = new NoopCircuitBreaker("dummy");

        hash = SwissHashFactory.getInstance().newLongSwissHash(recycler, breaker);
        longHash = new LongHash(1, BigArrays.NON_RECYCLING_INSTANCE);
        keys = generateKeys(uniqueKeys);
        lookupKeys = keys.clone();
        ids = new int[uniqueKeys];

        // For lookup-mode, we must pre-insert the benchmark keys
        if (mode.equals("lookup") || mode.equals("mixed")) {
            for (long k : keys) {
                hash.add(k);
                longHash.add(k);
            }
        }
    }

    // -----------------------
    // Benchmarks
    // -----------------------

    @Benchmark
    public long swissHashBenchmark() {
        return switch (mode) {
            case "insert" -> doInsert();
            case "lookup" -> doLookup();
            case "mixed" -> doMixed();
            default -> throw new IllegalArgumentException(mode);
        };
    }

    private long doInsert() {
        long sum = 0;
        for (long k : keys) {
            sum += hash.add(k);
        }
        return sum;
    }

    private long doLookup() {
        long sum = 0;
        for (long k : lookupKeys) {
            sum += hash.find(k);
        }
        return sum;
    }

    private long doMixed() {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        long sum = 0;

        for (long k : keys) {
            if (r.nextInt(100) < 80) { // 80% lookups
                sum += hash.find(k);
            } else { // 20% insert
                sum += hash.add(k ^ 0x9E3779B97F4A7C15L); // mutate to force growth
            }
        }
        return sum;
    }

    // -- LongHash
    @Benchmark
    public long longHashBenchmark() {
        return switch (mode) {
            case "insert" -> doInsertLH();
            case "lookup" -> doLookupLH();
            case "mixed" -> doMixedLH();
            default -> throw new IllegalArgumentException(mode);
        };
    }

    private long doInsertLH() {
        long sum = 0;
        for (long k : keys) {
            sum += longHash.add(k);
        }
        return sum;
    }

    private long doLookupLH() {
        long sum = 0;
        for (long k : lookupKeys) {
            sum += longHash.find(k);
        }
        return sum;
    }

    private long doMixedLH() {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        long sum = 0;

        for (long k : keys) {
            if (r.nextInt(100) < 80) { // 80% lookups
                sum += longHash.find(k);
            } else { // 20% insert
                sum += hash.add(k ^ 0x9E3779B97F4A7C15L); // mutate to force growth
            }
        }
        return sum;
    }

    // --

    // -----------------------
    // Key generation
    // -----------------------

    private long[] generateKeys(int size) {
        return switch (distribution) {
            case "uniform" -> genUniform(size);
            case "zipf" -> genZipf(size, 1.1);
            case "hot" -> genHot(size, 0.97);
            case "collision" -> genCollisions(size);
            default -> throw new IllegalArgumentException(distribution);
        };
    }

    private long[] genUniform(int size) {
        long[] arr = new long[size];
        ThreadLocalRandom r = ThreadLocalRandom.current();
        for (int i = 0; i < size; i++) {
            arr[i] = r.nextLong();
        }
        return arr;
    }

    private long[] genZipf(int size, double skew) {
        long[] arr = new long[size];
        int domain = size;
        double denom = 0;
        for (int i = 1; i <= domain; i++) {
            denom += 1.0 / Math.pow(i, skew);
        }

        ThreadLocalRandom r = ThreadLocalRandom.current();
        for (int i = 0; i < size; i++) {
            double u = r.nextDouble() * denom;
            double sum = 0;
            for (int k = 1; k <= domain; k++) {
                sum += 1.0 / Math.pow(k, skew);
                if (sum >= u) {
                    arr[i] = k;
                    break;
                }
            }
        }
        return arr;
    }

    private long[] genHot(int size, double hotRatio) {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        long hotKey = r.nextLong();
        long[] arr = new long[size];
        for (int i = 0; i < size; i++) {
            arr[i] = (r.nextDouble() < hotRatio) ? hotKey : r.nextLong();
        }
        return arr;
    }

    private long[] genCollisions(int size) {
        // Force collisions by clamping top bits so BitMixer mixes poorly
        long[] arr = new long[size];
        long seed = 0xABCDEFL;
        for (int i = 0; i < size; i++) {
            arr[i] = seed | ((long) i & 0xFFFF); // all share same high bits
        }
        return arr;
    }
}
