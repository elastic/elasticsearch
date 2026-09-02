/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.swisshash;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.PartitionedHashTable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.swisshash.BytesRefSwissHash;
import org.elasticsearch.swisshash.SwissHashFactory;
import org.elasticsearch.threadpool.ThreadPool;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Benchmark comparing BytesRefSwissTable vs legacy BytesRef hash structure.
 *
 * <p>It models the ES|QL STATS workload - inserts followed by a final iteration over results.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@State(Scope.Thread)
public class BytesRefSwissHashBenchmark {

    static {
        BenchmarkLogging.configure();
    }

    @Param({ "1000", "10000", "100000", "1000000", "10000000" })
    int cardinality;

    @Param({ "uniform", "duplicates", "clustered", "collision" })
    String distribution;

    @Param({ "8", "32", "64", "128" })
    int keyBytes;

    @Param({ "1", "4", "8" })
    int numWorkers;

    BytesRef[] keys;
    BytesRef[] trialKeys;

    BytesRefSwissHash swiss;
    BytesRefHash legacy;

    NoopCircuitBreaker breaker;
    LongLongSwissHashBenchmark.TestThreadPool threadPool;

    static final PartitionedHashTable.PartitionSplitter NOOP_SPLITTER = new PartitionedHashTable.PartitionSplitter() {
        @Override
        public void split(int firstId, short[] shiftedIds, int batchSize, int[] batchPartitionCounts, int[] partitionOffsets) {}

        @Override
        public void release(CircuitBreaker breaker) {}
    };

    // --------------------------- SETUP ---------------------------

    @Setup(Level.Trial)
    public void trialSetup() {
        breaker = new NoopCircuitBreaker("dummy");
        threadPool = new LongLongSwissHashBenchmark.TestThreadPool("bench", Settings.EMPTY);
        trialKeys = generate(distribution, cardinality);
    }

    @TearDown(Level.Trial)
    public void trialTearDown() {
        Releasables.close(threadPool);
    }

    @Setup(Level.Iteration)
    public void setup() {
        keys = null;
        keys = generate(distribution, cardinality);

        BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        PageCacheRecycler recycler = PageCacheRecycler.NON_RECYCLING_INSTANCE;
        swiss = SwissHashFactory.getInstance().newBytesRefSwissHash(recycler, breaker, bigArrays);
        legacy = new BytesRefHash(1, bigArrays);
    }

    /**
     * Build Swiss table completely, then iterate.
     * Mirrors STATS build -> finalize -> output.
     */
    @Benchmark
    public long swissBuildThenIterate(Blackhole bh) {
        return swissBuildThenIterateImpl(bh::consume);
    }

    long swissBuildThenIterateImpl(Consumer<BytesRef> bh) {
        for (BytesRef k : keys) {
            swiss.add(k);
        }
        BytesRef scratch = new BytesRef(new byte[1024]);
        for (int i = 0; i < swiss.size(); i++) {
            bh.accept(swiss.get(i, scratch));
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

    long legacyBuildThenIterateImpl(Consumer<BytesRef> bh) {
        for (BytesRef k : keys) {
            legacy.add(k);
        }
        BytesRef scratch = new BytesRef(new byte[1024]);
        for (int i = 0; i < legacy.size(); i++) {
            bh.accept(legacy.get(i, scratch));
        }
        return legacy.size();
    }

    @Benchmark
    public long testPartitionHashOnly() throws Exception {
        BytesRefSwissHash[] workers = new BytesRefSwissHash[numWorkers];
        CountDownLatch collectLatch = new CountDownLatch(numWorkers);
        Collection<PartitionedHashTable.PartitionedHashKeys> sharedPartitionedKeys = ConcurrentCollections.newDeque();
        AtomicInteger nextKeyIndex = new AtomicInteger(0);
        for (int w = 0; w < numWorkers; w++) {
            var worker = workers[w] = SwissHashFactory.getInstance().newBytesRefSwissHash(
                PageCacheRecycler.NON_RECYCLING_INSTANCE,
                breaker,
                BigArrays.NON_RECYCLING_INSTANCE
            );
            threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
                int offset;
                while ((offset = nextKeyIndex.getAndAdd(LongLongSwissHashBenchmark.CHUNK_SIZE)) < trialKeys.length) {
                    int len = Math.min(trialKeys.length - offset, LongLongSwissHashBenchmark.CHUNK_SIZE);
                    if (worker.size() + len > BytesRefSwissHash.PARTITION_THRESHOLD) {
                        sharedPartitionedKeys.add(worker.splitPartition(breaker, NOOP_SPLITTER));
                        worker.clear();
                    }
                    for (int i = 0; i < len; i++) {
                        worker.add(trialKeys[offset + i]);
                    }
                }
                if (worker.size() > 0) {
                    sharedPartitionedKeys.add(worker.splitPartition(breaker, NOOP_SPLITTER));
                    worker.clear();
                }
                collectLatch.countDown();
            });
        }
        collectLatch.await();
        long acc = 0;
        CountDownLatch mergeLatch = new CountDownLatch(numWorkers);
        AtomicInteger nextPartition = new AtomicInteger(0);
        final var partitionedHashKeys = new ArrayList<>(sharedPartitionedKeys);
        sharedPartitionedKeys.clear();
        for (int w = 0; w < numWorkers; w++) {
            BytesRefSwissHash partition = workers[w];
            threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
                int[] mergedIds = null;
                for (;;) {
                    int p = nextPartition.getAndIncrement();
                    if (p >= PartitionedHashTable.NUM_PARTITIONS) {
                        break;
                    }
                    partition.clear();
                    for (var gen : partitionedHashKeys) {
                        int keysInThisPartition = gen.keysInPartition(p);
                        if (mergedIds == null || mergedIds.length < keysInThisPartition) {
                            mergedIds = new int[ArrayUtil.oversize(keysInThisPartition, Integer.BYTES)];
                        }
                        partition.combinePartition(gen, p, mergedIds);
                        gen.releasePartition(breaker, p);
                    }
                }
                mergeLatch.countDown();
            });
        }
        mergeLatch.await();
        for (var worker : workers) {
            acc += worker.size();
            worker.close();
        }
        partitionedHashKeys.forEach(p -> p.releaseAll(breaker));
        return acc;
    }

    private BytesRef[] generate(String dist, int size) {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        BytesRef[] out = new BytesRef[size];

        switch (dist) {
            case "uniform":
                for (int i = 0; i < size; i++) {
                    byte[] data = new byte[keyBytes];
                    r.nextBytes(data);
                    out[i] = new BytesRef(data);
                }
                break;
            case "duplicates":
                // 80% of keys come from a small "hot" set
                int hotSet = Math.max(32, Math.min(1000, size / 50)); // ~2% of cardinality
                BytesRef[] hot = new BytesRef[hotSet];
                for (int i = 0; i < hotSet; i++) {
                    hot[i] = new BytesRef("hot" + i);
                }
                for (int i = 0; i < size; i++) {
                    if (r.nextInt(10) < 8) {               // 80% duplicates
                        out[i] = hot[r.nextInt(hotSet)];
                    } else {                               // 20% random noise
                        out[i] = new BytesRef("k" + r.nextLong());
                    }
                }
                break;
            case "clustered":
                final byte[] base = new byte[keyBytes];
                r.nextBytes(base);
                for (int i = 0; i < size; i++) {
                    byte[] data = base.clone();
                    data[i % keyBytes] ^= (byte) i;
                    out[i] = new BytesRef(data);
                }
                break;
            case "collision":
                // High shared-prefix collisions + varying suffix
                for (int i = 0; i < size; i++) {
                    byte[] data = new byte[keyBytes];
                    Arrays.fill(data, 0, keyBytes - 4, (byte) 0xAA);
                    data[keyBytes - 1] = (byte) i;
                    out[i] = new BytesRef(data);
                }
                break;
            default:
                throw new IllegalArgumentException("unknown distribution: " + dist);
        }
        return out;
    }
}
