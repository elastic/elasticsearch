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

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 3, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector", "-Xms20g", "-Xmx20g" })
@State(Scope.Thread)
public class BytesRefSwissHashPartitionBenchmark {
    static {
        BenchmarkLogging.configure();
    }

    @Param({ "1000000", "10000000", "100000000" })
    int cardinality;

    @Param({ "1", "4", "8" })
    int numWorkers;

    @Param({ "8", "32" })
    int keyBytes;

    @Param({ "false", "true" })
    boolean variableLength;

    BytesRef[] keys;

    BigArrays bigArrays;
    PageCacheRecycler recycler;
    NoopCircuitBreaker breaker;
    LongLongSwissHashBenchmark.TestThreadPool threadPool;

    static final PartitionedHashTable.PartitionSplitter NOOP_SPLITTER = new PartitionedHashTable.PartitionSplitter() {
        @Override
        public void split(int firstId, short[] shiftedIds, int batchSize, int[] batchPartitionCounts, int[] partitionOffsets) {}

        @Override
        public void release(CircuitBreaker breaker) {}
    };

    @Setup(Level.Trial)
    public void setup() {
        keys = generate(cardinality);
        bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        recycler = PageCacheRecycler.NON_RECYCLING_INSTANCE;
        breaker = new NoopCircuitBreaker("dummy");
        threadPool = new LongLongSwissHashBenchmark.TestThreadPool("test", Settings.EMPTY);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        Releasables.close(threadPool);
    }

    @Benchmark
    public long testPartitionHashOnly() throws Exception {
        BytesRefSwissHash[] workers = new BytesRefSwissHash[numWorkers];
        CountDownLatch collectLatch = new CountDownLatch(numWorkers);
        Collection<PartitionedHashTable.PartitionedHashKeys> sharedPartitionedKeys = ConcurrentCollections.newDeque();
        AtomicInteger nextKeyIndex = new AtomicInteger(0);
        for (int w = 0; w < numWorkers; w++) {
            var worker = workers[w] = SwissHashFactory.getInstance().newBytesRefSwissHash(recycler, breaker, bigArrays);
            threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
                int offset;
                while ((offset = nextKeyIndex.getAndAdd(LongLongSwissHashBenchmark.CHUNK_SIZE)) < keys.length) {
                    int len = Math.min(keys.length - offset, LongLongSwissHashBenchmark.CHUNK_SIZE);
                    if (worker.size() + len > BytesRefSwissHash.PARTITION_THRESHOLD) {
                        sharedPartitionedKeys.add(worker.splitPartition(breaker, NOOP_SPLITTER));
                        worker.clear();
                    }
                    for (int i = 0; i < len; i++) {
                        worker.add(keys[offset + i]);
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

    private BytesRef[] generate(int size) {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        BytesRef[] out = new BytesRef[size];
        final int minLen = keyBytes / 2;
        final int maxLen = keyBytes + keyBytes / 2;
        for (int i = 0; i < size; i++) {
            int len = variableLength ? r.nextInt(minLen, maxLen + 1) : keyBytes;
            byte[] data = new byte[len];
            r.nextBytes(data);
            out[i] = new BytesRef(data);
        }
        return out;
    }
}
