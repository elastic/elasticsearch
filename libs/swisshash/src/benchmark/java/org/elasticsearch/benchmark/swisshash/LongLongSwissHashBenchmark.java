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
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.PartitionedHashTable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.node.Node;
import org.elasticsearch.swisshash.LongLongSwissHash;
import org.elasticsearch.swisshash.SwissHashFactory;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.DefaultBuiltInExecutorBuilders;
import org.elasticsearch.threadpool.ExecutorBuilder;
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
@Fork(value = 3, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector", "-Xms10g", "-Xmx10g" })
@State(Scope.Thread)
public class LongLongSwissHashBenchmark {
    static {
        BenchmarkLogging.configure();
    }

    @Param({ "1000000", "10000000", "100000000" })
    int cardinality;

    long[] keys;

    BigArrays bigArrays;
    PageCacheRecycler recycler;
    NoopCircuitBreaker breaker;

    TestThreadPool threadPool;

    @Param({ "1", "4", "8" })
    int numWorkers;

    private static final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("bench"))
        .build();

    @Setup(Level.Trial)
    public void setup() {
        keys = generate(cardinality);
        bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        recycler = PageCacheRecycler.NON_RECYCLING_INSTANCE;
        breaker = new NoopCircuitBreaker("dummy");
        threadPool = new TestThreadPool("test", Settings.EMPTY);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        Releasables.close(threadPool);
    }

    static final int CHUNK_SIZE = 1024;

    public static class TestThreadPool extends ThreadPool implements Releasable {
        public TestThreadPool(String name, Settings settings, ExecutorBuilder<?>... customBuilders) {
            super(
                Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), name).put(settings).build(),
                MeterRegistry.NOOP,
                new DefaultBuiltInExecutorBuilders(),
                customBuilders
            );
        }

        @Override
        public void close() {
            ThreadPool.terminate(this, 10, TimeUnit.SECONDS);
        }
    }

    @Benchmark
    public long testOnePassHashOnly() {
        try (var swiss = SwissHashFactory.getInstance().newLongLongSwissHash(recycler, breaker)) {
            int offset = 0;
            long[] batch = new long[CHUNK_SIZE];
            int[] ids = new int[CHUNK_SIZE];
            while (offset < keys.length) {
                int len = Math.min(keys.length - offset, CHUNK_SIZE);
                System.arraycopy(keys, offset, batch, 0, len);
                if (swiss.supportBulkAdd()) {
                    swiss.bulkAdd(batch, batch, ids, len);
                } else {
                    for (int i = 0; i < len; i++) {
                        var v = batch[i];
                        long id = swiss.add(v, v);
                        if (id < 0) {
                            id = -1 - id;
                        }
                        ids[i] = (int) id;
                    }
                }
                offset += len;
            }
            return swiss.size();
        }
    }

    static final PartitionedHashTable.PartitionSplitter NOOP_SPLITTER = new PartitionedHashTable.PartitionSplitter() {
        @Override
        public void split(int firstId, short[] shiftedIds, int batchSize, int[] batchPartitionCounts, int[] partitionOffsets) {

        }

        @Override
        public void release(CircuitBreaker breaker) {

        }
    };

    @Benchmark
    public long testPartitionHashOnly() throws Exception {
        LongLongSwissHash[] workers = new LongLongSwissHash[numWorkers];
        CountDownLatch collectLatch = new CountDownLatch(numWorkers);
        Collection<PartitionedHashTable.PartitionedHashKeys> sharedPartitionedKeys = ConcurrentCollections.newDeque();
        AtomicInteger nextKeyIndex = new AtomicInteger(0);
        for (int w = 0; w < numWorkers; w++) {
            var worker = workers[w] = SwissHashFactory.getInstance().newLongLongSwissHash(recycler, breaker);
            threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
                long[] batchKeys = new long[CHUNK_SIZE];
                int[] ids = new int[CHUNK_SIZE];
                int offset;
                while ((offset = nextKeyIndex.getAndAdd(CHUNK_SIZE)) < keys.length) {
                    int len = Math.min(keys.length - offset, CHUNK_SIZE);
                    if (worker.size() + len > LongLongSwissHash.PARTITION_THRESHOLD) {
                        sharedPartitionedKeys.add(worker.splitPartition(breaker, NOOP_SPLITTER));
                        worker.clear();
                    }
                    if (worker.supportBulkAdd()) {
                        System.arraycopy(keys, offset, batchKeys, 0, len);
                        worker.bulkAdd(batchKeys, batchKeys, ids, len);
                    } else {
                        for (int i = 0; i < len; i++) {
                            var v = keys[offset + i];
                            var id = (int) worker.add(v, v);
                            if (id < 0) {
                                id = -1 - id;
                            }
                            ids[i] = id;
                        }
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
            LongLongSwissHash partition = workers[w];
            threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
                int[] mergedKeys = null;
                for (;;) {
                    int p = nextPartition.getAndIncrement();
                    if (p >= LongLongSwissHash.NUM_PARTITIONS) {
                        break;
                    }
                    partition.clear();
                    for (var gen : partitionedHashKeys) {
                        int keysInThisPartition = gen.keysInPartition(p);
                        if (mergedKeys == null || mergedKeys.length < keysInThisPartition) {
                            mergedKeys = new int[ArrayUtil.oversize(keysInThisPartition, Integer.BYTES)];
                        }
                        partition.combinePartition(gen, p, mergedKeys);
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

    private long[] generate(int size) {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        long[] out = new long[size];
        for (int i = 0; i < size; i++) {
            out[i] = r.nextLong(size * 10L);
        }
        return out;
    }
}
