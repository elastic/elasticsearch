/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.compute.aggregation;

import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
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

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Rigorous version of the rough perf check in
 * {@code PartitionedHashAggPrototypeTests} (esql compute module, Phase 2 of
 * scratch/partitioned-hash-aggregation-design.md): compares an unpartitioned
 * baseline against the "v1" skip-scan routing bridge and the bucket-sort
 * follow-up. The three aggregator strategies are duplicated here rather than
 * shared with the test, which are private nested classes in a test
 * sourceset not visible to this module - same reason
 * {@code LongSwissHashBenchmark} doesn't reuse {@code LongSwissHashTests}.
 *
 * <p>Each benchmark method builds a fresh table per invocation, so it
 * measures a full build of {@code TOTAL_ROWS} keys (including the one-time
 * single-to-partitioned conversion for the partitioned variants), not a
 * re-add into an already-built table.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector", "-Xms4g", "-Xmx4g" })
@State(Scope.Thread)
public class PartitionedHashAggBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    private static final int TOTAL_ROWS = 2_000_000;
    private static final int CONVERSION_THRESHOLD = 5_000;
    private static final int BATCH_SIZE = 4096;

    @Param({ "10000", "100000", "1000000" })
    int cardinality;

    @Param({ "8", "32" })
    int partitionCount;

    long[] keys;
    long[] values;

    PageCacheRecycler recycler;
    NoopCircuitBreaker breaker;

    @Setup(Level.Trial)
    public void setup() {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        keys = new long[TOTAL_ROWS];
        values = new long[TOTAL_ROWS];
        for (int i = 0; i < TOTAL_ROWS; i++) {
            keys[i] = r.nextInt(cardinality);
            values[i] = r.nextInt(1000);
        }
        recycler = PageCacheRecycler.NON_RECYCLING_INSTANCE;
        breaker = new NoopCircuitBreaker("dummy");
    }

    @Benchmark
    public long baseline() {
        try (BaselineSumAgg agg = new BaselineSumAgg(recycler, breaker)) {
            feed(agg::add);
            return agg.checksum();
        }
    }

    @Benchmark
    public long skipScan() {
        try (SkipScanPartitionedSumAgg agg = new SkipScanPartitionedSumAgg(partitionCount, CONVERSION_THRESHOLD, recycler, breaker)) {
            feed(agg::add);
            return agg.checksum();
        }
    }

    @Benchmark
    public long bucketSort() {
        try (BucketSortPartitionedSumAgg agg = new BucketSortPartitionedSumAgg(partitionCount, CONVERSION_THRESHOLD, recycler, breaker)) {
            feed(agg::add);
            return agg.checksum();
        }
    }

    private interface BatchConsumer {
        void add(long[] keys, long[] values, int length);
    }

    private void feed(BatchConsumer agg) {
        for (int offset = 0; offset < TOTAL_ROWS; offset += BATCH_SIZE) {
            int length = Math.min(BATCH_SIZE, TOTAL_ROWS - offset);
            agg.add(Arrays.copyOfRange(keys, offset, offset + length), Arrays.copyOfRange(values, offset, offset + length), length);
        }
    }

    private static long[] grow(long[] array, int index) {
        if (index >= array.length) {
            array = Arrays.copyOf(array, Math.max(index + 1, array.length * 2));
        }
        return array;
    }

    private static final class BaselineSumAgg implements Releasable {
        private final LongSwissHash table;
        private long[] sums = new long[16];

        BaselineSumAgg(PageCacheRecycler recycler, NoopCircuitBreaker breaker) {
            this.table = SwissHashFactory.getInstance().newLongSwissHash(recycler, breaker);
        }

        void add(long[] keys, long[] values, int length) {
            for (int i = 0; i < length; i++) {
                long ord = table.add(keys[i]);
                int groupId = ord >= 0 ? (int) ord : (int) (-1 - ord);
                sums = grow(sums, groupId);
                sums[groupId] += values[i];
            }
        }

        long checksum() {
            long acc = 0;
            long size = table.size();
            for (long id = 0; id < size; id++) {
                acc += table.get(id) ^ sums[(int) id];
            }
            return acc;
        }

        @Override
        public void close() {
            Releasables.close(table);
        }
    }

    private abstract static class AbstractPartitionedSumAgg implements Releasable {
        final int partitionCount;
        final int partitionBits;
        final long conversionThreshold;
        final PageCacheRecycler recycler;
        final NoopCircuitBreaker breaker;

        private LongSwissHash single;
        private long[] singleSums = new long[16];

        LongSwissHash[] partitions;
        long[][] partitionSums;

        AbstractPartitionedSumAgg(int partitionCount, long conversionThreshold, PageCacheRecycler recycler, NoopCircuitBreaker breaker) {
            if (Integer.bitCount(partitionCount) != 1) {
                throw new IllegalArgumentException("partitionCount must be a power of two, got " + partitionCount);
            }
            this.partitionCount = partitionCount;
            this.partitionBits = Integer.numberOfTrailingZeros(partitionCount);
            this.conversionThreshold = conversionThreshold;
            this.recycler = recycler;
            this.breaker = breaker;
            this.single = SwissHashFactory.getInstance().newLongSwissHash(recycler, breaker);
        }

        final void add(long[] keys, long[] values, int length) {
            if (partitions == null) {
                addSingle(keys, values, length);
                if (single.size() >= conversionThreshold) {
                    convert();
                }
            } else {
                addPartitioned(keys, values, length);
            }
        }

        private void addSingle(long[] keys, long[] values, int length) {
            for (int i = 0; i < length; i++) {
                long ord = single.add(keys[i]);
                int groupId = ord >= 0 ? (int) ord : (int) (-1 - ord);
                singleSums = grow(singleSums, groupId);
                singleSums[groupId] += values[i];
            }
        }

        private void convert() {
            partitions = new LongSwissHash[partitionCount];
            partitionSums = new long[partitionCount][];
            for (int p = 0; p < partitionCount; p++) {
                partitions[p] = SwissHashFactory.getInstance().newLongSwissHash(recycler, breaker);
                partitionSums[p] = new long[16];
            }
            long size = single.size();
            for (long id = 0; id < size; id++) {
                long key = single.get(id);
                long sum = singleSums[(int) id];
                int hash = LongSwissHash.hash(key);
                int partition = hash >>> (Integer.SIZE - partitionBits);
                long ord = partitions[partition].addWithHash(key, hash);
                int groupId = (int) ord;
                partitionSums[partition] = grow(partitionSums[partition], groupId);
                partitionSums[partition][groupId] += sum;
            }
            Releasables.close(single);
            single = null;
            singleSums = null;
        }

        abstract void addPartitioned(long[] keys, long[] values, int length);

        long checksum() {
            long acc = 0;
            if (partitions == null) {
                long size = single.size();
                for (long id = 0; id < size; id++) {
                    acc += single.get(id) ^ singleSums[(int) id];
                }
            } else {
                for (int p = 0; p < partitionCount; p++) {
                    LongSwissHash table = partitions[p];
                    long[] sums = partitionSums[p];
                    long size = table.size();
                    for (long id = 0; id < size; id++) {
                        acc += table.get(id) ^ sums[(int) id];
                    }
                }
            }
            return acc;
        }

        @Override
        public void close() {
            if (single != null) {
                Releasables.close(single);
            }
            if (partitions != null) {
                Releasables.close(partitions);
            }
        }
    }

    private static final class SkipScanPartitionedSumAgg extends AbstractPartitionedSumAgg {
        SkipScanPartitionedSumAgg(int partitionCount, long conversionThreshold, PageCacheRecycler recycler, NoopCircuitBreaker breaker) {
            super(partitionCount, conversionThreshold, recycler, breaker);
        }

        @Override
        void addPartitioned(long[] keys, long[] values, int length) {
            int[] partitionOf = new int[length];
            int[] localGroupId = new int[length];
            boolean[] touched = new boolean[partitionCount];
            for (int i = 0; i < length; i++) {
                int hash = LongSwissHash.hash(keys[i]);
                int partition = hash >>> (Integer.SIZE - partitionBits);
                long ord = partitions[partition].addWithHash(keys[i], hash);
                int groupId = ord >= 0 ? (int) ord : (int) (-1 - ord);
                partitionOf[i] = partition;
                localGroupId[i] = groupId;
                touched[partition] = true;
                partitionSums[partition] = grow(partitionSums[partition], groupId);
            }
            for (int p = 0; p < partitionCount; p++) {
                if (touched[p] == false) {
                    continue;
                }
                long[] sums = partitionSums[p];
                for (int i = 0; i < length; i++) {
                    if (partitionOf[i] != p) {
                        continue;
                    }
                    sums[localGroupId[i]] += values[i];
                }
            }
        }
    }

    private static final class BucketSortPartitionedSumAgg extends AbstractPartitionedSumAgg {
        BucketSortPartitionedSumAgg(int partitionCount, long conversionThreshold, PageCacheRecycler recycler, NoopCircuitBreaker breaker) {
            super(partitionCount, conversionThreshold, recycler, breaker);
        }

        @Override
        void addPartitioned(long[] keys, long[] values, int length) {
            int[] partitionOf = new int[length];
            int[] localGroupId = new int[length];
            int[] counts = new int[partitionCount];
            for (int i = 0; i < length; i++) {
                int hash = LongSwissHash.hash(keys[i]);
                int partition = hash >>> (Integer.SIZE - partitionBits);
                long ord = partitions[partition].addWithHash(keys[i], hash);
                int groupId = ord >= 0 ? (int) ord : (int) (-1 - ord);
                partitionOf[i] = partition;
                localGroupId[i] = groupId;
                partitionSums[partition] = grow(partitionSums[partition], groupId);
                counts[partition]++;
            }

            int[] offsets = new int[partitionCount + 1];
            for (int p = 0; p < partitionCount; p++) {
                offsets[p + 1] = offsets[p] + counts[p];
            }
            int[] cursor = Arrays.copyOf(offsets, partitionCount);
            int[] sortedPositions = new int[length];
            int[] sortedGroupIds = new int[length];
            for (int i = 0; i < length; i++) {
                int dest = cursor[partitionOf[i]]++;
                sortedPositions[dest] = i;
                sortedGroupIds[dest] = localGroupId[i];
            }

            for (int p = 0; p < partitionCount; p++) {
                int start = offsets[p];
                int end = offsets[p + 1];
                if (start == end) {
                    continue;
                }
                long[] sums = partitionSums[p];
                for (int k = start; k < end; k++) {
                    sums[sortedGroupIds[k]] += values[sortedPositions[k]];
                }
            }
        }
    }
}
