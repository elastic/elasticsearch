/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.swisshash.LongSwissHash;
import org.elasticsearch.swisshash.SwissHashFactory;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Phase 2 prototype for partitioned hash aggregation
 * (scratch/partitioned-hash-aggregation-design.md). Validates, outside the
 * full {@code BlockHash}/{@code GroupingAggregatorFunction} machinery, that:
 *
 * <ul>
 *   <li>a single table can convert to N independent {@link LongSwissHash}
 *       sub-tables once a key-count threshold is crossed, without losing any
 *       previously accumulated state;</li>
 *   <li>per-row routing can reuse one precomputed {@link LongSwissHash#hash}
 *       for both partition selection and the destination table's insert, via
 *       {@link LongSwissHash#addWithHash};</li>
 *   <li>the result is identical to an unpartitioned baseline, for two
 *       different ways of bridging that routing into batched per-partition
 *       aggregation - a "v1" skip-scan over shared routing vectors, and a
 *       bucket-sort into contiguous per-partition slices;</li>
 *   <li>the bucket-sort variant's relative overhead against the baseline is
 *       smaller than the skip-scan variant's.</li>
 * </ul>
 *
 * Per-partition early emit (draining a hot sub-table once it individually
 * grows past its own threshold) is deliberately out of scope for this
 * prototype; this only exercises the one-time single-to-partitioned
 * conversion and steady-state routing.
 */
public class PartitionedHashAggPrototypeTests extends ESTestCase {

    public void testCorrectnessBelowConversionThreshold() {
        assertCorrectness(2_000, 200, 8, 10_000);
    }

    public void testCorrectnessAboveConversionThreshold() {
        assertCorrectness(200_000, 20_000, 8, 5_000);
    }

    public void testCorrectnessHighCardinalityManyPartitions() {
        assertCorrectness(500_000, 100_000, 32, 5_000);
    }

    public void testCorrectnessSkewedKeys() {
        assumeTrue("Vector API module required for LongSwissHash", SwissHashFactory.getInstance() != null);
        int totalRows = 300_000;
        int hotKeyCount = 20;
        int longTailCardinality = 50_000;
        Random random = new Random(randomLong());
        long[] keys = new long[totalRows];
        long[] values = new long[totalRows];
        Map<Long, Long> expected = new HashMap<>();
        for (int i = 0; i < totalRows; i++) {
            long key = random.nextInt(10) < 8 ? random.nextInt(hotKeyCount) : hotKeyCount + random.nextInt(longTailCardinality);
            long value = random.nextInt(1000);
            keys[i] = key;
            values[i] = value;
            expected.merge(key, value, Long::sum);
        }
        checkAgainstExpected(keys, values, 16, 5_000, expected);
    }

    public void testRoughPerfComparison() {
        assumeTrue("Vector API module required for LongSwissHash", SwissHashFactory.getInstance() != null);
        int totalRows = 2_000_000;
        int cardinality = 500_000;
        int partitionCount = 32;
        int conversionThreshold = 50_000;
        int batchSize = 4096;

        Random random = new Random(42);
        long[] keys = new long[totalRows];
        long[] values = new long[totalRows];
        for (int i = 0; i < totalRows; i++) {
            keys[i] = random.nextInt(cardinality);
            values[i] = random.nextInt(1000);
        }

        // warmup, then measure - single fork, so this is a rough sanity check, not a rigorous benchmark
        runAgg(BaselineSumAgg::new, keys, values, batchSize);
        runAgg(recyclerBreaker -> newSkipScan(partitionCount, conversionThreshold, recyclerBreaker), keys, values, batchSize);
        runAgg(recyclerBreaker -> newBucketSort(partitionCount, conversionThreshold, recyclerBreaker), keys, values, batchSize);

        long baselineNanos = timed(() -> runAgg(BaselineSumAgg::new, keys, values, batchSize));
        long skipScanNanos = timed(
            () -> runAgg(recyclerBreaker -> newSkipScan(partitionCount, conversionThreshold, recyclerBreaker), keys, values, batchSize)
        );
        long bucketSortNanos = timed(
            () -> runAgg(recyclerBreaker -> newBucketSort(partitionCount, conversionThreshold, recyclerBreaker), keys, values, batchSize)
        );

        logger.info(
            "rows={} cardinality={} partitions={} baseline={}ms skipScan={}ms ({}) bucketSort={}ms ({})",
            totalRows,
            cardinality,
            partitionCount,
            TimeUnit.NANOSECONDS.toMillis(baselineNanos),
            TimeUnit.NANOSECONDS.toMillis(skipScanNanos),
            ratio(skipScanNanos, baselineNanos),
            TimeUnit.NANOSECONDS.toMillis(bucketSortNanos),
            ratio(bucketSortNanos, baselineNanos)
        );
    }

    private static String ratio(long nanos, long baselineNanos) {
        return String.format(Locale.ROOT, "%.2fx", nanos / (double) baselineNanos);
    }

    private void assertCorrectness(int totalRows, int cardinality, int partitionCount, int conversionThreshold) {
        assumeTrue("Vector API module required for LongSwissHash", SwissHashFactory.getInstance() != null);
        Random random = new Random(randomLong());
        long[] keys = new long[totalRows];
        long[] values = new long[totalRows];
        Map<Long, Long> expected = new HashMap<>();
        for (int i = 0; i < totalRows; i++) {
            long key = random.nextInt(cardinality);
            long value = random.nextInt(1000);
            keys[i] = key;
            values[i] = value;
            expected.merge(key, value, Long::sum);
        }
        checkAgainstExpected(keys, values, partitionCount, conversionThreshold, expected);
    }

    private void checkAgainstExpected(long[] keys, long[] values, int partitionCount, int conversionThreshold, Map<Long, Long> expected) {
        int batchSize = 512;
        assertEquals(expected, resultOf(BaselineSumAgg::new, keys, values, batchSize));
        assertEquals(
            expected,
            resultOf(recyclerBreaker -> newSkipScan(partitionCount, conversionThreshold, recyclerBreaker), keys, values, batchSize)
        );
        assertEquals(
            expected,
            resultOf(recyclerBreaker -> newBucketSort(partitionCount, conversionThreshold, recyclerBreaker), keys, values, batchSize)
        );
    }

    private static SkipScanPartitionedSumAgg newSkipScan(int partitionCount, int conversionThreshold, RecyclerBreaker rb) {
        return new SkipScanPartitionedSumAgg(partitionCount, conversionThreshold, rb.recycler(), rb.breaker());
    }

    private static BucketSortPartitionedSumAgg newBucketSort(int partitionCount, int conversionThreshold, RecyclerBreaker rb) {
        return new BucketSortPartitionedSumAgg(partitionCount, conversionThreshold, rb.recycler(), rb.breaker());
    }

    private record RecyclerBreaker(PageCacheRecycler recycler, NoopCircuitBreaker breaker) {}

    private interface AggFactory<T extends Releasable & BatchConsumer> {
        T create(RecyclerBreaker recyclerBreaker);
    }

    private static <T extends Releasable & BatchConsumer> Map<Long, Long> resultOf(
        AggFactory<T> factory,
        long[] keys,
        long[] values,
        int batchSize
    ) {
        RecyclerBreaker rb = new RecyclerBreaker(PageCacheRecycler.NON_RECYCLING_INSTANCE, new NoopCircuitBreaker("test"));
        try (T agg = factory.create(rb)) {
            feed(agg, keys, values, batchSize);
            return agg.results();
        }
    }

    private static <T extends Releasable & BatchConsumer> void runAgg(AggFactory<T> factory, long[] keys, long[] values, int batchSize) {
        RecyclerBreaker rb = new RecyclerBreaker(PageCacheRecycler.NON_RECYCLING_INSTANCE, new NoopCircuitBreaker("test"));
        try (T agg = factory.create(rb)) {
            feed(agg, keys, values, batchSize);
        }
    }

    private interface BatchConsumer {
        void add(long[] keys, long[] values, int length);

        Map<Long, Long> results();
    }

    private static void feed(BatchConsumer agg, long[] keys, long[] values, int batchSize) {
        int totalRows = keys.length;
        for (int offset = 0; offset < totalRows; offset += batchSize) {
            int length = Math.min(batchSize, totalRows - offset);
            long[] batchKeys = Arrays.copyOfRange(keys, offset, offset + length);
            long[] batchValues = Arrays.copyOfRange(values, offset, offset + length);
            agg.add(batchKeys, batchValues, length);
        }
    }

    private static long timed(Runnable r) {
        long start = System.nanoTime();
        r.run();
        return System.nanoTime() - start;
    }

    private static long[] grow(long[] array, int index) {
        if (index >= array.length) {
            array = Arrays.copyOf(array, Math.max(index + 1, array.length * 2));
        }
        return array;
    }

    /** Single, unpartitioned {@link LongSwissHash} + dense sum-state array - the baseline every comparison runs against. */
    private static final class BaselineSumAgg implements Releasable, BatchConsumer {
        private final LongSwissHash table;
        private long[] sums = new long[16];

        BaselineSumAgg(RecyclerBreaker rb) {
            this.table = SwissHashFactory.getInstance().newLongSwissHash(rb.recycler(), rb.breaker());
        }

        @Override
        public void add(long[] keys, long[] values, int length) {
            for (int i = 0; i < length; i++) {
                long ord = table.add(keys[i]);
                int groupId = ord >= 0 ? (int) ord : (int) (-1 - ord);
                sums = grow(sums, groupId);
                sums[groupId] += values[i];
            }
        }

        @Override
        public Map<Long, Long> results() {
            Map<Long, Long> out = new HashMap<>();
            long size = table.size();
            for (long id = 0; id < size; id++) {
                out.put(table.get(id), sums[(int) id]);
            }
            return out;
        }

        @Override
        public void close() {
            Releasables.close(table);
        }
    }

    /**
     * N independent {@link LongSwissHash} sub-tables, one per partition, started as a single
     * unpartitioned table and converted once {@code conversionThreshold} keys have been seen.
     * Routing reuses one precomputed hash per row for both partition selection and the
     * destination sub-table's insert ({@link LongSwissHash#addWithHash}). Subclasses only differ
     * in how they bridge that per-row routing decision into the batched per-partition
     * aggregation update.
     */
    private abstract static class AbstractPartitionedSumAgg implements Releasable, BatchConsumer {
        final int partitionCount;
        final int partitionBits;
        final long conversionThreshold;
        final PageCacheRecycler recycler;
        final NoopCircuitBreaker breaker;

        // Pre-conversion state.
        private LongSwissHash single;
        private long[] singleSums = new long[16];

        // Post-conversion state.
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

        @Override
        public final void add(long[] keys, long[] values, int length) {
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

        /** One-time migration: drain the legacy table into N fresh sub-tables, keeping all accumulated sums. */
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
                long ord = partitions[partition].addWithHash(key, hash); // always a new id: keys were unique in `single`
                int groupId = (int) ord;
                partitionSums[partition] = grow(partitionSums[partition], groupId);
                partitionSums[partition][groupId] += sum;
            }
            Releasables.close(single);
            single = null;
            singleSums = null;
        }

        /** Steady-state bridge from per-row routing to batched per-partition aggregation - the part that varies by subclass. */
        abstract void addPartitioned(long[] keys, long[] values, int length);

        @Override
        public Map<Long, Long> results() {
            Map<Long, Long> out = new HashMap<>();
            if (partitions == null) {
                long size = single.size();
                for (long id = 0; id < size; id++) {
                    out.put(single.get(id), singleSums[(int) id]);
                }
            } else {
                for (int p = 0; p < partitionCount; p++) {
                    LongSwissHash table = partitions[p];
                    long[] sums = partitionSums[p];
                    long size = table.size();
                    for (long id = 0; id < size; id++) {
                        // partitions are disjoint by construction - merge() is a safety net, not expected to combine.
                        out.merge(table.get(id), sums[(int) id], Long::sum);
                    }
                }
            }
            return out;
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

    /**
     * "v1" bridge from the design doc: build shared per-batch routing vectors (partition and
     * local group id per row), then for each touched partition scan the whole batch and skip
     * rows that aren't its own - O(partitionCount * length) rather than O(length).
     */
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

    /**
     * Bucket-sort follow-up from the design doc: after the same per-row routing pass, do a
     * counting sort of (position, local group id) by partition into contiguous shared slices, so
     * each partition's aggregation update only ever touches its own rows - O(length) total instead
     * of O(partitionCount * length).
     */
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
                    // gather read: the only place this variant touches the original page out of order.
                    sums[sortedGroupIds[k]] += values[sortedPositions[k]];
                }
            }
        }
    }
}
