/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;

import static org.elasticsearch.common.util.PartitionedHashTable.NUM_PARTITIONS;
import static org.elasticsearch.common.util.PartitionedHashTable.PARTITION_WRITE_BATCH;

/**
 * Aggregator state for an array of booleans. It is created in a mode where it
 * won't track the {@code groupId}s that are sent to it and it is the
 * responsibility of the caller to only fetch values for {@code groupId}s
 * that it has sent using the {@code selected} parameter when building the
 * results. This is fine when there are no {@code null} values in the input
 * data. But once there are null values in the input data it is
 * <strong>much</strong> more convenient to only send non-null values and
 * the tracking built into the grouping code can't track that. In that case
 * call {@link #enableGroupIdTracking} to transition the state into a mode
 * where it'll track which {@code groupIds} have been written.
 * <p>
 * This class is generated. Edit {@code X-ArrayState.java.st} instead.
 * </p>
 */
final class BooleanArrayState extends AbstractArrayState implements GroupingAggregatorState {
    private static final int WORDS_PER_PAGE = PageCacheRecycler.PAGE_SIZE_IN_BYTES / Long.BYTES;
    static final int PAGE_SIZE = WORDS_PER_PAGE * Long.SIZE;
    private static final int PAGE_SHIFT = Integer.numberOfTrailingZeros(PAGE_SIZE);
    private static final int PAGE_MASK = PAGE_SIZE - 1;
    private static final int INITIAL_SIZE = 256;

    private final boolean init;
    private final CircuitBreaker breaker;
    private long usedBytes;
    private int capacity;
    private long[][] pages;

    BooleanArrayState(BigArrays bigArrays, CircuitBreaker breaker, boolean init) {
        super(bigArrays);
        this.breaker = breaker;
        this.init = init;
        reserveBytes(bytesUsedByPagesArray(1) + bytesUsedByPage(INITIAL_SIZE / Long.SIZE));
        this.pages = new long[1][INITIAL_SIZE / Long.SIZE];
        this.capacity = INITIAL_SIZE;
        if (init) {
            Arrays.fill(pages[0], -1L);
        }
    }

    boolean get(int groupId) {
        return (pages[groupId >>> PAGE_SHIFT][(groupId & PAGE_MASK) >>> 6] & (1L << groupId)) != 0;
    }

    void set(int groupId, boolean value) {
        if (groupId >= capacity) {
            grow(groupId + 1);
        }
        final long[] page = pages[groupId >>> PAGE_SHIFT];
        final int word = (groupId & PAGE_MASK) >>> 6;
        if (value) {
            page[word] |= 1L << groupId;
        } else {
            page[word] &= ~(1L << groupId);
        }
        trackGroupId(groupId);
    }

    void min(int groupId, boolean value) {
        if (groupId >= capacity) {
            grow(groupId + 1);
        }
        if (value == false) {
            pages[groupId >>> PAGE_SHIFT][(groupId & PAGE_MASK) >>> 6] &= ~(1L << groupId);
        }
        trackGroupId(groupId);
    }

    void max(int groupId, boolean value) {
        if (groupId >= capacity) {
            grow(groupId + 1);
        }
        if (value) {
            pages[groupId >>> PAGE_SHIFT][(groupId & PAGE_MASK) >>> 6] |= 1L << groupId;
        }
        trackGroupId(groupId);
    }

    Block toValuesBlock(org.elasticsearch.compute.data.IntVector selected, DriverContext driverContext) {
        if (false == trackingGroupIds()) {
            try (var builder = driverContext.blockFactory().newBooleanVectorFixedBuilder(selected.getPositionCount())) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    builder.appendBoolean(i, get(selected.getInt(i)));
                }
                return builder.build().asBlock();
            }
        }
        try (BooleanBlock.Builder builder = driverContext.blockFactory().newBooleanBlockBuilder(selected.getPositionCount())) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int group = selected.getInt(i);
                if (hasValue(group)) {
                    builder.appendBoolean(get(group));
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    void ensureCapacity(int minSize) {
        if (minSize > capacity) {
            grow(minSize);
        }
    }

    private void grow(int minSize) {
        if (capacity < PAGE_SIZE) {
            final int oldLength = capacity / Long.SIZE;
            final int newLength = Math.min(WORDS_PER_PAGE, ArrayUtil.oversize(Math.ceilDiv(minSize, Long.SIZE), Long.BYTES));
            reserveBytes(bytesUsedByPage(newLength));
            pages[0] = Arrays.copyOf(pages[0], newLength);
            releaseBytes(bytesUsedByPage(oldLength));
            if (init) {
                Arrays.fill(pages[0], oldLength, newLength, -1L);
            }
            capacity = newLength * Long.SIZE;
            if (minSize <= capacity) {
                return;
            }
        }
        final int pageIndex = (minSize - 1) >>> PAGE_SHIFT;
        if (pageIndex >= pages.length) {
            final int newLength = ArrayUtil.oversize(pageIndex + 1, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
            reserveBytes(bytesUsedByPagesArray(newLength));
            final int oldLength = pages.length;
            pages = Arrays.copyOf(pages, newLength);
            releaseBytes(bytesUsedByPagesArray(oldLength));
        }
        if (minSize == capacity + 1) {
            pages[pageIndex] = newPage();
            capacity += PAGE_SIZE;
            return;
        }
        for (int p = capacity >>> PAGE_SHIFT; p <= pageIndex; p++) {
            assert pages[p] == null;
            pages[p] = newPage();
        }
        capacity = (pageIndex + 1) * PAGE_SIZE;
    }

    private long[] newPage() {
        reserveBytes(bytesUsedByPage(WORDS_PER_PAGE));
        final long[] page = new long[WORDS_PER_PAGE];
        if (init) {
            Arrays.fill(page, -1L);
        }
        return page;
    }

    /** Extracts an intermediate view of the contents of this state.  */
    @Override
    public void toIntermediate(
        Block[] blocks,
        int offset,
        IntVector selected,
        org.elasticsearch.compute.operator.DriverContext driverContext
    ) {
        assert blocks.length >= offset + 2;
        boolean allHaveValue = true;
        try (
            var valuesBuilder = driverContext.blockFactory().newBooleanVectorFixedBuilder(selected.getPositionCount());
            var hasValueBuilder = driverContext.blockFactory().newBooleanVectorFixedBuilder(selected.getPositionCount())
        ) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int group = selected.getInt(i);
                if (group < capacity && hasValue(group)) {
                    valuesBuilder.appendBoolean(i, get(group));
                    hasValueBuilder.appendBoolean(i, true);
                } else {
                    allHaveValue = false;
                    valuesBuilder.appendBoolean(i, false);
                    hasValueBuilder.appendBoolean(i, false);
                }
            }
            blocks[offset + 0] = valuesBuilder.build().asBlock();
            if (allHaveValue) {
                // switch to a constant block to reduce memory usage and allow fast checks
                blocks[offset + 1] = driverContext.blockFactory().newConstantBooleanBlockWith(true, selected.getPositionCount());
            } else {
                blocks[offset + 1] = hasValueBuilder.build().asBlock();
            }
        }
    }

    private void reserveBytes(long bytes) {
        breaker.addEstimateBytesAndMaybeBreak(bytes, "BooleanArrayState");
        usedBytes += bytes;
    }

    private void releaseBytes(long bytes) {
        breaker.addWithoutBreaking(-bytes);
        usedBytes -= bytes;
    }

    static long bytesUsedByPagesArray(int length) {
        return RamUsageEstimator.alignObjectSize(
            (long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) RamUsageEstimator.NUM_BYTES_OBJECT_REF * length
        );
    }

    static long bytesUsedByPage(int length) {
        return RamUsageEstimator.alignObjectSize((long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) Long.BYTES * length);
    }

    private static long bytesUsedByPartitionPage(int length) {
        return RamUsageEstimator.alignObjectSize((long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) Byte.BYTES * length);
    }

    private static long bytesUsedBySeenPage(int length) {
        return RamUsageEstimator.alignObjectSize((long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + length);
    }

    private static final class BooleanPartitionedState implements GroupingAggregatorFunction.PartitionedState {
        private static final long BASE_RAM_USAGE = RamUsageEstimator.shallowSizeOf(BooleanPartitionedState.class);
        private static final String LABEL = "BooleanArrayState#partition";

        private final long baseBytes;
        private final boolean[][] values;
        private final boolean[][] seen;

        private BooleanPartitionedState(CircuitBreaker breaker, int partitionSize, boolean trackSeen) {
            baseBytes = BASE_RAM_USAGE + bytesUsedByPagesArray(NUM_PARTITIONS) + (trackSeen ? bytesUsedByPagesArray(NUM_PARTITIONS) : 0);
            long pageBytes = bytesUsedByPartitionPage(partitionSize) + (trackSeen ? bytesUsedBySeenPage(partitionSize) : 0);
            breaker.addEstimateBytesAndMaybeBreak(baseBytes + NUM_PARTITIONS * pageBytes, LABEL);
            values = new boolean[NUM_PARTITIONS][partitionSize];
            seen = trackSeen ? new boolean[NUM_PARTITIONS][partitionSize] : null;
        }

        @Override
        public boolean hasAllValues(int partition) {
            return seen == null;
        }

        @Override
        public void releasePartition(CircuitBreaker breaker, int partition) {
            long usedBytes = 0;
            if (values[partition] != null) {
                usedBytes += bytesUsedByPartitionPage(values[partition].length);
                values[partition] = null;
            }
            if (seen != null && seen[partition] != null) {
                usedBytes += bytesUsedBySeenPage(seen[partition].length);
                seen[partition] = null;
            }
            breaker.addWithoutBreaking(-usedBytes);
        }

        @Override
        public void releaseAll(CircuitBreaker breaker) {
            long usedBytes = baseBytes;
            for (int p = 0; p < NUM_PARTITIONS; p++) {
                if (values[p] != null) {
                    usedBytes += bytesUsedByPartitionPage(values[p].length);
                    values[p] = null;
                }
                if (seen != null && seen[p] != null) {
                    usedBytes += bytesUsedBySeenPage(seen[p].length);
                    seen[p] = null;
                }
            }
            breaker.addWithoutBreaking(-usedBytes);
        }
    }

    private final class BooleanPartitionSplitter implements GroupingAggregatorFunction.PartitionSplitter {
        private final CircuitBreaker partitionBreaker;
        private BooleanPartitionedState partitionedState;

        private BooleanPartitionSplitter(CircuitBreaker partitionBreaker) {
            this.partitionBreaker = partitionBreaker;
            int partitionSize = ArrayUtil.oversize(Math.max(1, Math.ceilDiv(capacity, NUM_PARTITIONS)), Byte.BYTES);
            partitionedState = new BooleanPartitionedState(partitionBreaker, partitionSize, trackingGroupIds());
        }

        @Override
        public void split(int firstId, short[] shiftedIds, int batchSize, int[] batchPartitionCounts, int[] partitionOffsets) {
            if (partitionedState.seen == null) {
                splitWithAllValues(firstId, shiftedIds, batchSize, batchPartitionCounts, partitionOffsets);
                return;
            }
            for (int p = 0; p < NUM_PARTITIONS; p++) {
                final int count = batchPartitionCounts[p];
                if (count == 0) {
                    continue;
                }
                final int offset = partitionOffsets[p];
                ensurePartitionCapacity(p, offset + count);
                final int base = p * PARTITION_WRITE_BATCH;
                for (int i = 0; i < count; i++) {
                    final int id = firstId + shiftedIds[base + i];
                    if (hasValue(id)) {
                        assert id < capacity : id + ">=" + capacity;
                        partitionedState.values[p][offset + i] = get(id);
                        partitionedState.seen[p][offset + i] = true;
                    }
                }
            }
        }

        void splitWithAllValues(int firstId, short[] shiftedIds, int batchSize, int[] batchPartitionCounts, int[] partitionOffsets) {
            for (int p = 0; p < NUM_PARTITIONS; p++) {
                final int count = batchPartitionCounts[p];
                if (count == 0) {
                    continue;
                }
                final int offset = partitionOffsets[p];
                ensurePartitionCapacity(p, offset + count);
                final int base = p * PARTITION_WRITE_BATCH;
                for (int i = 0; i < count; i++) {
                    final int id = firstId + shiftedIds[base + i];
                    assert id < capacity : id + ">=" + capacity;
                    partitionedState.values[p][offset + i] = get(id);
                }
            }
        }

        private void ensurePartitionCapacity(int partition, int minSize) {
            final boolean[] oldValues = partitionedState.values[partition];
            if (oldValues.length >= minSize) {
                return;
            }
            final int newSize = ArrayUtil.oversize(minSize, Byte.BYTES);
            partitionBreaker.addEstimateBytesAndMaybeBreak(bytesUsedByPartitionPage(newSize), BooleanPartitionedState.LABEL);
            partitionedState.values[partition] = Arrays.copyOf(oldValues, newSize);
            partitionBreaker.addWithoutBreaking(-bytesUsedByPartitionPage(oldValues.length));

            if (partitionedState.seen != null) {
                final boolean[] oldSeen = partitionedState.seen[partition];
                partitionBreaker.addEstimateBytesAndMaybeBreak(bytesUsedBySeenPage(newSize), BooleanPartitionedState.LABEL);
                partitionedState.seen[partition] = Arrays.copyOf(oldSeen, newSize);
                partitionBreaker.addWithoutBreaking(-bytesUsedBySeenPage(oldSeen.length));
            }
        }

        @Override
        public BooleanPartitionedState finish() {
            final BooleanPartitionedState result = partitionedState;
            partitionedState = null;
            return result;
        }

        @Override
        public void release(CircuitBreaker breaker) {
            if (partitionedState != null) {
                partitionedState.releaseAll(breaker);
                partitionedState = null;
            }
        }
    }

    GroupingAggregatorFunction.PartitionSplitter createPartitioningSplitter(CircuitBreaker breaker) {
        return new BooleanPartitionSplitter(breaker);
    }

    boolean[] partitionValues(GroupingAggregatorFunction.PartitionedState source, int partition) {
        return ((BooleanPartitionedState) source).values[partition];
    }

    boolean[] partitionSeen(GroupingAggregatorFunction.PartitionedState source, int partition) {
        final boolean[][] seen = ((BooleanPartitionedState) source).seen;
        return seen == null ? null : seen[partition];
    }

    void appendPartition(boolean[] src, int firstId, int length) {
        final int end = firstId + length;
        assert end <= capacity : end + " > " + capacity;
        for (int id = firstId, i = 0; id < end; id++, i++) {
            final long[] page = pages[id >>> PAGE_SHIFT];
            final int word = (id & PAGE_MASK) >>> 6;
            if (src[i]) {
                page[word] |= 1L << id;
            } else {
                page[word] &= ~(1L << id);
            }
        }
        trackGroupIds(firstId, end);
    }

    @Override
    public void close() {
        pages = null;
        final long bytes = usedBytes;
        usedBytes = 0;
        Releasables.close(() -> breaker.addWithoutBreaking(-bytes), super::close);
    }
}
