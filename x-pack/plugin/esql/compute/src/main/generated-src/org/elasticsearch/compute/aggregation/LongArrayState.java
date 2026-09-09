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
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;

/**
 * Aggregator state for an array of longs. It is created in a mode where it
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
final class LongArrayState extends AbstractArrayState implements GroupingAggregatorState {
    static final int PAGE_SIZE = PageCacheRecycler.PAGE_SIZE_IN_BYTES / Long.BYTES;
    private static final int PAGE_SHIFT = Integer.numberOfTrailingZeros(PAGE_SIZE);
    private static final int PAGE_MASK = PAGE_SIZE - 1;
    private static final int INITIAL_SIZE = 256;

    private final long init;
    private final CircuitBreaker breaker;
    private long usedBytes;
    private int capacity;
    private long[][] pages;

    LongArrayState(BigArrays bigArrays, CircuitBreaker breaker, long init) {
        super(bigArrays);
        this.breaker = breaker;
        this.init = init;
        reserveBytes(bytesUsedByPagesArray(1) + bytesUsedByPage(INITIAL_SIZE));
        this.pages = new long[1][INITIAL_SIZE];
        this.capacity = INITIAL_SIZE;
        if (init != 0) {
            Arrays.fill(pages[0], init);
        }
    }

    long get(int groupId) {
        return pages[groupId >>> PAGE_SHIFT][groupId & PAGE_MASK];
    }

    void set(int groupId, long value) {
        if (groupId >= capacity) {
            grow(groupId + 1);
        }
        pages[groupId >>> PAGE_SHIFT][groupId & PAGE_MASK] = value;
        trackGroupId(groupId);
    }

    long getOrDefault(int groupId) {
        return groupId < capacity ? get(groupId) : init;
    }

    void increment(int groupId, long value) {
        if (groupId >= capacity) {
            grow(groupId + 1);
        }
        pages[groupId >>> PAGE_SHIFT][groupId & PAGE_MASK] += value;
        trackGroupId(groupId);
    }

    Block toValuesBlock(org.elasticsearch.compute.data.IntVector selected, DriverContext driverContext) {
        if (false == trackingGroupIds()) {
            try (var builder = driverContext.blockFactory().newLongVectorFixedBuilder(selected.getPositionCount())) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    builder.appendLong(i, get(selected.getInt(i)));
                }
                return builder.build().asBlock();
            }
        }
        try (LongBlock.Builder builder = driverContext.blockFactory().newLongBlockBuilder(selected.getPositionCount())) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int group = selected.getInt(i);
                if (hasValue(group)) {
                    builder.appendLong(get(group));
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
            final int oldLength = capacity;
            final int newLength = Math.min(PAGE_SIZE, ArrayUtil.oversize(minSize, Long.BYTES));
            reserveBytes(bytesUsedByPage(newLength));
            pages[0] = Arrays.copyOf(pages[0], newLength);
            releaseBytes(bytesUsedByPage(oldLength));
            if (init != 0) {
                Arrays.fill(pages[0], oldLength, newLength, init);
            }
            capacity = newLength;
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
        reserveBytes(bytesUsedByPage(PAGE_SIZE));
        final long[] page = new long[PAGE_SIZE];
        if (init != 0) {
            Arrays.fill(page, init);
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
            var valuesBuilder = driverContext.blockFactory().newLongVectorFixedBuilder(selected.getPositionCount());
            var hasValueBuilder = driverContext.blockFactory().newBooleanVectorFixedBuilder(selected.getPositionCount())
        ) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int group = selected.getInt(i);
                if (group < capacity && hasValue(group)) {
                    valuesBuilder.appendLong(i, get(group));
                    hasValueBuilder.appendBoolean(i, true);
                } else {
                    allHaveValue = false;
                    valuesBuilder.appendLong(i, 0);
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
        breaker.addEstimateBytesAndMaybeBreak(bytes, "LongArrayState");
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

    @Override
    public void close() {
        pages = null;
        final long bytes = usedBytes;
        usedBytes = 0;
        Releasables.close(() -> breaker.addWithoutBreaking(-bytes), super::close);
    }
}
