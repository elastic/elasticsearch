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
$if(long)$
import org.elasticsearch.compute.data.IntVector;
$endif$
import org.elasticsearch.compute.data.$Type$Block;
$if(int)$
import org.elasticsearch.compute.data.$Type$Vector;
$endif$
$if(boolean||double||float)$
import org.elasticsearch.compute.data.IntVector;
$endif$
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;

/**
 * Aggregator state for an array of $type$s. It is created in a mode where it
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
final class $Type$ArrayState extends AbstractArrayState implements GroupingAggregatorState {
$if(boolean)$
    private static final int WORDS_PER_PAGE = PageCacheRecycler.PAGE_SIZE_IN_BYTES / Long.BYTES;
    static final int PAGE_SIZE = WORDS_PER_PAGE * Long.SIZE;
$else$
    static final int PAGE_SIZE = PageCacheRecycler.PAGE_SIZE_IN_BYTES / $BYTES$;
$endif$
    private static final int PAGE_SHIFT = Integer.numberOfTrailingZeros(PAGE_SIZE);
    private static final int PAGE_MASK = PAGE_SIZE - 1;
    private static final int INITIAL_SIZE = 256;

    private final $type$ init;
    private final CircuitBreaker breaker;
    private long usedBytes;
    private int capacity;
$if(boolean)$
    private long[][] pages;

    $Type$ArrayState(BigArrays bigArrays, CircuitBreaker breaker, $type$ init) {
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

    $type$ get(int groupId) {
        return (pages[groupId >>> PAGE_SHIFT][(groupId & PAGE_MASK) >>> 6] & (1L << groupId)) != 0;
    }

    void set(int groupId, $type$ value) {
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

$else$
    private $type$[][] pages;

    $Type$ArrayState(BigArrays bigArrays, CircuitBreaker breaker, $type$ init) {
        super(bigArrays);
        this.breaker = breaker;
        this.init = init;
        reserveBytes(bytesUsedByPagesArray(1) + bytesUsedByPage(INITIAL_SIZE));
        this.pages = new $type$[1][INITIAL_SIZE];
        this.capacity = INITIAL_SIZE;
        if (init != 0) {
            Arrays.fill(pages[0], init);
        }
    }

    $type$ get(int groupId) {
        return pages[groupId >>> PAGE_SHIFT][groupId & PAGE_MASK];
    }

    void set(int groupId, $type$ value) {
        if (groupId >= capacity) {
            grow(groupId + 1);
        }
        pages[groupId >>> PAGE_SHIFT][groupId & PAGE_MASK] = value;
        trackGroupId(groupId);
    }
$endif$

    $type$ getOrDefault(int groupId) {
        return groupId < capacity ? get(groupId) : init;
    }

$if(long||double)$
    void increment(int groupId, $type$ value) {
        if (groupId >= capacity) {
            grow(groupId + 1);
        }
        pages[groupId >>> PAGE_SHIFT][groupId & PAGE_MASK] += value;
        trackGroupId(groupId);
    }
$endif$

    Block toValuesBlock(org.elasticsearch.compute.data.IntVector selected, DriverContext driverContext) {
        if (false == trackingGroupIds()) {
            try (var builder = driverContext.blockFactory().new$Type$VectorFixedBuilder(selected.getPositionCount())) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    builder.append$Type$(i, get(selected.getInt(i)));
                }
                return builder.build().asBlock();
            }
        }
        try ($Type$Block.Builder builder = driverContext.blockFactory().new$Type$BlockBuilder(selected.getPositionCount())) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int group = selected.getInt(i);
                if (hasValue(group)) {
                    builder.append$Type$(get(group));
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
$if(boolean)$
            final int oldLength = capacity / Long.SIZE;
            final int newLength = Math.min(WORDS_PER_PAGE, ArrayUtil.oversize(Math.ceilDiv(minSize, Long.SIZE), Long.BYTES));
$else$
            final int oldLength = capacity;
            final int newLength = Math.min(PAGE_SIZE, ArrayUtil.oversize(minSize, $BYTES$));
$endif$
            reserveBytes(bytesUsedByPage(newLength));
            pages[0] = Arrays.copyOf(pages[0], newLength);
            releaseBytes(bytesUsedByPage(oldLength));
$if(boolean)$
            if (init) {
                Arrays.fill(pages[0], oldLength, newLength, -1L);
            }
            capacity = newLength * Long.SIZE;
$else$
            if (init != 0) {
                Arrays.fill(pages[0], oldLength, newLength, init);
            }
            capacity = newLength;
$endif$
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

$if(boolean)$
    private long[] newPage() {
        reserveBytes(bytesUsedByPage(WORDS_PER_PAGE));
        final long[] page = new long[WORDS_PER_PAGE];
        if (init) {
            Arrays.fill(page, -1L);
        }
        return page;
    }

$else$
    private $type$[] newPage() {
        reserveBytes(bytesUsedByPage(PAGE_SIZE));
        final $type$[] page = new $type$[PAGE_SIZE];
        if (init != 0) {
            Arrays.fill(page, init);
        }
        return page;
    }
$endif$

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
            var valuesBuilder = driverContext.blockFactory().new$Type$VectorFixedBuilder(selected.getPositionCount());
            var hasValueBuilder = driverContext.blockFactory().newBooleanVectorFixedBuilder(selected.getPositionCount())
        ) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int group = selected.getInt(i);
                if (group < capacity && hasValue(group)) {
                    valuesBuilder.append$Type$(i, get(group));
                    hasValueBuilder.appendBoolean(i, true);
                } else {
                    allHaveValue = false;
                    valuesBuilder.append$Type$(i, $if(boolean)$false$else$0$endif$);
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
        breaker.addEstimateBytesAndMaybeBreak(bytes, "$Type$ArrayState");
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
        return RamUsageEstimator.alignObjectSize((long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) $if(boolean)$Long.BYTES$else$$BYTES$$endif$ * length);
    }

    @Override
    public void close() {
        pages = null;
        final long bytes = usedBytes;
        usedBytes = 0;
        Releasables.close(() -> breaker.addWithoutBreaking(-bytes), super::close);
    }
}
