/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;

import java.util.Arrays;
import java.util.List;

public class CountGroupingAggregatorFunction implements GroupingAggregatorFunction {

    private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
        new IntermediateStateDesc("count", ElementType.LONG),
        new IntermediateStateDesc("seen", ElementType.BOOLEAN)
    );

    private static final int INTS_PER_PAGE = PageCacheRecycler.PAGE_SIZE_IN_BYTES / Integer.BYTES;
    private static final int INT_PAGE_SHIFT = Integer.numberOfTrailingZeros(INTS_PER_PAGE);
    private static final int INT_PAGE_MASK = INTS_PER_PAGE - 1;

    private static final int LONGS_PER_PAGE = PageCacheRecycler.PAGE_SIZE_IN_BYTES / Long.BYTES;
    private static final int LONG_PAGE_SHIFT = Integer.numberOfTrailingZeros(LONGS_PER_PAGE);
    private static final int LONG_PAGE_MASK = LONGS_PER_PAGE - 1;

    private final CircuitBreaker breaker;
    private long usedBytes;
    private int capacity;
    private int[][] intPages;
    private long[][] longPages;

    private final List<Integer> channels;
    private final DriverContext driverContext;
    private final boolean countAll;

    public static List<IntermediateStateDesc> intermediateStateDesc() {
        return INTERMEDIATE_STATE_DESC;
    }

    CountGroupingAggregatorFunction(List<Integer> channels, DriverContext driverContext) {
        this.channels = channels;
        this.driverContext = driverContext;
        this.countAll = channels.isEmpty();
        this.breaker = driverContext.breaker();
        final int initialLength = 256;
        reserveBytes(bytesUsedByPagesArray(1) + bytesUsedByIntArray(initialLength));
        this.intPages = new int[1][initialLength];
        this.capacity = initialLength;
    }

    private int blockIndex() {
        return countAll ? 0 : channels.get(0);
    }

    @Override
    public int intermediateBlockCount() {
        return intermediateStateDesc().size();
    }

    @Override
    public AddInput prepareProcessRawInputPage(SeenGroupIds seenGroupIds, Page page) {
        Block valuesBlock = page.getBlock(blockIndex());
        if (countAll == false) {
            if (valuesBlock.areAllValuesNull()) {
                return null;
            }
            Vector valuesVector = valuesBlock.asVector();
            if (valuesVector == null) {
                return new AddInput() {
                    @Override
                    public void add(int positionOffset, IntArrayBlock groupIds) {
                        addRawInput(positionOffset, groupIds, valuesBlock);
                    }

                    @Override
                    public void add(int positionOffset, IntBigArrayBlock groupIds) {
                        addRawInput(positionOffset, groupIds, valuesBlock);
                    }

                    @Override
                    public void add(int positionOffset, IntVector groupIds) {
                        addRawInput(positionOffset, groupIds, valuesBlock);
                    }

                    @Override
                    public void close() {}
                };
            }
        }
        return new AddInput() {
            @Override
            public void add(int positionOffset, IntArrayBlock groupIds) {
                addRawInput(groupIds);
            }

            @Override
            public void add(int positionOffset, IntBigArrayBlock groupIds) {
                addRawInput(groupIds);
            }

            @Override
            public void add(int positionOffset, IntVector groupIds) {
                addRawInput(groupIds);
            }

            @Override
            public void close() {}
        };
    }

    private void addRawInput(int positionOffset, IntVector groups, Block values) {
        int position = positionOffset;
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++, position++) {
            if (values.isNull(position)) {
                continue;
            }
            int groupId = groups.getInt(groupPosition);
            accumulateCount(groupId, getBlockValueCountAtPosition(values, position));
        }
    }

    /**
     * Returns the number of values at a given position in a block
     * @param values block
     * @param position position to get the number of values
     * @return
     */
    protected int getBlockValueCountAtPosition(Block values, int position) {
        return values.getValueCount(position);
    }

    private void addRawInput(int positionOffset, IntArrayBlock groups, Block values) {
        int position = positionOffset;
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++, position++) {
            if (groups.isNull(groupPosition) || values.isNull(position)) {
                continue;
            }
            int groupStart = groups.getFirstValueIndex(groupPosition);
            int groupEnd = groupStart + groups.getValueCount(groupPosition);
            for (int g = groupStart; g < groupEnd; g++) {
                int groupId = groups.getInt(g);
                accumulateCount(groupId, getBlockValueCountAtPosition(values, position));
            }
        }
    }

    private void addRawInput(int positionOffset, IntBigArrayBlock groups, Block values) {
        int position = positionOffset;
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++, position++) {
            if (groups.isNull(groupPosition) || values.isNull(position)) {
                continue;
            }
            int groupStart = groups.getFirstValueIndex(groupPosition);
            int groupEnd = groupStart + groups.getValueCount(groupPosition);
            for (int g = groupStart; g < groupEnd; g++) {
                int groupId = groups.getInt(g);
                accumulateCount(groupId, getBlockValueCountAtPosition(values, position));
            }
        }
    }

    /**
     * This method is called for count all.
     */
    private void addRawInput(IntVector groups) {
        if (groups.isConstant()) {
            accumulateCount(groups.getInt(0), groups.getPositionCount());
        } else {
            for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
                int groupId = groups.getInt(groupPosition);
                accumulateCount(groupId, 1);
            }
        }
    }

    /**
     * This method is called for count all.
     */
    private void addRawInput(IntArrayBlock groups) {
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            if (groups.isNull(groupPosition)) {
                continue;
            }
            int groupStart = groups.getFirstValueIndex(groupPosition);
            int groupEnd = groupStart + groups.getValueCount(groupPosition);
            for (int g = groupStart; g < groupEnd; g++) {
                int groupId = groups.getInt(g);
                accumulateCount(groupId, 1);
            }
        }
    }

    /**
     * This method is called for count all.
     */
    private void addRawInput(IntBigArrayBlock groups) {
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            if (groups.isNull(groupPosition)) {
                continue;
            }
            int groupStart = groups.getFirstValueIndex(groupPosition);
            int groupEnd = groupStart + groups.getValueCount(groupPosition);
            for (int g = groupStart; g < groupEnd; g++) {
                int groupId = groups.getInt(g);
                accumulateCount(groupId, 1);
            }
        }
    }

    @Override
    public void selectedMayContainUnseenGroups(SeenGroupIds seenGroupIds) {
        // no need to track seen groups, as count returns 0 for groups without values.
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntArrayBlock groups, Page page) {
        assert channels.size() == intermediateBlockCount();
        assert page.getBlockCount() >= blockIndex() + intermediateStateDesc().size();
        LongVector count = page.<LongBlock>getBlock(channels.get(0)).asVector();
        BooleanVector seen = page.<BooleanBlock>getBlock(channels.get(1)).asVector();
        assert count.getPositionCount() == seen.getPositionCount();
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            if (groups.isNull(groupPosition)) {
                continue;
            }
            int groupStart = groups.getFirstValueIndex(groupPosition);
            int groupEnd = groupStart + groups.getValueCount(groupPosition);
            for (int g = groupStart; g < groupEnd; g++) {
                int groupId = groups.getInt(g);
                accumulateCount(groupId, count.getLong(groupPosition + positionOffset));
            }
        }
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntBigArrayBlock groups, Page page) {
        assert channels.size() == intermediateBlockCount();
        assert page.getBlockCount() >= blockIndex() + intermediateStateDesc().size();
        LongVector count = page.<LongBlock>getBlock(channels.get(0)).asVector();
        BooleanVector seen = page.<BooleanBlock>getBlock(channels.get(1)).asVector();
        assert count.getPositionCount() == seen.getPositionCount();
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            if (groups.isNull(groupPosition)) {
                continue;
            }
            int groupStart = groups.getFirstValueIndex(groupPosition);
            int groupEnd = groupStart + groups.getValueCount(groupPosition);
            for (int g = groupStart; g < groupEnd; g++) {
                int groupId = groups.getInt(g);
                accumulateCount(groupId, count.getLong(groupPosition + positionOffset));
            }
        }
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
        assert channels.size() == intermediateBlockCount();
        assert page.getBlockCount() >= blockIndex() + intermediateStateDesc().size();
        LongVector count = page.<LongBlock>getBlock(channels.get(0)).asVector();
        BooleanVector seen = page.<BooleanBlock>getBlock(channels.get(1)).asVector();
        assert count.getPositionCount() == seen.getPositionCount();
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            accumulateCount(groups.getInt(groupPosition), count.getLong(groupPosition + positionOffset));
        }
    }

    @Override
    public GroupingAggregatorFunction.PreparedForEvaluation prepareEvaluateIntermediate(
        IntVector selected,
        GroupingAggregatorEvaluationContext ctx
    ) {
        return this::evaluateIntermediate;
    }

    private void evaluateIntermediate(Block[] blocks, int offset, IntVector selectedInPage) {
        evaluateFinal(blocks, offset, selectedInPage);
        // Unlike other aggregations, we return 0 for groups without values instead of null.
        // Therefore, we can always return true for seen, and do not need to track seen groups.
        blocks[offset + 1] = driverContext.blockFactory().newConstantBooleanBlockWith(true, selectedInPage.getPositionCount());
    }

    @Override
    public GroupingAggregatorFunction.PreparedForEvaluation prepareEvaluateFinal(
        IntVector selected,
        GroupingAggregatorEvaluationContext ctx
    ) {
        return this::evaluateFinal;
    }

    private void accumulateCount(int groupId, long value) {
        assert (longPages == null) != (intPages == null);
        if (longPages != null) {
            accumulateLongCount(groupId, value);
            return;
        }
        if (groupId >= capacity) {
            growIntCounts(groupId);
        }
        final int[] intPage = intPages[groupId >>> INT_PAGE_SHIFT];
        final int indexInPage = groupId & INT_PAGE_MASK;
        final long total = intPage[indexInPage] + value;
        final int intTotal = (int) total;
        if (total == intTotal) {
            intPage[indexInPage] = intTotal;
        } else {
            migrateToLongCounts();
            accumulateLongCount(groupId, value);
        }
    }

    private void accumulateLongCount(int groupId, long value) {
        if (groupId >= capacity) {
            growLongCounts(groupId);
        }
        longPages[groupId >>> LONG_PAGE_SHIFT][groupId & LONG_PAGE_MASK] += value;
    }

    private void growIntCounts(int groupId) {
        if (capacity < INTS_PER_PAGE) {
            reserveBytes(bytesUsedByIntArray(INTS_PER_PAGE));
            intPages[0] = Arrays.copyOf(intPages[0], INTS_PER_PAGE);
            releaseBytes(bytesUsedByIntArray(capacity));
            capacity = INTS_PER_PAGE;
            if (capacity > groupId) {
                return;
            }
        }
        final int pageIndex = groupId >>> INT_PAGE_SHIFT;
        int oldLength = intPages.length;
        if (pageIndex >= oldLength) {
            final int newLength = ArrayUtil.oversize(pageIndex + 1, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
            reserveBytes(bytesUsedByPagesArray(newLength));
            intPages = Arrays.copyOf(intPages, newLength);
            releaseBytes(bytesUsedByPagesArray(oldLength));
        }
        if (capacity == groupId) {
            reserveBytes(bytesUsedByIntArray(INTS_PER_PAGE));
            intPages[pageIndex] = new int[INTS_PER_PAGE];
            capacity += INTS_PER_PAGE;
            return;
        }
        int lastPage = capacity >>> INT_PAGE_SHIFT;
        for (int i = lastPage; i <= pageIndex; i++) {
            assert intPages[i] == null;
            reserveBytes(bytesUsedByIntArray(INTS_PER_PAGE));
            intPages[i] = new int[INTS_PER_PAGE];
        }
        capacity = (pageIndex + 1) << INT_PAGE_SHIFT;
    }

    private void growLongCounts(int groupId) {
        if (capacity < LONGS_PER_PAGE) {
            reserveBytes(bytesUsedByLongArray(LONGS_PER_PAGE));
            longPages[0] = Arrays.copyOf(longPages[0], LONGS_PER_PAGE);
            releaseBytes(bytesUsedByLongArray(capacity));
            capacity = LONGS_PER_PAGE;
            if (capacity > groupId) {
                return;
            }
        }
        final int pageIndex = groupId >>> LONG_PAGE_SHIFT;
        int oldLength = longPages.length;
        if (pageIndex >= oldLength) {
            final int newLength = ArrayUtil.oversize(pageIndex + 1, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
            reserveBytes(bytesUsedByPagesArray(newLength));
            longPages = Arrays.copyOf(longPages, newLength);
            releaseBytes(bytesUsedByPagesArray(oldLength));
        }
        if (capacity == groupId) {
            reserveBytes(bytesUsedByLongArray(LONGS_PER_PAGE));
            longPages[pageIndex] = new long[LONGS_PER_PAGE];
            capacity += LONGS_PER_PAGE;
            return;
        }
        int lastPage = capacity >>> LONG_PAGE_SHIFT;
        for (int i = lastPage; i <= pageIndex; i++) {
            assert longPages[i] == null;
            reserveBytes(bytesUsedByLongArray(LONGS_PER_PAGE));
            longPages[i] = new long[LONGS_PER_PAGE];
        }
        capacity = (pageIndex + 1) << LONG_PAGE_SHIFT;
    }

    private void migrateToLongCounts() {
        assert longPages == null;
        final int numPages = Math.ceilDiv(capacity, LONGS_PER_PAGE);
        final int longsPerPage = Math.min(capacity, LONGS_PER_PAGE);
        reserveBytes(bytesUsedByPagesArray(numPages) + numPages * bytesUsedByLongArray(longsPerPage));
        longPages = new long[numPages][];
        for (int p = 0; p < numPages; p++) {
            longPages[p] = new long[longsPerPage];
        }
        for (int i = 0; i < capacity; i++) {
            longPages[i >>> LONG_PAGE_SHIFT][i & LONG_PAGE_MASK] = intPages[i >>> INT_PAGE_SHIFT][i & INT_PAGE_MASK];
        }
        long bytesUsedByInts = bytesUsedByPagesArray(intPages.length) + Math.ceilDiv(capacity, INTS_PER_PAGE) * bytesUsedByIntArray(
            Math.min(capacity, INTS_PER_PAGE)
        );
        intPages = null;
        releaseBytes(bytesUsedByInts);
    }

    private void reserveBytes(long bytes) {
        breaker.addEstimateBytesAndMaybeBreak(bytes, "CountGroupingAggregatorFunction");
        usedBytes += bytes;
    }

    private void releaseBytes(long bytes) {
        breaker.addWithoutBreaking(-bytes);
        usedBytes -= bytes;
    }

    private void evaluateFinal(Block[] blocks, int offset, IntVector selectedInPage) {
        try (LongVector.Builder builder = driverContext.blockFactory().newLongVectorFixedBuilder(selectedInPage.getPositionCount())) {
            final int positionCount = selectedInPage.getPositionCount();
            final int[][] pages = intPages;
            if (pages != null) {
                final int capacity = this.capacity;
                for (int i = 0; i < positionCount; i++) {
                    final int groupId = selectedInPage.getInt(i);
                    builder.appendLong(groupId < capacity ? pages[groupId >>> INT_PAGE_SHIFT][groupId & INT_PAGE_MASK] : 0L);
                }
            } else {
                final long[][] longs = longPages;
                final int capacity = this.capacity;
                for (int i = 0; i < positionCount; i++) {
                    final int groupId = selectedInPage.getInt(i);
                    builder.appendLong(groupId < capacity ? longs[groupId >>> LONG_PAGE_SHIFT][groupId & LONG_PAGE_MASK] : 0L);
                }
            }
            blocks[offset] = builder.build().asBlock();
        }
    }

    @Override
    public IntVector selectTopN(IntVector selected, int limit, boolean asc) {
        int positionCount = selected.getPositionCount();
        if (positionCount <= limit) {
            selected.incRef();
            return selected;
        }
        record GroupIdAndCount(int groupId, long count) {

        }
        long usedBytes = ((long) RamUsageEstimator.NUM_BYTES_OBJECT_REF * 2L + Long.BYTES + Integer.BYTES + Integer.BYTES) * limit;
        driverContext.blockFactory().adjustBreaker(usedBytes);
        try {
            final PriorityQueue<GroupIdAndCount> pq;
            if (asc) {
                pq = new PriorityQueue<>(limit) {
                    @Override
                    protected boolean lessThan(GroupIdAndCount a, GroupIdAndCount b) {
                        return a.count > b.count;
                    }
                };
            } else {
                pq = new PriorityQueue<>(limit) {
                    @Override
                    protected boolean lessThan(GroupIdAndCount a, GroupIdAndCount b) {
                        return a.count < b.count;
                    }
                };
            }
            final int[][] pages = intPages;
            if (pages != null) {
                final int capacity = this.capacity;
                for (int i = 0; i < positionCount; i++) {
                    final int groupId = selected.getInt(i);
                    final long count = groupId < capacity ? pages[groupId >>> INT_PAGE_SHIFT][groupId & INT_PAGE_MASK] : 0L;
                    pq.insertWithOverflow(new GroupIdAndCount(groupId, count));
                }
            } else {
                final long[][] longs = longPages;
                final int capacity = this.capacity;
                for (int i = 0; i < positionCount; i++) {
                    final int groupId = selected.getInt(i);
                    final long count = groupId < capacity ? longs[groupId >>> LONG_PAGE_SHIFT][groupId & LONG_PAGE_MASK] : 0L;
                    pq.insertWithOverflow(new GroupIdAndCount(groupId, count));
                }
            }
            final int[] topGroupIds = new int[pq.size()];
            int idx = 0;
            for (GroupIdAndCount groupIdAndCount : pq) {
                topGroupIds[idx++] = groupIdAndCount.groupId;
            }
            // sort the new selected
            Arrays.sort(topGroupIds);
            IntVector vector = driverContext.blockFactory().newIntArrayVector(topGroupIds, topGroupIds.length, usedBytes);
            usedBytes = 0;
            return vector;
        } finally {
            if (usedBytes > 0) {
                driverContext.blockFactory().adjustBreaker(-usedBytes);
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("[");
        sb.append("channels=").append(channels);
        sb.append("]");
        return sb.toString();
    }

    @Override
    public void close() {
        intPages = null;
        longPages = null;
        releaseBytes(usedBytes);
    }

    static long bytesUsedByPagesArray(int arrayLength) {
        return RamUsageEstimator.alignObjectSize(
            (long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) RamUsageEstimator.NUM_BYTES_OBJECT_REF * arrayLength
        );
    }

    static long bytesUsedByIntArray(int arrayLength) {
        return RamUsageEstimator.alignObjectSize((long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) Integer.BYTES * arrayLength);
    }

    static long bytesUsedByLongArray(int arrayLength) {
        return RamUsageEstimator.alignObjectSize((long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) Long.BYTES * arrayLength);
    }
}
