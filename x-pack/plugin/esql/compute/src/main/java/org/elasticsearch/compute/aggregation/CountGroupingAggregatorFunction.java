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
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.PartitionedHashTable;
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

    private LongArray counts;
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
        this.counts = driverContext.bigArrays().newLongArray(1024);
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
        if (groupId < counts.size()) {
            counts.increment(groupId, value);
        } else {
            counts = driverContext.bigArrays().grow(counts, groupId + 1);
            counts.set(groupId, value);
        }
    }

    private void evaluateFinal(Block[] blocks, int offset, IntVector selectedInPage) {
        try (LongVector.Builder builder = driverContext.blockFactory().newLongVectorFixedBuilder(selectedInPage.getPositionCount())) {
            final int positionCount = selectedInPage.getPositionCount();
            for (int i = 0; i < positionCount; i++) {
                final int si = selectedInPage.getInt(i);
                if (si < counts.size()) {
                    builder.appendLong(counts.get(si));
                } else {
                    builder.appendLong(0L);
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
            for (int i = 0; i < positionCount; i++) {
                final int groupId = selected.getInt(i);
                final long count = groupId < counts.size() ? counts.get(groupId) : 0L;
                pq.insertWithOverflow(new GroupIdAndCount(groupId, count));
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

    public void clear() {
        counts.clear();
    }

    @Override
    public void close() {
        counts.close();
    }

    @Override
    public boolean supportPartitioning() {
        return true;
    }

    /**
     * Per-partition count values in partition order. Values start as ints and are migrated to longs by the
     * partitioner if any count exceeds the int range.
     */
    record CountPartitionedGroupingState(CircuitBreaker breaker, int[][] intValues, long[][] longValues)
        implements
            PartitionedGroupingState {

        @Override
        public void releasePartition(int partition) {
            long bytes = 0;
            if (longValues != null && longValues[partition] != null) {
                bytes += (long) longValues[partition].length * Long.BYTES;
                longValues[partition] = null;
            }
            if (intValues != null && intValues[partition] != null) {
                bytes += (long) intValues[partition].length * Integer.BYTES;
                intValues[partition] = null;
            }
            breaker.addWithoutBreaking(-bytes);
        }

        @Override
        public void close() {
            long usedBytes = CountGroupingStatePartitioner.usedBytes(intValues) + CountGroupingStatePartitioner.usedBytes(longValues);
            if (intValues != null) {
                Arrays.fill(intValues, null);
            }
            if (longValues != null) {
                Arrays.fill(longValues, null);
            }
            breaker.addWithoutBreaking(-usedBytes);
        }
    }

    static final class CountGroupingStatePartitioner implements GroupingStatePartitioner {
        private static final String BREAKER_LABEL = "CountGroupingStatePartitioner";

        private final CircuitBreaker breaker;
        private final LongArray src;
        private long[] buffer = new long[0]; // TODO: share the buffer
        private long[][] longValues;
        private int[][] intValues;

        CountGroupingStatePartitioner(CircuitBreaker breaker, LongArray src, int estimateSizePerPartition) {
            this.breaker = breaker;
            this.src = src;
            final int cap = ArrayUtil.oversize(
                Math.max(PartitionedHashTable.PARTITION_WRITE_BATCH, estimateSizePerPartition),
                Integer.BYTES
            );
            breaker.addEstimateBytesAndMaybeBreak((long) PartitionedHashTable.NUM_PARTITIONS * cap * Integer.BYTES, BREAKER_LABEL);
            this.intValues = new int[PartitionedHashTable.NUM_PARTITIONS][];
            for (int p = 0; p < PartitionedHashTable.NUM_PARTITIONS; p++) {
                this.intValues[p] = new int[cap];
            }
        }

        @Override
        public void split(int firstId, short[] shiftedIds, int batchSize, int[] partitionCounts, int[] partitionOffsets) {
            if (buffer.length < batchSize) {
                buffer = new long[ArrayUtil.oversize(batchSize, Long.BYTES)];
            }
            final int readLen = (int) Math.clamp(src.size() - firstId, 0L, batchSize);
            if (readLen > 0) {
                src.bulkGet(firstId, buffer, 0, readLen);
            }
            if (readLen < batchSize) {
                Arrays.fill(buffer, readLen, batchSize, 0L);
            }
            if (intValues != null) {
                if (fitsInts(buffer, batchSize)) {
                    splitInts(shiftedIds, partitionCounts, partitionOffsets);
                    return;
                }
                migrateToLongs();
            }
            splitLongs(shiftedIds, partitionCounts, partitionOffsets);
        }

        private static boolean fitsInts(long[] buffer, int batchSize) {
            for (int i = 0; i < batchSize; i++) {
                if (buffer[i] > Integer.MAX_VALUE) {
                    return false;
                }
            }
            return true;
        }

        private void splitInts(short[] shiftedIds, int[] partitionCounts, int[] partitionOffsets) {
            final int[][] values = intValues;
            for (int p = 0; p < PartitionedHashTable.NUM_PARTITIONS; p++) {
                final int c = partitionCounts[p];
                if (c == 0) {
                    continue;
                }
                final int dst = partitionOffsets[p];
                int[] sub = values[p];
                final int currentLen = sub.length;
                if (currentLen < dst + c) {
                    final int newLength = ArrayUtil.oversize(dst + c, Integer.BYTES);
                    breaker.addEstimateBytesAndMaybeBreak((long) (newLength) * Integer.BYTES, BREAKER_LABEL);
                    sub = values[p] = Arrays.copyOf(sub, newLength);
                    breaker.addWithoutBreaking((long) -currentLen * Integer.BYTES);
                }
                final int base = p * PartitionedHashTable.PARTITION_WRITE_BATCH;
                for (int i = 0; i < c; i++) {
                    sub[dst + i] = (int) buffer[shiftedIds[base + i] & 0xFFFF];
                }
            }
        }

        private void splitLongs(short[] shiftedIds, int[] partitionCounts, int[] partitionOffsets) {
            final long[][] values = longValues;
            for (int p = 0; p < PartitionedHashTable.NUM_PARTITIONS; p++) {
                final int c = partitionCounts[p];
                if (c == 0) {
                    continue;
                }
                final int dst = partitionOffsets[p];
                long[] sub = values[p];
                if (sub.length < dst + c) {
                    final int newLength = ArrayUtil.oversize(dst + c, Long.BYTES);
                    breaker.addEstimateBytesAndMaybeBreak((long) (newLength - sub.length) * Long.BYTES, BREAKER_LABEL);
                    sub = values[p] = Arrays.copyOf(sub, newLength);
                }
                final int base = p * PartitionedHashTable.PARTITION_WRITE_BATCH;
                for (int i = 0; i < c; i++) {
                    sub[dst + i] = buffer[shiftedIds[base + i] & 0xFFFF];
                }
            }
        }

        private void migrateToLongs() {
            this.longValues = new long[PartitionedHashTable.NUM_PARTITIONS][];
            for (int p = 0; p < PartitionedHashTable.NUM_PARTITIONS; p++) {
                final int[] ints = intValues[p];
                breaker.addEstimateBytesAndMaybeBreak((long) ints.length * Long.BYTES, BREAKER_LABEL);
                final long[] longs = new long[ints.length];
                for (int i = 0; i < ints.length; i++) {
                    longs[i] = ints[i];
                }
                longValues[p] = longs;
                intValues[p] = null;
                breaker.addWithoutBreaking(-(long) longs.length * Integer.BYTES);
            }
            this.intValues = null;
        }

        @Override
        public PartitionedGroupingState finish() {
            var state = new CountPartitionedGroupingState(breaker, intValues, longValues);
            intValues = null;
            longValues = null;
            return state;
        }

        static long usedBytes(int[][] values) {
            if (values == null) {
                return 0L;
            }
            long usedBytes = 0;
            for (var v : values) {
                if (v != null) {
                    usedBytes += (long) v.length * Integer.BYTES;
                }
            }
            return usedBytes;
        }

        static long usedBytes(long[][] values) {
            if (values == null) {
                return 0;
            }
            long usedBytes = 0;
            for (var v : values) {
                if (v != null) {
                    usedBytes += (long) v.length * Long.BYTES;
                }
            }
            return usedBytes;
        }

        @Override
        public void close() {
            long bytes = usedBytes(intValues) + usedBytes(longValues);
            longValues = null;
            intValues = null;
            breaker.addWithoutBreaking(-bytes);
        }
    }

    @Override
    public GroupingStatePartitioner splitPartition(CircuitBreaker breaker, int estimateSizePerPartition) {
        return new CountGroupingStatePartitioner(breaker, counts, estimateSizePerPartition);
    }

    @Override
    public void combinePartition(PartitionedGroupingState partitioned, int partition, int[] mergedIds, int length, int maxGroupId) {
        if (length == 0) {
            return;
        }
        final CountPartitionedGroupingState state = (CountPartitionedGroupingState) partitioned;
        if (counts.size() <= maxGroupId) {
            counts = driverContext.bigArrays().grow(counts, maxGroupId);
        }
        final int[] ints = state.intValues != null ? state.intValues[partition] : null;
        final long[] longs = state.longValues != null ? state.longValues[partition] : null;
        assert ints != null || longs != null : "partition already released";
        if (ints != null) {
            for (int i = 0; i < length; i++) {
                counts.increment(mergedIds[i], ints[i]);
            }
        } else {
            for (int i = 0; i < length; i++) {
                counts.increment(mergedIds[i], longs[i]);
            }
        }
    }
}
