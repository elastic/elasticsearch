/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongArrayVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

import java.util.List;

/**
 * The sum of {@code long}s as a {@code double} for every group, which can't overflow. It is {@code SUM(TO_DOUBLE(x))}
 * for a {@code long} {@code x}, the compensated sum of the values as {@code double}s, without converting every value
 * in an extra pass. The state and the intermediate state are the ones of {@link SumDoubleAggregator}, so the partial
 * sums are merged, and partitioned, like the ones of a sum of {@code double}s.
 * <p>
 *     This is written by hand, and not generated from an annotated class, for the loops over the group ids that come
 *     as a vector, which is how they come unless a grouping key is null or multivalued. A generated aggregator adds
 *     the values to its state one by one, and the state makes room for the group and checks for values that aren't
 *     finite every time. Here room is made once for all the groups of the vector, and a {@code long} is always finite.
 * </p>
 * <p>
 *     When the group id is the same for a whole page, which is how a grouping key that is constant in the page comes,
 *     {@link #addConstantGroup the values are added on their own first}, like {@link SumAsDoubleLongAggregatorFunction}
 *     does without groups, and their exact sum goes into the compensated sum of the group. That is much faster than
 *     adding them one by one, and at least as accurate: no value above {@code 2^53} is rounded before it is added.
 * </p>
 */
public final class SumAsDoubleLongGroupingAggregatorFunction implements GroupingAggregatorFunction {
    private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
        new IntermediateStateDesc("value", ElementType.DOUBLE),
        new IntermediateStateDesc("delta", ElementType.DOUBLE),
        new IntermediateStateDesc("seen", ElementType.BOOLEAN)
    );

    private static final long LOW_MASK = 0xFFFF_FFFFL;

    public static List<IntermediateStateDesc> intermediateStateDesc() {
        return INTERMEDIATE_STATE_DESC;
    }

    private final SumDoubleAggregator.GroupingSumState state;
    private final List<Integer> channels;
    private final DriverContext driverContext;

    SumAsDoubleLongGroupingAggregatorFunction(List<Integer> channels, DriverContext driverContext) {
        this.channels = channels;
        this.state = SumDoubleAggregator.initGrouping(driverContext.bigArrays());
        this.driverContext = driverContext;
    }

    @Override
    public int intermediateBlockCount() {
        return INTERMEDIATE_STATE_DESC.size();
    }

    @Override
    public AddInput prepareProcessRawInputPage(SeenGroupIds seenGroupIds, Page page) {
        LongBlock block = page.getBlock(channels.get(0));
        if (block.areAllValuesNull()) {
            /*
             * All values are null so we can skip processing this block. But we
             * still need to track that some groups may not have been seen
             * so that they are initialized to null when we read their values.
             */
            state.enableGroupIdTracking(seenGroupIds);
            return null;
        }
        LongVector vector = block.asVector();
        if (vector == null) {
            if (block.mayHaveNulls()) {
                /*
                 * Some values in the block are null so some group ids may not
                 * be seen. We need to track which ones so we can initialize
                 * them to null when we read their values.
                 */
                state.enableGroupIdTracking(seenGroupIds);
            }
            return new AddInput() {
                @Override
                public void add(int positionOffset, IntArrayBlock groupIds) {
                    addRawInput(positionOffset, groupIds, block);
                }

                @Override
                public void add(int positionOffset, IntBigArrayBlock groupIds) {
                    addRawInput(positionOffset, groupIds, block);
                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {
                    addRawInput(positionOffset, groupIds, block);
                }

                @Override
                public void close() {}
            };
        }
        return new AddInput() {
            @Override
            public void add(int positionOffset, IntArrayBlock groupIds) {
                addRawInput(positionOffset, groupIds, vector);
            }

            @Override
            public void add(int positionOffset, IntBigArrayBlock groupIds) {
                addRawInput(positionOffset, groupIds, vector);
            }

            @Override
            public void add(int positionOffset, IntVector groupIds) {
                addRawInput(positionOffset, groupIds, vector);
            }

            @Override
            public void close() {}
        };
    }

    private void addRawInput(int positionOffset, IntVector groups, LongVector vector) {
        int positions = groups.getPositionCount();
        if (positions == 0) {
            return;
        }
        state.ensureCapacity(groups.max());
        if (vector.getClass() == LongArrayVector.class) {
            LongArrayVector arrayVector = (LongArrayVector) vector;
            if (groups.isConstant()) {
                addConstantGroup(positionOffset, positions, groups.getInt(0), arrayVector);
            } else {
                addRawArrayVector(positionOffset, groups, arrayVector);
            }
            return;
        }
        DoubleArray values = state.values;
        DoubleArray deltas = state.deltas;
        for (int p = 0; p < positions; p++) {
            add(values, deltas, groups.getInt(p), vector.getLong(positionOffset + p));
        }
    }

    /**
     * The hot loop, the same as the one for every other vector of values but on the kind that comes out of loading a
     * field, so that reading a value is reading an array.
     */
    private void addRawArrayVector(int positionOffset, IntVector groups, LongArrayVector vector) {
        int positions = groups.getPositionCount();
        DoubleArray values = state.values;
        DoubleArray deltas = state.deltas;
        for (int p = 0; p < positions; p++) {
            add(values, deltas, groups.getInt(p), vector.getLong(positionOffset + p));
        }
    }

    /**
     * Adds the values of a page that all belong to one group. This is the loop of
     * {@link SumAsDoubleLongAggregatorFunction}: the low and the high 32 bits of the values are summed apart, each in a
     * {@code long} that they can't overflow, which are two plain sums that the jvm vectorizes. Then the sum of the
     * page, which is exact, is added to the compensated sum of the group.
     */
    private void addConstantGroup(int positionOffset, int positions, int groupId, LongArrayVector vector) {
        int end = positionOffset + positions;
        long low = 0;
        long high = 0;
        for (int p = positionOffset; p < end; p++) {
            long v = vector.getLong(p);
            low += v & LOW_MASK;
            high += v >> 32;
        }
        // The sum is high * 2^32 + low. Move what low has above 32 bits into high, then it is mid * 2^32 + the low 32 bits.
        long mid = high + (low >>> 32);
        // It doesn't fit in a double, but these three parts of it do, without rounding: each has 32 bits.
        state.add((mid >> 32) * 0x1p64, groupId);
        state.add((mid & LOW_MASK) * 0x1p32, groupId);
        state.add(low & LOW_MASK, groupId);
    }

    private void addRawInput(int positionOffset, IntVector groups, LongBlock block) {
        int positions = groups.getPositionCount();
        if (positions == 0) {
            return;
        }
        state.ensureCapacity(groups.max());
        DoubleArray values = state.values;
        DoubleArray deltas = state.deltas;
        for (int p = 0; p < positions; p++) {
            int valuesPosition = positionOffset + p;
            if (block.isNull(valuesPosition)) {
                continue;
            }
            int groupId = groups.getInt(p);
            int start = block.getFirstValueIndex(valuesPosition);
            int end = start + block.getValueCount(valuesPosition);
            for (int i = start; i < end; i++) {
                add(values, deltas, groupId, block.getLong(i));
            }
        }
    }

    /**
     * Adds a value to the compensated sum of a group that the state has room for. This is what the state does when it
     * {@link SumDoubleAggregator.GroupingSumState#add adds} a value, without the parts that a {@code long} never needs:
     * it is finite, and so is every sum of them.
     */
    private void add(DoubleArray values, DoubleArray deltas, int groupId, long v) {
        state.trackGroupId(groupId);
        double value = values.get(groupId);
        assert Double.isFinite(value) : value;
        double correctedSum = v + deltas.get(groupId);
        double updatedValue = value + correctedSum;
        deltas.set(groupId, correctedSum - (updatedValue - value));
        values.set(groupId, updatedValue);
    }

    /**
     * Group ids come as a block when a grouping key is null or multivalued, which is rare enough to go through the state.
     */
    private void addRawInput(int positionOffset, IntBlock groups, LongVector vector) {
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            if (groups.isNull(groupPosition)) {
                continue;
            }
            double v = vector.getLong(groupPosition + positionOffset);
            int groupStart = groups.getFirstValueIndex(groupPosition);
            int groupEnd = groupStart + groups.getValueCount(groupPosition);
            for (int g = groupStart; g < groupEnd; g++) {
                state.add(v, groups.getInt(g));
            }
        }
    }

    private void addRawInput(int positionOffset, IntBlock groups, LongBlock block) {
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            if (groups.isNull(groupPosition)) {
                continue;
            }
            int valuesPosition = groupPosition + positionOffset;
            if (block.isNull(valuesPosition)) {
                continue;
            }
            int groupStart = groups.getFirstValueIndex(groupPosition);
            int groupEnd = groupStart + groups.getValueCount(groupPosition);
            int start = block.getFirstValueIndex(valuesPosition);
            int end = start + block.getValueCount(valuesPosition);
            for (int g = groupStart; g < groupEnd; g++) {
                int groupId = groups.getInt(g);
                for (int i = start; i < end; i++) {
                    state.add((double) block.getLong(i), groupId);
                }
            }
        }
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntArrayBlock groups, Page page) {
        addIntermediateInput(positionOffset, (IntBlock) groups, page);
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntBigArrayBlock groups, Page page) {
        addIntermediateInput(positionOffset, (IntBlock) groups, page);
    }

    private void addIntermediateInput(int positionOffset, IntBlock groups, Page page) {
        IntermediateInput input = intermediateInput(page);
        if (input == null) {
            return;
        }
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            if (groups.isNull(groupPosition)) {
                continue;
            }
            int valuesPosition = groupPosition + positionOffset;
            int groupStart = groups.getFirstValueIndex(groupPosition);
            int groupEnd = groupStart + groups.getValueCount(groupPosition);
            for (int g = groupStart; g < groupEnd; g++) {
                input.addTo(state, groups.getInt(g), valuesPosition);
            }
        }
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
        IntermediateInput input = intermediateInput(page);
        if (input == null) {
            return;
        }
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            input.addTo(state, groups.getInt(groupPosition), groupPosition + positionOffset);
        }
    }

    /**
     * The partial sums of a page of intermediate input, {@code null} if it has none.
     */
    private IntermediateInput intermediateInput(Page page) {
        assert channels.size() == intermediateBlockCount();
        Block valueUncast = page.getBlock(channels.get(0));
        Block deltaUncast = page.getBlock(channels.get(1));
        Block seenUncast = page.getBlock(channels.get(2));
        if (valueUncast.areAllValuesNull() || deltaUncast.areAllValuesNull() || seenUncast.areAllValuesNull()) {
            return null;
        }
        DoubleVector value = ((DoubleBlock) valueUncast).asVector();
        DoubleVector delta = ((DoubleBlock) deltaUncast).asVector();
        BooleanVector seen = ((BooleanBlock) seenUncast).asVector();
        assert value.getPositionCount() == delta.getPositionCount() && value.getPositionCount() == seen.getPositionCount();
        return new IntermediateInput(value, delta, seen);
    }

    private record IntermediateInput(DoubleVector value, DoubleVector delta, BooleanVector seen) {
        void addTo(SumDoubleAggregator.GroupingSumState state, int groupId, int position) {
            SumDoubleAggregator.combineIntermediate(
                state,
                groupId,
                value.getDouble(position),
                delta.getDouble(position),
                seen.getBoolean(position)
            );
        }
    }

    @Override
    public AddInput prepareProcessIntermediateInputPage(SeenGroupIds seenGroupIds, Page page) {
        BooleanVector seen = ((BooleanBlock) page.getBlock(channels.get(2))).asVector();
        if (seen == null || seen.isConstant() == false || seen.getBoolean(0) == false) {
            state.enableGroupIdTracking(seenGroupIds);
        }
        return new IntermediateAddInput(this, seenGroupIds, page);
    }

    @Override
    public void selectedMayContainUnseenGroups(SeenGroupIds seenGroupIds) {
        state.enableGroupIdTracking(seenGroupIds);
    }

    @Override
    public PreparedForEvaluation prepareEvaluateIntermediate(IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        return (blocks, offset, selectedInPage) -> state.toIntermediate(blocks, offset, selectedInPage, driverContext);
    }

    @Override
    public PreparedForEvaluation prepareEvaluateFinal(IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        return (blocks, offset, selectedInPage) -> blocks[offset] = SumDoubleAggregator.evaluateFinal(state, selectedInPage, ctx);
    }

    @Override
    public void maybeEnsureCapacity(int size) {
        state.ensureCapacity(size);
    }

    @Override
    public boolean supportPartitioning() {
        return true;
    }

    @Override
    public PartitionSplitter createPartitioningSplitter(CircuitBreaker breaker) {
        return state.createPartitioningSplitter(breaker);
    }

    @Override
    public void combinePartition(PartitionedState source, int partition, boolean appendOnly, int[] dstIds, int length) {
        SumDoubleAggregator.combinePartition(state, source, partition, appendOnly, dstIds, length);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName()).append("[");
        sb.append("channels=").append(channels);
        sb.append("]");
        return sb.toString();
    }

    @Override
    public void close() {
        state.close();
    }
}
