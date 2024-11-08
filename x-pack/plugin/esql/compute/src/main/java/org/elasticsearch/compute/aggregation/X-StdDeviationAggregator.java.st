/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

/**
 * A standard deviation aggregation definition for $type$.
 * This class is generated. Edit `X-StdDeviationAggregator.java.st` instead.
 */
@Aggregator(
    {
        @IntermediateState(name = "mean", type = "DOUBLE"),
        @IntermediateState(name = "m2", type = "DOUBLE"),
        @IntermediateState(name = "count", type = "LONG") }
)
@GroupingAggregator
public class StdDeviation$Type$Aggregator {

    public static StdDeviation$Type$State initSingle() {
        return new StdDeviation$Type$State();
    }

    public static void combine(StdDeviation$Type$State state, $type$ value) {
        state.add(value);
    }

    public static void combineIntermediate(StdDeviation$Type$State state, double mean, double m2, long count) {
        state.combine(mean, m2, count);
    }

    public static void evaluateIntermediate(StdDeviation$Type$State state, DriverContext driverContext, Block[] blocks, int offset) {
        assert blocks.length >= offset + 3;
        BlockFactory blockFactory = driverContext.blockFactory();
        blocks[offset + 0] = blockFactory.newConstantDoubleBlockWith(state.mean(), 1);
        blocks[offset + 1] = blockFactory.newConstantDoubleBlockWith(state.m2(), 1);
        blocks[offset + 2] = blockFactory.newConstantLongBlockWith(state.count(), 1);
    }

    public static Block evaluateFinal(StdDeviation$Type$State state, DriverContext driverContext) {
        final long count = state.count();
        final double m2 = state.m2();
        if (count == 0 || Double.isFinite(m2) == false) {
            return driverContext.blockFactory().newConstantNullBlock(1);
        }
        return driverContext.blockFactory().newConstantDoubleBlockWith(state.evaluateFinal(), 1);
    }

    public static GroupingStdDeviation$Type$State initGrouping(BigArrays bigArrays) {
        return new GroupingStdDeviation$Type$State(bigArrays);
    }

    public static void combine(GroupingStdDeviation$Type$State current, int groupId, $type$ value) {
        current.add(groupId, value);
    }

    public static void combineStates(
        GroupingStdDeviation$Type$State current,
        int groupId,
        GroupingStdDeviation$Type$State state,
        int statePosition
    ) {
        var st = state.states.get(statePosition);
        if (st != null) {
            current.combine(groupId, st.mean(), st.m2(), st.count());
        }
    }

    public static void combineIntermediate(GroupingStdDeviation$Type$State state, int groupId, double mean, double m2, long count) {
        state.combine(groupId, mean, m2, count);
    }

    public static void evaluateIntermediate(
        GroupingStdDeviation$Type$State state,
        Block[] blocks,
        int offset,
        IntVector selected,
        DriverContext driverContext
    ) {
        assert blocks.length >= offset + 3 : "blocks=" + blocks.length + ",offset=" + offset;
        try (
            var meanBuilder = driverContext.blockFactory().newDoubleBlockBuilder(selected.getPositionCount());
            var m2Builder = driverContext.blockFactory().newDoubleBlockBuilder(selected.getPositionCount());
            var countBuilder = driverContext.blockFactory().newLongBlockBuilder(selected.getPositionCount());
        ) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                final var groupId = selected.getInt(i);
                final var st = groupId < state.states.size() ? state.states.get(groupId) : null;
                if (st != null) {
                    meanBuilder.appendDouble(st.mean());
                    m2Builder.appendDouble(st.m2());
                    countBuilder.appendLong(st.count());
                } else {
                    meanBuilder.appendNull();
                    m2Builder.appendNull();
                    countBuilder.appendNull();
                }
            }
            blocks[offset + 0] = meanBuilder.build();
            blocks[offset + 1] = m2Builder.build();
            blocks[offset + 2] = countBuilder.build();
        }
    }

    public static Block evaluateFinal(GroupingStdDeviation$Type$State state, IntVector selected, DriverContext driverContext) {
        try (DoubleBlock.Builder builder = driverContext.blockFactory().newDoubleBlockBuilder(selected.getPositionCount())) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                final var groupId = selected.getInt(i);
                final var st = groupId < state.states.size() ? state.states.get(groupId) : null;
                if (st != null) {
                    final var m2 = st.m2();
                    if (Double.isFinite(m2) == false) {
                        builder.appendNull();
                    } else {
                        builder.appendDouble(st.evaluateFinal());
                    }
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    static final class StdDeviation$Type$State implements AggregatorState {

        private WelfordAlgorithm welfordAlgorithm;

        StdDeviation$Type$State() {
            this(0, 0, 0);
        }

        StdDeviation$Type$State(double mean, double m2, long count) {
            this.welfordAlgorithm = new WelfordAlgorithm(mean, m2, count);
        }

        public void add($type$ value) {
            welfordAlgorithm.add(value);
        }

        public void combine(double mean, double m2, long count) {
            welfordAlgorithm.add(mean, m2, count);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            StdDeviation$Type$Aggregator.evaluateIntermediate(this, driverContext, blocks, offset);
        }

        @Override
        public void close() {}

        public double mean() {
            return welfordAlgorithm.mean();
        }

        public double m2() {
            return welfordAlgorithm.m2();
        }

        public long count() {
            return welfordAlgorithm.count();
        }

        public double evaluateFinal() {
            return welfordAlgorithm.evaluate();
        }
    }

    static final class GroupingStdDeviation$Type$State implements GroupingAggregatorState {

        private ObjectArray<StdDeviation$Type$State> states;
        private final BigArrays bigArrays;

        GroupingStdDeviation$Type$State(BigArrays bigArrays) {
            this.states = bigArrays.newObjectArray(1);
            this.bigArrays = bigArrays;
        }

        public void combine(int groupId, double meanValue, double m2Value, long countValue) {
            ensureCapacity(groupId);
            var state = states.get(groupId);
            if (state == null) {
                state = new StdDeviation$Type$State(meanValue, m2Value, countValue);
                states.set(groupId, state);
            } else {
                state.combine(meanValue, m2Value, countValue);
            }
        }

        public void add(int groupId, $type$ value) {
            ensureCapacity(groupId);
            var state = states.get(groupId);
            if (state == null) {
                state = new StdDeviation$Type$State();
                states.set(groupId, state);
            }
            state.add(value);
        }

        private void ensureCapacity(int groupId) {
            states = bigArrays.grow(states, groupId + 1);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            StdDeviation$Type$Aggregator.evaluateIntermediate(this, blocks, offset, selected, driverContext);
        }

        @Override
        public void close() {
            Releasables.close(states);
        }

        void enableGroupIdTracking(SeenGroupIds seenGroupIds) {
            // noop - we handle the null states inside `toIntermediate` and `evaluateFinal`
        }
    }
}
