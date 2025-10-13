/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

@Aggregator(
    {
        @IntermediateState(name = "value", type = "DOUBLE"),
        // Unlike the compensated sum, the lossy sum does not use deltas. This unused deltas block is padded for alignment
        // with the compensated sum, allow the summation options are exposed to users without worrying about BWC issues.
        @IntermediateState(name = "unusedDeltas", type = "DOUBLE"),
        @IntermediateState(name = "seen", type = "BOOLEAN") }
)
@GroupingAggregator
class LossySumDoubleAggregator {

    public static SumState initSingle() {
        return new SumState();
    }

    public static void combine(SumState current, double v) {
        current.value += v;
    }

    public static void combine(SumState current, double value, double unusedDelta) {
        assert unusedDelta == 0.0 : "Lossy sum should not have delta " + unusedDelta;
        current.value += value;
    }

    public static void combineIntermediate(SumState state, double inValue, double unusedDelta, boolean seen) {
        // Unlike the compensated sum, the lossy sum does not use deltas. This unused deltas block is padded for alignment
        // with the compensated sum, allow the summation options are exposed to users without worrying about BWC issues.
        assert unusedDelta == 0.0 : "Lossy sum should not have delta " + unusedDelta;
        if (seen) {
            combine(state, inValue);
            state.seen(true);
        }
    }

    public static void evaluateIntermediate(SumState state, DriverContext driverContext, Block[] blocks, int offset) {
        assert blocks.length >= offset + 3;
        BlockFactory blockFactory = driverContext.blockFactory();
        blocks[offset + 0] = blockFactory.newConstantDoubleBlockWith(state.value, 1);
        blocks[offset + 1] = blockFactory.newConstantDoubleBlockWith(0.0, 1);
        blocks[offset + 2] = blockFactory.newConstantBooleanBlockWith(state.seen(), 1);
    }

    public static Block evaluateFinal(SumState state, DriverContext driverContext) {
        return driverContext.blockFactory().newConstantDoubleBlockWith(state.value, 1);
    }

    public static GroupingSumState initGrouping(BigArrays bigArrays) {
        return new GroupingSumState(bigArrays);
    }

    public static void combine(GroupingSumState current, int groupId, double v) {
        current.add(v, groupId);
    }

    public static void combineIntermediate(GroupingSumState current, int groupId, double value, double zeroDelta, boolean seen) {
        assert zeroDelta == 0.0 : zeroDelta;
        if (seen) {
            current.add(value, groupId);
        }
    }

    public static void evaluateIntermediate(
        GroupingSumState state,
        Block[] blocks,
        int offset,
        IntVector selected,
        DriverContext driverContext
    ) {
        assert blocks.length >= offset + 3;
        try (
            var valuesBuilder = driverContext.blockFactory().newDoubleVectorFixedBuilder(selected.getPositionCount());
            var seenBuilder = driverContext.blockFactory().newBooleanVectorFixedBuilder(selected.getPositionCount())
        ) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int group = selected.getInt(i);
                if (group < state.values.size()) {
                    valuesBuilder.appendDouble(state.values.get(group));
                } else {
                    valuesBuilder.appendDouble(0);
                }
                seenBuilder.appendBoolean(state.hasValue(group));
            }
            blocks[offset + 0] = valuesBuilder.build().asBlock();
            blocks[offset + 1] = driverContext.blockFactory().newConstantDoubleBlockWith(0.0, selected.getPositionCount());
            blocks[offset + 2] = seenBuilder.build().asBlock();
        }
    }

    public static Block evaluateFinal(GroupingSumState state, IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        try (DoubleBlock.Builder builder = ctx.blockFactory().newDoubleBlockBuilder(selected.getPositionCount())) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int si = selected.getInt(i);
                if (state.hasValue(si) && si < state.values.size()) {
                    builder.appendDouble(state.values.get(si));
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    public static GroupingAggregatorFunction.AddInput wrapAddInput(
        GroupingAggregatorFunction.AddInput delegate,
        GroupingSumState state,
        DoubleVector values
    ) {
        return new GroupingAggregatorFunction.AddInput() {
            @Override
            public void add(int positionOffset, IntArrayBlock groupIds) {
                delegate.add(positionOffset, groupIds);
            }

            @Override
            public void add(int positionOffset, IntBigArrayBlock groupIds) {
                delegate.add(positionOffset, groupIds);
            }

            @Override
            public void add(int positionOffset, IntVector groupIds) {
                if (groupIds.isConstant()) {
                    double sum = 0.0;
                    final int to = positionOffset + groupIds.getPositionCount();
                    for (int i = positionOffset; i < to; i++) {
                        sum += values.getDouble(i);
                    }
                    state.add(sum, groupIds.getInt(0));
                } else {
                    delegate.add(positionOffset, groupIds);
                }
            }

            @Override
            public void close() {
                Releasables.close(delegate);
            }
        };
    }

    static final class SumState implements AggregatorState {
        private boolean seen;
        double value;

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            LossySumDoubleAggregator.evaluateIntermediate(this, driverContext, blocks, offset);
        }

        @Override
        public void close() {}

        public boolean seen() {
            return seen;
        }

        public void seen(boolean seen) {
            this.seen = seen;
        }
    }

    static final class GroupingSumState extends AbstractArrayState implements GroupingAggregatorState {
        private DoubleArray values;

        GroupingSumState(BigArrays bigArrays) {
            super(bigArrays);
            boolean success = false;
            try {
                this.values = bigArrays.newDoubleArray(128);
                success = true;
            } finally {
                if (success == false) {
                    close();
                }
            }
        }

        void add(double valueToAdd, int groupId) {
            values = bigArrays.grow(values, groupId + 1);
            values.increment(groupId, valueToAdd);
            trackGroupId(groupId);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            LossySumDoubleAggregator.evaluateIntermediate(this, blocks, offset, selected, driverContext);
        }

        @Override
        public void close() {
            Releasables.close(values, super::close);
        }
    }
}
