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
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;

@Aggregator(
    {
        @IntermediateState(name = "value", type = "DOUBLE"),
        @IntermediateState(name = "delta", type = "DOUBLE"),
        @IntermediateState(name = "seen", type = "BOOLEAN") }
)
@GroupingAggregator
class SumDoubleAggregator {

    public static SumState initSingle() {
        return new SumState();
    }

    public static void combine(SumState current, double v) {
        current.add(v);
    }

    public static void combine(SumState current, double value, double delta) {
        current.add(value, delta);
    }

    public static void combineStates(SumState current, SumState state) {
        current.add(state.value(), state.delta());
    }

    public static void combineIntermediate(SumState state, double inValue, double inDelta, boolean seen) {
        if (seen) {
            combine(state, inValue, inDelta);
            state.seen(true);
        }
    }

    public static void evaluateIntermediate(SumState state, DriverContext driverContext, Block[] blocks, int offset) {
        assert blocks.length >= offset + 3;
        BlockFactory blockFactory = driverContext.blockFactory();
        blocks[offset + 0] = blockFactory.newConstantDoubleBlockWith(state.value(), 1);
        blocks[offset + 1] = blockFactory.newConstantDoubleBlockWith(state.delta(), 1);
        blocks[offset + 2] = blockFactory.newConstantBooleanBlockWith(state.seen(), 1);
    }

    public static Block evaluateFinal(SumState state, DriverContext driverContext) {
        double result = state.value();
        return DoubleBlock.newConstantBlockWith(result, 1, driverContext.blockFactory());
    }

    public static GroupingSumState initGrouping(BigArrays bigArrays) {
        return new GroupingSumState(bigArrays);
    }

    public static void combine(GroupingSumState current, int groupId, double v) {
        current.add(v, groupId);
    }

    public static void combineStates(GroupingSumState current, int groupId, GroupingSumState state, int statePosition) {
        if (state.hasValue(statePosition)) {
            current.add(state.values.get(statePosition), state.deltas.get(statePosition), groupId);
        }
    }

    public static void combineIntermediate(GroupingSumState current, int groupId, double inValue, double inDelta, boolean seen) {
        if (seen) {
            current.add(inValue, inDelta, groupId);
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
            var valuesBuilder = DoubleBlock.newBlockBuilder(selected.getPositionCount(), driverContext.blockFactory());
            var deltaBuilder = DoubleBlock.newBlockBuilder(selected.getPositionCount(), driverContext.blockFactory());
            var seenBuilder = BooleanBlock.newBlockBuilder(selected.getPositionCount(), driverContext.blockFactory())
        ) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int group = selected.getInt(i);
                if (group < state.values.size()) {
                    valuesBuilder.appendDouble(state.values.get(group));
                    deltaBuilder.appendDouble(state.deltas.get(group));
                } else {
                    valuesBuilder.appendDouble(0);
                    deltaBuilder.appendDouble(0);
                }
                seenBuilder.appendBoolean(state.hasValue(group));
            }
            blocks[offset + 0] = valuesBuilder.build();
            blocks[offset + 1] = deltaBuilder.build();
            blocks[offset + 2] = seenBuilder.build();
        }
    }

    public static Block evaluateFinal(GroupingSumState state, IntVector selected, DriverContext driverContext) {
        try (DoubleBlock.Builder builder = DoubleBlock.newBlockBuilder(selected.getPositionCount(), driverContext.blockFactory())) {
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

    static class SumState extends CompensatedSum implements AggregatorState {

        private boolean seen;

        SumState() {
            this(0, 0);
        }

        SumState(double value, double delta) {
            super(value, delta);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            SumDoubleAggregator.evaluateIntermediate(this, driverContext, blocks, offset);
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

    static class GroupingSumState extends AbstractArrayState implements GroupingAggregatorState {
        static final long BYTES_SIZE = Double.BYTES + Double.BYTES;

        DoubleArray values;
        DoubleArray deltas;

        GroupingSumState(BigArrays bigArrays) {
            super(bigArrays);
            boolean success = false;
            try {
                this.values = bigArrays.newDoubleArray(1);
                this.deltas = bigArrays.newDoubleArray(1);
                success = true;
            } finally {
                if (success == false) {
                    close();
                }
            }
        }

        void add(double valueToAdd, int groupId) {
            add(valueToAdd, 0d, groupId);
        }

        void add(double valueToAdd, double deltaToAdd, int groupId) {
            ensureCapacity(groupId);

            // If the value is Inf or NaN, just add it to the running tally to "convert" to
            // Inf/NaN. This keeps the behavior bwc from before kahan summing
            if (Double.isFinite(valueToAdd) == false) {
                values.increment(groupId, valueToAdd);
                return;
            }

            double value = values.get(groupId);
            if (Double.isFinite(value) == false) {
                // It isn't going to get any more infinite.
                return;
            }
            double delta = deltas.get(groupId);
            double correctedSum = valueToAdd + (delta + deltaToAdd);
            double updatedValue = value + correctedSum;
            deltas.set(groupId, correctedSum - (updatedValue - value));
            values.set(groupId, updatedValue);
            trackGroupId(groupId);
        }

        private void ensureCapacity(int groupId) {
            values = bigArrays.grow(values, groupId + 1);
            deltas = bigArrays.grow(deltas, groupId + 1);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            SumDoubleAggregator.evaluateIntermediate(this, blocks, offset, selected, driverContext);
        }

        @Override
        public void close() {
            Releasables.close(values, deltas, () -> super.close());
        }
    }
}
