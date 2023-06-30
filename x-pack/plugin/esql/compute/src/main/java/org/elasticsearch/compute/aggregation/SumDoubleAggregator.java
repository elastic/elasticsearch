/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.ConstantBooleanVector;
import org.elasticsearch.compute.data.ConstantDoubleVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongVector;
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

    public static void combineIntermediate(SumState state, DoubleVector values, DoubleVector deltas, BooleanVector seen) {
        if (seen.getBoolean(0)) {
            combine(state, values.getDouble(0), deltas.getDouble(0));
            state.seen(true);
        }
    }

    public static void evaluateIntermediate(SumState state, Block[] blocks, int offset) {
        assert blocks.length >= offset + 3;
        blocks[offset + 0] = new ConstantDoubleVector(state.value(), 1).asBlock();
        blocks[offset + 1] = new ConstantDoubleVector(state.delta(), 1).asBlock();
        blocks[offset + 2] = new ConstantBooleanVector(state.seen, 1).asBlock();
    }

    public static Block evaluateFinal(SumState state) {
        double result = state.value();
        return DoubleBlock.newConstantBlockWith(result, 1);
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
        } else {
            current.putNull(groupId);
        }
    }

    public static void combine(GroupingSumState current, int groupId, double value, double delta, boolean seen) {
        if (seen) {
            current.add(value, delta, groupId);
        } else {
            current.putNull(groupId);
        }
    }

    public static void combineIntermediate(
        LongVector groupIdVector,
        GroupingSumState state,
        DoubleVector values,
        DoubleVector deltas,
        BooleanVector seen
    ) {
        for (int position = 0; position < groupIdVector.getPositionCount(); position++) {
            int groupId = Math.toIntExact(groupIdVector.getLong(position));
            if (seen.getBoolean(position)) {
                state.add(values.getDouble(position), deltas.getDouble(position), groupId);
            } else {
                state.putNull(groupId);
            }
        }
    }

    public static void evaluateIntermediate(GroupingSumState state, Block[] blocks, int offset, IntVector selected) {
        assert blocks.length >= offset + 3;
        var valuesBuilder = DoubleBlock.newBlockBuilder(selected.getPositionCount());
        var deltaBuilder = DoubleBlock.newBlockBuilder(selected.getPositionCount());
        var nullsBuilder = BooleanBlock.newBlockBuilder(selected.getPositionCount());
        for (int i = 0; i < selected.getPositionCount(); i++) {
            int group = selected.getInt(i);
            valuesBuilder.appendDouble(state.values.get(group));
            deltaBuilder.appendDouble(state.deltas.get(group));
            if (state.seen != null) {
                nullsBuilder.appendBoolean(state.seen.get(group));
            }
        }
        blocks[offset + 0] = valuesBuilder.build();
        blocks[offset + 1] = deltaBuilder.build();
        if (state.seen != null) {
            blocks[offset + 2] = nullsBuilder.build();
        } else {
            blocks[offset + 2] = new ConstantBooleanVector(true, selected.getPositionCount()).asBlock();
        }
    }

    public static Block evaluateFinal(GroupingSumState state, IntVector selected) {
        DoubleBlock.Builder builder = DoubleBlock.newBlockBuilder(selected.getPositionCount());
        for (int i = 0; i < selected.getPositionCount(); i++) {
            if (state.hasValue(i)) {
                builder.appendDouble(state.values.get(selected.getInt(i)));
            } else {
                builder.appendNull();
            }
        }
        return builder.build();
    }

    static class SumState extends CompensatedSum implements AggregatorState<SumState> {

        private boolean seen;

        SumState() {
            this(0, 0);
        }

        SumState(double value, double delta) {
            super(value, delta);
        }

        @Override
        public long getEstimatedSize() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {}

        @Override
        public AggregatorStateSerializer<SumState> serializer() {
            throw new UnsupportedOperationException();
        }

        public boolean seen() {
            return seen;
        }

        public void seen(boolean seen) {
            this.seen = seen;
        }
    }

    static class GroupingSumState implements AggregatorState<GroupingSumState> {
        private final BigArrays bigArrays;
        static final long BYTES_SIZE = Double.BYTES + Double.BYTES;

        DoubleArray values;
        DoubleArray deltas;

        // total number of groups; <= values.length
        int largestGroupId;

        private BitArray seen;

        GroupingSumState(BigArrays bigArrays) {
            this.bigArrays = bigArrays;
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
            if (seen != null) {
                seen.set(groupId);
            }
        }

        void putNull(int groupId) {
            if (groupId > largestGroupId) {
                ensureCapacity(groupId);
                largestGroupId = groupId;
            }
            if (seen == null) {
                seen = new BitArray(groupId + 1, bigArrays);
                for (int i = 0; i < groupId; i++) {
                    seen.set(i);
                }
            } else {
                seen.ensureCapacity(groupId + 1);
            }
        }

        boolean hasValue(int index) {
            return seen == null || seen.get(index);
        }

        private void ensureCapacity(int groupId) {
            if (groupId > largestGroupId) {
                largestGroupId = groupId;
                values = bigArrays.grow(values, groupId + 1);
                deltas = bigArrays.grow(deltas, groupId + 1);
            }
        }

        @Override
        public long getEstimatedSize() {
            throw new UnsupportedOperationException();
        }

        @Override
        public AggregatorStateSerializer<GroupingSumState> serializer() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            Releasables.close(values, deltas, seen);
        }
    }
}
