/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ConstantBytesRefVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;

public final class LastValueStates {
    private LastValueStates() {}

    static class SingleState implements AggregatorState {
        private double lastValue = Double.NaN;

        private long lastTimestamp = 0;

        SingleState() {}

        private SingleState(BytesRef other) {
            try {
                try (ByteArrayStreamInput in = new ByteArrayStreamInput(other.bytes)) {
                    in.reset(other.bytes, other.offset, other.length);
                    lastValue = in.readDouble();
                    lastTimestamp = in.readVLong();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        BytesRef serialize() {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            OutputStreamStreamOutput out = new OutputStreamStreamOutput(baos);
            try {
                out.writeDouble(lastValue);
                out.writeVLong(lastTimestamp);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return new BytesRef(baos.toByteArray());
        }

        @Override
        public void close() {}

        void add(double v, long ts) {
            if (Double.isNaN(lastValue) || ts > lastTimestamp) {
                lastValue = v;
                lastTimestamp = ts;
            }
        }

        void add(SingleState other) {
            if (Double.isNaN(lastValue)) {
                if (Double.isNaN(other.lastValue) == false) {
                    lastValue = other.lastValue;
                    lastTimestamp = other.lastTimestamp;
                }
            } else if (Double.isNaN(other.lastValue) == false) {
                if (lastTimestamp < other.lastTimestamp) {
                    lastValue = other.lastValue;
                    lastTimestamp = other.lastTimestamp;
                } else {
                    other.lastValue = lastValue;
                    other.lastTimestamp = lastTimestamp;
                }
            }
        }

        void add(BytesRef other) {
            add(new SingleState(other));
        }

        /** Extracts an intermediate view of the contents of this state.  */
        @Override
        public void toIntermediate(Block[] blocks, int offset) {
            assert blocks.length >= offset + 1;
            blocks[offset] = new ConstantBytesRefVector(serialize(), 1).asBlock();
        }

        Block evaluateLastValue(DriverContext driverContext) {
            if (Double.isNaN(lastValue)) {
                return Block.constantNullBlock(1);
            }
            return DoubleBlock.newConstantBlockWith(lastValue, 1);
        }
    }

    static class GroupingState implements GroupingAggregatorState {
        private ObjectArray<SingleState> states;
        private final BigArrays bigArrays;

        GroupingState(BigArrays bigArrays) {
            this.bigArrays = Objects.requireNonNull(bigArrays);
            this.states = bigArrays.newObjectArray(1);
        }

        private SingleState getOrAddGroup(int groupId) {
            states = bigArrays.grow(states, groupId + 1);
            SingleState state = states.get(groupId);
            if (state == null) {
                state = new SingleState();
                states.set(groupId, state);
            }
            return state;
        }

        void add(int groupId, double v, long ts) {
            getOrAddGroup(groupId).add(v, ts);
        }

        void add(int groupId, SingleState other) {
            if (other != null) {
                getOrAddGroup(groupId).add(other);
            }
        }

        void enableGroupIdTracking(SeenGroupIds seenGroupIds) {
            // We always enable.
        }

        void add(int groupId, BytesRef other) {
            getOrAddGroup(groupId).add(new SingleState(other));
        }

        SingleState getOrNull(int position) {
            if (position < states.size()) {
                return states.get(position);
            }
            return null;
        }

        /** Extracts an intermediate view of the contents of this state.  */
        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            assert blocks.length >= offset + 1;
            try (var builder = BytesRefBlock.newBlockBuilder(selected.getPositionCount(), driverContext.blockFactory())) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    int group = selected.getInt(i);
                    SingleState state;
                    if (group < states.size()) {
                        state = getOrNull(group);
                        if (state == null) {
                            state = new SingleState();
                        }
                    } else {
                        state = new SingleState();
                    }
                    builder.appendBytesRef(state.serialize());
                }
                blocks[offset] = builder.build();
            }
        }

        Block evaluateLastValue(IntVector selected, DriverContext driverContext) {
            final DoubleBlock.Builder builder = DoubleBlock.newBlockBuilder(selected.getPositionCount(), driverContext.blockFactory());
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int si = selected.getInt(i);
                if (si >= states.size()) {
                    builder.appendNull();
                    continue;
                }
                final SingleState state = states.get(si);
                if (state != null) {
                    builder.appendDouble(state.lastValue);
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }

        @Override
        public void close() {
            states.close();
        }
    }
}
