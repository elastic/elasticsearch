/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ConstantBytesRefVector;
import org.elasticsearch.compute.data.ConstantNullBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;

public final class BytesRefState {

    public enum Operations {
        MAX, MIN
    }

     static class SingleState implements AggregatorState {
        private BytesRef value;
        private final Operations op;

        SingleState(Operations op) {
            this.value = null;
            this.op = op;
        }

        public BytesRef value() { return this.value; }

        @Override
        public void close() {}

        @Override
        public void toIntermediate(Block[] blocks, int offset) {
            assert blocks.length >= offset + 1;
            if (this.value == null) {
                blocks[offset] = Block.constantNullBlock(1);
                return;
            }
            blocks[offset] = new ConstantBytesRefVector(this.value, 1).asBlock();
        }

        void checkAndSet(BytesRef v) {
            if (v == null) {
                return;
            }

            if (this.op == Operations.MAX) {
                if (this.value == null || v.compareTo(this.value) > 0) {
                    this.value = new BytesRef(v.utf8ToString());
                }
            } else if (this.op == Operations.MIN) {
                if (this.value == null || v.compareTo(this.value) < 0) {
                    this.value = new BytesRef(v.utf8ToString());
                }
            }
        }

        Block evaluate(DriverContext driverContext) {
            if (this.value == null) {
                return Block.constantNullBlock(1);
            }

            return BytesRefBlock.newConstantBlockWith(this.value, 1);
        }
    }

    static class GroupingState implements GroupingAggregatorState {

        private final BigArrays bigArrays;
        private ObjectArray<BytesRefState.SingleState> states;
        private final Operations op;

        GroupingState(BigArrays bigArrays, Operations op) {
            this.bigArrays = bigArrays;
            this.states = bigArrays.newObjectArray(1);
            this.op = op;
        }

        @Override
        public void close() {
            states.close();
        }

        public BytesRefState.SingleState getOrAddGroup(int groupId) {
            this.states = this.bigArrays.grow(this.states, groupId + 1);
            SingleState s = this.states.get(groupId);
            if (s == null) {
                s = new BytesRefState.SingleState(this.op);
                this.states.set(groupId, s);
            }
            return s;
        }

        public BytesRefState.SingleState getOrNull(int groupId) {
            if (groupId < this.states.size()) {
                return states.get(groupId);
            } else {
                return null;
            }
        }

        /** Extracts an intermediate view of the contents of this state.  */
        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            assert blocks.length >= offset + 1;
            try (var builder = BytesRefBlock.newBlockBuilder(selected.getPositionCount(), driverContext.blockFactory())) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    int group = selected.getInt(i);
                    BytesRefState.SingleState state;
                    if (group < this.states.size()) {
                        state = getOrNull(group);
                        if (state == null) {
                            state = new BytesRefState.SingleState(this.op);
                        }
                    } else {
                        state = new BytesRefState.SingleState(this.op);
                    }
                    if (state.value == null) {
                        builder.appendNull();
                    } else {
                        builder.appendBytesRef(state.value);
                    }
                }
                blocks[offset] = builder.build();
            }
        }

        void add(int groupId, BytesRef v) {
            getOrAddGroup(groupId).checkAndSet(v);
        }

        void add(int groupId, BytesRefState.SingleState other) {
            if (other != null) {
                getOrAddGroup(groupId).checkAndSet(other.value);
            }
        }

        Block evaluate(IntVector selected, DriverContext driverContext) {
            try (BytesRefBlock.Builder builder = BytesRefBlock.newBlockBuilder(selected.getPositionCount(), driverContext.blockFactory())) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    int si = selected.getInt(i);
                    if (si >= this.states.size()) {
                        builder.appendNull();
                        continue;
                    }
                    final BytesRefState.SingleState state = this.states.get(si);
                    if (state != null && state.value != null) {
                        builder.appendBytesRef(state.value);
                    } else {
                        builder.appendNull();
                    }
                }
                return builder.build();
            }
        }

        void enableGroupIdTracking(SeenGroupIds seenGroupIds) {
            // We always enable.
        }

    }
}
