/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

public final class StdDevStates {

    private StdDevStates() {}

    static final class SingleState implements AggregatorState {

        private WelfordAlgorithm welfordAlgorithm;

        SingleState() {
            this(0, 0, 0);
        }

        SingleState(double mean, double m2, long count) {
            this.welfordAlgorithm = new WelfordAlgorithm(mean, m2, count);
        }

        public void add(long value) {
            welfordAlgorithm.add(value);
        }

        public void add(double value) {
            welfordAlgorithm.add(value);
        }

        public void add(int value) {
            welfordAlgorithm.add(value);
        }

        public void combine(double mean, double m2, long count) {
            welfordAlgorithm.add(mean, m2, count);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            assert blocks.length >= offset + 3;
            BlockFactory blockFactory = driverContext.blockFactory();
            blocks[offset + 0] = blockFactory.newConstantDoubleBlockWith(mean(), 1);
            blocks[offset + 1] = blockFactory.newConstantDoubleBlockWith(m2(), 1);
            blocks[offset + 2] = blockFactory.newConstantLongBlockWith(count(), 1);
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

    static final class GroupingState implements GroupingAggregatorState {

        private ObjectArray<SingleState> states;
        private final BigArrays bigArrays;

        GroupingState(BigArrays bigArrays) {
            this.states = bigArrays.newObjectArray(1);
            this.bigArrays = bigArrays;
        }

        SingleState getOrNull(int position) {
            if (position < states.size()) {
                return states.get(position);
            } else {
                return null;
            }
        }

        public void combine(int groupId, SingleState state) {
            if (state == null) {
                return;
            }
            combine(groupId, state.mean(), state.m2(), state.count());
        }

        public void combine(int groupId, double meanValue, double m2Value, long countValue) {
            ensureCapacity(groupId);
            var state = states.get(groupId);
            if (state == null) {
                state = new SingleState(meanValue, m2Value, countValue);
                states.set(groupId, state);
            } else {
                state.combine(meanValue, m2Value, countValue);
            }
        }

        public SingleState getOrSet(int groupId) {
            ensureCapacity(groupId);
            var state = states.get(groupId);
            if (state == null) {
                state = new SingleState();
                states.set(groupId, state);
            }
            return state;
        }

        public void add(int groupId, long value) {
            var state = getOrSet(groupId);
            state.add(value);
        }

        public void add(int groupId, double value) {
            var state = getOrSet(groupId);
            state.add(value);
        }

        public void add(int groupId, int value) {
            var state = getOrSet(groupId);
            state.add(value);
        }

        private void ensureCapacity(int groupId) {
            states = bigArrays.grow(states, groupId + 1);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            assert blocks.length >= offset + 3 : "blocks=" + blocks.length + ",offset=" + offset;
            try (
                var meanBuilder = driverContext.blockFactory().newDoubleBlockBuilder(selected.getPositionCount());
                var m2Builder = driverContext.blockFactory().newDoubleBlockBuilder(selected.getPositionCount());
                var countBuilder = driverContext.blockFactory().newLongBlockBuilder(selected.getPositionCount());
            ) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    final var groupId = selected.getInt(i);
                    final var state = groupId < states.size() ? states.get(groupId) : null;
                    if (state != null) {
                        meanBuilder.appendDouble(state.mean());
                        m2Builder.appendDouble(state.m2());
                        countBuilder.appendLong(state.count());
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

        @Override
        public void close() {
            Releasables.close(states);
        }

        void enableGroupIdTracking(SeenGroupIds seenGroupIds) {
            // noop - we handle the null states inside `toIntermediate` and `evaluateFinal`
        }
    }
}
