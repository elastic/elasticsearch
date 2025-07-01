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
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

public final class StdDevStates {

    private StdDevStates() {}

    public enum Variation {
        SAMPLE(0),
        POPULATION(1),
        SAMPLE_VARIANCE(2),
        POPULATION_VARIANCE(3);

        private final int index;

        Variation(int index) {
            this.index = index;
        }

        public int getIndex() {
            return index;
        }

        private static Variation getVariation(int index) {
            return switch (index) {
                case 0 -> SAMPLE;
                case 1 -> POPULATION;
                case 2 -> SAMPLE_VARIANCE;
                case 3 -> POPULATION_VARIANCE;
                default -> POPULATION;
            };
        }
    }

    static final class SingleState implements AggregatorState {

        private final WelfordAlgorithm welfordAlgorithm;
        private final Variation variation;

        SingleState(int variation) {
            this(0, 0, 0, variation);
        }

        SingleState(double mean, double m2, long count, int variation) {
            this.welfordAlgorithm = new WelfordAlgorithm(mean, m2, count);
            this.variation = Variation.getVariation(variation);
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
            return switch (variation) {
                case SAMPLE -> welfordAlgorithm.evaluateSample();
                case POPULATION -> welfordAlgorithm.evaluatePopulation();
                case SAMPLE_VARIANCE -> welfordAlgorithm.evaluateSampleVariance();
                case POPULATION_VARIANCE -> welfordAlgorithm.evaluatePopulationVariance();
            };
        }

        public Block evaluateFinal(DriverContext driverContext) {
            final long count = count();
            final double m2 = m2();
            if (count == 0 || Double.isFinite(m2) == false) {
                return driverContext.blockFactory().newConstantNullBlock(1);
            }
            return driverContext.blockFactory().newConstantDoubleBlockWith(evaluateFinal(), 1);
        }
    }

    static final class GroupingState implements GroupingAggregatorState {

        private ObjectArray<WelfordAlgorithm> states;
        private final BigArrays bigArrays;
        private final Variation variation;

        GroupingState(BigArrays bigArrays, int variation) {
            this.states = bigArrays.newObjectArray(1);
            this.bigArrays = bigArrays;
            this.variation = Variation.getVariation(variation);
        }

        WelfordAlgorithm getOrNull(int position) {
            if (position < states.size()) {
                return states.get(position);
            } else {
                return null;
            }
        }

        public void combine(int groupId, WelfordAlgorithm state) {
            if (state == null) {
                return;
            }
            combine(groupId, state.mean(), state.m2(), state.count());
        }

        public void combine(int groupId, double meanValue, double m2Value, long countValue) {
            ensureCapacity(groupId);
            var state = states.get(groupId);
            if (state == null) {
                state = new WelfordAlgorithm(meanValue, m2Value, countValue);
                states.set(groupId, state);
            } else {
                state.add(meanValue, m2Value, countValue);
            }
        }

        public WelfordAlgorithm getOrSet(int groupId) {
            ensureCapacity(groupId);
            var state = states.get(groupId);
            if (state == null) {
                state = new WelfordAlgorithm();
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
                        meanBuilder.appendDouble(0.0);
                        m2Builder.appendDouble(0.0);
                        countBuilder.appendLong(0);
                    }
                }
                blocks[offset + 0] = meanBuilder.build();
                blocks[offset + 1] = m2Builder.build();
                blocks[offset + 2] = countBuilder.build();
            }
        }

        public Block evaluateFinal(IntVector selected, DriverContext driverContext) {
            try (DoubleBlock.Builder builder = driverContext.blockFactory().newDoubleBlockBuilder(selected.getPositionCount())) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    final var groupId = selected.getInt(i);
                    final var st = getOrNull(groupId);
                    if (st != null) {
                        final var m2 = st.m2();
                        final var count = st.count();
                        if (count == 0 || Double.isFinite(m2) == false) {
                            builder.appendNull();
                        } else {
                            double result = switch (variation) {
                                case SAMPLE -> st.evaluateSample();
                                case POPULATION -> st.evaluatePopulation();
                                case SAMPLE_VARIANCE -> st.evaluateSampleVariance();
                                case POPULATION_VARIANCE -> st.evaluatePopulationVariance();
                            };
                            builder.appendDouble(result);
                        }
                    } else {
                        builder.appendNull();
                    }
                }
                return builder.build();
            }
        }

        @Override
        public void close() {
            Releasables.close(states);
        }

        @Override
        public void enableGroupIdTracking(SeenGroupIds seenGroupIds) {
            // noop - we handle the null states inside `toIntermediate` and `evaluateFinal`
        }
    }
}
