/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

/**
 * An aggregator that retrieves the first value it encounters. It is useful in cases where we want to get back the
 * first (or last) value of a field, where the sort field is either null or a constant literal. In such cases, any
 * value from the search field will suffice, and the state needs to be updated at most once.
 *
 * This class is generated. Edit `X-AnyValueAggregator.java.st` instead.
 */
@Aggregator({ @IntermediateState(name = "observed", type = "BOOLEAN"), @IntermediateState(name = "values", type = "DOUBLE_BLOCK") })
@GroupingAggregator({ @IntermediateState(name = "observed", type = "BOOLEAN"), @IntermediateState(name = "values", type = "DOUBLE_BLOCK") })
public class AnyDoubleAggregator {
    public static String describe() {
        return "any_double_aggregator";
    }

    public static SingleState initSingle(DriverContext driverContext) {
        return new SingleState(driverContext.bigArrays());
    }

    public static void combine(SingleState current, @Position int position, DoubleBlock values) {
        if (current.observed) {
            // Short-circuit if we've already observed a value in this state
            return;
        }
        overrideState(current, values, position);
    }

    public static void combineIntermediate(SingleState current, boolean observed, DoubleBlock values) {
        if (observed == false || current.observed) {
            // We've already observed a value, or the incoming state didn't. In both cases, we can short-circuit.
            return;
        }
        overrideState(current, values, 0);
    }

    public static Block evaluateFinal(SingleState current, DriverContext ctx) {
        return current.intermediateValuesBlockBuilder(ctx);
    }

    private static void overrideState(SingleState current, DoubleBlock values, int position) {
        current.observed = true;
        if (values.isNull(position)) {
            Releasables.close(current.values);
            current.values = null;
        } else {
            int count = values.getValueCount(position);
            int offset = values.getFirstValueIndex(position);
            DoubleArray a = null;
            boolean success = false;
            try {
                a = current.bigArrays.newDoubleArray(count);
                for (int i = 0; i < count; ++i) {
                    a.set(i, values.getDouble(offset + i));
                }
                success = true;
                Releasables.close(current.values);
                current.values = a;
            } finally {
                if (success == false) {
                    Releasables.close(a);
                }
            }
        }
    }

    public static class SingleState implements AggregatorState {
        private final BigArrays bigArrays;
        private boolean observed;
        private DoubleArray values;

        public SingleState(BigArrays bigArrays) {
            this.bigArrays = bigArrays;
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            assert blocks.length >= offset + 2;
            blocks[offset + 0] = driverContext.blockFactory().newConstantBooleanBlockWith(observed, 1);
            blocks[offset + 1] = intermediateValuesBlockBuilder(driverContext);
        }

        Block intermediateValuesBlockBuilder(DriverContext driverContext) {
            if (values == null) {
                return driverContext.blockFactory().newConstantNullBlock(1);
            }
            int size = (int) values.size();
            double[] vals = new double[size];
            for (int i = 0; i < size; ++i) {
                vals[i] = values.get(i);
            }
            return driverContext.blockFactory().newDoubleArrayBlock(vals, 1, new int[] { 0, size }, null, Block.MvOrdering.UNORDERED);
        }

        @Override
        public void close() {
            Releasables.close(values);
        }
    }

    public static GroupingState initGrouping(DriverContext driverContext) {
        return new GroupingState(driverContext.bigArrays());
    }

    public static void combine(GroupingState current, int group, @Position int position, DoubleBlock values) {
        current.collectValue(group, position, values);
    }

    public static void combineIntermediate(GroupingState current, int group, boolean observed, DoubleBlock values, int otherPosition) {
        if (observed == false) {
            return;
        }
        current.collectValue(group, otherPosition, values);
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        return state.evaluateFinal(selected, ctx);
    }

    public static final class GroupingState extends AbstractArrayState {
        private final BigArrays bigArrays;

        /**
         * The group-indexed observed flags
         */
        private ByteArray observed;

        /**
         * The group-indexed values
         * TODO: apply the firstValue/tailValues optimization from X-AllValueByTimestampAggregator.java.st
         * to inline single-element groups and avoid the ~64 byte ObjectArray wrapper overhead.
         */
        private ObjectArray<DoubleArray> values;

        GroupingState(BigArrays bigArrays) {
            super(bigArrays);
            this.bigArrays = bigArrays;
            boolean success = false;
            ByteArray observed = null;
            try {
                // Initialize observed
                observed = bigArrays.newByteArray(1, true);
                observed.set(0, (byte) -1);
                this.observed = observed;

                // Initialize values
                this.values = bigArrays.newObjectArray(1);
                this.values.set(0, null);

                enableGroupIdTracking(new SeenGroupIds.Empty());
                success = true;
            } finally {
                if (success == false) {
                    if (values != null) {
                        for (long i = 0; i < values.size(); ++i) {
                            Releasables.close(values.get(i));
                        }
                    }
                    Releasables.close(observed, values, super::close);
                }
            }
        }

        void collectValue(int group, int position, DoubleBlock valuesBlock) {
            if (withinBounds(group) && observed.get(group) == 1) {
                // We have already observed this group. We can short-circuit since any value is fine for this aggregator.
                return;
            }
            if (withinBounds(group) == false) {
                observed = bigArrays.grow(observed, group + 1);
                values = bigArrays.grow(values, group + 1);
            }

            // We always want to update here
            observed.set(group, (byte) 1);
            boolean success = false;
            DoubleArray groupValues = null;
            try {
                if (valuesBlock.isNull(position) == false) {
                    int count = valuesBlock.getValueCount(position);
                    int offset = valuesBlock.getFirstValueIndex(position);
                    groupValues = bigArrays.newDoubleArray(count);
                    for (int i = 0; i < count; ++i) {
                        groupValues.set(i, valuesBlock.getDouble(i + offset));
                    }
                }
                success = true;
                Releasables.close(values.get(group));
                values.set(group, groupValues);
            } finally {
                if (success == false) {
                    Releasables.close(groupValues);
                }
            }
            trackGroupId(group);
        }

        @Override
        public void close() {
            for (long i = 0; i < values.size(); ++i) {
                Releasables.close(values.get(i));
            }
            Releasables.close(observed, values, super::close);
        }

        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            try (var observedBlockBuilder = driverContext.blockFactory().newBooleanBlockBuilder(selected.getPositionCount())) {
                for (int p = 0; p < selected.getPositionCount(); ++p) {
                    int group = selected.getInt(p);
                    if (withinBounds(group)) {
                        // We must have seen this group before and saved its state
                        observedBlockBuilder.appendBoolean(observed.get(group) == 1);
                    } else {
                        observedBlockBuilder.appendBoolean(false);
                    }
                }

                // Create all intermediate state blocks
                blocks[offset + 0] = observedBlockBuilder.build();
                blocks[offset + 1] = intermediateValuesBlockBuilder(selected, driverContext.blockFactory());
            }
        }

        Block evaluateFinal(IntVector groups, GroupingAggregatorEvaluationContext evalContext) {
            return intermediateValuesBlockBuilder(groups, evalContext.blockFactory());
        }

        private boolean withinBounds(int group) {
            return group < Math.min(values.size(), observed.size());
        }

        private Block intermediateValuesBlockBuilder(IntVector groups, BlockFactory blockFactory) {
            try (var valuesBuilder = blockFactory.newDoubleBlockBuilder(groups.getPositionCount())) {
                for (int p = 0; p < groups.getPositionCount(); ++p) {
                    int group = groups.getInt(p);
                    int count = 0;
                    if (withinBounds(group) && observed.get(group) == 1 && values.get(group) != null) {
                        count = (int) values.get(group).size();
                    }
                    switch (count) {
                        case 0 -> valuesBuilder.appendNull();
                        case 1 -> valuesBuilder.appendDouble(values.get(group).get(0));
                        default -> {
                            valuesBuilder.beginPositionEntry();
                            for (int i = 0; i < count; ++i) {
                                valuesBuilder.appendDouble(values.get(group).get(i));
                            }
                            valuesBuilder.endPositionEntry();
                        }
                    }
                }
                return valuesBuilder.build();
            }
        }
    }
}
