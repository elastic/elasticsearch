/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

// begin generated imports
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
// end generated imports

/**
 * An aggregator that retrieves the first value it encounters. It is useful in cases where we want to get back the
 * first (or last) value of a field, where the sort field is either null or a constant literal. In such cases, any
 * value from the search field will suffice, and the state needs to be updated at most once.
 *
 * This class is generated. Edit `X-AnyValueAggregator.java.st` instead.
 */
@Aggregator({ @IntermediateState(name = "observed", type = "BOOLEAN"), @IntermediateState(name = "values", type = "LONG_BLOCK") })
@GroupingAggregator({ @IntermediateState(name = "observed", type = "BOOLEAN"), @IntermediateState(name = "values", type = "LONG_BLOCK") })
public class AnyLongAggregator {
    public static String describe() {
        return "any_long_aggregator";
    }

    public static SingleState initSingle(DriverContext driverContext) {
        return new SingleState(driverContext.bigArrays());
    }

    public static void combine(SingleState current, @Position int position, LongBlock values) {
        if (current.observed) {
            // Short-circuit if we've already observed a value in this state
            return;
        }
        overrideState(current, values, position);
    }

    public static void combineIntermediate(SingleState current, boolean observed, LongBlock values) {
        if (observed == false || current.observed) {
            // We've already observed a value, or the incoming state didn't. In both cases, we can short-circuit.
            return;
        }
        overrideState(current, values, 0);
    }

    public static Block evaluateFinal(SingleState current, DriverContext ctx) {
        return current.intermediateValuesBlockBuilder(ctx);
    }

    private static void overrideState(SingleState current, LongBlock values, int position) {
        current.observed = true;
        if (values.isNull(position)) {
            Releasables.close(current.values);
            current.values = null;
        } else {
            int count = values.getValueCount(position);
            int offset = values.getFirstValueIndex(position);
            LongArray a = null;
            boolean success = false;
            try {
                a = current.bigArrays.newLongArray(count);
                for (int i = 0; i < count; ++i) {
                    a.set(i, values.getLong(offset + i));
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
        private LongArray values;

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
            long[] vals = new long[size];
            for (int i = 0; i < size; ++i) {
                vals[i] = values.get(i);
            }
            return driverContext.blockFactory().newLongArrayBlock(vals, 1, new int[] { 0, size }, null, Block.MvOrdering.UNORDERED);
        }

        @Override
        public void close() {
            Releasables.close(values);
        }
    }

    public static GroupingState initGrouping(DriverContext driverContext) {
        return new GroupingState(driverContext.bigArrays());
    }

    public static void combine(GroupingState current, int group, @Position int position, LongBlock values) {
        current.collectValue(group, position, values);
    }

    public static void combineIntermediate(GroupingState current, int group, boolean observed, LongBlock values, int otherPosition) {
        if (observed == false) {
            return;
        }
        current.collectValue(group, otherPosition, values);
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        return state.evaluateFinal(selected, ctx);
    }

    public static final class GroupingState extends AbstractArrayState {

        /**
         * First values, stored in a dense array to minimize per-group overhead.
         */
        private LongArray firstValues;

        /**
         * The second-and-beyond values. Null for groups with zero or one value.
         */
        private ObjectArray<LongArray> tailValues;

        /**
         * Lazy; set bit means the group observed a null value.
         */
        private BitArray nullValue;

        /**
         * Highest group id seen so far; -1 when no groups have been processed.
         */
        private int maxGroupId = -1;

        GroupingState(BigArrays bigArrays) {
            super(bigArrays);
            boolean success = false;
            try {
                this.firstValues = bigArrays.newLongArray(1, false);
                enableGroupIdTracking(new SeenGroupIds.Empty());
                success = true;
            } finally {
                if (success == false) {
                    Releasables.close(firstValues, super::close);
                }
            }
        }

        void collectValue(int group, int position, LongBlock valuesBlock) {
            if (group <= maxGroupId && hasValue(group)) {
                // Short-circuit: we already have a value for this group.
                return;
            }
            if (group > maxGroupId) {
                grow(group);
                maxGroupId = group;
            }
            if (valuesBlock.isNull(position)) {
                setNullValue(group);
            } else {
                int count = valuesBlock.getValueCount(position);
                int offset = valuesBlock.getFirstValueIndex(position);
                firstValues.set(group, valuesBlock.getLong(offset));
                if (count > 1) {
                    LongArray tail = getTailForWriting(group, count - 1);
                    for (int i = 1; i < count; ++i) {
                        tail.set(i - 1, valuesBlock.getLong(offset + i));
                    }
                }
            }
            trackGroupId(group);
        }

        private boolean nullValue(int group) {
            return nullValue != null && nullValue.get(group);
        }

        private void setNullValue(int group) {
            if (nullValue == null) {
                nullValue = new BitArray(group + 1, bigArrays);
            }
            nullValue.set(group);
        }

        private void grow(int group) {
            firstValues = bigArrays.grow(firstValues, group + 1);
        }

        private LongArray getTail(int group) {
            if (tailValues == null || group >= tailValues.size()) {
                return null;
            }
            return tailValues.get(group);
        }

        private LongArray getTailForWriting(int group, int count) {
            LongArray existing;
            if (tailValues == null) {
                tailValues = bigArrays.newObjectArray(group + 1);
                existing = null;
            } else if (group >= tailValues.size()) {
                tailValues = bigArrays.grow(tailValues, group + 1);
                existing = null;
            } else {
                existing = tailValues.get(group);
            }
            if (existing == null) {
                LongArray tail = bigArrays.newLongArray(count);
                tailValues.set(group, tail);
                return tail;
            }
            if (existing.size() == count) {
                return existing;
            }
            LongArray resized = bigArrays.resize(existing, count);
            tailValues.set(group, resized);
            return resized;
        }

        @Override
        public void close() {
            if (tailValues != null) {
                for (long i = 0; i < tailValues.size(); i++) {
                    Releasables.close(tailValues.get(i));
                }
            }
            Releasables.close(nullValue, firstValues, tailValues, super::close);
        }

        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            try (var observedBlockBuilder = driverContext.blockFactory().newBooleanBlockBuilder(selected.getPositionCount())) {
                for (int p = 0; p < selected.getPositionCount(); ++p) {
                    int group = selected.getInt(p);
                    observedBlockBuilder.appendBoolean(group <= maxGroupId && hasValue(group));
                }
                blocks[offset + 0] = observedBlockBuilder.build();
                blocks[offset + 1] = intermediateValuesBlockBuilder(selected, driverContext.blockFactory());
            }
        }

        Block evaluateFinal(IntVector groups, GroupingAggregatorEvaluationContext evalContext) {
            return intermediateValuesBlockBuilder(groups, evalContext.blockFactory());
        }

        private Block intermediateValuesBlockBuilder(IntVector groups, BlockFactory blockFactory) {
            try (var valuesBuilder = blockFactory.newLongBlockBuilder(groups.getPositionCount())) {
                for (int p = 0; p < groups.getPositionCount(); ++p) {
                    int group = groups.getInt(p);
                    if (group > maxGroupId || hasValue(group) == false || nullValue(group)) {
                        valuesBuilder.appendNull();
                        continue;
                    }
                    LongArray tail = getTail(group);
                    int tailCount = tail == null ? 0 : (int) tail.size();
                    if (tailCount == 0) {
                        valuesBuilder.appendLong(firstValues.get(group));
                        continue;
                    }
                    valuesBuilder.beginPositionEntry();
                    valuesBuilder.appendLong(firstValues.get(group));
                    for (int i = 0; i < tailCount; ++i) {
                        valuesBuilder.appendLong(tail.get(i));
                    }
                    valuesBuilder.endPositionEntry();
                }
                return valuesBuilder.build();
            }
        }
    }
}
