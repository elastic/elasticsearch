/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

// begin generated imports
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.LongHashTable;
import org.elasticsearch.common.util.FloatArray;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.HashImplFactory;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
// end generated imports

/**
 * Aggregates field values for float.
 * This class is generated. Edit @{code X-ValuesAggregator.java.st} instead
 * of this file.
 */
@Aggregator({ @IntermediateState(name = "values", type = "FLOAT_BLOCK") })
@GroupingAggregator
class ValuesFloatAggregator {
    public static SingleState initSingle(BigArrays bigArrays) {
        return new SingleState(bigArrays);
    }

    public static void combine(SingleState state, float v) {
        state.values.add(Float.floatToIntBits(v));
    }

    public static void combineIntermediate(SingleState state, FloatBlock values) {
        int start = values.getFirstValueIndex(0);
        int end = start + values.getValueCount(0);
        for (int i = start; i < end; i++) {
            combine(state, values.getFloat(i));
        }
    }

    public static Block evaluateFinal(SingleState state, DriverContext driverContext) {
        return state.toBlock(driverContext.blockFactory());
    }

    public static GroupingState initGrouping(DriverContext driverContext) {
        return new GroupingState(driverContext);
    }

    public static void combine(GroupingState state, int groupId, float v) {
        state.addValue(groupId, v);
    }

    public static void combineIntermediate(GroupingState state, int groupId, FloatBlock values, int valuesPosition) {
        int start = values.getFirstValueIndex(valuesPosition);
        int end = start + values.getValueCount(valuesPosition);
        for (int i = start; i < end; i++) {
            state.addValue(groupId, values.getFloat(i));
        }
    }

    public static GroupingAggregatorFunction.PreparedForEvaluation prepareEvaluateIntermediate(
        GroupingState state,
        IntVector selected,
        GroupingAggregatorEvaluationContext ctx
    ) {
        return state.prepareForEmitting(ctx.blockFactory(), selected);
    }

    public static GroupingAggregatorFunction.PreparedForEvaluation prepareEvaluateFinal(
        GroupingState state,
        IntVector selected,
        GroupingAggregatorEvaluationContext ctx
    ) {
        return state.prepareForEmitting(ctx.blockFactory(), selected);
    }

    public static class SingleState implements AggregatorState {
        private final LongHashTable values;

        private SingleState(BigArrays bigArrays) {
            values = HashImplFactory.newLongHash(bigArrays);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            blocks[offset] = toBlock(driverContext.blockFactory());
        }

        Block toBlock(BlockFactory blockFactory) {
            if (values.size() == 0) {
                return blockFactory.newConstantNullBlock(1);
            }
            if (values.size() == 1) {
                return blockFactory.newConstantFloatBlockWith(Float.intBitsToFloat((int) values.get(0)), 1);
            }
            try (FloatBlock.Builder builder = blockFactory.newFloatBlockBuilder((int) values.size())) {
                builder.beginPositionEntry();
                for (int id = 0; id < values.size(); id++) {
                    builder.appendFloat(Float.intBitsToFloat((int) values.get(id)));
                }
                builder.endPositionEntry();
                return builder.build();
            }
        }

        @Override
        public void close() {
            values.close();
        }
    }

    /**
     * State for a grouped {@code VALUES} aggregation. This implementation
     * emphasizes collect-time performance over result rendering performance.
     * The first value in each group is collected in the {@code firstValues}
     * array, and subsequent values for each group are collected in {@code nextValues}.
     */
    public static class GroupingState implements GroupingAggregatorState {
        private final BlockFactory blockFactory;
        FloatArray firstValues;
        private BitArray seen;
        private int maxGroupId = -1;
        private final ValuesNextLong nextValues;

        private GroupingState(DriverContext driverContext) {
            this.blockFactory = driverContext.blockFactory();
            boolean success = false;
            try {
                this.firstValues = driverContext.bigArrays().newFloatArray(1, false);
                this.nextValues = new ValuesNextLong(driverContext.blockFactory());
                success = true;
            } finally {
                if (success == false) {
                    this.close();
                }
            }
        }

        void addValue(int groupId, float v) {
            if (groupId > maxGroupId) {
                firstValues = blockFactory.bigArrays().grow(firstValues, groupId + 1);
                firstValues.set(groupId, v);
                // We start in untracked mode, assuming every group has a value as an optimization to avoid allocating
                // and updating the seen bitset. However, once some groups don't have values, we initialize the seen bitset,
                // fill the groups that have values, and begin tracking incoming groups.
                if (seen == null && groupId > maxGroupId + 1) {
                    seen = new BitArray(groupId + 1, blockFactory.bigArrays());
                    seen.fill(0, maxGroupId + 1, true);
                }
                trackGroupId(groupId);
                maxGroupId = groupId;
            } else if (hasValue(groupId) == false) {
                firstValues.set(groupId, v);
                trackGroupId(groupId);
            } else if (firstValues.get(groupId) != v) {
                nextValues.add(groupId, v);
            }
        }

        @Override
        public void enableGroupIdTracking(SeenGroupIds seen) {
            // we track the seen values manually
        }

        private void trackGroupId(int groupId) {
            if (seen != null) {
                seen.set(groupId);
            }
        }

        /**
         * Returns true if the group has a value in firstValues; having a value in nextValues is optional.
         * Returns false if the group does not have values in either firstValues or nextValues.
         */
        private boolean hasValue(int groupId) {
            return seen == null || seen.get(groupId);
        }

        /**
         * Builds a {@link Block} with the unique values collected for the {@code #selected}
         * groups. This is the implementation of the final and intermediate results of the agg.
         */
        GroupingAggregatorFunction.PreparedForEvaluation prepareForEmitting(BlockFactory blockFactory, IntVector selected) {
            return new PreparedForEmitting(selected, blockFactory);
        }

        private class PreparedForEmitting implements GroupingAggregatorFunction.PreparedForEvaluation {
            private final BlockFactory blockFactory;
            private final ValuesNextPreparedForEmitting next;

            PreparedForEmitting(IntVector selected, BlockFactory blockFactory) {
                this.blockFactory = blockFactory;
                this.next = nextValues.prepareForEmitting(blockFactory, selected);
            }

            @Override
            public void evaluate(Block[] blocks, int offset, IntVector selectedInPage) {
                blocks[offset] = buildOutputBlock(blockFactory, selectedInPage, next);
            }

            @Override
            public void close() {
                next.close();
            }
        }

        Block buildOutputBlock(BlockFactory blockFactory, IntVector selected, ValuesNextPreparedForEmitting next) {
            /*
             * Insert the ids in order.
             */
            try (FloatBlock.Builder builder = blockFactory.newFloatBlockBuilder(selected.getPositionCount())) {
                for (int s = 0; s < selected.getPositionCount(); s++) {
                    int group = selected.getInt(s);
                    if (group > maxGroupId || hasValue(group) == false) {
                        builder.appendNull();
                        continue;
                    }
                    float firstValue = firstValues.get(group);
                    final int nextValuesStart = next.nextValuesStart(group);
                    final int nextValuesEnd = next.nextValuesEnd(group);
                    if (nextValuesEnd == nextValuesStart) {
                        builder.appendFloat(firstValue);
                    } else {
                        builder.beginPositionEntry();
                        builder.appendFloat(firstValue);
                        // append values from the nextValues
                        for (int i = nextValuesStart; i < nextValuesEnd; i++) {
                            builder.appendFloat(nextValues.getFloat(next, i));
                        }
                        builder.endPositionEntry();
                    }
                }
                return builder.build();
            }
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(seen, firstValues, nextValues);
        }
    }
}
