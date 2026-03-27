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
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.common.util.LongHashTable;
import org.elasticsearch.common.util.LongLongHash;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.HashImplFactory;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
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
 * Aggregates field values for BytesRef.
 * This class is generated. Edit @{code X-ValuesAggregator.java.st} instead
 * of this file.
 */
@Aggregator({ @IntermediateState(name = "values", type = "BYTES_REF_BLOCK") })
@GroupingAggregator
class ValuesBytesRefAggregator {
    public static SingleState initSingle(BigArrays bigArrays) {
        return new SingleState(bigArrays);
    }

    public static void combine(SingleState state, BytesRef v) {
        state.values.add(v);
    }

    public static void combineIntermediate(SingleState state, BytesRefBlock values) {
        BytesRef scratch = new BytesRef();
        int start = values.getFirstValueIndex(0);
        int end = start + values.getValueCount(0);
        for (int i = start; i < end; i++) {
            combine(state, values.getBytesRef(i, scratch));
        }
    }

    public static Block evaluateFinal(SingleState state, DriverContext driverContext) {
        return state.toBlock(driverContext.blockFactory());
    }

    public static GroupingState initGrouping(DriverContext driverContext) {
        return new GroupingState(driverContext);
    }

    public static GroupingAggregatorFunction.AddInput wrapAddInput(
        GroupingAggregatorFunction.AddInput delegate,
        GroupingState state,
        BytesRefBlock values
    ) {
        return ValuesBytesRefAggregators.wrapAddInput(delegate, state, values);
    }

    public static GroupingAggregatorFunction.AddInput wrapAddInput(
        GroupingAggregatorFunction.AddInput delegate,
        GroupingState state,
        BytesRefVector values
    ) {
        return ValuesBytesRefAggregators.wrapAddInput(delegate, state, values);
    }

    public static void combine(GroupingState state, int groupId, BytesRef v) {
        state.addValue(groupId, v);
    }

    public static void combineIntermediate(GroupingState state, int positionOffset, IntVector groups, BytesRefBlock values) {
        ValuesBytesRefAggregators.combineIntermediateInputValues(state, positionOffset, groups, values);
    }

    public static void combineIntermediate(GroupingState state, int positionOffset, IntBlock groups, BytesRefBlock values) {
        ValuesBytesRefAggregators.combineIntermediateInputValues(state, positionOffset, groups, values);
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
        private final BytesRefHash values;

        private SingleState(BigArrays bigArrays) {
            values = new BytesRefHash(1, bigArrays);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            blocks[offset] = toBlock(driverContext.blockFactory());
        }

        Block toBlock(BlockFactory blockFactory) {
            if (values.size() == 0) {
                return blockFactory.newConstantNullBlock(1);
            }
            BytesRef scratch = new BytesRef();
            if (values.size() == 1) {
                return blockFactory.newConstantBytesRefBlockWith(BytesRef.deepCopyOf(values.get(0, scratch)), 1);
            }
            try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder((int) values.size())) {
                builder.beginPositionEntry();
                for (int id = 0; id < values.size(); id++) {
                    builder.appendBytesRef(values.get(id, scratch));
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
        BytesRefHash bytes;
        private IntArray firstValues;
        private final ValuesNextLong nextValues;

        private GroupingState(DriverContext driverContext) {
            this.blockFactory = driverContext.blockFactory();
            boolean success = false;
            try {
                this.bytes = new BytesRefHash(1, driverContext.bigArrays());
                this.firstValues = driverContext.bigArrays().newIntArray(1, true);
                this.nextValues = new ValuesNextLong(driverContext.blockFactory());
                success = true;
            } finally {
                if (success == false) {
                    this.close();
                }
            }
        }

        void addValueOrdinal(int groupId, int valueOrdinal) {
            if (groupId < firstValues.size()) {
                int current = firstValues.get(groupId) - 1;
                if (current < 0) {
                    firstValues.set(groupId, valueOrdinal + 1);
                } else if (current != valueOrdinal) {
                    nextValues.add(groupId, valueOrdinal);
                }
            } else {
                firstValues = blockFactory.bigArrays().grow(firstValues, groupId + 1);
                firstValues.set(groupId, valueOrdinal + 1);
            }
        }

        void addValue(int groupId, BytesRef v) {
            int valueOrdinal = Math.toIntExact(BlockHash.hashOrdToGroup(bytes.add(v)));
            addValueOrdinal(groupId, valueOrdinal);
        }

        @Override
        public void enableGroupIdTracking(SeenGroupIds seen) {
            // we figure out seen values from firstValues since ordinals are non-negative
        }

        /**
         * Builds a {@link Block} with the unique values collected for the {@code #selected}
         * groups. This is the implementation of the final and intermediate results of the agg.
         */
        GroupingAggregatorFunction.PreparedForEvaluation prepareForEmitting(BlockFactory blockFactory, IntVector selected) {
            return new PreparedForEmitting(selected, blockFactory);
        }

        private class PreparedForEmitting implements GroupingAggregatorFunction.PreparedForEvaluation {
            private final IntVector selected;
            private final BlockFactory blockFactory;
            private final ValuesNextPreparedForEmitting next;

            PreparedForEmitting(IntVector selected, BlockFactory blockFactory) {
                this.selected = selected;
                this.blockFactory = blockFactory;
                this.next = nextValues.prepareForEmitting(blockFactory, selected);
            }

            @Override
            public void evaluate(Block[] blocks, int offset, IntVector selectedInPage) {
                /*
                 * When selected == selectedInPage we're building one block across the entire
                 * selected region. So it's safe to run the ordinals optimization which *takes*
                 * the ordinals.
                 */
                if (selected == selectedInPage && OrdinalBytesRefBlock.isDense(firstValues.size() + nextValues.size(), bytes.size())) {
                    blocks[offset] = buildOrdinalOutputBlock(blockFactory, selectedInPage, next);
                } else {
                    blocks[offset] = buildOutputBlock(blockFactory, selectedInPage, next);
                }
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
            BytesRef scratch = new BytesRef();
            try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(selected.getPositionCount())) {
                for (int s = 0; s < selected.getPositionCount(); s++) {
                    int group = selected.getInt(s);
                    int firstValue = group >= firstValues.size() ? -1 : firstValues.get(group) - 1;
                    if (firstValue < 0) {
                        builder.appendNull();
                        continue;
                    }
                    final int nextValuesStart = next.nextValuesStart(group);
                    final int nextValuesEnd = next.nextValuesEnd(group);
                    if (nextValuesEnd == nextValuesStart) {
                        builder.appendBytesRef(bytes.get(firstValue, scratch));
                    } else {
                        builder.beginPositionEntry();
                        builder.appendBytesRef(bytes.get(firstValue, scratch));
                        // append values from the nextValues
                        for (int i = nextValuesStart; i < nextValuesEnd; i++) {
                            var nextValue = nextValues.getInt(next, i);
                            builder.appendBytesRef(bytes.get(nextValue, scratch));
                        }
                        builder.endPositionEntry();
                    }
                }
                return builder.build();
            }
        }

        Block buildOrdinalOutputBlock(BlockFactory blockFactory, IntVector selected, ValuesNextPreparedForEmitting next) {
            BytesRefVector dict = null;
            IntBlock ordinals = null;
            BytesRefBlock result = null;
            var dictArray = bytes.getBytesRefs();
            dictArray.incRef();
            int estimateSize = Math.toIntExact(firstValues.size() + nextValues.size());
            try (var builder = blockFactory.newIntBlockBuilder(estimateSize)) {
                for (int s = 0; s < selected.getPositionCount(); s++) {
                    final int group = selected.getInt(s);
                    final int firstValue = group >= firstValues.size() ? -1 : firstValues.get(group) - 1;
                    if (firstValue < 0) {
                        builder.appendNull();
                        continue;
                    }
                    final int nextValuesStart = next.nextValuesStart(group);
                    final int nextValuesEnd = next.nextValuesEnd(group);
                    if (nextValuesEnd == nextValuesStart) {
                        builder.appendInt(firstValue);
                    } else {
                        builder.beginPositionEntry();
                        builder.appendInt(firstValue);
                        for (int i = nextValuesStart; i < nextValuesEnd; i++) {
                            builder.appendInt(nextValues.getInt(next, i));
                        }
                        builder.endPositionEntry();
                    }
                }
                ordinals = builder.build();
                dict = blockFactory.newBytesRefArrayVector(dictArray, Math.toIntExact(dictArray.size()));
                dictArray = null; // transfer ownership to dict
                result = new OrdinalBytesRefBlock(ordinals, dict);
                return result;
            } finally {
                if (result == null) {
                    Releasables.close(dictArray, dict, ordinals);
                }
            }
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(bytes, firstValues, nextValues);
        }
    }
}
