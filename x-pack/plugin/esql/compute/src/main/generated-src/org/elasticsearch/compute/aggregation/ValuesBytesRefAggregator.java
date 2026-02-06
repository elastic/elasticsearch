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

    public static Block evaluateFinal(GroupingState state, IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        return state.toBlock(ctx.blockFactory(), selected);
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
     * Values after the first in each group are collected in a hash, keyed by the pair of groupId and value.
     * When emitting the output, we need to iterate the hash one group at a time to build the output block,
     * which would require O(N^2). To avoid this, we compute the counts for each group and remap the hash id
     * to an array, allowing us to build the output in O(N) instead.
     */
    private static class NextValues implements Releasable {
        private final BlockFactory blockFactory;
        private final LongHashTable hashes;
        private int[] selectedCounts = null;
        private int[] ids = null;
        private long extraMemoryUsed = 0;

        private NextValues(BlockFactory blockFactory) {
            this.blockFactory = blockFactory;
            this.hashes = HashImplFactory.newLongHash(blockFactory);
        }

        void addValue(int groupId, int v) {
            /*
             * Encode the groupId and value into a single long -
             * the top 32 bits for the group, the bottom 32 for the value.
             */
            hashes.add((((long) groupId) << Integer.SIZE) | (v & 0xFFFFFFFFL));
        }

        int getValue(int index) {
            long both = hashes.get(ids[index]);
            return (int) (both & 0xFFFFFFFFL);
        }

        private void reserveBytesForIntArray(long numElements) {
            long adjust = RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + numElements * Integer.BYTES);
            blockFactory.adjustBreaker(adjust);
            extraMemoryUsed += adjust;
        }

        private void prepareForEmitting(IntVector selected) {
            if (hashes.size() == 0) {
                return;
            }
            /*
             * Get a count of all groups less than the maximum selected group. Count
             * *downwards* so that we can flip the sign on all of the actually selected
             * groups. Negative values in this array are always unselected groups.
             */
            int selectedCountsLen = selected.max() + 1;
            reserveBytesForIntArray(selectedCountsLen);
            this.selectedCounts = new int[selectedCountsLen];
            for (int id = 0; id < hashes.size(); id++) {
                long both = hashes.get(id);
                int group = (int) (both >>> Float.SIZE);
                if (group < selectedCounts.length) {
                    selectedCounts[group]--;
                }
            }

            /*
             * Total the selected groups and turn the counts into the start index into a sort-of
             * off-by-one running count. It's really the number of values that have been inserted
             * into the results before starting on this group. Unselected groups will still
             * have negative counts.
             *
             * For example, if
             * | Group | Value Count | Selected |
             * |-------|-------------|----------|
             * |     0 | 3           | <-       |
             * |     1 | 1           | <-       |
             * |     2 | 2           |          |
             * |     3 | 1           | <-       |
             * |     4 | 4           | <-       |
             *
             * Then the total is 9 and the counts array will contain 0, 3, -2, 4, 5
             */
            int total = 0;
            for (int s = 0; s < selected.getPositionCount(); s++) {
                int group = selected.getInt(s);
                int count = -selectedCounts[group];
                selectedCounts[group] = total;
                total += count;
            }

            /*
             * Build a list of ids to insert in order *and* convert the running
             * count in selectedCounts[group] into the end index (exclusive) in
             * ids for each group.
             * Here we use the negative counts to signal that a group hasn't been
             * selected and the id containing values for that group is ignored.
             *
             * For example, if
             * | Group | Value Count | Selected |
             * |-------|-------------|----------|
             * |     0 | 3           | <-       |
             * |     1 | 1           | <-       |
             * |     2 | 2           |          |
             * |     3 | 1           | <-       |
             * |     4 | 4           | <-       |
             *
             * Then the total is 9 and the counts array will start with 0, 3, -2, 4, 5.
             * The counts will end with 3, 4, -2, 5, 9.
             */
            reserveBytesForIntArray(total);

            this.ids = new int[total];
            for (int id = 0; id < hashes.size(); id++) {
                long both = hashes.get(id);
                int group = (int) (both >>> Float.SIZE);
                ids[selectedCounts[group]++] = id;
            }
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(hashes, () -> blockFactory.adjustBreaker(-extraMemoryUsed));
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
        private final NextValues nextValues;

        private GroupingState(DriverContext driverContext) {
            this.blockFactory = driverContext.blockFactory();
            boolean success = false;
            try {
                this.bytes = new BytesRefHash(1, driverContext.bigArrays());
                this.firstValues = driverContext.bigArrays().newIntArray(1, true);
                this.nextValues = new NextValues(driverContext.blockFactory());
                success = true;
            } finally {
                if (success == false) {
                    this.close();
                }
            }
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            blocks[offset] = toBlock(driverContext.blockFactory(), selected);
        }

        void addValueOrdinal(int groupId, int valueOrdinal) {
            if (groupId < firstValues.size()) {
                int current = firstValues.get(groupId) - 1;
                if (current < 0) {
                    firstValues.set(groupId, valueOrdinal + 1);
                } else if (current != valueOrdinal) {
                    nextValues.addValue(groupId, valueOrdinal);
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
        Block toBlock(BlockFactory blockFactory, IntVector selected) {
            nextValues.prepareForEmitting(selected);
            if (OrdinalBytesRefBlock.isDense(firstValues.size() + nextValues.hashes.size(), bytes.size())) {
                return buildOrdinalOutputBlock(blockFactory, selected);
            } else {
                return buildOutputBlock(blockFactory, selected);
            }
        }

        Block buildOutputBlock(BlockFactory blockFactory, IntVector selected) {
            /*
             * Insert the ids in order.
             */
            BytesRef scratch = new BytesRef();
            final int[] nextValueCounts = nextValues.selectedCounts;
            try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(selected.getPositionCount())) {
                int nextValuesStart = 0;
                for (int s = 0; s < selected.getPositionCount(); s++) {
                    int group = selected.getInt(s);
                    int firstValue = group >= firstValues.size() ? -1 : firstValues.get(group) - 1;
                    if (firstValue < 0) {
                        builder.appendNull();
                        continue;
                    }
                    final int nextValuesEnd = nextValueCounts != null ? nextValueCounts[group] : nextValuesStart;
                    if (nextValuesEnd == nextValuesStart) {
                        builder.appendBytesRef(bytes.get(firstValue, scratch));
                    } else {
                        builder.beginPositionEntry();
                        builder.appendBytesRef(bytes.get(firstValue, scratch));
                        // append values from the nextValues
                        for (int i = nextValuesStart; i < nextValuesEnd; i++) {
                            var nextValue = nextValues.getValue(i);
                            builder.appendBytesRef(bytes.get(nextValue, scratch));
                        }
                        builder.endPositionEntry();
                        nextValuesStart = nextValuesEnd;
                    }
                }
                return builder.build();
            }
        }

        Block buildOrdinalOutputBlock(BlockFactory blockFactory, IntVector selected) {
            BytesRefVector dict = null;
            IntBlock ordinals = null;
            BytesRefBlock result = null;
            var dictArray = bytes.getBytesRefs();
            dictArray.incRef();
            int estimateSize = Math.toIntExact(firstValues.size() + nextValues.hashes.size());
            final int[] nextValueCounts = nextValues.selectedCounts;
            try (var builder = blockFactory.newIntBlockBuilder(estimateSize)) {
                int nextValuesStart = 0;
                for (int s = 0; s < selected.getPositionCount(); s++) {
                    final int group = selected.getInt(s);
                    final int firstValue = group >= firstValues.size() ? -1 : firstValues.get(group) - 1;
                    if (firstValue < 0) {
                        builder.appendNull();
                        continue;
                    }
                    final int nextValuesEnd = nextValueCounts != null ? nextValueCounts[group] : nextValuesStart;
                    if (nextValuesEnd == nextValuesStart) {
                        builder.appendInt(firstValue);
                    } else {
                        builder.beginPositionEntry();
                        builder.appendInt(firstValue);
                        for (int i = nextValuesStart; i < nextValuesEnd; i++) {
                            builder.appendInt(nextValues.getValue(i));
                        }
                        builder.endPositionEntry();
                    }
                    nextValuesStart = nextValuesEnd;
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
