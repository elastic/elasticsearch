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
import org.elasticsearch.common.util.LongLongHash;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.swisstable.LongSwissTable;
// end generated imports

/**
 * Aggregates field values for long.
 * This class is generated. Edit @{code X-ValuesAggregator.java.st} instead
 * of this file.
 */
@Aggregator({ @IntermediateState(name = "values", type = "LONG_BLOCK") })
@GroupingAggregator
class ValuesLongAggregator {
    public static SingleState initSingle(BigArrays bigArrays) {
        return new SingleState(bigArrays);
    }

    public static void combine(SingleState state, long v) {
        state.values.add(v);
    }

    public static void combineIntermediate(SingleState state, LongBlock values) {
        int start = values.getFirstValueIndex(0);
        int end = start + values.getValueCount(0);
        for (int i = start; i < end; i++) {
            combine(state, values.getLong(i));
        }
    }

    public static Block evaluateFinal(SingleState state, DriverContext driverContext) {
        return state.toBlock(driverContext.blockFactory());
    }

    public static GroupingState initGrouping(DriverContext driverContext) {
        return new GroupingState(driverContext);
    }

    public static void combine(GroupingState state, int groupId, long v) {
        state.addValue(groupId, v);
    }

    public static void combineIntermediate(GroupingState state, int groupId, LongBlock values, int valuesPosition) {
        int start = values.getFirstValueIndex(valuesPosition);
        int end = start + values.getValueCount(valuesPosition);
        for (int i = start; i < end; i++) {
            state.addValue(groupId, values.getLong(i));
        }
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        return state.toBlock(ctx.blockFactory(), selected);
    }

    public static class SingleState implements AggregatorState {
        private final LongHash values;

        private SingleState(BigArrays bigArrays) {
            values = new LongHash(1, bigArrays);
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
                return blockFactory.newConstantLongBlockWith(values.get(0), 1);
            }
            try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder((int) values.size())) {
                builder.beginPositionEntry();
                for (int id = 0; id < values.size(); id++) {
                    builder.appendLong(values.get(id));
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
        private final LongLongHash hashes;
        private int[] selectedCounts = null;
        private int[] ids = null;
        private long extraMemoryUsed = 0;

        private NextValues(BlockFactory blockFactory) {
            this.blockFactory = blockFactory;
            this.hashes = new LongLongHash(1, blockFactory.bigArrays());
        }

        void addValue(int groupId, long v) {
            hashes.add(groupId, v);
        }

        long getValue(int index) {
            return hashes.getKey2(ids[index]);
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
                int group = (int) hashes.getKey1(id);
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
                int group = (int) hashes.getKey1(id);
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
        LongArray firstValues;
        private BitArray seen;
        private int maxGroupId = -1;
        private final NextValues nextValues;

        private GroupingState(DriverContext driverContext) {
            this.blockFactory = driverContext.blockFactory();
            boolean success = false;
            try {
                this.firstValues = driverContext.bigArrays().newLongArray(1, false);
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

        void addValue(int groupId, long v) {
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
                nextValues.addValue(groupId, v);
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
        Block toBlock(BlockFactory blockFactory, IntVector selected) {
            nextValues.prepareForEmitting(selected);
            return buildOutputBlock(blockFactory, selected);
        }

        Block buildOutputBlock(BlockFactory blockFactory, IntVector selected) {
            /*
             * Insert the ids in order.
             */
            final int[] nextValueCounts = nextValues.selectedCounts;
            try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(selected.getPositionCount())) {
                int nextValuesStart = 0;
                for (int s = 0; s < selected.getPositionCount(); s++) {
                    int group = selected.getInt(s);
                    if (group > maxGroupId || hasValue(group) == false) {
                        builder.appendNull();
                        continue;
                    }
                    long firstValue = firstValues.get(group);
                    final int nextValuesEnd = nextValueCounts != null ? nextValueCounts[group] : nextValuesStart;
                    if (nextValuesEnd == nextValuesStart) {
                        builder.appendLong(firstValue);
                    } else {
                        builder.beginPositionEntry();
                        builder.appendLong(firstValue);
                        // append values from the nextValues
                        for (int i = nextValuesStart; i < nextValuesEnd; i++) {
                            var nextValue = nextValues.getValue(i);
                            builder.appendLong(nextValue);
                        }
                        builder.endPositionEntry();
                        nextValuesStart = nextValuesEnd;
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
