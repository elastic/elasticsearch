/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

// begin generated imports
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.common.util.LongLongHash;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
// end generated imports

/**
 * Aggregates field values for double.
 * This class is generated. Edit @{code X-SparklineAggregator.java.st} instead
 * of this file.
 */
@Aggregator({ @IntermediateState(name = "values", type = "DOUBLE_BLOCK"), @IntermediateState(name = "timestamps", type = "LONG_BLOCK") })
@GroupingAggregator
class SparklineDoubleAggregator {
    public static SingleState initSingle(BigArrays bigArrays) {
        return new SingleState(bigArrays);
    }

    public static void combine(SingleState state, double value, long timestamp) {
        state.values.add(Tuple.tuple(timestamp, value));
    }

    public static void combineIntermediate(SingleState state, DoubleBlock values, LongBlock timestamps) {
        int start = values.getFirstValueIndex(0);
        int end = start + values.getValueCount(0);
        for (int i = start; i < end; i++) {
            combine(state, values.getDouble(i), timestamps.getLong(i));
        }
    }

    public static Block evaluateFinal(SingleState state, DriverContext driverContext) {
        return state.toFinal(driverContext.blockFactory());
    }

    public static GroupingState initGrouping(DriverContext driverContext) {
        return new GroupingState(driverContext);
    }

    public static void combine(GroupingState state, int groupId, double value, long timestamp) {
        state.addValue(groupId, value, timestamp);
    }

    public static void combineIntermediate(GroupingState state, int groupId, DoubleBlock values, LongBlock timestamps, int valuesPosition) {
        int start = values.getFirstValueIndex(valuesPosition);
        int end = start + values.getValueCount(valuesPosition);
        for (int i = start; i < end; i++) {
            combine(state, groupId, values.getDouble(i), timestamps.getLong(i));
        }
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        return state.toFinal(ctx.blockFactory(), selected);
    }

    public static class SingleState implements AggregatorState {
        private final List<Tuple<Long, Double>> values;

        private SingleState(BigArrays bigArrays) {
            values = new ArrayList<>();
        }

        // TODO: Why does it have to output all the blocks? Where are these blocks used?
        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            BlockFactory blockFactory = driverContext.blockFactory();
            try (
                DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder(values.size());
                LongBlock.Builder timestampsBuilder = blockFactory.newLongBlockBuilder(values.size());
            ) {
                builder.beginPositionEntry();
                timestampsBuilder.beginPositionEntry();
                for (int id = 0; id < values.size(); id++) {
                    builder.appendDouble(values.get(id).v2());
                    timestampsBuilder.appendLong(values.get(id).v1());
                }
                builder.endPositionEntry();
                timestampsBuilder.endPositionEntry();
                blocks[offset] = builder.build();
                blocks[offset + 1] = timestampsBuilder.build();
            }
        }

        Block toFinal(BlockFactory blockFactory) {
            values.sort(Comparator.comparingLong(Tuple::v1));
            try (DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder(values.size())) {
                builder.beginPositionEntry();
                for (int id = 0; id < values.size(); id++) {
                    builder.appendDouble(values.get(id).v2());
                }
                builder.endPositionEntry();
                return builder.build();
            }
        }

        @Override
        public void close() {}
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

        void addValue(int groupId, double v) {
            hashes.add(groupId, Double.doubleToLongBits(v));
        }

        double getValue(int index) {
            return Double.longBitsToDouble(hashes.getKey2(ids[index]));
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
     * State for a grouped {@code SPARKLINE} aggregation. This implementation
     * emphasizes collect-time performance over result rendering performance.
     * The first value in each group is collected in the {@code firstValues}
     * array, and subsequent values for each group are collected in {@code nextValues}.
     */
    public static class GroupingState implements GroupingAggregatorState {
        private final BlockFactory blockFactory;
        DoubleArray firstValues;
        private BitArray seen;
        private int maxGroupId = -1;
        private final NextValues nextValues;
        private final Map<Integer, List<Tuple<Long, Double>>> values;

        private GroupingState(DriverContext driverContext) {
            this.blockFactory = driverContext.blockFactory();
            boolean success = false;
            try {
                this.firstValues = driverContext.bigArrays().newDoubleArray(1, false);
                this.nextValues = new NextValues(driverContext.blockFactory());
                success = true;
            } finally {
                if (success == false) {
                    this.close();
                }
            }
            values = new HashMap<>();
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            nextValues.prepareForEmitting(selected);
            /*
             * Insert the ids in order.
             */
            final int[] nextValueCounts = nextValues.selectedCounts;
            try (
                DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder(selected.getPositionCount());
                LongBlock.Builder timestampsBuilder = blockFactory.newLongBlockBuilder(selected.getPositionCount())
            ) {
                int nextValuesStart = 0;
                for (int s = 0; s < selected.getPositionCount(); s++) {
                    int group = selected.getInt(s);
                    if (group > maxGroupId || hasValue(group) == false) {
                        builder.appendNull();
                        timestampsBuilder.appendNull();
                        continue;
                    }

                    builder.beginPositionEntry();
                    timestampsBuilder.beginPositionEntry();
                    List<Tuple<Long, Double>> valuesInGroup = values.get(group);
                    for (int id = 0; id < valuesInGroup.size(); id++) {
                        builder.appendDouble(valuesInGroup.get(id).v2());
                        timestampsBuilder.appendLong(valuesInGroup.get(id).v1());
                    }
                    builder.endPositionEntry();
                    timestampsBuilder.endPositionEntry();
                }
                blocks[offset] = builder.build();
                blocks[offset + 1] = timestampsBuilder.build();
            }
        }

        void addValue(int groupId, double v, long t) {
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
            List<Tuple<Long, Double>> valuesInGroup = values.computeIfAbsent(groupId, g -> new ArrayList<>());
            valuesInGroup.add(Tuple.tuple(t, v));
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
        Block toFinal(BlockFactory blockFactory, IntVector selected) {
            nextValues.prepareForEmitting(selected);
            /*
             * Insert the ids in order.
             */
            final int[] nextValueCounts = nextValues.selectedCounts;
            try (DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder(selected.getPositionCount())) {
                int nextValuesStart = 0;
                for (int s = 0; s < selected.getPositionCount(); s++) {
                    int group = selected.getInt(s);
                    if (group > maxGroupId || hasValue(group) == false) {
                        builder.appendNull();
                        continue;
                    }
                    double firstValue = firstValues.get(group);
                    final int nextValuesEnd = nextValueCounts != null ? nextValueCounts[group] : nextValuesStart;

                    List<Tuple<Long, Double>> valuesInGroup = values.get(group);
                    valuesInGroup.sort(Comparator.comparingLong(Tuple::v1));
                    builder.beginPositionEntry();
                    for (int id = 0; id < valuesInGroup.size(); id++) {
                        builder.appendDouble(valuesInGroup.get(id).v2());
                    }
                    builder.endPositionEntry();
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
