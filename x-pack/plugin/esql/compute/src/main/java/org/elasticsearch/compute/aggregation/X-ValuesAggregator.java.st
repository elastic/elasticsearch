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
import org.elasticsearch.common.util.$if(BytesRef)$Int$else$$Type$$endif$Array;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.HashImplFactory;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.$Type$Block;
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
 * Aggregates field values for $type$.
 * This class is generated. Edit @{code X-ValuesAggregator.java.st} instead
 * of this file.
 */
@Aggregator({ @IntermediateState(name = "values", type = "$TYPE$_BLOCK") })
@GroupingAggregator
class Values$Type$Aggregator {
    public static SingleState initSingle(BigArrays bigArrays) {
        return new SingleState(bigArrays);
    }

    public static void combine(SingleState state, $type$ v) {
$if(float)$
        state.values.add(Float.floatToIntBits(v));
$elseif(double)$
        state.values.add(Double.doubleToLongBits(v));
$else$
        state.values.add(v);
$endif$
    }

    public static void combineIntermediate(SingleState state, $Type$Block values) {
$if(BytesRef)$
        BytesRef scratch = new BytesRef();
$endif$
        int start = values.getFirstValueIndex(0);
        int end = start + values.getValueCount(0);
        for (int i = start; i < end; i++) {
$if(BytesRef)$
            combine(state, values.getBytesRef(i, scratch));
$else$
            combine(state, values.get$Type$(i));
$endif$
        }
    }

    public static Block evaluateFinal(SingleState state, DriverContext driverContext) {
        return state.toBlock(driverContext.blockFactory());
    }

    public static GroupingState initGrouping(DriverContext driverContext) {
        return new GroupingState(driverContext);
    }

$if(BytesRef)$
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
$endif$

    public static void combine(GroupingState state, int groupId, $type$ v) {
        state.addValue(groupId, v);
    }

$if(BytesRef)$
    public static void combineIntermediate(GroupingState state, int positionOffset, IntVector groups, $Type$Block values) {
        ValuesBytesRefAggregators.combineIntermediateInputValues(state, positionOffset, groups, values);
    }

    public static void combineIntermediate(GroupingState state, int positionOffset, IntBlock groups, $Type$Block values) {
        ValuesBytesRefAggregators.combineIntermediateInputValues(state, positionOffset, groups, values);
    }

$else$
    public static void combineIntermediate(GroupingState state, int groupId, $Type$Block values, int valuesPosition) {
        int start = values.getFirstValueIndex(valuesPosition);
        int end = start + values.getValueCount(valuesPosition);
        for (int i = start; i < end; i++) {
            state.addValue(groupId, values.get$Type$(i));
        }
    }
$endif$

    public static Block evaluateFinal(GroupingState state, IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        return state.toBlock(ctx.blockFactory(), selected);
    }

    public static class SingleState implements AggregatorState {
$if(BytesRef)$
        private final BytesRefHash values;

$else$
        private final LongHash values;

$endif$
        private SingleState(BigArrays bigArrays) {
$if(BytesRef)$
            values = new BytesRefHash(1, bigArrays);
$else$
            values = new LongHash(1, bigArrays);
$endif$
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            blocks[offset] = toBlock(driverContext.blockFactory());
        }

        Block toBlock(BlockFactory blockFactory) {
            if (values.size() == 0) {
                return blockFactory.newConstantNullBlock(1);
            }
$if(BytesRef)$
            BytesRef scratch = new BytesRef();
$endif$
            if (values.size() == 1) {
$if(long)$
                return blockFactory.newConstantLongBlockWith(values.get(0), 1);
$elseif(float)$
                return blockFactory.newConstantFloatBlockWith(Float.intBitsToFloat((int) values.get(0)), 1);
$elseif(double)$
                return blockFactory.newConstantDoubleBlockWith(Double.longBitsToDouble(values.get(0)), 1);
$elseif(int)$
                return blockFactory.newConstantIntBlockWith((int) values.get(0), 1);
$elseif(BytesRef)$
                return blockFactory.newConstantBytesRefBlockWith(BytesRef.deepCopyOf(values.get(0, scratch)), 1);
$endif$
            }
            try ($Type$Block.Builder builder = blockFactory.new$Type$BlockBuilder((int) values.size())) {
                builder.beginPositionEntry();
                for (int id = 0; id < values.size(); id++) {
$if(long)$
                    builder.appendLong(values.get(id));
$elseif(float)$
                    builder.appendFloat(Float.intBitsToFloat((int) values.get(id)));
$elseif(double)$
                    builder.appendDouble(Double.longBitsToDouble(values.get(id)));
$elseif(int)$
                    builder.appendInt((int) values.get(id));
$elseif(BytesRef)$
                    builder.appendBytesRef(values.get(id, scratch));
$endif$
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
$if(long||double)$
        private final LongLongHash hashes;
$else$
        private final LongHashTable hashes;
$endif$
        private int[] selectedCounts = null;
        private int[] ids = null;
        private long extraMemoryUsed = 0;

        private NextValues(BlockFactory blockFactory) {
            this.blockFactory = blockFactory;
$if(long||double)$
            this.hashes = new LongLongHash(1, blockFactory.bigArrays());
$else$
            this.hashes = HashImplFactory.newLongHash(blockFactory);
$endif$
        }

        void addValue(int groupId, $if(BytesRef)$int$else$$type$$endif$ v) {
$if(long)$
            hashes.add(groupId, v);
$elseif(double)$
            hashes.add(groupId, Double.doubleToLongBits(v));
$elseif(int||BytesRef)$
            /*
             * Encode the groupId and value into a single long -
             * the top 32 bits for the group, the bottom 32 for the value.
             */
            hashes.add((((long) groupId) << Integer.SIZE) | (v & 0xFFFFFFFFL));
$elseif(float)$
            /*
             * Encode the groupId and value into a single long -
             * the top 32 bits for the group, the bottom 32 for the value.
             */
            hashes.add((((long) groupId) << Float.SIZE) | (Float.floatToIntBits(v) & 0xFFFFFFFFL));
$endif$
        }

        $if(BytesRef)$int$else$$type$$endif$ getValue(int index) {
$if(long)$
            return hashes.getKey2(ids[index]);
$elseif(double)$
            return Double.longBitsToDouble(hashes.getKey2(ids[index]));
$elseif(float)$
            long both = hashes.get(ids[index]);
            return Float.intBitsToFloat((int) (both & 0xFFFFFFFFL));
$elseif(BytesRef||int)$
            long both = hashes.get(ids[index]);
            return (int) (both & 0xFFFFFFFFL);
$endif$
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
$if(long||double)$
                int group = (int) hashes.getKey1(id);
$elseif(float||int||BytesRef)$
                long both = hashes.get(id);
                int group = (int) (both >>> Float.SIZE);
$endif$
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
$if(long||double)$
                int group = (int) hashes.getKey1(id);
$elseif(float||int||BytesRef)$
                long both = hashes.get(id);
                int group = (int) (both >>> Float.SIZE);
$endif$
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
$if(BytesRef)$
        BytesRefHash bytes;
        private IntArray firstValues;
$else$
        $Type$Array firstValues;
        private BitArray seen;
        private int maxGroupId = -1;
$endif$
        private final NextValues nextValues;

        private GroupingState(DriverContext driverContext) {
            this.blockFactory = driverContext.blockFactory();
            boolean success = false;
            try {
$if(BytesRef)$
                this.bytes = new BytesRefHash(1, driverContext.bigArrays());
                this.firstValues = driverContext.bigArrays().newIntArray(1, true);
$else$
                this.firstValues = driverContext.bigArrays().new$Type$Array(1, false);
$endif$
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

$if(BytesRef)$
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

$endif$
        void addValue(int groupId, $type$ v) {
$if(BytesRef)$
            int valueOrdinal = Math.toIntExact(BlockHash.hashOrdToGroup(bytes.add(v)));
            addValueOrdinal(groupId, valueOrdinal);
$else$
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
$endif$
        }

$if(BytesRef)$
        @Override
        public void enableGroupIdTracking(SeenGroupIds seen) {
            // we figure out seen values from firstValues since ordinals are non-negative
        }

$else$
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

$endif$
        /**
         * Builds a {@link Block} with the unique values collected for the {@code #selected}
         * groups. This is the implementation of the final and intermediate results of the agg.
         */
        Block toBlock(BlockFactory blockFactory, IntVector selected) {
            nextValues.prepareForEmitting(selected);
$if(BytesRef)$
            if (OrdinalBytesRefBlock.isDense(firstValues.size() + nextValues.hashes.size(), bytes.size())) {
                return buildOrdinalOutputBlock(blockFactory, selected);
            } else {
                return buildOutputBlock(blockFactory, selected);
            }
$else$
            return buildOutputBlock(blockFactory, selected);
$endif$
        }

        Block buildOutputBlock(BlockFactory blockFactory, IntVector selected) {
            /*
             * Insert the ids in order.
             */
$if(BytesRef)$
            BytesRef scratch = new BytesRef();
$endif$
            final int[] nextValueCounts = nextValues.selectedCounts;
            try ($Type$Block.Builder builder = blockFactory.new$Type$BlockBuilder(selected.getPositionCount())) {
                int nextValuesStart = 0;
                for (int s = 0; s < selected.getPositionCount(); s++) {
                    int group = selected.getInt(s);
$if(BytesRef)$
                    int firstValue = group >= firstValues.size() ? -1 : firstValues.get(group) - 1;
                    if (firstValue < 0) {
                        builder.appendNull();
                        continue;
                    }
$else$
                    if (group > maxGroupId || hasValue(group) == false) {
                        builder.appendNull();
                        continue;
                    }
                    $type$ firstValue = firstValues.get(group);
$endif$
                    final int nextValuesEnd = nextValueCounts != null ? nextValueCounts[group] : nextValuesStart;
                    if (nextValuesEnd == nextValuesStart) {
                        builder.append$Type$($if(BytesRef)$bytes.get(firstValue, scratch)$else$firstValue$endif$);
                    } else {
                        builder.beginPositionEntry();
                        builder.append$Type$($if(BytesRef)$bytes.get(firstValue, scratch)$else$firstValue$endif$);
                        // append values from the nextValues
                        for (int i = nextValuesStart; i < nextValuesEnd; i++) {
                            var nextValue = nextValues.getValue(i);
                            builder.append$Type$($if(BytesRef)$bytes.get(nextValue, scratch)$else$nextValue$endif$);
                        }
                        builder.endPositionEntry();
                        nextValuesStart = nextValuesEnd;
                    }
                }
                return builder.build();
            }
        }

$if(BytesRef)$
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
$endif$

        @Override
        public void close() {
            Releasables.closeExpectNoException($if(BytesRef)$bytes$else$seen$endif$, firstValues, nextValues);
        }
    }
}
