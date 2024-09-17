/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

$if(BytesRef)$
import org.apache.lucene.util.BytesRef;
$endif$
import org.elasticsearch.common.util.BigArrays;
$if(BytesRef)$
import org.elasticsearch.common.util.BytesRefHash;
$else$
import org.elasticsearch.common.util.LongHash;
$endif$
$if(long||double||BytesRef)$
import org.elasticsearch.common.util.LongLongHash;
$endif$
$if(BytesRef)$
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
$endif$
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
$if(int||double||float||BytesRef)$
import org.elasticsearch.compute.data.$Type$Block;
$endif$
import org.elasticsearch.compute.data.IntVector;
$if(long)$
import org.elasticsearch.compute.data.LongBlock;
$endif$
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasable;
$if(BytesRef)$
import org.elasticsearch.core.Releasables;

$else$

$endif$
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

    public static GroupingState initGrouping(BigArrays bigArrays) {
        return new GroupingState(bigArrays);
    }

    public static void combine(GroupingState state, int groupId, $type$ v) {
$if(long)$
        state.values.add(groupId, v);
$elseif(double)$
        state.values.add(groupId, Double.doubleToLongBits(v));
$elseif(BytesRef)$
        state.values.add(groupId, BlockHash.hashOrdToGroup(state.bytes.add(v)));
$elseif(int)$
        /*
         * Encode the groupId and value into a single long -
         * the top 32 bits for the group, the bottom 32 for the value.
         */
        state.values.add((((long) groupId) << Integer.SIZE) | (v & 0xFFFFFFFFL));
$elseif(float)$
        /*
         * Encode the groupId and value into a single long -
         * the top 32 bits for the group, the bottom 32 for the value.
         */
        state.values.add((((long) groupId) << Float.SIZE) | (Float.floatToIntBits(v) & 0xFFFFFFFFL));
$endif$
    }

    public static void combineIntermediate(GroupingState state, int groupId, $Type$Block values, int valuesPosition) {
$if(BytesRef)$
        BytesRef scratch = new BytesRef();
$endif$
        int start = values.getFirstValueIndex(valuesPosition);
        int end = start + values.getValueCount(valuesPosition);
        for (int i = start; i < end; i++) {
$if(BytesRef)$
            combine(state, groupId, values.getBytesRef(i, scratch));
$else$
            combine(state, groupId, values.get$Type$(i));
$endif$
        }
    }

    public static void combineStates(GroupingState current, int currentGroupId, GroupingState state, int statePosition) {
$if(BytesRef)$
        BytesRef scratch = new BytesRef();
$endif$
        for (int id = 0; id < state.values.size(); id++) {
$if(long||BytesRef)$
            if (state.values.getKey1(id) == statePosition) {
                long value = state.values.getKey2(id);
$elseif(double)$
            if (state.values.getKey1(id) == statePosition) {
                double value = Double.longBitsToDouble(state.values.getKey2(id));
$elseif(int)$
            long both = state.values.get(id);
            int group = (int) (both >>> Integer.SIZE);
            if (group == statePosition) {
                int value = (int) both;
$elseif(float)$
            long both = state.values.get(id);
            int group = (int) (both >>> Float.SIZE);
            if (group == statePosition) {
                float value = Float.intBitsToFloat((int) both);
$endif$
                combine(current, currentGroupId, $if(BytesRef)$state.bytes.get(value, scratch)$else$value$endif$);
            }
        }
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, DriverContext driverContext) {
        return state.toBlock(driverContext.blockFactory(), selected);
    }

    public static class SingleState implements Releasable {
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

        void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
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
     * State for a grouped {@code VALUES} aggregation. This implementation
     * emphasizes collect-time performance over the performance of rendering
     * results. That's good, but it's a pretty intensive emphasis, requiring
     * an {@code O(n^2)} operation for collection to support a {@code O(1)}
     * collector operation. But at least it's fairly simple.
     */
    public static class GroupingState implements Releasable {
$if(long||double)$
        private final LongLongHash values;

$elseif(BytesRef)$
        private final LongLongHash values;
        private final BytesRefHash bytes;

$elseif(int||float)$
        private final LongHash values;

$endif$
        private GroupingState(BigArrays bigArrays) {
$if(long||double)$
            values = new LongLongHash(1, bigArrays);
$elseif(BytesRef)$
            values = new LongLongHash(1, bigArrays);
            bytes = new BytesRefHash(1, bigArrays);
$elseif(int||float)$
            values = new LongHash(1, bigArrays);
$endif$
        }

        void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            blocks[offset] = toBlock(driverContext.blockFactory(), selected);
        }

        Block toBlock(BlockFactory blockFactory, IntVector selected) {
            if (values.size() == 0) {
                return blockFactory.newConstantNullBlock(selected.getPositionCount());
            }
$if(BytesRef)$
            BytesRef scratch = new BytesRef();
$endif$
            try ($Type$Block.Builder builder = blockFactory.new$Type$BlockBuilder(selected.getPositionCount())) {
                for (int s = 0; s < selected.getPositionCount(); s++) {
                    int selectedGroup = selected.getInt(s);
                    /*
                     * Count can effectively be in three states - 0, 1, many. We use those
                     * states to buffer the first value, so we can avoid calling
                     * beginPositionEntry on single valued fields.
                     */
                    int count = 0;
                    $if(BytesRef)$long$else$$type$$endif$ first = 0;
                    for (int id = 0; id < values.size(); id++) {
$if(long||BytesRef)$
                        if (values.getKey1(id) == selectedGroup) {
                            long value = values.getKey2(id);
$elseif(double)$
                        if (values.getKey1(id) == selectedGroup) {
                            double value = Double.longBitsToDouble(values.getKey2(id));
$elseif(float)$
                        long both = values.get(id);
                        int group = (int) (both >>> Float.SIZE);
                        if (group == selectedGroup) {
                            float value = Float.intBitsToFloat((int) both);
$elseif(int)$
                        long both = values.get(id);
                        int group = (int) (both >>> Integer.SIZE);
                        if (group == selectedGroup) {
                            int value = (int) both;
$endif$
                            switch (count) {
                                case 0 -> first = value;
                                case 1 -> {
                                    builder.beginPositionEntry();
                                    builder.append$Type$($if(BytesRef)$bytes.get(first, scratch)$else$first$endif$);
                                    builder.append$Type$($if(BytesRef)$bytes.get(value, scratch)$else$value$endif$);
                                }
                                default -> builder.append$Type$($if(BytesRef)$bytes.get(value, scratch)$else$value$endif$);
                            }
                            count++;
                        }
                    }
                    switch (count) {
                        case 0 -> builder.appendNull();
                        case 1 -> builder.append$Type$($if(BytesRef)$bytes.get(first, scratch)$else$first$endif$);
                        default -> builder.endPositionEntry();
                    }
                }
                return builder.build();
            }
        }

        void enableGroupIdTracking(SeenGroupIds seen) {
            // we figure out seen values from nulls on the values block
        }

        @Override
        public void close() {
$if(BytesRef)$
            Releasables.closeExpectNoException(values, bytes);
$else$
            values.close();
$endif$
        }
    }
}
