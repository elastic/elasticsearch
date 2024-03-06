/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongHash;
$if(long||double)$
import org.elasticsearch.common.util.LongLongHash;
$endif$
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
$if(int)$
import org.elasticsearch.compute.data.IntBlock;
$elseif(double)$
import org.elasticsearch.compute.data.DoubleBlock;
$endif$
import org.elasticsearch.compute.data.IntVector;
$if(long)$
import org.elasticsearch.compute.data.LongBlock;
$endif$
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasable;

@Aggregator({ @IntermediateState(name = "values", type = "$TYPE$_BLOCK") })
@GroupingAggregator
class Values$Type$Aggregator {

    public static SingleState initSingle(BigArrays bigArrays) {
        return new SingleState(bigArrays);
    }

    public static void combine(SingleState state, $type$ v) {
$if(double)$
        state.values.add(Double.doubleToLongBits(v));
$else$
        state.values.add(v);
$endif$
    }

    public static void combineIntermediate(SingleState state, $Type$Block block) {
        int start = block.getFirstValueIndex(0);
        int end = start + block.getValueCount(0);
        for (int i = start; i < end; i++) {
            combine(state, block.get$Type$(i));
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
$elseif(int)$
        state.values.add((((long) groupId) << Integer.SIZE) | (v & 0xFFFFFFFFL));
$endif$
    }

    public static void combineIntermediate(GroupingState state, int groupId, $Type$Block values, int valuesPosition) {
        int start = values.getFirstValueIndex(valuesPosition);
        int end = start + values.getValueCount(valuesPosition);
        for (int i = start; i < end; i++) {
            combine(state, groupId, values.get$Type$(i));
        }
    }

    public static void combineStates(GroupingState current, int currentGroupId, GroupingState state, int statePosition) {
        throw new UnsupportedOperationException();
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, DriverContext driverContext) {
        return state.toBlock(driverContext.blockFactory(), selected);
    }

    public static class SingleState implements Releasable {
        private final LongHash values;

        private SingleState(BigArrays bigArrays) {
            values = new LongHash(1, bigArrays);
        }

        void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            blocks[offset] = toBlock(driverContext.blockFactory());
        }

        Block toBlock(BlockFactory blockFactory) {
            if (values.size() == 0) {
                return blockFactory.newConstantNullBlock(1);
            }
            try ($Type$Block.Builder builder = blockFactory.new$Type$BlockBuilder((int) values.size())) {
                builder.beginPositionEntry();
                for (int id = 0; id < values.size(); id++) {
$if(long)$
                    builder.appendLong(values.get(id));
$elseif(double)$
                    builder.appendDouble(Double.longBitsToDouble(values.get(id)));
$elseif(int)$
                    builder.appendInt((int) values.get(id));
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

    public static class GroupingState implements Releasable {
$if(long||double)$
        private final LongLongHash values;

$elseif(int)$
        private final LongHash values;

$endif$

        private GroupingState(BigArrays bigArrays) {
$if(long||double)$
            values = new LongLongHash(1, bigArrays);
$elseif(int)$
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
            try ($Type$Block.Builder builder = blockFactory.new$Type$BlockBuilder(selected.getPositionCount())) {
                for (int s = 0; s < selected.getPositionCount(); s++) {
                    int selectedGroup = selected.getInt(s);
                    /*
                     * Count can effectively be in three states - 0, 1, many. We use those
                     * states to buffer the first value, so we can avoid calling
                     * beginPositionEntry on single valued fields.
                     */
                    int count = 0;
                    $type$ first = 0;
                    for (int id = 0; id < values.size(); id++) {
$if(long)$
                        long group = values.getKey1(id);
                        if (group == selectedGroup) {
                            long value = values.getKey2(id);
$elseif(double)$
                        long group = values.getKey1(id);
                        if (group == selectedGroup) {
                            double value = Double.longBitsToDouble(values.getKey2(id));
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
                                    builder.append$Type$(first);
                                    builder.append$Type$(value);
                                }
                                default -> builder.append$Type$(value);
                            }
                            count++;
                        }
                    }
                    switch (count) {
                        case 0 -> builder.appendNull();
                        case 1 -> builder.append$Type$(first);
                        default -> builder.endPositionEntry();
                    }
                }
                return builder.build();
            }
        }

        void enableGroupIdTracking(SeenGroupIds seen) {
            // we figure out seen values from their
        }

        @Override
        public void close() {
            values.close();
        }
    }
}
