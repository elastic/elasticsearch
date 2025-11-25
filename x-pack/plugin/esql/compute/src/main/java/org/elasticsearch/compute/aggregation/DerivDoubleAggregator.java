/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

@Aggregator(
    {
        @IntermediateState(name = "count", type = "LONG"),
        @IntermediateState(name = "sumVal", type = "DOUBLE"),
        @IntermediateState(name = "sumTs", type = "LONG"),
        @IntermediateState(name = "sumTsVal", type = "DOUBLE"),
        @IntermediateState(name = "sumTsSq", type = "LONG") }
)
@GroupingAggregator
class DerivDoubleAggregator {

    public static SimpleLinearRegressionWithTimeseries initSingle(DriverContext driverContext) {
        return new SimpleLinearRegressionWithTimeseries();
    }

    public static void combine(SimpleLinearRegressionWithTimeseries current, double value, long timestamp) {
        current.add(timestamp, value);
    }

    public static void combineIntermediate(
        SimpleLinearRegressionWithTimeseries state,
        long count,
        double sumVal,
        long sumTs,
        double sumTsVal,
        long sumTsSq
    ) {
        state.count += count;
        state.sumVal += sumVal;
        state.sumTs += sumTs;
        state.sumTsVal += sumTsVal;
        state.sumTsSq += sumTsSq;
    }

    public static Block evaluateFinal(SimpleLinearRegressionWithTimeseries state, DriverContext driverContext) {
        BlockFactory blockFactory = driverContext.blockFactory();
        var slope = state.slope();
        if (Double.isNaN(slope)) {
            return blockFactory.newConstantNullBlock(1);
        }
        return blockFactory.newConstantDoubleBlockWith(slope, 1);
    }

    public static GroupingState initGrouping(DriverContext driverContext) {
        return new GroupingState(driverContext.bigArrays());
    }

    public static void combine(GroupingState state, int groupId, double value, long timestamp) {
        state.getAndGrow(groupId).add(timestamp, value);
    }

    public static void combineIntermediate(
        GroupingState state,
        int groupId,
        long count,
        double sumVal,
        long sumTs,
        double sumTsVal,
        long sumTsSq
    ) {
        combineIntermediate(state.getAndGrow(groupId), count, sumVal, sumTs, sumTsVal, sumTsSq);
    }

    public static Block evaluateFinal(GroupingState state, IntVector selectedGroups, GroupingAggregatorEvaluationContext ctx) {
        try (DoubleBlock.Builder builder = ctx.driverContext().blockFactory().newDoubleBlockBuilder(selectedGroups.getPositionCount())) {
            for (int i = 0; i < selectedGroups.getPositionCount(); i++) {
                int groupId = selectedGroups.getInt(i);
                SimpleLinearRegressionWithTimeseries slr = state.get(groupId);
                if (slr == null) {
                    builder.appendNull();
                    continue;
                }
                double result = slr.slope();
                if (Double.isNaN(result)) {
                    builder.appendNull();
                    continue;
                }
                builder.appendDouble(result);
            }
            return builder.build();
        }
    }

    public static final class GroupingState extends AbstractArrayState {
        private ObjectArray<SimpleLinearRegressionWithTimeseries> states;

        GroupingState(BigArrays bigArrays) {
            super(bigArrays);
            states = bigArrays.newObjectArray(1);
        }

        SimpleLinearRegressionWithTimeseries get(int groupId) {
            if (groupId >= states.size()) {
                return null;
            }
            return states.get(groupId);
        }

        SimpleLinearRegressionWithTimeseries getAndGrow(int groupId) {
            if (groupId >= states.size()) {
                states = bigArrays.grow(states, groupId + 1);
            }
            SimpleLinearRegressionWithTimeseries slr = states.get(groupId);
            if (slr == null) {
                slr = new SimpleLinearRegressionWithTimeseries();
                states.set(groupId, slr);
            }
            return slr;
        }

        @Override
        public void close() {
            Releasables.close(states, super::close);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            try (
                LongBlock.Builder countBuilder = driverContext.blockFactory().newLongBlockBuilder(selected.getPositionCount());
                DoubleBlock.Builder sumValBuilder = driverContext.blockFactory().newDoubleBlockBuilder(selected.getPositionCount());
                LongBlock.Builder sumTsBuilder = driverContext.blockFactory().newLongBlockBuilder(selected.getPositionCount());
                DoubleBlock.Builder sumTsValBuilder = driverContext.blockFactory().newDoubleBlockBuilder(selected.getPositionCount());
                LongBlock.Builder sumTsSqBuilder = driverContext.blockFactory().newLongBlockBuilder(selected.getPositionCount())
            ) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    int groupId = selected.getInt(i);
                    SimpleLinearRegressionWithTimeseries slr = get(groupId);
                    if (slr == null) {
                        countBuilder.appendNull();
                        sumValBuilder.appendNull();
                        sumTsBuilder.appendNull();
                        sumTsValBuilder.appendNull();
                        sumTsSqBuilder.appendNull();
                    } else {
                        countBuilder.appendLong(slr.count);
                        sumValBuilder.appendDouble(slr.sumVal);
                        sumTsBuilder.appendLong(slr.sumTs);
                        sumTsValBuilder.appendDouble(slr.sumTsVal);
                        sumTsSqBuilder.appendLong(slr.sumTsSq);
                    }
                }
                blocks[offset] = countBuilder.build();
                blocks[offset + 1] = sumValBuilder.build();
                blocks[offset + 2] = sumTsBuilder.build();
                blocks[offset + 3] = sumTsValBuilder.build();
                blocks[offset + 4] = sumTsSqBuilder.build();
            }
        }
    }
}
