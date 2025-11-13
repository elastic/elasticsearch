/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.LongArray;
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
@GroupingAggregator(
    { @IntermediateState(name = "timestamps", type = "LONG_BLOCK"), @IntermediateState(name = "values", type = "DOUBLE_BLOCK") }
)
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

    public static void combine(GroupingState state, int groupId, long timestamp, double value) {
        // TODO
    }

    public static void combineIntermediate(
        GroupingState state,
        int groupId,
        LongBlock timestamps, // stylecheck
        DoubleBlock values,
        int otherPosition
    ) {
        // TODO use groupId
        state.collectValue(groupId, timestamps.getLong(otherPosition), values.getDouble(otherPosition));
    }

    public static Block evaluateFinal(GroupingState state, IntVector selectedGroups, GroupingAggregatorEvaluationContext ctx) {
        // Block evaluatePercentile(IntVector selected, DriverContext driverContext) {
        // try (DoubleBlock.Builder builder = driverContext.blockFactory().newDoubleBlockBuilder(selected.getPositionCount())) {
        // for (int i = 0; i < selected.getPositionCount(); i++) {
        // int si = selected.getInt(i);
        // if (si >= digests.size()) {
        // builder.appendNull();
        // continue;
        // }
        // final TDigestState digest = digests.get(si);
        // if (percentile != null && digest != null && digest.size() > 0) {
        // builder.appendDouble(digest.quantile(percentile / 100));
        // } else {
        // builder.appendNull();
        // }
        // }
        // return builder.build();
        // }
        // }
        try (DoubleBlock.Builder builder = ctx.driverContext().blockFactory().newDoubleBlockBuilder(selectedGroups.getPositionCount())) {
            for (int i = 0; i < selectedGroups.getPositionCount(); i++) {
                int groupId = selectedGroups.getInt(i);
                // TODO must use groupId
                double result = 1.0;
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
        private final BigArrays bigArrays;
        private LongArray timestamps;
        private DoubleArray values;

        GroupingState(BigArrays bigArrays) {
            super(bigArrays);
            this.bigArrays = bigArrays;
            this.timestamps = bigArrays.newLongArray(1L);
            this.values = bigArrays.newDoubleArray(1L);
        }

        void collectValue(int groupId, long timestamp, double value) {
            if (groupId < timestamps.size()) {
                timestamps.set(groupId, timestamp);
            } else {
                timestamps = bigArrays.grow(timestamps, groupId + 1);
            }
            timestamps.set(groupId, timestamp);
            if (groupId < values.size()) {
                values.set(groupId, value);
            } else {
                values = bigArrays.grow(values, groupId + 1);
            }
            values.set(groupId, value);
        }

        @Override
        public void close() {
            Releasables.close(timestamps, values, super::close);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            try (
                LongBlock.Builder timestampBuilder = driverContext.blockFactory().newLongBlockBuilder(selected.getPositionCount());
                DoubleBlock.Builder valueBuilder = driverContext.blockFactory().newDoubleBlockBuilder(selected.getPositionCount());
            ) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    int groupId = selected.getInt(i);
                    timestampBuilder.appendLong(timestamps.get(groupId));
                    valueBuilder.appendDouble(values.get(groupId));
                }
                blocks[offset] = timestampBuilder.build();
                blocks[offset + 1] = valueBuilder.build();
            }
        }
    }
}
