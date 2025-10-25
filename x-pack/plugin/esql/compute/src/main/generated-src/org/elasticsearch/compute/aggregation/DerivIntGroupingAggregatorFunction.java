/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

import java.util.List;

@SuppressWarnings("cast")
public class DerivIntGroupingAggregatorFunction implements GroupingAggregatorFunction {

    private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
        new IntermediateStateDesc("sumVal", ElementType.INT),
        new IntermediateStateDesc("sumTs", ElementType.LONG),
        new IntermediateStateDesc("sumTsVal", ElementType.INT),
        new IntermediateStateDesc("sumTsSq", ElementType.LONG),
        new IntermediateStateDesc("count", ElementType.LONG)
    );

    private final List<Integer> channels;
    private final DriverContext driverContext;
    private ObjectArray<SimpleLinearRegressionWithTimeseries> states;

    public DerivIntGroupingAggregatorFunction(List<Integer> channels, DriverContext driverContext) {
        this.states = driverContext.bigArrays().newObjectArray(256);
        this.channels = channels;
        this.driverContext = driverContext;
    }

    public static class Supplier implements AggregatorFunctionSupplier {

        @Override
        public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
            throw new UnsupportedOperationException("DerivGroupingAggregatorFunction does not support non-grouping aggregation");
        }

        @Override
        public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
            return INTERMEDIATE_STATE_DESC;
        }

        @Override
        public AggregatorFunction aggregator(DriverContext driverContext, List<Integer> channels) {
            throw new UnsupportedOperationException("DerivGroupingAggregatorFunction does not support non-grouping aggregation");
        }

        @Override
        public GroupingAggregatorFunction groupingAggregator(DriverContext driverContext, List<Integer> channels) {
            return new DerivIntGroupingAggregatorFunction(channels, driverContext);
        }

        @Override
        public String describe() {
            return "derivative";
        }
    }

    @Override
    public AddInput prepareProcessRawInputPage(SeenGroupIds seenGroupIds, Page page) {
        final IntBlock valueBlock = page.getBlock(channels.get(0));
        final LongBlock timestampBlock = page.getBlock(channels.get(1));
        final IntVector valueVector = valueBlock.asVector();
        return new AddInput() {
            @Override
            public void add(int positionOffset, IntArrayBlock groupIds) {
                addBlockInput(positionOffset, groupIds, timestampBlock, valueVector);
            }

            @Override
            public void add(int positionOffset, IntBigArrayBlock groupIds) {
                addBlockInput(positionOffset, groupIds, timestampBlock, valueVector);
            }

            @Override
            public void add(int positionOffset, IntVector groupIds) {
                for (int groupPosition = 0; groupPosition < groupIds.getPositionCount(); groupPosition++) {
                    int valuesPosition = groupPosition + positionOffset;
                    int groupId = groupIds.getInt(groupPosition);
                    int vValue = valueVector.getInt(valuesPosition);
                    long ts = timestampBlock.getLong(valuesPosition);
                    SimpleLinearRegressionWithTimeseries state = states.get(groupId);
                    if (state == null) {
                        state = new SimpleLinearRegressionWithTimeseries();
                        states.set(groupId, state);
                    }
                    state.add(ts, (double) vValue);  // TODO - value needs to be converted to double
                }
            }

            @Override
            public void close() {

            }

            private void addBlockInput(int positionOffset, IntBlock groupIds, LongBlock timestampBlock, IntVector valueVector) {
                for (int groupPosition = 0; groupPosition < groupIds.getPositionCount(); groupPosition++) {
                    if (groupIds.isNull(groupPosition)) {
                        continue;
                    }
                    int valuePosition = groupPosition + positionOffset;
                    if (valueBlock.isNull(valuePosition)) {
                        continue;
                    }
                    int groupStart = groupIds.getFirstValueIndex(groupPosition);
                    int groupEnd = groupStart + groupIds.getValueCount(groupPosition);
                    for (int g = groupStart; g < groupEnd; g++) {
                        int groupId = groupIds.getInt(g);
                        int vStart = valueBlock.getFirstValueIndex(valuePosition);
                        int vEnd = vStart + valueBlock.getValueCount(valuePosition);
                        for (int v = vStart; v < vEnd; v++) {
                            long ts = timestampBlock.getLong(valuePosition);
                            int val = valueVector.getInt(valuePosition);
                            SimpleLinearRegressionWithTimeseries state = states.get(groupId);
                            if (state == null) {
                                state = new SimpleLinearRegressionWithTimeseries();
                                states.set(groupId, state);
                            }
                            state.add(ts, val);  // TODO - value needs to be converted to double
                        }
                    }
                }
            }
        };
    }

    @Override
    public void selectedMayContainUnseenGroups(SeenGroupIds seenGroupIds) {
        // No-op
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntArrayBlock groupIdVector, Page page) {
        addIntermediateBlockInput(positionOffset, groupIdVector, page);
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntBigArrayBlock groupIdVector, Page page) {
        addIntermediateBlockInput(positionOffset, groupIdVector, page);
    }

    private void addIntermediateBlockInput(int positionOffset, IntBlock groupIdVector, Page page) {
        IntBlock sumValBlock = page.getBlock(channels.get(0));
        LongBlock sumTsBlock = page.getBlock(channels.get(1));
        IntBlock sumTsValBlock = page.getBlock(channels.get(2));
        LongBlock sumTsSqBlock = page.getBlock(channels.get(3));
        LongBlock countBlock = page.getBlock(channels.get(4));

        if (sumTsBlock.getTotalValueCount() != sumValBlock.getTotalValueCount()
            || sumTsBlock.getTotalValueCount() != sumTsValBlock.getTotalValueCount()
            || sumTsBlock.getTotalValueCount() != countBlock.getTotalValueCount()) {
            throw new IllegalStateException("Mismatched intermediate state block value counts");
        }

        for (int groupPos = 0; groupPos < groupIdVector.getPositionCount(); groupPos++) {
            int valuePos = groupPos + positionOffset;

            int firstGroup = groupIdVector.getFirstValueIndex(groupPos);
            int lastGroup = firstGroup + groupIdVector.getValueCount(groupPos);

            for (int g = firstGroup; g < lastGroup; g++) {
                int groupId = groupIdVector.getInt(g);
                states = driverContext.bigArrays().grow(states, groupId + 1);
                var state = states.get(groupId);
                if (state == null) {
                    state = new SimpleLinearRegressionWithTimeseries(); // TODO - what happens for int / long
                    states.set(groupId, state);
                }
                long sumTs = sumTsBlock.getLong(valuePos);
                int sumVal = sumValBlock.getInt(valuePos);
                int sumTsVal = sumTsValBlock.getInt(valuePos);
                long sumTsSq = sumTsSqBlock.getLong(valuePos);
                long count = countBlock.getLong(valuePos);
                state.sumTs += sumTs;
                state.sumVal += sumVal;
                state.sumTsVal += sumTsVal;
                state.sumTsSq += sumTsSq;
                state.count += count;
            }
        }

    }

    @Override
    public void addIntermediateInput(int positionOffset, IntVector groupIdVector, Page page) {
        IntBlock sumValBlock = page.getBlock(channels.get(0));
        LongBlock sumTsBlock = page.getBlock(channels.get(1));
        IntBlock sumTsValBlock = page.getBlock(channels.get(2));
        LongBlock sumTsSqBlock = page.getBlock(channels.get(3));
        LongBlock countBlock = page.getBlock(channels.get(4));

        if (sumTsBlock.getTotalValueCount() != sumValBlock.getTotalValueCount()
            || sumTsBlock.getTotalValueCount() != sumTsValBlock.getTotalValueCount()
            || sumTsBlock.getTotalValueCount() != countBlock.getTotalValueCount()) {
            throw new IllegalStateException("Mismatched intermediate state block value counts");
        }

        for (int groupPos = 0; groupPos < groupIdVector.getPositionCount(); groupPos++) {
            int valuePos = groupPos + positionOffset;
            int groupId = groupIdVector.getInt(groupPos);
            states = driverContext.bigArrays().grow(states, groupId + 1);
            var state = states.get(groupId);
            if (state == null) {
                state = new SimpleLinearRegressionWithTimeseries(); // TODO: what about double conversion
                states.set(groupId, state);
            }
            long sumTs = sumTsBlock.getLong(valuePos);
            int sumVal = sumValBlock.getInt(valuePos);
            int sumTsVal = sumTsValBlock.getInt(valuePos);
            long sumTsSq = sumTsSqBlock.getLong(valuePos);
            long count = countBlock.getLong(valuePos);
            state.sumTs += sumTs;
            state.sumVal += sumVal;
            state.sumTsVal += sumTsVal;
            state.sumTsSq += sumTsSq;
            state.count += count;
        }
    }

    @Override
    public void evaluateIntermediate(Block[] blocks, int offset, IntVector selected) {
        BlockFactory blockFactory = driverContext.blockFactory();
        int positionCount = selected.getPositionCount();
        try (
            var sumValBuilder = blockFactory.newIntBlockBuilder(positionCount);
            var sumTsBuilder = blockFactory.newLongBlockBuilder(positionCount);
            var sumTsValBuilder = blockFactory.newIntBlockBuilder(positionCount);
            var sumTsSqBuilder = blockFactory.newLongBlockBuilder(positionCount);
            var countBuilder = blockFactory.newLongBlockBuilder(positionCount)
        ) {
            for (int p = 0; p < positionCount; p++) {
                int groupId = selected.getInt(p);
                SimpleLinearRegressionWithTimeseries state = states.get(groupId);
                if (state == null) {
                    sumValBuilder.appendNull();
                    sumTsBuilder.appendNull();
                    sumTsValBuilder.appendNull();
                    sumTsSqBuilder.appendNull();
                    countBuilder.appendNull();
                } else {
                    sumValBuilder.appendInt((int) state.sumVal);
                    sumTsBuilder.appendLong(state.sumTs);
                    sumTsValBuilder.appendInt((int) state.sumTsVal); // TODO: fix this actually
                    sumTsSqBuilder.appendLong(state.sumTsSq);
                    countBuilder.appendLong(state.count);
                }
            }
            blocks[offset] = sumValBuilder.build();
            blocks[offset + 1] = sumTsBuilder.build();
            blocks[offset + 2] = sumTsValBuilder.build();
            blocks[offset + 3] = sumTsSqBuilder.build();
            blocks[offset + 4] = countBuilder.build();
        }
    }

    @Override
    public void evaluateFinal(Block[] blocks, int offset, IntVector selected, GroupingAggregatorEvaluationContext evaluationContext) {
        BlockFactory blockFactory = driverContext.blockFactory();
        int positionCount = selected.getPositionCount();
        try (var resultBuilder = blockFactory.newDoubleBlockBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                int groupId = selected.getInt(p);
                SimpleLinearRegressionWithTimeseries state = states.get(groupId);
                if (state == null) {
                    resultBuilder.appendNull();
                } else {
                    double deriv = state.slope();
                    resultBuilder.appendDouble(deriv);
                }
            }
            blocks[offset] = resultBuilder.build();
        }
    }

    @Override
    public int intermediateBlockCount() {
        return INTERMEDIATE_STATE_DESC.size();
    }

    @Override
    public void close() {
        Releasables.close(states);
    }
}
