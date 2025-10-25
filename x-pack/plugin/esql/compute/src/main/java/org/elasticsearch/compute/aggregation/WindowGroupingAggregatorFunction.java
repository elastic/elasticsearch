/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * A {@link GroupingAggregatorFunction} that wraps another, and apply a window function on the final aggregation.
 */
record WindowGroupingAggregatorFunction(GroupingAggregatorFunction next, AggregatorFunctionSupplier supplier, long windowInterval)
    implements
        GroupingAggregatorFunction {

    @Override
    public AddInput prepareProcessRawInputPage(SeenGroupIds seenGroupIds, Page page) {
        return next.prepareProcessRawInputPage(seenGroupIds, page);
    }

    @Override
    public void selectedMayContainUnseenGroups(SeenGroupIds seenGroupIds) {
        next.selectedMayContainUnseenGroups(seenGroupIds);
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntArrayBlock groupIdVector, Page page) {
        next.addIntermediateInput(positionOffset, groupIdVector, page);
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntBigArrayBlock groupIdVector, Page page) {
        next.addIntermediateInput(positionOffset, groupIdVector, page);
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntVector groupIdVector, Page page) {
        next.addIntermediateInput(positionOffset, groupIdVector, page);
    }

    @Override
    public void evaluateIntermediate(Block[] blocks, int offset, IntVector selected) {
        next.evaluateIntermediate(blocks, offset, selected);
    }

    @Override
    public void evaluateFinal(Block[] blocks, int offset, IntVector selected, GroupingAggregatorEvaluationContext evaluationContext) {
        if (evaluationContext instanceof TimeSeriesGroupingAggregatorEvaluationContext tsContext && windowInterval > 0) {
            evaluateFinalWithWindow(blocks, offset, selected, tsContext);
        } else {
            next.evaluateFinal(blocks, offset, selected, evaluationContext);
        }
    }

    private void evaluateFinalWithWindow(
        Block[] blocks,
        int offset,
        IntVector selected,
        TimeSeriesGroupingAggregatorEvaluationContext evaluationContext
    ) {
        int blockCount = next.intermediateBlockCount();
        List<Integer> channels = IntStream.range(0, blockCount).boxed().toList();
        GroupingAggregator.Factory aggregatorFactory = supplier.groupingAggregatorFactory(AggregatorMode.FINAL, channels);
        try (GroupingAggregator finalAgg = aggregatorFactory.apply(evaluationContext.driverContext())) {
            Block[] intermediateBlocks = new Block[blockCount];
            Map<Integer, Integer> groupIdToPositions = new HashMap<>();
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int groupId = selected.getInt(i);
                groupIdToPositions.put(groupId, i);
            }
            try {
                next.evaluateIntermediate(intermediateBlocks, 0, selected);
                Page page = new Page(intermediateBlocks);
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    int groupId = selected.getInt(i);
                    handleWindow(groupId, groupIdToPositions, page, finalAgg.aggregatorFunction(), evaluationContext);
                }
            } finally {
                Releasables.close(intermediateBlocks);
            }
            finalAgg.evaluate(
                blocks,
                offset,
                selected,
                new TimeSeriesGroupingAggregatorEvaluationContext(evaluationContext.driverContext()) {
                    @Override
                    public long rangeStartInMillis(int groupId) {
                        return evaluationContext.rangeEndInMillis(groupId) - windowInterval;
                    }

                    @Override
                    public long rangeEndInMillis(int groupId) {
                        return evaluationContext.rangeEndInMillis(groupId);
                    }

                    @Override
                    public List<Integer> groupIdsFromWindow(int groupId, long windowInterval) {
                        throw new UnsupportedOperationException("");
                    }
                }
            );
        }
    }

    private void handleWindow(
        int groupId,
        Map<Integer, Integer> groupIdToPositions,
        Page page,
        GroupingAggregatorFunction fn,
        TimeSeriesGroupingAggregatorEvaluationContext context
    ) {
        List<Integer> groupIds = context.groupIdsFromWindow(groupId, windowInterval);
        BlockFactory blockFactory = context.driverContext().blockFactory();
        try (IntVector groups = blockFactory.newConstantIntVector(groupId, 1)) {
            for (int nextGroupId : groupIds) {
                int position = groupIdToPositions.get(nextGroupId);
                fn.addIntermediateInput(position, groups, page);
            }
        }
    }

    @Override
    public int intermediateBlockCount() {
        return next.intermediateBlockCount();
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(next);
    }
}
