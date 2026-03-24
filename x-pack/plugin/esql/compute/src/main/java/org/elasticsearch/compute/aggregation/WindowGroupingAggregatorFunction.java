/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

/**
 * A {@link GroupingAggregatorFunction} that wraps another, and apply a window function on the final aggregation.
 */
public record WindowGroupingAggregatorFunction(GroupingAggregatorFunction next, AggregatorFunctionSupplier supplier, Duration window)
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
        if (evaluationContext instanceof TimeSeriesGroupingAggregatorEvaluationContext timeSeriesContext) {
            evaluateFinalWithWindow(blocks, offset, selected, timeSeriesContext);
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
        if (selected.getPositionCount() > 0) {
            // TODO: rewrite to NO_WINDOW in the planner if the bucket and the window are the same
            int groupId = selected.getInt(0);
            long startTime = evaluationContext.rangeStartInMillis(groupId);
            long endTime = evaluationContext.rangeEndInMillis(groupId);
            if (endTime - startTime == window.toMillis()) {
                next.evaluateFinal(blocks, offset, selected, evaluationContext);
                return;
            }
        }
        int blockCount = next.intermediateBlockCount();
        List<Integer> channels = IntStream.range(0, blockCount).boxed().toList();
        GroupingAggregator.Factory aggregatorFactory = supplier.groupingAggregatorFactory(AggregatorMode.FINAL, channels);
        try (GroupingAggregator finalAgg = aggregatorFactory.apply(evaluationContext.driverContext())) {
            Block[] intermediateBlocks = new Block[blockCount];
            int[] backwards = new int[selected.getPositionCount()];
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int groupId = selected.getInt(i);
                backwards = ArrayUtil.grow(backwards, groupId + 1);
                backwards[groupId] = i;
            }
            try {
                next.evaluateIntermediate(intermediateBlocks, 0, selected);
                Page page = new Page(intermediateBlocks);
                GroupingAggregatorFunction finalAggFunction = finalAgg.aggregatorFunction();
                boolean hasNullIntermediateState = hasNullIntermediateState(page);
                if (hasNullIntermediateState) {
                    addNonNullIntermediateInput(finalAggFunction, selected, page);
                } else {
                    finalAggFunction.addIntermediateInput(0, selected, page);
                }
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    int groupId = selected.getInt(i);
                    mergeBucketsFromWindow(groupId, backwards, page, finalAggFunction, evaluationContext, hasNullIntermediateState);
                }
            } finally {
                Releasables.close(intermediateBlocks);
            }
            finalAgg.evaluate(
                blocks,
                offset,
                selected,
                // expand the window to cover the new range
                new TimeSeriesGroupingAggregatorEvaluationContext(evaluationContext.driverContext()) {
                    @Override
                    public long rangeStartInMillis(int groupId) {
                        return evaluationContext.rangeStartInMillis(groupId);
                    }

                    @Override
                    public long rangeEndInMillis(int groupId) {
                        return rangeStartInMillis(groupId) + window.toMillis();
                    }

                    @Override
                    public List<Integer> groupIdsFromWindow(int startingGroupId, Duration window) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public int previousGroupId(int currentGroupId) {
                        return -1;
                    }

                    @Override
                    public int nextGroupId(int currentGroupId) {
                        return -1;
                    }

                    @Override
                    public void computeAdjacentGroupIds() {
                        // not used by #nextGroupId and #previousGroupId
                    }
                }
            );
        }
    }

    private void mergeBucketsFromWindow(
        int startingGroupId,
        int[] groupIdToPositions,
        Page page,
        GroupingAggregatorFunction fn,
        TimeSeriesGroupingAggregatorEvaluationContext context,
        boolean hasNullIntermediateState
    ) {
        var groupIds = context.groupIdsFromWindow(startingGroupId, window);
        if (groupIds.size() > 1) {
            try (IntVector oneGroup = context.driverContext().blockFactory().newConstantIntVector(startingGroupId, 1)) {
                for (int g : groupIds) {
                    if (g != startingGroupId) {
                        int position = groupIdToPositions[g];
                        if (hasNullIntermediateState == false) {
                            fn.addIntermediateInput(position, oneGroup, page);
                            continue;
                        }
                        if (hasIntermediateState(page, position) == false) {
                            continue;
                        }
                        try (Page singlePositionPage = filterPage(page, position)) {
                            fn.addIntermediateInput(0, oneGroup, singlePositionPage);
                        }
                    }
                }
            }
        }
    }

    private static void addNonNullIntermediateInput(GroupingAggregatorFunction fn, IntVector groups, Page page) {
        int[] positions = positionsWithIntermediateState(page);
        if (positions.length == 0) {
            return;
        }
        try (IntVector filteredGroups = groups.filter(false, positions); Page filteredPage = filterPage(page, positions)) {
            fn.addIntermediateInput(0, filteredGroups, filteredPage);
        }
    }

    private static int[] positionsWithIntermediateState(Page page) {
        int[] positions = new int[page.getPositionCount()];
        int count = 0;
        for (int position = 0; position < page.getPositionCount(); position++) {
            if (hasIntermediateState(page, position)) {
                positions[count++] = position;
            }
        }
        return Arrays.copyOf(positions, count);
    }

    private static boolean hasNullIntermediateState(Page page) {
        return positionsWithIntermediateState(page).length != page.getPositionCount();
    }

    private static boolean hasIntermediateState(Page page, int position) {
        for (int i = 0; i < page.getBlockCount(); i++) {
            if (page.getBlock(i).isNull(position)) {
                return false;
            }
        }
        return true;
    }

    private static Page filterPage(Page page, int... positions) {
        Block[] filteredBlocks = new Block[page.getBlockCount()];
        boolean success = false;
        try {
            for (int i = 0; i < filteredBlocks.length; i++) {
                filteredBlocks[i] = page.getBlock(i).filter(false, positions);
            }
            success = true;
            return new Page(positions.length, filteredBlocks);
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(filteredBlocks);
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

    @Override
    public String toString() {
        return "Window[agg=" + next + ", window=" + window + "]";
    }
}
