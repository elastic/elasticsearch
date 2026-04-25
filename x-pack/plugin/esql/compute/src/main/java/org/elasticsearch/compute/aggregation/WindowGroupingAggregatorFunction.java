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
import java.util.function.IntConsumer;
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
    public GroupingAggregatorFunction.PreparedForEvaluation prepareEvaluateIntermediate(
        IntVector selected,
        GroupingAggregatorEvaluationContext ctx
    ) {
        return next.prepareEvaluateIntermediate(selected, ctx);
    }

    @Override
    public GroupingAggregatorFunction.PreparedForEvaluation prepareEvaluateFinal(
        IntVector selectedGroups,
        GroupingAggregatorEvaluationContext ctx
    ) {
        if (ctx instanceof TimeSeriesGroupingAggregatorEvaluationContext timeSeriesContext) {
            return prepareEvaluateFinalWithWindow(selectedGroups, timeSeriesContext);
        }
        return next.prepareEvaluateFinal(selectedGroups, ctx);
    }

    private GroupingAggregatorFunction.PreparedForEvaluation prepareEvaluateFinalWithWindow(
        IntVector selectedGroups,
        TimeSeriesGroupingAggregatorEvaluationContext ctx
    ) {
        if (selectedGroups.getPositionCount() == 0) {
            return next.prepareEvaluateFinal(selectedGroups, ctx);
        }

        // TODO: rewrite to NO_WINDOW in the planner if the bucket and the window are the same
        int groupId = selectedGroups.getInt(0);
        long startTime = ctx.rangeStartInMillis(groupId);
        long endTime = ctx.rangeEndInMillis(groupId);
        if (endTime - startTime == window.toMillis()) {
            return next.prepareEvaluateFinal(selectedGroups, ctx);
        }
        // When output filtering is active, allGroupIds contains every group (including sub-bucket
        // groups that won't appear in the final output). We need all of them for evaluateIntermediate
        // so that neighbor data is available during the window merge. When null, selected already
        // contains every group.
        IntVector allGroups = ctx.allGroupIds();
        if (allGroups == null) {
            allGroups = selectedGroups;
        }
        int blockCount = next.intermediateBlockCount();
        List<Integer> channels = IntStream.range(0, blockCount).boxed().toList();
        GroupingAggregator.Factory aggregatorFactory = supplier.groupingAggregatorFactory(AggregatorMode.FINAL, channels);
        GroupingAggregator finalAgg = aggregatorFactory.apply(ctx.driverContext());
        try {
            Block[] intermediateBlocks = new Block[blockCount];
            int[] backwards = new int[allGroups.getPositionCount()];
            for (int i = 0; i < allGroups.getPositionCount(); i++) {
                int gid = allGroups.getInt(i);
                backwards = ArrayUtil.grow(backwards, gid + 1);
                backwards[gid] = i;
            }
            try {
                // TODO slice into pages
                try (PreparedForEvaluation prepared = next.prepareEvaluateIntermediate(allGroups, ctx)) {
                    prepared.evaluate(intermediateBlocks, 0, allGroups);
                }
                Page page = new Page(intermediateBlocks);
                GroupingAggregatorFunction finalAggFunction = finalAgg.aggregatorFunction();
                addInitialIntermediateInput(finalAggFunction, allGroups, page);
                for (int i = 0; i < selectedGroups.getPositionCount(); i++) {
                    groupId = selectedGroups.getInt(i);
                    mergeBucketsFromWindow(groupId, backwards, page, finalAggFunction, ctx);
                }
            } finally {
                Releasables.close(intermediateBlocks);
            }
            PreparedForEvaluation delegate = finalAgg.prepareForEvaluate(
                selectedGroups,
                new TimeSeriesGroupingAggregatorEvaluationContext(ctx.driverContext()) {
                    // expand the window to cover the new range
                    @Override
                    public long rangeStartInMillis(int groupId) {
                        return ctx.rangeStartInMillis(groupId);
                    }

                    @Override
                    public long rangeEndInMillis(int groupId) {
                        return rangeStartInMillis(groupId) + window.toMillis();
                    }

                    @Override
                    public void forEachGroupInWindow(int startingGroupId, Duration window, IntConsumer action) {
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
                        /* not used by {@link this#nextGroupId} and {@link this##previousGroupId} */
                    }
                }
            );
            GroupingAggregator takeFinalAgg = finalAgg;
            finalAgg = null;
            // Leave the final agg open until the prepared results are closed.
            return new PreparedForEvaluation() {
                @Override
                public void evaluate(Block[] blocks, int offset, IntVector selectedInPage) {
                    delegate.evaluate(blocks, offset, selectedInPage);
                }

                @Override
                public void close() {
                    Releasables.close(delegate, takeFinalAgg);
                }
            };
        } finally {
            Releasables.close(finalAgg);
        }
    }

    private void mergeBucketsFromWindow(
        int startingGroupId,
        int[] groupIdToPositions,
        Page page,
        GroupingAggregatorFunction fn,
        TimeSeriesGroupingAggregatorEvaluationContext context
    ) {
        int[] singlePosition = new int[1];
        try (var oneGroup = context.driverContext().blockFactory().newConstantIntVector(startingGroupId, 1)) {
            context.forEachGroupInWindow(startingGroupId, window, groupId -> {
                if (groupId == startingGroupId) {
                    return;
                }
                int position = groupIdToPositions[groupId];
                if (hasIntermediateState(page, position)) {
                    singlePosition[0] = position;
                    addIntermediateInputWithFilter(fn, page, singlePosition, oneGroup);
                }
            });
        }
    }

    /**
     * Adds the non-NULL positions of {@code page} as intermediate input to {@code fn}.
     */
    private static void addInitialIntermediateInput(GroupingAggregatorFunction fn, IntVector groups, Page page) {
        final int positionCount = page.getPositionCount();

        int[] positions = new int[positionCount];
        int count = 0;
        for (int p = 0; p < positionCount; p++) {
            if (hasIntermediateState(page, p)) {
                positions[count++] = p;
            }
        }

        if (count == 0) {
            return;
        }

        if (count == positionCount) {
            fn.addIntermediateInput(0, groups, page);
            return;
        }

        final int[] validPositions = Arrays.copyOf(positions, count);
        try (IntVector filteredGroups = groups.filter(false, validPositions)) {
            addIntermediateInputWithFilter(fn, page, validPositions, filteredGroups);
        }
    }

    private static void addIntermediateInputWithFilter(
        GroupingAggregatorFunction fn,
        Page page,
        int[] keepPositions,
        IntVector filteredGroups
    ) {
        final int blockCount = page.getBlockCount();
        final Block[] filteredBlocks = new Block[blockCount];
        boolean success = false;

        // TODO: Pass offset and length directly instead of copy:
        // https://github.com/elastic/elasticsearch/pull/145195
        try {
            for (int b = 0; b < blockCount; b++) {
                filteredBlocks[b] = page.getBlock(b).filter(false, keepPositions);
            }
            success = true;
            try (Page filteredPage = new Page(keepPositions.length, filteredBlocks)) {
                fn.addIntermediateInput(0, filteredGroups, filteredPage);
            }
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(filteredBlocks);
            }
        }
    }

    private static boolean hasIntermediateState(Page page, int position) {
        final int blockCount = page.getBlockCount();
        for (int b = 0; b < blockCount; b++) {
            Block block = page.getBlock(b);
            if (block.mayHaveNulls() && block.isNull(position)) {
                return false;
            }
        }
        return true;
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
