/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.compute.aggregation.blockhash.TimeSeriesBlockHash;
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
        if (selected.getPositionCount() == 0) {
            return next.prepareEvaluateFinal(selected, ctx);
        }

        // TODO: rewrite to NO_WINDOW in the planner if the bucket and the window are the same
        int groupId = selected.getInt(0);
        long startTime = ctx.rangeStartInMillis(groupId);
        long endTime = ctx.rangeEndInMillis(groupId);
        if (endTime - startTime == window.toMillis()) {
            return next.prepareEvaluateFinal(selected, ctx);
        }

        // TODO: rewrite to NO_WINDOW in the planner if the bucket and the window are the same
        int groupId = selectedGroups.getInt(0);
        long startTime = ctx.rangeStartInMillis(groupId);
        long endTime = ctx.rangeEndInMillis(groupId);
        if (endTime - startTime == window.toMillis()) {
            return next.prepareEvaluateFinal(selectedGroups, ctx);
        }

        // Output filtering keeps only output-aligned groups in selectedGroups.
        // Window merges still need intermediate state for all groups.
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
            int[] backwards = new int[selected.getPositionCount()];
            for (int i = 0; i < selected.getPositionCount(); i++) {
                groupId = selected.getInt(i);
                backwards = ArrayUtil.grow(backwards, groupId + 1);
                backwards[groupId] = i;
            }
            try {
                // TODO slice into pages
                try (PreparedForEvaluation prepared = next.prepareEvaluateIntermediate(allGroups, ctx)) {
                    prepared.evaluate(intermediateBlocks, 0, allGroups);
                }
                Page page = new Page(intermediateBlocks);
                GroupingAggregatorFunction finalAggFunction = finalAgg.aggregatorFunction();
                addInitialIntermediateInput(finalAggFunction, selected, page);
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    groupId = selected.getInt(i);
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
        try (var oneGroup = context.driverContext().blockFactory().newConstantIntVector(startingGroupId, 1)) {
            context.forEachGroupInWindow(startingGroupId, window, groupId -> {
                if (groupId == startingGroupId) {
                    return;
                }
                int position = groupIdToPositions[groupId];
                if (hasIntermediateState(page, position)) {
                    fn.addIntermediateInput(position, oneGroup, page);
                }
            });
        }
    }

    /**
     * Adds the non-null positions of {@code page} as intermediate input to {@code fn}
     */
    private static void addInitialIntermediateInput(GroupingAggregatorFunction fn, IntVector groups, Page page) {
        boolean[] mightHaveNulls = new boolean[page.getBlockCount()];
        boolean anyNullable = false;
        for (int i = 0; i < page.getBlockCount(); i++) {
            mightHaveNulls[i] = page.getBlock(i).mayHaveNulls();
            anyNullable |= mightHaveNulls[i];
        }
        if (anyNullable == false) {
            fn.addIntermediateInput(0, groups, page);
            return;
        }
        int[] positions = new int[page.getPositionCount()];
        int count = 0;
        for (int pos = 0; pos < page.getPositionCount(); pos++) {
            boolean hasState = true;
            for (int b = 0; b < page.getBlockCount(); b++) {
                if (mightHaveNulls[b] && page.getBlock(b).isNull(pos)) {
                    hasState = false;
                    break;
                }
            }
            if (hasState) {
                positions[count++] = pos;
            }
        }
        if (count == page.getPositionCount()) {
            fn.addIntermediateInput(0, groups, page);
            return;
        }
        if (count > 0) {
            // TODO: Should we add a filter(boolean, int[] positions, int count) overload to Block/Vector to avoid this copy?
            int[] validPositions = Arrays.copyOf(positions, count);
            Block[] filteredBlocks = new Block[page.getBlockCount()];
            boolean success = false;
            try {
                for (int b = 0; b < filteredBlocks.length; b++) {
                    filteredBlocks[b] = page.getBlock(b).filter(false, validPositions);
                }
                success = true;
                try (IntVector filteredGroups = groups.filter(false, validPositions); Page filteredPage = new Page(count, filteredBlocks)) {
                    fn.addIntermediateInput(0, filteredGroups, filteredPage);
                }
            } finally {
                if (success == false) {
                    Releasables.closeExpectNoException(filteredBlocks);
                }
                int position = groupIdToPositions[groupId];
                if (position >= 0 && hasIntermediateState(page, position)) {
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

    private static boolean hasIntermediateState(Page page, int position) {
        for (int i = 0; i < page.getBlockCount(); i++) {
            if (page.getBlock(i).isNull(position)) {
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
