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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;

import java.time.Duration;
import java.util.List;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;

/**
 * A {@link GroupingAggregatorFunction} that wraps another, and apply a window function on the final aggregation.
 * <p>
 *     For a window {@code W} over buckets of width {@code B}, the final value of an output bucket ending at
 *     {@code E} merges the per-bucket states of all buckets fully covered by {@code (E - W, E]}. When the window is
 *     not an exact multiple of the bucket ({@code W = k * B + r} with {@code r > 0}), a second {@code partialNext}
 *     function carries, per bucket, the state of only the trailing {@code r} of that bucket; the merge then adds the
 *     boundary bucket's partial state to the {@code k} full buckets. The partial state is computed by a separate,
 *     ordinary aggregate whose input is filtered to the trailing {@code r} of each bucket; {@code partialNext} reads
 *     that aggregate's state channels in the partial-input phase, or filters raw rows itself in single-phase
 *     execution. Both merge walks are range-driven through the {@link TimeSeriesGroupingAggregatorEvaluationContext},
 *     so they are agnostic to the bucket labeling convention.
 * </p>
 */
public record WindowGroupingAggregatorFunction(
    GroupingAggregatorFunction next,
    @Nullable GroupingAggregatorFunction partialNext,
    AggregatorFunctionSupplier supplier,
    Duration window,
    @Nullable Duration partial
) implements GroupingAggregatorFunction {

    public WindowGroupingAggregatorFunction {
        assert (partialNext == null) == (partial == null) : "partial function and partial duration must be set together";
    }

    public WindowGroupingAggregatorFunction(GroupingAggregatorFunction next, AggregatorFunctionSupplier supplier, Duration window) {
        this(next, null, supplier, window, null);
    }

    @Override
    public AddInput prepareProcessRawInputPage(SeenGroupIds seenGroupIds, Page page) {
        if (partialNext == null) {
            return next.prepareProcessRawInputPage(seenGroupIds, page);
        }
        AddInput nextAdd = next.prepareProcessRawInputPage(seenGroupIds, page);
        AddInput partialAdd = null;
        try {
            partialAdd = partialNext.prepareProcessRawInputPage(seenGroupIds, page);
            AddInput combined = combine(nextAdd, partialAdd);
            nextAdd = null;
            partialAdd = null;
            return combined;
        } finally {
            Releasables.close(nextAdd, partialAdd);
        }
    }

    @Override
    public AddInput prepareProcessIntermediateInputPage(SeenGroupIds seenGroupIds, Page page) {
        if (partialNext == null) {
            return next.prepareProcessIntermediateInputPage(seenGroupIds, page);
        }
        AddInput nextAdd = next.prepareProcessIntermediateInputPage(seenGroupIds, page);
        AddInput partialAdd = null;
        try {
            partialAdd = partialNext.prepareProcessIntermediateInputPage(seenGroupIds, page);
            AddInput combined = combine(nextAdd, partialAdd);
            nextAdd = null;
            partialAdd = null;
            return combined;
        } finally {
            Releasables.close(nextAdd, partialAdd);
        }
    }

    private static AddInput combine(AddInput nextAdd, AddInput partialAdd) {
        if (nextAdd == null) {
            return partialAdd;
        }
        if (partialAdd == null) {
            return nextAdd;
        }
        return new CombinedAddInput(nextAdd, partialAdd);
    }

    private record CombinedAddInput(AddInput nextAdd, AddInput partialAdd) implements AddInput {
        @Override
        public void add(int positionOffset, IntArrayBlock groupIds, int maxGroupId) {
            nextAdd.add(positionOffset, groupIds, maxGroupId);
            partialAdd.add(positionOffset, groupIds, maxGroupId);
        }

        @Override
        public void add(int positionOffset, IntBigArrayBlock groupIds, int maxGroupId) {
            nextAdd.add(positionOffset, groupIds, maxGroupId);
            partialAdd.add(positionOffset, groupIds, maxGroupId);
        }

        @Override
        public void add(int positionOffset, IntVector groupIds, int maxGroupId) {
            nextAdd.add(positionOffset, groupIds, maxGroupId);
            partialAdd.add(positionOffset, groupIds, maxGroupId);
        }

        @Override
        public void close() {
            Releasables.close(nextAdd, partialAdd);
        }
    }

    @Override
    public void selectedMayContainUnseenGroups(SeenGroupIds seenGroupIds) {
        next.selectedMayContainUnseenGroups(seenGroupIds);
        if (partialNext != null) {
            partialNext.selectedMayContainUnseenGroups(seenGroupIds);
        }
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntArrayBlock groupIdVector, int maxGroupId, Page page) {
        next.addIntermediateInput(positionOffset, groupIdVector, maxGroupId, page);
        if (partialNext != null) {
            partialNext.addIntermediateInput(positionOffset, groupIdVector, maxGroupId, page);
        }
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntBigArrayBlock groupIdVector, int maxGroupId, Page page) {
        next.addIntermediateInput(positionOffset, groupIdVector, maxGroupId, page);
        if (partialNext != null) {
            partialNext.addIntermediateInput(positionOffset, groupIdVector, maxGroupId, page);
        }
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntVector groupIdVector, int maxGroupId, Page page) {
        next.addIntermediateInput(positionOffset, groupIdVector, maxGroupId, page);
        if (partialNext != null) {
            partialNext.addIntermediateInput(positionOffset, groupIdVector, maxGroupId, page);
        }
    }

    @Override
    public GroupingAggregatorFunction.PreparedForEvaluation prepareEvaluateIntermediate(
        IntVector selected,
        GroupingAggregatorEvaluationContext ctx
    ) {
        if (partialNext != null) {
            // the wrapper is only created for the phase that emits final values. The partial state is emitted by
            // its own aggregate, never through this wrapper
            throw new UnsupportedOperationException("windowed aggregation with a partial side never outputs intermediate state");
        }
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
        if (partialNext != null) {
            throw new IllegalStateException("windowed aggregation with a partial channel requires a time-series evaluation context");
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

        if (partialNext == null) {
            // TODO: rewrite to NO_WINDOW in the planner if the bucket and the window are the same
            int groupId = selectedGroups.getInt(0);
            long startTime = ctx.rangeStartInMillis(groupId);
            long endTime = ctx.rangeEndInMillis(groupId);
            if (endTime - startTime == window.toMillis()) {
                return next.prepareEvaluateFinal(selectedGroups, ctx);
            }
        }
        int blockCount = next.intermediateBlockCount();
        List<Integer> channels = IntStream.range(0, blockCount).boxed().toList();
        GroupingAggregator.Factory aggregatorFactory = supplier.groupingAggregatorFactory(AggregatorMode.FINAL, channels);
        GroupingAggregator finalAgg = aggregatorFactory.apply(ctx.driverContext());
        try {
            Block[] fullBlocks = new Block[blockCount];
            Block[] partialBlocks = partialNext == null ? null : new Block[partialNext.intermediateBlockCount()];
            int[] backwards = new int[selectedGroups.getPositionCount()];
            int maxSelectedGroupId = -1;
            for (int i = 0; i < selectedGroups.getPositionCount(); i++) {
                int gid = selectedGroups.getInt(i);
                backwards = ArrayUtil.grow(backwards, gid + 1);
                backwards[gid] = i;
                maxSelectedGroupId = Math.max(maxSelectedGroupId, gid);
            }
            try {
                // TODO slice into pages
                try (PreparedForEvaluation prepared = next.prepareEvaluateIntermediate(selectedGroups, ctx)) {
                    prepared.evaluate(fullBlocks, 0, selectedGroups);
                }
                Page fullPage = new Page(fullBlocks);
                Page partialPage = null;
                if (partialNext != null) {
                    try (PreparedForEvaluation prepared = partialNext.prepareEvaluateIntermediate(selectedGroups, ctx)) {
                        prepared.evaluate(partialBlocks, 0, selectedGroups);
                    }
                    partialPage = new Page(partialBlocks);
                }
                GroupingAggregatorFunction finalAggFunction = finalAgg.aggregatorFunction();
                // a group created by window expansion can have a completely empty window (nothing in its own bucket
                // and nothing in the merged range); track unseen groups so such a group evaluates to null, not to
                // the aggregator's default value
                finalAggFunction.selectedMayContainUnseenGroups(new SeenGroupIds.Empty());
                finalAggFunction.addIntermediateInput(0, selectedGroups, maxSelectedGroupId, fullPage);
                // the range covered by whole buckets; the remainder, if any, comes from the boundary bucket's
                // partial state. It depends only on the window and the remainder, so it is the same for every group.
                long fullSpan = partial == null ? window.toMillis() : window.toMillis() - partial.toMillis();
                for (int i = 0; i < selectedGroups.getPositionCount(); i++) {
                    int groupId = selectedGroups.getInt(i);
                    mergeBucketsFromWindow(groupId, backwards, fullPage, partialPage, fullSpan, finalAggFunction, ctx);
                }
            } finally {
                Releasables.close(fullBlocks);
                if (partialBlocks != null) {
                    Releasables.close(partialBlocks);
                }
            }
            PreparedForEvaluation delegate = finalAgg.prepareForEvaluate(
                selectedGroups,
                new TimeSeriesGroupingAggregatorEvaluationContext(ctx.driverContext()) {
                    // expand the window to cover the new range
                    @Override
                    public long rangeStartInMillis(int groupId) {
                        return ctx.rangeEndInMillis(groupId) - window.toMillis();
                    }

                    @Override
                    public long rangeEndInMillis(int groupId) {
                        return ctx.rangeEndInMillis(groupId);
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
                    public void forEachGroupInRange(int startingGroupId, long rangeStartMillis, long rangeEndMillis, IntConsumer action) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public void computeAdjacentGroupIds() {
                        // not used by previousGroupId and nextGroupId
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
        Page fullPage,
        @Nullable Page partialPage,
        long fullSpan,
        GroupingAggregatorFunction fn,
        TimeSeriesGroupingAggregatorEvaluationContext context
    ) {
        try (var oneGroup = context.driverContext().blockFactory().newConstantIntVector(startingGroupId, 1)) {
            long end = context.rangeEndInMillis(startingGroupId);
            context.forEachGroupInRange(startingGroupId, end - fullSpan, end, g -> {
                assert g != startingGroupId && g >= 0 && g < groupIdToPositions.length;
                int position = groupIdToPositions[g];
                fn.addIntermediateInput(position, oneGroup, startingGroupId, fullPage);
            });
            if (partialPage != null) {
                // the boundary bucket is the one right before the full span; derive its width from the starting group
                long bucketLength = end - context.rangeStartInMillis(startingGroupId);
                long fullStart = end - fullSpan;
                context.forEachGroupInRange(startingGroupId, fullStart - bucketLength, fullStart, g -> {
                    assert g != startingGroupId && g >= 0 && g < groupIdToPositions.length;
                    int position = groupIdToPositions[g];
                    fn.addIntermediateInput(position, oneGroup, startingGroupId, partialPage);
                });
            }
        }
    }

    @Override
    public int intermediateBlockCount() {
        // with a partial side this is the consumed input width (full state plus partial state); the wrapper never
        // emits intermediate state in that shape - see prepareEvaluateIntermediate
        return next.intermediateBlockCount() + (partialNext == null ? 0 : partialNext.intermediateBlockCount());
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(next, partialNext);
    }

    @Override
    public String toString() {
        return "Window[agg=" + next + ", window=" + window + (partial == null ? "" : ", partial=" + partial) + "]";
    }
}
