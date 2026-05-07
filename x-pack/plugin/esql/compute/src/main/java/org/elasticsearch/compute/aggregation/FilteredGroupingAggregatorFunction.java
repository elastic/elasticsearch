/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.ToMask;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.core.Releasables;

import java.util.stream.IntStream;

/**
 * A {@link GroupingAggregatorFunction} that wraps another, filtering which positions
 * are supplied to the aggregator.
 * <p>
 *     This filtering works by setting all of the group ids for filtered positions to
 *     {@code null}. {@link GroupingAggregatorFunction} will then skip collecting those
 *     positions.
 * </p>
 */
public record FilteredGroupingAggregatorFunction(GroupingAggregatorFunction next, ExpressionEvaluator filter)
    implements
        GroupingAggregatorFunction {

    public FilteredGroupingAggregatorFunction {
        // Filtered aggregators may filter out entire pages, so the underlying aggregator
        // may not see all groups. It needs to know this so it can initialize unseen groups to null.
        next.selectedMayContainUnseenGroups(new SeenGroupIds.Empty());
    }

    @Override
    public AddInput prepareProcessRawInputPage(SeenGroupIds seenGroupIds, Page page) {
        try (BooleanBlock filterResult = ((BooleanBlock) filter.eval(page))) {
            ToMask mask = filterResult.toMask();
            // TODO warn on mv fields
            AddInput nextAdd = null;
            try {
                nextAdd = next.prepareProcessRawInputPage(seenGroupIds, page);
                if (nextAdd == null) {
                    return null;
                }
                AddInput result = new FilteredAddInput(mask.mask(), nextAdd, page.getPositionCount());
                mask = null;
                nextAdd = null;
                return result;
            } finally {
                Releasables.close(mask, nextAdd);
            }
        }
    }

    private record FilteredAddInput(BooleanVector mask, AddInput nextAdd, int positionCount) implements AddInput {
        @Override
        public void add(int positionOffset, IntArrayBlock groupIds) {
            addBlock(positionOffset, groupIds);
        }

        @Override
        public void add(int positionOffset, IntBigArrayBlock groupIds) {
            addBlock(positionOffset, groupIds);
        }

        @Override
        public void add(int positionOffset, IntVector groupIds) {
            addBlock(positionOffset, groupIds.asBlock());
        }

        private void addBlock(int positionOffset, IntBlock groupIds) {
            if (positionOffset == 0) {
                try (IntBlock filtered = groupIds.keepMask(mask)) {
                    nextAdd.add(positionOffset, filtered);
                }
            } else {
                try (
                    BooleanVector offsetMask = mask.filter(
                        false,
                        IntStream.range(positionOffset, positionOffset + groupIds.getPositionCount()).toArray()
                    );
                    IntBlock filtered = groupIds.keepMask(offsetMask)
                ) {
                    nextAdd.add(positionOffset, filtered);
                }
            }
        }

        @Override
        public void close() {
            Releasables.close(mask, nextAdd);
        }
    }

    @Override
    public void selectedMayContainUnseenGroups(SeenGroupIds seenGroupIds) {
        // nothing to do - we already put the underlying agg into this state
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
        IntVector selected,
        GroupingAggregatorEvaluationContext ctx
    ) {
        return next.prepareEvaluateFinal(selected, ctx);
    }

    @Override
    public int intermediateBlockCount() {
        return next.intermediateBlockCount();
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(next, filter);
    }
}
