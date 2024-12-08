/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.ToMask;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
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
record FilteredGroupingAggregatorFunction(GroupingAggregatorFunction next, EvalOperator.ExpressionEvaluator filter)
    implements
        GroupingAggregatorFunction {

    FilteredGroupingAggregatorFunction {
        next.selectedMayContainUnseenGroups(new SeenGroupIds.Empty());
    }

    @Override
    public AddInput prepareProcessPage(SeenGroupIds seenGroupIds, Page page) {
        try (BooleanBlock filterResult = ((BooleanBlock) filter.eval(page))) {
            ToMask mask = filterResult.toMask();
            // TODO warn on mv fields
            AddInput nextAdd = null;
            try {
                nextAdd = next.prepareProcessPage(seenGroupIds, page);
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
        public void add(int positionOffset, IntBlock groupIds) {
            if (positionOffset == 0) {
                try (IntBlock filtered = groupIds.keepMask(mask)) {
                    nextAdd.add(positionOffset, filtered);
                }
            } else {
                try (
                    BooleanVector offsetMask = mask.filter(
                        IntStream.range(positionOffset, positionOffset + groupIds.getPositionCount()).toArray()
                    );
                    IntBlock filtered = groupIds.keepMask(offsetMask)
                ) {
                    nextAdd.add(positionOffset, filtered);
                }
            }
        }

        @Override
        public void add(int positionOffset, IntVector groupIds) {
            add(positionOffset, groupIds.asBlock());
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
    public void addIntermediateInput(int positionOffset, IntVector groupIdVector, Page page) {
        next.addIntermediateInput(positionOffset, groupIdVector, page);
    }

    @Override
    public void addIntermediateRowInput(int groupId, GroupingAggregatorFunction input, int position) {
        next.addIntermediateRowInput(groupId, ((FilteredGroupingAggregatorFunction) input).next(), position);
    }

    @Override
    public void evaluateIntermediate(Block[] blocks, int offset, IntVector selected) {
        next.evaluateIntermediate(blocks, offset, selected);
    }

    @Override
    public void evaluateFinal(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
        next.evaluateFinal(blocks, offset, selected, driverContext);
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
