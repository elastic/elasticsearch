/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasable;

import java.util.function.Function;

public class GroupingAggregator implements Releasable {
    private final GroupingAggregatorFunction aggregatorFunction;

    private final AggregatorMode mode;

    public interface Factory extends Function<DriverContext, GroupingAggregator>, Describable {}

    public GroupingAggregator(GroupingAggregatorFunction aggregatorFunction, AggregatorMode mode) {
        this.aggregatorFunction = aggregatorFunction;
        this.mode = mode;
    }

    /** The number of Blocks required for evaluation. */
    public int evaluateBlockCount() {
        return mode.isOutputPartial() ? aggregatorFunction.intermediateBlockCount() : 1;
    }

    /**
     * Prepare to process a single page of results.
     */
    public GroupingAggregatorFunction.AddInput prepareProcessPage(Page page) {
        if (mode.isInputPartial()) {
            return new GroupingAggregatorFunction.AddInput() {
                @Override
                public void add(int positionOffset, LongBlock groupIds) {
                    throw new IllegalStateException("Intermediate group id must not have nulls");
                }

                @Override
                public void add(int positionOffset, LongVector groupIds) {
                    aggregatorFunction.addIntermediateInput(positionOffset, groupIds, page);
                }
            };
        } else {
            return aggregatorFunction.prepareProcessPage(page);
        }
    }

    /**
     * Add the position-th row from the intermediate output of the given aggregator to this aggregator at the groupId position
     */
    public void addIntermediateRow(int groupId, GroupingAggregator input, int position) {
        aggregatorFunction.addIntermediateRowInput(groupId, input.aggregatorFunction, position);
    }

    /**
     * Build the results for this aggregation.
     * @param selected the groupIds that have been selected to be included in
     *                 the results. Always ascending.
     */
    public void evaluate(Block[] blocks, int offset, IntVector selected) {
        if (mode.isOutputPartial()) {
            aggregatorFunction.evaluateIntermediate(blocks, offset, selected);
        } else {
            aggregatorFunction.evaluateFinal(blocks, offset, selected);
        }
    }

    @Override
    public void close() {
        aggregatorFunction.close();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("[");
        sb.append("aggregatorFunction=").append(aggregatorFunction).append(", ");
        sb.append("mode=").append(mode);
        sb.append("]");
        return sb.toString();
    }
}
