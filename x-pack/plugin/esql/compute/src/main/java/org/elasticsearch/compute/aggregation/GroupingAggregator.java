/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasable;

import java.util.function.Function;

public record GroupingAggregator(GroupingAggregatorFunction aggregatorFunction, AggregatorMode mode) implements Releasable {

    public interface Factory extends Function<DriverContext, GroupingAggregator>, Describable {

    }

    /**
     * The number of Blocks required for evaluation.
     */
    public int evaluateBlockCount() {
        return mode.isOutputPartial() ? aggregatorFunction.intermediateBlockCount() : 1;
    }

    /**
     * Prepare to process a single page of results.
     */
    public GroupingAggregatorFunction.AddInput prepareProcessPage(SeenGroupIds seenGroupIds, Page page) {
        if (mode.isInputPartial()) {
            return new GroupingAggregatorFunction.AddInput() {
                @Override
                public void add(int positionOffset, IntArrayBlock groupIds) {
                    aggregatorFunction.addIntermediateInput(positionOffset, groupIds, page);
                }

                @Override
                public void add(int positionOffset, IntBigArrayBlock groupIds) {
                    aggregatorFunction.addIntermediateInput(positionOffset, groupIds, page);
                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {
                    aggregatorFunction.addIntermediateInput(positionOffset, groupIds, page);
                }

                @Override
                public void close() {}
            };
        } else {
            return aggregatorFunction.prepareProcessRawInputPage(seenGroupIds, page);
        }
    }

    public GroupingAggregatorFunction.PreparedForEvaluation prepareForEvaluate(
        IntVector selected,
        GroupingAggregatorEvaluationContext ctx
    ) {
        if (mode.isOutputPartial()) {
            return aggregatorFunction.prepareEvaluateIntermediate(selected, ctx);
        } else {
            return aggregatorFunction.prepareEvaluateFinal(selected, ctx);
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
