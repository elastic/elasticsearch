/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.Experimental;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.core.Releasable;

import java.util.function.Supplier;

@Experimental
public class GroupingAggregator implements Releasable {
    private final GroupingAggregatorFunction aggregatorFunction;

    private final AggregatorMode mode;

    private final int intermediateChannel;

    public record GroupingAggregatorFactory(
        BigArrays bigArrays,
        GroupingAggregatorFunction.Factory aggCreationFunc,
        AggregatorMode mode,
        int inputChannel
    ) implements Supplier<GroupingAggregator>, Describable {

        @Override
        public GroupingAggregator get() {
            return new GroupingAggregator(bigArrays, aggCreationFunc, mode, inputChannel);
        }

        @Override
        public String describe() {
            return aggCreationFunc.describe();
        }
    }

    public GroupingAggregator(
        BigArrays bigArrays,
        GroupingAggregatorFunction.Factory aggCreationFunc,
        AggregatorMode mode,
        int inputChannel
    ) {
        this.aggregatorFunction = aggCreationFunc.build(bigArrays, mode, inputChannel);
        this.mode = mode;
        this.intermediateChannel = mode.isInputPartial() ? inputChannel : -1;
    }

    public void processPage(Vector groupIdVector, Page page) {
        if (mode.isInputPartial()) {
            aggregatorFunction.addIntermediateInput(groupIdVector, page.getBlock(intermediateChannel));
        } else {
            aggregatorFunction.addRawInput(groupIdVector, page);
        }
    }

    /**
     * Add the position-th row from the intermediate output of the given aggregator to this aggregator at the groupId position
     */
    public void addIntermediateRow(int groupId, GroupingAggregator input, int position) {
        aggregatorFunction.addIntermediateRowInput(groupId, input.aggregatorFunction, position);
    }

    public Block evaluate() {
        if (mode.isOutputPartial()) {
            return aggregatorFunction.evaluateIntermediate();
        } else {
            return aggregatorFunction.evaluateFinal();
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
