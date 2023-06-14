/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasable;

import java.util.function.Function;

@Experimental
public class GroupingAggregator implements Releasable {

    public static final Object[] EMPTY_PARAMS = new Object[] {};

    private final GroupingAggregatorFunction aggregatorFunction;

    private final AggregatorMode mode;

    private final int intermediateChannel;

    public interface Factory extends Function<DriverContext, GroupingAggregator>, Describable {}

    public record GroupingAggregatorFactory(
        // TODO remove when no longer used
        BigArrays bigArrays,
        AggregationName aggName,
        AggregationType aggType,
        Object[] parameters,
        AggregatorMode mode,
        int inputChannel
    ) implements Factory {

        public GroupingAggregatorFactory(
            BigArrays bigArrays,
            GroupingAggregatorFunction.Factory aggFunctionFactory,
            Object[] parameters,
            AggregatorMode mode,
            int inputChannel
        ) {
            this(bigArrays, aggFunctionFactory.name(), aggFunctionFactory.type(), parameters, mode, inputChannel);
        }

        public GroupingAggregatorFactory(
            BigArrays bigArrays,
            GroupingAggregatorFunction.Factory aggFunctionFactory,
            AggregatorMode mode,
            int inputChannel
        ) {
            this(bigArrays, aggFunctionFactory, EMPTY_PARAMS, mode, inputChannel);
        }

        @Override
        public GroupingAggregator apply(DriverContext driverContext) {
            return new GroupingAggregator(bigArrays, GroupingAggregatorFunction.of(aggName, aggType), parameters, mode, inputChannel);
        }

        @Override
        public String describe() {
            return GroupingAggregatorFunction.of(aggName, aggType).describe();
        }
    }

    public GroupingAggregator(
        BigArrays bigArrays,
        GroupingAggregatorFunction.Factory aggCreationFunc,
        Object[] parameters,
        AggregatorMode mode,
        int inputChannel
    ) {
        this.aggregatorFunction = aggCreationFunc.build(bigArrays, mode, inputChannel, parameters);
        this.mode = mode;
        this.intermediateChannel = mode.isInputPartial() ? inputChannel : -1;
    }

    public GroupingAggregator(GroupingAggregatorFunction aggregatorFunction, AggregatorMode mode, int inputChannel) {
        this.aggregatorFunction = aggregatorFunction;
        this.mode = mode;
        this.intermediateChannel = mode.isInputPartial() ? inputChannel : -1;
    }

    public void processPage(LongBlock groupIdBlock, Page page) {
        final LongVector groupIdVector = groupIdBlock.asVector();
        if (mode.isInputPartial()) {
            if (groupIdVector == null) {
                throw new IllegalStateException("Intermediate group id must not have nulls");
            }
            aggregatorFunction.addIntermediateInput(groupIdVector, page.getBlock(intermediateChannel));
        } else {
            if (groupIdVector != null) {
                aggregatorFunction.addRawInput(groupIdVector, page);
            } else {
                aggregatorFunction.addRawInput(groupIdBlock, page);
            }
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
    public Block evaluate(IntVector selected) {
        if (mode.isOutputPartial()) {
            return aggregatorFunction.evaluateIntermediate(selected);
        } else {
            return aggregatorFunction.evaluateFinal(selected);
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
