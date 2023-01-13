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
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;

import java.util.function.BiFunction;

@Experimental
public interface GroupingAggregatorFunction extends Releasable {

    void addRawInput(LongVector groupIdBlock, Page page);

    void addIntermediateInput(LongVector groupIdBlock, Block block);

    /**
     * Add the position-th row from the intermediate output of the given aggregator function to the groupId
     */
    void addIntermediateRowInput(int groupId, GroupingAggregatorFunction input, int position);

    Block evaluateIntermediate();

    Block evaluateFinal();

    record Factory(String name, String type, BiFunction<BigArrays, Integer, GroupingAggregatorFunction> create) implements Describable {
        public GroupingAggregatorFunction build(BigArrays bigArrays, AggregatorMode mode, int inputChannel) {
            if (mode.isInputPartial()) {
                return create.apply(bigArrays, -1);
            } else {
                return create.apply(bigArrays, inputChannel);
            }
        }

        @Override
        public String describe() {
            return type == null ? name : name + " of " + type;
        }
    }

    Factory AVG_DOUBLES = new Factory("avg", "doubles", AvgDoubleGroupingAggregatorFunction::create);
    Factory AVG_LONGS = new Factory("avg", "longs", AvgLongGroupingAggregatorFunction::create);

    Factory COUNT = new Factory("count", null, GroupingCountAggregator::create);

    Factory MIN_DOUBLES = new Factory("min", "doubles", MinDoubleGroupingAggregatorFunction::create);
    Factory MIN_LONGS = new Factory("min", "longs", MinLongGroupingAggregatorFunction::create);

    Factory MAX_DOUBLES = new Factory("max", "doubles", MaxDoubleGroupingAggregatorFunction::create);
    Factory MAX_LONGS = new Factory("max", "longs", MaxLongGroupingAggregatorFunction::create);

    Factory MEDIAN_ABSOLUTE_DEVIATION_DOUBLES = new Factory(
        "median_absolute_deviation",
        "doubles",
        MedianAbsoluteDeviationDoubleGroupingAggregatorFunction::create
    );

    Factory MEDIAN_ABSOLUTE_DEVIATION_LONGS = new Factory(
        "median_absolute_deviation",
        "longs",
        MedianAbsoluteDeviationLongGroupingAggregatorFunction::create
    );

    Factory SUM_DOUBLES = new Factory("sum", "doubles", SumDoubleGroupingAggregatorFunction::create);
    Factory SUM_LONGS = new Factory("sum", "longs", SumLongGroupingAggregatorFunction::create);
}
