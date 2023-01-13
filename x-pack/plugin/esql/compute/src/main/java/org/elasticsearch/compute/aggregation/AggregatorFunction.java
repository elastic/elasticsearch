/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;

import java.util.function.IntFunction;

@Experimental
public interface AggregatorFunction {

    void addRawInput(Page page);

    void addIntermediateInput(Block block);

    Block evaluateIntermediate();

    Block evaluateFinal();

    record Factory(String name, String type, IntFunction<AggregatorFunction> build) implements Describable {
        public AggregatorFunction build(int inputChannel) {
            return build.apply(inputChannel);
        }

        @Override
        public String describe() {
            return type == null ? name : name + " of " + type;
        }
    }

    Factory AVG_DOUBLES = new Factory("avg", "doubles", AvgDoubleAggregatorFunction::create);
    Factory AVG_LONGS = new Factory("avg", "longs", AvgLongAggregatorFunction::create);

    Factory COUNT = new Factory("count", null, CountRowsAggregator::create);

    Factory MAX_DOUBLES = new Factory("max", "doubles", MaxDoubleAggregatorFunction::create);
    Factory MAX_LONGS = new Factory("max", "longs", MaxLongAggregatorFunction::create);

    Factory MEDIAN_ABSOLUTE_DEVIATION_DOUBLES = new Factory(
        "median_absolute_deviation",
        "doubles",
        MedianAbsoluteDeviationDoubleAggregatorFunction::create
    );
    Factory MEDIAN_ABSOLUTE_DEVIATION_LONGS = new Factory(
        "median_absolute_deviation",
        "longs",
        MedianAbsoluteDeviationLongAggregatorFunction::create
    );

    Factory MIN_DOUBLES = new Factory("min", "doubles", MinDoubleAggregatorFunction::create);
    Factory MIN_LONGS = new Factory("min", "longs", MinLongAggregatorFunction::create);

    Factory SUM_DOUBLES = new Factory("sum", "doubles", SumDoubleAggregatorFunction::create);
    Factory SUM_LONGS = new Factory("sum", "longs", SumLongAggregatorFunction::create);
}
