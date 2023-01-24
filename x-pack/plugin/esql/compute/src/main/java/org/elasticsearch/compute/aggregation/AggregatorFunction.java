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

import static org.elasticsearch.compute.aggregation.AggregationName.avg;
import static org.elasticsearch.compute.aggregation.AggregationName.count;
import static org.elasticsearch.compute.aggregation.AggregationName.max;
import static org.elasticsearch.compute.aggregation.AggregationName.median;
import static org.elasticsearch.compute.aggregation.AggregationName.median_absolute_deviation;
import static org.elasticsearch.compute.aggregation.AggregationName.min;
import static org.elasticsearch.compute.aggregation.AggregationName.sum;
import static org.elasticsearch.compute.aggregation.AggregationType.agnostic;
import static org.elasticsearch.compute.aggregation.AggregationType.doubles;
import static org.elasticsearch.compute.aggregation.AggregationType.longs;

@Experimental
public interface AggregatorFunction {

    void addRawInput(Page page);

    void addIntermediateInput(Block block);

    Block evaluateIntermediate();

    Block evaluateFinal();

    record Factory(AggregationName name, AggregationType type, IntFunction<AggregatorFunction> build) implements Describable {
        public AggregatorFunction build(int inputChannel) {
            return build.apply(inputChannel);
        }

        @Override
        public String describe() {
            return type == agnostic ? name.name() : name + " of " + type;
        }
    }

    static Factory of(AggregationName name, AggregationType type) {
        return switch (type) {
            case agnostic -> switch (name) {
                    case count -> COUNT;
                    default -> throw new IllegalArgumentException("unknown " + name + ", type:" + type);
                };
            case longs -> switch (name) {
                    case avg -> AVG_LONGS;
                    case count -> COUNT;
                    case max -> MAX_LONGS;
                    case median -> MEDIAN_LONGS;
                    case median_absolute_deviation -> MEDIAN_ABSOLUTE_DEVIATION_LONGS;
                    case min -> MIN_LONGS;
                    case sum -> SUM_LONGS;
                };
            case doubles -> switch (name) {
                    case avg -> AVG_DOUBLES;
                    case count -> COUNT;
                    case max -> MAX_DOUBLES;
                    case median -> MEDIAN_DOUBLES;
                    case median_absolute_deviation -> MEDIAN_ABSOLUTE_DEVIATION_DOUBLES;
                    case min -> MIN_DOUBLES;
                    case sum -> SUM_DOUBLES;
                };
        };
    }

    Factory AVG_DOUBLES = new Factory(avg, doubles, AvgDoubleAggregatorFunction::create);
    Factory AVG_LONGS = new Factory(avg, longs, AvgLongAggregatorFunction::create);

    Factory COUNT = new Factory(count, agnostic, CountAggregatorFunction::create);

    Factory MAX_DOUBLES = new Factory(max, doubles, MaxDoubleAggregatorFunction::create);
    Factory MAX_LONGS = new Factory(max, longs, MaxLongAggregatorFunction::create);

    Factory MEDIAN_DOUBLES = new Factory(median, doubles, MedianDoubleAggregatorFunction::create);
    Factory MEDIAN_LONGS = new Factory(median, longs, MedianLongAggregatorFunction::create);

    Factory MEDIAN_ABSOLUTE_DEVIATION_DOUBLES = new Factory(
        median_absolute_deviation,
        doubles,
        MedianAbsoluteDeviationDoubleAggregatorFunction::create
    );
    Factory MEDIAN_ABSOLUTE_DEVIATION_LONGS = new Factory(
        median_absolute_deviation,
        longs,
        MedianAbsoluteDeviationLongAggregatorFunction::create
    );

    Factory MIN_DOUBLES = new Factory(min, doubles, MinDoubleAggregatorFunction::create);
    Factory MIN_LONGS = new Factory(min, longs, MinLongAggregatorFunction::create);

    Factory SUM_DOUBLES = new Factory(sum, doubles, SumDoubleAggregatorFunction::create);
    Factory SUM_LONGS = new Factory(sum, longs, SumLongAggregatorFunction::create);
}
