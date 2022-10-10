/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;

import java.util.function.BiFunction;

public interface AggregatorFunction {

    void addRawInput(Page page);

    void addIntermediateInput(Block block);

    Block evaluateIntermediate();

    Block evaluateFinal();

    BiFunction<AggregatorMode, Integer, AggregatorFunction> doubleAvg = (AggregatorMode mode, Integer inputChannel) -> {
        if (mode.isInputPartial()) {
            return DoubleAvgAggregator.createIntermediate();
        } else {
            return DoubleAvgAggregator.create(inputChannel);
        }
    };

    BiFunction<AggregatorMode, Integer, AggregatorFunction> longAvg = (AggregatorMode mode, Integer inputChannel) -> {
        if (mode.isInputPartial()) {
            return LongAvgAggregator.createIntermediate();
        } else {
            return LongAvgAggregator.create(inputChannel);
        }
    };

    BiFunction<AggregatorMode, Integer, AggregatorFunction> count = (AggregatorMode mode, Integer inputChannel) -> {
        if (mode.isInputPartial()) {
            return CountRowsAggregator.createIntermediate();
        } else {
            return CountRowsAggregator.create(inputChannel);
        }
    };

    BiFunction<AggregatorMode, Integer, AggregatorFunction> max = (AggregatorMode mode, Integer inputChannel) -> {
        if (mode.isInputPartial()) {
            return MaxAggregator.createIntermediate();
        } else {
            return MaxAggregator.create(inputChannel);
        }
    };

    BiFunction<AggregatorMode, Integer, AggregatorFunction> sum = (AggregatorMode mode, Integer inputChannel) -> {
        if (mode.isInputPartial()) {
            return SumAggregator.createIntermediate();
        } else {
            return SumAggregator.create(inputChannel);
        }
    };
}
