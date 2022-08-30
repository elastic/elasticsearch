/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.aggregation;

import org.elasticsearch.xpack.sql.action.compute.data.Block;
import org.elasticsearch.xpack.sql.action.compute.data.Page;

import java.util.function.BiFunction;

public interface AggregatorFunction {

    void addRawInput(Page page);

    void addIntermediateInput(Block block);

    Block evaluateIntermediate();

    Block evaluateFinal();

    BiFunction<AggregatorMode, Integer, AggregatorFunction> avg = (AggregatorMode mode, Integer inputChannel) -> {
        if (mode.isInputPartial()) {
            return AvgAggregator.create(inputChannel);
        } else {
            return AvgAggregator.createIntermediate();
        }
    };

    BiFunction<AggregatorMode, Integer, AggregatorFunction> count = (AggregatorMode mode, Integer inputChannel) -> {
        if (mode.isInputPartial()) {
            return CountRowsAggregator.create(inputChannel);
        } else {
            return CountRowsAggregator.createIntermediate();
        }
    };

    BiFunction<AggregatorMode, Integer, AggregatorFunction> max = (AggregatorMode mode, Integer inputChannel) -> {
        if (mode.isInputPartial()) {
            return MaxAggregator.create(inputChannel);
        } else {
            return MaxAggregator.createIntermediate();
        }
    };
}
