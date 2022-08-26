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

public interface GroupingAggregatorFunction {

    void addRawInput(Block groupIdBlock, Page page);

    void addIntermediateInput(Block groupIdBlock, Block block);

    Block evaluateIntermediate();

    Block evaluateFinal();

    BiFunction<AggregatorMode, Integer, GroupingAggregatorFunction> avg = (AggregatorMode mode, Integer inputChannel) -> {
        if (mode.isInputRaw()) {
            return GroupingAvgAggregator.create(inputChannel);
        } else {
            return GroupingAvgAggregator.createIntermediate();
        }
    };
}
