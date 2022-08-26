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

public class Aggregator {
    private final AggregatorFunction aggregatorFunction;

    private final AggregatorMode mode;

    private final int intermediateChannel;

    public Aggregator(BiFunction<AggregatorMode, Integer, AggregatorFunction> aggCreationFunc, AggregatorMode mode, int inputChannel) {
        this.aggregatorFunction = aggCreationFunc.apply(mode, inputChannel);
        this.mode = mode;
        if (mode.isInputRaw()) {
            intermediateChannel = -1;
        } else {
            this.intermediateChannel = inputChannel;
        }
    }

    public void processPage(Page page) {
        if (mode.isInputRaw()) {
            aggregatorFunction.addRawInput(page);
        } else {
            aggregatorFunction.addIntermediateInput(page.getBlock(intermediateChannel));
        }
    }

    public Block evaluate() {
        if (mode.isOutputPartial()) {
            return aggregatorFunction.evaluateIntermediate();
        } else {
            return aggregatorFunction.evaluateFinal();
        }
    }
}
