/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasable;

import java.util.function.Function;

public class Aggregator implements Releasable {

    public static final Object[] EMPTY_PARAMS = new Object[] {};

    private final AggregatorFunction aggregatorFunction;

    private final AggregatorMode mode;

    public interface Factory extends Function<DriverContext, Aggregator>, Describable {}

    public Aggregator(AggregatorFunction aggregatorFunction, AggregatorMode mode) {
        this.aggregatorFunction = aggregatorFunction;
        this.mode = mode;
    }

    /** The number of Blocks required for evaluation. */
    public int evaluateBlockCount() {
        return mode.isOutputPartial() ? aggregatorFunction.intermediateBlockCount() : 1;
    }

    public void processPage(Page page) {
        if (mode.isInputPartial()) {
            aggregatorFunction.addIntermediateInput(page);
        } else {
            aggregatorFunction.addRawInput(page);
        }
    }

    public void evaluate(Block[] blocks, int offset, DriverContext driverContext) {
        if (mode.isOutputPartial()) {
            aggregatorFunction.evaluateIntermediate(blocks, offset, driverContext);
        } else {
            aggregatorFunction.evaluateFinal(blocks, offset, driverContext);
        }
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

    @Override
    public void close() {
        aggregatorFunction.close();
    }
}
