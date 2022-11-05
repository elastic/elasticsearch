/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.Experimental;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;

import java.util.function.BiFunction;
import java.util.function.Supplier;

@Experimental
public class Aggregator {
    private final AggregatorFunction aggregatorFunction;

    private final AggregatorMode mode;

    private final int intermediateChannel;

    public record AggregatorFactory(AggregatorFunction.AggregatorFunctionFactory aggCreationFunc, AggregatorMode mode, int inputChannel)
        implements
            Supplier<Aggregator>,
            Describable {
        @Override
        public Aggregator get() {
            return new Aggregator(aggCreationFunc, mode, inputChannel);
        }

        @Override
        public String describe() {
            return aggCreationFunc.describe();
        }
    }

    public Aggregator(BiFunction<AggregatorMode, Integer, AggregatorFunction> aggCreationFunc, AggregatorMode mode, int inputChannel) {
        this.aggregatorFunction = aggCreationFunc.apply(mode, inputChannel);
        this.mode = mode;
        this.intermediateChannel = mode.isInputPartial() ? inputChannel : -1;
    }

    public void processPage(Page page) {
        if (mode.isInputPartial()) {
            aggregatorFunction.addIntermediateInput(page.getBlock(intermediateChannel));
        } else {
            aggregatorFunction.addRawInput(page);
        }
    }

    public Block evaluate() {
        if (mode.isOutputPartial()) {
            return aggregatorFunction.evaluateIntermediate();
        } else {
            return aggregatorFunction.evaluateFinal();
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
}
