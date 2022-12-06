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

import java.util.function.Supplier;

@Experimental
public class Aggregator {
    private final AggregatorFunction aggregatorFunction;

    private final AggregatorMode mode;

    private final int intermediateChannel;

    public record AggregatorFactory(AggregatorFunction.Provider provider, AggregatorMode mode, int inputChannel)
        implements
            Supplier<Aggregator>,
            Describable {
        @Override
        public Aggregator get() {
            return new Aggregator(provider, mode, inputChannel);
        }

        @Override
        public String describe() {
            return provider.describe();
        }
    }

    public Aggregator(AggregatorFunction.Provider provider, AggregatorMode mode, int inputChannel) {
        assert mode.isInputPartial() || inputChannel >= 0;
        // input channel is used both to signal the creation of the page (when the input is not partial)
        this.aggregatorFunction = provider.create(mode.isInputPartial() ? -1 : inputChannel);
        // and to indicate the page during the intermediate phase
        this.intermediateChannel = mode.isInputPartial() ? inputChannel : -1;
        this.mode = mode;
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
