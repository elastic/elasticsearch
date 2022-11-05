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

@Experimental
public interface AggregatorFunction {

    void addRawInput(Page page);

    void addIntermediateInput(Block block);

    Block evaluateIntermediate();

    Block evaluateFinal();

    abstract class AggregatorFunctionFactory implements BiFunction<AggregatorMode, Integer, AggregatorFunction>, Describable {

        private final String name;

        AggregatorFunctionFactory(String name) {
            this.name = name;
        }

        @Override
        public String describe() {
            return name;
        }
    }

    AggregatorFunctionFactory doubleAvg = new AggregatorFunctionFactory("doubleAvg") {
        @Override
        public AggregatorFunction apply(AggregatorMode mode, Integer inputChannel) {
            if (mode.isInputPartial()) {
                return DoubleAvgAggregator.createIntermediate();
            } else {
                return DoubleAvgAggregator.create(inputChannel);
            }
        }
    };

    AggregatorFunctionFactory longAvg = new AggregatorFunctionFactory("longAvg") {
        @Override
        public AggregatorFunction apply(AggregatorMode mode, Integer inputChannel) {
            if (mode.isInputPartial()) {
                return LongAvgAggregator.createIntermediate();
            } else {
                return LongAvgAggregator.create(inputChannel);
            }
        }
    };

    AggregatorFunctionFactory count = new AggregatorFunctionFactory("count") {
        @Override
        public AggregatorFunction apply(AggregatorMode mode, Integer inputChannel) {
            if (mode.isInputPartial()) {
                return CountRowsAggregator.createIntermediate();
            } else {
                return CountRowsAggregator.create(inputChannel);
            }
        }
    };

    AggregatorFunctionFactory max = new AggregatorFunctionFactory("max") {
        @Override
        public AggregatorFunction apply(AggregatorMode mode, Integer inputChannel) {
            if (mode.isInputPartial()) {
                return MaxAggregator.createIntermediate();
            } else {
                return MaxAggregator.create(inputChannel);
            }
        }
    };

    AggregatorFunctionFactory sum = new AggregatorFunctionFactory("sum") {
        @Override
        public AggregatorFunction apply(AggregatorMode mode, Integer inputChannel) {
            if (mode.isInputPartial()) {
                return SumAggregator.createIntermediate();
            } else {
                return SumAggregator.create(inputChannel);
            }
        }
    };
}
