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

    Factory AVG_DOUBLE = new Factory("avg", "doubles", DoubleAvgAggregator::create);

    Factory AVG_LONG = new Factory("avg", "longs", LongAvgAggregator::create);

    Factory COUNT = new Factory("count", null, CountRowsAggregator::create);

    Factory MAX = new Factory("max", null, MaxAggregator::create);

    Factory MIN = new Factory("min", null, MinAggregator::create);

    Factory SUM = new Factory("sum", null, SumAggregator::create);
}
