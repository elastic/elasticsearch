/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.Experimental;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;

import java.util.function.BiFunction;

@Experimental
public interface GroupingAggregatorFunction {

    void addRawInput(Block groupIdBlock, Page page);

    void addIntermediateInput(Block groupIdBlock, Block block);

    Block evaluateIntermediate();

    Block evaluateFinal();

    abstract class GroupingAggregatorFunctionFactory
        implements
            BiFunction<AggregatorMode, Integer, GroupingAggregatorFunction>,
            Describable {

        private final String name;

        GroupingAggregatorFunctionFactory(String name) {
            this.name = name;
        }

        @Override
        public String describe() {
            return name;
        }
    }

    GroupingAggregatorFunctionFactory avg = new GroupingAggregatorFunctionFactory("avg") {
        @Override
        public GroupingAggregatorFunction apply(AggregatorMode mode, Integer inputChannel) {
            // TODO real BigArrays
            if (mode.isInputPartial()) {
                return GroupingAvgAggregator.createIntermediate(BigArrays.NON_RECYCLING_INSTANCE);
            } else {
                return GroupingAvgAggregator.create(BigArrays.NON_RECYCLING_INSTANCE, inputChannel);
            }
        }
    };

    GroupingAggregatorFunctionFactory count = new GroupingAggregatorFunctionFactory("count") {
        @Override
        public GroupingAggregatorFunction apply(AggregatorMode mode, Integer inputChannel) {
            if (mode.isInputPartial()) {
                return GroupingCountAggregator.createIntermediate();
            } else {
                return GroupingCountAggregator.create(inputChannel);
            }
        }
    };

    GroupingAggregatorFunctionFactory min = new GroupingAggregatorFunctionFactory("min") {
        @Override
        public GroupingAggregatorFunction apply(AggregatorMode mode, Integer inputChannel) {
            if (mode.isInputPartial()) {
                return GroupingMinAggregator.createIntermediate();
            } else {
                return GroupingMinAggregator.create(inputChannel);
            }
        }
    };

    GroupingAggregatorFunctionFactory max = new GroupingAggregatorFunctionFactory("max") {
        @Override
        public GroupingAggregatorFunction apply(AggregatorMode mode, Integer inputChannel) {
            if (mode.isInputPartial()) {
                return GroupingMaxAggregator.createIntermediate();
            } else {
                return GroupingMaxAggregator.create(inputChannel);
            }
        }
    };

    GroupingAggregatorFunctionFactory sum = new GroupingAggregatorFunctionFactory("sum") {
        @Override
        public GroupingAggregatorFunction apply(AggregatorMode mode, Integer inputChannel) {
            if (mode.isInputPartial()) {
                return GroupingSumAggregator.createIntermediate();
            } else {
                return GroupingSumAggregator.create(inputChannel);
            }
        }
    };
}
