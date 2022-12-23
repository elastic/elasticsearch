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
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.core.Releasable;

@Experimental
public interface GroupingAggregatorFunction extends Releasable {

    void addRawInput(Vector groupIdBlock, Page page);

    void addIntermediateInput(Vector groupIdBlock, Block block);

    /**
     * Add the position-th row from the intermediate output of the given aggregator function to the groupId
     */
    void addIntermediateRowInput(int groupId, GroupingAggregatorFunction input, int position);

    Block evaluateIntermediate();

    Block evaluateFinal();

    abstract class Factory implements Describable {

        private final String name;

        Factory(String name) {
            this.name = name;
        }

        public abstract GroupingAggregatorFunction build(BigArrays bigArrays, AggregatorMode mode, int inputChannel);

        @Override
        public String describe() {
            return name;
        }
    }

    Factory AVG = new Factory("avg") {
        @Override
        public GroupingAggregatorFunction build(BigArrays bigArrays, AggregatorMode mode, int inputChannel) {
            if (mode.isInputPartial()) {
                return GroupingAvgAggregator.createIntermediate(bigArrays);
            } else {
                return GroupingAvgAggregator.create(bigArrays, inputChannel);
            }
        }
    };

    Factory COUNT = new Factory("count") {
        @Override
        public GroupingAggregatorFunction build(BigArrays bigArrays, AggregatorMode mode, int inputChannel) {
            if (mode.isInputPartial()) {
                return GroupingCountAggregator.createIntermediate(bigArrays);
            } else {
                return GroupingCountAggregator.create(bigArrays, inputChannel);
            }
        }
    };

    Factory MIN = new Factory("min") {
        @Override
        public GroupingAggregatorFunction build(BigArrays bigArrays, AggregatorMode mode, int inputChannel) {
            if (mode.isInputPartial()) {
                return GroupingMinAggregator.createIntermediate(bigArrays);
            } else {
                return GroupingMinAggregator.create(bigArrays, inputChannel);
            }
        }
    };

    Factory MAX = new Factory("max") {
        @Override
        public GroupingAggregatorFunction build(BigArrays bigArrays, AggregatorMode mode, int inputChannel) {
            if (mode.isInputPartial()) {
                return GroupingMaxAggregator.createIntermediate(bigArrays);
            } else {
                return GroupingMaxAggregator.create(bigArrays, inputChannel);
            }
        }
    };

    Factory SUM = new Factory("sum") {
        @Override
        public GroupingAggregatorFunction build(BigArrays bigArrays, AggregatorMode mode, int inputChannel) {
            if (mode.isInputPartial()) {
                return GroupingSumAggregator.createIntermediate(bigArrays);
            } else {
                return GroupingSumAggregator.create(bigArrays, inputChannel);
            }
        }
    };
}
