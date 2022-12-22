/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AggregationOperator;
import org.elasticsearch.compute.operator.ForkingOperatorTestCase;
import org.elasticsearch.compute.operator.Operator;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public abstract class AggregatorTestCase extends ForkingOperatorTestCase {
    protected abstract AggregatorFunction.Factory aggregatorFunction();

    protected abstract String expectedDescriptionOfAggregator();

    protected abstract void assertSimpleResult(int end, Block result);

    // TODO tests for no input
    // TODO tests for description

    @Override
    protected Operator.OperatorFactory simpleWithMode(BigArrays bigArrays, AggregatorMode mode) {
        return new AggregationOperator.AggregationOperatorFactory(
            List.of(new Aggregator.AggregatorFactory(aggregatorFunction(), mode, 0)),
            mode
        );
    }

    @Override
    protected final String expectedDescriptionOfSimple() {
        return "AggregationOperator(mode = SINGLE, aggs = " + expectedDescriptionOfAggregator() + ")";
    }

    @Override
    protected final void assertSimpleOutput(int end, List<Page> results) {
        assertThat(results, hasSize(1));
        assertThat(results.get(0).getBlockCount(), equalTo(1));
        assertThat(results.get(0).getPositionCount(), equalTo(1));

        Block result = results.get(0).getBlock(0);
        assertSimpleResult(end, result);
    }

    @Override
    protected ByteSizeValue smallEnoughToCircuitBreak() {
        assumeTrue("doesn't use big array so never breaks", false);
        return null;
    }
}
