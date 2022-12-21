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
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OperatorTestCase;
import org.elasticsearch.compute.operator.PageConsumerOperator;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public abstract class AggregatorTestCase extends OperatorTestCase {
    protected abstract AggregatorFunction.Provider aggregatorFunction();

    protected abstract void assertSimpleResult(int end, Block result);

    @Override
    protected final Operator simple(BigArrays bigArrays) {
        return operator(AggregatorMode.SINGLE);
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

    public void testInitialFinal() {
        int end = between(1_000, 100_000);
        List<Page> results = new ArrayList<>();

        try (
            Driver d = new Driver(
                simpleInput(end),
                List.of(operator(AggregatorMode.INITIAL), operator(AggregatorMode.FINAL)),
                new PageConsumerOperator(page -> results.add(page)),
                () -> {}
            )
        ) {
            d.run();
        }
        assertSimpleOutput(end, results);
    }

    public void testInitialIntermediateFinal() {
        int end = between(1_000, 100_000);
        List<Page> results = new ArrayList<>();

        try (
            Driver d = new Driver(
                simpleInput(end),
                List.of(operator(AggregatorMode.INITIAL), operator(AggregatorMode.INTERMEDIATE), operator(AggregatorMode.FINAL)),
                new PageConsumerOperator(page -> results.add(page)),
                () -> {}
            )
        ) {
            d.run();
        }
        assertSimpleOutput(end, results);
    }

    protected final Operator operator(AggregatorMode mode) {
        return new AggregationOperator(List.of(new Aggregator.AggregatorFactory(aggregatorFunction(), mode, 0).get()));
    }
}
