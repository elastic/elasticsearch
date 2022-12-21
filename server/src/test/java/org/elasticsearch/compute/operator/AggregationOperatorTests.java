/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.Aggregator;
import org.elasticsearch.compute.aggregation.AggregatorFunctionProviders;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.AvgLongAggregatorTests;
import org.elasticsearch.compute.aggregation.MaxAggregatorTests;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class AggregationOperatorTests extends OperatorTestCase {
    @Override
    protected Operator simple(BigArrays bigArrays) {
        return operator(AggregatorMode.SINGLE, 0, 0);
    }

    @Override
    protected void assertSimpleOutput(int end, List<Page> results) {
        assertThat(results, hasSize(1));
        assertThat(results.get(0).getBlockCount(), equalTo(2));
        assertThat(results.get(0).getPositionCount(), equalTo(1));

        AvgLongAggregatorTests avg = new AvgLongAggregatorTests();
        MaxAggregatorTests max = new MaxAggregatorTests();

        Block avgs = results.get(0).getBlock(0);
        Block maxs = results.get(0).getBlock(1);
        avg.assertSimpleResult(end, avgs);
        max.assertSimpleResult(end, maxs);
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
                List.of(operator(AggregatorMode.INITIAL, 0, 0), operator(AggregatorMode.FINAL, 0, 1)),
                new PageConsumerOperator(page -> results.add(page)),
                () -> {}
            )
        ) {
            d.run();
        }
        assertSimpleOutput(end, results);
    }

    public void testManyInitialFinal() {
        int end = between(1_000, 100_000);

        List<Page> partials = oneDriverPerPage(simpleInput(end), () -> List.of(operator(AggregatorMode.INITIAL, 0, 0)));

        List<Page> results = new ArrayList<>();
        try (
            Driver d = new Driver(
                new CannedSourceOperator(partials.iterator()),
                List.of(operator(AggregatorMode.FINAL, 0, 1)),
                new PageConsumerOperator(results::add),
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
                List.of(
                    operator(AggregatorMode.INITIAL, 0, 0),
                    operator(AggregatorMode.INTERMEDIATE, 0, 1),
                    operator(AggregatorMode.FINAL, 0, 1)
                ),
                new PageConsumerOperator(page -> results.add(page)),
                () -> {}
            )
        ) {
            d.run();
        }
        assertSimpleOutput(end, results);
    }

    private <T> Collection<List<T>> randomSplits(List<T> in) {
        return in.stream().collect(Collectors.groupingBy(s -> randomInt(in.size() - 1))).values();
    }

    public void testManyInitialManyPartialFinal() {
        int end = between(1_000, 100_000);

        List<Page> partials = oneDriverPerPage(simpleInput(end), () -> List.of(operator(AggregatorMode.INITIAL, 0, 0)));
        Collections.shuffle(partials, random());
        List<Page> intermediates = oneDriverPerPageList(
            randomSplits(partials).iterator(),
            () -> List.of(operator(AggregatorMode.INTERMEDIATE, 0, 1))
        );

        List<Page> results = new ArrayList<>();
        try (
            Driver d = new Driver(
                new CannedSourceOperator(intermediates.iterator()),
                List.of(operator(AggregatorMode.FINAL, 0, 1)),
                new PageConsumerOperator(results::add),
                () -> {}
            )
        ) {
            d.run();
        }
        assertSimpleOutput(end, results);
    }

    private Operator operator(AggregatorMode mode, int channel1, int channel2) {
        return new AggregationOperator(
            List.of(
                new Aggregator.AggregatorFactory(AggregatorFunctionProviders.avgLong(), mode, channel1).get(),
                new Aggregator.AggregatorFactory(AggregatorFunctionProviders.max(), mode, channel2).get()
            )
        );
    }
}
