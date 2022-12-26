/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.data.Page;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Test case for all operators that support parallel operation in the
 * shape of "single", "initial", "intermediate", and "final" modes.
 */
public abstract class ForkingOperatorTestCase extends OperatorTestCase {
    protected abstract Operator.OperatorFactory simpleWithMode(BigArrays bigArrays, AggregatorMode mode);

    @Override
    protected final Operator.OperatorFactory simple(BigArrays bigArrays) {
        return simpleWithMode(bigArrays, AggregatorMode.SINGLE);
    }

    public final void testInitialFinal() {
        BigArrays bigArrays = nonBreakingBigArrays();
        int end = between(1_000, 100_000);
        List<Page> results = new ArrayList<>();

        try (
            Driver d = new Driver(
                simpleInput(end),
                List.of(simpleWithMode(bigArrays, AggregatorMode.INITIAL).get(), simpleWithMode(bigArrays, AggregatorMode.FINAL).get()),
                new PageConsumerOperator(page -> results.add(page)),
                () -> {}
            )
        ) {
            d.run();
        }
        assertSimpleOutput(end, results);
    }

    public final void testManyInitialFinal() {
        BigArrays bigArrays = nonBreakingBigArrays();
        int end = between(1_000, 100_000);

        List<Page> partials = oneDriverPerPage(simpleInput(end), () -> List.of(simpleWithMode(bigArrays, AggregatorMode.INITIAL).get()));

        List<Page> results = new ArrayList<>();
        try (
            Driver d = new Driver(
                new CannedSourceOperator(partials.iterator()),
                List.of(simpleWithMode(bigArrays, AggregatorMode.FINAL).get()),
                new PageConsumerOperator(results::add),
                () -> {}
            )
        ) {
            d.run();
        }
        assertSimpleOutput(end, results);
    }

    public final void testInitialIntermediateFinal() {
        BigArrays bigArrays = nonBreakingBigArrays();
        int end = between(1_000, 100_000);
        List<Page> results = new ArrayList<>();

        try (
            Driver d = new Driver(
                simpleInput(end),
                List.of(
                    simpleWithMode(bigArrays, AggregatorMode.INITIAL).get(),
                    simpleWithMode(bigArrays, AggregatorMode.INTERMEDIATE).get(),
                    simpleWithMode(bigArrays, AggregatorMode.FINAL).get()
                ),
                new PageConsumerOperator(page -> results.add(page)),
                () -> {}
            )
        ) {
            d.run();
        }
        assertSimpleOutput(end, results);
    }

    public final void testManyInitialManyPartialFinal() {
        BigArrays bigArrays = nonBreakingBigArrays();
        int end = between(1_000, 100_000);

        List<Page> partials = oneDriverPerPage(simpleInput(end), () -> List.of(simpleWithMode(bigArrays, AggregatorMode.INITIAL).get()));
        Collections.shuffle(partials, random());
        List<Page> intermediates = oneDriverPerPageList(
            randomSplits(partials).iterator(),
            () -> List.of(simpleWithMode(bigArrays, AggregatorMode.INTERMEDIATE).get())
        );

        List<Page> results = new ArrayList<>();
        try (
            Driver d = new Driver(
                new CannedSourceOperator(intermediates.iterator()),
                List.of(simpleWithMode(bigArrays, AggregatorMode.FINAL).get()),
                new PageConsumerOperator(results::add),
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
}
