/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.operator.SequenceDoubleBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;

import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsInAnyOrder;

public class ValuesDoubleAggregatorFunctionTests extends AggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceDoubleBlockSourceOperator(blockFactory, IntStream.range(0, size).mapToDouble(i -> randomDouble()));
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new ValuesDoubleAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "values of doubles";
    }

    @Override
    public void assertSimpleOutput(List<Block> input, Block result) {
        TreeSet<?> set = new TreeSet<>((List<?>) BlockUtils.toJavaObject(result, 0));
        Object[] values = input.stream()
            .flatMapToDouble(AggregatorFunctionTestCase::allDoubles)
            .boxed()
            .collect(Collectors.toSet())
            .toArray(Object[]::new);
        if (false == set.containsAll(Arrays.asList(values))) {
            assertThat(set, containsInAnyOrder(values));
        }
    }
}
