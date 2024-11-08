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
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.LongFloatTupleBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Tuple;

import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ValuesFloatGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {
    @Override
    protected AggregatorFunctionSupplier aggregatorFunction(List<Integer> inputChannels) {
        return new ValuesFloatAggregatorFunctionSupplier(inputChannels);
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "values of floats";
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new LongFloatTupleBlockSourceOperator(
            blockFactory,
            LongStream.range(0, size).mapToObj(l -> Tuple.tuple(randomLongBetween(0, 4), randomFloat()))
        );
    }

    @Override
    public void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        Object[] values = input.stream().flatMap(p -> allFloats(p, group)).collect(Collectors.toSet()).toArray(Object[]::new);
        Object resultValue = BlockUtils.toJavaObject(result, position);
        switch (values.length) {
            case 0 -> assertThat(resultValue, nullValue());
            case 1 -> assertThat(resultValue, equalTo(values[0]));
            default -> {
                TreeSet<?> set = new TreeSet<>((List<?>) resultValue);
                if (false == set.containsAll(Arrays.asList(values))) {
                    assertThat(set, containsInAnyOrder(values));
                }
            }
        }
    }
}
