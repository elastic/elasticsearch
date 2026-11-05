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
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.operator.blocksource.LongDoubleTupleBlockSourceOperator;
import org.elasticsearch.core.Tuple;

import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ValuesDoubleGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {
    private final boolean withNulls = randomBoolean();

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new ValuesDoubleAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "values of doubles";
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new LongDoubleTupleBlockSourceOperator(
            blockFactory,
            LongStream.range(0, size)
                .mapToObj(l -> Tuple.tuple(randomLongBetween(0, 100), withNulls && randomBoolean() ? null : randomDouble()))
        );
    }

    @Override
    public void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        Object[] values = input.stream()
            .flatMapToDouble(p -> allDoubles(p, group))
            .boxed()
            .collect(Collectors.toSet())
            .toArray(Object[]::new);
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

    /**
     * Like the non-grouped aggregation, the grouped one deduplicates values on their bits: a repeated {@code NaN} is kept
     * once and {@code -0.0} is kept apart from {@code 0.0}, including when the first value of the group is one of them.
     */
    public void testDeduplicatesOnBits() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        Block[] blocks = new Block[1];
        try (ValuesDoubleAggregator.GroupingState state = ValuesDoubleAggregator.initGrouping(driverContext)) {
            ValuesDoubleAggregator.combine(state, 0, Double.NaN);
            ValuesDoubleAggregator.combine(state, 0, Double.NaN);
            ValuesDoubleAggregator.combine(state, 1, 0.0);
            ValuesDoubleAggregator.combine(state, 1, -0.0);
            try (
                IntVector selected = blockFactory.newIntArrayVector(new int[] { 0, 1 }, 2);
                GroupingAggregatorEvaluationContext ctx = new GroupingAggregatorEvaluationContext(driverContext);
                GroupingAggregatorFunction.PreparedForEvaluation prepared = ValuesDoubleAggregator.prepareEvaluateFinal(
                    state,
                    selected,
                    ctx
                )
            ) {
                prepared.evaluate(blocks, 0, selected);
            }
        }
        try (Block result = blocks[0]) {
            assertEquals(Double.NaN, BlockUtils.toJavaObject(result, 0));
            assertEquals(List.of(0.0, -0.0), BlockUtils.toJavaObject(result, 1));
        }
    }
}
