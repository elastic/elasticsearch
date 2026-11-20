/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.operator.blocksource.LongDoubleTupleBlockSourceOperator;
import org.elasticsearch.core.Tuple;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class MinDoubleGroupingAggregatorFunctionTests extends PartitionedGroupingAggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int end) {
        return new LongDoubleTupleBlockSourceOperator(
            blockFactory,
            LongStream.range(0, end).mapToObj(l -> Tuple.tuple(randomLongBetween(0, 4), randomDoubleWithEdgeCases()))
        );
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new MinDoubleAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "min of doubles";
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        double[] values = input.stream().flatMapToDouble(p -> allDoubles(p, group)).toArray();
        if (values.length == 0) {
            assertThat(result.isNull(position), equalTo(true));
            return;
        }
        assertThat(result.isNull(position), equalTo(false));
        assertThat(((DoubleBlock) result).getDouble(position), equalTo(skipNaNMin(values)));
    }

    public void testGroupsOfOnlyNaNAnswerNaNBesideGroupsWithFiniteValues() {
        List<Tuple<Long, Double>> input = List.of(
            Tuple.tuple(0L, Double.NaN),
            Tuple.tuple(0L, Double.NaN),
            Tuple.tuple(1L, Double.NaN),
            Tuple.tuple(1L, 3.0),
            Tuple.tuple(1L, 1.0),
            Tuple.tuple(2L, 0.0),
            Tuple.tuple(2L, -0.0)
        );

        assertThat(minByGroup(input), equalTo(Map.of(0L, Double.NaN, 1L, 1.0, 2L, -0.0)));
    }

    private Map<Long, Double> minByGroup(List<Tuple<Long, Double>> input) {
        var runner = new TestDriverRunner().builder(driverContext());
        runner.input(new LongDoubleTupleBlockSourceOperator(runner.blockFactory(), input.stream()));
        Map<Long, Double> byGroup = new HashMap<>();
        for (Page page : runner.run(simple())) {
            LongBlock groups = page.getBlock(0);
            DoubleBlock mins = page.getBlock(1);
            for (int p = 0; p < page.getPositionCount(); p++) {
                assertFalse(mins.isNull(p));
                byGroup.put(groups.getLong(p), mins.getDouble(p));
            }
        }
        return byGroup;
    }

    private static double randomDoubleWithEdgeCases() {
        return switch (randomIntBetween(0, 7)) {
            case 0 -> Double.NaN;
            case 1 -> 0.0;
            case 2 -> -0.0;
            default -> randomDouble();
        };
    }

    private static double skipNaNMin(double[] values) {
        OptionalDouble min = Arrays.stream(values).filter(v -> Double.isNaN(v) == false).min();
        return min.isPresent() ? min.getAsDouble() : Double.NaN;
    }
}
