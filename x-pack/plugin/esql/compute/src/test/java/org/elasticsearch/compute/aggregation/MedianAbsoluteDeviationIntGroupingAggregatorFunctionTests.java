/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.LongIntBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.compute.aggregation.MedianAbsoluteDeviationDoubleGroupingAggregatorFunctionTests.medianAbsoluteDeviation;
import static org.hamcrest.Matchers.equalTo;

public class MedianAbsoluteDeviationIntGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {

    @Override
    protected SourceOperator simpleInput(int end) {
        int[][] samples = new int[][] {
            { 12, 125, 20, 20, 43, 60, 90 },
            { 1, 15, 20, 30, 40, 75, 1000 },
            { 2, 175, 20, 25 },
            { 5, 30, 30, 30, 43 },
            { 7, 15, 30 } };
        List<Tuple<Long, Integer>> values = new ArrayList<>();
        for (int i = 0; i < samples.length; i++) {
            List<Integer> list = Arrays.stream(samples[i]).boxed().collect(Collectors.toList());
            Randomness.shuffle(list);
            for (int v : list) {
                values.add(Tuple.tuple((long) i, v));
            }
        }
        return new LongIntBlockSourceOperator(values);
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction(BigArrays bigArrays, List<Integer> inputChannels) {
        return new MedianAbsoluteDeviationIntAggregatorFunctionSupplier(bigArrays, inputChannels);
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "median_absolute_deviation of ints";
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, long group) {
        assertThat(
            ((DoubleBlock) result).getDouble(position),
            equalTo(medianAbsoluteDeviation(input.stream().flatMapToInt(p -> allInts(p, group)).asDoubleStream()))
        );
    }
}
