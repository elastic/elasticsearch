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
import org.elasticsearch.compute.operator.LongDoubleTupleBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

import static org.hamcrest.Matchers.equalTo;

public class MedianAbsoluteDeviationDoubleGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {

    @Override
    protected SourceOperator simpleInput(int end) {
        double[][] samples = new double[][] {
            { 1.2, 1.25, 2.0, 2.0, 4.3, 6.0, 9.0 },
            { 0.1, 1.5, 2.0, 3.0, 4.0, 7.5, 100.0 },
            { 0.2, 1.75, 2.0, 2.5 },
            { 0.5, 3.0, 3.0, 3.0, 4.3 },
            { 0.25, 1.5, 3.0 } };
        List<Tuple<Long, Double>> values = new ArrayList<>();
        for (int i = 0; i < samples.length; i++) {
            List<Double> list = Arrays.stream(samples[i]).boxed().collect(Collectors.toList());
            Randomness.shuffle(list);
            for (double v : list) {
                values.add(Tuple.tuple((long) i, v));
            }
        }
        return new LongDoubleTupleBlockSourceOperator(values);
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction(BigArrays bigArrays, List<Integer> inputChannels) {
        return new MedianAbsoluteDeviationDoubleAggregatorFunctionSupplier(bigArrays, inputChannels);
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "median_absolute_deviation of doubles";
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, long group) {
        assertThat(
            ((DoubleBlock) result).getDouble(position),
            equalTo(medianAbsoluteDeviation(input.stream().flatMapToDouble(p -> allDoubles(p, group))))
        );
    }

    static double medianAbsoluteDeviation(DoubleStream s) {
        double[] data = s.toArray();
        double median = median(Arrays.stream(data));
        return median(Arrays.stream(data).map(d -> Math.abs(median - d)));
    }

    static double median(DoubleStream s) {
        // The input data is small enough that tdigest will find the actual median.
        double[] data = s.sorted().toArray();
        if (data.length == 0) {
            return 0;
        }
        int c = data.length / 2;
        return data.length % 2 == 0 ? (data[c - 1] + data[c]) / 2 : data[c];
    }
}
