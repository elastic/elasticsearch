/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.LongFloatTupleBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.closeTo;

public class MedianAbsoluteDeviationFloatGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int end) {
        Float[][] samples = new Float[][] {
            { 1.2f, 1.25f, 2.0f, 2.0f, 4.3f, 6.0f, 9.0f },
            { 0.1f, 1.5f, 2.0f, 3.0f, 4.0f, 7.5f, 100.0f },
            { 0.2f, 1.75f, 2.0f, 2.5f },
            { 0.5f, 3.0f, 3.0f, 3.0f, 4.3f },
            { 0.25f, 1.5f, 3.0f } };
        List<Tuple<Long, Float>> values = new ArrayList<>();
        for (int i = 0; i < samples.length; i++) {
            List<Float> list = Arrays.stream(samples[i]).collect(Collectors.toList());
            Randomness.shuffle(list);
            for (float v : list) {
                values.add(Tuple.tuple((long) i, v));
            }
        }
        return new LongFloatTupleBlockSourceOperator(blockFactory, values.subList(0, Math.min(values.size(), end)));
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new MedianAbsoluteDeviationFloatAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "median_absolute_deviation of floats";
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        double medianAbsoluteDeviation = medianAbsoluteDeviation(input.stream().flatMap(p -> allFloats(p, group)));
        assertThat(((DoubleBlock) result).getDouble(position), closeTo(medianAbsoluteDeviation, medianAbsoluteDeviation * .000001));
    }

    static double medianAbsoluteDeviation(Stream<Float> s) {
        Float[] data = s.toArray(Float[]::new);
        float median = median(Arrays.stream(data));
        return median(Arrays.stream(data).map(d -> Math.abs(median - d)));
    }

    static float median(Stream<Float> s) {
        // The input data is small enough that tdigest will find the actual median.
        Float[] data = s.sorted().toArray(Float[]::new);
        if (data.length == 0) {
            return 0;
        }
        int c = data.length / 2;
        return data.length % 2 == 0 ? (data[c - 1] + data[c]) / 2 : data[c];
    }
}
