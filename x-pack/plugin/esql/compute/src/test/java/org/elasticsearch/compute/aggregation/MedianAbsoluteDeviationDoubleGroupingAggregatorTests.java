/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.operator.LongDoubleTupleBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class MedianAbsoluteDeviationDoubleGroupingAggregatorTests extends GroupingAggregatorTestCase {

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
    protected GroupingAggregatorFunction.Factory aggregatorFunction() {
        return GroupingAggregatorFunction.MEDIAN_ABSOLUTE_DEVIATION_DOUBLES;
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "median_absolute_deviation of doubles";
    }

    @Override
    public void assertSimpleBucket(Block result, int end, int position, int bucket) {
        double[] expectedValues = new double[] { 0.8, 1.5, 0.375, 0.0, 1.25 };
        assertThat(bucket, allOf(greaterThanOrEqualTo(0), lessThanOrEqualTo(4)));
        assertThat(result.getDouble(position), equalTo(expectedValues[bucket]));
    }
}
