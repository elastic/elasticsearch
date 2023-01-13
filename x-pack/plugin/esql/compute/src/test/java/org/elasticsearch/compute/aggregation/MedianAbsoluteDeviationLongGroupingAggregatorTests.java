/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.TupleBlockSourceOperator;
import org.elasticsearch.core.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class MedianAbsoluteDeviationLongGroupingAggregatorTests extends GroupingAggregatorTestCase {

    @Override
    protected SourceOperator simpleInput(int end) {
        long[][] samples = new long[][] {
            { 12, 125, 20, 20, 43, 60, 90 },
            { 1, 15, 20, 30, 40, 75, 1000 },
            { 2, 175, 20, 25 },
            { 5, 30, 30, 30, 43 },
            { 7, 15, 30 } };
        List<Tuple<Long, Long>> values = new ArrayList<>();
        for (int i = 0; i < samples.length; i++) {
            List<Long> list = Arrays.stream(samples[i]).boxed().collect(Collectors.toList());
            Randomness.shuffle(list);
            for (long v : list) {
                values.add(Tuple.tuple((long) i, v));
            }
        }
        return new TupleBlockSourceOperator(values);
    }

    @Override
    protected GroupingAggregatorFunction.Factory aggregatorFunction() {
        return GroupingAggregatorFunction.MEDIAN_ABSOLUTE_DEVIATION_LONGS;
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "median_absolute_deviation of longs";
    }

    @Override
    public void assertSimpleBucket(Block result, int end, int position, int bucket) {
        double[] expectedValues = new double[] { 23.0, 15, 11.5, 0.0, 8.0 };
        assertThat(bucket, allOf(greaterThanOrEqualTo(0), lessThanOrEqualTo(4)));
        assertThat(result.getDouble(position), equalTo(expectedValues[bucket]));
    }
}
