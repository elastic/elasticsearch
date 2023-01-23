/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.LongDoubleTupleBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Tuple;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class MedianDoubleGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {

    @Override
    protected SourceOperator simpleInput(int end) {
        double[][] samples = new double[][] {
            { 1.2, 1.25, 2.0, 2.0, 4.3, 6.0, 9.0 },
            { 0.1, 1.5, 2.0, 3.0, 4.0, 7.5, 100.0 },
            { 0.2, 1.5, 2.0, 2.5 },
            { 0.5, 3.0, 3.0, 3.0, 4.3 },
            { 0.25, 1.5, 3.0 } };
        List<Tuple<Long, Double>> values = new ArrayList<>();
        for (int i = 0; i < samples.length; i++) {
            for (double v : samples[i]) {
                values.add(Tuple.tuple((long) i, v));
            }
        }
        Randomness.shuffle(values);
        return new LongDoubleTupleBlockSourceOperator(values);
    }

    @Override
    protected GroupingAggregatorFunction.Factory aggregatorFunction() {
        return GroupingAggregatorFunction.MEDIAN_DOUBLES;
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "median of doubles";
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, long group) {
        int bucket = Math.toIntExact(group);
        double[] expectedValues = new double[] { 2.0, 3.0, 1.75, 3.0, 1.5 };
        assertThat(bucket, allOf(greaterThanOrEqualTo(0), lessThanOrEqualTo(4)));
        assertThat(((DoubleBlock) result).getDouble(position), equalTo(expectedValues[bucket]));
    }
}
