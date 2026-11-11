/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.aggregation.PromqlHistogramStates.Bucket;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.TestWarningsSource;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

public class PromqlHistogramFractionGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {
    private double lower;
    private double upper;

    @Before
    public void initParameters() {
        lower = randomFrom(0.0, 0.5, 1.0);
        upper = randomFrom(1.5, 2.0, Double.POSITIVE_INFINITY);
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new PromqlHistogramFractionAggregatorFunctionSupplier(TestWarningsSource.INSTANCE, lower, upper);
    }

    @Override
    protected boolean assertNoLeakedWarnings() {
        return false;
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "promql_histogram_fraction";
    }

    @Override
    protected int inputCount() {
        return 2;
    }

    @Override
    protected boolean supportsMultiValues() {
        return false;
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return PromqlHistogramQuantileTestHelpers.groupedBucketRowsSource(blockFactory, size);
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        List<Bucket> buckets = new ArrayList<>();
        for (Page page : input) {
            matchingGroups(page, group).forEach(p -> PromqlHistogramQuantileTestHelpers.appendBuckets(page, 1, 2, p, buckets));
        }
        if (buckets.isEmpty()) {
            assertTrue(result.isNull(position));
            return;
        }
        double expected = PromqlHistogramQuantileTestHelpers.expectedFraction(lower, upper, buckets);
        PromqlHistogramFractionAggregatorFunctionTests.assertFractionResult((DoubleBlock) result, position, expected);
    }
}
