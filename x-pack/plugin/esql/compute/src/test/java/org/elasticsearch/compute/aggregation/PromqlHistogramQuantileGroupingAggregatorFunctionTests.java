/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.aggregation.PromqlHistogramQuantileStates.Bucket;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.TestWarningsSource;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

public class PromqlHistogramQuantileGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {
    private double quantile;

    @Before
    public void initParameters() {
        quantile = randomFrom(0.0, 0.25, 0.5, 0.75, 1.0);
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new PromqlHistogramQuantileAggregatorFunctionSupplier(TestWarningsSource.INSTANCE, quantile);
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "promql_histogram_quantile";
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
        double expected = PromqlHistogramQuantileTestHelpers.expectedQuantile(quantile, buckets);
        PromqlHistogramQuantileAggregatorFunctionTests.assertQuantileResult((DoubleBlock) result, position, expected);
    }
}
