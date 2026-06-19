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

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class PromqlHistogramQuantileAggregatorFunctionTests extends AggregatorFunctionTestCase {
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
    protected int maximumTestRowCount() {
        return 1_000;
    }

    @Override
    protected boolean supportsMultiValues() {
        return false;
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return PromqlHistogramQuantileTestHelpers.bucketRowsSource(blockFactory, size);
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, Block result) {
        List<Bucket> buckets = PromqlHistogramQuantileTestHelpers.bucketsFromPages(input, 0, 1);
        double expected = PromqlHistogramQuantileTestHelpers.expectedQuantile(quantile, buckets);
        assertQuantileResult((DoubleBlock) result, 0, expected);
    }

    static void assertQuantileResult(DoubleBlock result, int position, double expected) {
        // A NaN estimate means the histogram cannot produce a quantile; the aggregator emits null rather than NaN.
        if (Double.isNaN(expected)) {
            assertTrue(result.isNull(position));
        } else {
            assertThat(result.getDouble(position), equalTo(expected));
        }
    }
}
