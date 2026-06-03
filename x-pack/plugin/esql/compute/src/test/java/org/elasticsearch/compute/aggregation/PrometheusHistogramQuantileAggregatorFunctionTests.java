/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.aggregation.PrometheusHistogramQuantileStates.Bucket;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.compute.test.TestWarningsSource;
import org.elasticsearch.compute.test.operator.blocksource.ListRowsBlockSourceOperator;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

public class PrometheusHistogramQuantileAggregatorFunctionTests extends AggregatorFunctionTestCase {
    private double quantile;

    @Before
    public void initParameters() {
        quantile = randomFrom(0.0, 0.25, 0.5, 0.75, 1.0);
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new PrometheusHistogramQuantileAggregatorFunctionSupplier(TestWarningsSource.INSTANCE, quantile);
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "prometheus_histogram_quantile";
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
        return PrometheusHistogramQuantileTestHelpers.bucketRowsSource(blockFactory, size);
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, Block result) {
        List<Bucket> buckets = PrometheusHistogramQuantileTestHelpers.bucketsFromPages(input, 0, 1);
        double expected = PrometheusHistogramQuantileTestHelpers.expectedQuantile(quantile, buckets);
        assertQuantileResult((DoubleBlock) result, 0, expected);
    }

    public void testWarnsAndSkipsUnparseableBound() {
        DriverContext driverContext = driverContext();
        // A valid two-bucket histogram plus a malformed `le` label, which must be skipped with a warning.
        List<List<Object>> rows = List.of(List.of(2.0, "1.0"), List.of(4.0, "+Inf"), List.of(1.0, "not_a_number"));
        List<Page> results = new ArrayList<>();
        List<String> warnings = new ArrayList<>();
        try (
            Driver driver = TestDriverFactory.create(
                driverContext,
                new ListRowsBlockSourceOperator(driverContext.blockFactory(), List.of(ElementType.DOUBLE, ElementType.BYTES_REF), rows),
                List.of(simple().get(driverContext)),
                new TestResultPageSinkOperator(results::add),
                () -> warnings.addAll(threadContext.getResponseHeaders().getOrDefault("Warning", List.of()))
            )
        ) {
            new TestDriverRunner().run(driver);
        }

        // The bad bucket is dropped rather than failing the query: a single result is still produced.
        assertThat(results.size(), equalTo(1));
        assertThat(
            ((DoubleBlock) results.get(0).getBlock(0)).getDouble(0),
            equalTo(
                PrometheusHistogramQuantileStates.bucketQuantile(
                    quantile,
                    List.of(new Bucket(1.0, 2.0), new Bucket(Double.POSITIVE_INFINITY, 4.0))
                )
            )
        );
        // The warning names the offending `le` value, mirroring Prometheus' "bad bucket label" warning.
        assertThat(
            warnings,
            hasItem(containsString("java.lang.NumberFormatException: bucket label [le] has a malformed value of [not_a_number]"))
        );
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
