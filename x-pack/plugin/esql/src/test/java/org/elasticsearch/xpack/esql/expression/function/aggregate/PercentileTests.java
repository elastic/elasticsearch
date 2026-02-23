/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.aggregation.TDigestStates;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.core.Types;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramMerger;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramQuantile;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractAggregationTestCase;
import org.elasticsearch.xpack.esql.expression.function.MultiRowTestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.nullValue;

public class PercentileTests extends AbstractAggregationTestCase {
    public PercentileTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var suppliers = new ArrayList<TestCaseSupplier>();

        var fieldCases = Stream.of(
            MultiRowTestCaseSupplier.intCases(1, 1000, Integer.MIN_VALUE, Integer.MAX_VALUE, true),
            MultiRowTestCaseSupplier.longCases(1, 1000, Long.MIN_VALUE, Long.MAX_VALUE, true),
            MultiRowTestCaseSupplier.doubleCases(1, 1000, -Double.MAX_VALUE, Double.MAX_VALUE, true),
            MultiRowTestCaseSupplier.exponentialHistogramCases(1, 100),
            MultiRowTestCaseSupplier.tdigestCases(1, 100)
        ).flatMap(List::stream).toList();

        var percentileCases = Stream.of(
            TestCaseSupplier.intCases(0, 100, true),
            TestCaseSupplier.longCases(0, 100, true),
            TestCaseSupplier.doubleCases(0, 100, true)
        ).flatMap(List::stream).toList();

        for (var field : fieldCases) {
            for (var percentile : percentileCases) {
                suppliers.add(makeSupplier(field, percentile));
            }
        }

        return parameterSuppliersFromTypedDataWithDefaultChecks(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Percentile(source, args.get(0), args.get(1));
    }

    private static TestCaseSupplier makeSupplier(
        TestCaseSupplier.TypedDataSupplier fieldSupplier,
        TestCaseSupplier.TypedDataSupplier percentileSupplier
    ) {
        return new TestCaseSupplier(List.of(fieldSupplier.type(), percentileSupplier.type()), () -> {
            var fieldTypedData = fieldSupplier.get();
            var percentileTypedData = percentileSupplier.get().forceLiteral();

            var percentile = ((Number) percentileTypedData.data()).doubleValue();

            Double expected = switch (fieldTypedData.type()) {
                case EXPONENTIAL_HISTOGRAM -> getExpectedPercentileForExponentialHistograms(
                    Types.forciblyCast(fieldTypedData.multiRowData()),
                    percentile
                );
                case TDIGEST -> getExpectedPercentileForTDigests(Types.forciblyCast(fieldTypedData.multiRowData()), percentile);
                default -> getExpectedPercentileForNumbers(Types.forciblyCast(fieldTypedData.multiRowData()), percentile);
            };

            return new TestCaseSupplier.TestCase(
                List.of(fieldTypedData, percentileTypedData),
                standardAggregatorName("Percentile", fieldSupplier.type()),
                DataType.DOUBLE,
                expected == null ? nullValue() : closeTo(expected, Math.abs(expected * 1e-10))
            );
        });
    }

    private static Double getExpectedPercentileForNumbers(List<Number> values, double percentile) {
        try (var digest = TDigestState.create(newLimitedBreaker(ByteSizeValue.ofMb(100)), 1000)) {
            for (var value : values) {
                digest.add(value.doubleValue());
            }
            return digest.size() == 0 ? null : digest.quantile(percentile / 100);
        }
    }

    public static Double getExpectedPercentileForExponentialHistograms(List<ExponentialHistogram> values, double percentile) {
        ExponentialHistogram merged = ExponentialHistogram.merge(
            ExponentialHistogramMerger.DEFAULT_MAX_HISTOGRAM_BUCKETS,
            ExponentialHistogramCircuitBreaker.noop(),
            values.stream().filter(Objects::nonNull).toList().iterator()
        );
        double result = ExponentialHistogramQuantile.getQuantile(merged, percentile / 100.0);
        return Double.isNaN(result) ? null : result;
    }

    public static Double getExpectedPercentileForTDigests(List<TDigestHolder> values, double percentile) {
        TDigestState merged = TDigestState.createWithoutCircuitBreaking(TDigestStates.COMPRESSION);
        values.stream().filter(Objects::nonNull).forEach(tDigestHolder -> tDigestHolder.addTo(merged));
        double result = merged.quantile(percentile / 100.0);
        return Double.isNaN(result) ? null : result;
    }

    @Override
    public void testFold() {
        var typedData = testCase.getData().getFirst();
        assumeFalse(
            "PERCENTILE expects a different result for -0.0 when folded",
            typedData.type() == DataType.DOUBLE && typedData.multiRowData().size() == 1 && typedData.multiRowData().getFirst().equals(-0.0)
        );

        super.testFold();
    }
}
