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
import org.elasticsearch.core.Types;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractAggregationTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.MultiRowTestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.appliesTo;
import static org.elasticsearch.xpack.esql.expression.function.aggregate.PercentileTests.getExpectedPercentileForExponentialHistograms;
import static org.elasticsearch.xpack.esql.expression.function.aggregate.PercentileTests.getExpectedPercentileForTDigests;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MedianTests extends AbstractAggregationTestCase {
    public MedianTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var suppliers = Stream.of(
            MultiRowTestCaseSupplier.intCases(1, 1000, Integer.MIN_VALUE, Integer.MAX_VALUE, true),
            MultiRowTestCaseSupplier.longCases(1, 1000, Long.MIN_VALUE, Long.MAX_VALUE, true),
            MultiRowTestCaseSupplier.doubleCases(1, 1000, -Double.MAX_VALUE, Double.MAX_VALUE, true),
            MultiRowTestCaseSupplier.exponentialHistogramCases(1, 100)
        ).flatMap(List::stream).map(MedianTests::makeSupplier).collect(Collectors.toCollection(ArrayList::new));

        // TDigest was added in 9.4
        FunctionAppliesTo shapeAppliesTo = appliesTo(FunctionAppliesToLifecycle.PREVIEW, "9.4.0", "", true);
        Stream.of(MultiRowTestCaseSupplier.tdigestCases(1, 100))
            .flatMap(List::stream)
            .map(s -> s.withAppliesTo(shapeAppliesTo))
            .map(MedianTests::makeSupplier)
            .forEach(suppliers::add);

        suppliers.addAll(
            List.of(
                // Folding
                new TestCaseSupplier(
                    List.of(DataType.INTEGER),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(TestCaseSupplier.TypedData.multiRow(List.of(200), DataType.INTEGER, "number")),
                        standardAggregatorName("Percentile", DataType.INTEGER),
                        DataType.DOUBLE,
                        equalTo(200.)
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.LONG),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(TestCaseSupplier.TypedData.multiRow(List.of(200L), DataType.LONG, "number")),
                        standardAggregatorName("Percentile", DataType.LONG),
                        DataType.DOUBLE,
                        equalTo(200.)
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.DOUBLE),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(TestCaseSupplier.TypedData.multiRow(List.of(200.), DataType.DOUBLE, "number")),
                        standardAggregatorName("Percentile", DataType.DOUBLE),
                        DataType.DOUBLE,
                        equalTo(200.)
                    )
                )
            )
        );

        return parameterSuppliersFromTypedDataWithDefaultChecks(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Median(source, args.get(0));
    }

    private static TestCaseSupplier makeSupplier(TestCaseSupplier.TypedDataSupplier fieldSupplier) {
        return new TestCaseSupplier(List.of(fieldSupplier.type()), () -> {
            var fieldTypedData = fieldSupplier.get();

            Double expected;
            if (fieldTypedData.type() == DataType.EXPONENTIAL_HISTOGRAM) {

                // Note that the merging used underneath can be dependent on the order if zero-buckets are involved
                // therefore the percentile in theory could vary slightly
                // however, it seems that the order is the same in the tests vs the reference computation
                // if we ever encounter flakes here, we should replace the equalTo() assertion with an assertion on the relative error
                expected = getExpectedPercentileForExponentialHistograms(Types.forciblyCast(fieldTypedData.multiRowData()), 50);
            } else if (fieldTypedData.type() == DataType.TDIGEST) {
                expected = getExpectedPercentileForTDigests(Types.forciblyCast(fieldTypedData.multiRowData()), 50);
            } else {
                try (var digest = TDigestState.create(newLimitedBreaker(ByteSizeValue.ofMb(100)), 1000)) {
                    for (var value : fieldTypedData.multiRowData()) {
                        digest.add(((Number) value).doubleValue());
                    }

                    expected = digest.size() == 0 ? null : digest.quantile(0.5);
                }
            }
            return new TestCaseSupplier.TestCase(
                List.of(fieldTypedData),
                standardAggregatorName("Percentile", fieldSupplier.type()),
                DataType.DOUBLE,
                expected == null ? nullValue() : closeTo(expected, Math.abs(expected * 1e-10))
            );
        });
    }

    @Override
    public void testFold() {
        var typedData = testCase.getData().getFirst();
        assumeFalse(
            "MEDIAN expects a different result for -0.0 when folded",
            typedData.type() == DataType.DOUBLE && typedData.multiRowData().size() == 1 && typedData.multiRowData().getFirst().equals(-0.0)
        );

        super.testFold();
    }
}
