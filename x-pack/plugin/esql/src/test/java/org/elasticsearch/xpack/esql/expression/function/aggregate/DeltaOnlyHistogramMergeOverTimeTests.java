/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.compute.aggregation.DeltaOnlyHistogramMergeOverTimeExponentialHistogramAggregator;
import org.elasticsearch.compute.aggregation.DeltaOnlyHistogramMergeOverTimeTDigestAggregator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractAggregationTestCase;
import org.elasticsearch.xpack.esql.expression.function.MultiRowTestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class DeltaOnlyHistogramMergeOverTimeTests extends AbstractAggregationTestCase {

    public DeltaOnlyHistogramMergeOverTimeTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var suppliers = new ArrayList<TestCaseSupplier>();
        var histogramSuppliers = Stream.of(
            MultiRowTestCaseSupplier.exponentialHistogramCases(1, 100),
            MultiRowTestCaseSupplier.tdigestCases(1, 100)
        ).flatMap(List::stream).toList();

        for (TestCaseSupplier.TypedDataSupplier fieldSupplier : histogramSuppliers) {
            for (RateTests.TemporalityParameter temporality : RateTests.TemporalityParameter.values()) {
                suppliers.add(makeSupplier(fieldSupplier, temporality));
            }
        }
        return parameterSuppliersFromTypedDataWithDefaultChecks(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new DeltaOnlyHistogramMergeOverTime(source, args.get(0), Literal.TRUE, AggregateFunction.NO_WINDOW, args.get(1));
    }

    @Override
    public void testAggregate() {
        assumeTrue("time-series aggregation doesn't support ungrouped", false);
    }

    @Override
    public void testAggregateToString() {
        assumeTrue("time-series aggregation doesn't support ungrouped", false);
    }

    @Override
    public void testAggregateIntermediate() {
        assumeTrue("time-series aggregation doesn't support ungrouped", false);
    }

    private static TestCaseSupplier makeSupplier(
        TestCaseSupplier.TypedDataSupplier fieldSupplier,
        RateTests.TemporalityParameter temporality
    ) {
        return new TestCaseSupplier(
            fieldSupplier.name() + " temporality=" + temporality,
            List.of(fieldSupplier.type(), DataType.KEYWORD),
            () -> {
                var fieldTypedData = fieldSupplier.get();
                List<Object> dataRows = fieldTypedData.multiRowData();

                List<Object> temporalities = new ArrayList<>();
                Object temporalityValue = temporality.byteValue();
                for (int row = 0; row < dataRows.size(); row++) {
                    temporalities.add(temporalityValue);
                }
                TestCaseSupplier.TypedData temporalityField = TestCaseSupplier.TypedData.multiRow(
                    temporalities,
                    DataType.KEYWORD,
                    "_temporality"
                );

                Matcher<?> resultMatcher = histogramMergeMatcher(dataRows, fieldTypedData.type(), temporality);
                TestCaseSupplier.TestCase result = new TestCaseSupplier.TestCase(
                    List.of(fieldTypedData, temporalityField),
                    standardAggregatorName("DeltaOnlyHistogramMergeOverTime", fieldTypedData.type()),
                    fieldTypedData.type(),
                    resultMatcher
                );
                boolean hasNonNullData = dataRows.stream().anyMatch(v -> v != null);
                if (hasNonNullData) {
                    if (temporality == RateTests.TemporalityParameter.INVALID) {
                        return result.withWarning(
                            "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded."
                        )
                            .withWarning(
                                "Line 1:1: org.elasticsearch.compute.aggregation.InvalidTemporalityException: "
                                    + "Invalid temporality value: [gotcha], expected [cumulative] or [delta]"
                            );
                    } else if (temporality == RateTests.TemporalityParameter.CUMULATIVE) {
                        String cumulativeWarning = fieldTypedData.type() == DataType.TDIGEST
                            ? DeltaOnlyHistogramMergeOverTimeTDigestAggregator.CUMULATIVE_UNSUPPORTED_WARNING
                            : DeltaOnlyHistogramMergeOverTimeExponentialHistogramAggregator.CUMULATIVE_UNSUPPORTED_WARNING;
                        return result.withWarning(
                            "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded."
                        ).withWarning("Line 1:1: java.lang.IllegalArgumentException: " + cumulativeWarning);
                    }
                }
                return result;
            }
        );
    }

    private static Matcher<?> histogramMergeMatcher(List<Object> dataRows, DataType type, RateTests.TemporalityParameter temporality) {
        if (temporality == RateTests.TemporalityParameter.CUMULATIVE || temporality == RateTests.TemporalityParameter.INVALID) {
            return Matchers.nullValue();
        }
        if (type == DataType.EXPONENTIAL_HISTOGRAM) {
            return HistogramMergeTests.createExpectedExponentialHistogramMatcher(dataRows);
        } else if (type == DataType.TDIGEST) {
            return HistogramMergeTests.createExpectedTDigestMatcher(dataRows);
        }
        throw new IllegalArgumentException("Unsupported data type [" + type + "]");
    }
}
