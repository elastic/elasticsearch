/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramMerger;
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
import java.util.Objects;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class HistogramMergeOverTimeTests extends AbstractAggregationTestCase {

    public HistogramMergeOverTimeTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var suppliers = new ArrayList<TestCaseSupplier>();
        var histogramSuppliers = MultiRowTestCaseSupplier.exponentialHistogramCases(1, 100);

        for (TestCaseSupplier.TypedDataSupplier fieldSupplier : histogramSuppliers) {
            for (RateTests.TemporalityParameter temporality : RateTests.TemporalityParameter.values()) {
                suppliers.add(makeSupplier(fieldSupplier, temporality));
            }
        }
        return parameterSuppliersFromTypedDataWithDefaultChecks(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new HistogramMergeOverTime(source, args.get(0), Literal.TRUE, AggregateFunction.NO_WINDOW, args.get(1), args.get(2));
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
            List.of(fieldSupplier.type(), DataType.DATETIME, DataType.KEYWORD, DataType.INTEGER, DataType.LONG),
            () -> {
                var fieldTypedData = fieldSupplier.get();
                List<Object> dataRows = fieldTypedData.multiRowData();

                List<Long> timestamps = new ArrayList<>();
                List<Object> temporalities = new ArrayList<>();
                List<Integer> slices = new ArrayList<>();
                List<Long> maxTimestamps = new ArrayList<>();
                long lastTimestamp = randomLongBetween(0, 1_000_000);
                Object temporalityValue = temporality.byteValue();
                for (int row = 0; row < dataRows.size(); row++) {
                    lastTimestamp += randomLongBetween(1, 10_000);
                    timestamps.add(lastTimestamp);
                    temporalities.add(temporalityValue);
                    slices.add(0);
                    maxTimestamps.add(Long.MAX_VALUE);
                }
                TestCaseSupplier.TypedData timestampsField = TestCaseSupplier.TypedData.multiRow(
                    timestamps.reversed(),
                    DataType.DATETIME,
                    "timestamps"
                );
                TestCaseSupplier.TypedData temporalityField = TestCaseSupplier.TypedData.multiRow(
                    temporalities,
                    DataType.KEYWORD,
                    "_temporality"
                );
                TestCaseSupplier.TypedData sliceIndexField = TestCaseSupplier.TypedData.multiRow(slices, DataType.INTEGER, "_slice_index");
                TestCaseSupplier.TypedData maxTimestampField = TestCaseSupplier.TypedData.multiRow(
                    maxTimestamps,
                    DataType.LONG,
                    "_max_timestamp"
                );

                List<String> expectedWarnings = new ArrayList<>();
                Matcher<?> resultMatcher = histogramMergeMatcher(dataRows, temporality, expectedWarnings);
                TestCaseSupplier.TestCase result = new TestCaseSupplier.TestCase(
                    List.of(fieldTypedData, timestampsField, temporalityField, sliceIndexField, maxTimestampField),
                    "IncreaseExponentialHistogram",
                    DataType.EXPONENTIAL_HISTOGRAM,
                    resultMatcher
                );
                for (String warning : expectedWarnings) {
                    result = result.withWarning(warning);
                }
                return result;
            }
        );
    }

    private static Matcher<?> histogramMergeMatcher(
        List<Object> dataRows,
        RateTests.TemporalityParameter temporality,
        List<String> expectedWarnings
    ) {
        boolean hasNonNullData = dataRows.stream().anyMatch(Objects::nonNull);
        if (temporality == RateTests.TemporalityParameter.INVALID) {
            if (hasNonNullData) {
                expectedWarnings.add("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.");
                expectedWarnings.add(
                    "Line 1:1: org.elasticsearch.compute.aggregation.InvalidTemporalityException: "
                        + "Invalid temporality value: [gotcha], expected [cumulative] or [delta]"
                );
            }
            return Matchers.nullValue();
        }
        if (temporality == RateTests.TemporalityParameter.CUMULATIVE) {
            return cumulativeMatcher(dataRows, expectedWarnings);
        }
        // For delta, the function should behave exactly the same as HistogramMerge
        return HistogramMergeTests.createExpectedExponentialHistogramMatcher(dataRows);
    }

    private static Matcher<?> cumulativeMatcher(List<Object> dataRows, List<String> expectedWarnings) {
        ExponentialHistogramCircuitBreaker breaker = ExponentialHistogramCircuitBreaker.noop();
        List<ExponentialHistogram> nonNull = dataRows.stream().filter(Objects::nonNull).map(ExponentialHistogram.class::cast).toList();

        if (nonNull.isEmpty()) {
            return Matchers.nullValue();
        }

        ExponentialHistogram newestValue = nonNull.getFirst();
        ExponentialHistogram oldestValue = nonNull.getLast();
        // if our last value is 0 (=empty) the aggregator assumes delta temporality (for bwc) and includes the first sample
        // in the output, so we should do the same in our expected result
        boolean isDeltaHeuristic = newestValue.isEmpty();
        if (nonNull.size() == 1 && isDeltaHeuristic == false) {
            return Matchers.nullValue();
        }

        ExponentialHistogramMerger resets = ExponentialHistogramMerger.create(breaker);
        long prevCount = newestValue.valueCount();
        for (int i = 1; i < nonNull.size(); i++) {
            ExponentialHistogram h = nonNull.get(i);
            if (h.valueCount() > prevCount) {
                resets.add(h);
            }
            prevCount = h.valueCount();
        }

        ExponentialHistogram firstValue = isDeltaHeuristic ? ExponentialHistogram.empty() : oldestValue;

        ExponentialHistogramMerger lastPlusResets = ExponentialHistogramMerger.create(breaker);
        lastPlusResets.add(newestValue);
        lastPlusResets.add(resets.get());

        ExponentialHistogramMerger result = ExponentialHistogramMerger.create(breaker);
        boolean areCumulative = result.setToDifference(lastPlusResets.get(), firstValue);
        if (areCumulative == false) {
            expectedWarnings.add("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.");
            expectedWarnings.add(
                "Line 1:1: java.lang.IllegalArgumentException: "
                    + "Expected cumulative histograms but they were not, results might be inaccurate"
            );
        }
        return equalTo(result.get());
    }
}
