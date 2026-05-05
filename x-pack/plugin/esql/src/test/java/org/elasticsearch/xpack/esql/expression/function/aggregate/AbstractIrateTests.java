/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

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

public abstract class AbstractIrateTests extends AbstractAggregationTestCase {
    protected AbstractIrateTests(Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
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

    protected static List<List<TestCaseSupplier.TypedDataSupplier>> valuesSuppliers() {
        return List.of(
            MultiRowTestCaseSupplier.longCases(1, 1000, 0, 1000_000_000, true),
            MultiRowTestCaseSupplier.intCases(1, 1000, 0, 1000_000_000, true),
            MultiRowTestCaseSupplier.doubleCases(1, 1000, 0, 1000_000_000, true)
        );
    }

    private static DataType counterType(DataType type) {
        return switch (type) {
            case DOUBLE -> DataType.COUNTER_DOUBLE;
            case LONG -> DataType.COUNTER_LONG;
            case INTEGER -> DataType.COUNTER_INTEGER;
            default -> throw new AssertionError("unknown type for counter: " + type);
        };
    }

    protected static TestCaseSupplier makeSupplier(
        TestCaseSupplier.TypedDataSupplier fieldSupplier,
        RateTests.TemporalityParameter temporality,
        boolean supportsDelta,
        String aggName
    ) {
        DataType type = counterType(fieldSupplier.type());
        return new TestCaseSupplier(
            fieldSupplier.name() + " temporality=" + temporality,
            List.of(type, DataType.DATETIME, DataType.KEYWORD, DataType.INTEGER, DataType.LONG),
            () -> {
                TestCaseSupplier.TypedData fieldTypedData = fieldSupplier.get();
                List<Object> dataRows = fieldTypedData.multiRowData();
                if (randomBoolean()) {
                    List<Object> withNulls = new ArrayList<>(dataRows.size());
                    for (Object dataRow : dataRows) {
                        if (randomBoolean()) {
                            withNulls.add(null);
                        } else {
                            withNulls.add(dataRow);
                        }
                    }
                    dataRows = withNulls;
                }
                fieldTypedData = TestCaseSupplier.TypedData.multiRow(dataRows, type, fieldTypedData.name());
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
                TestCaseSupplier.TypedData sliceIndexType = TestCaseSupplier.TypedData.multiRow(slices, DataType.INTEGER, "_slice_index");
                TestCaseSupplier.TypedData nextTimestampType = TestCaseSupplier.TypedData.multiRow(
                    maxTimestamps,
                    DataType.LONG,
                    "_max_timestamp"
                );

                List<Object> nonNullDataRows = dataRows.stream().filter(Objects::nonNull).toList();
                final Matcher<?> matcher = irateMatcher(nonNullDataRows, timestamps, temporality, supportsDelta);
                TestCaseSupplier.TestCase result = new TestCaseSupplier.TestCase(
                    List.of(fieldTypedData, timestampsField, temporalityField, sliceIndexType, nextTimestampType),
                    standardAggregatorName(aggName, fieldTypedData.type()),
                    DataType.DOUBLE,
                    matcher
                );
                if (nonNullDataRows.isEmpty() == false) {
                    if (temporality == RateTests.TemporalityParameter.INVALID) {
                        return result.withWarning(
                            "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded."
                        )
                            .withWarning(
                                "Line 1:1: org.elasticsearch.compute.aggregation.InvalidTemporalityException: "
                                    + "Invalid temporality value: [gotcha], expected [cumulative] or [delta]"
                            );
                    } else if (temporality == RateTests.TemporalityParameter.DELTA && supportsDelta == false) {
                        return result.withWarning(
                            "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded."
                        )
                            .withWarning(
                                "Line 1:1: java.lang.IllegalArgumentException: Some nodes in your cluster don't support delta temporality"
                                    + " for counters yet. The affected time series are excluded from irate calculations."
                                    + " Upgrade your cluster to fix this."
                            );
                    }
                }
                return result;
            }
        );
    }

    protected static Matcher<?> irateMatcher(
        List<Object> nonNullDataRows,
        List<Long> timestamps,
        RateTests.TemporalityParameter temporality,
        boolean supportsDelta
    ) {
        if (nonNullDataRows.size() < 2
            || (temporality == RateTests.TemporalityParameter.DELTA && supportsDelta == false)
            || temporality == RateTests.TemporalityParameter.INVALID) {
            return Matchers.nullValue();
        }
        double lastValue = ((Number) nonNullDataRows.getFirst()).doubleValue();
        double secondLastValue = ((Number) nonNullDataRows.get(1)).doubleValue();
        double increase;
        if (temporality != RateTests.TemporalityParameter.DELTA && lastValue >= secondLastValue) {
            increase = lastValue - secondLastValue;
        } else {
            increase = lastValue == -0.0 ? 0 : lastValue;
        }
        if (increase == 0.0) {
            return Matchers.equalTo(0.0);
        }
        long largestTimestamp = timestamps.get(0);
        long secondLargestTimestamp = timestamps.get(1);
        long smallestTimestamp = timestamps.getLast();
        return Matchers.allOf(
            Matchers.greaterThanOrEqualTo(increase / (largestTimestamp - smallestTimestamp) * 1000 * 0.9),
            Matchers.lessThanOrEqualTo(
                increase / (largestTimestamp - secondLargestTimestamp) * (largestTimestamp - smallestTimestamp) * 1000
            )
        );
    }
}
