/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractAggregationTestCase;
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.MultiRowTestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.appliesTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class IrateTests extends AbstractAggregationTestCase {
    public IrateTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var suppliers = new ArrayList<TestCaseSupplier>();

        var valuesSuppliers = List.of(
            MultiRowTestCaseSupplier.longCases(1, 1000, 0, 1000_000_000, true),
            MultiRowTestCaseSupplier.intCases(1, 1000, 0, 1000_000_000, true),
            MultiRowTestCaseSupplier.doubleCases(1, 1000, 0, 1000_000_000, true)
        );
        for (List<TestCaseSupplier.TypedDataSupplier> valuesSupplier : valuesSuppliers) {
            for (TestCaseSupplier.TypedDataSupplier fieldSupplier : valuesSupplier) {
                for (RateTests.TemporalityParameter temporality : RateTests.TemporalityParameter.values()) {
                    suppliers.add(makeSupplier(fieldSupplier, temporality));
                }
            }
        }
        return parameterSuppliersFromTypedDataWithDefaultChecks(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Irate(source, args.get(0), Literal.TRUE, AggregateFunction.NO_WINDOW, args.get(1), args.get(2));
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

    private static DataType counterType(DataType type) {
        return switch (type) {
            case DOUBLE -> DataType.COUNTER_DOUBLE;
            case LONG -> DataType.COUNTER_LONG;
            case INTEGER -> DataType.COUNTER_INTEGER;
            default -> throw new AssertionError("unknown type for counter: " + type);
        };
    }

    private static TestCaseSupplier makeSupplier(
        TestCaseSupplier.TypedDataSupplier fieldSupplier,
        RateTests.TemporalityParameter temporality
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
                final Matcher<?> matcher = irateMatcher(nonNullDataRows, timestamps, temporality);
                TestCaseSupplier.TestCase result = new TestCaseSupplier.TestCase(
                    List.of(fieldTypedData, timestampsField, temporalityField, sliceIndexType, nextTimestampType),
                    standardAggregatorName("Irate", fieldTypedData.type()),
                    DataType.DOUBLE,
                    matcher
                );
                if (temporality == RateTests.TemporalityParameter.INVALID && nonNullDataRows.isEmpty() == false) {
                    return result.withWarning(
                        "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded."
                    )
                        .withWarning(
                            "Line 1:1: org.elasticsearch.compute.aggregation.InvalidTemporalityException: "
                                + "Invalid temporality value: [gotcha], expected [cumulative] or [delta]"
                        );
                }
                if (temporality == RateTests.TemporalityParameter.DELTA && nonNullDataRows.isEmpty() == false) {
                    return result.withWarning(
                        "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded."
                    )
                        .withWarning(
                            "Line 1:1: java.lang.IllegalArgumentException: Some nodes in your cluster don't support delta temporality yet,"
                                + " delta temporality series will be ignored. Upgrade your cluster to fix this."
                        );
                }
                return result;
            }
        );
    }

    private static Matcher<?> irateMatcher(
        List<Object> nonNullDataRows,
        List<Long> timestamps,
        RateTests.TemporalityParameter temporality
    ) {
        // TODO: implement delta temporality support for irate; for now it returns null
        if (nonNullDataRows.size() < 2
            || temporality == RateTests.TemporalityParameter.DELTA
            || temporality == RateTests.TemporalityParameter.INVALID) {
            return Matchers.nullValue();
        }
        var lastValue = ((Number) nonNullDataRows.getFirst()).doubleValue();
        var secondLastValue = ((Number) nonNullDataRows.get(1)).doubleValue();
        var increase = lastValue >= secondLastValue ? lastValue - secondLastValue : lastValue;
        var largestTimestamp = timestamps.get(0);
        var secondLargestTimestamp = timestamps.get(1);
        var smallestTimestamp = timestamps.getLast();
        return Matchers.allOf(
            Matchers.greaterThanOrEqualTo(increase / (largestTimestamp - smallestTimestamp) * 1000 * 0.9),
            Matchers.lessThanOrEqualTo(
                increase / (largestTimestamp - secondLargestTimestamp) * (largestTimestamp - smallestTimestamp) * 1000
            )
        );
    }

    public static List<DocsV3Support.Param> signatureTypes(List<DocsV3Support.Param> params) {
        assertThat(params, hasSize(5));
        assertThat(params.get(1).dataType(), equalTo(DataType.DATETIME));
        assertThat(params.get(2).dataType(), equalTo(DataType.KEYWORD));
        assertThat(params.get(3).dataType(), equalTo(DataType.INTEGER));
        assertThat(params.get(4).dataType(), equalTo(DataType.LONG));
        ArrayList<DocsV3Support.Param> result = new ArrayList<>();
        result.add(params.get(0));
        var preview = appliesTo(FunctionAppliesToLifecycle.PREVIEW, "9.3.0", "", false);
        result.add(new DocsV3Support.Param(DataType.TIME_DURATION, List.of(preview)));
        return result;
    }
}
