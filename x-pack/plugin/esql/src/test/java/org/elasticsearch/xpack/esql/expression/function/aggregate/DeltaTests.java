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
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractAggregationTestCase;
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;
import org.elasticsearch.xpack.esql.expression.function.MultiRowTestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class DeltaTests extends AbstractAggregationTestCase {
    public DeltaTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var suppliers = new ArrayList<TestCaseSupplier>();

        var valuesSuppliers = List.of(
            MultiRowTestCaseSupplier.longCases(1, 1000, -1000_000_000, 1000_000_000, false),
            MultiRowTestCaseSupplier.intCases(1, 1000, -1000_000_000, 1000_000_000, false),
            MultiRowTestCaseSupplier.doubleCases(1, 1000, -1000_000_000, 1000_000_000, false)

        );
        for (List<TestCaseSupplier.TypedDataSupplier> valuesSupplier : valuesSuppliers) {
            for (TestCaseSupplier.TypedDataSupplier fieldSupplier : valuesSupplier) {
                TestCaseSupplier testCaseSupplier = makeSupplier(fieldSupplier);
                suppliers.add(testCaseSupplier);
            }
        }
        return parameterSuppliersFromTypedDataWithDefaultChecks(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Delta(source, args.get(0), args.get(1));
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

    private static TestCaseSupplier makeSupplier(TestCaseSupplier.TypedDataSupplier fieldSupplier) {
        DataType type = fieldSupplier.type();
        return new TestCaseSupplier(fieldSupplier.name(), List.of(type, DataType.DATETIME, DataType.INTEGER, DataType.LONG), () -> {
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
            List<Integer> slices = new ArrayList<>();
            List<Long> maxTimestamps = new ArrayList<>();
            long lastTimestamp = randomLongBetween(0, 1_000_000);
            for (int row = 0; row < dataRows.size(); row++) {
                lastTimestamp += randomLongBetween(1, 10_000);
                timestamps.add(lastTimestamp);
                slices.add(0);
                maxTimestamps.add(Long.MAX_VALUE);
            }
            TestCaseSupplier.TypedData timestampsField = TestCaseSupplier.TypedData.multiRow(
                timestamps.reversed(),
                DataType.DATETIME,
                "timestamps"
            );
            TestCaseSupplier.TypedData sliceIndexType = TestCaseSupplier.TypedData.multiRow(slices, DataType.INTEGER, "_slice_index");
            TestCaseSupplier.TypedData nextTimestampType = TestCaseSupplier.TypedData.multiRow(
                maxTimestamps,
                DataType.LONG,
                "_max_timestamp"
            );

            dataRows = dataRows.stream().filter(Objects::nonNull).toList();
            final Matcher<?> matcher;
            if (dataRows.size() < 2) {
                matcher = Matchers.nullValue();
            } else {
                var maxdelta = switch (fieldTypedData.type().widenSmallNumeric()) {
                    case INTEGER, COUNTER_INTEGER -> dataRows.stream().mapToInt(v -> (Integer) v).max().orElse(0);
                    case LONG, COUNTER_LONG -> dataRows.stream().mapToLong(v -> (Long) v).max().orElse(0L);
                    case DOUBLE, COUNTER_DOUBLE -> dataRows.stream().mapToDouble(v -> (Double) v).max().orElse(0.0);
                    default -> throw new IllegalStateException("Unexpected value: " + fieldTypedData.type());
                };
                var mindelta = switch (fieldTypedData.type().widenSmallNumeric()) {
                    case INTEGER, COUNTER_INTEGER -> dataRows.stream().mapToInt(v -> (Integer) v).min().orElse(0);
                    case LONG, COUNTER_LONG -> dataRows.stream().mapToLong(v -> (Long) v).min().orElse(0L);
                    case DOUBLE, COUNTER_DOUBLE -> dataRows.stream().mapToDouble(v -> (Double) v).min().orElse(0.0);
                    default -> throw new IllegalStateException("Unexpected value: " + fieldTypedData.type());
                };
                maxdelta = Math.max(maxdelta, maxdelta - Math.min(mindelta, 0));
                mindelta = -maxdelta;
                matcher = Matchers.allOf(Matchers.greaterThanOrEqualTo(mindelta), Matchers.lessThanOrEqualTo(maxdelta));
            }
            return new TestCaseSupplier.TestCase(
                List.of(fieldTypedData, timestampsField, sliceIndexType, nextTimestampType),
                standardAggregatorName("Delta", fieldTypedData.type()),
                DataType.DOUBLE,
                matcher
            );
        });
    }

    public static List<DocsV3Support.Param> signatureTypes(List<DocsV3Support.Param> params) {
        assertThat(params, hasSize(4));
        assertThat(params.get(1).dataType(), equalTo(DataType.DATETIME));
        assertThat(params.get(2).dataType(), equalTo(DataType.INTEGER));
        assertThat(params.get(3).dataType(), equalTo(DataType.LONG));
        return List.of(params.get(0));
    }
}
