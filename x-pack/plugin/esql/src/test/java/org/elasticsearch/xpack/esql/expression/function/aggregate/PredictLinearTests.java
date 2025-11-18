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
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
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

public class PredictLinearTests extends AbstractFunctionTestCase {
    public PredictLinearTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
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
                List<TestCaseSupplier> testCaseSuppliers = makeSuppliers(fieldSupplier);
                suppliers.addAll(testCaseSuppliers);
            }
        }
        List<Object[]> parameters = new ArrayList<>(suppliers.size());
        for (TestCaseSupplier supplier : suppliers) {
            parameters.add(new Object[] { supplier });
        }
        return parameters;
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new PredictLinear(source, args.get(0), Literal.TRUE, AggregateFunction.NO_WINDOW, List.of(args.get(1), args.get(2)));
    }

    @SuppressWarnings("unchecked")
    private static List<TestCaseSupplier> makeSuppliers(TestCaseSupplier.TypedDataSupplier fieldSupplier) {
        DataType type = fieldSupplier.type();
        return List.of(DataType.LONG, DataType.TIME_DURATION, DataType.DOUBLE, DataType.INTEGER)
            .stream()
            .map(tType -> new TestCaseSupplier(fieldSupplier.name(), List.of(type, DataType.DATETIME, tType), () -> {
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
                TestCaseSupplier.TypedData theTField = TestCaseSupplier.TypedData.multiRow(maxTimestamps, tType, "some_t_field");

                List<Object> nonNullDataRows = dataRows.stream().filter(Objects::nonNull).toList();
                Matcher<?> matcher;
                if (nonNullDataRows.size() < 2) {
                    matcher = Matchers.nullValue();
                } else {
                    var lastValue = ((Number) nonNullDataRows.getFirst()).doubleValue();
                    var secondLastValue = ((Number) nonNullDataRows.get(1)).doubleValue();
                    var increase = lastValue >= secondLastValue ? lastValue - secondLastValue : lastValue;
                    var largestTimestamp = timestamps.get(0);
                    var secondLargestTimestamp = timestamps.get(1);
                    var smallestTimestamp = timestamps.getLast();
                    matcher = Matchers.allOf(
                        Matchers.greaterThanOrEqualTo(increase / (largestTimestamp - smallestTimestamp) * 1000 * 0.9),
                        Matchers.lessThanOrEqualTo(
                            increase / (largestTimestamp - secondLargestTimestamp) * (largestTimestamp - smallestTimestamp) * 1000
                        )
                    );
                }

                return new TestCaseSupplier.TestCase(
                    List.of(fieldTypedData, timestampsField, theTField),
                    Matchers.stringContainsInOrder("GroupingAggregator", "PredictLinear", "GroupingAggregatorFunction"),
                    DataType.DOUBLE,
                    matcher
                );
            }))
            .toList();
    }

    public static List<DocsV3Support.Param> signatureTypes(List<DocsV3Support.Param> params) {
        assertThat(params, hasSize(3));
        assertThat(params.get(1).dataType(), equalTo(DataType.DATETIME));
        assertThat(
            params.get(2).dataType(),
            Matchers.in(List.of(DataType.LONG, DataType.TIME_DURATION, DataType.DOUBLE, DataType.INTEGER))
        );
        return List.of(params.get(0), params.get(2));
    }
}
