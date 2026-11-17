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
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.appliesTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class ChangesTests extends AbstractAggregationTestCase {
    public ChangesTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var suppliers = new ArrayList<TestCaseSupplier>();

        var numericSuppliers = List.of(
            MultiRowTestCaseSupplier.intCases(1, 1000, -1_000_000_000, 1_000_000_000, true),
            MultiRowTestCaseSupplier.longCases(1, 1000, -1_000_000_000, 1_000_000_000, true),
            MultiRowTestCaseSupplier.doubleCases(1, 1000, -1_000_000_000, 1_000_000_000, true)
        );
        for (List<TestCaseSupplier.TypedDataSupplier> typeSuppliers : numericSuppliers) {
            for (TestCaseSupplier.TypedDataSupplier fieldSupplier : typeSuppliers) {
                suppliers.add(makeSupplier(fieldSupplier, fieldSupplier.type()));
                suppliers.add(makeSupplier(fieldSupplier, counterType(fieldSupplier.type())));
            }
        }
        for (TestCaseSupplier.TypedDataSupplier fieldSupplier : MultiRowTestCaseSupplier.exponentialHistogramCases(1, 100)) {
            suppliers.add(makeSupplier(fieldSupplier, DataType.EXPONENTIAL_HISTOGRAM));
        }

        return parameterSuppliersFromTypedDataWithDefaultChecks(suppliers, NullTypeExpectation.OUTPUT_KEEPS_TYPE);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Changes(source, args.get(0), args.get(1), Literal.TRUE, AggregateFunction.NO_WINDOW);
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
            default -> throw new AssertionError("unknown numeric type: " + type);
        };
    }

    private static TestCaseSupplier makeSupplier(TestCaseSupplier.TypedDataSupplier fieldSupplier, DataType type) {
        return new TestCaseSupplier(
            fieldSupplier.name() + " as " + type.typeName(),
            List.of(type, DataType.DATETIME, DataType.INTEGER),
            () -> {
                TestCaseSupplier.TypedData suppliedField = fieldSupplier.get();
                List<Object> dataRows = suppliedField.multiRowData();
                if (randomBoolean()) {
                    List<Object> withNulls = new ArrayList<>(dataRows.size());
                    for (Object dataRow : dataRows) {
                        withNulls.add(randomBoolean() ? null : dataRow);
                    }
                    dataRows = withNulls;
                }
                TestCaseSupplier.TypedData field = TestCaseSupplier.TypedData.multiRow(dataRows, type, suppliedField.name());

                List<Long> timestamps = new ArrayList<>(dataRows.size());
                List<Integer> slices = new ArrayList<>(dataRows.size());
                long timestamp = randomLongBetween(0, 1_000_000);
                for (int row = 0; row < dataRows.size(); row++) {
                    timestamp += randomLongBetween(1, 10_000);
                    timestamps.add(timestamp);
                    slices.add(0);
                }

                TestCaseSupplier.TypedData timestampField = TestCaseSupplier.TypedData.multiRow(
                    timestamps.reversed(),
                    DataType.DATETIME,
                    "timestamps"
                );
                TestCaseSupplier.TypedData sliceIndexField = TestCaseSupplier.TypedData.multiRow(slices, DataType.INTEGER, "_slice_index");
                return new TestCaseSupplier.TestCase(
                    List.of(field, timestampField, sliceIndexField),
                    standardAggregatorName("Changes", type),
                    DataType.LONG,
                    changesMatcher(dataRows, type)
                );
            }
        );
    }

    private static Matcher<?> changesMatcher(List<Object> dataRows, DataType type) {
        Object previous = null;
        long changes = 0;
        boolean seen = false;
        for (Object value : dataRows) {
            if (value == null) {
                continue;
            }
            if (seen && valuesEqual(previous, value, type) == false) {
                changes++;
            }
            previous = value;
            seen = true;
        }
        return seen ? equalTo(changes) : Matchers.nullValue();
    }

    private static boolean valuesEqual(Object left, Object right, DataType type) {
        return switch (type) {
            case DOUBLE, COUNTER_DOUBLE -> {
                double leftDouble = (double) left;
                double rightDouble = (double) right;
                yield leftDouble == rightDouble || (Double.isNaN(leftDouble) && Double.isNaN(rightDouble));
            }
            case EXPONENTIAL_HISTOGRAM -> left.equals(right);
            default -> left.equals(right);
        };
    }

    public static List<DocsV3Support.Param> signatureTypes(List<DocsV3Support.Param> params) {
        assertThat(params, hasSize(3));
        assertThat(params.get(1).dataType(), equalTo(DataType.DATETIME));
        assertThat(params.get(2).dataType(), equalTo(DataType.INTEGER));
        var ga = appliesTo(FunctionAppliesToLifecycle.GA, "9.6.0", "", false);
        DocsV3Support.Param window = new DocsV3Support.Param(DataType.TIME_DURATION, List.of(ga));
        return List.of(params.get(0), window);
    }

    /**
     * Filters out implicitly injected parameters to ensure CONSTANT hint validation
     * only checks declared @Param arguments.
     */
    public static List<TestCaseSupplier.TypedData> providedParameters(List<TestCaseSupplier.TypedData> params) {
        assertThat(params, hasSize(3));
        assertThat(params.get(1).type(), equalTo(DataType.DATETIME));
        assertThat(params.get(2).type(), equalTo(DataType.INTEGER));
        return List.of(params.get(0));
    }
}
