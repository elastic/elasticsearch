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
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.MultiRowTestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.appliesTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class LastOverTimeTests extends AbstractAggregationTestCase {
    public LastOverTimeTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var suppliers = new ArrayList<TestCaseSupplier>();

        var valuesSuppliers = List.of(
            MultiRowTestCaseSupplier.longCases(1, 1000, Long.MIN_VALUE, Long.MAX_VALUE, true),
            MultiRowTestCaseSupplier.intCases(1, 1000, Integer.MIN_VALUE, Integer.MAX_VALUE, true),
            MultiRowTestCaseSupplier.doubleCases(1, 1000, -Double.MAX_VALUE, Double.MAX_VALUE, true),
            MultiRowTestCaseSupplier.tsidCases(1, 1000)
        );
        for (List<TestCaseSupplier.TypedDataSupplier> valuesSupplier : valuesSuppliers) {
            for (TestCaseSupplier.TypedDataSupplier fieldSupplier : valuesSupplier) {
                DataType type = fieldSupplier.type();
                suppliers.add(makeSupplier(fieldSupplier, type));
                switch (type) {
                    case LONG -> suppliers.add(makeSupplier(fieldSupplier, DataType.COUNTER_LONG));
                    case DOUBLE -> suppliers.add(makeSupplier(fieldSupplier, DataType.COUNTER_DOUBLE));
                    case INTEGER -> suppliers.add(makeSupplier(fieldSupplier, DataType.COUNTER_INTEGER));
                }
            }
        }
        return parameterSuppliersFromTypedDataWithDefaultChecks(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new LastOverTime(source, args.get(0), AggregateFunction.NO_WINDOW, args.get(1));
    }

    @Override
    public void testAggregate() {
        assumeTrue("time-series aggregation doesn't support ungrouped", false);
    }

    @Override
    public void testAggregateIntermediate() {
        assumeTrue("time-series aggregation doesn't support ungrouped", false);
    }

    private static TestCaseSupplier makeSupplier(TestCaseSupplier.TypedDataSupplier fieldSupplier, DataType type) {
        return new TestCaseSupplier(fieldSupplier.name(), List.of(type, DataType.DATETIME), () -> {
            TestCaseSupplier.TypedData fieldTypedData = fieldSupplier.get();
            List<Object> dataRows = fieldTypedData.multiRowData();
            fieldTypedData = TestCaseSupplier.TypedData.multiRow(dataRows, type, fieldTypedData.name());
            List<Long> timestamps = IntStream.range(0, dataRows.size()).mapToLong(unused -> randomNonNegativeLong()).boxed().toList();
            TestCaseSupplier.TypedData timestampsField = TestCaseSupplier.TypedData.multiRow(timestamps, DataType.DATETIME, "timestamps");
            Object expected = null;
            long lastTimestamp = Long.MIN_VALUE;
            for (int i = 0; i < dataRows.size(); i++) {
                if (i == 0) {
                    expected = dataRows.get(i);
                    lastTimestamp = timestamps.get(i);
                } else if (timestamps.get(i) > lastTimestamp) {
                    expected = dataRows.get(i);
                    lastTimestamp = timestamps.get(i);
                }
            }
            return new TestCaseSupplier.TestCase(
                List.of(fieldTypedData, timestampsField),
                standardAggregatorName("Last", type) + "ByTimestamp",
                fieldSupplier.type(),
                equalTo(expected)
            );
        });
    }

    public static List<DocsV3Support.Param> signatureTypes(List<DocsV3Support.Param> params) {
        assertThat(params, hasSize(2));
        assertThat(params.get(1).dataType(), equalTo(DataType.DATETIME));
        var preview = appliesTo(FunctionAppliesToLifecycle.PREVIEW, "9.3.0", "", false);
        DocsV3Support.Param window = new DocsV3Support.Param(DataType.TIME_DURATION, List.of(preview));
        return List.of(params.get(0), window);
    }
}
