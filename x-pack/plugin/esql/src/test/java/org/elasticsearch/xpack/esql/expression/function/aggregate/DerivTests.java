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
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class DerivTests extends AbstractAggregationTestCase {
    public DerivTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
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
                TestCaseSupplier testCaseSupplier = makeSupplier(fieldSupplier);
                suppliers.add(testCaseSupplier);
            }
        }
        return parameterSuppliersFromTypedDataWithDefaultChecks(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Deriv(source, args.get(0), Literal.TRUE, AggregateFunction.NO_WINDOW, args.get(1));
    }

    @SuppressWarnings("unchecked")
    private static TestCaseSupplier makeSupplier(TestCaseSupplier.TypedDataSupplier fieldSupplier) {
        DataType type = fieldSupplier.type();
        return new TestCaseSupplier(fieldSupplier.name(), List.of(type, DataType.DATETIME), () -> {
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
            long lastTimestamp = randomLongBetween(0, 1_000_000);
            for (int row = 0; row < dataRows.size(); row++) {
                lastTimestamp += randomLongBetween(1, 10_000);
                timestamps.add(lastTimestamp);
            }
            timestamps = timestamps.reversed();
            double sumVal = 0d;
            double sumTs = 0d;
            double sumTsVal = 0d;
            double sumTsSquare = 0d;
            int count = 0;
            for (int i = 0; i < dataRows.size(); i++) {
                Object row = dataRows.get(i);
                if (row == null) {
                    continue;
                }
                double val = ((Number) row).doubleValue();
                long ts = timestamps.get(i);
                count++;
                sumVal += val;
                sumTs += ts;
                sumTsVal += ts * val;
                sumTsSquare += ts * ts;
            }
            double numerator = count * sumTsVal - sumTs * sumVal;
            double denominator = count * sumTsSquare - sumTs * sumTs;
            Matcher<?> matcher;
            if (count < 2 || denominator == 0d) {
                matcher = Matchers.nullValue();
            } else {
                double slope = numerator / denominator * 1000.0;
                double tolerance = Math.max(0.001, Math.abs(slope) * 1e-6);
                matcher = closeTo(slope, tolerance);
            }
            TestCaseSupplier.TypedData timestampsField = TestCaseSupplier.TypedData.multiRow(timestamps, DataType.DATETIME, "timestamps");
            return new TestCaseSupplier.TestCase(
                List.of(fieldTypedData, timestampsField),
                standardAggregatorName("Deriv", fieldTypedData.type()),
                DataType.DOUBLE,
                matcher
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
