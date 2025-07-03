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
import org.elasticsearch.xpack.esql.expression.function.MultiRowTestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.IntStream;

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
            MultiRowTestCaseSupplier.doubleCases(1, 1000, -Double.MAX_VALUE, Double.MAX_VALUE, true)

        );
        for (List<TestCaseSupplier.TypedDataSupplier> valuesSupplier : valuesSuppliers) {
            for (TestCaseSupplier.TypedDataSupplier fieldSupplier : valuesSupplier) {
                TestCaseSupplier testCaseSupplier = makeSupplier(fieldSupplier);
                suppliers.add(testCaseSupplier);
            }
        }
        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new LastOverTime(source, args.get(0), args.get(1));
    }

    @Override
    public void testAggregate() {
        assumeTrue("time-series aggregation doesn't support ungrouped", false);
    }

    @Override
    public void testAggregateIntermediate() {
        assumeTrue("time-series aggregation doesn't support ungrouped", false);
    }

    private static TestCaseSupplier makeSupplier(TestCaseSupplier.TypedDataSupplier fieldSupplier) {
        DataType type = fieldSupplier.type();
        return new TestCaseSupplier(fieldSupplier.name(), List.of(type, DataType.DATETIME), () -> {
            TestCaseSupplier.TypedData fieldTypedData = fieldSupplier.get();
            List<Object> dataRows = fieldTypedData.multiRowData();
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
                "LastOverTime[field=Attribute[channel=0],timestamp=Attribute[channel=1]]",
                type,
                equalTo(expected)
            );
        });
    }

    public static List<DataType> signatureTypes(List<DataType> testCaseTypes) {
        assertThat(testCaseTypes, hasSize(2));
        assertThat(testCaseTypes.get(1), equalTo(DataType.DATETIME));
        return List.of(testCaseTypes.get(0));
    }
}
