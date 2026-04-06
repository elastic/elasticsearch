/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractAggregationTestCase;
import org.elasticsearch.xpack.esql.expression.function.MultiRowTestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class SparklineTests extends AbstractAggregationTestCase {
    public SparklineTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var suppliers = new ArrayList<TestCaseSupplier>();

        List<TestCaseSupplier.TypedDataSupplier> fieldSuppliers = List.of(
            MultiRowTestCaseSupplier.intCases(1, 100, 0, 1_000_000, true).get(0),
            MultiRowTestCaseSupplier.longCases(1, 100, 0L, 1_000_000_000L, true).get(0),
            MultiRowTestCaseSupplier.doubleCases(1, 100, 0, 1_000_000, true).get(0)
        );

        var fromToTypes = List.of(DataType.DATETIME, DataType.KEYWORD, DataType.TEXT);

        int maximumTypes = Math.max(fieldSuppliers.size(), fromToTypes.size());
        for (int i = 0; i < maximumTypes; i++) {
            suppliers.add(
                makeSupplier(
                    fieldSuppliers.get(i % fieldSuppliers.size()),
                    DataType.DATETIME,
                    fromToTypes.get(i % fromToTypes.size()),
                    fromToTypes.get(i % fromToTypes.size())
                )
            );
        }

        return parameterSuppliersFromTypedDataWithDefaultChecks(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Sparkline(source, args.get(0), args.get(1), args.get(2), args.get(3), args.get(4));
    }

    @Override
    public void testAggregate() {
        assumeTrue("Sparkline does not implement ToAggregator", false);
    }

    @Override
    public void testAggregateToString() {
        assumeTrue("Sparkline does not implement ToAggregator", false);
    }

    @Override
    public void testAggregateIntermediate() {
        assumeTrue("Sparkline does not implement ToAggregator", false);
    }

    @Override
    public void testGroupingAggregate() {
        assumeTrue("Sparkline does not implement ToAggregator", false);
    }

    @Override
    public void testGroupingAggregateToString() {
        assumeTrue("Sparkline does not implement ToAggregator", false);
    }

    private static Object randomValueForType(DataType type) {
        return switch (type) {
            case INTEGER -> randomIntBetween(1, 1_000_000);
            case LONG -> randomLongBetween(1, 1_000_000_000L);
            case DOUBLE -> (double) randomLongBetween(1, 1_000_000);
            case DATETIME -> randomLongBetween(0, 2_000_000_000_000L);
            case DATE_NANOS -> randomLongBetween(0, 2_000_000_000_000_000_000L);
            case KEYWORD, TEXT -> new BytesRef("2024-01-01T00:00:00.000Z");
            default -> throw new IllegalArgumentException("Unsupported type: " + type);
        };
    }

    private static TestCaseSupplier makeSupplier(
        TestCaseSupplier.TypedDataSupplier fieldSupplier,
        DataType keyType,
        DataType fromType,
        DataType toType
    ) {
        return new TestCaseSupplier(
            fieldSupplier.name() + " with " + keyType + " key, " + fromType + " from, " + toType + " to",
            List.of(fieldSupplier.type(), keyType, DataType.INTEGER, fromType, toType),
            () -> {
                TestCaseSupplier.TypedData fieldData = fieldSupplier.get();
                TestCaseSupplier.TypedData keyData = new TestCaseSupplier.TypedData(randomValueForType(keyType), keyType, "key");
                TestCaseSupplier.TypedData bucketsData = new TestCaseSupplier.TypedData(
                    randomIntBetween(1, 100),
                    DataType.INTEGER,
                    "buckets"
                );
                TestCaseSupplier.TypedData fromData = new TestCaseSupplier.TypedData(randomValueForType(fromType), fromType, "from");
                TestCaseSupplier.TypedData toData = new TestCaseSupplier.TypedData(randomValueForType(toType), toType, "to");

                return new TestCaseSupplier.TestCase(
                    List.of(fieldData, keyData, bucketsData, fromData, toData),
                    "Sparkline" + fieldSupplier.name(),
                    fieldSupplier.type(),
                    Matchers.anything()
                );
            }
        );
    }
}
