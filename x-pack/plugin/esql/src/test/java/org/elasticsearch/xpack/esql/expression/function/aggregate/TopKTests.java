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
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class TopKTests extends AbstractAggregationTestCase {
    public TopKTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var suppliers = new ArrayList<TestCaseSupplier>();

        for (var limitCaseSupplier : TestCaseSupplier.intCases(1, 100, false)) {
            Stream.of(
                MultiRowTestCaseSupplier.intCases(1, 100, Integer.MIN_VALUE, Integer.MAX_VALUE, true),
                MultiRowTestCaseSupplier.longCases(1, 100, Long.MIN_VALUE, Long.MAX_VALUE, true),
                MultiRowTestCaseSupplier.doubleCases(1, 100, -Double.MAX_VALUE, Double.MAX_VALUE, true),
                MultiRowTestCaseSupplier.dateCases(1, 100),
                MultiRowTestCaseSupplier.booleanCases(1, 100),
                MultiRowTestCaseSupplier.ipCases(1, 100),
                MultiRowTestCaseSupplier.stringCases(1, 100, DataType.KEYWORD),
                MultiRowTestCaseSupplier.stringCases(1, 100, DataType.TEXT)
            )
                .flatMap(List::stream)
                .map(fieldCaseSupplier -> makeSupplier(fieldCaseSupplier, limitCaseSupplier))
                .collect(Collectors.toCollection(() -> suppliers));
        }

        return parameterSuppliersFromTypedDataWithDefaultChecks(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new TopK(source, args.get(0), args.get(1));
    }

    @SuppressWarnings("unchecked")
    private static TestCaseSupplier makeSupplier(
        TestCaseSupplier.TypedDataSupplier fieldSupplier,
        TestCaseSupplier.TypedDataSupplier limitCaseSupplier
    ) {
        List<DataType> dataTypes = List.of(fieldSupplier.type(), DataType.INTEGER);
        DataType expectedType = fieldSupplier.type().noText();

        return new TestCaseSupplier(fieldSupplier.name() + ", " + limitCaseSupplier.name(), dataTypes, () -> {
            var fieldTypedData = fieldSupplier.get();
            var limitTypedData = limitCaseSupplier.get().forceLiteral();
            var limit = (int) limitTypedData.getValue();

            // TOPK ranks descending, like TOP(field, k, "DESC").
            var comparator = Map.Entry.<Comparable<? super Comparable<?>>, Comparable<? super Comparable<?>>>comparingByKey()
                .thenComparing(Map.Entry::getValue);
            comparator = comparator.reversed();
            List<?> expected = IntStream.range(0, fieldTypedData.multiRowData().size())
                .mapToObj(
                    i -> Map.<Comparable<? super Comparable<?>>, Comparable<? super Comparable<?>>>entry(
                        (Comparable<? super Comparable<?>>) fieldTypedData.multiRowData().get(i),
                        (Comparable<? super Comparable<?>>) fieldTypedData.multiRowData().get(i)
                    )
                )
                .sorted(comparator)
                .map(Map.Entry::getValue)
                .limit(limit)
                .toList();

            // A limit of 1 surrogates through TOP into MAX and never runs the TOP code.
            String baseName = limit == 1 ? "Max" : "Top";
            return new TestCaseSupplier.TestCase(
                List.of(fieldTypedData, limitTypedData),
                standardAggregatorName(baseName, fieldTypedData.type()),
                expectedType,
                equalTo(expected.size() == 1 ? expected.get(0) : expected)
            );
        });
    }
}
