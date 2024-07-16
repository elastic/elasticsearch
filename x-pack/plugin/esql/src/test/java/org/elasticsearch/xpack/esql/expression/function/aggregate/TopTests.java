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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class TopTests extends AbstractAggregationTestCase {
    public TopTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var suppliers = new ArrayList<TestCaseSupplier>();

        for (var limitCaseSupplier : TestCaseSupplier.intCases(1, 1000, false)) {
            for (String order : List.of("asc", "desc")) {
                Stream.of(
                    MultiRowTestCaseSupplier.intCases(1, 1000, Integer.MIN_VALUE, Integer.MAX_VALUE, true),
                    MultiRowTestCaseSupplier.longCases(1, 1000, Long.MIN_VALUE, Long.MAX_VALUE, true),
                    MultiRowTestCaseSupplier.doubleCases(1, 1000, -Double.MAX_VALUE, Double.MAX_VALUE, true),
                    MultiRowTestCaseSupplier.dateCases(1, 1000)
                )
                    .flatMap(List::stream)
                    .map(fieldCaseSupplier -> TopTests.makeSupplier(fieldCaseSupplier, limitCaseSupplier, order))
                    .collect(Collectors.toCollection(() -> suppliers));
            }
        }

        suppliers.addAll(
            List.of(
                // Surrogates
                new TestCaseSupplier(
                    List.of(DataType.INTEGER, DataType.INTEGER, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(List.of(5, 8, -2, 0, 200), DataType.INTEGER, "field"),
                            new TestCaseSupplier.TypedData(1, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData(new BytesRef("desc"), DataType.KEYWORD, "order").forceLiteral()
                        ),
                        "Top[field=Attribute[channel=0], limit=Attribute[channel=1], order=Attribute[channel=2]]",
                        DataType.INTEGER,
                        equalTo(200)
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.LONG, DataType.INTEGER, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(List.of(5L, 8L, -2L, 0L, 200L), DataType.LONG, "field"),
                            new TestCaseSupplier.TypedData(1, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData(new BytesRef("desc"), DataType.KEYWORD, "order").forceLiteral()
                        ),
                        "Top[field=Attribute[channel=0], limit=Attribute[channel=1], order=Attribute[channel=2]]",
                        DataType.LONG,
                        equalTo(200L)
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.DOUBLE, DataType.INTEGER, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(List.of(5., 8., -2., 0., 200.), DataType.DOUBLE, "field"),
                            new TestCaseSupplier.TypedData(1, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData(new BytesRef("desc"), DataType.KEYWORD, "order").forceLiteral()
                        ),
                        "Top[field=Attribute[channel=0], limit=Attribute[channel=1], order=Attribute[channel=2]]",
                        DataType.DOUBLE,
                        equalTo(200.)
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.DATETIME, DataType.INTEGER, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(List.of(5L, 8L, 2L, 0L, 200L), DataType.DATETIME, "field"),
                            new TestCaseSupplier.TypedData(1, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData(new BytesRef("desc"), DataType.KEYWORD, "order").forceLiteral()
                        ),
                        "Top[field=Attribute[channel=0], limit=Attribute[channel=1], order=Attribute[channel=2]]",
                        DataType.DATETIME,
                        equalTo(200L)
                    )
                ),

                // Folding
                new TestCaseSupplier(
                    List.of(DataType.INTEGER, DataType.INTEGER, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(List.of(200), DataType.INTEGER, "field"),
                            new TestCaseSupplier.TypedData(1, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData(new BytesRef("desc"), DataType.KEYWORD, "order").forceLiteral()
                        ),
                        "Top[field=Attribute[channel=0], limit=Attribute[channel=1], order=Attribute[channel=2]]",
                        DataType.INTEGER,
                        equalTo(200)
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.LONG, DataType.INTEGER, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(List.of(200L), DataType.LONG, "field"),
                            new TestCaseSupplier.TypedData(1, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData(new BytesRef("desc"), DataType.KEYWORD, "order").forceLiteral()
                        ),
                        "Top[field=Attribute[channel=0], limit=Attribute[channel=1], order=Attribute[channel=2]]",
                        DataType.LONG,
                        equalTo(200L)
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.DOUBLE, DataType.INTEGER, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(List.of(200.), DataType.DOUBLE, "field"),
                            new TestCaseSupplier.TypedData(1, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData(new BytesRef("desc"), DataType.KEYWORD, "order").forceLiteral()
                        ),
                        "Top[field=Attribute[channel=0], limit=Attribute[channel=1], order=Attribute[channel=2]]",
                        DataType.DOUBLE,
                        equalTo(200.)
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.DATETIME, DataType.INTEGER, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(List.of(200L), DataType.DATETIME, "field"),
                            new TestCaseSupplier.TypedData(1, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData(new BytesRef("desc"), DataType.KEYWORD, "order").forceLiteral()
                        ),
                        "Top[field=Attribute[channel=0], limit=Attribute[channel=1], order=Attribute[channel=2]]",
                        DataType.DATETIME,
                        equalTo(200L)
                    )
                ),

                // Resolution errors
                new TestCaseSupplier(
                    List.of(DataType.LONG, DataType.INTEGER, DataType.KEYWORD),
                    () -> TestCaseSupplier.TestCase.typeError(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(List.of(5L, 8L, 2L, 0L, 200L), DataType.LONG, "field"),
                            new TestCaseSupplier.TypedData(0, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData(new BytesRef("desc"), DataType.KEYWORD, "order").forceLiteral()
                        ),
                        "Limit must be greater than 0 in [], found [0]"
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.LONG, DataType.INTEGER, DataType.KEYWORD),
                    () -> TestCaseSupplier.TestCase.typeError(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(List.of(5L, 8L, 2L, 0L, 200L), DataType.LONG, "field"),
                            new TestCaseSupplier.TypedData(2, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData(new BytesRef("wrong-order"), DataType.KEYWORD, "order").forceLiteral()
                        ),
                        "Invalid order value in [], expected [ASC, DESC] but got [wrong-order]"
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.LONG, DataType.INTEGER, DataType.KEYWORD),
                    () -> TestCaseSupplier.TestCase.typeError(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(List.of(5L, 8L, 2L, 0L, 200L), DataType.LONG, "field"),
                            new TestCaseSupplier.TypedData(null, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData(new BytesRef("desc"), DataType.KEYWORD, "order").forceLiteral()
                        ),
                        "second argument of [] cannot be null, received [limit]"
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.LONG, DataType.INTEGER, DataType.KEYWORD),
                    () -> TestCaseSupplier.TestCase.typeError(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(List.of(5L, 8L, 2L, 0L, 200L), DataType.LONG, "field"),
                            new TestCaseSupplier.TypedData(1, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData(null, DataType.KEYWORD, "order").forceLiteral()
                        ),
                        "third argument of [] cannot be null, received [order]"
                    )
                )
            )
        );

        return parameterSuppliersFromTypedDataWithDefaultChecks(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Top(source, args.get(0), args.get(1), args.get(2));
    }

    @SuppressWarnings("unchecked")
    private static TestCaseSupplier makeSupplier(
        TestCaseSupplier.TypedDataSupplier fieldSupplier,
        TestCaseSupplier.TypedDataSupplier limitCaseSupplier,
        String order
    ) {
        return new TestCaseSupplier(List.of(fieldSupplier.type(), DataType.INTEGER, DataType.KEYWORD), () -> {
            var fieldTypedData = fieldSupplier.get();
            var limitTypedData = limitCaseSupplier.get().forceLiteral();
            var limit = (int) limitTypedData.getValue();
            var expected = fieldTypedData.multiRowData()
                .stream()
                .map(v -> (Comparable<? super Comparable<?>>) v)
                .sorted(order.equals("asc") ? Comparator.naturalOrder() : Comparator.reverseOrder())
                .limit(limit)
                .toList();

            return new TestCaseSupplier.TestCase(
                List.of(
                    fieldTypedData,
                    limitTypedData,
                    new TestCaseSupplier.TypedData(new BytesRef(order), DataType.KEYWORD, order + " order").forceLiteral()
                ),
                "Top[field=Attribute[channel=0], limit=Attribute[channel=1], order=Attribute[channel=2]]",
                fieldSupplier.type(),
                equalTo(expected.size() == 1 ? expected.get(0) : expected)
            );
        });
    }
}
