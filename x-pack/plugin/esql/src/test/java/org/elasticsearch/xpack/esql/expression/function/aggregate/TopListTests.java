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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractAggregationTestCase;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;
import org.junit.Ignore;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class TopListTests extends AbstractAggregationTestCase {
    public TopListTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var suppliers = List.of(
            new TestCaseSupplier(
                List.of(DataType.INTEGER, DataType.INTEGER, DataType.KEYWORD),
                () -> {
                    var limit = randomIntBetween(1, 3);
                    return new TestCaseSupplier.TestCase(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(Stream.of(5, 8, -2, 0, 200), DataType.INTEGER, "field"),
                            new TestCaseSupplier.TypedData(limit, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData(new BytesRef("desc"), DataType.KEYWORD, "order").forceLiteral()
                        ),
                        "TopList[field=Attribute[channel=0], limit=Attribute[channel=1], order=Attribute[channel=2]]",
                        DataType.INTEGER,
                        equalTo(List.of(200, 8, 5).subList(0, limit))
                    );
                }
            ),
            new TestCaseSupplier(
                List.of(DataType.LONG, DataType.INTEGER, DataType.KEYWORD),
                () -> {
                    var limit = randomIntBetween(1, 3);
                    return new TestCaseSupplier.TestCase(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(Stream.of(5L, 8L, -2L, 0L, 200L), DataType.LONG, "field"),
                            new TestCaseSupplier.TypedData(limit, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData(new BytesRef("desc"), DataType.KEYWORD, "order").forceLiteral()
                        ),
                        "TopList[field=Attribute[channel=0], limit=Attribute[channel=1], order=Attribute[channel=2]]",
                        DataType.LONG,
                        equalTo(List.of(200L, 8L, 5L).subList(0, limit))
                    );
                }
            ),
            new TestCaseSupplier(
                List.of(DataType.DOUBLE, DataType.INTEGER, DataType.KEYWORD),
                () -> {
                    var limit = randomIntBetween(1, 3);
                    return new TestCaseSupplier.TestCase(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(Stream.of(5., 8., -2., 0., 200.), DataType.DOUBLE, "field"),
                            new TestCaseSupplier.TypedData(limit, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData(new BytesRef("desc"), DataType.KEYWORD, "order").forceLiteral()
                        ),
                        "TopList[field=Attribute[channel=0], limit=Attribute[channel=1], order=Attribute[channel=2]]",
                        DataType.DOUBLE,
                        equalTo(List.of(200., 8., 5.).subList(0, limit))
                    );
                }
            ),
            new TestCaseSupplier(
                List.of(DataType.DATETIME, DataType.INTEGER, DataType.KEYWORD),
                () -> {
                    var limit = randomIntBetween(1, 3);
                    return new TestCaseSupplier.TestCase(
                        List.of(
                            TestCaseSupplier.TypedData.multiRow(Stream.of(5L, 8L, 2L, 0L, 200L), DataType.DATETIME, "field"),
                            new TestCaseSupplier.TypedData(limit, DataType.INTEGER, "limit").forceLiteral(),
                            new TestCaseSupplier.TypedData(new BytesRef("desc"), DataType.KEYWORD, "order").forceLiteral()
                        ),
                        "TopList[field=Attribute[channel=0], limit=Attribute[channel=1], order=Attribute[channel=2]]",
                        DataType.DATETIME,
                        equalTo(List.of(200L, 8L, 5L).subList(0, limit))
                    );
                }
            ),

            // Resolution errors
            new TestCaseSupplier(
                List.of(DataType.LONG, DataType.INTEGER, DataType.KEYWORD),
                () -> TestCaseSupplier.TestCase.typeError(
                    List.of(
                        TestCaseSupplier.TypedData.multiRow(Stream.of(5L, 8L, 2L, 0L, 200L), DataType.LONG, "field"),
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
                        TestCaseSupplier.TypedData.multiRow(Stream.of(5L, 8L, 2L, 0L, 200L), DataType.LONG, "field"),
                        new TestCaseSupplier.TypedData(2, DataType.INTEGER, "limit").forceLiteral(),
                        new TestCaseSupplier.TypedData(new BytesRef("wrong-order"), DataType.KEYWORD, "order").forceLiteral()
                    ),
                    "Invalid order value in [], expected [ASC, DESC] but got [wrong-order]"
                )
            )
        );

        return parameterSuppliersFromTypedData(suppliers);
        //return parameterSuppliersFromTypedData(errorsForCasesWithoutExamples(anyNullIsNull(true, suppliers)));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new TopList(source, args.get(0), args.get(1), args.get(2));
    }
}
