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
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class TopListTests extends AbstractAggregationTestCase {
    public TopListTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var suppliers = List.of(
            // All types
            new TestCaseSupplier(List.of(DataType.INTEGER, DataType.INTEGER, DataType.KEYWORD), () -> {
                var limit = randomIntBetween(2, 4);
                return new TestCaseSupplier.TestCase(
                    List.of(
                        TestCaseSupplier.TypedData.multiRow(List.of(5, 8, -2, 0, 200), DataType.INTEGER, "field"),
                        new TestCaseSupplier.TypedData(limit, DataType.INTEGER, "limit").forceLiteral(),
                        new TestCaseSupplier.TypedData(new BytesRef("desc"), DataType.KEYWORD, "order").forceLiteral()
                    ),
                    "TopList[field=Attribute[channel=0], limit=Attribute[channel=1], order=Attribute[channel=2]]",
                    DataType.INTEGER,
                    equalTo(List.of(200, 8, 5, 0).subList(0, limit))
                );
            }),
            new TestCaseSupplier(List.of(DataType.LONG, DataType.INTEGER, DataType.KEYWORD), () -> {
                var limit = randomIntBetween(2, 4);
                return new TestCaseSupplier.TestCase(
                    List.of(
                        TestCaseSupplier.TypedData.multiRow(List.of(5L, 8L, -2L, 0L, 200L), DataType.LONG, "field"),
                        new TestCaseSupplier.TypedData(limit, DataType.INTEGER, "limit").forceLiteral(),
                        new TestCaseSupplier.TypedData(new BytesRef("desc"), DataType.KEYWORD, "order").forceLiteral()
                    ),
                    "TopList[field=Attribute[channel=0], limit=Attribute[channel=1], order=Attribute[channel=2]]",
                    DataType.LONG,
                    equalTo(List.of(200L, 8L, 5L, 0L).subList(0, limit))
                );
            }),
            new TestCaseSupplier(List.of(DataType.DOUBLE, DataType.INTEGER, DataType.KEYWORD), () -> {
                var limit = randomIntBetween(2, 4);
                return new TestCaseSupplier.TestCase(
                    List.of(
                        TestCaseSupplier.TypedData.multiRow(List.of(5., 8., -2., 0., 200.), DataType.DOUBLE, "field"),
                        new TestCaseSupplier.TypedData(limit, DataType.INTEGER, "limit").forceLiteral(),
                        new TestCaseSupplier.TypedData(new BytesRef("desc"), DataType.KEYWORD, "order").forceLiteral()
                    ),
                    "TopList[field=Attribute[channel=0], limit=Attribute[channel=1], order=Attribute[channel=2]]",
                    DataType.DOUBLE,
                    equalTo(List.of(200., 8., 5., 0.).subList(0, limit))
                );
            }),
            new TestCaseSupplier(List.of(DataType.DATETIME, DataType.INTEGER, DataType.KEYWORD), () -> {
                var limit = randomIntBetween(2, 4);
                return new TestCaseSupplier.TestCase(
                    List.of(
                        TestCaseSupplier.TypedData.multiRow(List.of(5L, 8L, -2L, 0L, 200L), DataType.DATETIME, "field"),
                        new TestCaseSupplier.TypedData(limit, DataType.INTEGER, "limit").forceLiteral(),
                        new TestCaseSupplier.TypedData(new BytesRef("desc"), DataType.KEYWORD, "order").forceLiteral()
                    ),
                    "TopList[field=Attribute[channel=0], limit=Attribute[channel=1], order=Attribute[channel=2]]",
                    DataType.DATETIME,
                    equalTo(List.of(200L, 8L, 5L, 0L).subList(0, limit))
                );
            }),

            // Surrogates
            new TestCaseSupplier(
                List.of(DataType.INTEGER, DataType.INTEGER, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        TestCaseSupplier.TypedData.multiRow(List.of(5, 8, -2, 0, 200), DataType.INTEGER, "field"),
                        new TestCaseSupplier.TypedData(1, DataType.INTEGER, "limit").forceLiteral(),
                        new TestCaseSupplier.TypedData(new BytesRef("desc"), DataType.KEYWORD, "order").forceLiteral()
                    ),
                    "TopList[field=Attribute[channel=0], limit=Attribute[channel=1], order=Attribute[channel=2]]",
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
                    "TopList[field=Attribute[channel=0], limit=Attribute[channel=1], order=Attribute[channel=2]]",
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
                    "TopList[field=Attribute[channel=0], limit=Attribute[channel=1], order=Attribute[channel=2]]",
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
                    "TopList[field=Attribute[channel=0], limit=Attribute[channel=1], order=Attribute[channel=2]]",
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
                    "TopList[field=Attribute[channel=0], limit=Attribute[channel=1], order=Attribute[channel=2]]",
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
                    "TopList[field=Attribute[channel=0], limit=Attribute[channel=1], order=Attribute[channel=2]]",
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
                    "TopList[field=Attribute[channel=0], limit=Attribute[channel=1], order=Attribute[channel=2]]",
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
                    "TopList[field=Attribute[channel=0], limit=Attribute[channel=1], order=Attribute[channel=2]]",
                    DataType.DATETIME,
                    equalTo(200L)
                )
            ),

            // Validation errors
            new TestCaseSupplier(
                List.of(DataType.LONG, DataType.INTEGER, DataType.KEYWORD),
                () -> TestCaseSupplier.TestCase.validationFailure(
                    List.of(
                        TestCaseSupplier.TypedData.multiRow(List.of(5L, 8L, 2L, 0L, 200L), DataType.LONG, "field"),
                        new TestCaseSupplier.TypedData(0, DataType.INTEGER, "limit").forceLiteral(),
                        new TestCaseSupplier.TypedData(new BytesRef("desc"), DataType.KEYWORD, "order").forceLiteral()
                    ),
                    "second argument of [] must be greater than 0, received [limit]"
                )
            ),
            new TestCaseSupplier(
                List.of(DataType.LONG, DataType.INTEGER, DataType.KEYWORD),
                () -> TestCaseSupplier.TestCase.validationFailure(
                    List.of(
                        TestCaseSupplier.TypedData.multiRow(List.of(5L, 8L, 2L, 0L, 200L), DataType.LONG, "field"),
                        new TestCaseSupplier.TypedData(2, DataType.INTEGER, "limit").forceLiteral(),
                        new TestCaseSupplier.TypedData(new BytesRef("wrong-order"), DataType.KEYWORD, "order").forceLiteral()
                    ),
                    "third argument of [] must be either 'ASC' or 'DESC', received [order]"
                )
            ),
            new TestCaseSupplier(
                List.of(DataType.LONG, DataType.INTEGER, DataType.KEYWORD),
                () -> TestCaseSupplier.TestCase.validationFailure(
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
                () -> TestCaseSupplier.TestCase.validationFailure(
                    List.of(
                        TestCaseSupplier.TypedData.multiRow(List.of(5L, 8L, 2L, 0L, 200L), DataType.LONG, "field"),
                        new TestCaseSupplier.TypedData(1, DataType.INTEGER, "limit").forceLiteral(),
                        new TestCaseSupplier.TypedData(null, DataType.KEYWORD, "order").forceLiteral()
                    ),
                    "third argument of [] cannot be null, received [order]"
                )
            )
        );

        return parameterSuppliersFromTypedDataWithDefaultChecks(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new TopList(source, args.get(0), args.get(1), args.get(2));
    }
}
