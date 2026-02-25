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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.anything;

public class SparklineTests extends AbstractAggregationTestCase {
    public SparklineTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var suppliers = new ArrayList<TestCaseSupplier>();

        suppliers.add(
            new TestCaseSupplier(
                "field type error",
                List.of(DataType.KEYWORD, DataType.DATETIME, DataType.INTEGER, DataType.KEYWORD, DataType.KEYWORD),
                () -> TestCaseSupplier.TestCase.typeError(
                    List.of(
                        TestCaseSupplier.TypedData.multiRow(List.of(new BytesRef("a"), new BytesRef("b")), DataType.KEYWORD, "field"),
                        TestCaseSupplier.TypedData.multiRow(List.of(1000L, 2000L), DataType.DATETIME, "key"),
                        new TestCaseSupplier.TypedData(10, DataType.INTEGER, "buckets").forceLiteral(),
                        new TestCaseSupplier.TypedData(new BytesRef("2024-01-01"), DataType.KEYWORD, "from").forceLiteral(),
                        new TestCaseSupplier.TypedData(new BytesRef("2024-12-31"), DataType.KEYWORD, "to").forceLiteral()
                    ),
                    "first argument of [source] must be [long or integer], found value [field] type [keyword]"
                )
            )
        );

        suppliers.add(
            new TestCaseSupplier(
                "key type error",
                List.of(DataType.LONG, DataType.INTEGER, DataType.INTEGER, DataType.KEYWORD, DataType.KEYWORD),
                () -> TestCaseSupplier.TestCase.typeError(
                    List.of(
                        TestCaseSupplier.TypedData.multiRow(List.of(1L, 2L, 3L), DataType.LONG, "field"),
                        TestCaseSupplier.TypedData.multiRow(List.of(1, 2, 3), DataType.INTEGER, "key"),
                        new TestCaseSupplier.TypedData(10, DataType.INTEGER, "buckets").forceLiteral(),
                        new TestCaseSupplier.TypedData(new BytesRef("2024-01-01"), DataType.KEYWORD, "from").forceLiteral(),
                        new TestCaseSupplier.TypedData(new BytesRef("2024-12-31"), DataType.KEYWORD, "to").forceLiteral()
                    ),
                    "second argument of [source] must be [datetime], found value [key] type [integer]"
                )
            )
        );

        suppliers.add(
            new TestCaseSupplier(
                "buckets type error",
                List.of(DataType.LONG, DataType.DATETIME, DataType.KEYWORD, DataType.KEYWORD, DataType.KEYWORD),
                () -> TestCaseSupplier.TestCase.typeError(
                    List.of(
                        TestCaseSupplier.TypedData.multiRow(List.of(1L, 2L, 3L), DataType.LONG, "field"),
                        TestCaseSupplier.TypedData.multiRow(List.of(1000L, 2000L, 3000L), DataType.DATETIME, "key"),
                        new TestCaseSupplier.TypedData(new BytesRef("bad"), DataType.KEYWORD, "buckets").forceLiteral(),
                        new TestCaseSupplier.TypedData(new BytesRef("2024-01-01"), DataType.KEYWORD, "from").forceLiteral(),
                        new TestCaseSupplier.TypedData(new BytesRef("2024-12-31"), DataType.KEYWORD, "to").forceLiteral()
                    ),
                    "third argument of [source] must be [integer], found value [buckets] type [keyword]"
                )
            )
        );

        suppliers.add(
            new TestCaseSupplier(
                "from type error",
                List.of(DataType.LONG, DataType.DATETIME, DataType.INTEGER, DataType.INTEGER, DataType.KEYWORD),
                () -> TestCaseSupplier.TestCase.typeError(
                    List.of(
                        TestCaseSupplier.TypedData.multiRow(List.of(1L, 2L, 3L), DataType.LONG, "field"),
                        TestCaseSupplier.TypedData.multiRow(List.of(1000L, 2000L, 3000L), DataType.DATETIME, "key"),
                        new TestCaseSupplier.TypedData(10, DataType.INTEGER, "buckets").forceLiteral(),
                        new TestCaseSupplier.TypedData(42, DataType.INTEGER, "from").forceLiteral(),
                        new TestCaseSupplier.TypedData(new BytesRef("2024-12-31"), DataType.KEYWORD, "to").forceLiteral()
                    ),
                    "fourth argument of [source] must be [string or datetime], found value [from] type [integer]"
                )
            )
        );

        suppliers.add(
            new TestCaseSupplier(
                "to type error",
                List.of(DataType.LONG, DataType.DATETIME, DataType.INTEGER, DataType.KEYWORD, DataType.INTEGER),
                () -> TestCaseSupplier.TestCase.typeError(
                    List.of(
                        TestCaseSupplier.TypedData.multiRow(List.of(1L, 2L, 3L), DataType.LONG, "field"),
                        TestCaseSupplier.TypedData.multiRow(List.of(1000L, 2000L, 3000L), DataType.DATETIME, "key"),
                        new TestCaseSupplier.TypedData(10, DataType.INTEGER, "buckets").forceLiteral(),
                        new TestCaseSupplier.TypedData(new BytesRef("2024-01-01"), DataType.KEYWORD, "from").forceLiteral(),
                        new TestCaseSupplier.TypedData(42, DataType.INTEGER, "to").forceLiteral()
                    ),
                    "fifth argument of [source] must be [string or datetime], found value [to] type [integer]"
                )
            )
        );

        suppliers.add(
            new TestCaseSupplier(
                "integer field with keyword range",
                List.of(DataType.INTEGER, DataType.DATETIME, DataType.INTEGER, DataType.KEYWORD, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        TestCaseSupplier.TypedData.multiRow(List.of(1, 2, 3), DataType.INTEGER, "field"),
                        TestCaseSupplier.TypedData.multiRow(List.of(1000L, 2000L, 3000L), DataType.DATETIME, "key"),
                        new TestCaseSupplier.TypedData(10, DataType.INTEGER, "buckets").forceLiteral(),
                        new TestCaseSupplier.TypedData(new BytesRef("2024-01-01"), DataType.KEYWORD, "from").forceLiteral(),
                        new TestCaseSupplier.TypedData(new BytesRef("2024-12-31"), DataType.KEYWORD, "to").forceLiteral()
                    ),
                    "Sparkline",
                    DataType.INTEGER,
                    anything()
                )
            )
        );

        suppliers.add(
            new TestCaseSupplier(
                "long field with text range",
                List.of(DataType.LONG, DataType.DATETIME, DataType.INTEGER, DataType.TEXT, DataType.TEXT),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        TestCaseSupplier.TypedData.multiRow(List.of(1L, 2L, 3L), DataType.LONG, "field"),
                        TestCaseSupplier.TypedData.multiRow(List.of(1000L, 2000L, 3000L), DataType.DATETIME, "key"),
                        new TestCaseSupplier.TypedData(10, DataType.INTEGER, "buckets").forceLiteral(),
                        new TestCaseSupplier.TypedData(new BytesRef("2024-01-01"), DataType.TEXT, "from").forceLiteral(),
                        new TestCaseSupplier.TypedData(new BytesRef("2024-12-31"), DataType.TEXT, "to").forceLiteral()
                    ),
                    "Sparkline",
                    DataType.LONG,
                    anything()
                )
            )
        );

        suppliers.add(
            new TestCaseSupplier(
                "integer field with date range",
                List.of(DataType.INTEGER, DataType.DATETIME, DataType.INTEGER, DataType.DATETIME, DataType.DATETIME),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        TestCaseSupplier.TypedData.multiRow(List.of(1, 2, 3), DataType.INTEGER, "field"),
                        TestCaseSupplier.TypedData.multiRow(List.of(1000L, 2000L, 3000L), DataType.DATETIME, "key"),
                        new TestCaseSupplier.TypedData(10, DataType.INTEGER, "buckets").forceLiteral(),
                        new TestCaseSupplier.TypedData(1704067200000L, DataType.DATETIME, "from").forceLiteral(),
                        new TestCaseSupplier.TypedData(1735689600000L, DataType.DATETIME, "to").forceLiteral()
                    ),
                    "Sparkline",
                    DataType.INTEGER,
                    anything()
                )
            )
        );

        suppliers.add(
            new TestCaseSupplier(
                "double field with date range",
                List.of(DataType.DOUBLE, DataType.DATETIME, DataType.INTEGER, DataType.DATETIME, DataType.DATETIME),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        TestCaseSupplier.TypedData.multiRow(List.of(1.0, 2.0, 3.0), DataType.DOUBLE, "field"),
                        TestCaseSupplier.TypedData.multiRow(List.of(1000L, 2000L, 3000L), DataType.DATETIME, "key"),
                        new TestCaseSupplier.TypedData(10, DataType.INTEGER, "buckets").forceLiteral(),
                        new TestCaseSupplier.TypedData(1704067200000L, DataType.DATETIME, "from").forceLiteral(),
                        new TestCaseSupplier.TypedData(1735689600000L, DataType.DATETIME, "to").forceLiteral()
                    ),
                    "Sparkline",
                    DataType.DOUBLE,
                    anything()
                )
            )
        );

        suppliers.add(
            new TestCaseSupplier(
                "float field with date range",
                List.of(DataType.FLOAT, DataType.DATETIME, DataType.INTEGER, DataType.DATETIME, DataType.DATETIME),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        TestCaseSupplier.TypedData.multiRow(List.of(1.0f, 2.0f, 3.0f), DataType.FLOAT, "field"),
                        TestCaseSupplier.TypedData.multiRow(List.of(1000L, 2000L, 3000L), DataType.DATETIME, "key"),
                        new TestCaseSupplier.TypedData(10, DataType.INTEGER, "buckets").forceLiteral(),
                        new TestCaseSupplier.TypedData(1704067200000L, DataType.DATETIME, "from").forceLiteral(),
                        new TestCaseSupplier.TypedData(1735689600000L, DataType.DATETIME, "to").forceLiteral()
                    ),
                    "Sparkline",
                    DataType.FLOAT,
                    anything()
                )
            )
        );

        return parameterSuppliersFromTypedDataWithDefaultChecks(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Sparkline(source, args.get(0), args.get(1), args.get(2), args.get(3), args.get(4));
    }

    @Override
    public void testAggregate() {
        assumeTrue("Sparkline does not implement ToAggregator", testCase.getExpectedTypeError() != null);
        super.testAggregate();
    }

    @Override
    public void testAggregateToString() {
        assumeTrue("Sparkline does not implement ToAggregator", testCase.getExpectedTypeError() != null);
        super.testAggregateToString();
    }

    @Override
    public void testGroupingAggregate() {
        assumeTrue("Sparkline does not implement ToAggregator", testCase.getExpectedTypeError() != null);
        super.testGroupingAggregate();
    }

    @Override
    public void testGroupingAggregateToString() {
        assumeTrue("Sparkline does not implement ToAggregator", testCase.getExpectedTypeError() != null);
        super.testGroupingAggregateToString();
    }

    @Override
    public void testAggregateIntermediate() {
        assumeTrue("Sparkline does not implement ToAggregator", testCase.getExpectedTypeError() != null);
        super.testAggregateIntermediate();
    }

    @Override
    public void testFold() {
        assumeTrue("Sparkline does not implement ToAggregator", testCase.getExpectedTypeError() != null);
        super.testFold();
    }
}
