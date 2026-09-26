/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.math.BigInteger;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class ToHumanTests extends AbstractScalarFunctionTestCase {
    public ToHumanTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedDataWithDefaultChecks(
            true,
            List.of(
                new TestCaseSupplier(
                    "bytes with automatic unit",
                    List.of(DataType.LONG, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(1500L, DataType.LONG, "field"),
                            keywordLiteral("unit", "bytes")
                        ),
                        "ToHumanLongEvaluator[field=Attribute[channel=0]]",
                        DataType.KEYWORD,
                        equalTo(new BytesRef("1.46 KB"))
                    )
                ),
                new TestCaseSupplier(
                    "bytes with target unit",
                    List.of(DataType.LONG, DataType.KEYWORD, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(3L * 1024 * 1024 * 1024, DataType.LONG, "field"),
                            keywordLiteral("unit", "bytes"),
                            keywordLiteral("target_unit", "GB")
                        ),
                        "ToHumanLongEvaluator[field=Attribute[channel=0]]",
                        DataType.KEYWORD,
                        equalTo(new BytesRef("3 GB"))
                    )
                ),
                new TestCaseSupplier(
                    "bytes with target unit B",
                    List.of(DataType.LONG, DataType.KEYWORD, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(512L, DataType.LONG, "field"),
                            keywordLiteral("unit", "bytes"),
                            keywordLiteral("target_unit", "B")
                        ),
                        "ToHumanLongEvaluator[field=Attribute[channel=0]]",
                        DataType.KEYWORD,
                        equalTo(new BytesRef("512 B"))
                    )
                ),
                new TestCaseSupplier(
                    "negative bytes with automatic unit",
                    List.of(DataType.LONG, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(-2048L, DataType.LONG, "field"),
                            keywordLiteral("unit", "bytes")
                        ),
                        "ToHumanLongEvaluator[field=Attribute[channel=0]]",
                        DataType.KEYWORD,
                        equalTo(new BytesRef("-2 KB"))
                    )
                ),
                new TestCaseSupplier(
                    "duration in nanos with automatic unit",
                    List.of(DataType.INTEGER, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(1500000000, DataType.INTEGER, "field"),
                            keywordLiteral("unit", "duration")
                        ),
                        "ToHumanIntEvaluator[field=Attribute[channel=0]]",
                        DataType.KEYWORD,
                        equalTo(new BytesRef("1.5 s"))
                    )
                ),
                new TestCaseSupplier(
                    "duration in nanos pinned to milliseconds",
                    List.of(DataType.LONG, DataType.KEYWORD, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(1500000L, DataType.LONG, "field"),
                            keywordLiteral("unit", "duration"),
                            keywordLiteral("target_unit", "ms")
                        ),
                        "ToHumanLongEvaluator[field=Attribute[channel=0]]",
                        DataType.KEYWORD,
                        equalTo(new BytesRef("1.5 ms"))
                    )
                ),
                new TestCaseSupplier(
                    "small duration in nanos",
                    List.of(DataType.LONG, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(500L, DataType.LONG, "field"),
                            keywordLiteral("unit", "duration")
                        ),
                        "ToHumanLongEvaluator[field=Attribute[channel=0]]",
                        DataType.KEYWORD,
                        equalTo(new BytesRef("500 ns"))
                    )
                ),
                new TestCaseSupplier(
                    "bits with automatic unit",
                    List.of(DataType.DOUBLE, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(2500000.0, DataType.DOUBLE, "field"),
                            keywordLiteral("unit", "bits")
                        ),
                        "ToHumanDoubleEvaluator[field=Attribute[channel=0]]",
                        DataType.KEYWORD,
                        equalTo(new BytesRef("2.5 Mbit"))
                    )
                ),
                new TestCaseSupplier(
                    "bits with target unit",
                    List.of(DataType.LONG, DataType.KEYWORD, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(1500L, DataType.LONG, "field"),
                            keywordLiteral("unit", "bits"),
                            keywordLiteral("target_unit", "kbit")
                        ),
                        "ToHumanLongEvaluator[field=Attribute[channel=0]]",
                        DataType.KEYWORD,
                        equalTo(new BytesRef("1.5 kbit"))
                    )
                ),
                new TestCaseSupplier(
                    "percent renders a ratio",
                    List.of(DataType.DOUBLE, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(0.425, DataType.DOUBLE, "field"),
                            keywordLiteral("unit", "percent")
                        ),
                        "ToHumanDoubleEvaluator[field=Attribute[channel=0]]",
                        DataType.KEYWORD,
                        equalTo(new BytesRef("42.5%"))
                    )
                ),
                new TestCaseSupplier(
                    "unsigned long is humanized as double",
                    List.of(DataType.UNSIGNED_LONG, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(BigInteger.valueOf(2048), DataType.UNSIGNED_LONG, "field"),
                            keywordLiteral("unit", "bytes")
                        ),
                        "ToHumanDoubleEvaluator[field=ToDoubleFromUnsignedLongEvaluator[l=Attribute[channel=0]]]",
                        DataType.KEYWORD,
                        equalTo(new BytesRef("2 KB"))
                    )
                )
            )
        );
    }

    private static TestCaseSupplier.TypedData keywordLiteral(String name, String value) {
        return new TestCaseSupplier.TypedData(new BytesRef(value), DataType.KEYWORD, name).forceLiteral();
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToHuman(source, args.get(0), args.get(1), args.size() < 3 ? null : args.get(2));
    }
}
