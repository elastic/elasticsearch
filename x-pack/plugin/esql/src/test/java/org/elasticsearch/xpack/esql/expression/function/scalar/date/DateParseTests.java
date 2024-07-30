/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class DateParseTests extends AbstractScalarFunctionTestCase {
    public DateParseTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedDataWithDefaultChecks(
            true,
            List.of(
                new TestCaseSupplier(
                    "Basic Case",
                    List.of(DataType.KEYWORD, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(new BytesRef("yyyy-MM-dd"), DataType.KEYWORD, "first"),
                            new TestCaseSupplier.TypedData(new BytesRef("2023-05-05"), DataType.KEYWORD, "second")
                        ),
                        "DateParseEvaluator[val=Attribute[channel=1], formatter=Attribute[channel=0], zoneId=Z]",
                        DataType.DATETIME,
                        equalTo(1683244800000L)
                    )
                ),
                new TestCaseSupplier(
                    "With Text",
                    List.of(DataType.KEYWORD, DataType.TEXT),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(new BytesRef("yyyy-MM-dd"), DataType.KEYWORD, "first"),
                            new TestCaseSupplier.TypedData(new BytesRef("2023-05-05"), DataType.TEXT, "second")
                        ),
                        "DateParseEvaluator[val=Attribute[channel=1], formatter=Attribute[channel=0], zoneId=Z]",
                        DataType.DATETIME,
                        equalTo(1683244800000L)
                    )
                ),
                new TestCaseSupplier(
                    "With Both Text",
                    List.of(DataType.TEXT, DataType.TEXT),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(new BytesRef("yyyy-MM-dd"), DataType.TEXT, "first"),
                            new TestCaseSupplier.TypedData(new BytesRef("2023-05-05"), DataType.TEXT, "second")
                        ),
                        "DateParseEvaluator[val=Attribute[channel=1], formatter=Attribute[channel=0], zoneId=Z]",
                        DataType.DATETIME,
                        equalTo(1683244800000L)
                    )
                ),
                new TestCaseSupplier(
                    "With keyword",
                    List.of(DataType.TEXT, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(new BytesRef("yyyy-MM-dd"), DataType.TEXT, "first"),
                            new TestCaseSupplier.TypedData(new BytesRef("2023-05-05"), DataType.KEYWORD, "second")
                        ),
                        "DateParseEvaluator[val=Attribute[channel=1], formatter=Attribute[channel=0], zoneId=Z]",
                        DataType.DATETIME,
                        equalTo(1683244800000L)
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.KEYWORD, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(new BytesRef("not a format"), DataType.KEYWORD, "first"),
                            new TestCaseSupplier.TypedData(new BytesRef("2023-05-05"), DataType.KEYWORD, "second")

                        ),
                        "DateParseEvaluator[val=Attribute[channel=1], formatter=Attribute[channel=0], zoneId=Z]",
                        DataType.DATETIME,
                        is(nullValue())
                    ).withWarning("Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.")
                        .withWarning(
                            "Line -1:-1: java.lang.IllegalArgumentException: Invalid format: " + "[not a format]: Unknown pattern letter: o"
                        )
                        .withFoldingException(
                            InvalidArgumentException.class,
                            "invalid date pattern for []: Invalid format: [not a format]: Unknown pattern letter: o"
                        )
                ),
                new TestCaseSupplier(
                    List.of(DataType.KEYWORD, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(new BytesRef("yyyy-MM-dd"), DataType.KEYWORD, "first"),
                            new TestCaseSupplier.TypedData(new BytesRef("not a date"), DataType.KEYWORD, "second")

                        ),
                        "DateParseEvaluator[val=Attribute[channel=1], formatter=Attribute[channel=0], zoneId=Z]",
                        DataType.DATETIME,
                        is(nullValue())
                    ).withWarning("Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.")
                        .withWarning(
                            "Line -1:-1: java.lang.IllegalArgumentException: "
                                + "failed to parse date field [not a date] with format [yyyy-MM-dd]"
                        )
                )
            ),
            (v, p) -> "string"
        );
    }

    public void testInvalidPattern() {
        String pattern = "invalid";
        DriverContext driverContext = driverContext();
        InvalidArgumentException e = expectThrows(
            InvalidArgumentException.class,
            () -> evaluator(
                new DateParse(
                    Source.EMPTY,
                    new Literal(Source.EMPTY, new BytesRef(pattern), DataType.KEYWORD),
                    field("str", DataType.KEYWORD)
                )
            ).get(driverContext)
        );
        assertThat(e.getMessage(), startsWith("invalid date pattern for []: Invalid format: [" + pattern + "]"));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new DateParse(source, args.get(0), args.size() > 1 ? args.get(1) : null);
    }
}
