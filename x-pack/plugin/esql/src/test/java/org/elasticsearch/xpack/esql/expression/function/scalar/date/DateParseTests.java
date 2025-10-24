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
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
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
        List<TestCaseSupplier> cases = new ArrayList<>();
        cases.add(
            new TestCaseSupplier(
                "Basic Case",
                List.of(DataType.KEYWORD, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BytesRef("yyyy-MM-dd"), DataType.KEYWORD, "first"),
                        new TestCaseSupplier.TypedData(new BytesRef("2023-05-05"), DataType.KEYWORD, "second")
                    ),
                    "DateParseEvaluator[val=Attribute[channel=1], formatter=Attribute[channel=0]]",
                    DataType.DATETIME,
                    equalTo(1683244800000L)
                )
            )
        );
        cases.add(new TestCaseSupplier("Timezoned Case", List.of(DataType.KEYWORD, DataType.KEYWORD), () -> {
            long ts_sec = 1657585450L; // 2022-07-12T00:24:10Z
            int hours = randomIntBetween(0, 23);
            String date = String.format(Locale.ROOT, "12/Jul/2022:%02d:24:10 +0900", hours);
            long expected_ts = (ts_sec + (hours - 9) * 3600L) * 1000L;
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef("dd/MMM/yyyy:HH:mm:ss Z"), DataType.KEYWORD, "first"),
                    new TestCaseSupplier.TypedData(new BytesRef(date), DataType.KEYWORD, "second")
                ),
                "DateParseEvaluator[val=Attribute[channel=1], formatter=Attribute[channel=0]]",
                DataType.DATETIME,
                equalTo(expected_ts)
            );
        }));
        cases.add(
            new TestCaseSupplier(
                "With Text",
                List.of(DataType.KEYWORD, DataType.TEXT),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BytesRef("yyyy-MM-dd"), DataType.KEYWORD, "first"),
                        new TestCaseSupplier.TypedData(new BytesRef("2023-05-05"), DataType.TEXT, "second")
                    ),
                    "DateParseEvaluator[val=Attribute[channel=1], formatter=Attribute[channel=0]]",
                    DataType.DATETIME,
                    equalTo(1683244800000L)
                )
            )
        );
        cases.add(
            new TestCaseSupplier(
                "With Both Text",
                List.of(DataType.TEXT, DataType.TEXT),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BytesRef("yyyy-MM-dd"), DataType.TEXT, "first"),
                        new TestCaseSupplier.TypedData(new BytesRef("2023-05-05"), DataType.TEXT, "second")
                    ),
                    "DateParseEvaluator[val=Attribute[channel=1], formatter=Attribute[channel=0]]",
                    DataType.DATETIME,
                    equalTo(1683244800000L)
                )
            )
        );
        cases.add(
            new TestCaseSupplier(
                "With keyword",
                List.of(DataType.TEXT, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BytesRef("yyyy-MM-dd"), DataType.TEXT, "first"),
                        new TestCaseSupplier.TypedData(new BytesRef("2023-05-05"), DataType.KEYWORD, "second")
                    ),
                    "DateParseEvaluator[val=Attribute[channel=1], formatter=Attribute[channel=0]]",
                    DataType.DATETIME,
                    equalTo(1683244800000L)
                )
            )
        );
        cases.add(
            new TestCaseSupplier(
                List.of(DataType.KEYWORD, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BytesRef("not a format"), DataType.KEYWORD, "first"),
                        new TestCaseSupplier.TypedData(new BytesRef("2023-05-05"), DataType.KEYWORD, "second")

                    ),
                    "DateParseEvaluator[val=Attribute[channel=1], formatter=Attribute[channel=0]]",
                    DataType.DATETIME,
                    is(nullValue())
                ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                    .withWarning("Line 1:1: java.lang.IllegalArgumentException: Invalid format: [not a format]: Unknown pattern letter: o")
                    .withFoldingException(
                        InvalidArgumentException.class,
                        "invalid date pattern for [source]: Invalid format: [not a format]: Unknown pattern letter: o"
                    )
            )
        );
        cases.add(
            new TestCaseSupplier(
                List.of(DataType.KEYWORD, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BytesRef("yyyy-MM-dd"), DataType.KEYWORD, "first"),
                        new TestCaseSupplier.TypedData(new BytesRef("not a date"), DataType.KEYWORD, "second")

                    ),
                    "DateParseEvaluator[val=Attribute[channel=1], formatter=Attribute[channel=0]]",
                    DataType.DATETIME,
                    is(nullValue())
                ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                    .withWarning(
                        "Line 1:1: java.lang.IllegalArgumentException: "
                            + "failed to parse date field [not a date] with format [yyyy-MM-dd]"
                    )
            )
        );
        cases = anyNullIsNull(true, cases);
        cases.add(
            new TestCaseSupplier(
                List.of(DataType.KEYWORD, DataType.KEYWORD, DataType.OBJECT),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BytesRef("yyyy-MM-dd"), DataType.KEYWORD, "pattern"),
                        new TestCaseSupplier.TypedData(new BytesRef("2023-05-05"), DataType.KEYWORD, "date"),
                        new TestCaseSupplier.TypedData(
                            new MapExpression(
                                Source.EMPTY,
                                List.of(
                                    new Literal(Source.EMPTY, new BytesRef("time_zone"), DataType.KEYWORD),
                                    new Literal(Source.EMPTY, new BytesRef("UTC"), DataType.KEYWORD)
                                )
                            ),
                            DataType.OBJECT,
                            "options"
                        ).forceLiteral()
                    ),
                    "DateParseEvaluator[val=Attribute[channel=1], formatter=Attribute[channel=0]]",
                    DataType.DATETIME,
                    equalTo(1683244800000L)
                )
            )
        );
        return parameterSuppliersFromTypedData(randomizeBytesRefsOffset(cases));
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
                    field("str", DataType.KEYWORD),
                    null
                )
            ).get(driverContext)
        );
        assertThat(e.getMessage(), startsWith("invalid date pattern for []: Invalid format: [" + pattern + "]"));
    }

    public void testInvalidLocale() {
        String pattern = "YYYY";
        String locale = "nonexistinglocale";
        DriverContext driverContext = driverContext();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> evaluator(
                new DateParse(
                    Source.EMPTY,
                    new Literal(Source.EMPTY, new BytesRef(pattern), DataType.KEYWORD),
                    field("str", DataType.KEYWORD),
                    new MapExpression(
                        Source.EMPTY,
                        List.of(
                            new Literal(Source.EMPTY, new BytesRef("locale"), DataType.KEYWORD),
                            new Literal(Source.EMPTY, new BytesRef(locale), DataType.KEYWORD)
                        )
                    )
                )
            ).get(driverContext)
        );
        assertThat(e.getMessage(), startsWith("Unknown language: " + locale));
    }

    public void testInvalidTimezone() {
        String pattern = "YYYY";
        String timezone = "NON-EXISTING-TIMEZONE";
        DriverContext driverContext = driverContext();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> evaluator(
                new DateParse(
                    Source.EMPTY,
                    new Literal(Source.EMPTY, new BytesRef(pattern), DataType.KEYWORD),
                    field("str", DataType.KEYWORD),
                    new MapExpression(
                        Source.EMPTY,
                        List.of(
                            new Literal(Source.EMPTY, new BytesRef("time_zone"), DataType.KEYWORD),
                            new Literal(Source.EMPTY, new BytesRef(timezone), DataType.KEYWORD)
                        )
                    )
                )
            ).get(driverContext)
        );
        assertThat(e.getMessage(), startsWith("unsupported timezone [" + timezone + "]"));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new DateParse(source, args.get(0), args.size() > 1 ? args.get(1) : null, args.size() == 3 ? args.get(2) : null);
    }
}
