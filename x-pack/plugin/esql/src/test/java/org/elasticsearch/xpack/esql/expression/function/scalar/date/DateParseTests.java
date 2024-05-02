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
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.ql.InvalidArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class DateParseTests extends AbstractFunctionTestCase {
    public DateParseTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(
            errorsForCasesWithoutExamples(
                anyNullIsNull(
                    true,
                    List.of(
                        new TestCaseSupplier(
                            "Basic Case",
                            List.of(DataTypes.KEYWORD, DataTypes.KEYWORD),
                            () -> new TestCaseSupplier.TestCase(
                                List.of(
                                    new TestCaseSupplier.TypedData(new BytesRef("yyyy-MM-dd"), DataTypes.KEYWORD, "first"),
                                    new TestCaseSupplier.TypedData(new BytesRef("2023-05-05"), DataTypes.KEYWORD, "second")
                                ),
                                "DateParseEvaluator[val=Attribute[channel=1], formatter=Attribute[channel=0], zoneId=Z]",
                                DataTypes.DATETIME,
                                equalTo(1683244800000L)
                            )
                        ),
                        new TestCaseSupplier(
                            "With Text",
                            List.of(DataTypes.KEYWORD, DataTypes.TEXT),
                            () -> new TestCaseSupplier.TestCase(
                                List.of(
                                    new TestCaseSupplier.TypedData(new BytesRef("yyyy-MM-dd"), DataTypes.KEYWORD, "first"),
                                    new TestCaseSupplier.TypedData(new BytesRef("2023-05-05"), DataTypes.TEXT, "second")
                                ),
                                "DateParseEvaluator[val=Attribute[channel=1], formatter=Attribute[channel=0], zoneId=Z]",
                                DataTypes.DATETIME,
                                equalTo(1683244800000L)
                            )
                        ),
                        new TestCaseSupplier(
                            "With Both Text",
                            List.of(DataTypes.TEXT, DataTypes.TEXT),
                            () -> new TestCaseSupplier.TestCase(
                                List.of(
                                    new TestCaseSupplier.TypedData(new BytesRef("yyyy-MM-dd"), DataTypes.TEXT, "first"),
                                    new TestCaseSupplier.TypedData(new BytesRef("2023-05-05"), DataTypes.TEXT, "second")
                                ),
                                "DateParseEvaluator[val=Attribute[channel=1], formatter=Attribute[channel=0], zoneId=Z]",
                                DataTypes.DATETIME,
                                equalTo(1683244800000L)
                            )
                        ),
                        new TestCaseSupplier(
                            "With keyword",
                            List.of(DataTypes.TEXT, DataTypes.KEYWORD),
                            () -> new TestCaseSupplier.TestCase(
                                List.of(
                                    new TestCaseSupplier.TypedData(new BytesRef("yyyy-MM-dd"), DataTypes.TEXT, "first"),
                                    new TestCaseSupplier.TypedData(new BytesRef("2023-05-05"), DataTypes.KEYWORD, "second")
                                ),
                                "DateParseEvaluator[val=Attribute[channel=1], formatter=Attribute[channel=0], zoneId=Z]",
                                DataTypes.DATETIME,
                                equalTo(1683244800000L)
                            )
                        ),
                        new TestCaseSupplier(
                            List.of(DataTypes.KEYWORD, DataTypes.KEYWORD),
                            () -> new TestCaseSupplier.TestCase(
                                List.of(
                                    new TestCaseSupplier.TypedData(new BytesRef("not a format"), DataTypes.KEYWORD, "first"),
                                    new TestCaseSupplier.TypedData(new BytesRef("2023-05-05"), DataTypes.KEYWORD, "second")

                                ),
                                "DateParseEvaluator[val=Attribute[channel=1], formatter=Attribute[channel=0], zoneId=Z]",
                                DataTypes.DATETIME,
                                is(nullValue())
                            ).withWarning("Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.")
                                .withWarning(
                                    "Line -1:-1: java.lang.IllegalArgumentException: Invalid format: "
                                        + "[not a format]: Unknown pattern letter: o"
                                )
                                .withFoldingException(
                                    InvalidArgumentException.class,
                                    "invalid date pattern for []: Invalid format: [not a format]: Unknown pattern letter: o"
                                )
                        ),
                        new TestCaseSupplier(
                            List.of(DataTypes.KEYWORD, DataTypes.KEYWORD),
                            () -> new TestCaseSupplier.TestCase(
                                List.of(
                                    new TestCaseSupplier.TypedData(new BytesRef("yyyy-MM-dd"), DataTypes.KEYWORD, "first"),
                                    new TestCaseSupplier.TypedData(new BytesRef("not a date"), DataTypes.KEYWORD, "second")

                                ),
                                "DateParseEvaluator[val=Attribute[channel=1], formatter=Attribute[channel=0], zoneId=Z]",
                                DataTypes.DATETIME,
                                is(nullValue())
                            ).withWarning("Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.")
                                .withWarning(
                                    "Line -1:-1: java.lang.IllegalArgumentException: "
                                        + "failed to parse date field [not a date] with format [yyyy-MM-dd]"
                                )
                        )
                    )
                )
            )
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
                    new Literal(Source.EMPTY, new BytesRef(pattern), DataTypes.KEYWORD),
                    field("str", DataTypes.KEYWORD)
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
