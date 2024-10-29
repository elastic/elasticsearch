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
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractConfigurationFunctionTestCase;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class DateExtractTests extends AbstractConfigurationFunctionTestCase {
    public DateExtractTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedDataWithDefaultChecks(
            true,
            List.of(
                new TestCaseSupplier(
                    List.of(DataType.KEYWORD, DataType.DATETIME),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(new BytesRef("YeAr"), DataType.KEYWORD, "chrono"),
                            new TestCaseSupplier.TypedData(1687944333000L, DataType.DATETIME, "date")
                        ),
                        "DateExtractEvaluator[value=Attribute[channel=1], chronoField=Attribute[channel=0], zone=Z]",
                        DataType.LONG,
                        equalTo(2023L)
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.TEXT, DataType.DATETIME),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(new BytesRef("YeAr"), DataType.TEXT, "chrono"),
                            new TestCaseSupplier.TypedData(1687944333000L, DataType.DATETIME, "date")
                        ),
                        "DateExtractEvaluator[value=Attribute[channel=1], chronoField=Attribute[channel=0], zone=Z]",
                        DataType.LONG,
                        equalTo(2023L)
                    )
                ),
                new TestCaseSupplier(
                    List.of(DataType.KEYWORD, DataType.DATETIME),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(new BytesRef("not a unit"), DataType.KEYWORD, "chrono"),
                            new TestCaseSupplier.TypedData(0L, DataType.DATETIME, "date")

                        ),
                        "DateExtractEvaluator[value=Attribute[channel=1], chronoField=Attribute[channel=0], zone=Z]",
                        DataType.LONG,
                        is(nullValue())
                    ).withWarning("Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.")
                        .withWarning(
                            "Line -1:-1: java.lang.IllegalArgumentException: "
                                + "No enum constant java.time.temporal.ChronoField.NOT A UNIT"
                        )
                        .withFoldingException(InvalidArgumentException.class, "invalid date field for []: not a unit")
                )
            ),
            (v, p) -> switch (p) {
                case 0 -> "string";
                case 1 -> "datetime";
                default -> "";
            }
        );
    }

    public void testAllChronoFields() {
        long epochMilli = 1687944333123L;
        ZonedDateTime date = Instant.ofEpochMilli(epochMilli).atZone(EsqlTestUtils.TEST_CFG.zoneId());
        for (ChronoField value : ChronoField.values()) {
            DateExtract instance = new DateExtract(
                Source.EMPTY,
                new Literal(Source.EMPTY, new BytesRef(value.name()), DataType.KEYWORD),
                new Literal(Source.EMPTY, epochMilli, DataType.DATETIME),
                EsqlTestUtils.TEST_CFG
            );

            assertThat(instance.fold(), is(date.getLong(value)));
            assertThat(
                DateExtract.process(epochMilli, new BytesRef(value.name()), EsqlTestUtils.TEST_CFG.zoneId()),
                is(date.getLong(value))
            );
        }
    }

    public void testInvalidChrono() {
        String chrono = randomAlphaOfLength(10);
        DriverContext driverContext = driverContext();
        InvalidArgumentException e = expectThrows(
            InvalidArgumentException.class,
            () -> evaluator(
                new DateExtract(
                    Source.EMPTY,
                    new Literal(Source.EMPTY, new BytesRef(chrono), DataType.KEYWORD),
                    field("str", DataType.DATETIME),
                    null
                )
            ).get(driverContext)
        );
        assertThat(e.getMessage(), equalTo("invalid date field for []: " + chrono));
    }

    @Override
    protected Expression buildWithConfiguration(Source source, List<Expression> args, Configuration configuration) {
        return new DateExtract(source, args.get(0), args.get(1), configuration);
    }
}
