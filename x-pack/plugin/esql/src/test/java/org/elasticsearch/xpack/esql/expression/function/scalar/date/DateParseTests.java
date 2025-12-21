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
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractConfigurationFunctionTestCase;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

import static org.elasticsearch.test.ReadableMatchers.matchesDateMillis;
import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomConfiguration;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.TEST_SOURCE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class DateParseTests extends AbstractConfigurationFunctionTestCase {
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
                    startsWith("DateParseEvaluator[val=Attribute[channel=1], formatter=Attribute[channel=0], zoneId=Z, locale="),
                    DataType.DATETIME,
                    equalTo(1683244800000L)
                ).withConfiguration(TEST_SOURCE, configurationForTimezone(ZoneOffset.UTC))
            )
        );
        cases.add(new TestCaseSupplier("Timezoned Case", List.of(DataType.KEYWORD, DataType.KEYWORD), () -> {
            long ts_sec = 1657585450L; // 2022-07-12T00:24:10Z
            int hours = randomIntBetween(0, 23);
            String date = String.format(Locale.ROOT, "12/Jul/2022:%02d:24:10 +0900", hours);
            long expected_ts = (ts_sec + (hours - 9) * 3600L) * 1000L;
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef("dd/MMM/yyyy:HH:mm:ss XX"), DataType.KEYWORD, "first"),
                    new TestCaseSupplier.TypedData(new BytesRef(date), DataType.KEYWORD, "second")
                ),
                startsWith("DateParseEvaluator[val=Attribute[channel=1], formatter=Attribute[channel=0], zoneId="),
                DataType.DATETIME,
                matchesDateMillis(expected_ts)
            ).withConfiguration(TEST_SOURCE, configurationForLocale(Locale.US));
        }));

        for (DataType dateType : DataType.stringTypes()) {
            cases.add(
                new TestCaseSupplier(
                    "With " + dateType,
                    List.of(dateType),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(new TestCaseSupplier.TypedData(new BytesRef("2023-05-05T00:00:00.000Z"), dateType, "second")),
                        "DateParseConstantEvaluator[val=Attribute[channel=0], "
                            + "formatter=format[strict_date_optional_time] locale[], zoneId=Z, locale=]",
                        DataType.DATETIME,
                        equalTo(1683244800000L)
                    ).withConfiguration(TEST_SOURCE, configurationForTimezone(ZoneOffset.UTC))
                )
            );

            for (DataType formatType : DataType.stringTypes()) {
                cases.add(
                    new TestCaseSupplier(
                        "With " + formatType + " and " + dateType,
                        List.of(formatType, dateType),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(new BytesRef("yyyy-MM-dd"), formatType, "first"),
                                new TestCaseSupplier.TypedData(new BytesRef("2023-05-05"), dateType, "second")
                            ),
                            "DateParseEvaluator[val=Attribute[channel=1], formatter=Attribute[channel=0], zoneId=Z, locale=]",
                            DataType.DATETIME,
                            equalTo(1683244800000L)
                        ).withConfiguration(TEST_SOURCE, configurationForTimezone(ZoneOffset.UTC))
                    )
                );
            }
        }

        cases.add(
            new TestCaseSupplier(
                List.of(DataType.KEYWORD, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BytesRef("not a format"), DataType.KEYWORD, "first"),
                        new TestCaseSupplier.TypedData(new BytesRef("2023-05-05"), DataType.KEYWORD, "second")
                    ),
                    startsWith("DateParseEvaluator[val=Attribute[channel=1], formatter=Attribute[channel=0], zoneId="),
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
                    startsWith("DateParseEvaluator[val=Attribute[channel=1], formatter=Attribute[channel=0], zoneId="),
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

        for (DataType dateType : List.of(DataType.KEYWORD, DataType.TEXT)) {
            cases.add(
                new TestCaseSupplier(
                    "Map with " + dateType,
                    List.of(dateType, DataType.OBJECT),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(new BytesRef("2023-05-05T00:00:00.000Z"), dateType, "second"),
                            new TestCaseSupplier.TypedData(
                                new MapExpression(
                                    Source.EMPTY,
                                    List.of(
                                        new Literal(Source.EMPTY, new BytesRef("time_zone"), DataType.KEYWORD),
                                        new Literal(Source.EMPTY, new BytesRef("Z"), DataType.KEYWORD),

                                        new Literal(Source.EMPTY, new BytesRef("locale"), DataType.KEYWORD),
                                        new Literal(Source.EMPTY, new BytesRef("en-us"), DataType.KEYWORD)
                                    )
                                ),
                                DataType.OBJECT,
                                "options"
                            ).forceLiteral()
                        ),
                        "DateParseConstantEvaluator[val=Attribute[channel=0], "
                            + "formatter=format[strict_date_optional_time] locale[en_US], zoneId=Z, locale=en_US]",
                        DataType.DATETIME,
                        equalTo(1683244800000L)
                    )
                )
            );

            for (DataType formatType : List.of(DataType.KEYWORD, DataType.TEXT)) {
                cases.add(
                    new TestCaseSupplier(
                        "Map with " + formatType + " and " + dateType,
                        List.of(formatType, dateType, DataType.OBJECT),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(new BytesRef("yyyy-MM-dd"), formatType, "first"),
                                new TestCaseSupplier.TypedData(new BytesRef("2023-05-05"), dateType, "second"),
                                new TestCaseSupplier.TypedData(
                                    new MapExpression(
                                        Source.EMPTY,
                                        List.of(
                                            new Literal(Source.EMPTY, new BytesRef("time_zone"), DataType.KEYWORD),
                                            new Literal(Source.EMPTY, new BytesRef("Z"), DataType.KEYWORD),

                                            new Literal(Source.EMPTY, new BytesRef("locale"), DataType.KEYWORD),
                                            new Literal(Source.EMPTY, new BytesRef("en-us"), DataType.KEYWORD)
                                        )
                                    ),
                                    DataType.OBJECT,
                                    "options"
                                ).forceLiteral()
                            ),
                            "DateParseEvaluator[val=Attribute[channel=1], formatter=Attribute[channel=0], zoneId=Z, locale=en_US]",
                            DataType.DATETIME,
                            equalTo(1683244800000L)
                        )
                    )
                );
            }
        }

        // Timezones
        cases.addAll(casesFor("yyyy-MM-dd", "2023-05-05", "Z", "en-us", "2023-05-05T00:00:00.000Z"));
        cases.addAll(casesFor("dd-mm-yyyy", "10-10-2025", "Europe/Paris", "en-us", "2024-12-31T23:00:00.000Z"));
        cases.addAll(casesFor("yyyy-MM-dd", "2023-05-05", "-05:00", "en-us", "2023-05-05T00:00:00.000-05:00"));
        cases.addAll(casesFor("yyyy-MM-dd", "2023-05-05", "Europe/Madrid", "en-us", "2023-05-05T00:00:00.000+02:00"));
        cases.addAll(casesFor("yyyy-MM-dd XXX", "2023-05-05 +08:45", "Europe/Madrid", "en-us", "2023-05-05T00:00:00.000+08:45"));
        cases.addAll(
            casesFor("yyyy-MM-dd'T'HH:mm:ss.SSS", "2023-05-05T11:22:33.444", "Europe/Madrid", "en-us", "2023-05-05T11:22:33.444+02:00")
        );
        cases.addAll(casesForDefaultFormat("2023-05-05T11:22:33.444", "Europe/Madrid", "en-us", "2023-05-05T11:22:33.444+02:00"));
        cases.addAll(casesForDefaultFormat("2023-05-05T11:22:33.444+05:00", "Europe/Madrid", "en-us", "2023-05-05T11:22:33.444+05:00"));

        // Locales
        cases.addAll(casesFor("yyyy-MMM-dd", "2023-Jan-05", "Z", "en-us", "2023-01-05T00:00:00.000Z"));
        cases.addAll(casesFor("yyyy-MMM-dd", "2023-ene-05", "Z", "es-es", "2023-01-05T00:00:00.000Z"));
        cases.addAll(
            casesFor("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", "2023-05-05T11:22:33.444+01:00", "Z", "es-es", "2023-05-05T11:22:33.444+01:00")
        );
        cases.addAll(
            casesFor("yyyy-MMM-dd'T'HH:mm:ss.SSSXXX", "2023-may-05T11:22:33.444+01:00", "Z", "es-es", "2023-05-05T11:22:33.444+01:00")
        );
        cases.addAll(
            casesFor("yyyy-MMM-dd'T'HH:mm:ss.SSSXXX", "2023-may-05T11:22:33.444+01:00", "Z", "es-es", "2023-05-05T11:22:33.444+01:00")
        );
        cases.addAll(casesForDefaultFormat("2023-05-05T11:22:33.444+02:00", "Z", "es-es", "2023-05-05T11:22:33.444+02:00"));
        cases.addAll(casesForDefaultFormat("2023-05-05T11:22:33.444", "Europe/Madrid", "es-es", "2023-05-05T11:22:33.444+02:00"));

        cases.addAll(
            casesFor("yyyy-MM-dd'T'HH:mm:ss.SSS", "2023-05-05T11:22:33.444", "Europe/Madrid", "es-es", "2023-05-05T11:22:33.444+02:00")
        );
        cases.addAll(casesFor("yyyy-MMM-dd", "2023-ene-05", "Europe/Madrid", "es-es", "2023-01-05T00:00:00.000+01:00"));

        return parameterSuppliersFromTypedData(randomizeBytesRefsOffset(cases));
    }

    private static List<TestCaseSupplier> casesFor(
        String format,
        String date,
        String zoneIdString,
        String localeString,
        String expectedString
    ) {
        ZoneId zoneId = ZoneId.of(zoneIdString);
        Locale locale = Locale.forLanguageTag(localeString);

        return List.of(
            // Zone and locale in the options map
            new TestCaseSupplier(
                format + ", " + date + ", " + zoneIdString + ", " + localeString + ", " + expectedString + " (map)",
                List.of(DataType.KEYWORD, DataType.KEYWORD, DataType.OBJECT),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BytesRef(format), DataType.KEYWORD, "pattern"),
                        new TestCaseSupplier.TypedData(new BytesRef(date), DataType.KEYWORD, "date"),
                        new TestCaseSupplier.TypedData(
                            new MapExpression(
                                Source.EMPTY,
                                List.of(
                                    new Literal(Source.EMPTY, new BytesRef("time_zone"), DataType.KEYWORD),
                                    new Literal(Source.EMPTY, new BytesRef(zoneIdString), DataType.KEYWORD),

                                    new Literal(Source.EMPTY, new BytesRef("locale"), DataType.KEYWORD),
                                    new Literal(Source.EMPTY, new BytesRef(localeString), DataType.KEYWORD)
                                )
                            ),
                            DataType.OBJECT,
                            "options"
                        ).forceLiteral()
                    ),
                    "DateParseEvaluator[val=Attribute[channel=1], formatter=Attribute[channel=0], zoneId="
                        + zoneId
                        + ", locale="
                        + locale
                        + "]",
                    DataType.DATETIME,
                    matchesDateMillis(expectedString)
                )
            ),

            // Zone and locale in the configuration
            new TestCaseSupplier(
                format + ", " + date + ", " + zoneIdString + ", " + localeString + ", " + expectedString + " (config)",
                List.of(DataType.KEYWORD, DataType.KEYWORD, DataType.OBJECT),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BytesRef(format), DataType.KEYWORD, "pattern"),
                        new TestCaseSupplier.TypedData(new BytesRef(date), DataType.KEYWORD, "date"),
                        new TestCaseSupplier.TypedData(
                            new MapExpression(
                                Source.EMPTY,
                                List.of(
                                    new Literal(Source.EMPTY, new BytesRef("locale"), DataType.KEYWORD),
                                    new Literal(Source.EMPTY, new BytesRef(localeString), DataType.KEYWORD)
                                )
                            ),
                            DataType.OBJECT,
                            "options"
                        ).forceLiteral()
                    ),
                    "DateParseEvaluator[val=Attribute[channel=1], formatter=Attribute[channel=0], zoneId="
                        + zoneId
                        + ", locale="
                        + locale
                        + "]",
                    DataType.DATETIME,
                    matchesDateMillis(expectedString)
                ).withConfiguration(TEST_SOURCE, configurationForTimezone(zoneId))
            )
        );
    }

    private static List<TestCaseSupplier> casesForDefaultFormat(
        String date,
        String zoneIdString,
        String localeString,
        String expectedString
    ) {
        ZoneId zoneId = ZoneId.of(zoneIdString);
        Locale locale = Locale.forLanguageTag(localeString);

        return List.of(
            new TestCaseSupplier(
                date + ", " + zoneIdString + ", " + localeString + ", " + expectedString + " (map)",
                List.of(DataType.KEYWORD, DataType.OBJECT),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BytesRef(date), DataType.KEYWORD, "date"),
                        new TestCaseSupplier.TypedData(
                            new MapExpression(
                                Source.EMPTY,
                                List.of(
                                    new Literal(Source.EMPTY, new BytesRef("time_zone"), DataType.KEYWORD),
                                    new Literal(Source.EMPTY, new BytesRef(zoneIdString), DataType.KEYWORD),

                                    new Literal(Source.EMPTY, new BytesRef("locale"), DataType.KEYWORD),
                                    new Literal(Source.EMPTY, new BytesRef(localeString), DataType.KEYWORD)
                                )
                            ),
                            DataType.OBJECT,
                            "options"
                        ).forceLiteral()
                    ),
                    "DateParseConstantEvaluator[val=Attribute[channel=0], formatter=format[strict_date_optional_time] locale["
                        + locale
                        + "], zoneId="
                        + zoneId
                        + ", locale="
                        + locale
                        + "]",
                    DataType.DATETIME,
                    matchesDateMillis(expectedString)
                )
            ),
            new TestCaseSupplier(
                date + ", " + zoneIdString + ", " + localeString + ", " + expectedString + " (config)",
                List.of(DataType.KEYWORD, DataType.OBJECT),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BytesRef(date), DataType.KEYWORD, "date"),
                        new TestCaseSupplier.TypedData(
                            new MapExpression(
                                Source.EMPTY,
                                List.of(
                                    new Literal(Source.EMPTY, new BytesRef("locale"), DataType.KEYWORD),
                                    new Literal(Source.EMPTY, new BytesRef(localeString), DataType.KEYWORD)
                                )
                            ),
                            DataType.OBJECT,
                            "options"
                        ).forceLiteral()
                    ),
                    "DateParseConstantEvaluator[val=Attribute[channel=0], formatter=format[strict_date_optional_time] locale["
                        + locale
                        + "], zoneId="
                        + zoneId
                        + ", locale="
                        + locale
                        + "]",
                    DataType.DATETIME,
                    matchesDateMillis(expectedString)
                ).withConfiguration(TEST_SOURCE, configurationForTimezone(zoneId))
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
                    new Literal(Source.EMPTY, new BytesRef(pattern), DataType.KEYWORD),
                    field("str", DataType.KEYWORD),
                    null,
                    randomConfiguration()
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
                    ),
                    randomConfiguration()
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
                    ),
                    randomConfiguration()
                )
            ).get(driverContext)
        );
        assertThat(e.getMessage(), startsWith("unsupported timezone [" + timezone + "]"));
    }

    @Override
    protected Expression buildWithConfiguration(Source source, List<Expression> args, Configuration configuration) {
        return new DateParse(
            source,
            args.get(0),
            args.size() > 1 ? args.get(1) : null,
            args.size() == 3 ? args.get(2) : null,
            configuration
        );
    }
}
