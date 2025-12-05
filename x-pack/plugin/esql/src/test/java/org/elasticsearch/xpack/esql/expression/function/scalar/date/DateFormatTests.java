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
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractConfigurationFunctionTestCase;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

import static org.elasticsearch.test.ReadableMatchers.matchesBytesRef;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.TEST_SOURCE;
import static org.hamcrest.Matchers.matchesPattern;

public class DateFormatTests extends AbstractConfigurationFunctionTestCase {
    public DateFormatTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        ///
        /// UTC and en_en cases
        ///
        // Formatter supplied cases
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                (format, value) -> new BytesRef(
                    DateFormatter.forPattern(((BytesRef) format).utf8ToString()).formatMillis(((Instant) value).toEpochMilli())
                ),
                DataType.KEYWORD,
                TestCaseSupplier.dateFormatCases(),
                TestCaseSupplier.dateCases(Instant.parse("1900-01-01T00:00:00.00Z"), Instant.parse("9999-12-31T00:00:00.00Z")),
                matchesPattern(
                    "DateFormatMillisEvaluator\\[val=Attribute\\[channel=1], "
                        + "formatter=Attribute\\[(channel=0|\\w+)], zoneId=Z, locale=en_US]"
                ),
                (lhs, rhs) -> List.of(),
                false
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                (format, value) -> new BytesRef(
                    DateFormatter.forPattern(((BytesRef) format).utf8ToString()).formatNanos(DateUtils.toLong((Instant) value))
                ),
                DataType.KEYWORD,
                TestCaseSupplier.dateFormatCases(),
                TestCaseSupplier.dateNanosCases(),
                matchesPattern(
                    "DateFormatNanosEvaluator\\[val=Attribute\\[channel=1], "
                        + "formatter=Attribute\\[(channel=0|\\w+)], zoneId=Z, locale=en_US]"
                ),
                (lhs, rhs) -> List.of(),
                false
            )
        );
        // Default formatter cases
        TestCaseSupplier.unary(
            suppliers,
            "DateFormatMillisConstantEvaluator[val=Attribute[channel=0], "
                + "formatter=format[strict_date_optional_time] locale[en_US]]",
            TestCaseSupplier.dateCases(Instant.parse("1900-01-01T00:00:00.00Z"), Instant.parse("9999-12-31T00:00:00.00Z")),
            DataType.KEYWORD,
            (value) -> new BytesRef(EsqlDataTypeConverter.DEFAULT_DATE_TIME_FORMATTER.formatMillis(((Instant) value).toEpochMilli())),
            List.of()
        );
        TestCaseSupplier.unary(
            suppliers,
            "DateFormatNanosConstantEvaluator[val=Attribute[channel=0], "
                + "formatter=format[strict_date_optional_time] locale[en_US]]",
            TestCaseSupplier.dateNanosCases(),
            DataType.KEYWORD,
            (value) -> new BytesRef(EsqlDataTypeConverter.DEFAULT_DATE_TIME_FORMATTER.formatNanos(DateUtils.toLong((Instant) value))),
            List.of()
        );
        suppliers = TestCaseSupplier.mapTestCases(
            suppliers,
            testCase -> testCase.withConfiguration(
                TestCaseSupplier.TEST_SOURCE,
                configurationForTimezoneAndLocale(ZoneOffset.UTC, Locale.US)
            )
        );

        ///
        /// Custom locales
        ///
        suppliers.addAll(casesFor("MMM", "2020-01-01T00:00:00.00Z", "Z", "es-es", "ene"));
        suppliers.addAll(casesFor("VV", "2020-01-01T00:00:00.00Z", "Z", "es-es", "Z"));
        suppliers.addAll(casesForDefaultFormat("2020-01-01T00:00:00.00Z", "Z", "es-es", "2020-01-01T00:00:00.000Z"));

        ///
        /// Custom timezones
        ///
        suppliers.addAll(casesFor("VV", "2020-01-01T00:00:00.00Z", "Europe/Paris", "en-us", "Europe/Paris"));
        suppliers.addAll(casesFor("VV", "2020-01-01T00:00:00.00Z", "+05:45", "en-us", "+05:45"));
        suppliers.addAll(casesFor("MMM", "2020-01-01T00:00:00.00+01:00", "Z", "en-us", "Dec"));
        suppliers.addAll(casesFor("MMM", "2020-01-01T00:00:00.00+01:00", "Europe/Paris", "en-us", "Jan"));
        suppliers.addAll(casesFor("date_time", "2020-01-01T00:00:00.00+01:00", "Europe/Paris", "en-us", "2020-01-01T00:00:00.000+01:00"));
        suppliers.addAll(casesForDefaultFormat("2020-01-01T00:00:00.00+01:00", "Europe/Paris", "en-us", "2020-01-01T00:00:00.000+01:00"));

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    private static List<TestCaseSupplier> casesFor(String format, String date, String timezone, String localeTag, String expectedString) {
        long dateMillis = Instant.parse(date).toEpochMilli();
        ZoneId zoneId = ZoneId.of(timezone);
        Locale locale = Locale.forLanguageTag(localeTag);
        return List.of(
            new TestCaseSupplier(
                format + " - " + date + " (millis) - " + locale,
                List.of(DataType.KEYWORD, DataType.DATETIME),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(format, DataType.KEYWORD, "format"),
                        new TestCaseSupplier.TypedData(dateMillis, DataType.DATETIME, "date")
                    ),
                    "DateFormatMillisEvaluator[val=Attribute[channel=1], formatter=Attribute[channel=0], zoneId="
                        + zoneId
                        + ", locale="
                        + locale
                        + "]",
                    DataType.KEYWORD,
                    matchesBytesRef(expectedString)
                ).withConfiguration(TEST_SOURCE, configurationForTimezoneAndLocale(zoneId, locale))
            ),
            new TestCaseSupplier(
                format + " - " + date + " (nanos) - " + locale,
                List.of(DataType.KEYWORD, DataType.DATE_NANOS),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(format, DataType.KEYWORD, "format"),
                        new TestCaseSupplier.TypedData(DateUtils.toNanoSeconds(dateMillis), DataType.DATE_NANOS, "date")
                    ),
                    "DateFormatNanosEvaluator[val=Attribute[channel=1], formatter=Attribute[channel=0], zoneId="
                        + zoneId
                        + ", locale="
                        + locale
                        + "]",
                    DataType.KEYWORD,
                    matchesBytesRef(expectedString)
                ).withConfiguration(TEST_SOURCE, configurationForTimezoneAndLocale(zoneId, locale))
            )
        );
    }

    private static List<TestCaseSupplier> casesForDefaultFormat(String date, String timezone, String localeTag, String expectedString) {
        long dateMillis = Instant.parse(date).toEpochMilli();
        ZoneId zoneId = ZoneId.of(timezone);
        Locale locale = Locale.forLanguageTag(localeTag);
        return List.of(
            new TestCaseSupplier(
                date + " (millis) - " + locale,
                List.of(DataType.DATETIME),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(dateMillis, DataType.DATETIME, "date")),
                    "DateFormatMillisConstantEvaluator[val=Attribute[channel=0], formatter=format[strict_date_optional_time] locale["
                        + locale
                        + "]]",
                    DataType.KEYWORD,
                    matchesBytesRef(expectedString)
                ).withConfiguration(TEST_SOURCE, configurationForTimezoneAndLocale(zoneId, locale))
            ),
            new TestCaseSupplier(
                date + " (nanos) - " + locale,
                List.of(DataType.DATE_NANOS),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(DateUtils.toNanoSeconds(dateMillis), DataType.DATE_NANOS, "date")),
                    "DateFormatNanosConstantEvaluator[val=Attribute[channel=0], formatter=format[strict_date_optional_time] locale["
                        + locale
                        + "]]",
                    DataType.KEYWORD,
                    matchesBytesRef(expectedString)
                ).withConfiguration(TEST_SOURCE, configurationForTimezoneAndLocale(zoneId, locale))
            )
        );
    }

    @Override
    protected Expression buildWithConfiguration(Source source, List<Expression> args, Configuration configuration) {
        return new DateFormat(source, args.get(0), args.size() == 2 ? args.get(1) : null, configuration);
    }
}
