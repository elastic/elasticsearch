/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.xpack.esql.analysis.AnalyzerSettings;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractConfigurationFunctionTestCase;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.hamcrest.Matchers;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.TextStyle;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.test.ReadableMatchers.matchesBytesRef;
import static org.hamcrest.Matchers.equalTo;

public class DayNameTests extends AbstractConfigurationFunctionTestCase {

    public DayNameTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        // UTC, English
        suppliers.addAll(generateTest("2019-03-11T00:00:00.00Z", "Monday", "Z", "en"));
        suppliers.addAll(generateTest("2022-07-26T23:59:59.99Z", "Tuesday", "Z", "en"));
        suppliers.addAll(generateTest("2017-10-11T23:12:32.12Z", "Wednesday", "Z", "en"));
        suppliers.addAll(generateTest("2023-01-05T07:39:01.28Z", "Thursday", "Z", "en"));
        suppliers.addAll(generateTest("2023-02-17T10:25:33.38Z", "Friday", "Z", "en"));
        suppliers.addAll(generateTest("2013-06-15T22:55:33.82Z", "Saturday", "Z", "en"));
        suppliers.addAll(generateTest("2024-08-18T01:01:29.49Z", "Sunday", "Z", "en"));

        // Other timezones and locales
        suppliers.addAll(generateTest("2019-03-11T22:00:00.00Z", "Tuesday", "+05:00", "en"));
        suppliers.addAll(generateTest("2019-03-11T00:00:00.00Z", "Sunday", "America/New_York", "en"));
        suppliers.addAll(generateTest("2019-03-11T00:00:00.00Z", "lunes", "Z", "es"));
        suppliers.addAll(generateTest("2019-03-11T00:00:00.00Z", "domingo", "America/New_York", "es"));

        suppliers.add(
            new TestCaseSupplier(
                "Null",
                List.of(DataType.DATETIME),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(null, DataType.DATETIME, "date")),
                    Matchers.startsWith("DayNameMillisEvaluator[val=Attribute[channel=0], zoneId="),
                    DataType.KEYWORD,
                    equalTo(null)
                )
            )
        );

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    private static List<TestCaseSupplier> generateTest(String dateTime, String expectedWeekDay, String zoneIdString, String localeTag) {
        ZoneId zoneId = ZoneId.of(zoneIdString);
        Locale locale = Locale.forLanguageTag(localeTag);
        return List.of(
            new TestCaseSupplier(
                dateTime + " (millis) - " + zoneId + ", " + locale,
                List.of(DataType.DATETIME),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(toMillis(dateTime), DataType.DATETIME, "date")),
                    Matchers.startsWith("DayNameMillisEvaluator[val=Attribute[channel=0], zoneId=" + zoneId + ", locale=" + locale + "]"),
                    DataType.KEYWORD,
                    matchesBytesRef(expectedWeekDay)
                ).withConfiguration(TestCaseSupplier.TEST_SOURCE, configurationForTimezoneAndLocale(zoneId, locale))
            ),
            new TestCaseSupplier(
                dateTime + " (nanos) - " + zoneId + ", " + locale,
                List.of(DataType.DATE_NANOS),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(toNanos(dateTime), DataType.DATE_NANOS, "date")),
                    Matchers.is("DayNameNanosEvaluator[val=Attribute[channel=0], zoneId=" + zoneId + ", locale=" + locale + "]"),
                    DataType.KEYWORD,
                    matchesBytesRef(expectedWeekDay)
                ).withConfiguration(TestCaseSupplier.TEST_SOURCE, configurationForTimezoneAndLocale(zoneId, locale))
            )
        );
    }

    private static long toMillis(String timestamp) {
        return Instant.parse(timestamp).toEpochMilli();
    }

    private static long toNanos(String timestamp) {
        return DateUtils.toLong(Instant.parse(timestamp));
    }

    @Override
    protected Expression buildWithConfiguration(Source source, List<Expression> args, Configuration configuration) {
        return new DayName(source, args.get(0), configuration);
    }

    public void testRandomLocale() {
        long randomMillis = randomMillisUpToYear9999();
        Configuration cfg = configWithZoneAndLocale(randomZone(), randomLocale(random()));
        String expected = Instant.ofEpochMilli(randomMillis)
            .atZone(cfg.zoneId())
            .getDayOfWeek()
            .getDisplayName(TextStyle.FULL, cfg.locale());

        DayName func = new DayName(Source.EMPTY, new Literal(Source.EMPTY, randomMillis, DataType.DATETIME), cfg);
        assertThat(BytesRefs.toBytesRef(expected), equalTo(func.fold(FoldContext.small())));
    }

    public void testFixedLocaleAndTime() {
        long randomMillis = toMillis("2019-03-16T00:00:00.00Z");
        Configuration cfg = configWithZoneAndLocale(ZoneId.of("America/Sao_Paulo"), Locale.of("pt", "br"));
        String expected = "sexta-feira";

        DayName func = new DayName(Source.EMPTY, new Literal(Source.EMPTY, randomMillis, DataType.DATETIME), cfg);
        assertThat(BytesRefs.toBytesRef(expected), equalTo(func.fold(FoldContext.small())));
    }

    private Configuration configWithZoneAndLocale(ZoneId zone, Locale locale) {
        return new Configuration(
            zone,
            locale,
            null,
            null,
            QueryPragmas.EMPTY,
            AnalyzerSettings.QUERY_RESULT_TRUNCATION_MAX_SIZE.getDefault(Settings.EMPTY),
            AnalyzerSettings.QUERY_RESULT_TRUNCATION_DEFAULT_SIZE.getDefault(Settings.EMPTY),
            "",
            false,
            Map.of(),
            System.nanoTime(),
            randomBoolean(),
            AnalyzerSettings.QUERY_TIMESERIES_RESULT_TRUNCATION_MAX_SIZE.getDefault(Settings.EMPTY),
            AnalyzerSettings.QUERY_TIMESERIES_RESULT_TRUNCATION_DEFAULT_SIZE.getDefault(Settings.EMPTY)
        );
    }
}
