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
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractConfigurationFunctionTestCase;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
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

import static org.hamcrest.Matchers.equalTo;

public class MonthNameTests extends AbstractConfigurationFunctionTestCase {

    public MonthNameTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Override
    protected Expression buildWithConfiguration(Source source, List<Expression> args, Configuration configuration) {
        return new MonthName(source, args.get(0), configuration);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        suppliers.addAll(generateTest("1994-01-19T00:00:00.00Z", "January"));
        suppliers.addAll(generateTest("1995-02-20T23:59:59.99Z", "February"));
        suppliers.addAll(generateTest("1996-03-21T23:12:32.12Z", "March"));
        suppliers.addAll(generateTest("1997-04-22T07:39:01.28Z", "April"));
        suppliers.addAll(generateTest("1998-05-23T10:25:33.38Z", "May"));
        suppliers.addAll(generateTest("1999-06-24T22:55:33.82Z", "June"));
        suppliers.addAll(generateTest("2000-07-25T01:01:29.49Z", "July"));
        suppliers.addAll(generateTest("2001-08-25T01:01:29.49Z", "August"));
        suppliers.addAll(generateTest("2002-09-25T01:01:29.49Z", "September"));
        suppliers.addAll(generateTest("2003-10-25T01:01:29.49Z", "October"));
        suppliers.addAll(generateTest("2004-11-25T01:01:29.49Z", "November"));
        suppliers.addAll(generateTest("2005-12-25T01:01:29.49Z", "December"));

        suppliers.add(
            new TestCaseSupplier(
                List.of(DataType.DATETIME),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(null, DataType.DATETIME, "date")),
                    Matchers.startsWith("MonthNameMillisEvaluator[val=Attribute[channel=0], zoneId=Z, locale=en_US]"),
                    DataType.KEYWORD,
                    equalTo(null)
                )
            )
        );

        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(true, suppliers);
    }

    private static List<TestCaseSupplier> generateTest(String dateTime, String expectedMonthName) {
        return List.of(
            new TestCaseSupplier(
                List.of(DataType.DATETIME),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(toMillis(dateTime), DataType.DATETIME, "date")),
                    Matchers.startsWith("MonthNameMillisEvaluator[val=Attribute[channel=0], zoneId=Z, locale=en_US]"),
                    DataType.KEYWORD,
                    equalTo(new BytesRef(expectedMonthName))
                )
            ),
            new TestCaseSupplier(
                List.of(DataType.DATE_NANOS),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(toNanos(dateTime), DataType.DATE_NANOS, "date")),
                    Matchers.is("MonthNameNanosEvaluator[val=Attribute[channel=0], zoneId=Z, locale=en_US]"),
                    DataType.KEYWORD,
                    equalTo(new BytesRef(expectedMonthName))
                )
            )
        );
    }

    private static long toMillis(String timestamp) {
        return Instant.parse(timestamp).toEpochMilli();
    }

    private static long toNanos(String timestamp) {
        return DateUtils.toLong(Instant.parse(timestamp));
    }

    public void testRandomLocale() {
        long randomMillis = randomMillisUpToYear9999();
        Configuration cfg = configWithZoneAndLocale(randomZone(), randomLocale(random()));
        String expected = Instant.ofEpochMilli(randomMillis).atZone(cfg.zoneId()).getMonth().getDisplayName(TextStyle.FULL, cfg.locale());

        MonthName func = new MonthName(Source.EMPTY, new Literal(Source.EMPTY, randomMillis, DataType.DATETIME), cfg);
        assertThat(BytesRefs.toBytesRef(expected), equalTo(func.fold(FoldContext.small())));
    }

    public void testFixedLocaleAndTime() {
        long randomMillis = toMillis("1996-03-21T00:00:00.00Z");
        Configuration cfg = configWithZoneAndLocale(ZoneId.of("America/Sao_Paulo"), Locale.of("pt", "br"));
        String expected = "março";

        MonthName func = new MonthName(Source.EMPTY, new Literal(Source.EMPTY, randomMillis, DataType.DATETIME), cfg);
        assertThat(BytesRefs.toBytesRef(expected), equalTo(func.fold(FoldContext.small())));
    }

    private Configuration configWithZoneAndLocale(ZoneId zone, Locale locale) {
        return new Configuration(
            zone,
            locale,
            null,
            null,
            new QueryPragmas(Settings.EMPTY),
            EsqlPlugin.QUERY_RESULT_TRUNCATION_MAX_SIZE.getDefault(Settings.EMPTY),
            EsqlPlugin.QUERY_RESULT_TRUNCATION_DEFAULT_SIZE.getDefault(Settings.EMPTY),
            "",
            false,
            Map.of(),
            System.nanoTime(),
            randomBoolean()
        );
    }
}
