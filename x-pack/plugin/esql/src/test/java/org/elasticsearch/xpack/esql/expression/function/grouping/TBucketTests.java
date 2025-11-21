/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractConfigurationFunctionTestCase;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.elasticsearch.test.ReadableMatchers.matchesDateMillis;
import static org.elasticsearch.test.ReadableMatchers.matchesDateNanos;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.TEST_SOURCE;
import static org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTruncTests.makeTruncDurationTestCases;
import static org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTruncTests.makeTruncPeriodTestCases;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

// The amount of date trunc cases sometimes exceed the 20 minutes
@TimeoutSuite(millis = 60 * TimeUnits.MINUTE)
public class TBucketTests extends AbstractConfigurationFunctionTestCase {
    public TBucketTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        dateCasesWithSpan(
            suppliers,
            "fixed date with period",
            () -> DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2023-01-01T00:00:00.00Z"),
            DataType.DATE_PERIOD,
            Period.ofYears(1),
            "[YEAR_OF_CENTURY in Z][fixed to midnight]"
        );
        dateCasesWithSpan(
            suppliers,
            "fixed date with duration",
            () -> DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2023-02-17T09:00:00.00Z"),
            DataType.TIME_DURATION,
            Duration.ofDays(1L),
            "[86400000 in Z][fixed]"
        );
        dateNanosCasesWithSpan(
            suppliers,
            "fixed date nanos with period",
            () -> DateUtils.toLong(Instant.parse("2023-01-01T00:00:00.00Z")),
            DataType.DATE_PERIOD,
            Period.ofYears(1)
        );
        dateNanosCasesWithSpan(
            suppliers,
            "fixed date nanos with duration",
            () -> DateUtils.toLong(Instant.parse("2023-02-17T09:00:00.00Z")),
            DataType.TIME_DURATION,
            Duration.ofDays(1L)
        );
        dateTruncCases(suppliers);
        return parameterSuppliersFromTypedData(suppliers);
    }

    private static void dateCasesWithSpan(
        List<TestCaseSupplier> suppliers,
        String name,
        LongSupplier date,
        DataType spanType,
        Object span,
        String spanStr
    ) {
        suppliers.add(new TestCaseSupplier(name, List.of(spanType, DataType.DATETIME), () -> {
            List<TestCaseSupplier.TypedData> args = new ArrayList<>();
            args.add(new TestCaseSupplier.TypedData(span, spanType, "buckets").forceLiteral());
            args.add(new TestCaseSupplier.TypedData(date.getAsLong(), DataType.DATETIME, "@timestamp"));

            return new TestCaseSupplier.TestCase(
                args,
                "DateTruncDatetimeEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding" + spanStr + "]",
                DataType.DATETIME,
                resultsMatcher(args)
            ).withConfiguration(TEST_SOURCE, configurationForTimezone(ZoneOffset.UTC));
        }));
    }

    private static void dateNanosCasesWithSpan(
        List<TestCaseSupplier> suppliers,
        String name,
        LongSupplier date,
        DataType spanType,
        Object span
    ) {
        suppliers.add(new TestCaseSupplier(name, List.of(spanType, DataType.DATE_NANOS), () -> {
            List<TestCaseSupplier.TypedData> args = new ArrayList<>();
            args.add(new TestCaseSupplier.TypedData(span, spanType, "buckets").forceLiteral());
            args.add(new TestCaseSupplier.TypedData(date.getAsLong(), DataType.DATE_NANOS, "@timestamp"));
            return new TestCaseSupplier.TestCase(
                args,
                Matchers.startsWith("DateTruncDateNanosEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding["),
                DataType.DATE_NANOS,
                resultsMatcher(args)
            ).withConfiguration(TEST_SOURCE, configurationForTimezone(ZoneOffset.UTC));
        }));
    }

    private static void dateTruncCases(List<TestCaseSupplier> suppliers) {
        makeTruncPeriodTestCases().stream().map(data -> {
            List<TestCaseSupplier> caseSuppliers = new ArrayList<>();

            caseSuppliers.add(
                new TestCaseSupplier(
                    data.testCaseNameForMillis(),
                    List.of(DataType.DATE_PERIOD, DataType.DATETIME),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(data.period(), DataType.DATE_PERIOD, "interval").forceLiteral(),
                            new TestCaseSupplier.TypedData(data.inputDateAsMillis(), DataType.DATETIME, "@timestamp")
                        ),
                        Matchers.startsWith("DateTruncDatetimeEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding["),
                        DataType.DATETIME,
                        matchesDateMillis(data.expectedDate())
                    ).withConfiguration(TEST_SOURCE, configurationForTimezone(data.zoneId()))
                )
            );

            if (data.canBeConvertedToNanos()) {
                caseSuppliers.add(
                    new TestCaseSupplier(
                        data.testCaseNameForNanos(),
                        List.of(DataType.DATE_PERIOD, DataType.DATE_NANOS),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(data.period(), DataType.DATE_PERIOD, "interval").forceLiteral(),
                                new TestCaseSupplier.TypedData(data.inputDateAsNanos(), DataType.DATE_NANOS, "@timestamp")
                            ),
                            Matchers.startsWith("DateTruncDateNanosEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding["),
                            DataType.DATE_NANOS,
                            matchesDateNanos(data.expectedDate())
                        ).withConfiguration(TEST_SOURCE, configurationForTimezone(data.zoneId()))
                    )
                );
            }

            return caseSuppliers;
        }).forEach(suppliers::addAll);

        makeTruncDurationTestCases().stream().map(data -> {
            List<TestCaseSupplier> caseSuppliers = new ArrayList<>();

            caseSuppliers.add(
                new TestCaseSupplier(
                    data.testCaseNameForMillis(),
                    List.of(DataType.TIME_DURATION, DataType.DATETIME),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(data.duration(), DataType.TIME_DURATION, "interval").forceLiteral(),
                            new TestCaseSupplier.TypedData(data.inputDateAsMillis(), DataType.DATETIME, "@timestamp")
                        ),
                        Matchers.startsWith("DateTruncDatetimeEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding["),
                        DataType.DATETIME,
                        matchesDateMillis(data.expectedDate())
                    ).withConfiguration(TEST_SOURCE, configurationForTimezone(data.zoneId()))
                )
            );

            if (data.canBeConvertedToNanos()) {
                caseSuppliers.add(
                    new TestCaseSupplier(
                        data.testCaseNameForNanos(),
                        List.of(DataType.TIME_DURATION, DataType.DATE_NANOS),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(data.duration(), DataType.TIME_DURATION, "interval").forceLiteral(),
                                new TestCaseSupplier.TypedData(data.inputDateAsNanos(), DataType.DATE_NANOS, "@timestamp")
                            ),
                            Matchers.startsWith("DateTruncDateNanosEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding["),
                            DataType.DATE_NANOS,
                            matchesDateNanos(data.expectedDate())
                        ).withConfiguration(TEST_SOURCE, configurationForTimezone(data.zoneId()))
                    )
                );
            }

            return caseSuppliers;
        }).forEach(suppliers::addAll);
    }

    private static Matcher<Object> resultsMatcher(List<TestCaseSupplier.TypedData> typedData) {
        if (typedData.get(1).type() == DataType.DATE_NANOS) {
            long nanos = ((Number) typedData.get(1).data()).longValue();
            long expected = DateUtils.toNanoSeconds(
                Rounding.builder(Rounding.DateTimeUnit.DAY_OF_MONTH).build().prepareForUnknown().round(DateUtils.toMilliSeconds(nanos))
            );
            LogManager.getLogger(getTestClass()).info("Expected: " + DateUtils.toInstant(expected));
            LogManager.getLogger(getTestClass()).info("Input: " + DateUtils.toInstant(nanos));
            return equalTo(expected);
        }

        // For DATETIME, we use the millis value directly
        long millis = ((Number) typedData.get(1).data()).longValue();
        long expected = Rounding.builder(Rounding.DateTimeUnit.DAY_OF_MONTH).build().prepareForUnknown().round(millis);
        LogManager.getLogger(getTestClass()).info("Expected: " + Instant.ofEpochMilli(expected));
        LogManager.getLogger(getTestClass()).info("Input: " + Instant.ofEpochMilli(millis));
        return equalTo(expected);
    }

    @Override
    protected Expression buildWithConfiguration(Source source, List<Expression> args, Configuration configuration) {
        return new TBucket(source, args.get(0), args.get(1), configuration);
    }

    @Override
    protected boolean canSerialize() {
        return false;
    }

    public static List<DocsV3Support.Param> signatureTypes(List<DocsV3Support.Param> params) {
        assertThat(params, hasSize(2));
        assertThat(params.get(1).dataType(), anyOf(equalTo(DataType.DATE_NANOS), equalTo(DataType.DATETIME)));
        return List.of(params.get(0));
    }
}
