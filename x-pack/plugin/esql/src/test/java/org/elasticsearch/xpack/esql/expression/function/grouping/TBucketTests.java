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
import org.apache.lucene.util.BytesRef;
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

// The amount of date trunc cases sometimes exceed the 20 minutes
@TimeoutSuite(millis = 60 * TimeUnits.MINUTE)
public class TBucketTests extends AbstractConfigurationFunctionTestCase {
    public TBucketTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    private static final DataType[] DATE_BOUNDS_TYPE = new DataType[] { DataType.DATETIME, DataType.KEYWORD, DataType.TEXT };

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        dateCases(suppliers, "fixed date", () -> DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2023-02-17T09:00:00.00Z"));
        dateNanosCases(suppliers, "fixed date nanos", () -> DateUtils.toLong(Instant.parse("2023-02-17T09:00:00.00Z")));
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

    private static void dateCases(List<TestCaseSupplier> suppliers, String name, LongSupplier date) {
        for (DataType fromType : DATE_BOUNDS_TYPE) {
            for (DataType toType : DATE_BOUNDS_TYPE) {
                suppliers.add(new TestCaseSupplier(name, List.of(DataType.INTEGER, fromType, toType, DataType.DATETIME), () -> {
                    List<TestCaseSupplier.TypedData> args = new ArrayList<>();
                    args.add(new TestCaseSupplier.TypedData(50, DataType.INTEGER, "buckets").forceLiteral());
                    args.add(dateBound("from", fromType, "2023-02-01T00:00:00.00Z"));
                    args.add(dateBound("to", toType, "2023-03-01T09:00:00.00Z"));
                    args.add(new TestCaseSupplier.TypedData(date.getAsLong(), DataType.DATETIME, "@timestamp"));
                    return new TestCaseSupplier.TestCase(
                        args,
                        "DateTruncDatetimeEvaluator[fieldVal=Attribute[channel=0], "
                            + "rounding=Rounding[DAY_OF_MONTH in Z][fixed to midnight]]",
                        DataType.DATETIME,
                        dateResultsMatcher(date, Rounding.DateTimeUnit.DAY_OF_MONTH)
                    ).withConfiguration(TEST_SOURCE, configurationForTimezone(ZoneOffset.UTC));
                }));
                suppliers.add(new TestCaseSupplier(name, List.of(DataType.INTEGER, fromType, toType, DataType.DATETIME), () -> {
                    List<TestCaseSupplier.TypedData> args = new ArrayList<>();
                    args.add(new TestCaseSupplier.TypedData(4, DataType.INTEGER, "buckets").forceLiteral());
                    args.add(dateBound("from", fromType, "2023-02-17T09:00:00Z"));
                    args.add(dateBound("to", toType, "2023-02-17T12:00:00Z"));
                    args.add(new TestCaseSupplier.TypedData(date.getAsLong(), DataType.DATETIME, "@timestamp"));
                    return new TestCaseSupplier.TestCase(
                        args,
                        "DateTruncDatetimeEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding[3600000 in Z][fixed]]",
                        DataType.DATETIME,
                        equalTo(Rounding.builder(Rounding.DateTimeUnit.HOUR_OF_DAY).build().prepareForUnknown().round(date.getAsLong()))
                    ).withConfiguration(TEST_SOURCE, configurationForTimezone(ZoneOffset.UTC));
                }));
            }
        }
    }

    private static void dateNanosCases(List<TestCaseSupplier> suppliers, String name, LongSupplier date) {
        for (DataType fromType : DATE_BOUNDS_TYPE) {
            for (DataType toType : DATE_BOUNDS_TYPE) {
                suppliers.add(new TestCaseSupplier(name, List.of(DataType.INTEGER, fromType, toType, DataType.DATE_NANOS), () -> {
                    List<TestCaseSupplier.TypedData> args = new ArrayList<>();
                    args.add(new TestCaseSupplier.TypedData(50, DataType.INTEGER, "buckets").forceLiteral());
                    args.add(dateBound("from", fromType, "2023-02-01T00:00:00.00Z"));
                    args.add(dateBound("to", toType, "2023-03-01T09:00:00.00Z"));
                    args.add(new TestCaseSupplier.TypedData(date.getAsLong(), DataType.DATE_NANOS, "@timestamp"));
                    return new TestCaseSupplier.TestCase(
                        args,
                        Matchers.startsWith("DateTruncDateNanosEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding["),
                        DataType.DATE_NANOS,
                        dateNanosResultsMatcher(date, Rounding.DateTimeUnit.DAY_OF_MONTH)
                    ).withConfiguration(TEST_SOURCE, configurationForTimezone(ZoneOffset.UTC));
                }));
                suppliers.add(new TestCaseSupplier(name, List.of(DataType.INTEGER, fromType, toType, DataType.DATE_NANOS), () -> {
                    List<TestCaseSupplier.TypedData> args = new ArrayList<>();
                    args.add(new TestCaseSupplier.TypedData(4, DataType.INTEGER, "buckets").forceLiteral());
                    args.add(dateBound("from", fromType, "2023-02-17T09:00:00Z"));
                    args.add(dateBound("to", toType, "2023-02-17T12:00:00Z"));
                    args.add(new TestCaseSupplier.TypedData(date.getAsLong(), DataType.DATE_NANOS, "@timestamp"));
                    return new TestCaseSupplier.TestCase(
                        args,
                        Matchers.startsWith("DateTruncDateNanosEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding["),
                        DataType.DATE_NANOS,
                        equalTo(Rounding.builder(Rounding.DateTimeUnit.HOUR_OF_DAY).build().prepareForUnknown().round(date.getAsLong()))
                    ).withConfiguration(TEST_SOURCE, configurationForTimezone(ZoneOffset.UTC));
                }));
            }
        }
    }

    private static TestCaseSupplier.TypedData dateBound(String name, DataType type, String date) {
        Object value;
        if (type == DataType.DATETIME) {
            value = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(date);
        } else {
            value = new BytesRef(date);
        }
        return new TestCaseSupplier.TypedData(value, type, name).forceLiteral();
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
                spanResultsMatcher(args)
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
                spanResultsMatcher(args)
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

    private static Matcher<Object> dateResultsMatcher(LongSupplier date, Rounding.DateTimeUnit unit) {
        long millis = date.getAsLong();
        long expected = Rounding.builder(unit).build().prepareForUnknown().round(millis);
        return equalTo(expected);
    }

    private static Matcher<Object> dateNanosResultsMatcher(LongSupplier date, Rounding.DateTimeUnit unit) {
        long nanos = date.getAsLong();
        long expected = DateUtils.toNanoSeconds(Rounding.builder(unit).build().prepareForUnknown().round(DateUtils.toMilliSeconds(nanos)));
        return equalTo(expected);
    }

    private static Matcher<Object> spanResultsMatcher(List<TestCaseSupplier.TypedData> typedData) {
        int tsIndex = typedData.size() - 1;
        if (typedData.get(tsIndex).type() == DataType.DATE_NANOS) {
            long nanos = ((Number) typedData.get(tsIndex).data()).longValue();
            long expected = DateUtils.toNanoSeconds(
                Rounding.builder(Rounding.DateTimeUnit.DAY_OF_MONTH).build().prepareForUnknown().round(DateUtils.toMilliSeconds(nanos))
            );
            LogManager.getLogger(getTestClass()).info("Expected: " + DateUtils.toInstant(expected));
            LogManager.getLogger(getTestClass()).info("Input: " + DateUtils.toInstant(nanos));
            return equalTo(expected);
        }

        long millis = ((Number) typedData.get(tsIndex).data()).longValue();
        long expected = Rounding.builder(Rounding.DateTimeUnit.DAY_OF_MONTH).build().prepareForUnknown().round(millis);
        LogManager.getLogger(getTestClass()).info("Expected: " + Instant.ofEpochMilli(expected));
        LogManager.getLogger(getTestClass()).info("Input: " + Instant.ofEpochMilli(millis));
        return equalTo(expected);
    }

    @Override
    protected Expression buildWithConfiguration(Source source, List<Expression> args, Configuration configuration) {
        if (args.size() == 4) {
            return new TBucket(source, args.get(0), args.get(1), args.get(2), args.get(3), configuration);
        }
        return new TBucket(source, args.get(0), null, null, args.get(1), configuration);
    }

    @Override
    protected boolean canSerialize() {
        return false;
    }

    public static List<DocsV3Support.Param> signatureTypes(List<DocsV3Support.Param> params) {
        assertThat(params.size(), anyOf(equalTo(2), equalTo(4)));
        int tsIndex = params.size() - 1;
        assertThat(params.get(tsIndex).dataType(), anyOf(equalTo(DataType.DATE_NANOS), equalTo(DataType.DATETIME)));
        return params.subList(0, tsIndex);
    }
}
