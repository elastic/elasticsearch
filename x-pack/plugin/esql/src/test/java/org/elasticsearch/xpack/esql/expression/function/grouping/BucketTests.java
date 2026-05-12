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
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
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
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.elasticsearch.test.ReadableMatchers.matchesDateMillis;
import static org.elasticsearch.test.ReadableMatchers.matchesDateNanos;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.TEST_SOURCE;
import static org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTruncTests.makeTruncDurationTestCases;
import static org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTruncTests.makeTruncPeriodTestCases;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

// The amount of date trunc cases sometimes exceed the 20 minutes
@TimeoutSuite(millis = 60 * TimeUnits.MINUTE)
public class BucketTests extends AbstractConfigurationFunctionTestCase {

    // Shared evaluator-string prefixes for dateTruncCases — reused across hundreds of suppliers to avoid
    // per-supplier Matcher allocation during parameter collection.
    private static final Matcher<String> DATETIME_TRUNC_EVAL_PREFIX = Matchers.startsWith(
        "DateTruncDatetimeEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding["
    );
    private static final Matcher<String> DATE_NANOS_TRUNC_EVAL_PREFIX = Matchers.startsWith(
        "DateTruncDateNanosEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding["
    );

    // Shared metadata maps reused across many test cases to avoid per-supplier allocation. Bigger wins
    // belong to the dateCases/dateNanosCases/numberCases/numberCasesWithSpan suppliers, which all share
    // a small handful of (interval, unit) shapes.
    private static final Map<String, Object> META_DAY_1 = Map.of("bucket", Map.of("interval", 1L, "unit", "day"));
    private static final Map<String, Object> META_WEEK_1 = Map.of("bucket", Map.of("interval", 1L, "unit", "week"));
    private static final Map<String, Object> META_MONTH_1 = Map.of("bucket", Map.of("interval", 1L, "unit", "month"));
    private static final Map<String, Object> META_QUARTER_1 = Map.of("bucket", Map.of("interval", 1L, "unit", "quarter"));
    private static final Map<String, Object> META_YEAR_1 = Map.of("bucket", Map.of("interval", 1L, "unit", "year"));
    private static final Map<String, Object> META_HOUR_1 = Map.of("bucket", Map.of("interval", 1L, "unit", "hour"));
    private static final Map<String, Object> META_NUMERIC_50 = Map.of("bucket", Map.of("interval", 50.0));

    public BucketTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        dateCases(suppliers, "fixed date", () -> DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2023-02-17T09:00:00.00Z"));
        dateRoundingHandlesNonNestedCalendarUnits(suppliers);
        dateNanosCases(suppliers, "fixed date nanos", () -> DateUtils.toLong(Instant.parse("2023-02-17T09:00:00.00Z")));
        // Span cases — one per branch of expectedDateMetadataForSpan, registered for both DATETIME and DATE_NANOS.
        // The dates below are picked so BUCKET's rounded value equals the day-of-month rounding used by resultsMatcher
        // (i.e. the input lies on a span boundary AND on a midnight UTC), so the existing helper can validate the
        // BUCKET output without per-span result matchers.
        dateAndNanosSpanCase(
            suppliers,
            "with day period",
            "1970-01-01T00:00:00.00Z",
            DataType.DATE_PERIOD,
            Period.ofDays(1),
            "[DAY_OF_MONTH in Z][fixed to midnight]"
        );
        // 1970-01-05 is the first Monday at or after epoch (epoch itself was a Thursday, so WEEK_OF_WEEKYEAR rounding
        // would walk it back to 1969-12-29 — outside the date_nanos representable range).
        dateAndNanosSpanCase(
            suppliers,
            "with week period",
            "1970-01-05T00:00:00.00Z",
            DataType.DATE_PERIOD,
            Period.ofDays(7),
            "[WEEK_OF_WEEKYEAR in Z][fixed to midnight]"
        );
        dateAndNanosSpanCase(
            suppliers,
            "with multi-day period",
            "1970-01-01T00:00:00.00Z",
            DataType.DATE_PERIOD,
            Period.ofDays(10),
            "[864000000 in Z][fixed]"
        );
        dateAndNanosSpanCase(
            suppliers,
            "with month period",
            "1970-01-01T00:00:00.00Z",
            DataType.DATE_PERIOD,
            Period.ofMonths(1),
            "[MONTH_OF_YEAR in Z][fixed to midnight]"
        );
        dateAndNanosSpanCase(
            suppliers,
            "with quarter period",
            "1970-01-01T00:00:00.00Z",
            DataType.DATE_PERIOD,
            Period.ofMonths(3),
            "[QUARTER_OF_YEAR in Z][fixed to midnight]"
        );
        dateAndNanosSpanCase(
            suppliers,
            "with multi-month period",
            "1970-01-01T00:00:00.00Z",
            DataType.DATE_PERIOD,
            Period.ofMonths(6),
            "[MONTHS_OF_YEAR in Z][fixed to midnight]"
        );
        // ESQL parses `N quarters` as Period.ofMonths(N * 3); for N > 1 the multiplier ends up on MONTHS_OF_YEAR.
        // 1972-01-01 is a 9-month boundary from year 1 CE (the reference point used by roundIntervalMonthOfYear).
        dateAndNanosSpanCase(
            suppliers,
            "with multi-quarter period",
            "1972-01-01T00:00:00.00Z",
            DataType.DATE_PERIOD,
            Period.ofMonths(9),
            "[MONTHS_OF_YEAR in Z][fixed to midnight]"
        );
        dateAndNanosSpanCase(
            suppliers,
            "with year period",
            "1970-01-01T00:00:00.00Z",
            DataType.DATE_PERIOD,
            Period.ofYears(1),
            "[YEAR_OF_CENTURY in Z][fixed to midnight]"
        );
        // Multi-year buckets are aligned to year 1 CE (via DateUtils.roundYearInterval), not the epoch, so 5-year buckets
        // start at 1966/1971/1976/... — pick 1971 to land exactly on a boundary.
        dateAndNanosSpanCase(
            suppliers,
            "with multi-year period",
            "1971-01-01T00:00:00.00Z",
            DataType.DATE_PERIOD,
            Period.ofYears(5),
            "[YEARS_OF_CENTURY in Z][fixed to midnight]"
        );
        dateAndNanosSpanCase(
            suppliers,
            "with multi-day duration",
            "1970-01-01T00:00:00.00Z",
            DataType.TIME_DURATION,
            Duration.ofDays(3L),
            "[259200000 in Z][fixed]"
        );
        dateAndNanosSpanCase(
            suppliers,
            "with day duration",
            "1970-01-01T00:00:00.00Z",
            DataType.TIME_DURATION,
            Duration.ofDays(1L),
            "[86400000 in Z][fixed]"
        );
        dateAndNanosSpanCase(
            suppliers,
            "with hour duration",
            "1970-01-01T00:00:00.00Z",
            DataType.TIME_DURATION,
            Duration.ofHours(3L),
            "[10800000 in Z][fixed]"
        );
        dateAndNanosSpanCase(
            suppliers,
            "with minute duration",
            "1970-01-01T00:00:00.00Z",
            DataType.TIME_DURATION,
            Duration.ofMinutes(5L),
            "[300000 in Z][fixed]"
        );
        dateAndNanosSpanCase(
            suppliers,
            "with second duration",
            "1970-01-01T00:00:00.00Z",
            DataType.TIME_DURATION,
            Duration.ofSeconds(10L),
            "[10000 in Z][fixed]"
        );
        dateAndNanosSpanCase(
            suppliers,
            "with millisecond duration",
            "1970-01-01T00:00:00.00Z",
            DataType.TIME_DURATION,
            Duration.ofMillis(13),
            "[13 in Z][fixed]"
        );
        dateTruncCases(suppliers);
        numberCases(suppliers, "fixed long", DataType.LONG, () -> 100L);
        numberCasesWithSpan(suppliers, "fixed long with span", DataType.LONG, () -> 100L);
        numberCases(suppliers, "fixed int", DataType.INTEGER, () -> 100);
        numberCasesWithSpan(suppliers, "fixed int with span", DataType.INTEGER, () -> 100);
        numberCases(suppliers, "fixed double", DataType.DOUBLE, () -> 100.0);
        numberCasesWithSpan(suppliers, "fixed double with span", DataType.DOUBLE, () -> 100.);
        return parameterSuppliersFromTypedData(
            anyNullIsNull(
                suppliers,
                (nullPosition, nullValueDataType, original) -> nullPosition == 0 && nullValueDataType == DataType.NULL
                    ? DataType.NULL
                    : original.expectedType(),
                (nullPosition, nullData, original) -> nullPosition == 0 ? original : equalTo("LiteralsEvaluator[lit=null]")
            )
        );
    }

    // TODO once we cast above the functions we can drop these
    private static final DataType[] DATE_BOUNDS_TYPE = new DataType[] { DataType.DATETIME, DataType.KEYWORD, DataType.TEXT };

    private static void dateCases(List<TestCaseSupplier> suppliers, String name, LongSupplier date) {
        for (DataType fromType : DATE_BOUNDS_TYPE) {
            for (DataType toType : DATE_BOUNDS_TYPE) {
                suppliers.add(new TestCaseSupplier(name, List.of(DataType.DATETIME, DataType.INTEGER, fromType, toType), () -> {
                    List<TestCaseSupplier.TypedData> args = new ArrayList<>();
                    args.add(new TestCaseSupplier.TypedData(date.getAsLong(), DataType.DATETIME, "field"));
                    // TODO more "from" and "to" and "buckets"
                    args.add(new TestCaseSupplier.TypedData(50, DataType.INTEGER, "buckets").forceLiteral());
                    args.add(dateBound("from", fromType, "2023-02-01T00:00:00.00Z"));
                    args.add(dateBound("to", toType, "2023-03-01T09:00:00.00Z"));
                    return new TestCaseSupplier.TestCase(
                        args,
                        "DateTruncDatetimeEvaluator[fieldVal=Attribute[channel=0], "
                            + "rounding=Rounding[DAY_OF_MONTH in Z][fixed to midnight]]",
                        DataType.DATETIME,
                        resultsMatcher(args)
                    ).withConfiguration(TEST_SOURCE, configurationForTimezone(ZoneOffset.UTC)).withExtra(META_DAY_1);
                }));
                // same as above, but a low bucket count and datetime bounds that match it (at hour span)
                suppliers.add(new TestCaseSupplier(name, List.of(DataType.DATETIME, DataType.INTEGER, fromType, toType), () -> {
                    List<TestCaseSupplier.TypedData> args = new ArrayList<>();
                    args.add(new TestCaseSupplier.TypedData(date.getAsLong(), DataType.DATETIME, "field"));
                    args.add(new TestCaseSupplier.TypedData(4, DataType.INTEGER, "buckets").forceLiteral());
                    args.add(dateBound("from", fromType, "2023-02-17T09:00:00Z"));
                    args.add(dateBound("to", toType, "2023-02-17T12:00:00Z"));
                    return new TestCaseSupplier.TestCase(
                        args,
                        "DateTruncDatetimeEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding[3600000 in Z][fixed]]",
                        DataType.DATETIME,
                        equalTo(Rounding.builder(Rounding.DateTimeUnit.HOUR_OF_DAY).build().prepareForUnknown().round(date.getAsLong()))
                    ).withConfiguration(TEST_SOURCE, configurationForTimezone(ZoneOffset.UTC)).withExtra(META_HOUR_1);
                }));
            }
        }
    }

    private static void dateRoundingHandlesNonNestedCalendarUnits(List<TestCaseSupplier> suppliers) {
        suppliers.add(
            new TestCaseSupplier(
                "month boundary can still be weekly",
                List.of(DataType.DATETIME, DataType.INTEGER, DataType.DATETIME, DataType.DATETIME),
                () -> {
                    long date = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2024-02-01T12:00:00Z");
                    List<TestCaseSupplier.TypedData> args = new ArrayList<>();
                    args.add(new TestCaseSupplier.TypedData(date, DataType.DATETIME, "field"));
                    args.add(new TestCaseSupplier.TypedData(1, DataType.INTEGER, "buckets").forceLiteral());
                    args.add(dateBound("from", DataType.DATETIME, "2024-01-31T00:00:00Z"));
                    args.add(dateBound("to", DataType.DATETIME, "2024-02-02T00:00:00Z"));
                    return new TestCaseSupplier.TestCase(
                        args,
                        Matchers.containsString("rounding=Rounding[WEEK_OF_WEEKYEAR in Z][fixed to midnight]"),
                        DataType.DATETIME,
                        equalTo(Rounding.builder(Rounding.DateTimeUnit.WEEK_OF_WEEKYEAR).build().prepareForUnknown().round(date))
                    ).withConfiguration(TEST_SOURCE, configurationForTimezone(ZoneOffset.UTC)).withExtra(META_WEEK_1);
                }
            )
        );

        suppliers.add(
            new TestCaseSupplier(
                "week boundary can still be monthly",
                List.of(DataType.DATETIME, DataType.INTEGER, DataType.DATETIME, DataType.DATETIME),
                () -> {
                    long date = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2024-01-08T12:00:00Z");
                    List<TestCaseSupplier.TypedData> args = new ArrayList<>();
                    args.add(new TestCaseSupplier.TypedData(date, DataType.DATETIME, "field"));
                    args.add(new TestCaseSupplier.TypedData(1, DataType.INTEGER, "buckets").forceLiteral());
                    args.add(dateBound("from", DataType.DATETIME, "2024-01-07T00:00:00Z"));
                    args.add(dateBound("to", DataType.DATETIME, "2024-01-09T00:00:00Z"));
                    return new TestCaseSupplier.TestCase(
                        args,
                        Matchers.containsString("rounding=Rounding[MONTH_OF_YEAR in Z][fixed to midnight]"),
                        DataType.DATETIME,
                        equalTo(Rounding.builder(Rounding.DateTimeUnit.MONTH_OF_YEAR).build().prepareForUnknown().round(date))
                    ).withConfiguration(TEST_SOURCE, configurationForTimezone(ZoneOffset.UTC)).withExtra(META_MONTH_1);
                }
            )
        );
    }

    private static void dateTruncCases(List<TestCaseSupplier> suppliers) {
        makeTruncPeriodTestCases().stream().map(data -> {
            List<TestCaseSupplier> caseSuppliers = new ArrayList<>();

            caseSuppliers.add(
                new TestCaseSupplier(
                    data.testCaseNameForMillis(),
                    List.of(DataType.DATETIME, DataType.DATE_PERIOD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(data.inputDateAsMillis(), DataType.DATETIME, "field"),
                            new TestCaseSupplier.TypedData(data.period(), DataType.DATE_PERIOD, "interval").forceLiteral()
                        ),
                        DATETIME_TRUNC_EVAL_PREFIX,
                        DataType.DATETIME,
                        matchesDateMillis(data.expectedDate())
                    ).withConfiguration(TEST_SOURCE, configurationForTimezone(data.zoneId()))
                        .withExtra(expectedDateMetadataForSpan(data.period()))
                )
            );

            if (data.canBeConvertedToNanos()) {
                caseSuppliers.add(
                    new TestCaseSupplier(
                        data.testCaseNameForNanos(),
                        List.of(DataType.DATE_NANOS, DataType.DATE_PERIOD),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(data.inputDateAsNanos(), DataType.DATE_NANOS, "field"),
                                new TestCaseSupplier.TypedData(data.period(), DataType.DATE_PERIOD, "interval").forceLiteral()
                            ),
                            DATE_NANOS_TRUNC_EVAL_PREFIX,
                            DataType.DATE_NANOS,
                            matchesDateNanos(data.expectedDate())
                        ).withConfiguration(TEST_SOURCE, configurationForTimezone(data.zoneId()))
                            .withExtra(expectedDateMetadataForSpan(data.period()))
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
                    List.of(DataType.DATETIME, DataType.TIME_DURATION),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(data.inputDateAsMillis(), DataType.DATETIME, "field"),
                            new TestCaseSupplier.TypedData(data.duration(), DataType.TIME_DURATION, "interval").forceLiteral()
                        ),
                        DATETIME_TRUNC_EVAL_PREFIX,
                        DataType.DATETIME,
                        matchesDateMillis(data.expectedDate())
                    ).withConfiguration(TEST_SOURCE, configurationForTimezone(data.zoneId()))
                        .withExtra(expectedDateMetadataForSpan(data.duration()))
                )
            );

            if (data.canBeConvertedToNanos()) {
                caseSuppliers.add(
                    new TestCaseSupplier(
                        data.testCaseNameForNanos(),
                        List.of(DataType.DATE_NANOS, DataType.TIME_DURATION),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(data.inputDateAsNanos(), DataType.DATE_NANOS, "field"),
                                new TestCaseSupplier.TypedData(data.duration(), DataType.TIME_DURATION, "interval").forceLiteral()
                            ),
                            DATE_NANOS_TRUNC_EVAL_PREFIX,
                            DataType.DATE_NANOS,
                            matchesDateNanos(data.expectedDate())
                        ).withConfiguration(TEST_SOURCE, configurationForTimezone(data.zoneId()))
                            .withExtra(expectedDateMetadataForSpan(data.duration()))
                    )
                );
            }

            return caseSuppliers;
        }).forEach(suppliers::addAll);
    }

    /**
     * Returns the metadata that {@link Bucket#getIntervalMetadata} should produce for a span argument
     * (a {@link Period} or {@link Duration}). Mirrors the canonical-unit selection performed by
     * {@link Rounding#getInterval()} so each test case can assert exact interval/unit values without
     * round-tripping through the production logic under test.
     */
    private static Map<String, Object> expectedDateMetadataForSpan(Object span) {
        if (span instanceof Period p) {
            // Logic mirrors DateTrunc.createRounding for Period.
            if (p.getDays() == 1) return META_DAY_1;
            if (p.getDays() == 7) return META_WEEK_1;
            if (p.getDays() > 1) return Map.of("bucket", Map.of("interval", (long) p.getDays(), "unit", "day"));
            if (p.getMonths() == 1) return META_MONTH_1;
            if (p.getMonths() == 3) return META_QUARTER_1;
            if (p.getMonths() > 0) return Map.of("bucket", Map.of("interval", (long) p.getMonths(), "unit", "month"));
            if (p.getYears() == 1) return META_YEAR_1;
            if (p.getYears() > 0) return Map.of("bucket", Map.of("interval", (long) p.getYears(), "unit", "year"));
            throw new AssertionError("Unsupported period: " + p);
        }
        if (span instanceof Duration d) {
            // TimeValue picks the largest exact unit when stringifying; we mirror that here.
            long ms = d.toMillis();
            if (ms % (24L * 3600_000L) == 0) {
                long n = ms / (24L * 3600_000L);
                return n == 1 ? META_DAY_1 : Map.of("bucket", Map.of("interval", n, "unit", "day"));
            }
            if (ms % 3600_000L == 0) {
                long n = ms / 3600_000L;
                return n == 1 ? META_HOUR_1 : Map.of("bucket", Map.of("interval", n, "unit", "hour"));
            }
            if (ms % 60_000L == 0) return Map.of("bucket", Map.of("interval", ms / 60_000L, "unit", "minute"));
            if (ms % 1000L == 0) return Map.of("bucket", Map.of("interval", ms / 1000L, "unit", "second"));
            return Map.of("bucket", Map.of("interval", ms, "unit", "millisecond"));
        }
        throw new IllegalArgumentException("Unsupported span: " + span);
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
        suppliers.add(new TestCaseSupplier(name, List.of(DataType.DATETIME, spanType), () -> {
            List<TestCaseSupplier.TypedData> args = new ArrayList<>();
            args.add(new TestCaseSupplier.TypedData(date.getAsLong(), DataType.DATETIME, "field"));
            args.add(new TestCaseSupplier.TypedData(span, spanType, "buckets").forceLiteral());
            return new TestCaseSupplier.TestCase(
                args,
                "DateTruncDatetimeEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding" + spanStr + "]",
                DataType.DATETIME,
                resultsMatcher(args)
            ).withConfiguration(TEST_SOURCE, configurationForTimezone(ZoneOffset.UTC)).withExtra(expectedDateMetadataForSpan(span));
        }));
    }

    /**
     * Registers a BUCKET test case for both DATETIME and DATE_NANOS using the same date string, span, and
     * Rounding-toString suffix. Used to mirror every span shape (calendar units, multi-N intervals, durations) onto
     * both date types so {@link #parameters} covers each branch of {@link #expectedDateMetadataForSpan}.
     */
    private static void dateAndNanosSpanCase(
        List<TestCaseSupplier> suppliers,
        String description,
        String dateString,
        DataType spanType,
        Object span,
        String spanStr
    ) {
        dateCasesWithSpan(
            suppliers,
            "fixed date " + description,
            () -> DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(dateString),
            spanType,
            span,
            spanStr
        );
        dateNanosCasesWithSpan(
            suppliers,
            "fixed date nanos " + description,
            () -> DateUtils.toLong(Instant.parse(dateString)),
            spanType,
            span,
            spanStr
        );
    }

    private static void dateNanosCasesWithSpan(
        List<TestCaseSupplier> suppliers,
        String name,
        LongSupplier date,
        DataType spanType,
        Object span,
        String spanStr
    ) {
        suppliers.add(new TestCaseSupplier(name, List.of(DataType.DATE_NANOS, spanType), () -> {
            List<TestCaseSupplier.TypedData> args = new ArrayList<>();
            args.add(new TestCaseSupplier.TypedData(date.getAsLong(), DataType.DATE_NANOS, "field"));
            args.add(new TestCaseSupplier.TypedData(span, spanType, "buckets").forceLiteral());
            return new TestCaseSupplier.TestCase(
                args,
                "DateTruncDateNanosEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding" + spanStr + "]",
                DataType.DATE_NANOS,
                resultsMatcher(args)
            ).withConfiguration(TEST_SOURCE, configurationForTimezone(ZoneOffset.UTC)).withExtra(expectedDateMetadataForSpan(span));
        }));
    }

    private static void dateNanosCases(List<TestCaseSupplier> suppliers, String name, LongSupplier date) {
        for (DataType fromType : DATE_BOUNDS_TYPE) {
            for (DataType toType : DATE_BOUNDS_TYPE) {
                suppliers.add(new TestCaseSupplier(name, List.of(DataType.DATE_NANOS, DataType.INTEGER, fromType, toType), () -> {
                    List<TestCaseSupplier.TypedData> args = new ArrayList<>();
                    args.add(new TestCaseSupplier.TypedData(date.getAsLong(), DataType.DATE_NANOS, "field"));
                    // TODO more "from" and "to" and "buckets"
                    args.add(new TestCaseSupplier.TypedData(50, DataType.INTEGER, "buckets").forceLiteral());
                    args.add(dateBound("from", fromType, "2023-02-01T00:00:00.00Z"));
                    args.add(dateBound("to", toType, "2023-03-01T09:00:00.00Z"));
                    return new TestCaseSupplier.TestCase(
                        args,
                        "DateTruncDateNanosEvaluator[fieldVal=Attribute[channel=0], "
                            + "rounding=Rounding[DAY_OF_MONTH in Z][fixed to midnight]]",
                        DataType.DATE_NANOS,
                        resultsMatcher(args)
                    ).withConfiguration(TEST_SOURCE, configurationForTimezone(ZoneOffset.UTC)).withExtra(META_DAY_1);
                }));
                // same as above, but a low bucket count and datetime bounds that match it (at hour span)
                suppliers.add(new TestCaseSupplier(name, List.of(DataType.DATE_NANOS, DataType.INTEGER, fromType, toType), () -> {
                    List<TestCaseSupplier.TypedData> args = new ArrayList<>();
                    args.add(new TestCaseSupplier.TypedData(date.getAsLong(), DataType.DATE_NANOS, "field"));
                    args.add(new TestCaseSupplier.TypedData(4, DataType.INTEGER, "buckets").forceLiteral());
                    args.add(dateBound("from", fromType, "2023-02-17T09:00:00Z"));
                    args.add(dateBound("to", toType, "2023-02-17T12:00:00Z"));
                    return new TestCaseSupplier.TestCase(
                        args,
                        "DateTruncDateNanosEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding[3600000 in Z][fixed]]",
                        DataType.DATE_NANOS,
                        equalTo(Rounding.builder(Rounding.DateTimeUnit.HOUR_OF_DAY).build().prepareForUnknown().round(date.getAsLong()))
                    ).withConfiguration(TEST_SOURCE, configurationForTimezone(ZoneOffset.UTC)).withExtra(META_HOUR_1);
                }));
            }
        }
    }

    private static final DataType[] NUMBER_BOUNDS_TYPES = new DataType[] { DataType.INTEGER, DataType.LONG, DataType.DOUBLE };

    private static void numberCases(List<TestCaseSupplier> suppliers, String name, DataType numberType, Supplier<Number> number) {
        for (DataType fromType : NUMBER_BOUNDS_TYPES) {
            for (DataType toType : NUMBER_BOUNDS_TYPES) {
                suppliers.add(new TestCaseSupplier(name, List.of(numberType, DataType.INTEGER, fromType, toType), () -> {
                    List<TestCaseSupplier.TypedData> args = new ArrayList<>();
                    args.add(new TestCaseSupplier.TypedData(number.get(), "field"));
                    // TODO more "from" and "to" and "buckets"
                    args.add(new TestCaseSupplier.TypedData(50, DataType.INTEGER, "buckets").forceLiteral());
                    args.add(numericBound("from", fromType, 0.0));
                    args.add(numericBound("to", toType, 1000.0));
                    // TODO more number types for "from" and "to"
                    String attr = "Attribute[channel=0]";
                    if (numberType == DataType.INTEGER) {
                        attr = "CastIntToDoubleEvaluator[v=" + attr + "]";
                    } else if (numberType == DataType.LONG) {
                        attr = "CastLongToDoubleEvaluator[v=" + attr + "]";
                    }
                    return new TestCaseSupplier.TestCase(
                        args,
                        "MulDoublesEvaluator[lhs=FloorDoubleEvaluator[val=DivDoublesEvaluator[lhs="
                            + attr
                            + ", "
                            + "rhs=LiteralsEvaluator[lit=50.0]]], rhs=LiteralsEvaluator[lit=50.0]]",
                        DataType.DOUBLE,
                        resultsMatcher(args)
                    ).withExtra(META_NUMERIC_50);
                }));
            }
        }
    }

    private static TestCaseSupplier.TypedData numericBound(String name, DataType type, double value) {
        Number v;
        if (type == DataType.INTEGER) {
            v = (int) value;
        } else if (type == DataType.LONG) {
            v = (long) value;
        } else {
            v = value;
        }
        return new TestCaseSupplier.TypedData(v, type, name).forceLiteral();
    }

    private static void numberCasesWithSpan(List<TestCaseSupplier> suppliers, String name, DataType numberType, Supplier<Number> number) {
        for (Number span : List.of(50, 50L, 50d)) {
            DataType spanType = DataType.fromJava(span);
            suppliers.add(new TestCaseSupplier(name, List.of(numberType, spanType), () -> {
                List<TestCaseSupplier.TypedData> args = new ArrayList<>();
                args.add(new TestCaseSupplier.TypedData(number.get(), "field"));
                args.add(new TestCaseSupplier.TypedData(span, spanType, "span").forceLiteral());
                String attr = "Attribute[channel=0]";
                if (numberType == DataType.INTEGER) {
                    attr = "CastIntToDoubleEvaluator[v=" + attr + "]";
                } else if (numberType == DataType.LONG) {
                    attr = "CastLongToDoubleEvaluator[v=" + attr + "]";
                }
                return new TestCaseSupplier.TestCase(
                    args,
                    "MulDoublesEvaluator[lhs=FloorDoubleEvaluator[val=DivDoublesEvaluator[lhs="
                        + attr
                        + ", "
                        + "rhs=LiteralsEvaluator[lit=50.0]]], rhs=LiteralsEvaluator[lit=50.0]]",
                    DataType.DOUBLE,
                    resultsMatcher(args)
                ).withExtra(META_NUMERIC_50);
            }));
        }

    }

    private static TestCaseSupplier.TypedData keywordDateLiteral(String name, DataType type, String date) {
        return new TestCaseSupplier.TypedData(date, type, name).forceLiteral();
    }

    private static Matcher<Object> resultsMatcher(List<TestCaseSupplier.TypedData> typedData) {
        if (typedData.get(0).type() == DataType.DATETIME) {
            long millis = ((Number) typedData.get(0).data()).longValue();
            long expected = Rounding.builder(Rounding.DateTimeUnit.DAY_OF_MONTH).build().prepareForUnknown().round(millis);
            LogManager.getLogger(getTestClass()).info("Expected: " + Instant.ofEpochMilli(expected));
            LogManager.getLogger(getTestClass()).info("Input: " + Instant.ofEpochMilli(millis));
            return equalTo(expected);
        }
        if (typedData.get(0).type() == DataType.DATE_NANOS) {
            long nanos = ((Number) typedData.get(0).data()).longValue();
            long expected = DateUtils.toNanoSeconds(
                Rounding.builder(Rounding.DateTimeUnit.DAY_OF_MONTH).build().prepareForUnknown().round(DateUtils.toMilliSeconds(nanos))
            );
            LogManager.getLogger(getTestClass()).info("Expected: " + DateUtils.toInstant(expected));
            LogManager.getLogger(getTestClass()).info("Input: " + DateUtils.toInstant(nanos));
            return equalTo(expected);
        }
        return equalTo(((Number) typedData.get(0).data()).doubleValue());
    }

    @Override
    protected Expression buildWithConfiguration(Source source, List<Expression> args, Configuration configuration) {
        Expression from = null;
        Expression to = null;
        if (args.size() > 2) {
            from = args.get(2);
            to = args.get(3);
        }
        return new Bucket(source, args.get(0), args.get(1), from, to, configuration);
    }

    /**
     * Verifies that {@link Bucket#getIntervalMetadata} returns the metadata embedded as
     * {@link TestCaseSupplier.TestCase#extra()} for each parametrized case. Every supplier is responsible for
     * declaring the expected interval/unit, so adding a new field type or argument shape to BUCKET requires
     * declaring its expected metadata here too — the metadata path can't drift from the function's supported types.
     * <p>
     * In addition to the exact-match assertion, structural checks pin the shape of the metadata map per field type
     * so a new BUCKET-supported type that produces malformed metadata trips this test even before the per-case
     * expected value can be set.
     */
    public void testGetIntervalMetadata() {
        Expression expr = buildFieldExpression(testCase);
        assertThat(expr, instanceOf(Bucket.class));
        Bucket bucket = (Bucket) expr;

        TestCaseSupplier.TypedData fieldData = testCase.getData().get(0);
        if (fieldData.data() == null && fieldData.type() != DataType.NULL) {
            // anyNullIsNull's first flavor sets data=null at position 0 but keeps the original field type.
            // Because the field is built as an Attribute (not a Literal), null data never reaches the
            // metadata path; this case doesn't represent a null-arg-to-BUCKET scenario (production folds a
            // null field literal at the optimizer instead). Skip rather than assert.
            return;
        }

        if (testCase.getData().stream().anyMatch(d -> d.data() == null)) {
            // Null literal in a forceLiteral argument (buckets/from/to) or a NULL-typed field. In production
            // the optimizer folds null inputs before metadata extraction (verified end-to-end in
            // BucketColumnMetadataIT), so getIntervalMetadata is never invoked with null fold values in real
            // queries. Direct invocation surfaces no metadata — null return or null-cast NPE both qualify.
            Map<String, Object> metadata = null;
            try {
                metadata = bucket.getIntervalMetadata(FoldContext.small());
            } catch (RuntimeException | AssertionError ignored) {
                // Expected for null fold values in buckets/from/to.
            }
            assertThat(metadata, nullValue());
            return;
        }

        Map<String, Object> metadata = bucket.getIntervalMetadata(FoldContext.small());

        assertThat(metadata, notNullValue());
        assertThat(metadata.keySet(), contains("bucket"));

        @SuppressWarnings("unchecked")
        Map<String, Object> inner = (Map<String, Object>) metadata.get("bucket");
        assertThat(inner, hasKey("interval"));

        DataType fieldType = testCase.getData().get(0).type();
        if (fieldType == DataType.DATETIME || fieldType == DataType.DATE_NANOS) {
            assertThat(inner.get("interval"), instanceOf(Long.class));
            assertThat(inner, hasKey("unit"));
            assertThat(inner.get("unit"), instanceOf(String.class));
        } else if (fieldType.isNumeric()) {
            assertThat(inner.get("interval"), instanceOf(Double.class));
            assertThat(inner.containsKey("unit"), equalTo(false));
        } else {
            fail("BUCKET supports field type [" + fieldType + "] but getIntervalMetadata has no branch for it");
        }

        Object expected = testCase.extra();
        assertThat("test case is missing expected metadata in withExtra(...)", expected, instanceOf(Map.class));
        assertThat(metadata, equalTo(expected));
    }

    /**
     * In Elasticsearch, we think of these parameters are optional because you don't
     * have to supply them. But you have to supply them in some cases. It depends on
     * the signatures. And when we're rendering the signatures for kibana it's more
     * correct to say that all parameters are required. They'll render like
     * {@snippet lang="markdown" :
     * | field    | buckets         | from     | to       | result   |
     * | -------- | --------------- | -------- | -------- | -------- |
     * | `date`   | `date_period`   |          |          | `date`   |
     * | `date`   | `time_duration` |          |          | `date`   |
     * | `date`   | `integer`       | `date`   | `date`   | `date`   |
     * | `double` | `double`        |          |          | `double` |
     * | `double` | `integer`       | `double` | `double` | `double` |
     * ...
     * }
     * And all of those listed versions *are* required.
     */
    public static EsqlFunctionRegistry.ArgSignature patchKibanaSignature(EsqlFunctionRegistry.ArgSignature arg) {
        return new EsqlFunctionRegistry.ArgSignature(
            arg.name(),
            arg.type(),
            arg.description(),
            false,
            arg.variadic(),
            arg.targetDataType()
        );
    }
}
