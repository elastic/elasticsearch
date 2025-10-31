/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractConfigurationFunctionTestCase;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Matchers;

import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.TEST_SOURCE;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.DEFAULT_DATE_TIME_FORMATTER;
import static org.hamcrest.Matchers.equalTo;

/**
 * Parameterized testing for {@link DateTrunc}.  See also {@link DateTruncRoundingTests} for non-parametrized tests.
 */
public class DateTruncTests extends AbstractConfigurationFunctionTestCase {

    public DateTruncTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        makeTruncPeriodTestCases().stream().map(DateTruncTests::ofDatePeriod).forEach(suppliers::addAll);
        makeTruncDurationTestCases().stream().map(DateTruncTests::ofDuration).forEach(suppliers::addAll);

        suppliers.add(randomSecond());

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    public record PeriodTestCaseData(Period period, String inputDate, @Nullable String zoneIdString, String expectedDate) {
        public ZoneId zoneId() {
            return zoneIdString == null ? randomZone() : ZoneId.of(zoneIdString);
        }

        public long inputDateAsMillis() {
            return Instant.parse(inputDate).toEpochMilli();
        }

        public long expectedDateAsMillis() {
            return Instant.parse(expectedDate).toEpochMilli();
        }
    }

    public record DurationTestCaseData(Duration duration, String inputDate, @Nullable String zoneIdString, String expectedDate) {
        public ZoneId zoneId() {
            return zoneIdString == null ? randomZone() : ZoneId.of(zoneIdString);
        }

        public long inputDateAsMillis() {
            return Instant.parse(inputDate).toEpochMilli();
        }

        public long expectedDateAsMillis() {
            return Instant.parse(expectedDate).toEpochMilli();
        }
    }

    public static List<DurationTestCaseData> makeTruncDurationTestCases() {
        String ts = "2023-02-17T10:25:33.38Z";
        return List.of(
            ///
            /// Timezone agnostic (<2h intervals)
            ///
            new DurationTestCaseData(Duration.ofMillis(100), ts, null, "2023-02-17T10:25:33.30Z"),
            new DurationTestCaseData(Duration.ofSeconds(1), ts, null, "2023-02-17T10:25:33Z"),
            new DurationTestCaseData(Duration.ofMinutes(1), ts, null, "2023-02-17T10:25:00Z"),
            new DurationTestCaseData(Duration.ofHours(1), ts, null, "2023-02-17T10:00:00Z"),
            new DurationTestCaseData(Duration.ofSeconds(30), ts, null, "2023-02-17T10:25:30Z"),
            new DurationTestCaseData(Duration.ofMinutes(15), ts, null, "2023-02-17T10:15:00Z"),

            ///
            /// Timezone dependent (>=2h intervals)
            ///
            new DurationTestCaseData(Duration.ofHours(3), ts, "UTC", "2023-02-17T09:00:00Z"),
            new DurationTestCaseData(Duration.ofHours(3), ts, "-02:00", "2023-02-17T08:00:00Z"),
            new DurationTestCaseData(Duration.ofHours(3), "2020-01-01T05:30:00Z", "UTC", "2020-01-01T03:00:00Z"),
            new DurationTestCaseData(Duration.ofHours(3), "2020-01-01T05:30:00Z", "+01:00", "2020-01-01T05:00:00Z"),
            new DurationTestCaseData(Duration.ofMinutes(3 * 60), "2020-01-01T05:30:00Z", "+01:00", "2020-01-01T05:00:00Z"),

            // TODO: What to do with hours/minutes/seconds that are not divisors of a full day?
            //new DurationTestCaseData(Duration.ofHours(5), "2020-01-01T05:30:00Z", "+01", "2020-01-01T05:00:00Z"),

            ///
            /// Daylight savings
            ///
            //- +1 -> +2 at 2025-03-30T02:00:00+01:00
            new DurationTestCaseData(Duration.ofHours(3), "2025-03-30T00:00:00+01:00", "Europe/Paris", "2025-03-30T00:00:00+01:00"),
            new DurationTestCaseData(Duration.ofHours(3), "2025-03-30T01:00:00+01:00", "Europe/Paris", "2025-03-30T00:00:00+01:00"),
            new DurationTestCaseData(Duration.ofHours(3), "2025-03-30T03:00:00+02:00", "Europe/Paris", "2025-03-30T03:00:00+02:00"),
            new DurationTestCaseData(Duration.ofHours(3), "2025-03-30T04:00:00+02:00", "Europe/Paris", "2025-03-30T03:00:00+02:00"),
            // - +2 -> +1 at 2025-10-26T03:00:00+02:00
            new DurationTestCaseData(Duration.ofHours(3), "2025-10-26T01:00:00+02:00", "Europe/Paris", "2025-10-26T00:00:00+02:00"),
            new DurationTestCaseData(Duration.ofHours(3), "2025-10-26T02:00:00+02:00", "Europe/Paris", "2025-10-26T00:00:00+02:00"),
            new DurationTestCaseData(Duration.ofHours(3), "2025-10-26T02:00:00+01:00", "Europe/Paris", "2025-10-26T00:00:00+02:00"),
            new DurationTestCaseData(Duration.ofHours(3), "2025-10-26T03:00:00+01:00", "Europe/Paris", "2025-10-26T03:00:00+01:00"),
            new DurationTestCaseData(Duration.ofHours(3), "2025-10-26T04:00:00+01:00", "Europe/Paris", "2025-10-26T03:00:00+01:00"),
            // Bigger intervals
            new DurationTestCaseData(Duration.ofHours(12), "2025-10-26T02:00:00+02:00", "Europe/Rome", "2025-10-26T00:00:00+02:00"),
            new DurationTestCaseData(Duration.ofHours(24), "2025-10-26T02:00:00+02:00", "Europe/Rome", "2025-10-26T00:00:00+02:00"),
            // TODO: What to do with hours over a day?
            // new DurationTestCaseData(Duration.ofHours(48), "2025-10-26T02:00:00+02:00", "Europe/Rome", "2025-10-26T00:00:00+02:00")

            ///
            /// Partial hours timezones
            ///
            new DurationTestCaseData(Duration.ofMinutes(1), "2025-10-26T02:09:09+01:15", "+01:15", "2025-10-26T02:09:00+01:15"),
            new DurationTestCaseData(Duration.ofMinutes(30), "2025-10-26T02:09:09+01:15", "+01:15", "2025-10-26T02:00:00+01:15"),
            new DurationTestCaseData(Duration.ofHours(1), "2025-10-26T02:09:09+01:15", "+01:15", "2025-10-26T02:00:00+01:15"),
        new DurationTestCaseData(Duration.ofHours(3), "2025-10-26T02:09:09+01:15", "+01:15", "2025-10-26T00:00:00+01:15"),
            new DurationTestCaseData(Duration.ofHours(24), "2025-10-26T02:09:09+01:15", "+01:15", "2025-10-26T00:00:00+01:15")
        );
    }

    public static List<PeriodTestCaseData> makeTruncPeriodTestCases() {
        String ts = "2023-02-17T10:25:33.38Z";
        return List.of(
            ///
            /// UTC
            ///
            new PeriodTestCaseData(Period.ofDays(1), ts, "UTC", "2023-02-17T00:00:00.00Z"),
            new PeriodTestCaseData(Period.ofMonths(1), ts, "UTC", "2023-02-01T00:00:00.00Z"),
            new PeriodTestCaseData(Period.ofYears(1), ts, "UTC", "2023-01-01T00:00:00.00Z"),
            new PeriodTestCaseData(Period.ofDays(10), ts, "UTC", "2023-02-12T00:00:00.00Z"),
            // 7 days period should return weekly rounding
            new PeriodTestCaseData(Period.ofDays(7), ts, "UTC", "2023-02-13T00:00:00.00Z"),
            // 3 months period should return quarterly
            new PeriodTestCaseData(Period.ofMonths(3), ts, "UTC", "2023-01-01T00:00:00.00Z"),
            // arbitrary period of months and years
            new PeriodTestCaseData(Period.ofMonths(7), ts, "UTC", "2022-11-01T00:00:00.00Z"),
            new PeriodTestCaseData(Period.ofYears(5), ts, "UTC", "2021-01-01T00:00:00.00Z"),

            ///
            /// Timezone with DST (-5 to -4 at 2025-03-09T02:00:00-05, and -4 to -5 at 2025-11-02T02:00:00-04)
            ///
            new PeriodTestCaseData(Period.ofDays(1), "2025-03-09T06:00:00-04:00", "America/New_York", "2025-03-09T00:00:00-05:00"),
            new PeriodTestCaseData(Period.ofMonths(1), "2025-03-09T06:00:00-04:00", "America/New_York", "2025-03-01T00:00:00-05:00"),
            new PeriodTestCaseData(Period.ofYears(1), "2025-03-09T06:00:00-04:00", "America/New_York", "2025-01-01T00:00:00-05:00"),
            new PeriodTestCaseData(Period.ofDays(10), "2025-03-09T06:00:00-04:00", "America/New_York", "2025-03-03T00:00:00-05:00"),
            new PeriodTestCaseData(Period.ofDays(1), "2025-11-02T05:00:00-05:00", "America/New_York", "2025-11-02T00:00:00-04:00"),
            new PeriodTestCaseData(Period.ofDays(7), "2025-11-02T05:00:00-05:00", "America/New_York", "2025-10-27T00:00:00-04:00"),
            new PeriodTestCaseData(Period.ofMonths(3), "2025-11-02T05:00:00-05:00", "America/New_York", "2025-10-01T00:00:00-04:00"),

            ///
            /// Partial hours timezones
            ///
            new PeriodTestCaseData(Period.ofDays(1), "2025-03-09T07:08:09-05:45", "-05:45", "2025-03-09T00:00:00-05:45"),
            new PeriodTestCaseData(Period.ofMonths(1), "2025-03-09T07:08:09-05:45", "-05:45", "2025-03-01T00:00:00-05:45"),
            new PeriodTestCaseData(Period.ofYears(1), "2025-03-09T07:08:09-05:45", "-05:45", "2025-01-01T00:00:00-05:45"),
            new PeriodTestCaseData(Period.ofDays(10), "2025-03-09T07:08:09-05:45", "-05:45", "2025-03-03T00:00:00-05:45"),
            new PeriodTestCaseData(Period.ofDays(1), "2025-03-09T07:08:09-05:45", "-05:45", "2025-03-09T00:00:00-05:45"),
            new PeriodTestCaseData(Period.ofDays(7), "2025-03-09T07:08:09-05:45", "-05:45", "2025-03-03T00:00:00-05:45"),
            new PeriodTestCaseData(Period.ofMonths(3), "2025-03-09T07:08:09-05:45", "-05:45", "2025-01-01T00:00:00-05:45")
        );
    }

    private static List<TestCaseSupplier> ofDatePeriod(PeriodTestCaseData data) {
        return List.of(
            new TestCaseSupplier(
                "period, millis; " + data.period() + ", " + data.zoneIdString() + ", " + data.inputDate(),
                List.of(DataType.DATE_PERIOD, DataType.DATETIME),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(data.period(), DataType.DATE_PERIOD, "interval").forceLiteral(),
                        new TestCaseSupplier.TypedData(data.inputDateAsMillis(), DataType.DATETIME, "date")
                    ),
                    Matchers.startsWith("DateTruncDatetimeEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding["),
                    DataType.DATETIME,
                    new DateMillisMatcher(data.expectedDate()) // equalTo(data.expectedDateAsMillis())
                ).withConfiguration(TEST_SOURCE, configurationForTimezone(data.zoneId()))
            ),
            new TestCaseSupplier(
                "period, nanos; " + data.period() + ", " + data.zoneIdString() + ", " + data.inputDate(),
                List.of(DataType.DATE_PERIOD, DataType.DATE_NANOS),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(data.period(), DataType.DATE_PERIOD, "interval").forceLiteral(),
                        new TestCaseSupplier.TypedData(DateUtils.toNanoSeconds(data.inputDateAsMillis()), DataType.DATE_NANOS, "date")
                    ),
                    Matchers.startsWith("DateTruncDateNanosEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding["),
                    DataType.DATE_NANOS,
                    new DateNanosMatcher(data.expectedDate()) // equalTo(toNanos(data.expectedDate()))
                ).withConfiguration(TEST_SOURCE, configurationForTimezone(data.zoneId()))
            )
        );
    }

    private static List<TestCaseSupplier> ofDuration(DurationTestCaseData data) {
        return List.of(
            new TestCaseSupplier(
                "period, millis; " + data.duration() + ", " + data.zoneIdString() + ", " + data.inputDate(),
                List.of(DataType.TIME_DURATION, DataType.DATETIME),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(data.duration(), DataType.TIME_DURATION, "interval").forceLiteral(),
                        new TestCaseSupplier.TypedData(data.inputDateAsMillis(), DataType.DATETIME, "date")
                    ),
                    Matchers.startsWith("DateTruncDatetimeEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding["),
                    DataType.DATETIME,
                    new DateMillisMatcher(data.expectedDate()) // equalTo(data.expectedDateAsMillis())
                ).withConfiguration(TEST_SOURCE, configurationForTimezone(data.zoneId()))
            ),
            new TestCaseSupplier(
                "period, nanos; " + data.duration() + ", " + data.zoneIdString() + ", " + data.inputDate(),
                List.of(DataType.TIME_DURATION, DataType.DATE_NANOS),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(data.duration(), DataType.TIME_DURATION, "interval").forceLiteral(),
                        new TestCaseSupplier.TypedData(DateUtils.toNanoSeconds(data.inputDateAsMillis()), DataType.DATE_NANOS, "date")
                    ),
                    Matchers.startsWith("DateTruncDateNanosEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding["),
                    DataType.DATE_NANOS,
                    new DateNanosMatcher(data.expectedDate()) // equalTo(toNanos(data.expectedDate()))
                ).withConfiguration(TEST_SOURCE, configurationForTimezone(data.zoneId()))
            )
        );
    }

    private static TestCaseSupplier randomSecond() {
        return new TestCaseSupplier("random second", List.of(DataType.TIME_DURATION, DataType.DATETIME), () -> {
            String dateFragment = randomIntBetween(2000, 2050)
                + "-"
                + pad(randomIntBetween(1, 12))
                + "-"
                + pad(randomIntBetween(1, 28))
                + "T"
                + pad(randomIntBetween(0, 23))
                + ":"
                + pad(randomIntBetween(0, 59))
                + ":"
                + pad(randomIntBetween(0, 59));
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(Duration.ofSeconds(1), DataType.TIME_DURATION, "interval").forceLiteral(),
                    new TestCaseSupplier.TypedData(toMillis(dateFragment + ".38Z"), DataType.DATETIME, "date")
                ),
                Matchers.startsWith("DateTruncDatetimeEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding["),
                DataType.DATETIME,
                equalTo(toMillis(dateFragment + ".00Z"))
            );
        });
    }

    private static String pad(int i) {
        return i > 9 ? "" + i : "0" + i;
    }

    private static long toMillis(String timestamp) {
        return Instant.parse(timestamp).toEpochMilli();
    }

    private static long toNanos(String timestamp) {
        return DateUtils.toLong(Instant.parse(timestamp));
    }

    @Override
    protected Expression buildWithConfiguration(Source source, List<Expression> args, Configuration configuration) {
        return new DateTrunc(source, args.get(0), args.get(1), configuration);
    }

    public static class DateMillisMatcher extends BaseMatcher<Long> {
        private final long timeMillis;

        public DateMillisMatcher(String date) {
            this.timeMillis = toMillis(date);
        }

        @Override
        public boolean matches(Object item) {
            return item instanceof Long && timeMillis == (Long) item;
        }

        @Override
        public void describeMismatch(Object item, org.hamcrest.Description description) {
            description.appendText("was ");
            if (item instanceof Long l) {
                description.appendValue(DEFAULT_DATE_TIME_FORMATTER.formatMillis(l));
            } else {
                description.appendValue(item);
            }
        }

        @Override
        public void describeTo(org.hamcrest.Description description) {
            description.appendText(DEFAULT_DATE_TIME_FORMATTER.formatMillis(timeMillis));
        }
    }

    public static class DateNanosMatcher extends BaseMatcher<Long> {
        private final long timeNanos;

        public DateNanosMatcher(String date) {
            this.timeNanos = toNanos(date);
        }

        @Override
        public boolean matches(Object item) {
            return item instanceof Long && timeNanos == (Long) item;
        }

        @Override
        public void describeMismatch(Object item, org.hamcrest.Description description) {
            description.appendText("was ");
            if (item instanceof Long l) {
                description.appendValue(DEFAULT_DATE_TIME_FORMATTER.formatNanos(l));
            } else {
                description.appendValue(item);
            }
        }

        @Override
        public void describeTo(org.hamcrest.Description description) {
            description.appendText(DEFAULT_DATE_TIME_FORMATTER.formatNanos(timeNanos));
        }
    }
}
