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
import org.hamcrest.Matchers;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.elasticsearch.test.ReadableMatchers.matchesDateMillis;
import static org.elasticsearch.test.ReadableMatchers.matchesDateNanos;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.TEST_SOURCE;
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
        public String testCaseNameForMillis() {
            var zoneIdName = zoneIdString == null ? "random" : zoneIdString;
            return "period, millis; " + period + ", " + zoneIdName + ", " + inputDate;
        }

        public String testCaseNameForNanos() {
            var zoneIdName = zoneIdString == null ? "random" : zoneIdString;
            return "period, nanos; " + period + ", " + zoneIdName + ", " + inputDate;
        }

        public ZoneId zoneId() {
            return zoneIdString == null ? randomZone() : ZoneId.of(zoneIdString);
        }

        public long inputDateAsMillis() {
            return Instant.parse(inputDate).toEpochMilli();
        }

        public long inputDateAsNanos() {
            long millis = inputDateAsMillis();
            assumeTrue("Before epoch dates can't be converted to nanos", millis >= 0);
            return DateUtils.toNanoSeconds(inputDateAsMillis());
        }
    }

    public record DurationTestCaseData(Duration duration, String inputDate, @Nullable String zoneIdString, String expectedDate) {
        public String testCaseNameForMillis() {
            var zoneIdName = zoneIdString == null ? "random" : zoneIdString;
            return "duration, millis; " + duration + ", " + zoneIdName + ", " + inputDate;
        }

        public String testCaseNameForNanos() {
            var zoneIdName = zoneIdString == null ? "random" : zoneIdString;
            return "duration, nanos; " + duration + ", " + zoneIdName + ", " + inputDate;
        }

        public ZoneId zoneId() {
            return zoneIdString == null ? randomZone() : ZoneId.of(zoneIdString);
        }

        public long inputDateAsMillis() {
            return Instant.parse(inputDate).toEpochMilli();
        }

        public long inputDateAsNanos() {
            long millis = inputDateAsMillis();
            assumeTrue("Before epoch dates can't be converted to nanos", millis >= 0);
            return DateUtils.toNanoSeconds(inputDateAsMillis());
        }
    }

    public static List<DurationTestCaseData> makeTruncDurationTestCases() {
        String ts = "2023-02-17T10:25:33.38Z";
        return List.of(
            ///
            /// Timezone agnostic (<=1m intervals)
            ///
            new DurationTestCaseData(Duration.ofMillis(100), ts, null, "2023-02-17T10:25:33.30Z"),
            new DurationTestCaseData(Duration.ofSeconds(1), ts, null, "2023-02-17T10:25:33Z"),
            new DurationTestCaseData(Duration.ofMinutes(1), ts, null, "2023-02-17T10:25:00Z"),
            new DurationTestCaseData(Duration.ofSeconds(30), ts, null, "2023-02-17T10:25:30Z"),

            ///
            /// Timezone dependent (>1m intervals)
            ///
            new DurationTestCaseData(Duration.ofHours(1), ts, "UTC", "2023-02-17T10:00:00Z"),
            new DurationTestCaseData(Duration.ofMinutes(15), ts, "UTC", "2023-02-17T10:15:00Z"),
            new DurationTestCaseData(Duration.ofHours(3), ts, "UTC", "2023-02-17T09:00:00Z"),
            new DurationTestCaseData(Duration.ofHours(3), ts, "-02:00", "2023-02-17T08:00:00Z"),
            new DurationTestCaseData(Duration.ofHours(3), "2020-01-01T05:30:00Z", "UTC", "2020-01-01T03:00:00Z"),
            new DurationTestCaseData(Duration.ofHours(3), "2020-01-01T05:30:00Z", "+01:00", "2020-01-01T05:00:00Z"),
            new DurationTestCaseData(Duration.ofMinutes(3 * 60), "2020-01-01T05:30:00Z", "+01:00", "2020-01-01T05:00:00Z"),
            new DurationTestCaseData(Duration.ofHours(6), "2024-03-01T00:30:00Z", "-03", "2024-02-29T21:00:00Z"),
            new DurationTestCaseData(Duration.ofHours(24), "2024-03-01T00:30:00Z", "-03", "2024-02-29T03:00:00Z"),
            new DurationTestCaseData(Duration.ofHours(5), "2020-01-01T05:30:00Z", "+01", "2020-01-01T01:00:00Z"),

            ///
            /// Daylight savings
            ///
            // - +1 -> +2 at 2025-03-30T02:00:00+01:00
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
            new DurationTestCaseData(Duration.ofHours(48), "2025-10-26T02:00:00+02:00", "Europe/Rome", "2025-10-25T00:00:00+02:00"),

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
        List<PeriodTestCaseData> cases = new ArrayList<>();

        // Add generic cases for either UTC, fixed timezones and timezones with minutes.
        // Note that we can't do this with variable timezones, as date formats accept only offsets.
        //
        // For every unit, we test 2 cases: 1 unit, and multiple units.
        // Then, for every case, we check 4 boundaries (↑Bucket1, ↓Bucket2, ↑Bucket2, ↓Bucket3) to ensure the exact size of the buckets.
        final List<String> timezones = List.of("Z", "-08:00", "+04:00", "+11:45", "Europe/Madrid", "America/New_York");
        Stream.of(
            // Days
            new PeriodTestCaseData(Period.ofDays(1), "2023-02-16T23:59:59.99", "", "2023-02-16T00:00:00"),
            new PeriodTestCaseData(Period.ofDays(1), "2023-02-17T00:00:00", "", "2023-02-17T00:00:00"),
            new PeriodTestCaseData(Period.ofDays(1), "2023-02-17T23:59:59.99", "", "2023-02-17T00:00:00"),
            new PeriodTestCaseData(Period.ofDays(1), "2023-02-18T00:00:00", "", "2023-02-18T00:00:00"),
            new PeriodTestCaseData(Period.ofDays(10), "2023-02-11T23:59:59.99", "", "2023-02-02T00:00:00"),
            new PeriodTestCaseData(Period.ofDays(10), "2023-02-12T00:00:00", "", "2023-02-12T00:00:00"),
            new PeriodTestCaseData(Period.ofDays(10), "2023-02-21T23:59:59.99", "", "2023-02-12T00:00:00"),
            new PeriodTestCaseData(Period.ofDays(10), "2023-02-22T00:00:00", "", "2023-02-22T00:00:00"),
            // Weeks
            new PeriodTestCaseData(Period.ofDays(7), "2023-02-05T23:59:59.99", "", "2023-01-30T00:00:00"),
            new PeriodTestCaseData(Period.ofDays(7), "2023-02-06T00:00:00", "", "2023-02-06T00:00:00"),
            new PeriodTestCaseData(Period.ofDays(7), "2023-02-12T23:59:59.99", "", "2023-02-06T00:00:00"),
            new PeriodTestCaseData(Period.ofDays(7), "2023-02-13T00:00:00", "", "2023-02-13T00:00:00"),
            new PeriodTestCaseData(Period.ofDays(21), "2023-01-25T23:59:59.99", "", "2023-01-05T00:00:00"),
            new PeriodTestCaseData(Period.ofDays(21), "2023-01-26T00:00:00", "", "2023-01-26T00:00:00"),
            new PeriodTestCaseData(Period.ofDays(21), "2023-02-15T23:59:59.99", "", "2023-01-26T00:00:00"),
            new PeriodTestCaseData(Period.ofDays(21), "2023-02-16T00:00:00", "", "2023-02-16T00:00:00"),
            // Months
            new PeriodTestCaseData(Period.ofMonths(1), "2024-02-29T23:59:59.99", "", "2024-02-01T00:00:00"),
            new PeriodTestCaseData(Period.ofMonths(1), "2024-03-01T00:00:00", "", "2024-03-01T00:00:00"),
            new PeriodTestCaseData(Period.ofMonths(1), "2024-03-31T23:59:59.99", "", "2024-03-01T00:00:00"),
            new PeriodTestCaseData(Period.ofMonths(1), "2024-04-01T00:00:00", "", "2024-04-01T00:00:00"),
            new PeriodTestCaseData(Period.ofMonths(7), "2022-10-31T23:59:59.99", "", "2022-04-01T00:00:00"),
            new PeriodTestCaseData(Period.ofMonths(7), "2022-11-01T00:00:00", "", "2022-11-01T00:00:00"),
            new PeriodTestCaseData(Period.ofMonths(7), "2023-05-31T23:59:59.99", "", "2022-11-01T00:00:00"),
            new PeriodTestCaseData(Period.ofMonths(7), "2023-06-01T00:00:00", "", "2023-06-01T00:00:00"),
            // Quarters
            new PeriodTestCaseData(Period.ofMonths(3), "2023-12-31T23:59:59.99", "", "2023-10-01T00:00:00"),
            new PeriodTestCaseData(Period.ofMonths(3), "2024-01-01T00:00:00", "", "2024-01-01T00:00:00"),
            new PeriodTestCaseData(Period.ofMonths(3), "2024-03-31T23:59:59.99", "", "2024-01-01T00:00:00"),
            new PeriodTestCaseData(Period.ofMonths(3), "2024-04-01T00:00:00", "", "2024-04-01T00:00:00"),
            new PeriodTestCaseData(Period.ofMonths(6), "2023-12-31T23:59:59.99", "", "2023-07-01T00:00:00"),
            new PeriodTestCaseData(Period.ofMonths(6), "2024-01-01T00:00:00", "", "2024-01-01T00:00:00"),
            new PeriodTestCaseData(Period.ofMonths(6), "2024-06-30T23:59:59.99", "", "2024-01-01T00:00:00"),
            new PeriodTestCaseData(Period.ofMonths(6), "2024-07-01T00:00:00", "", "2024-07-01T00:00:00"),
            // Years
            new PeriodTestCaseData(Period.ofYears(1), "2022-12-31T23:59:59.99", "", "2022-01-01T00:00:00"),
            new PeriodTestCaseData(Period.ofYears(1), "2023-01-01T00:00:00", "", "2023-01-01T00:00:00"),
            new PeriodTestCaseData(Period.ofYears(1), "2023-12-31T23:59:59.99", "", "2023-01-01T00:00:00"),
            new PeriodTestCaseData(Period.ofYears(1), "2024-01-01T00:00:00", "", "2024-01-01T00:00:00"),
            new PeriodTestCaseData(Period.ofYears(5), "2020-12-31T23:59:59.99", "", "2016-01-01T00:00:00"),
            new PeriodTestCaseData(Period.ofYears(5), "2021-01-01T00:00:00", "", "2021-01-01T00:00:00"),
            new PeriodTestCaseData(Period.ofYears(5), "2025-12-31T23:59:59.99", "", "2021-01-01T00:00:00"),
            new PeriodTestCaseData(Period.ofYears(5), "2026-01-01T00:00:00", "", "2026-01-01T00:00:00"),
            // Negative years
            new PeriodTestCaseData(Period.ofYears(4), "-0004-12-31T23:59:59.99", "", "-0007-01-01T00:00:00"),
            new PeriodTestCaseData(Period.ofYears(4), "-0003-01-01T00:00:00", "", "-0003-01-01T00:00:00"),
            new PeriodTestCaseData(Period.ofYears(4), "-0001-12-31T23:59:59.99", "", "-0003-01-01T00:00:00"),
            new PeriodTestCaseData(Period.ofYears(4), "0001-01-01T00:00:00", "", "0001-01-01T00:00:00")
        ).forEach(c -> timezones.forEach(timezone -> {
            // Convert the timezone to the offset in each local time.
            // This is required as date strings can't have a zone name as its zone.
            var inputOffset = timezone.startsWith("+") || timezone.startsWith("-")
                ? timezone
                : LocalDateTime.parse(c.inputDate()).atZone(ZoneId.of(timezone)).getOffset().getId();
            var expectedOffset = timezone.startsWith("+") || timezone.startsWith("-")
                ? timezone
                : LocalDateTime.parse(c.expectedDate()).atZone(ZoneId.of(timezone)).getOffset().getId();
            cases.add(new PeriodTestCaseData(c.period(), c.inputDate() + inputOffset, timezone, c.expectedDate() + expectedOffset));
        }));

        // Special cases
        cases.addAll(
            List.of(
                ///
            /// DST boundaries (e.g. New York: -5 to -4 at 2025-03-09T02:00:00-05, and -4 to -5 at 2025-11-02T02:00:00-04)
            ///
                // Days
                new PeriodTestCaseData(Period.ofDays(1), "2025-03-09T06:00:00-04:00", "America/New_York", "2025-03-09T00:00:00-05:00"),
                new PeriodTestCaseData(Period.ofDays(1), "2025-11-02T05:00:00-05:00", "America/New_York", "2025-11-02T00:00:00-04:00")
            )
        );
        return cases;
    }

    private static List<TestCaseSupplier> ofDatePeriod(PeriodTestCaseData data) {
        return List.of(
            new TestCaseSupplier(
                data.testCaseNameForMillis(),
                List.of(DataType.DATE_PERIOD, DataType.DATETIME),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(data.period(), DataType.DATE_PERIOD, "interval").forceLiteral(),
                        new TestCaseSupplier.TypedData(data.inputDateAsMillis(), DataType.DATETIME, "date")
                    ),
                    Matchers.startsWith("DateTruncDatetimeEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding["),
                    DataType.DATETIME,
                    matchesDateMillis(data.expectedDate())
                ).withConfiguration(TEST_SOURCE, configurationForTimezone(data.zoneId()))
            ),
            new TestCaseSupplier(
                data.testCaseNameForNanos(),
                List.of(DataType.DATE_PERIOD, DataType.DATE_NANOS),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(data.period(), DataType.DATE_PERIOD, "interval").forceLiteral(),
                        new TestCaseSupplier.TypedData(data.inputDateAsNanos(), DataType.DATE_NANOS, "date")
                    ),
                    Matchers.startsWith("DateTruncDateNanosEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding["),
                    DataType.DATE_NANOS,
                    matchesDateNanos(data.expectedDate())
                ).withConfiguration(TEST_SOURCE, configurationForTimezone(data.zoneId()))
            )
        );
    }

    private static List<TestCaseSupplier> ofDuration(DurationTestCaseData data) {
        return List.of(
            new TestCaseSupplier(
                data.testCaseNameForMillis(),
                List.of(DataType.TIME_DURATION, DataType.DATETIME),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(data.duration(), DataType.TIME_DURATION, "interval").forceLiteral(),
                        new TestCaseSupplier.TypedData(data.inputDateAsMillis(), DataType.DATETIME, "date")
                    ),
                    Matchers.startsWith("DateTruncDatetimeEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding["),
                    DataType.DATETIME,
                    matchesDateMillis(data.expectedDate())
                ).withConfiguration(TEST_SOURCE, configurationForTimezone(data.zoneId()))
            ),
            new TestCaseSupplier(
                data.testCaseNameForNanos(),
                List.of(DataType.TIME_DURATION, DataType.DATE_NANOS),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(data.duration(), DataType.TIME_DURATION, "interval").forceLiteral(),
                        new TestCaseSupplier.TypedData(data.inputDateAsNanos(), DataType.DATE_NANOS, "date")
                    ),
                    Matchers.startsWith("DateTruncDateNanosEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding["),
                    DataType.DATE_NANOS,
                    matchesDateNanos(data.expectedDate())
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

    @Override
    protected Expression buildWithConfiguration(Source source, List<Expression> args, Configuration configuration) {
        return new DateTrunc(source, args.get(0), args.get(1), configuration);
    }
}
