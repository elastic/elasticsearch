/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.time;

import org.elasticsearch.test.ESTestCase;

import java.time.Instant;
import java.time.LocalDate;
import java.time.Month;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;

import static org.elasticsearch.common.time.DateUtils.MAX_MILLIS_BEFORE_MINUS_9999;
import static org.elasticsearch.common.time.DateUtils.MAX_NANOSECOND_INSTANT;
import static org.elasticsearch.common.time.DateUtils.MAX_NANOSECOND_IN_MILLIS;
import static org.elasticsearch.common.time.DateUtils.clampToNanosRange;
import static org.elasticsearch.common.time.DateUtils.compareNanosToMillis;
import static org.elasticsearch.common.time.DateUtils.toInstant;
import static org.elasticsearch.common.time.DateUtils.toLong;
import static org.elasticsearch.common.time.DateUtils.toLongMillis;
import static org.elasticsearch.common.time.DateUtils.toMilliSeconds;
import static org.elasticsearch.common.time.DateUtils.toNanoSeconds;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

public class DateUtilsTests extends ESTestCase {

    public void testCompareNanosToMillis() {
        assertThat(MAX_NANOSECOND_IN_MILLIS * 1_000_000, lessThan(Long.MAX_VALUE));

        assertThat(compareNanosToMillis(toLong(Instant.EPOCH), Instant.EPOCH.toEpochMilli()), is(0));

        // This should be 1, because the millisecond version should truncate a bit
        assertThat(compareNanosToMillis(toLong(MAX_NANOSECOND_INSTANT), MAX_NANOSECOND_INSTANT.toEpochMilli()), is(1));

        assertThat(compareNanosToMillis(toLong(MAX_NANOSECOND_INSTANT), -1000), is(1));
        // millis before epoch
        assertCompareInstants(
            randomInstantBetween(Instant.EPOCH, MAX_NANOSECOND_INSTANT),
            randomInstantBetween(Instant.ofEpochMilli(MAX_MILLIS_BEFORE_MINUS_9999), Instant.ofEpochMilli(-1L))
        );

        // millis after nanos range
        assertCompareInstants(
            randomInstantBetween(Instant.EPOCH, MAX_NANOSECOND_INSTANT),
            randomInstantBetween(MAX_NANOSECOND_INSTANT.plusMillis(1), Instant.ofEpochMilli(Long.MAX_VALUE))
        );

        // both in range
        Instant nanos = randomInstantBetween(Instant.EPOCH, MAX_NANOSECOND_INSTANT);
        Instant millis = randomInstantBetween(Instant.EPOCH, MAX_NANOSECOND_INSTANT);

        assertCompareInstants(nanos, millis);
    }

    /**
     *  check that compareNanosToMillis is consistent with Instant#compare.
     */
    private void assertCompareInstants(Instant nanos, Instant millis) {
        assertThat(compareNanosToMillis(toLong(nanos), millis.toEpochMilli()), equalTo(nanos.compareTo(millis)));
    }

    public void testInstantToLong() {
        assertThat(toLong(Instant.EPOCH), is(0L));

        Instant instant = createRandomInstant();
        long timeSinceEpochInNanos = instant.getEpochSecond() * 1_000_000_000 + instant.getNano();
        assertThat(toLong(instant), is(timeSinceEpochInNanos));
    }

    public void testInstantToLongMin() {
        Instant tooEarlyInstant = ZonedDateTime.parse("1677-09-21T00:12:43.145224191Z").toInstant();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> toLong(tooEarlyInstant));
        assertThat(e.getMessage(), containsString("is before"));
        e = expectThrows(IllegalArgumentException.class, () -> toLong(Instant.EPOCH.minusMillis(1)));
        assertThat(e.getMessage(), containsString("is before"));
    }

    public void testInstantToLongMax() {
        Instant tooLateInstant = ZonedDateTime.parse("2262-04-11T23:47:16.854775808Z").toInstant();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> toLong(tooLateInstant));
        assertThat(e.getMessage(), containsString("is after"));
    }

    public void testInstantToLongMillis() {
        assertThat(toLongMillis(Instant.EPOCH), is(0L));

        Instant instant = createRandomInstant();
        long timeSinceEpochInMillis = instant.toEpochMilli();
        assertThat(toLongMillis(instant), is(timeSinceEpochInMillis));

        Instant maxInstant = Instant.ofEpochSecond(Long.MAX_VALUE / 1000);
        long maxInstantMillis = maxInstant.toEpochMilli();
        assertThat(toLongMillis(maxInstant), is(maxInstantMillis));

        Instant minInstant = Instant.ofEpochSecond(Long.MIN_VALUE / 1000);
        long minInstantMillis = minInstant.toEpochMilli();
        assertThat(toLongMillis(minInstant), is(minInstantMillis));
    }

    public void testInstantToLongMillisMin() {
        /* negative millisecond value of this instant exceeds the maximum value a java long variable can store */
        Instant tooEarlyInstant = Instant.MIN;
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> toLongMillis(tooEarlyInstant));
        assertThat(e.getMessage(), containsString("too far in the past"));

        Instant tooEarlyInstant2 = Instant.ofEpochSecond(Long.MIN_VALUE / 1000 - 1);
        e = expectThrows(IllegalArgumentException.class, () -> toLongMillis(tooEarlyInstant2));
        assertThat(e.getMessage(), containsString("too far in the past"));
    }

    public void testInstantToLongMillisMax() {
        /* millisecond value of this instant exceeds the maximum value a java long variable can store */
        Instant tooLateInstant = Instant.MAX;
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> toLongMillis(tooLateInstant));
        assertThat(e.getMessage(), containsString("too far in the future"));

        Instant tooLateInstant2 = Instant.ofEpochSecond(Long.MAX_VALUE / 1000 + 1);
        e = expectThrows(IllegalArgumentException.class, () -> toLongMillis(tooLateInstant2));
        assertThat(e.getMessage(), containsString("too far in the future"));
    }

    public void testLongToInstant() {
        assertThat(toInstant(0), is(Instant.EPOCH));
        assertThat(toInstant(1), is(Instant.EPOCH.plusNanos(1)));

        Instant instant = createRandomInstant();
        long nowInNs = toLong(instant);
        assertThat(toInstant(nowInNs), is(instant));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> toInstant(-1));
        assertThat(e.getMessage(), is("nanoseconds [-1] are before the epoch in 1970 and cannot be processed in nanosecond resolution"));

        e = expectThrows(IllegalArgumentException.class, () -> toInstant(Long.MIN_VALUE));
        assertThat(
            e.getMessage(),
            is("nanoseconds [" + Long.MIN_VALUE + "] are before the epoch in 1970 and cannot be processed in nanosecond resolution")
        );

        assertThat(toInstant(Long.MAX_VALUE), is(ZonedDateTime.parse("2262-04-11T23:47:16.854775807Z").toInstant()));
    }

    public void testClampToNanosRange() {
        assertThat(clampToNanosRange(Instant.EPOCH), equalTo(Instant.EPOCH));

        Instant instant = createRandomInstant();
        assertThat(clampToNanosRange(instant), equalTo(instant));
    }

    public void testClampToNanosRangeMin() {
        assertThat(clampToNanosRange(Instant.EPOCH.minusMillis(1)), equalTo(Instant.EPOCH));

        Instant tooEarlyInstant = ZonedDateTime.parse("1677-09-21T00:12:43.145224191Z").toInstant();
        assertThat(clampToNanosRange(tooEarlyInstant), equalTo(Instant.EPOCH));
    }

    public void testClampToNanosRangeMax() {
        Instant tooLateInstant = ZonedDateTime.parse("2262-04-11T23:47:16.854775808Z").toInstant();
        assertThat(clampToNanosRange(tooLateInstant), equalTo(DateUtils.MAX_NANOSECOND_INSTANT));
    }

    public void testNanosToMillis() {
        assertThat(toMilliSeconds(0), is(Instant.EPOCH.toEpochMilli()));

        Instant instant = createRandomInstant();
        long nowInNs = toLong(instant);
        assertThat(toMilliSeconds(nowInNs), is(instant.toEpochMilli()));
    }

    public void testMillisToNanos() {
        assertThat(toNanoSeconds(0), equalTo(0L));

        Instant instant = Instant.ofEpochSecond(randomLongBetween(0, Long.MAX_VALUE) / 1_000_000_000L);
        long nowInMs = instant.toEpochMilli();
        assertThat(toNanoSeconds(nowInMs), equalTo(toLong(instant)));

        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> toNanoSeconds(-1));
        assertThat(exc.getMessage(), containsString("before the epoch"));

        long millis = DateUtils.MAX_NANOSECOND_IN_MILLIS + randomLongBetween(0, 1000000);
        exc = expectThrows(IllegalArgumentException.class, () -> toNanoSeconds(millis));
        assertThat(exc.getMessage(), containsString("after 2262"));
    }

    private Instant createRandomInstant() {
        long seconds = randomLongBetween(0, Long.MAX_VALUE) / 1_000_000_000L;
        long nanos = randomLongBetween(0, 999_999_999L);
        return Instant.ofEpochSecond(seconds, nanos);
    }

    public void testRoundFloor() {
        assertThat(DateUtils.roundFloor(0, randomLongBetween(0, Long.MAX_VALUE)), is(0L));

        ChronoField randomChronoField = randomFrom(
            ChronoField.DAY_OF_MONTH,
            ChronoField.HOUR_OF_DAY,
            ChronoField.MINUTE_OF_HOUR,
            ChronoField.SECOND_OF_MINUTE
        );
        long unitMillis = randomChronoField.getBaseUnit().getDuration().toMillis();

        int year = randomIntBetween(-3000, 3000);
        int month = randomIntBetween(1, 12);
        int day = randomIntBetween(1, YearMonth.of(year, month).lengthOfMonth());
        int hour = randomIntBetween(1, 23);
        int minute = randomIntBetween(1, 59);
        int second = randomIntBetween(1, 59);
        int nanos = randomIntBetween(1, 999_999_999);

        ZonedDateTime randomDate = ZonedDateTime.of(year, month, day, hour, minute, second, nanos, ZoneOffset.UTC);

        ZonedDateTime result = switch (randomChronoField) {
            case SECOND_OF_MINUTE -> randomDate.withNano(0);
            case MINUTE_OF_HOUR -> randomDate.withNano(0).withSecond(0);
            case HOUR_OF_DAY -> randomDate.withNano(0).withSecond(0).withMinute(0);
            case DAY_OF_MONTH -> randomDate.withNano(0).withSecond(0).withMinute(0).withHour(0);
            default -> randomDate;
        };

        long rounded = DateUtils.roundFloor(randomDate.toInstant().toEpochMilli(), unitMillis);
        assertThat(rounded, is(result.toInstant().toEpochMilli()));
    }

    public void testRoundQuarterOfYear() {
        assertThat(DateUtils.roundQuarterOfYear(0), is(0L));
        long lastQuarter1969 = ZonedDateTime.of(1969, 10, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
        assertThat(DateUtils.roundQuarterOfYear(-1), is(lastQuarter1969));

        int year = randomIntBetween(1970, 2040);
        int month = randomIntBetween(1, 12);
        int day = randomIntBetween(1, YearMonth.of(year, month).lengthOfMonth());

        ZonedDateTime randomZonedDateTime = ZonedDateTime.of(
            year,
            month,
            day,
            randomIntBetween(0, 23),
            randomIntBetween(0, 59),
            randomIntBetween(0, 59),
            999_999_999,
            ZoneOffset.UTC
        );
        long quarterInMillis = Year.of(randomZonedDateTime.getYear())
            .atMonth(Month.of(month).firstMonthOfQuarter())
            .atDay(1)
            .atStartOfDay(ZoneOffset.UTC)
            .toInstant()
            .toEpochMilli();
        long result = DateUtils.roundQuarterOfYear(randomZonedDateTime.toInstant().toEpochMilli());
        assertThat(result, is(quarterInMillis));
    }

    public void testRoundMonthOfYear() {
        assertThat(DateUtils.roundMonthOfYear(0), is(0L));
        assertThat(DateUtils.roundMonthOfYear(1), is(0L));
        long dec1969 = LocalDate.of(1969, 12, 1).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
        assertThat(DateUtils.roundMonthOfYear(-1), is(dec1969));

        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> DateUtils.roundIntervalMonthOfYear(0, -1));
        assertThat(exc.getMessage(), is("month interval must be strictly positive, got [-1]"));
        long epochMilli = LocalDate.of(1969, 10, 1).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
        assertThat(DateUtils.roundIntervalMonthOfYear(1, 5), is(epochMilli));
        epochMilli = LocalDate.of(1969, 6, 1).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
        assertThat(DateUtils.roundIntervalMonthOfYear(-1, 13), is(epochMilli));
        epochMilli = LocalDate.of(2024, 8, 1).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
        assertThat(DateUtils.roundIntervalMonthOfYear(1737378896000L, 7), is(epochMilli));
        epochMilli = LocalDate.of(-2026, 4, 1).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
        assertThat(DateUtils.roundIntervalMonthOfYear(-126068400000000L, 11), is(epochMilli));
    }

    public void testRoundYear() {
        assertThat(DateUtils.roundYear(0), is(0L));
        assertThat(DateUtils.roundYear(1), is(0L));
        long startOf1969 = ZonedDateTime.of(1969, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
        assertThat(DateUtils.roundYear(-1), is(startOf1969));
        long endOf1970 = ZonedDateTime.of(1970, 12, 31, 23, 59, 59, 999_999_999, ZoneOffset.UTC).toInstant().toEpochMilli();
        assertThat(DateUtils.roundYear(endOf1970), is(0L));
        // test with some leap year
        long endOf1996 = ZonedDateTime.of(1996, 12, 31, 23, 59, 59, 999_999_999, ZoneOffset.UTC).toInstant().toEpochMilli();
        long startOf1996 = Year.of(1996).atDay(1).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
        assertThat(DateUtils.roundYear(endOf1996), is(startOf1996));

        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> DateUtils.roundYearInterval(0, -1));
        assertThat(exc.getMessage(), is("year interval must be strictly positive, got [-1]"));
        assertThat(DateUtils.roundYearInterval(0, 2), is(startOf1969));
        long startOf1968 = Year.of(1968).atDay(1).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
        assertThat(DateUtils.roundYearInterval(0, 7), is(startOf1968));
        long startOf1966 = Year.of(1966).atDay(1).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
        assertThat(DateUtils.roundYearInterval(1, 5), is(startOf1966));
        long startOf1961 = Year.of(1961).atDay(1).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
        assertThat(DateUtils.roundYearInterval(-1, 10), is(startOf1961));
        long startOf1992 = Year.of(1992).atDay(1).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
        assertThat(DateUtils.roundYearInterval(endOf1996, 11), is(startOf1992));
        long epochMilli = Year.of(-2034).atDay(1).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
        assertThat(DateUtils.roundYearInterval(-126068400000000L, 11), is(epochMilli));
    }

    public void testRoundWeek() {
        long epochMilli = Year.of(1969).atMonth(12).atDay(29).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
        assertThat(DateUtils.roundWeekOfWeekYear(0), is(epochMilli));
        assertThat(DateUtils.roundWeekOfWeekYear(1), is(epochMilli));
        assertThat(DateUtils.roundWeekOfWeekYear(-1), is(epochMilli));

        epochMilli = Year.of(2025).atMonth(1).atDay(20).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
        assertThat(DateUtils.roundWeekOfWeekYear(1737378896000L), is(epochMilli));
    }
}
