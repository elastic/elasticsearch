/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.time;

import org.elasticsearch.test.ESTestCase;
import org.joda.time.DateTimeZone;

import java.time.Instant;
import java.time.LocalDate;
import java.time.Month;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.common.time.DateUtils.toInstant;
import static org.elasticsearch.common.time.DateUtils.toLong;
import static org.elasticsearch.common.time.DateUtils.toMilliSeconds;
import static org.elasticsearch.common.time.DateUtils.toNanoSeconds;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DateUtilsTests extends ESTestCase {
    private static final Set<String> IGNORE = new HashSet<>(Arrays.asList(
        "Eire", "Europe/Dublin", // dublin timezone in joda does not account for DST
        "Asia/Qostanay" // this has been added in joda 2.10.2 but is not part of the JDK 12.0.1 tzdata yet
    ));

    public void testTimezoneIds() {
        assertNull(DateUtils.dateTimeZoneToZoneId(null));
        assertNull(DateUtils.zoneIdToDateTimeZone(null));
        for (String jodaId : DateTimeZone.getAvailableIDs()) {
            if (IGNORE.contains(jodaId)) continue;
            DateTimeZone jodaTz = DateTimeZone.forID(jodaId);
            ZoneId zoneId = DateUtils.dateTimeZoneToZoneId(jodaTz); // does not throw
            long now = 0;
            assertThat(jodaId, zoneId.getRules().getOffset(Instant.ofEpochMilli(now)).getTotalSeconds() * 1000,
                equalTo(jodaTz.getOffset(now)));
            if (DateUtils.DEPRECATED_SHORT_TIMEZONES.containsKey(jodaTz.getID())) {
                assertWarnings("Use of short timezone id " + jodaId + " is deprecated. Use " + zoneId.getId() + " instead");
            }
            // roundtrip does not throw either
            assertNotNull(DateUtils.zoneIdToDateTimeZone(zoneId));
        }
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
        Instant tooEarlyInstant = ZonedDateTime.parse("2262-04-11T23:47:16.854775808Z").toInstant();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> toLong(tooEarlyInstant));
        assertThat(e.getMessage(), containsString("is after"));
    }

    public void testLongToInstant() {
        assertThat(toInstant(0), is(Instant.EPOCH));
        assertThat(toInstant(1), is(Instant.EPOCH.plusNanos(1)));

        Instant instant = createRandomInstant();
        long nowInNs = toLong(instant);
        assertThat(toInstant(nowInNs), is(instant));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> toInstant(-1));
        assertThat(e.getMessage(),
            is("nanoseconds [-1] are before the epoch in 1970 and cannot be processed in nanosecond resolution"));

        e = expectThrows(IllegalArgumentException.class, () -> toInstant(Long.MIN_VALUE));
        assertThat(e.getMessage(),
            is("nanoseconds [" + Long.MIN_VALUE + "] are before the epoch in 1970 and cannot be processed in nanosecond resolution"));

        assertThat(toInstant(Long.MAX_VALUE),
            is(ZonedDateTime.parse("2262-04-11T23:47:16.854775807Z").toInstant()));
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

        IllegalArgumentException exc =
            expectThrows(IllegalArgumentException.class, () -> toNanoSeconds(-1));
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

        ChronoField randomChronoField =
            randomFrom(ChronoField.DAY_OF_MONTH, ChronoField.HOUR_OF_DAY, ChronoField.MINUTE_OF_HOUR, ChronoField.SECOND_OF_MINUTE);
        long unitMillis = randomChronoField.getBaseUnit().getDuration().toMillis();

        int year = randomIntBetween(-3000, 3000);
        int month = randomIntBetween(1, 12);
        int day = randomIntBetween(1, YearMonth.of(year, month).lengthOfMonth());
        int hour = randomIntBetween(1, 23);
        int minute = randomIntBetween(1, 59);
        int second = randomIntBetween(1, 59);
        int nanos = randomIntBetween(1, 999_999_999);

        ZonedDateTime randomDate = ZonedDateTime.of(year, month, day, hour, minute, second, nanos, ZoneOffset.UTC);

        ZonedDateTime result = randomDate;
        switch (randomChronoField) {
            case SECOND_OF_MINUTE:
                result = result.withNano(0);
                break;
            case MINUTE_OF_HOUR:
                result = result.withNano(0).withSecond(0);
                break;
            case HOUR_OF_DAY:
                result = result.withNano(0).withSecond(0).withMinute(0);
                break;
            case DAY_OF_MONTH:
                result = result.withNano(0).withSecond(0).withMinute(0).withHour(0);
                break;
        }

        long rounded = DateUtils.roundFloor(randomDate.toInstant().toEpochMilli(), unitMillis);
        assertThat(rounded, is(result.toInstant().toEpochMilli()));
    }

    public void testRoundQuarterOfYear() {
        assertThat(DateUtils.roundQuarterOfYear(0), is(0L));
        long lastQuarter1969 = ZonedDateTime.of(1969, 10, 1, 0, 0, 0, 0, ZoneOffset.UTC)
            .toInstant().toEpochMilli();
        assertThat(DateUtils.roundQuarterOfYear(-1), is(lastQuarter1969));

        int year = randomIntBetween(1970, 2040);
        int month = randomIntBetween(1, 12);
        int day = randomIntBetween(1, YearMonth.of(year, month).lengthOfMonth());

        ZonedDateTime randomZonedDateTime = ZonedDateTime.of(year, month, day,
            randomIntBetween(0, 23), randomIntBetween(0, 59), randomIntBetween(0, 59), 999_999_999, ZoneOffset.UTC);
        long quarterInMillis = Year.of(randomZonedDateTime.getYear()).atMonth(Month.of(month).firstMonthOfQuarter()).atDay(1)
            .atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        long result = DateUtils.roundQuarterOfYear(randomZonedDateTime.toInstant().toEpochMilli());
        assertThat(result, is(quarterInMillis));
    }

    public void testRoundMonthOfYear() {
        assertThat(DateUtils.roundMonthOfYear(0), is(0L));
        assertThat(DateUtils.roundMonthOfYear(1), is(0L));
        long dec1969 = LocalDate.of(1969, 12, 1).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
        assertThat(DateUtils.roundMonthOfYear(-1), is(dec1969));
    }

    public void testRoundYear() {
        assertThat(DateUtils.roundYear(0), is(0L));
        assertThat(DateUtils.roundYear(1), is(0L));
        long startOf1969 = ZonedDateTime.of(1969, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)
            .toInstant().toEpochMilli();
        assertThat(DateUtils.roundYear(-1), is(startOf1969));
        long endOf1970 = ZonedDateTime.of(1970, 12, 31, 23, 59, 59, 999_999_999, ZoneOffset.UTC)
            .toInstant().toEpochMilli();
        assertThat(DateUtils.roundYear(endOf1970), is(0L));
        // test with some leapyear
        long endOf1996 = ZonedDateTime.of(1996, 12, 31, 23, 59, 59, 999_999_999, ZoneOffset.UTC)
            .toInstant().toEpochMilli();
        long startOf1996 = Year.of(1996).atDay(1).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
        assertThat(DateUtils.roundYear(endOf1996), is(startOf1996));
    }
}
