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

package org.elasticsearch.common.rounding;

import org.elasticsearch.common.rounding.TimeZoneRounding.TimeIntervalRounding;
import org.elasticsearch.common.rounding.TimeZoneRounding.TimeUnitRounding;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 */
public class TimeZoneRoundingTests extends ESTestCase {
    final static DateTimeZone JERUSALEM_TIMEZONE = DateTimeZone.forID("Asia/Jerusalem");

    public void testUTCTimeUnitRounding() {
        Rounding tzRounding = TimeZoneRounding.builder(DateTimeUnit.MONTH_OF_YEAR).build();
        assertThat(tzRounding.round(utc("2009-02-03T01:01:01")), equalTo(utc("2009-02-01T00:00:00.000Z")));
        assertThat(tzRounding.nextRoundingValue(utc("2009-02-01T00:00:00.000Z")), equalTo(utc("2009-03-01T00:00:00.000Z")));

        tzRounding = TimeZoneRounding.builder(DateTimeUnit.WEEK_OF_WEEKYEAR).build();
        assertThat(tzRounding.round(utc("2012-01-10T01:01:01")), equalTo(utc("2012-01-09T00:00:00.000Z")));
        assertThat(tzRounding.nextRoundingValue(utc("2012-01-09T00:00:00.000Z")), equalTo(utc("2012-01-16T00:00:00.000Z")));

        tzRounding = TimeZoneRounding.builder(DateTimeUnit.WEEK_OF_WEEKYEAR).offset(-TimeValue.timeValueHours(24).millis()).build();
        assertThat(tzRounding.round(utc("2012-01-10T01:01:01")), equalTo(utc("2012-01-08T00:00:00.000Z")));
        assertThat(tzRounding.nextRoundingValue(utc("2012-01-08T00:00:00.000Z")), equalTo(utc("2012-01-15T00:00:00.000Z")));
    }

    public void testUTCIntervalRounding() {
        Rounding tzRounding = TimeZoneRounding.builder(TimeValue.timeValueHours(12)).build();
        assertThat(tzRounding.round(utc("2009-02-03T01:01:01")), equalTo(utc("2009-02-03T00:00:00.000Z")));
        long roundKey = tzRounding.roundKey(utc("2009-02-03T01:01:01"));
        assertThat(roundKey, equalTo(tzRounding.roundKey(utc("2009-02-03T00:00:00.000Z"))));
        assertThat(tzRounding.valueForKey(roundKey), equalTo(utc("2009-02-03T00:00:00.000Z")));
        assertThat(tzRounding.nextRoundingValue(utc("2009-02-03T00:00:00.000Z")), equalTo(utc("2009-02-03T12:00:00.000Z")));
        assertThat(tzRounding.round(utc("2009-02-03T13:01:01")), equalTo(utc("2009-02-03T12:00:00.000Z")));
        assertThat(tzRounding.nextRoundingValue(utc("2009-02-03T12:00:00.000Z")), equalTo(utc("2009-02-04T00:00:00.000Z")));

        tzRounding = TimeZoneRounding.builder(TimeValue.timeValueHours(48)).build();
        assertThat(tzRounding.round(utc("2009-02-03T01:01:01")), equalTo(utc("2009-02-03T00:00:00.000Z")));
        assertThat(tzRounding.nextRoundingValue(utc("2009-02-03T00:00:00.000Z")), equalTo(utc("2009-02-05T00:00:00.000Z")));
        assertThat(tzRounding.round(utc("2009-02-05T13:01:01")), equalTo(utc("2009-02-05T00:00:00.000Z")));
        assertThat(tzRounding.nextRoundingValue(utc("2009-02-05T00:00:00.000Z")), equalTo(utc("2009-02-07T00:00:00.000Z")));
    }

    /**
     * test TimeIntervalTimeZoneRounding, (interval &lt; 12h) with time zone shift
     */
    public void testTimeIntervalTimeZoneRounding() {
        Rounding tzRounding = TimeZoneRounding.builder(TimeValue.timeValueHours(6)).timeZone(DateTimeZone.forOffsetHours(-1)).build();
        assertThat(tzRounding.round(utc("2009-02-03T00:01:01")), equalTo(utc("2009-02-02T19:00:00.000Z")));
        long roundKey = tzRounding.roundKey(utc("2009-02-03T00:01:01"));
        assertThat(roundKey, equalTo(tzRounding.roundKey(utc("2009-02-02T19:00:00.000Z"))));
        assertThat(tzRounding.valueForKey(roundKey), equalTo(utc("2009-02-02T19:00:00.000Z")));
        assertThat(tzRounding.nextRoundingValue(utc("2009-02-02T19:00:00.000Z")), equalTo(utc("2009-02-03T01:00:00.000Z")));

        assertThat(tzRounding.round(utc("2009-02-03T13:01:01")), equalTo(utc("2009-02-03T13:00:00.000Z")));
        assertThat(tzRounding.nextRoundingValue(utc("2009-02-03T13:00:00.000Z")), equalTo(utc("2009-02-03T19:00:00.000Z")));
    }

    /**
     * test DayIntervalTimeZoneRounding, (interval &gt;= 12h) with time zone shift
     */
    public void testDayIntervalTimeZoneRounding() {
        Rounding tzRounding = TimeZoneRounding.builder(TimeValue.timeValueHours(12)).timeZone(DateTimeZone.forOffsetHours(-8)).build();
        assertThat(tzRounding.round(utc("2009-02-03T00:01:01")), equalTo(utc("2009-02-02T20:00:00.000Z")));
        long roundKey = tzRounding.roundKey(utc("2009-02-03T00:01:01"));
        assertThat(roundKey, equalTo(tzRounding.roundKey(utc("2009-02-02T20:00:00.000Z"))));
        assertThat(tzRounding.valueForKey(roundKey), equalTo(utc("2009-02-02T20:00:00.000Z")));
        assertThat(tzRounding.nextRoundingValue(utc("2009-02-02T20:00:00.000Z")), equalTo(utc("2009-02-03T08:00:00.000Z")));

        assertThat(tzRounding.round(utc("2009-02-03T13:01:01")), equalTo(utc("2009-02-03T08:00:00.000Z")));
        assertThat(tzRounding.nextRoundingValue(utc("2009-02-03T08:00:00.000Z")), equalTo(utc("2009-02-03T20:00:00.000Z")));
    }

    public void testDayTimeZoneRounding() {
        int timezoneOffset = -2;
        Rounding tzRounding = TimeZoneRounding.builder(DateTimeUnit.DAY_OF_MONTH).timeZone(DateTimeZone.forOffsetHours(timezoneOffset))
                .build();
        assertThat(tzRounding.round(0), equalTo(0L - TimeValue.timeValueHours(24 + timezoneOffset).millis()));
        assertThat(tzRounding.nextRoundingValue(0L - TimeValue.timeValueHours(24 + timezoneOffset).millis()), equalTo(0L - TimeValue
                .timeValueHours(timezoneOffset).millis()));

        tzRounding = TimeZoneRounding.builder(DateTimeUnit.DAY_OF_MONTH).timeZone(DateTimeZone.forID("-08:00")).build();
        assertThat(tzRounding.round(utc("2012-04-01T04:15:30Z")), equalTo(utc("2012-03-31T08:00:00Z")));
        assertThat(toUTCDateString(tzRounding.nextRoundingValue(utc("2012-03-31T08:00:00Z"))),
                equalTo(toUTCDateString(utc("2012-04-01T08:0:00Z"))));

        tzRounding = TimeZoneRounding.builder(DateTimeUnit.MONTH_OF_YEAR).timeZone(DateTimeZone.forID("-08:00")).build();
        assertThat(tzRounding.round(utc("2012-04-01T04:15:30Z")), equalTo(utc("2012-03-01T08:00:00Z")));
        assertThat(toUTCDateString(tzRounding.nextRoundingValue(utc("2012-03-01T08:00:00Z"))),
                equalTo(toUTCDateString(utc("2012-04-01T08:0:00Z"))));

        // date in Feb-3rd, but still in Feb-2nd in -02:00 timezone
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.DAY_OF_MONTH).timeZone(DateTimeZone.forID("-02:00")).build();
        assertThat(tzRounding.round(utc("2009-02-03T01:01:01")), equalTo(utc("2009-02-02T02:00:00")));
        long roundKey = tzRounding.roundKey(utc("2009-02-03T01:01:01"));
        assertThat(roundKey, equalTo(tzRounding.roundKey(utc("2009-02-02T02:00:00.000Z"))));
        assertThat(tzRounding.valueForKey(roundKey), equalTo(utc("2009-02-02T02:00:00.000Z")));
        assertThat(tzRounding.nextRoundingValue(utc("2009-02-02T02:00:00")), equalTo(utc("2009-02-03T02:00:00")));

        // date in Feb-3rd, also in -02:00 timezone
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.DAY_OF_MONTH).timeZone(DateTimeZone.forID("-02:00")).build();
        assertThat(tzRounding.round(utc("2009-02-03T02:01:01")), equalTo(utc("2009-02-03T02:00:00")));
        roundKey = tzRounding.roundKey(utc("2009-02-03T02:01:01"));
        assertThat(roundKey, equalTo(tzRounding.roundKey(utc("2009-02-03T02:00:00.000Z"))));
        assertThat(tzRounding.valueForKey(roundKey), equalTo(utc("2009-02-03T02:00:00.000Z")));
        assertThat(tzRounding.nextRoundingValue(utc("2009-02-03T02:00:00")), equalTo(utc("2009-02-04T02:00:00")));
    }

    public void testTimeTimeZoneRounding() {
        // hour unit
        Rounding tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).timeZone(DateTimeZone.forOffsetHours(-2)).build();
        assertThat(tzRounding.round(0), equalTo(0L));
        assertThat(tzRounding.nextRoundingValue(0L), equalTo(TimeValue.timeValueHours(1L).getMillis()));

        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).timeZone(DateTimeZone.forOffsetHours(-2)).build();
        assertThat(tzRounding.round(utc("2009-02-03T01:01:01")), equalTo(utc("2009-02-03T01:00:00")));
        assertThat(tzRounding.nextRoundingValue(utc("2009-02-03T01:00:00")), equalTo(utc("2009-02-03T02:00:00")));
    }

    public void testTimeUnitRoundingDST() {
        Rounding tzRounding;
        // testing savings to non savings switch
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).timeZone(DateTimeZone.forID("UTC")).build();
        assertThat(tzRounding.round(time("2014-10-26T01:01:01", DateTimeZone.forOffsetHours(2))),  // CEST = UTC+2
                equalTo(time("2014-10-26T01:00:00", DateTimeZone.forOffsetHours(2))));
        assertThat(tzRounding.nextRoundingValue(time("2014-10-26T01:00:00", DateTimeZone.forOffsetHours(2))),
                equalTo(time("2014-10-26T02:00:00", DateTimeZone.forOffsetHours(2))));
        assertThat(tzRounding.nextRoundingValue(time("2014-10-26T02:00:00", DateTimeZone.forOffsetHours(2))),
                equalTo(time("2014-10-26T03:00:00", DateTimeZone.forOffsetHours(2))));

        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).timeZone(DateTimeZone.forID("CET")).build();
        assertThat(tzRounding.round(time("2014-10-26T01:01:01", DateTimeZone.forOffsetHours(2))),  // CEST = UTC+2
                equalTo(time("2014-10-26T01:00:00", DateTimeZone.forOffsetHours(2))));
        assertThat(tzRounding.nextRoundingValue(time("2014-10-26T01:00:00", DateTimeZone.forOffsetHours(2))),
                equalTo(time("2014-10-26T02:00:00", DateTimeZone.forOffsetHours(2))));
        assertThat(tzRounding.nextRoundingValue(time("2014-10-26T02:00:00", DateTimeZone.forOffsetHours(2))),
                equalTo(time("2014-10-26T03:00:00", DateTimeZone.forOffsetHours(2))));

        // testing non savings to savings switch
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).timeZone(DateTimeZone.forID("UTC")).build();
        assertThat(tzRounding.round(time("2014-03-30T01:01:01", DateTimeZone.forOffsetHours(1))),  // CET = UTC+1
                equalTo(time("2014-03-30T01:00:00", DateTimeZone.forOffsetHours(1))));
        assertThat(tzRounding.nextRoundingValue(time("2014-03-30T01:00:00", DateTimeZone.forOffsetHours(1))),
                equalTo(time("2014-03-30T02:00:00", DateTimeZone.forOffsetHours(1))));
        assertThat(tzRounding.nextRoundingValue(time("2014-03-30T02:00:00", DateTimeZone.forOffsetHours(1))),
                equalTo(time("2014-03-30T03:00:00", DateTimeZone.forOffsetHours(1))));

        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).timeZone(DateTimeZone.forID("CET")).build();
        assertThat(tzRounding.round(time("2014-03-30T01:01:01", DateTimeZone.forOffsetHours(1))),  // CET = UTC+1
                equalTo(time("2014-03-30T01:00:00", DateTimeZone.forOffsetHours(1))));
        assertThat(tzRounding.nextRoundingValue(time("2014-03-30T01:00:00", DateTimeZone.forOffsetHours(1))),
                equalTo(time("2014-03-30T02:00:00", DateTimeZone.forOffsetHours(1))));
        assertThat(tzRounding.nextRoundingValue(time("2014-03-30T02:00:00", DateTimeZone.forOffsetHours(1))),
                equalTo(time("2014-03-30T03:00:00", DateTimeZone.forOffsetHours(1))));

        // testing non savings to savings switch (America/Chicago)
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).timeZone(DateTimeZone.forID("UTC")).build();
        assertThat(tzRounding.round(time("2014-03-09T03:01:01", DateTimeZone.forID("America/Chicago"))),
                equalTo(time("2014-03-09T03:00:00", DateTimeZone.forID("America/Chicago"))));

        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).timeZone(DateTimeZone.forID("America/Chicago")).build();
        assertThat(tzRounding.round(time("2014-03-09T03:01:01", DateTimeZone.forID("America/Chicago"))),
                equalTo(time("2014-03-09T03:00:00", DateTimeZone.forID("America/Chicago"))));

        // testing savings to non savings switch 2013 (America/Chicago)
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).timeZone(DateTimeZone.forID("UTC")).build();
        assertThat(tzRounding.round(time("2013-11-03T06:01:01", DateTimeZone.forID("America/Chicago"))),
                equalTo(time("2013-11-03T06:00:00", DateTimeZone.forID("America/Chicago"))));

        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).timeZone(DateTimeZone.forID("America/Chicago")).build();
        assertThat(tzRounding.round(time("2013-11-03T06:01:01", DateTimeZone.forID("America/Chicago"))),
                equalTo(time("2013-11-03T06:00:00", DateTimeZone.forID("America/Chicago"))));

        // testing savings to non savings switch 2014 (America/Chicago)
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).timeZone(DateTimeZone.forID("UTC")).build();
        assertThat(tzRounding.round(time("2014-11-02T06:01:01", DateTimeZone.forID("America/Chicago"))),
                equalTo(time("2014-11-02T06:00:00", DateTimeZone.forID("America/Chicago"))));

        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).timeZone(DateTimeZone.forID("America/Chicago")).build();
        assertThat(tzRounding.round(time("2014-11-02T06:01:01", DateTimeZone.forID("America/Chicago"))),
                equalTo(time("2014-11-02T06:00:00", DateTimeZone.forID("America/Chicago"))));
    }

    /**
     * Randomized test on TimeUnitRounding.
     * Test uses random {@link DateTimeUnit} and {@link DateTimeZone} and often (50% of the time) chooses
     * test dates that are exactly on or close to offset changes (e.g. DST) in the chosen time zone.
     *
     * It rounds the test date down and up and performs various checks on the rounding unit interval that is
     * defined by this. Assumptions tested are described in {@link #assertInterval(long, long, long, TimeZoneRounding)}
     */
    public void testTimeZoneRoundingRandom() {
        for (int i = 0; i < 1000; ++i) {
            DateTimeUnit timeUnit = randomTimeUnit();
            DateTimeZone timezone = randomDateTimeZone();
            TimeZoneRounding rounding = new TimeZoneRounding.TimeUnitRounding(timeUnit, timezone);
            long date = Math.abs(randomLong() % (2 * (long) 10e11)); // 1970-01-01T00:00:00Z - 2033-05-18T05:33:20.000+02:00
            long unitMillis = timeUnit.field(timezone).getDurationField().getUnitMillis();
            if (randomBoolean()) {
                nastyDate(date, timezone, unitMillis);
            }
            final long roundedDate = rounding.round(date);
            final long nextRoundingValue = rounding.nextRoundingValue(roundedDate);

            assertInterval(roundedDate, date, nextRoundingValue, rounding);

            // check correct unit interval width for units smaller than a day, they should be fixed size except for transitions
            if (unitMillis <= DateTimeConstants.MILLIS_PER_DAY) {
                // if the interval defined didn't cross timezone offset transition, it should cover unitMillis width
                if (timezone.getOffset(roundedDate - 1) == timezone.getOffset(nextRoundingValue + 1)) {
                    assertThat("unit interval width not as expected for [" + timeUnit + "], [" + timezone + "] at "
                            + new DateTime(roundedDate), nextRoundingValue - roundedDate, equalTo(unitMillis));
                }
            }
        }
    }

    /**
     * To be even more nasty, go to a transition in the selected time zone.
     * In one third of the cases stay there, otherwise go half a unit back or forth
     */
    private static long nastyDate(long initialDate, DateTimeZone timezone, long unitMillis) {
        long date = timezone.nextTransition(initialDate);
        if (randomBoolean()) {
            return date + (randomLong() % unitMillis);  // positive and negative offset possible
        } else {
            return date;
        }
    }

    /**
     * randomized test on {@link TimeIntervalRounding} with random interval and time zone offsets
     */
    public void testIntervalRoundingRandom() {
        for (int i = 0; i < 1000; ++i) {
            // max random interval is a year, can be negative
            long interval = Math.abs(randomLong() % (TimeUnit.DAYS.toMillis(365)));
            TimeZoneRounding rounding;
            int timezoneOffset = randomIntBetween(-23, 23);
            rounding = new TimeZoneRounding.TimeIntervalRounding(interval, DateTimeZone.forOffsetHours(timezoneOffset));
            long date = Math.abs(randomLong() % ((long) 10e11));
            final long roundedDate = rounding.round(date);
            final long nextRoundingValue = rounding.nextRoundingValue(roundedDate);
            assertThat("Rounding should be idempotent", roundedDate, equalTo(rounding.round(roundedDate)));
            assertThat("Rounded value smaller or equal than unrounded, regardless of timezone", roundedDate, lessThanOrEqualTo(date));
            assertThat("NextRounding value should be greater than date", nextRoundingValue, greaterThan(roundedDate));
            assertThat("NextRounding value should be interval from rounded value", nextRoundingValue - roundedDate, equalTo(interval));
            assertThat("NextRounding value should be a rounded date", nextRoundingValue, equalTo(rounding.round(nextRoundingValue)));
        }
    }

    /**
     * special test for DST switch from #9491
     */
    public void testAmbiguousHoursAfterDSTSwitch() {
        Rounding tzRounding;
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).timeZone(JERUSALEM_TIMEZONE).build();
        // Both timestamps "2014-10-25T22:30:00Z" and "2014-10-25T23:30:00Z" are "2014-10-26T01:30:00" in local time because
        // of DST switch between them. This test checks that they are both returned to their correct UTC time after rounding.
        assertThat(tzRounding.round(time("2014-10-25T22:30:00", DateTimeZone.UTC)), equalTo(time("2014-10-25T22:00:00", DateTimeZone.UTC)));
        assertThat(tzRounding.round(time("2014-10-25T23:30:00", DateTimeZone.UTC)), equalTo(time("2014-10-25T23:00:00", DateTimeZone.UTC)));

        // Day interval
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.DAY_OF_MONTH).timeZone(JERUSALEM_TIMEZONE).build();
        assertThat(tzRounding.round(time("2014-11-11T17:00:00", JERUSALEM_TIMEZONE)), equalTo(time("2014-11-11T00:00:00", JERUSALEM_TIMEZONE)));
        // DST on
        assertThat(tzRounding.round(time("2014-08-11T17:00:00", JERUSALEM_TIMEZONE)), equalTo(time("2014-08-11T00:00:00", JERUSALEM_TIMEZONE)));
        // Day of switching DST on -> off
        assertThat(tzRounding.round(time("2014-10-26T17:00:00", JERUSALEM_TIMEZONE)), equalTo(time("2014-10-26T00:00:00", JERUSALEM_TIMEZONE)));
        // Day of switching DST off -> on
        assertThat(tzRounding.round(time("2015-03-27T17:00:00", JERUSALEM_TIMEZONE)), equalTo(time("2015-03-27T00:00:00", JERUSALEM_TIMEZONE)));

        // Month interval
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.MONTH_OF_YEAR).timeZone(JERUSALEM_TIMEZONE).build();
        assertThat(tzRounding.round(time("2014-11-11T17:00:00", JERUSALEM_TIMEZONE)), equalTo(time("2014-11-01T00:00:00", JERUSALEM_TIMEZONE)));
        // DST on
        assertThat(tzRounding.round(time("2014-10-10T17:00:00", JERUSALEM_TIMEZONE)), equalTo(time("2014-10-01T00:00:00", JERUSALEM_TIMEZONE)));

        // Year interval
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.YEAR_OF_CENTURY).timeZone(JERUSALEM_TIMEZONE).build();
        assertThat(tzRounding.round(time("2014-11-11T17:00:00", JERUSALEM_TIMEZONE)), equalTo(time("2014-01-01T00:00:00", JERUSALEM_TIMEZONE)));

        // Two timestamps in same year and different timezone offset ("Double buckets" issue - #9491)
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.YEAR_OF_CENTURY).timeZone(JERUSALEM_TIMEZONE).build();
        assertThat(tzRounding.round(time("2014-11-11T17:00:00", JERUSALEM_TIMEZONE)),
                equalTo(tzRounding.round(time("2014-08-11T17:00:00", JERUSALEM_TIMEZONE))));
    }

    /**
     * test for #10025, strict local to UTC conversion can cause joda exceptions
     * on DST start
     */
    public void testLenientConversionDST() {
        DateTimeZone tz = DateTimeZone.forID("America/Sao_Paulo");
        long start = time("2014-10-18T20:50:00.000", tz);
        long end = time("2014-10-19T01:00:00.000", tz);
        Rounding tzRounding = new TimeZoneRounding.TimeUnitRounding(DateTimeUnit.MINUTES_OF_HOUR, tz);
        Rounding dayTzRounding = new TimeZoneRounding.TimeIntervalRounding(60000, tz);
        for (long time = start; time < end; time = time + 60000) {
            assertThat(tzRounding.nextRoundingValue(time), greaterThan(time));
            assertThat(dayTzRounding.nextRoundingValue(time), greaterThan(time));
        }
    }

    public void testEdgeCasesTransition() {
        {
            // standard +/-1 hour DST transition, CET
            DateTimeUnit timeUnit = DateTimeUnit.HOUR_OF_DAY;
            DateTimeZone timezone = DateTimeZone.forID("CET");
            TimeZoneRounding rounding = new TimeZoneRounding.TimeUnitRounding(timeUnit, timezone);

            // 29 Mar 2015 - Daylight Saving Time Started
            // at 02:00:00 clocks were turned forward 1 hour to 03:00:00
            assertInterval(time("2015-03-29T00:00:00.000+01:00"), time("2015-03-29T01:00:00.000+01:00"), rounding, 60);
            assertInterval(time("2015-03-29T01:00:00.000+01:00"), time("2015-03-29T03:00:00.000+02:00"), rounding, 60);
            assertInterval(time("2015-03-29T03:00:00.000+02:00"), time("2015-03-29T04:00:00.000+02:00"), rounding, 60);

            // 25 Oct 2015 - Daylight Saving Time Ended
            // at 03:00:00 clocks were turned backward 1 hour to 02:00:00
            assertInterval(time("2015-10-25T01:00:00.000+02:00"), time("2015-10-25T02:00:00.000+02:00"), rounding, 60);
            assertInterval(time("2015-10-25T02:00:00.000+02:00"), time("2015-10-25T02:00:00.000+01:00"), rounding, 60);
            assertInterval(time("2015-10-25T02:00:00.000+01:00"), time("2015-10-25T03:00:00.000+01:00"), rounding, 60);
        }

        {
            // time zone "Asia/Kathmandu"
            // 1 Jan 1986 - Time Zone Change (IST → NPT), at 00:00:00 clocks were turned forward 00:15 minutes
            //
            // hour rounding is stable before 1985-12-31T23:00:00.000 and after 1986-01-01T01:00:00.000+05:45
            // the interval between is 105 minutes long because the hour after transition starts at 00:15
            // which is not a round value for hourly rounding
            DateTimeUnit timeUnit = DateTimeUnit.HOUR_OF_DAY;
            DateTimeZone timezone = DateTimeZone.forID("Asia/Kathmandu");
            TimeZoneRounding rounding = new TimeZoneRounding.TimeUnitRounding(timeUnit, timezone);

            assertInterval(time("1985-12-31T22:00:00.000+05:30"), time("1985-12-31T23:00:00.000+05:30"), rounding, 60);
            assertInterval(time("1985-12-31T23:00:00.000+05:30"), time("1986-01-01T01:00:00.000+05:45"), rounding, 105);
            assertInterval(time("1986-01-01T01:00:00.000+05:45"), time("1986-01-01T02:00:00.000+05:45"), rounding, 60);
        }

        {
            // time zone "Australia/Lord_Howe"
            // 3 Mar 1991 - Daylight Saving Time Ended
            // at 02:00:00 clocks were turned backward 0:30 hours to Sunday, 3 March 1991, 01:30:00
            DateTimeUnit timeUnit = DateTimeUnit.HOUR_OF_DAY;
            DateTimeZone timezone = DateTimeZone.forID("Australia/Lord_Howe");
            TimeZoneRounding rounding = new TimeZoneRounding.TimeUnitRounding(timeUnit, timezone);

            assertInterval(time("1991-03-03T00:00:00.000+11:00"), time("1991-03-03T01:00:00.000+11:00"), rounding, 60);
            assertInterval(time("1991-03-03T01:00:00.000+11:00"), time("1991-03-03T02:00:00.000+10:30"), rounding, 90);
            assertInterval(time("1991-03-03T02:00:00.000+10:30"), time("1991-03-03T03:00:00.000+10:30"), rounding, 60);

            // 27 Oct 1991 - Daylight Saving Time Started
            // at 02:00:00 clocks were turned forward 0:30 hours to 02:30:00
            assertInterval(time("1991-10-27T00:00:00.000+10:30"), time("1991-10-27T01:00:00.000+10:30"), rounding, 60);
            // the interval containing the switch time is 90 minutes long
            assertInterval(time("1991-10-27T01:00:00.000+10:30"), time("1991-10-27T03:00:00.000+11:00"), rounding, 90);
            assertInterval(time("1991-10-27T03:00:00.000+11:00"), time("1991-10-27T04:00:00.000+11:00"), rounding, 60);
        }

        {
            // time zone "Pacific/Chatham"
            // 5 Apr 2015 - Daylight Saving Time Ended
            // at 03:45:00 clocks were turned backward 1 hour to 02:45:00
            DateTimeUnit timeUnit = DateTimeUnit.HOUR_OF_DAY;
            DateTimeZone timezone = DateTimeZone.forID("Pacific/Chatham");
            TimeZoneRounding rounding = new TimeZoneRounding.TimeUnitRounding(timeUnit, timezone);

            assertInterval(time("2015-04-05T02:00:00.000+13:45"), time("2015-04-05T03:00:00.000+13:45"), rounding, 60);
            assertInterval(time("2015-04-05T03:00:00.000+13:45"), time("2015-04-05T03:00:00.000+12:45"), rounding, 60);
            assertInterval(time("2015-04-05T03:00:00.000+12:45"), time("2015-04-05T04:00:00.000+12:45"), rounding, 60);

            // 27 Sep 2015 - Daylight Saving Time Started
            // at 02:45:00 clocks were turned forward 1 hour to 03:45:00

            assertInterval(time("2015-09-27T01:00:00.000+12:45"), time("2015-09-27T02:00:00.000+12:45"), rounding, 60);
            assertInterval(time("2015-09-27T02:00:00.000+12:45"), time("2015-09-27T04:00:00.000+13:45"), rounding, 60);
            assertInterval(time("2015-09-27T04:00:00.000+13:45"), time("2015-09-27T05:00:00.000+13:45"), rounding, 60);
        }
    }

    private static void assertInterval(long rounded, long nextRoundingValue, TimeZoneRounding rounding, int minutes) {
        assertInterval(rounded, dateBetween(rounded, nextRoundingValue), nextRoundingValue, rounding);
        assertEquals(DateTimeConstants.MILLIS_PER_MINUTE * minutes, nextRoundingValue - rounded);
    }

    /**
     * perform a number on assertions and checks on {@link TimeUnitRounding} intervals
     * @param rounded the expected low end of the rounding interval
     * @param unrounded a date in the interval to be checked for rounding
     * @param nextRoundingValue the expected upper end of the rounding interval
     * @param rounding the rounding instance
     */
    private static void assertInterval(long rounded, long unrounded, long nextRoundingValue, TimeZoneRounding rounding) {
        assert rounded <= unrounded && unrounded <= nextRoundingValue;
        assertThat("rounding should be idempotent " + rounding, rounded, equalTo(rounding.round(rounded)));
        assertThat("rounded value smaller or equal than unrounded" + rounding, rounded, lessThanOrEqualTo(unrounded));
        assertThat("values less than rounded should round further down" + rounding, rounding.round(rounded - 1), lessThan(rounded));
        assertThat("nextRounding value should be greater than date" + rounding, nextRoundingValue, greaterThan(unrounded));
        assertThat("nextRounding value should be a rounded date" + rounding, nextRoundingValue, equalTo(rounding.round(nextRoundingValue)));
        assertThat("values above nextRounding should round down there" + rounding, rounding.round(nextRoundingValue + 1),
                equalTo(nextRoundingValue));

        long dateBetween = dateBetween(rounded, nextRoundingValue);
        assertThat("dateBetween should round down to roundedDate" + rounding, rounding.round(dateBetween), equalTo(rounded));
        assertThat("dateBetween should round up to nextRoundingValue" + rounding, rounding.nextRoundingValue(dateBetween),
                equalTo(nextRoundingValue));
    }

    private static long dateBetween(long lower, long upper) {
        long dateBetween = lower + Math.abs((randomLong() % (upper - lower)));
        assert lower <= dateBetween && dateBetween <= upper;
        return dateBetween;
    }

    private static DateTimeUnit randomTimeUnit() {
        byte id = (byte) randomIntBetween(1, 8);
        return DateTimeUnit.resolve(id);
    }

    private static String toUTCDateString(long time) {
        return new DateTime(time, DateTimeZone.UTC).toString();
    }

    private static long utc(String time) {
        return time(time, DateTimeZone.UTC);
    }

    private static long time(String time) {
        return ISODateTimeFormat.dateOptionalTimeParser().parseMillis(time);
    }

    private static long time(String time, DateTimeZone zone) {
        return ISODateTimeFormat.dateOptionalTimeParser().withZone(zone).parseMillis(time);
    }
}
