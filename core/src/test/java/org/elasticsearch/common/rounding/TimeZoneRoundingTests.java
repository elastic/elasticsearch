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

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 */
public class TimeZoneRoundingTests extends ESTestCase {

    final static DateTimeZone JERUSALEM_TIMEZONE = DateTimeZone.forID("Asia/Jerusalem");

    @Test
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

    @Test
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
     * test TimeIntervalTimeZoneRounding, (interval < 12h) with time zone shift
     */
    @Test
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
     * test DayIntervalTimeZoneRounding, (interval >= 12h) with time zone shift
     */
    @Test
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

    @Test
    public void testDayTimeZoneRounding() {
        int timezoneOffset = -2;
        Rounding tzRounding = TimeZoneRounding.builder(DateTimeUnit.DAY_OF_MONTH).timeZone(DateTimeZone.forOffsetHours(timezoneOffset))
                .build();
        assertThat(tzRounding.round(0), equalTo(0l - TimeValue.timeValueHours(24 + timezoneOffset).millis()));
        assertThat(tzRounding.nextRoundingValue(0l - TimeValue.timeValueHours(24 + timezoneOffset).millis()), equalTo(0l - TimeValue
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

    @Test
    public void testTimeTimeZoneRounding() {
        // hour unit
        Rounding tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).timeZone(DateTimeZone.forOffsetHours(-2)).build();
        assertThat(tzRounding.round(0), equalTo(0l));
        assertThat(tzRounding.nextRoundingValue(0l), equalTo(TimeValue.timeValueHours(1l).getMillis()));

        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).timeZone(DateTimeZone.forOffsetHours(-2)).build();
        assertThat(tzRounding.round(utc("2009-02-03T01:01:01")), equalTo(utc("2009-02-03T01:00:00")));
        assertThat(tzRounding.nextRoundingValue(utc("2009-02-03T01:00:00")), equalTo(utc("2009-02-03T02:00:00")));
    }

    @Test
    public void testTimeUnitRoundingDST() {
        Rounding tzRounding;
        // testing savings to non savings switch
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).timeZone(DateTimeZone.forID("UTC")).build();
        assertThat(tzRounding.round(time("2014-10-26T01:01:01", DateTimeZone.forID("CET"))),
                equalTo(time("2014-10-26T01:00:00", DateTimeZone.forID("CET"))));

        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).timeZone(DateTimeZone.forID("CET")).build();
        assertThat(tzRounding.round(time("2014-10-26T01:01:01", DateTimeZone.forID("CET"))),
                equalTo(time("2014-10-26T01:00:00", DateTimeZone.forID("CET"))));

        // testing non savings to savings switch
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).timeZone(DateTimeZone.forID("UTC")).build();
        assertThat(tzRounding.round(time("2014-03-30T01:01:01", DateTimeZone.forID("CET"))),
                equalTo(time("2014-03-30T01:00:00", DateTimeZone.forID("CET"))));

        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).timeZone(DateTimeZone.forID("CET")).build();
        assertThat(tzRounding.round(time("2014-03-30T01:01:01", DateTimeZone.forID("CET"))),
                equalTo(time("2014-03-30T01:00:00", DateTimeZone.forID("CET"))));

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
     * randomized test on TimeUnitRounding with random time units and time zone offsets
     */
    @Test
    public void testTimeZoneRoundingRandom() {
        for (int i = 0; i < 1000; ++i) {
            DateTimeUnit timeUnit = randomTimeUnit();
            TimeZoneRounding rounding;
            int timezoneOffset = randomIntBetween(-23, 23);
            rounding = new TimeZoneRounding.TimeUnitRounding(timeUnit, DateTimeZone.forOffsetHours(timezoneOffset));
            long date = Math.abs(randomLong() % ((long) 10e11));
            final long roundedDate = rounding.round(date);
            final long nextRoundingValue = rounding.nextRoundingValue(roundedDate);
            assertThat("Rounding should be idempotent", roundedDate, equalTo(rounding.round(roundedDate)));
            assertThat("Rounded value smaller or equal than unrounded, regardless of timezone", roundedDate, lessThanOrEqualTo(date));
            assertThat("NextRounding value should be greater than date", nextRoundingValue, greaterThan(roundedDate));
            assertThat("NextRounding value should be a rounded date", nextRoundingValue, equalTo(rounding.round(nextRoundingValue)));
        }
    }

    /**
     * randomized test on TimeIntervalRounding with random interval and time zone offsets
     */
    @Test
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
    @Test
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
    @Test
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

    private DateTimeUnit randomTimeUnit() {
        byte id = (byte) randomIntBetween(1, 8);
        return DateTimeUnit.resolve(id);
    }

    private String toUTCDateString(long time) {
        return new DateTime(time, DateTimeZone.UTC).toString();
    }

    private long utc(String time) {
        return time(time, DateTimeZone.UTC);
    }

    private long time(String time, DateTimeZone zone) {
        return ISODateTimeFormat.dateOptionalTimeParser().withZone(zone).parseMillis(time);
    }
}
