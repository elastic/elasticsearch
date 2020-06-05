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

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.rounding.Rounding.TimeIntervalRounding;
import org.elasticsearch.common.rounding.Rounding.TimeUnitRounding;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.startsWith;

public class TimeZoneRoundingTests extends ESTestCase {

    public void testUTCTimeUnitRounding() {
        Rounding tzRounding = Rounding.builder(DateTimeUnit.MONTH_OF_YEAR).build();
        DateTimeZone tz = DateTimeZone.UTC;
        assertThat(tzRounding.round(time("2009-02-03T01:01:01")), isDate(time("2009-02-01T00:00:00.000Z"), tz));
        assertThat(tzRounding.nextRoundingValue(time("2009-02-01T00:00:00.000Z")), isDate(time("2009-03-01T00:00:00.000Z"), tz));

        tzRounding = Rounding.builder(DateTimeUnit.WEEK_OF_WEEKYEAR).build();
        assertThat(tzRounding.round(time("2012-01-10T01:01:01")), isDate(time("2012-01-09T00:00:00.000Z"), tz));
        assertThat(tzRounding.nextRoundingValue(time("2012-01-09T00:00:00.000Z")), isDate(time("2012-01-16T00:00:00.000Z"), tz));

        tzRounding = Rounding.builder(DateTimeUnit.QUARTER).build();
        assertThat(tzRounding.round(time("2012-01-10T01:01:01")), isDate(time("2012-01-01T00:00:00.000Z"), tz));
        assertThat(tzRounding.nextRoundingValue(time("2012-01-09T00:00:00.000Z")), isDate(time("2012-04-01T00:00:00.000Z"), tz));

        tzRounding = Rounding.builder(DateTimeUnit.HOUR_OF_DAY).build();
        assertThat(tzRounding.round(time("2012-01-10T01:01:01")), isDate(time("2012-01-10T01:00:00.000Z"), tz));
        assertThat(tzRounding.nextRoundingValue(time("2012-01-09T00:00:00.000Z")), isDate(time("2012-01-09T01:00:00.000Z"), tz));

        tzRounding = Rounding.builder(DateTimeUnit.DAY_OF_MONTH).build();
        assertThat(tzRounding.round(time("2012-01-10T01:01:01")), isDate(time("2012-01-10T00:00:00.000Z"), tz));
        assertThat(tzRounding.nextRoundingValue(time("2012-01-09T00:00:00.000Z")), isDate(time("2012-01-10T00:00:00.000Z"), tz));

        tzRounding = Rounding.builder(DateTimeUnit.YEAR_OF_CENTURY).build();
        assertThat(tzRounding.round(time("2012-01-10T01:01:01")), isDate(time("2012-01-01T00:00:00.000Z"), tz));
        assertThat(tzRounding.nextRoundingValue(time("2012-01-09T00:00:00.000Z")), isDate(time("2013-01-01T00:00:00.000Z"), tz));

        tzRounding = Rounding.builder(DateTimeUnit.MINUTES_OF_HOUR).build();
        assertThat(tzRounding.round(time("2012-01-10T01:01:01")), isDate(time("2012-01-10T01:01:00.000Z"), tz));
        assertThat(tzRounding.nextRoundingValue(time("2012-01-09T00:00:00.000Z")), isDate(time("2012-01-09T00:01:00.000Z"), tz));

        tzRounding = Rounding.builder(DateTimeUnit.SECOND_OF_MINUTE).build();
        assertThat(tzRounding.round(time("2012-01-10T01:01:01")), isDate(time("2012-01-10T01:01:01.000Z"), tz));
        assertThat(tzRounding.nextRoundingValue(time("2012-01-09T00:00:00.000Z")), isDate(time("2012-01-09T00:00:01.000Z"), tz));
    }

    public void testUTCIntervalRounding() {
        Rounding tzRounding = Rounding.builder(TimeValue.timeValueHours(12)).build();
        DateTimeZone tz = DateTimeZone.UTC;
        assertThat(tzRounding.round(time("2009-02-03T01:01:01")), isDate(time("2009-02-03T00:00:00.000Z"), tz));
        assertThat(tzRounding.nextRoundingValue(time("2009-02-03T00:00:00.000Z")), isDate(time("2009-02-03T12:00:00.000Z"), tz));
        assertThat(tzRounding.round(time("2009-02-03T13:01:01")), isDate(time("2009-02-03T12:00:00.000Z"), tz));
        assertThat(tzRounding.nextRoundingValue(time("2009-02-03T12:00:00.000Z")), isDate(time("2009-02-04T00:00:00.000Z"), tz));

        tzRounding = Rounding.builder(TimeValue.timeValueHours(48)).build();
        assertThat(tzRounding.round(time("2009-02-03T01:01:01")), isDate(time("2009-02-03T00:00:00.000Z"), tz));
        assertThat(tzRounding.nextRoundingValue(time("2009-02-03T00:00:00.000Z")), isDate(time("2009-02-05T00:00:00.000Z"), tz));
        assertThat(tzRounding.round(time("2009-02-05T13:01:01")), isDate(time("2009-02-05T00:00:00.000Z"), tz));
        assertThat(tzRounding.nextRoundingValue(time("2009-02-05T00:00:00.000Z")), isDate(time("2009-02-07T00:00:00.000Z"), tz));
    }

    /**
     * test TimeIntervalRounding, (interval &lt; 12h) with time zone shift
     */
    public void testTimeIntervalRounding() {
        DateTimeZone tz = DateTimeZone.forOffsetHours(-1);
        Rounding tzRounding = Rounding.builder(TimeValue.timeValueHours(6)).timeZone(tz).build();
        assertThat(tzRounding.round(time("2009-02-03T00:01:01")), isDate(time("2009-02-02T19:00:00.000Z"), tz));
        assertThat(tzRounding.nextRoundingValue(time("2009-02-02T19:00:00.000Z")), isDate(time("2009-02-03T01:00:00.000Z"), tz));

        assertThat(tzRounding.round(time("2009-02-03T13:01:01")), isDate(time("2009-02-03T13:00:00.000Z"), tz));
        assertThat(tzRounding.nextRoundingValue(time("2009-02-03T13:00:00.000Z")), isDate(time("2009-02-03T19:00:00.000Z"), tz));
    }

    /**
     * test DayIntervalRounding, (interval &gt;= 12h) with time zone shift
     */
    public void testDayIntervalRounding() {
        DateTimeZone tz = DateTimeZone.forOffsetHours(-8);
        Rounding tzRounding = Rounding.builder(TimeValue.timeValueHours(12)).timeZone(tz).build();
        assertThat(tzRounding.round(time("2009-02-03T00:01:01")), isDate(time("2009-02-02T20:00:00.000Z"), tz));
        assertThat(tzRounding.nextRoundingValue(time("2009-02-02T20:00:00.000Z")), isDate(time("2009-02-03T08:00:00.000Z"), tz));

        assertThat(tzRounding.round(time("2009-02-03T13:01:01")), isDate(time("2009-02-03T08:00:00.000Z"), tz));
        assertThat(tzRounding.nextRoundingValue(time("2009-02-03T08:00:00.000Z")), isDate(time("2009-02-03T20:00:00.000Z"), tz));
    }

    public void testDayRounding() {
        int timezoneOffset = -2;
        Rounding tzRounding = Rounding.builder(DateTimeUnit.DAY_OF_MONTH).timeZone(DateTimeZone.forOffsetHours(timezoneOffset))
                .build();
        assertThat(tzRounding.round(0), equalTo(0L - TimeValue.timeValueHours(24 + timezoneOffset).millis()));
        assertThat(tzRounding.nextRoundingValue(0L - TimeValue.timeValueHours(24 + timezoneOffset).millis()), equalTo(TimeValue
                .timeValueHours(-timezoneOffset).millis()));

        DateTimeZone tz = DateTimeZone.forID("-08:00");
        tzRounding = Rounding.builder(DateTimeUnit.DAY_OF_MONTH).timeZone(tz).build();
        assertThat(tzRounding.round(time("2012-04-01T04:15:30Z")), isDate(time("2012-03-31T08:00:00Z"), tz));

        tzRounding = Rounding.builder(DateTimeUnit.MONTH_OF_YEAR).timeZone(tz).build();
        assertThat(tzRounding.round(time("2012-04-01T04:15:30Z")), equalTo(time("2012-03-01T08:00:00Z")));

        // date in Feb-3rd, but still in Feb-2nd in -02:00 timezone
        tz = DateTimeZone.forID("-02:00");
        tzRounding = Rounding.builder(DateTimeUnit.DAY_OF_MONTH).timeZone(tz).build();
        assertThat(tzRounding.round(time("2009-02-03T01:01:01")), isDate(time("2009-02-02T02:00:00"), tz));
        assertThat(tzRounding.nextRoundingValue(time("2009-02-02T02:00:00")), isDate(time("2009-02-03T02:00:00"), tz));

        // date in Feb-3rd, also in -02:00 timezone
        tzRounding = Rounding.builder(DateTimeUnit.DAY_OF_MONTH).timeZone(tz).build();
        assertThat(tzRounding.round(time("2009-02-03T02:01:01")), isDate(time("2009-02-03T02:00:00"), tz));
        assertThat(tzRounding.nextRoundingValue(time("2009-02-03T02:00:00")), isDate(time("2009-02-04T02:00:00"), tz));
    }

    public void testTimeRounding() {
        // hour unit
        DateTimeZone tz = DateTimeZone.forOffsetHours(-2);
        Rounding tzRounding = Rounding.builder(DateTimeUnit.HOUR_OF_DAY).timeZone(tz).build();
        assertThat(tzRounding.round(0), equalTo(0L));
        assertThat(tzRounding.nextRoundingValue(0L), equalTo(TimeValue.timeValueHours(1L).getMillis()));

        assertThat(tzRounding.round(time("2009-02-03T01:01:01")), isDate(time("2009-02-03T01:00:00"), tz));
        assertThat(tzRounding.nextRoundingValue(time("2009-02-03T01:00:00")), isDate(time("2009-02-03T02:00:00"), tz));
    }

    public void testTimeUnitRoundingDST() {
        Rounding tzRounding;
        // testing savings to non savings switch
        DateTimeZone cet = DateTimeZone.forID("CET");
        tzRounding = Rounding.builder(DateTimeUnit.HOUR_OF_DAY).timeZone(cet).build();
        assertThat(tzRounding.round(time("2014-10-26T01:01:01", cet)), isDate(time("2014-10-26T01:00:00+02:00"), cet));
        assertThat(tzRounding.nextRoundingValue(time("2014-10-26T01:00:00", cet)),isDate(time("2014-10-26T02:00:00+02:00"), cet));
        assertThat(tzRounding.nextRoundingValue(time("2014-10-26T02:00:00", cet)), isDate(time("2014-10-26T02:00:00+01:00"), cet));

        // testing non savings to savings switch
        tzRounding = Rounding.builder(DateTimeUnit.HOUR_OF_DAY).timeZone(cet).build();
        assertThat(tzRounding.round(time("2014-03-30T01:01:01", cet)), isDate(time("2014-03-30T01:00:00+01:00"), cet));
        assertThat(tzRounding.nextRoundingValue(time("2014-03-30T01:00:00", cet)), isDate(time("2014-03-30T03:00:00", cet), cet));
        assertThat(tzRounding.nextRoundingValue(time("2014-03-30T03:00:00", cet)), isDate(time("2014-03-30T04:00:00", cet), cet));

        // testing non savings to savings switch (America/Chicago)
        DateTimeZone chg = DateTimeZone.forID("America/Chicago");
        Rounding tzRounding_utc = Rounding.builder(DateTimeUnit.HOUR_OF_DAY).timeZone(DateTimeZone.UTC).build();
        assertThat(tzRounding.round(time("2014-03-09T03:01:01", chg)), isDate(time("2014-03-09T03:00:00", chg), chg));

        Rounding tzRounding_chg = Rounding.builder(DateTimeUnit.HOUR_OF_DAY).timeZone(chg).build();
        assertThat(tzRounding_chg.round(time("2014-03-09T03:01:01", chg)), isDate(time("2014-03-09T03:00:00", chg), chg));

        // testing savings to non savings switch 2013 (America/Chicago)
        assertThat(tzRounding_utc.round(time("2013-11-03T06:01:01", chg)), isDate(time("2013-11-03T06:00:00", chg), chg));
        assertThat(tzRounding_chg.round(time("2013-11-03T06:01:01", chg)), isDate(time("2013-11-03T06:00:00", chg), chg));

        // testing savings to non savings switch 2014 (America/Chicago)
        assertThat(tzRounding_utc.round(time("2014-11-02T06:01:01", chg)), isDate(time("2014-11-02T06:00:00", chg), chg));
        assertThat(tzRounding_chg.round(time("2014-11-02T06:01:01", chg)), isDate(time("2014-11-02T06:00:00", chg), chg));
    }

    /**
     * Randomized test on TimeUnitRounding. Test uses random
     * {@link DateTimeUnit} and {@link DateTimeZone} and often (50% of the time)
     * chooses test dates that are exactly on or close to offset changes (e.g.
     * DST) in the chosen time zone.
     *
     * It rounds the test date down and up and performs various checks on the
     * rounding unit interval that is defined by this. Assumptions tested are
     * described in
     * {@link #assertInterval(long, long, long, Rounding, DateTimeZone)}
     */
    public void testRoundingRandom() {
        for (int i = 0; i < 1000; ++i) {
            DateTimeUnit timeUnit = randomTimeUnit();
            DateTimeZone tz = randomDateTimeZone();
            Rounding rounding = new Rounding.TimeUnitRounding(timeUnit, tz);
            long date = Math.abs(randomLong() % (2 * (long) 10e11)); // 1970-01-01T00:00:00Z - 2033-05-18T05:33:20.000+02:00
            long unitMillis = timeUnit.field(tz).getDurationField().getUnitMillis();
            if (randomBoolean()) {
                nastyDate(date, tz, unitMillis);
            }
            final long roundedDate = rounding.round(date);
            final long nextRoundingValue = rounding.nextRoundingValue(roundedDate);

            assertInterval(roundedDate, date, nextRoundingValue, rounding, tz);

            // check correct unit interval width for units smaller than a day, they should be fixed size except for transitions
            if (unitMillis <= DateTimeConstants.MILLIS_PER_DAY) {
                // if the interval defined didn't cross timezone offset transition, it should cover unitMillis width
                if (tz.getOffset(roundedDate - 1) == tz.getOffset(nextRoundingValue + 1)) {
                    assertThat("unit interval width not as expected for [" + timeUnit + "], [" + tz + "] at "
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
     * test DST end with interval rounding
     * CET: 25 October 2015, 03:00:00 clocks were turned backward 1 hour to 25 October 2015, 02:00:00 local standard time
     */
    public void testTimeIntervalCET_DST_End() {
        long interval = TimeUnit.MINUTES.toMillis(20);
        DateTimeZone tz = DateTimeZone.forID("CET");
        Rounding rounding = new TimeIntervalRounding(interval, tz);

        assertThat(rounding.round(time("2015-10-25T01:55:00+02:00")), isDate(time("2015-10-25T01:40:00+02:00"), tz));
        assertThat(rounding.round(time("2015-10-25T02:15:00+02:00")), isDate(time("2015-10-25T02:00:00+02:00"), tz));
        assertThat(rounding.round(time("2015-10-25T02:35:00+02:00")), isDate(time("2015-10-25T02:20:00+02:00"), tz));
        assertThat(rounding.round(time("2015-10-25T02:55:00+02:00")), isDate(time("2015-10-25T02:40:00+02:00"), tz));
        // after DST shift
        assertThat(rounding.round(time("2015-10-25T02:15:00+01:00")), isDate(time("2015-10-25T02:00:00+01:00"), tz));
        assertThat(rounding.round(time("2015-10-25T02:35:00+01:00")), isDate(time("2015-10-25T02:20:00+01:00"), tz));
        assertThat(rounding.round(time("2015-10-25T02:55:00+01:00")), isDate(time("2015-10-25T02:40:00+01:00"), tz));
        assertThat(rounding.round(time("2015-10-25T03:15:00+01:00")), isDate(time("2015-10-25T03:00:00+01:00"), tz));
    }

    /**
     * test DST start with interval rounding
     * CET: 27 March 2016, 02:00:00 clocks were turned forward 1 hour to 27 March 2016, 03:00:00 local daylight time
     */
    public void testTimeIntervalCET_DST_Start() {
        long interval = TimeUnit.MINUTES.toMillis(20);
        DateTimeZone tz = DateTimeZone.forID("CET");
        Rounding rounding = new TimeIntervalRounding(interval, tz);
        // test DST start
        assertThat(rounding.round(time("2016-03-27T01:55:00+01:00")), isDate(time("2016-03-27T01:40:00+01:00"), tz));
        assertThat(rounding.round(time("2016-03-27T02:00:00+01:00")), isDate(time("2016-03-27T03:00:00+02:00"), tz));
        assertThat(rounding.round(time("2016-03-27T03:15:00+02:00")), isDate(time("2016-03-27T03:00:00+02:00"), tz));
        assertThat(rounding.round(time("2016-03-27T03:35:00+02:00")), isDate(time("2016-03-27T03:20:00+02:00"), tz));
    }

    /**
     * test DST start with offset not fitting interval, e.g. Asia/Kathmandu
     * adding 15min on 1986-01-01T00:00:00 the interval from
     * 1986-01-01T00:15:00+05:45 to 1986-01-01T00:20:00+05:45 to only be 5min
     * long
     */
    public void testTimeInterval_Kathmandu_DST_Start() {
        long interval = TimeUnit.MINUTES.toMillis(20);
        DateTimeZone tz = DateTimeZone.forID("Asia/Kathmandu");
        Rounding rounding = new TimeIntervalRounding(interval, tz);
        assertThat(rounding.round(time("1985-12-31T23:55:00+05:30")), isDate(time("1985-12-31T23:40:00+05:30"), tz));
        assertThat(rounding.round(time("1986-01-01T00:16:00+05:45")), isDate(time("1986-01-01T00:15:00+05:45"), tz));
        assertThat(time("1986-01-01T00:15:00+05:45") - time("1985-12-31T23:40:00+05:30"), equalTo(TimeUnit.MINUTES.toMillis(20)));
        assertThat(rounding.round(time("1986-01-01T00:26:00+05:45")), isDate(time("1986-01-01T00:20:00+05:45"), tz));
        assertThat(time("1986-01-01T00:20:00+05:45") - time("1986-01-01T00:15:00+05:45"), equalTo(TimeUnit.MINUTES.toMillis(5)));
        assertThat(rounding.round(time("1986-01-01T00:46:00+05:45")), isDate(time("1986-01-01T00:40:00+05:45"), tz));
        assertThat(time("1986-01-01T00:40:00+05:45") - time("1986-01-01T00:20:00+05:45"), equalTo(TimeUnit.MINUTES.toMillis(20)));
    }

    /**
     * Special test for intervals that don't fit evenly into rounding interval.
     * In this case, when interval crosses DST transition point, rounding in local
     * time can land in a DST gap which results in wrong UTC rounding values.
     */
    public void testIntervalRounding_NotDivisibleInteval() {
        DateTimeZone tz = DateTimeZone.forID("CET");
        long interval = TimeUnit.MINUTES.toMillis(14);
        Rounding rounding = new Rounding.TimeIntervalRounding(interval, tz);

        assertThat(rounding.round(time("2016-03-27T01:41:00+01:00")), isDate(time("2016-03-27T01:30:00+01:00"), tz));
        assertThat(rounding.round(time("2016-03-27T01:51:00+01:00")), isDate(time("2016-03-27T01:44:00+01:00"), tz));
        assertThat(rounding.round(time("2016-03-27T01:59:00+01:00")), isDate(time("2016-03-27T01:58:00+01:00"), tz));
        assertThat(rounding.round(time("2016-03-27T03:05:00+02:00")), isDate(time("2016-03-27T03:00:00+02:00"), tz));
        assertThat(rounding.round(time("2016-03-27T03:12:00+02:00")), isDate(time("2016-03-27T03:08:00+02:00"), tz));
        assertThat(rounding.round(time("2016-03-27T03:25:00+02:00")), isDate(time("2016-03-27T03:22:00+02:00"), tz));
        assertThat(rounding.round(time("2016-03-27T03:39:00+02:00")), isDate(time("2016-03-27T03:36:00+02:00"), tz));
    }

    /**
     * Test for half day rounding intervals scrossing DST.
     */
    public void testIntervalRounding_HalfDay_DST() {
        DateTimeZone tz = DateTimeZone.forID("CET");
        long interval = TimeUnit.HOURS.toMillis(12);
        Rounding rounding = new Rounding.TimeIntervalRounding(interval, tz);

        assertThat(rounding.round(time("2016-03-26T01:00:00+01:00")), isDate(time("2016-03-26T00:00:00+01:00"), tz));
        assertThat(rounding.round(time("2016-03-26T13:00:00+01:00")), isDate(time("2016-03-26T12:00:00+01:00"), tz));
        assertThat(rounding.round(time("2016-03-27T01:00:00+01:00")), isDate(time("2016-03-27T00:00:00+01:00"), tz));
        assertThat(rounding.round(time("2016-03-27T13:00:00+02:00")), isDate(time("2016-03-27T12:00:00+02:00"), tz));
        assertThat(rounding.round(time("2016-03-28T01:00:00+02:00")), isDate(time("2016-03-28T00:00:00+02:00"), tz));
        assertThat(rounding.round(time("2016-03-28T13:00:00+02:00")), isDate(time("2016-03-28T12:00:00+02:00"), tz));
    }

    /**
     * randomized test on {@link TimeIntervalRounding} with random interval and time zone offsets
     */
    public void testIntervalRoundingRandom() {
        for (int i = 0; i < 1000; i++) {
            TimeUnit unit = randomFrom(new TimeUnit[] {TimeUnit.MINUTES, TimeUnit.HOURS, TimeUnit.DAYS});
            long interval = unit.toMillis(randomIntBetween(1, 365));
            DateTimeZone tz = randomDateTimeZone();
            Rounding rounding = new Rounding.TimeIntervalRounding(interval, tz);
            long mainDate = Math.abs(randomLong() % (2 * (long) 10e11)); // 1970-01-01T00:00:00Z - 2033-05-18T05:33:20.000+02:00
            if (randomBoolean()) {
                mainDate = nastyDate(mainDate, tz, interval);
            }
            // check two intervals around date
            long previousRoundedValue = Long.MIN_VALUE;
            for (long date = mainDate - 2 * interval; date < mainDate + 2 * interval; date += interval / 2) {
                try {
                    final long roundedDate = rounding.round(date);
                    final long nextRoundingValue = rounding.nextRoundingValue(roundedDate);
                    assertThat("Rounding should be idempotent", roundedDate, equalTo(rounding.round(roundedDate)));
                    assertThat("Rounded value smaller or equal than unrounded", roundedDate, lessThanOrEqualTo(date));
                    assertThat("Values smaller than rounded value should round further down", rounding.round(roundedDate - 1),
                            lessThan(roundedDate));
                    assertThat("Rounding should be >= previous rounding value", roundedDate, greaterThanOrEqualTo(previousRoundedValue));

                    if (tz.isFixed()) {
                        assertThat("NextRounding value should be greater than date", nextRoundingValue, greaterThan(roundedDate));
                        assertThat("NextRounding value should be interval from rounded value", nextRoundingValue - roundedDate,
                                equalTo(interval));
                        assertThat("NextRounding value should be a rounded date", nextRoundingValue,
                                equalTo(rounding.round(nextRoundingValue)));
                    }
                    previousRoundedValue = roundedDate;
                } catch (AssertionError e) {
                    logger.error("Rounding error at {}, timezone {}, interval: {},", new DateTime(date, tz), tz, interval);
                    throw e;
                }
            }
        }
    }

    /**
     * Test that rounded values are always greater or equal to last rounded value if date is increasing.
     * The example covers an interval around 2011-10-30T02:10:00+01:00, time zone CET, interval: 2700000ms
     */
    public void testIntervalRoundingMonotonic_CET() {
        long interval = TimeUnit.MINUTES.toMillis(45);
        DateTimeZone tz = DateTimeZone.forID("CET");
        Rounding rounding = new Rounding.TimeIntervalRounding(interval, tz);
        List<Tuple<String, String>> expectedDates = new ArrayList<>();
        // first date is the date to be rounded, second the expected result
        expectedDates.add(new Tuple<>("2011-10-30T01:40:00.000+02:00", "2011-10-30T01:30:00.000+02:00"));
        expectedDates.add(new Tuple<>("2011-10-30T02:02:30.000+02:00", "2011-10-30T01:30:00.000+02:00"));
        expectedDates.add(new Tuple<>("2011-10-30T02:25:00.000+02:00", "2011-10-30T02:15:00.000+02:00"));
        expectedDates.add(new Tuple<>("2011-10-30T02:47:30.000+02:00", "2011-10-30T02:15:00.000+02:00"));
        expectedDates.add(new Tuple<>("2011-10-30T02:10:00.000+01:00", "2011-10-30T02:15:00.000+02:00"));
        expectedDates.add(new Tuple<>("2011-10-30T02:32:30.000+01:00", "2011-10-30T02:15:00.000+01:00"));
        expectedDates.add(new Tuple<>("2011-10-30T02:55:00.000+01:00", "2011-10-30T02:15:00.000+01:00"));
        expectedDates.add(new Tuple<>("2011-10-30T03:17:30.000+01:00", "2011-10-30T03:00:00.000+01:00"));

        long previousDate = Long.MIN_VALUE;
        for (Tuple<String, String> dates : expectedDates) {
                final long roundedDate = rounding.round(time(dates.v1()));
                assertThat(roundedDate, isDate(time(dates.v2()), tz));
                assertThat(roundedDate, greaterThanOrEqualTo(previousDate));
                previousDate = roundedDate;
        }
        // here's what this means for interval widths
        assertEquals(TimeUnit.MINUTES.toMillis(45), time("2011-10-30T02:15:00.000+02:00") - time("2011-10-30T01:30:00.000+02:00"));
        assertEquals(TimeUnit.MINUTES.toMillis(60), time("2011-10-30T02:15:00.000+01:00") - time("2011-10-30T02:15:00.000+02:00"));
        assertEquals(TimeUnit.MINUTES.toMillis(45), time("2011-10-30T03:00:00.000+01:00") - time("2011-10-30T02:15:00.000+01:00"));
    }

    /**
     * special test for DST switch from #9491
     */
    public void testAmbiguousHoursAfterDSTSwitch() {
        Rounding tzRounding;
        final DateTimeZone tz = DateTimeZone.forID("Asia/Jerusalem");
        tzRounding = Rounding.builder(DateTimeUnit.HOUR_OF_DAY).timeZone(tz).build();
        assertThat(tzRounding.round(time("2014-10-26T00:30:00+03:00")), isDate(time("2014-10-26T00:00:00+03:00"), tz));
        assertThat(tzRounding.round(time("2014-10-26T01:30:00+03:00")), isDate(time("2014-10-26T01:00:00+03:00"), tz));
        // the utc date for "2014-10-25T03:00:00+03:00" and "2014-10-25T03:00:00+02:00" is the same, local time turns back 1h here
        assertThat(time("2014-10-26T03:00:00+03:00"), isDate(time("2014-10-26T02:00:00+02:00"), tz));
        assertThat(tzRounding.round(time("2014-10-26T01:30:00+02:00")), isDate(time("2014-10-26T01:00:00+02:00"), tz));
        assertThat(tzRounding.round(time("2014-10-26T02:30:00+02:00")), isDate(time("2014-10-26T02:00:00+02:00"), tz));

        // Day interval
        tzRounding = Rounding.builder(DateTimeUnit.DAY_OF_MONTH).timeZone(tz).build();
        assertThat(tzRounding.round(time("2014-11-11T17:00:00", tz)), isDate(time("2014-11-11T00:00:00", tz), tz));
        // DST on
        assertThat(tzRounding.round(time("2014-08-11T17:00:00", tz)), isDate(time("2014-08-11T00:00:00", tz), tz));
        // Day of switching DST on -> off
        assertThat(tzRounding.round(time("2014-10-26T17:00:00", tz)), isDate(time("2014-10-26T00:00:00", tz), tz));
        // Day of switching DST off -> on
        assertThat(tzRounding.round(time("2015-03-27T17:00:00", tz)), isDate(time("2015-03-27T00:00:00", tz), tz));

        // Month interval
        tzRounding = Rounding.builder(DateTimeUnit.MONTH_OF_YEAR).timeZone(tz).build();
        assertThat(tzRounding.round(time("2014-11-11T17:00:00", tz)), isDate(time("2014-11-01T00:00:00", tz), tz));
        // DST on
        assertThat(tzRounding.round(time("2014-10-10T17:00:00", tz)), isDate(time("2014-10-01T00:00:00", tz), tz));

        // Year interval
        tzRounding = Rounding.builder(DateTimeUnit.YEAR_OF_CENTURY).timeZone(tz).build();
        assertThat(tzRounding.round(time("2014-11-11T17:00:00", tz)), isDate(time("2014-01-01T00:00:00", tz), tz));

        // Two timestamps in same year and different timezone offset ("Double buckets" issue - #9491)
        tzRounding = Rounding.builder(DateTimeUnit.YEAR_OF_CENTURY).timeZone(tz).build();
        assertThat(tzRounding.round(time("2014-11-11T17:00:00", tz)),
                isDate(tzRounding.round(time("2014-08-11T17:00:00", tz)), tz));
    }

    /**
     * test for #10025, strict local to UTC conversion can cause joda exceptions
     * on DST start
     */
    public void testLenientConversionDST() {
        DateTimeZone tz = DateTimeZone.forID("America/Sao_Paulo");
        long start = time("2014-10-18T20:50:00.000", tz);
        long end = time("2014-10-19T01:00:00.000", tz);
        Rounding tzRounding = new Rounding.TimeUnitRounding(DateTimeUnit.MINUTES_OF_HOUR, tz);
        Rounding dayTzRounding = new Rounding.TimeIntervalRounding(60000, tz);
        for (long time = start; time < end; time = time + 60000) {
            assertThat(tzRounding.nextRoundingValue(time), greaterThan(time));
            assertThat(dayTzRounding.nextRoundingValue(time), greaterThan(time));
        }
    }

    public void testEdgeCasesTransition() {
        {
            // standard +/-1 hour DST transition, CET
            DateTimeUnit timeUnit = DateTimeUnit.HOUR_OF_DAY;
            DateTimeZone tz = DateTimeZone.forID("CET");
            Rounding rounding = new Rounding.TimeUnitRounding(timeUnit, tz);

            // 29 Mar 2015 - Daylight Saving Time Started
            // at 02:00:00 clocks were turned forward 1 hour to 03:00:00
            assertInterval(time("2015-03-29T00:00:00.000+01:00"), time("2015-03-29T01:00:00.000+01:00"), rounding, 60, tz);
            assertInterval(time("2015-03-29T01:00:00.000+01:00"), time("2015-03-29T03:00:00.000+02:00"), rounding, 60, tz);
            assertInterval(time("2015-03-29T03:00:00.000+02:00"), time("2015-03-29T04:00:00.000+02:00"), rounding, 60, tz);

            // 25 Oct 2015 - Daylight Saving Time Ended
            // at 03:00:00 clocks were turned backward 1 hour to 02:00:00
            assertInterval(time("2015-10-25T01:00:00.000+02:00"), time("2015-10-25T02:00:00.000+02:00"), rounding, 60, tz);
            assertInterval(time("2015-10-25T02:00:00.000+02:00"), time("2015-10-25T02:00:00.000+01:00"), rounding, 60, tz);
            assertInterval(time("2015-10-25T02:00:00.000+01:00"), time("2015-10-25T03:00:00.000+01:00"), rounding, 60, tz);
        }

        {
            // time zone "Asia/Kathmandu"
            // 1 Jan 1986 - Time Zone Change (IST → NPT), at 00:00:00 clocks were turned forward 00:15 minutes
            //
            // hour rounding is stable before 1985-12-31T23:00:00.000 and after 1986-01-01T01:00:00.000+05:45
            // the interval between is 105 minutes long because the hour after transition starts at 00:15
            // which is not a round value for hourly rounding
            DateTimeUnit timeUnit = DateTimeUnit.HOUR_OF_DAY;
            DateTimeZone tz = DateTimeZone.forID("Asia/Kathmandu");
            Rounding rounding = new Rounding.TimeUnitRounding(timeUnit, tz);

            assertInterval(time("1985-12-31T22:00:00.000+05:30"), time("1985-12-31T23:00:00.000+05:30"), rounding, 60, tz);
            assertInterval(time("1985-12-31T23:00:00.000+05:30"), time("1986-01-01T01:00:00.000+05:45"), rounding, 105, tz);
            assertInterval(time("1986-01-01T01:00:00.000+05:45"), time("1986-01-01T02:00:00.000+05:45"), rounding, 60, tz);
        }

        {
            // time zone "Australia/Lord_Howe"
            // 3 Mar 1991 - Daylight Saving Time Ended
            // at 02:00:00 clocks were turned backward 0:30 hours to Sunday, 3 March 1991, 01:30:00
            DateTimeUnit timeUnit = DateTimeUnit.HOUR_OF_DAY;
            DateTimeZone tz = DateTimeZone.forID("Australia/Lord_Howe");
            Rounding rounding = new Rounding.TimeUnitRounding(timeUnit, tz);

            assertInterval(time("1991-03-03T00:00:00.000+11:00"), time("1991-03-03T01:00:00.000+11:00"), rounding, 60, tz);
            assertInterval(time("1991-03-03T01:00:00.000+11:00"), time("1991-03-03T02:00:00.000+10:30"), rounding, 90, tz);
            assertInterval(time("1991-03-03T02:00:00.000+10:30"), time("1991-03-03T03:00:00.000+10:30"), rounding, 60, tz);

            // 27 Oct 1991 - Daylight Saving Time Started
            // at 02:00:00 clocks were turned forward 0:30 hours to 02:30:00
            assertInterval(time("1991-10-27T00:00:00.000+10:30"), time("1991-10-27T01:00:00.000+10:30"), rounding, 60, tz);
            // the interval containing the switch time is 90 minutes long
            assertInterval(time("1991-10-27T01:00:00.000+10:30"), time("1991-10-27T03:00:00.000+11:00"), rounding, 90, tz);
            assertInterval(time("1991-10-27T03:00:00.000+11:00"), time("1991-10-27T04:00:00.000+11:00"), rounding, 60, tz);
        }

        {
            // time zone "Pacific/Chatham"
            // 5 Apr 2015 - Daylight Saving Time Ended
            // at 03:45:00 clocks were turned backward 1 hour to 02:45:00
            DateTimeUnit timeUnit = DateTimeUnit.HOUR_OF_DAY;
            DateTimeZone tz = DateTimeZone.forID("Pacific/Chatham");
            Rounding rounding = new Rounding.TimeUnitRounding(timeUnit, tz);

            assertInterval(time("2015-04-05T02:00:00.000+13:45"), time("2015-04-05T03:00:00.000+13:45"), rounding, 60, tz);
            assertInterval(time("2015-04-05T03:00:00.000+13:45"), time("2015-04-05T03:00:00.000+12:45"), rounding, 60, tz);
            assertInterval(time("2015-04-05T03:00:00.000+12:45"), time("2015-04-05T04:00:00.000+12:45"), rounding, 60, tz);

            // 27 Sep 2015 - Daylight Saving Time Started
            // at 02:45:00 clocks were turned forward 1 hour to 03:45:00

            assertInterval(time("2015-09-27T01:00:00.000+12:45"), time("2015-09-27T02:00:00.000+12:45"), rounding, 60, tz);
            assertInterval(time("2015-09-27T02:00:00.000+12:45"), time("2015-09-27T04:00:00.000+13:45"), rounding, 60, tz);
            assertInterval(time("2015-09-27T04:00:00.000+13:45"), time("2015-09-27T05:00:00.000+13:45"), rounding, 60, tz);
        }
    }

    public void testDST_Europe_Rome() {
        // time zone "Europe/Rome", rounding to days. Rome had two midnights on the day the clocks went back in 1978, and
        // timeZone.convertLocalToUTC() gives the later of the two because Rome is east of UTC, whereas we want the earlier.

        DateTimeUnit timeUnit = DateTimeUnit.DAY_OF_MONTH;
        DateTimeZone tz = DateTimeZone.forID("Europe/Rome");
        Rounding rounding = new TimeUnitRounding(timeUnit, tz);

        {
            long timeBeforeFirstMidnight = time("1978-09-30T23:59:00+02:00");
            long floor = rounding.round(timeBeforeFirstMidnight);
            assertThat(floor, isDate(time("1978-09-30T00:00:00+02:00"), tz));
        }

        {
            long timeBetweenMidnights = time("1978-10-01T00:30:00+02:00");
            long floor = rounding.round(timeBetweenMidnights);
            assertThat(floor, isDate(time("1978-10-01T00:00:00+02:00"), tz));
        }

        {
            long timeAfterSecondMidnight = time("1978-10-01T00:30:00+01:00");
            long floor = rounding.round(timeAfterSecondMidnight);
            assertThat(floor, isDate(time("1978-10-01T00:00:00+02:00"), tz));

            long prevFloor = rounding.round(floor - 1);
            assertThat(prevFloor, lessThan(floor));
            assertThat(prevFloor, isDate(time("1978-09-30T00:00:00+02:00"), tz));
        }
    }

    /**
     * Test for a time zone whose days overlap because the clocks are set back across midnight at the end of DST.
     */
    public void testDST_America_St_Johns() {
        // time zone "America/St_Johns", rounding to days.
        DateTimeUnit timeUnit = DateTimeUnit.DAY_OF_MONTH;
        DateTimeZone tz = DateTimeZone.forID("America/St_Johns");
        Rounding rounding = new TimeUnitRounding(timeUnit, tz);

        // 29 October 2006 - Daylight Saving Time ended, changing the UTC offset from -02:30 to -03:30.
        // This happened at 02:31 UTC, 00:01 local time, so the clocks were set back 1 hour to 23:01 on the 28th.
        // This means that 2006-10-29 has _two_ midnights, one in the -02:30 offset and one in the -03:30 offset.
        // Only the first of these is considered "rounded". Moreover, the extra time between 23:01 and 23:59
        // should be considered as part of the 28th even though it comes after midnight on the 29th.

        {
            // Times before the first midnight should be rounded up to the first midnight.
            long timeBeforeFirstMidnight = time("2006-10-28T23:30:00.000-02:30");
            long floor = rounding.round(timeBeforeFirstMidnight);
            assertThat(floor, isDate(time("2006-10-28T00:00:00.000-02:30"), tz));
            long ceiling = rounding.nextRoundingValue(timeBeforeFirstMidnight);
            assertThat(ceiling, isDate(time("2006-10-29T00:00:00.000-02:30"), tz));
            assertInterval(floor, timeBeforeFirstMidnight, ceiling, rounding, tz);
        }

        {
            // Times between the two midnights which are on the later day should be rounded down to the later day's midnight.
            long timeBetweenMidnights = time("2006-10-29T00:00:30.000-02:30");
            // (this is halfway through the last minute before the clocks changed, in which local time was ambiguous)

            long floor = rounding.round(timeBetweenMidnights);
            assertThat(floor, isDate(time("2006-10-29T00:00:00.000-02:30"), tz));

            long ceiling = rounding.nextRoundingValue(timeBetweenMidnights);
            assertThat(ceiling, isDate(time("2006-10-30T00:00:00.000-03:30"), tz));

            assertInterval(floor, timeBetweenMidnights, ceiling, rounding, tz);
        }

        {
            // Times between the two midnights which are on the earlier day should be rounded down to the earlier day's midnight.
            long timeBetweenMidnights = time("2006-10-28T23:30:00.000-03:30");
            // (this is halfway through the hour after the clocks changed, in which local time was ambiguous)

            long floor = rounding.round(timeBetweenMidnights);
            assertThat(floor, isDate(time("2006-10-28T00:00:00.000-02:30"), tz));

            long ceiling = rounding.nextRoundingValue(timeBetweenMidnights);
            assertThat(ceiling, isDate(time("2006-10-29T00:00:00.000-02:30"), tz));

            assertInterval(floor, timeBetweenMidnights, ceiling, rounding, tz);
        }

        {
            // Times after the second midnight should be rounded down to the first midnight.
            long timeAfterSecondMidnight = time("2006-10-29T06:00:00.000-03:30");
            long floor = rounding.round(timeAfterSecondMidnight);
            assertThat(floor, isDate(time("2006-10-29T00:00:00.000-02:30"), tz));
            long ceiling = rounding.nextRoundingValue(timeAfterSecondMidnight);
            assertThat(ceiling, isDate(time("2006-10-30T00:00:00.000-03:30"), tz));
            assertInterval(floor, timeAfterSecondMidnight, ceiling, rounding, tz);
        }
    }

    /**
     * tests for dst transition with overlaps and day roundings.
     */
    public void testDST_END_Edgecases() {
        // First case, dst happens at 1am local time, switching back one hour.
        // We want the overlapping hour to count for the next day, making it a 25h interval

        DateTimeUnit timeUnit = DateTimeUnit.DAY_OF_MONTH;
        DateTimeZone tz = DateTimeZone.forID("Atlantic/Azores");
        Rounding rounding = new Rounding.TimeUnitRounding(timeUnit, tz);

        // Sunday, 29 October 2000, 01:00:00 clocks were turned backward 1 hour
        // to Sunday, 29 October 2000, 00:00:00 local standard time instead
        // which means there were two midnights that day.

        long midnightBeforeTransition = time("2000-10-29T00:00:00", tz);
        long midnightOfTransition = time("2000-10-29T00:00:00-01:00");
        assertEquals(60L * 60L * 1000L, midnightOfTransition - midnightBeforeTransition);
        long nextMidnight = time("2000-10-30T00:00:00", tz);

        assertInterval(midnightBeforeTransition, nextMidnight, rounding, 25 * 60, tz);

        assertThat(rounding.round(time("2000-10-29T06:00:00-01:00")), isDate(time("2000-10-29T00:00:00Z"), tz));

        // Second case, dst happens at 0am local time, switching back one hour to 23pm local time.
        // We want the overlapping hour to count for the previous day here

        tz = DateTimeZone.forID("America/Lima");
        rounding = new Rounding.TimeUnitRounding(timeUnit, tz);

        // Sunday, 1 April 1990, 00:00:00 clocks were turned backward 1 hour to
        // Saturday, 31 March 1990, 23:00:00 local standard time instead

        midnightBeforeTransition = time("1990-03-31T00:00:00.000-04:00");
        nextMidnight = time("1990-04-01T00:00:00.000-05:00");
        assertInterval(midnightBeforeTransition, nextMidnight, rounding, 25 * 60, tz);

        // make sure the next interval is 24h long again
        long midnightAfterTransition = time("1990-04-01T00:00:00.000-05:00");
        nextMidnight = time("1990-04-02T00:00:00.000-05:00");
        assertInterval(midnightAfterTransition, nextMidnight, rounding, 24 * 60, tz);
    }

    /**
     * Test that time zones are correctly parsed. There is a bug with
     * Joda 2.9.4 (see https://github.com/JodaOrg/joda-time/issues/373)
     */
    public void testsTimeZoneParsing() {
        final DateTime expected = new DateTime(2016, 11, 10, 5, 37, 59, randomDateTimeZone());

        // Formatter used to print and parse the sample date.
        // Printing the date works but parsing it back fails
        // with Joda 2.9.4
        DateTimeFormatter formatter = DateTimeFormat.forPattern("YYYY-MM-dd'T'HH:mm:ss " + randomFrom("ZZZ", "[ZZZ]", "'['ZZZ']'"));

        String dateTimeAsString = formatter.print(expected);
        assertThat(dateTimeAsString, startsWith("2016-11-10T05:37:59 "));

        DateTime parsedDateTime = formatter.parseDateTime(dateTimeAsString);
        assertThat(parsedDateTime.getZone(), equalTo(expected.getZone()));
    }

    private static void assertInterval(long rounded, long nextRoundingValue, Rounding rounding, int minutes,
            DateTimeZone tz) {
        assertInterval(rounded, dateBetween(rounded, nextRoundingValue), nextRoundingValue, rounding, tz);
        assertEquals(DateTimeConstants.MILLIS_PER_MINUTE * minutes, nextRoundingValue - rounded);
    }

    /**
     * perform a number on assertions and checks on {@link TimeUnitRounding} intervals
     * @param rounded the expected low end of the rounding interval
     * @param unrounded a date in the interval to be checked for rounding
     * @param nextRoundingValue the expected upper end of the rounding interval
     * @param rounding the rounding instance
     */
    private static void assertInterval(long rounded, long unrounded, long nextRoundingValue, Rounding rounding, DateTimeZone tz) {
        assertThat("rounding should be idempotent ", rounding.round(rounded), isDate(rounded, tz));
        assertThat("rounded value smaller or equal than unrounded" + rounding, rounded, lessThanOrEqualTo(unrounded));
        assertThat("values less than rounded should round further down" + rounding, rounding.round(rounded - 1), lessThan(rounded));
        assertThat("nextRounding value should be a rounded date", rounding.round(nextRoundingValue), isDate(nextRoundingValue, tz));
        assertThat("values above nextRounding should round down there", rounding.round(nextRoundingValue + 1),
                isDate(nextRoundingValue, tz));

        if (isTimeWithWellDefinedRounding(tz, unrounded)) {
            assertThat("nextRounding value should be greater than date" + rounding, nextRoundingValue, greaterThan(unrounded));

            long dateBetween = dateBetween(rounded, nextRoundingValue);
            assertThat("dateBetween [" + new DateTime(dateBetween, tz) + "] should round down to roundedDate",
                rounding.round(dateBetween), isDate(rounded, tz));
            assertThat("dateBetween [" + new DateTime(dateBetween, tz) + "] should round up to nextRoundingValue",
                rounding.nextRoundingValue(dateBetween), isDate(nextRoundingValue, tz));
        }
    }

    private static boolean isTimeWithWellDefinedRounding(DateTimeZone tz, long t) {
        if (tz.getID().equals("America/St_Johns")
            || tz.getID().equals("America/Goose_Bay")
            || tz.getID().equals("America/Moncton")
            || tz.getID().equals("Canada/Newfoundland")) {

            // Clocks went back at 00:01 between 1987 and 2010, causing overlapping days.
            // These timezones are otherwise uninteresting, so just skip this period.

            return t <= time("1987-10-01T00:00:00Z")
                || t >= time("2010-12-01T00:00:00Z");
        }

        if (tz.getID().equals("Antarctica/Casey")) {

            // Clocks went back 3 hours at 02:00 on 2010-03-05, causing overlapping days.

            return t <= time("2010-03-03T00:00:00Z")
                || t >= time("2010-03-07T00:00:00Z");
        }

        return true;
    }

    private static long dateBetween(long lower, long upper) {
        long dateBetween = randomLongBetween(lower, upper - 1);
        assert lower <= dateBetween && dateBetween < upper;
        return dateBetween;
    }

    private static DateTimeUnit randomTimeUnit() {
        byte id = (byte) randomIntBetween(1, 8);
        return DateTimeUnit.resolve(id);
    }

    private static long time(String time) {
        return time(time, DateTimeZone.UTC);
    }

    private static long time(String time, DateTimeZone zone) {
        return ISODateTimeFormat.dateOptionalTimeParser().withZone(zone).parseMillis(time);
    }

    private static Matcher<Long> isDate(final long expected, DateTimeZone tz) {
        return new TypeSafeMatcher<Long>() {
            @Override
            public boolean matchesSafely(final Long item) {
                return expected == item.longValue();
            }

            @Override
            public void describeTo(Description description) {
                description.appendText(new DateTime(expected, tz) + " [" + expected + "] ");
            }

            @Override
            protected void describeMismatchSafely(final Long actual, final Description mismatchDescription) {
                mismatchDescription.appendText(" was ").appendValue(new DateTime(actual, tz) + " [" + actual + "]");
            }
        };
    }
}
