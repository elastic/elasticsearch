/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.rounding.DateTimeUnit;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneOffsetTransitionRule;
import java.time.zone.ZoneRules;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class RoundingTests extends ESTestCase {

    public void testUTCTimeUnitRounding() {
        Rounding tzRounding = Rounding.builder(Rounding.DateTimeUnit.MONTH_OF_YEAR).build();
        ZoneId tz = ZoneOffset.UTC;
        assertThat(tzRounding.round(time("2009-02-03T01:01:01")), isDate(time("2009-02-01T00:00:00.000Z"), tz));
        assertThat(tzRounding.nextRoundingValue(time("2009-02-01T00:00:00.000Z")), isDate(time("2009-03-01T00:00:00.000Z"), tz));

        tzRounding = Rounding.builder(Rounding.DateTimeUnit.WEEK_OF_WEEKYEAR).build();
        assertThat(tzRounding.round(time("2012-01-10T01:01:01")), isDate(time("2012-01-09T00:00:00.000Z"), tz));
        assertThat(tzRounding.nextRoundingValue(time("2012-01-09T00:00:00.000Z")), isDate(time("2012-01-16T00:00:00.000Z"), tz));

        tzRounding = Rounding.builder(Rounding.DateTimeUnit.QUARTER_OF_YEAR).build();
        assertThat(tzRounding.round(time("2012-01-10T01:01:01")), isDate(time("2012-01-01T00:00:00.000Z"), tz));
        assertThat(tzRounding.nextRoundingValue(time("2012-01-09T00:00:00.000Z")), isDate(time("2012-04-01T00:00:00.000Z"), tz));

        tzRounding = Rounding.builder(Rounding.DateTimeUnit.HOUR_OF_DAY).build();
        assertThat(tzRounding.round(time("2012-01-10T01:01:01")), isDate(time("2012-01-10T01:00:00.000Z"), tz));
        assertThat(tzRounding.nextRoundingValue(time("2012-01-09T00:00:00.000Z")), isDate(time("2012-01-09T01:00:00.000Z"), tz));

        tzRounding = Rounding.builder(Rounding.DateTimeUnit.DAY_OF_MONTH).build();
        assertThat(tzRounding.round(time("2012-01-10T01:01:01")), isDate(time("2012-01-10T00:00:00.000Z"), tz));
        assertThat(tzRounding.nextRoundingValue(time("2012-01-09T00:00:00.000Z")), isDate(time("2012-01-10T00:00:00.000Z"), tz));

        tzRounding = Rounding.builder(Rounding.DateTimeUnit.YEAR_OF_CENTURY).build();
        assertThat(tzRounding.round(time("2012-01-10T01:01:01")), isDate(time("2012-01-01T00:00:00.000Z"), tz));
        assertThat(tzRounding.nextRoundingValue(time("2012-01-09T00:00:00.000Z")), isDate(time("2013-01-01T00:00:00.000Z"), tz));

        tzRounding = Rounding.builder(Rounding.DateTimeUnit.MINUTES_OF_HOUR).build();
        assertThat(tzRounding.round(time("2012-01-10T01:01:01")), isDate(time("2012-01-10T01:01:00.000Z"), tz));
        assertThat(tzRounding.nextRoundingValue(time("2012-01-09T00:00:00.000Z")), isDate(time("2012-01-09T00:01:00.000Z"), tz));

        tzRounding = Rounding.builder(Rounding.DateTimeUnit.SECOND_OF_MINUTE).build();
        assertThat(tzRounding.round(time("2012-01-10T01:01:01")), isDate(time("2012-01-10T01:01:01.000Z"), tz));
        assertThat(tzRounding.nextRoundingValue(time("2012-01-09T00:00:00.000Z")), isDate(time("2012-01-09T00:00:01.000Z"), tz));
    }

    public void testUTCIntervalRounding() {
        Rounding tzRounding = Rounding.builder(TimeValue.timeValueHours(12)).build();
        ZoneId tz = ZoneOffset.UTC;
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
        ZoneId tz = ZoneOffset.ofHours(-1);
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
        ZoneId tz = ZoneOffset.ofHours(-8);
        Rounding tzRounding = Rounding.builder(TimeValue.timeValueHours(12)).timeZone(tz).build();
        assertThat(tzRounding.round(time("2009-02-03T00:01:01")), isDate(time("2009-02-02T20:00:00.000Z"), tz));
        assertThat(tzRounding.nextRoundingValue(time("2009-02-02T20:00:00.000Z")), isDate(time("2009-02-03T08:00:00.000Z"), tz));

        assertThat(tzRounding.round(time("2009-02-03T13:01:01")), isDate(time("2009-02-03T08:00:00.000Z"), tz));
        assertThat(tzRounding.nextRoundingValue(time("2009-02-03T08:00:00.000Z")), isDate(time("2009-02-03T20:00:00.000Z"), tz));
    }

    public void testDayRounding() {
        int timezoneOffset = -2;
        Rounding tzRounding = Rounding.builder(Rounding.DateTimeUnit.DAY_OF_MONTH)
            .timeZone(ZoneOffset.ofHours(timezoneOffset)).build();
        assertThat(tzRounding.round(0), equalTo(0L - TimeValue.timeValueHours(24 + timezoneOffset).millis()));
        assertThat(tzRounding.nextRoundingValue(0L - TimeValue.timeValueHours(24 + timezoneOffset).millis()), equalTo(TimeValue
            .timeValueHours(-timezoneOffset).millis()));

        ZoneId tz = ZoneId.of("-08:00");
        tzRounding = Rounding.builder(Rounding.DateTimeUnit.DAY_OF_MONTH).timeZone(tz).build();
        assertThat(tzRounding.round(time("2012-04-01T04:15:30Z")), isDate(time("2012-03-31T08:00:00Z"), tz));

        tzRounding = Rounding.builder(Rounding.DateTimeUnit.MONTH_OF_YEAR).timeZone(tz).build();
        assertThat(tzRounding.round(time("2012-04-01T04:15:30Z")), equalTo(time("2012-03-01T08:00:00Z")));

        // date in Feb-3rd, but still in Feb-2nd in -02:00 timezone
        tz = ZoneId.of("-02:00");
        tzRounding = Rounding.builder(Rounding.DateTimeUnit.DAY_OF_MONTH).timeZone(tz).build();
        assertThat(tzRounding.round(time("2009-02-03T01:01:01")), isDate(time("2009-02-02T02:00:00"), tz));
        assertThat(tzRounding.nextRoundingValue(time("2009-02-02T02:00:00")), isDate(time("2009-02-03T02:00:00"), tz));

        // date in Feb-3rd, also in -02:00 timezone
        tzRounding = Rounding.builder(Rounding.DateTimeUnit.DAY_OF_MONTH).timeZone(tz).build();
        assertThat(tzRounding.round(time("2009-02-03T02:01:01")), isDate(time("2009-02-03T02:00:00"), tz));
        assertThat(tzRounding.nextRoundingValue(time("2009-02-03T02:00:00")), isDate(time("2009-02-04T02:00:00"), tz));
    }

    public void testTimeRounding() {
        // hour unit
        ZoneId tz = ZoneOffset.ofHours(-2);
        Rounding tzRounding = Rounding.builder(Rounding.DateTimeUnit.HOUR_OF_DAY).timeZone(tz).build();
        assertThat(tzRounding.round(0), equalTo(0L));
        assertThat(tzRounding.nextRoundingValue(0L), equalTo(TimeValue.timeValueHours(1L).getMillis()));

        assertThat(tzRounding.round(time("2009-02-03T01:01:01")), isDate(time("2009-02-03T01:00:00"), tz));
        assertThat(tzRounding.nextRoundingValue(time("2009-02-03T01:00:00")), isDate(time("2009-02-03T02:00:00"), tz));
    }

    public void testTimeUnitRoundingDST() {
        Rounding tzRounding;
        // testing savings to non savings switch
        ZoneId cet = ZoneId.of("CET");
        tzRounding = Rounding.builder(Rounding.DateTimeUnit.HOUR_OF_DAY).timeZone(cet).build();
        assertThat(tzRounding.round(time("2014-10-26T01:01:01", cet)), isDate(time("2014-10-26T01:00:00+02:00"), cet));
        assertThat(tzRounding.nextRoundingValue(time("2014-10-26T01:00:00", cet)),isDate(time("2014-10-26T02:00:00+02:00"), cet));
        assertThat(tzRounding.nextRoundingValue(time("2014-10-26T02:00:00", cet)), isDate(time("2014-10-26T02:00:00+01:00"), cet));

        // testing non savings to savings switch
        tzRounding = Rounding.builder(Rounding.DateTimeUnit.HOUR_OF_DAY).timeZone(cet).build();
        assertThat(tzRounding.round(time("2014-03-30T01:01:01", cet)), isDate(time("2014-03-30T01:00:00+01:00"), cet));
        assertThat(tzRounding.nextRoundingValue(time("2014-03-30T01:00:00", cet)), isDate(time("2014-03-30T03:00:00", cet), cet));
        assertThat(tzRounding.nextRoundingValue(time("2014-03-30T03:00:00", cet)), isDate(time("2014-03-30T04:00:00", cet), cet));

        // testing non savings to savings switch (America/Chicago)
        ZoneId chg = ZoneId.of("America/Chicago");
        Rounding tzRounding_utc = Rounding.builder(Rounding.DateTimeUnit.HOUR_OF_DAY)
            .timeZone(ZoneOffset.UTC).build();
        assertThat(tzRounding.round(time("2014-03-09T03:01:01", chg)), isDate(time("2014-03-09T03:00:00", chg), chg));

        Rounding tzRounding_chg = Rounding.builder(Rounding.DateTimeUnit.HOUR_OF_DAY).timeZone(chg).build();
        assertThat(tzRounding_chg.round(time("2014-03-09T03:01:01", chg)), isDate(time("2014-03-09T03:00:00", chg), chg));

        // testing savings to non savings switch 2013 (America/Chicago)
        assertThat(tzRounding_utc.round(time("2013-11-03T06:01:01", chg)), isDate(time("2013-11-03T06:00:00", chg), chg));
        assertThat(tzRounding_chg.round(time("2013-11-03T06:01:01", chg)), isDate(time("2013-11-03T06:00:00", chg), chg));

        // testing savings to non savings switch 2014 (America/Chicago)
        assertThat(tzRounding_utc.round(time("2014-11-02T06:01:01", chg)), isDate(time("2014-11-02T06:00:00", chg), chg));
        assertThat(tzRounding_chg.round(time("2014-11-02T06:01:01", chg)), isDate(time("2014-11-02T06:00:00", chg), chg));
    }

    public void testOffsetRounding() {
        long twoHours = TimeUnit.HOURS.toMillis(2);
        long oneDay = TimeUnit.DAYS.toMillis(1);
        Rounding rounding = Rounding.builder(Rounding.DateTimeUnit.DAY_OF_MONTH).offset(twoHours).build();
        assertThat(rounding.round(0), equalTo(-oneDay + twoHours));
        assertThat(rounding.round(twoHours), equalTo(twoHours));
        assertThat(rounding.nextRoundingValue(-oneDay), equalTo(-oneDay + twoHours));
        assertThat(rounding.nextRoundingValue(0), equalTo(twoHours));
        assertThat(rounding.withoutOffset().round(0), equalTo(0L));
        assertThat(rounding.withoutOffset().nextRoundingValue(0), equalTo(oneDay));

        rounding = Rounding.builder(Rounding.DateTimeUnit.DAY_OF_MONTH).offset(-twoHours).build();
        assertThat(rounding.round(0), equalTo(-twoHours));
        assertThat(rounding.round(oneDay - twoHours), equalTo(oneDay - twoHours));
        assertThat(rounding.nextRoundingValue(-oneDay), equalTo(-twoHours));
        assertThat(rounding.nextRoundingValue(0), equalTo(oneDay - twoHours));
        assertThat(rounding.withoutOffset().round(0), equalTo(0L));
        assertThat(rounding.withoutOffset().nextRoundingValue(0), equalTo(oneDay));

        rounding = Rounding.builder(Rounding.DateTimeUnit.DAY_OF_MONTH).timeZone(ZoneId.of("America/New_York")).offset(-twoHours).build();
        assertThat(rounding.round(time("2020-11-01T09:00:00")), equalTo(time("2020-11-01T02:00:00")));
    }

    /**
     * Randomized test on TimeUnitRounding. Test uses random
     * {@link DateTimeUnit} and {@link ZoneId} and often (50% of the time)
     * chooses test dates that are exactly on or close to offset changes (e.g.
     * DST) in the chosen time zone.
     *
     * It rounds the test date down and up and performs various checks on the
     * rounding unit interval that is defined by this. Assumptions tested are
     * described in
     * {@link #assertInterval(long, long, long, Rounding, ZoneId)}
     */
    public void testRandomTimeUnitRounding() {
        for (int i = 0; i < 1000; ++i) {
            Rounding.DateTimeUnit unit = randomFrom(Rounding.DateTimeUnit.values());
            ZoneId tz = randomZone();
            long[] bounds = randomDateBounds(unit);
            assertUnitRoundingSameAsJavaUtilTimeImplementation(unit, tz, bounds[0], bounds[1]);
        }
    }

    /**
     * This test chooses a date in the middle of the transition, so that we can test
     * if the transition which is before the minLookup, but still should be applied
     * is not skipped
     */
    public void testRoundingAroundDST() {
        Rounding.DateTimeUnit unit = Rounding.DateTimeUnit.DAY_OF_MONTH;
        ZoneId tz = ZoneId.of("Canada/Newfoundland");
        long minLookup = 688618001000L; // 1991-10-28T02:46:41.527Z
        long maxLookup = 688618001001L; // +1sec
        // there is a Transition[Overlap at 1991-10-27T00:01-02:30 to -03:30] ”
        assertUnitRoundingSameAsJavaUtilTimeImplementation(unit, tz, minLookup, maxLookup);
    }

    private void assertUnitRoundingSameAsJavaUtilTimeImplementation(Rounding.DateTimeUnit unit, ZoneId tz, long start, long end) {
        Rounding rounding = new Rounding.TimeUnitRounding(unit, tz);
        Rounding.Prepared prepared = rounding.prepare(start, end);

        // Check that rounding is internally consistent and consistent with nextRoundingValue
        long date = dateBetween(start, end);
        long unitMillis = unit.getField().getBaseUnit().getDuration().toMillis();
        // FIXME this was copy pasted from the other impl and not used. breaks the nasty date actually gets assigned
        if (randomBoolean()) {
            nastyDate(date, tz, unitMillis);
        }
        final long roundedDate = prepared.round(date);
        final long nextRoundingValue = prepared.nextRoundingValue(roundedDate);

        assertInterval(roundedDate, date, nextRoundingValue, rounding, tz);

        // check correct unit interval width for units smaller than a day, they should be fixed size except for transitions
        if (unitMillis <= 86400 * 1000) {
            // if the interval defined didn't cross timezone offset transition, it should cover unitMillis width
            int offsetRounded = tz.getRules().getOffset(Instant.ofEpochMilli(roundedDate - 1)).getTotalSeconds();
            int offsetNextValue = tz.getRules().getOffset(Instant.ofEpochMilli(nextRoundingValue + 1)).getTotalSeconds();
            if (offsetRounded == offsetNextValue) {
                assertThat("unit interval width not as expected for [" + unit + "], [" + tz + "] at "
                    + Instant.ofEpochMilli(roundedDate), nextRoundingValue - roundedDate, equalTo(unitMillis));
            }
        }

        // Round a whole bunch of dates and make sure they line up with the known good java time implementation
        Rounding.Prepared javaTimeRounding = rounding.prepareJavaTime();
        for (int d = 0; d < 1000; d++) {
            date = dateBetween(start, end);
            long javaRounded = javaTimeRounding.round(date);
            long esRounded = prepared.round(date);
            if (javaRounded != esRounded) {
                fail("Expected [" + rounding + "] to round [" + Instant.ofEpochMilli(date) + "] to ["
                        + Instant.ofEpochMilli(javaRounded) + "] but instead rounded to [" + Instant.ofEpochMilli(esRounded) + "]");
            }
            long javaNextRoundingValue = javaTimeRounding.nextRoundingValue(date);
            long esNextRoundingValue = prepared.nextRoundingValue(date);
            if (javaNextRoundingValue != esNextRoundingValue) {
                fail("Expected [" + rounding + "] to round [" + Instant.ofEpochMilli(date) + "] to ["
                        + Instant.ofEpochMilli(esRounded) + "] and nextRoundingValue to be ["
                        + Instant.ofEpochMilli(javaNextRoundingValue) + "] but instead was to ["
                        + Instant.ofEpochMilli(esNextRoundingValue) + "]");
            }
        }
    }

    /**
     * To be even more nasty, go to a transition in the selected time zone.
     * In one third of the cases stay there, otherwise go half a unit back or forth
     */
    private static long nastyDate(long initialDate, ZoneId timezone, long unitMillis) {
        ZoneOffsetTransition transition = timezone.getRules().nextTransition(Instant.ofEpochMilli(initialDate));
        long date = initialDate;
        if (transition != null) {
            date = transition.getInstant().toEpochMilli();
        }
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
        ZoneId tz = ZoneId.of("CET");
        Rounding rounding = new Rounding.TimeIntervalRounding(interval, tz);

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
        ZoneId tz = ZoneId.of("CET");
        Rounding rounding = new Rounding.TimeIntervalRounding(interval, tz);
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
        ZoneId tz = ZoneId.of("Asia/Kathmandu");
        Rounding rounding = new Rounding.TimeIntervalRounding(interval, tz);
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
        long interval = TimeUnit.MINUTES.toMillis(14);
        ZoneId tz = ZoneId.of("CET");
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
        long interval = TimeUnit.HOURS.toMillis(12);
        ZoneId tz = ZoneId.of("CET");
        Rounding rounding = new Rounding.TimeIntervalRounding(interval, tz);

        assertThat(rounding.round(time("2016-03-26T01:00:00+01:00")), isDate(time("2016-03-26T00:00:00+01:00"), tz));
        assertThat(rounding.round(time("2016-03-26T13:00:00+01:00")), isDate(time("2016-03-26T12:00:00+01:00"), tz));
        assertThat(rounding.round(time("2016-03-27T01:00:00+01:00")), isDate(time("2016-03-27T00:00:00+01:00"), tz));
        assertThat(rounding.round(time("2016-03-27T13:00:00+02:00")), isDate(time("2016-03-27T12:00:00+02:00"), tz));
        assertThat(rounding.round(time("2016-03-28T01:00:00+02:00")), isDate(time("2016-03-28T00:00:00+02:00"), tz));
        assertThat(rounding.round(time("2016-03-28T13:00:00+02:00")), isDate(time("2016-03-28T12:00:00+02:00"), tz));
    }

    public void testRandomTimeIntervalRounding() {
        for (int i = 0; i < 1000; i++) {
            int unitCount = randomIntBetween(1, 365);
            TimeUnit unit = randomFrom(TimeUnit.MINUTES, TimeUnit.HOURS, TimeUnit.DAYS);
            long interval = unit.toMillis(unitCount);
            ZoneId tz = randomZone();
            Rounding rounding = new Rounding.TimeIntervalRounding(interval, tz);
            long mainDate = randomDate();
            if (randomBoolean()) {
                mainDate = nastyDate(mainDate, tz, interval);
            }
            long min = mainDate - 2 * interval;
            long max = mainDate + 2 * interval;

            /*
             * Prepare a rounding with two extra intervals of range because
             * in the tests far below we call round(round(min) - 1). The first
             * round might spit out a time below min - interval if min is near
             * a daylight savings time transition. So we request an extra big
             * range just in case.
             */
            Rounding.Prepared prepared = rounding.prepare(min - 2 * interval, max);

            // Round a whole bunch of dates and make sure they line up with the known good java time implementation
            Rounding.Prepared javaTimeRounding = rounding.prepareJavaTime();
            for (int d = 0; d < 1000; d++) {
                long date = dateBetween(min, max);
                long javaRounded = javaTimeRounding.round(date);
                long esRounded = prepared.round(date);
                if (javaRounded != esRounded) {
                    fail("Expected [" + unitCount + " " + unit + " in " + tz + "] to round [" + Instant.ofEpochMilli(date) + "] to ["
                            + Instant.ofEpochMilli(javaRounded) + "] but instead rounded to [" + Instant.ofEpochMilli(esRounded) + "]");
                }
                long javaNextRoundingValue = javaTimeRounding.nextRoundingValue(date);
                long esNextRoundingValue = prepared.nextRoundingValue(date);
                if (javaNextRoundingValue != esNextRoundingValue) {
                    fail("Expected [" + unitCount + " " + unit + " in " + tz + "] to round [" + Instant.ofEpochMilli(date) + "] to ["
                            + Instant.ofEpochMilli(esRounded) + "] and nextRoundingValue to be ["
                            + Instant.ofEpochMilli(javaNextRoundingValue) + "] but instead was to ["
                            + Instant.ofEpochMilli(esNextRoundingValue) + "]");
                }
            }

            // check two intervals around date
            long previousRoundedValue = Long.MIN_VALUE;
            for (long date = min; date < max; date += interval / 2) {
                try {
                    final long roundedDate = rounding.round(date);
                    final long nextRoundingValue = prepared.nextRoundingValue(roundedDate);
                    assertThat("Rounding should be idempotent", roundedDate, equalTo(prepared.round(roundedDate)));
                    assertThat("Rounded value smaller or equal than unrounded", roundedDate, lessThanOrEqualTo(date));
                    assertThat("Values smaller than rounded value should round further down", prepared.round(roundedDate - 1),
                        lessThan(roundedDate));
                    assertThat("Rounding should be >= previous rounding value", roundedDate, greaterThanOrEqualTo(previousRoundedValue));
                    assertThat("NextRounding value should be greater than date", nextRoundingValue, greaterThan(roundedDate));
                    assertThat("NextRounding value rounds to itself", nextRoundingValue,
                        isDate(rounding.round(nextRoundingValue), tz));

                    if (tz.getRules().isFixedOffset()) {
                        assertThat("NextRounding value should be interval from rounded value", nextRoundingValue - roundedDate,
                            equalTo(interval));
                    }
                    previousRoundedValue = roundedDate;
                } catch (AssertionError e) {
                    ZonedDateTime dateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(date), tz);
                    ZonedDateTime previousRoundedValueDate = ZonedDateTime.ofInstant(Instant.ofEpochMilli(previousRoundedValue), tz);
                    logger.error("Rounding error at {}/{}, timezone {}, interval: {} previousRoundedValue {}/{}", dateTime, date,
                        tz, interval, previousRoundedValueDate, previousRoundedValue);
                    throw e;
                }
            }
        }
    }

    /**
     * Check a {@link Rounding.Prepared#nextRoundingValue} that was difficult
     * to build well with the java.time APIs.
     */
    public void testHardNextRoundingValue() {
        Rounding rounding = new Rounding.TimeIntervalRounding(960000, ZoneId.of("Europe/Minsk"));
        long rounded = rounding.prepareForUnknown().round(877824908400L);
        long next = rounding.prepareForUnknown().nextRoundingValue(rounded);
        assertThat(next, greaterThan(rounded));
    }

    /**
     * Check a {@link Rounding.Prepared#nextRoundingValue} that was difficult
     * to build well with the java.time APIs.
     */
    public void testOtherHardNextRoundingValue() {
        Rounding rounding = new Rounding.TimeIntervalRounding(480000, ZoneId.of("Portugal"));
        long rounded = rounding.prepareJavaTime().round(972780720000L);
        long next = rounding.prepareJavaTime().nextRoundingValue(rounded);
        assertThat(next, greaterThan(rounded));
    }

    /**
     * Check a {@link Rounding.Prepared#nextRoundingValue} that was difficult
     * to build well our janky Newton's Method/binary search hybrid.
     */
    public void testHardNewtonsMethod() {
        ZoneId tz = ZoneId.of("Asia/Jerusalem");
        Rounding rounding = new Rounding.TimeIntervalRounding(19800000, tz);
        assertThat(rounding.prepareJavaTime().nextRoundingValue(1824929914182L), isDate(time("2027-10-31T01:30:00", tz), tz));
    }

    /**
     * Check a {@link Rounding.Prepared#nextRoundingValue} that was difficult
     * to build well with the java.time APIs.
     */
    public void testOtherHardNewtonsMethod() {
        ZoneId tz = ZoneId.of("America/Glace_Bay");
        Rounding rounding = new Rounding.TimeIntervalRounding(13800000, tz);
        assertThat(rounding.prepareJavaTime().nextRoundingValue(1383463147373L), isDate(time("2013-11-03T03:40:00", tz), tz));
    }

    /**
     * Test that rounded values are always greater or equal to last rounded value if date is increasing.
     * The example covers an interval around 2011-10-30T02:10:00+01:00, time zone CET, interval: 2700000ms
     */
    public void testIntervalRoundingMonotonic_CET() {
        long interval = TimeUnit.MINUTES.toMillis(45);
        ZoneId tz = ZoneId.of("CET");
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
            assertThat(dates.toString(), roundedDate, isDate(time(dates.v2()), tz));
            assertThat(dates.toString(), roundedDate, greaterThanOrEqualTo(previousDate));
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
        final ZoneId tz = ZoneId.of("Asia/Jerusalem");
        tzRounding = Rounding.builder(Rounding.DateTimeUnit.HOUR_OF_DAY).timeZone(tz).build();
        assertThat(tzRounding.round(time("2014-10-26T00:30:00+03:00")), isDate(time("2014-10-26T00:00:00+03:00"), tz));
        assertThat(tzRounding.round(time("2014-10-26T01:30:00+03:00")), isDate(time("2014-10-26T01:00:00+03:00"), tz));
        // the utc date for "2014-10-25T03:00:00+03:00" and "2014-10-25T03:00:00+02:00" is the same, local time turns back 1h here
        assertThat(time("2014-10-26T03:00:00+03:00"), isDate(time("2014-10-26T02:00:00+02:00"), tz));
        assertThat(tzRounding.round(time("2014-10-26T01:30:00+02:00")), isDate(time("2014-10-26T01:00:00+02:00"), tz));
        assertThat(tzRounding.round(time("2014-10-26T02:30:00+02:00")), isDate(time("2014-10-26T02:00:00+02:00"), tz));

        // Day interval
        tzRounding = Rounding.builder(Rounding.DateTimeUnit.DAY_OF_MONTH).timeZone(tz).build();
        assertThat(tzRounding.round(time("2014-11-11T17:00:00", tz)), isDate(time("2014-11-11T00:00:00", tz), tz));
        // DST on
        assertThat(tzRounding.round(time("2014-08-11T17:00:00", tz)), isDate(time("2014-08-11T00:00:00", tz), tz));
        // Day of switching DST on -> off
        assertThat(tzRounding.round(time("2014-10-26T17:00:00", tz)), isDate(time("2014-10-26T00:00:00", tz), tz));
        // Day of switching DST off -> on
        assertThat(tzRounding.round(time("2015-03-27T17:00:00", tz)), isDate(time("2015-03-27T00:00:00", tz), tz));

        // Month interval
        tzRounding = Rounding.builder(Rounding.DateTimeUnit.MONTH_OF_YEAR).timeZone(tz).build();
        assertThat(tzRounding.round(time("2014-11-11T17:00:00", tz)), isDate(time("2014-11-01T00:00:00", tz), tz));
        // DST on
        assertThat(tzRounding.round(time("2014-10-10T17:00:00", tz)), isDate(time("2014-10-01T00:00:00", tz), tz));

        // Year interval
        tzRounding = Rounding.builder(Rounding.DateTimeUnit.YEAR_OF_CENTURY).timeZone(tz).build();
        assertThat(tzRounding.round(time("2014-11-11T17:00:00", tz)), isDate(time("2014-01-01T00:00:00", tz), tz));

        // Two timestamps in same year and different timezone offset ("Double buckets" issue - #9491)
        tzRounding = Rounding.builder(Rounding.DateTimeUnit.YEAR_OF_CENTURY).timeZone(tz).build();
        assertThat(tzRounding.round(time("2014-11-11T17:00:00", tz)),
            isDate(tzRounding.round(time("2014-08-11T17:00:00", tz)), tz));
    }

    /**
     * test for #10025, strict local to UTC conversion can cause joda exceptions
     * on DST start
     */
    public void testLenientConversionDST() {
        ZoneId tz = ZoneId.of("America/Sao_Paulo");

        long start = time("2014-10-18T20:50:00.000", tz);
        long end = time("2014-10-19T01:00:00.000", tz);
        Rounding tzRounding = new Rounding.TimeUnitRounding(Rounding.DateTimeUnit.MINUTES_OF_HOUR, tz);
        Rounding dayTzRounding = new Rounding.TimeIntervalRounding(60000, tz);
        for (long time = start; time < end; time = time + 60000) {
            assertThat(tzRounding.nextRoundingValue(time), greaterThan(time));
            assertThat(dayTzRounding.nextRoundingValue(time), greaterThan(time));
        }
    }

    public void testEdgeCasesTransition() {
        {
            // standard +/-1 hour DST transition, CET
            ZoneId tz = ZoneId.of("CET");
            Rounding rounding = new Rounding.TimeUnitRounding(Rounding.DateTimeUnit.HOUR_OF_DAY, tz);

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
            ZoneId tz = ZoneId.of("Asia/Kathmandu");
            Rounding rounding = new Rounding.TimeUnitRounding(Rounding.DateTimeUnit.HOUR_OF_DAY, tz);

            assertInterval(time("1985-12-31T22:00:00.000+05:30"), time("1985-12-31T23:00:00.000+05:30"), rounding, 60, tz);
            assertInterval(time("1985-12-31T23:00:00.000+05:30"), time("1986-01-01T01:00:00.000+05:45"), rounding, 105, tz);
            assertInterval(time("1986-01-01T01:00:00.000+05:45"), time("1986-01-01T02:00:00.000+05:45"), rounding, 60, tz);
        }

        {
            // time zone "Australia/Lord_Howe"
            // 3 Mar 1991 - Daylight Saving Time Ended
            // at 02:00:00 clocks were turned backward 0:30 hours to Sunday, 3 March 1991, 01:30:00
            ZoneId tz = ZoneId.of("Australia/Lord_Howe");
            Rounding rounding = new Rounding.TimeUnitRounding(Rounding.DateTimeUnit.HOUR_OF_DAY, tz);

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
            ZoneId tz = ZoneId.of("Pacific/Chatham");
            Rounding rounding = new Rounding.TimeUnitRounding(Rounding.DateTimeUnit.HOUR_OF_DAY, tz);

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

        ZoneId tz = ZoneId.of("Europe/Rome");
        Rounding rounding = new Rounding.TimeUnitRounding(Rounding.DateTimeUnit.DAY_OF_MONTH, tz);

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
        ZoneId tz = ZoneId.of("America/St_Johns");
        Rounding rounding = new Rounding.TimeUnitRounding(Rounding.DateTimeUnit.DAY_OF_MONTH, tz);

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
     * Tests for DST transitions that cause the rounding to jump "backwards" because they round
     * from one back to the previous day. Usually these rounding start before
     */
    public void testForwardsBackwardsTimeZones() {
        for (String zoneId : JAVA_ZONE_IDS) {
            ZoneId tz = ZoneId.of(zoneId);
            ZoneRules rules = tz.getRules();
            for (ZoneOffsetTransition transition : rules.getTransitions()) {
                checkForForwardsBackwardsTransition(tz, transition);
            }
            int firstYear;
            if (rules.getTransitions().isEmpty()) {
                // Pick an arbitrary year to start the range
                firstYear = 1999;
            } else {
                ZoneOffsetTransition lastTransition = rules.getTransitions().get(rules.getTransitions().size() - 1);
                firstYear = lastTransition.getDateTimeAfter().getYear() + 1;
            }
            // Pick an arbitrary year to end the range too
            int lastYear = 2050;
            int year = randomFrom(firstYear, lastYear);
            for (ZoneOffsetTransitionRule transitionRule : rules.getTransitionRules()) {
                ZoneOffsetTransition transition = transitionRule.createTransition(year);
                checkForForwardsBackwardsTransition(tz, transition);
            }
        }
    }

    private void checkForForwardsBackwardsTransition(ZoneId tz, ZoneOffsetTransition transition) {
        if (transition.getDateTimeBefore().getYear() < 1950) {
            // We don't support transitions far in the past at all
            return;
        }
        if (false == transition.isOverlap()) {
            // Only overlaps cause the array rounding to have trouble
            return;
        }
        if (transition.getDateTimeBefore().getDayOfMonth() == transition.getDateTimeAfter().getDayOfMonth()) {
            // Only when the rounding changes the day
            return;
        }
        if (transition.getDateTimeBefore().getMinute() == 0) {
            // But roundings that change *at* midnight are safe because they don't "jump" to the next day.
            return;
        }
        logger.info(
            "{} from {}{} to {}{}",
            tz,
            transition.getDateTimeBefore(),
            transition.getOffsetBefore(),
            transition.getDateTimeAfter(),
            transition.getOffsetAfter()
        );
        long millisSinceEpoch = TimeUnit.SECONDS.toMillis(transition.toEpochSecond());
        long twoHours = TimeUnit.HOURS.toMillis(2);
        assertUnitRoundingSameAsJavaUtilTimeImplementation(
            Rounding.DateTimeUnit.DAY_OF_MONTH,
            tz,
            millisSinceEpoch - twoHours,
            millisSinceEpoch + twoHours
        );
    }

    /**
     * tests for dst transition with overlaps and day roundings.
     */
    public void testDST_END_Edgecases() {
        // First case, dst happens at 1am local time, switching back one hour.
        // We want the overlapping hour to count for the next day, making it a 25h interval

        ZoneId tz = ZoneId.of("Atlantic/Azores");
        Rounding.DateTimeUnit timeUnit = Rounding.DateTimeUnit.DAY_OF_MONTH;
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

        tz = ZoneId.of("America/Lima");
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

    public void testBeforeOverlapLarge() {
        // Moncton has a perfectly normal hour long Daylight Savings time.
        ZoneId tz = ZoneId.of("America/Moncton");
        Rounding rounding = Rounding.builder(Rounding.DateTimeUnit.HOUR_OF_DAY).timeZone(tz).build();
        assertThat(rounding.round(time("2003-10-26T03:43:35.079Z")), isDate(time("2003-10-26T03:00:00Z"), tz));
    }

    public void testBeforeOverlapSmall() {
        /*
         * Lord Howe is fun because Daylight Savings time is only 30 minutes
         * so we round HOUR_OF_DAY differently.
         */
        ZoneId tz = ZoneId.of("Australia/Lord_Howe");
        Rounding rounding = Rounding.builder(Rounding.DateTimeUnit.HOUR_OF_DAY).timeZone(tz).build();
        assertThat(rounding.round(time("2018-03-31T15:25:15.148Z")), isDate(time("2018-03-31T14:00:00Z"), tz));
    }

    public void testQuarterOfYear() {
        /*
         * If we're not careful with how we look up local time offsets we can
         * end up not loading the offsets far enough back to round this time
         * to QUARTER_OF_YEAR properly.
         */
        ZoneId tz = ZoneId.of("Asia/Baghdad");
        Rounding rounding = Rounding.builder(Rounding.DateTimeUnit.QUARTER_OF_YEAR).timeZone(tz).build();
        assertThat(rounding.round(time("2006-12-31T13:21:44.308Z")), isDate(time("2006-09-30T20:00:00Z"), tz));
    }

    public void testPrepareLongRangeRoundsToMidnight() {
        ZoneId tz = ZoneId.of("America/New_York");
        long min = time("01980-01-01T00:00:00Z");
        long max = time("10000-01-01T00:00:00Z");
        Rounding rounding = Rounding.builder(Rounding.DateTimeUnit.QUARTER_OF_YEAR).timeZone(tz).build();
        assertThat(rounding.round(time("2006-12-31T13:21:44.308Z")), isDate(time("2006-10-01T04:00:00Z"), tz));
        assertThat(rounding.round(time("9000-12-31T13:21:44.308Z")), isDate(time("9000-10-01T04:00:00Z"), tz));

        Rounding.Prepared prepared = rounding.prepare(min, max);
        assertThat(prepared.round(time("2006-12-31T13:21:44.308Z")), isDate(time("2006-10-01T04:00:00Z"), tz));
        assertThat(prepared.round(time("9000-12-31T13:21:44.308Z")), isDate(time("9000-10-01T04:00:00Z"), tz));
    }

    public void testPrepareLongRangeRoundsNotToMidnight() {
        ZoneId tz = ZoneId.of("Australia/Lord_Howe");
        long min = time("01980-01-01T00:00:00Z");
        long max = time("10000-01-01T00:00:00Z");
        Rounding rounding = Rounding.builder(Rounding.DateTimeUnit.HOUR_OF_DAY).timeZone(tz).build();
        assertThat(rounding.round(time("2018-03-31T15:25:15.148Z")), isDate(time("2018-03-31T14:00:00Z"), tz));
        assertThat(rounding.round(time("9000-03-31T15:25:15.148Z")), isDate(time("9000-03-31T15:00:00Z"), tz));

        Rounding.Prepared prepared = rounding.prepare(min, max);
        assertThat(prepared.round(time("2018-03-31T15:25:15.148Z")), isDate(time("2018-03-31T14:00:00Z"), tz));
        assertThat(prepared.round(time("9000-03-31T15:25:15.148Z")), isDate(time("9000-03-31T15:00:00Z"), tz));
    }

    /**
     * Example of when we round past when local clocks were wound forward.
     */
    public void testIntervalBeforeGap() {
        ZoneId tz = ZoneId.of("Africa/Cairo");
        Rounding rounding = Rounding.builder(TimeValue.timeValueDays(257)).timeZone(tz).build();
        assertThat(rounding.round(time("1969-07-08T09:00:14.599Z")), isDate(time("1969-04-18T22:00:00Z"), tz));
    }

    /**
     * Example of when we round past when local clocks were wound backwards,
     * <strong>and</strong> then past the time they were wound forwards before
     * that. So, we jumped back a long, long way.
     */
    public void testIntervalTwoTransitions() {
        ZoneId tz = ZoneId.of("America/Detroit");
        Rounding rounding = Rounding.builder(TimeValue.timeValueDays(279)).timeZone(tz).build();
        assertThat(rounding.round(time("1982-11-10T02:51:22.662Z")), isDate(time("1982-03-23T05:00:00Z"), tz));
    }

    public void testFixedIntervalRoundingSize() {
        Rounding unitRounding = Rounding.builder(TimeValue.timeValueHours(10)).build();
        Rounding.Prepared prepared = unitRounding.prepare(time("2010-01-01T00:00:00.000Z"), time("2020-01-01T00:00:00.000Z"));
        assertThat(prepared.roundingSize(time("2015-01-01T00:00:00.000Z"), Rounding.DateTimeUnit.SECOND_OF_MINUTE),
            closeTo(36000.0, 0.000001));
        assertThat(prepared.roundingSize(time("2015-01-01T00:00:00.000Z"), Rounding.DateTimeUnit.MINUTES_OF_HOUR),
            closeTo(600.0, 0.000001));
        assertThat(prepared.roundingSize(time("2015-01-01T00:00:00.000Z"), Rounding.DateTimeUnit.HOUR_OF_DAY),
            closeTo(10.0, 0.000001));
        assertThat(prepared.roundingSize(time("2015-01-01T00:00:00.000Z"), Rounding.DateTimeUnit.DAY_OF_MONTH),
            closeTo(10.0 / 24.0, 0.000001));
        assertThat(prepared.roundingSize(time("2015-01-01T00:00:00.000Z"), Rounding.DateTimeUnit.WEEK_OF_WEEKYEAR),
            closeTo(10.0 / 168.0, 0.000001));
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
            () -> prepared.roundingSize(time("2015-01-01T00:00:00.000Z"), Rounding.DateTimeUnit.MONTH_OF_YEAR));
        assertThat(ex.getMessage(), equalTo("Cannot use month-based rate unit [month] with fixed interval based histogram, " +
            "only week, day, hour, minute and second are supported for this histogram"));
        ex = expectThrows(IllegalArgumentException.class,
            () -> prepared.roundingSize(time("2015-01-01T00:00:00.000Z"), Rounding.DateTimeUnit.QUARTER_OF_YEAR));
        assertThat(ex.getMessage(), equalTo("Cannot use month-based rate unit [quarter] with fixed interval based histogram, " +
            "only week, day, hour, minute and second are supported for this histogram"));
        ex = expectThrows(IllegalArgumentException.class,
            () -> prepared.roundingSize(time("2015-01-01T00:00:00.000Z"), Rounding.DateTimeUnit.YEAR_OF_CENTURY));
        assertThat(ex.getMessage(), equalTo("Cannot use month-based rate unit [year] with fixed interval based histogram, " +
            "only week, day, hour, minute and second are supported for this histogram"));
    }

    public void testMillisecondsBasedUnitCalendarRoundingSize() {
        Rounding unitRounding = Rounding.builder(Rounding.DateTimeUnit.HOUR_OF_DAY).build();
        Rounding.Prepared prepared = unitRounding.prepare(time("2010-01-01T00:00:00.000Z"), time("2020-01-01T00:00:00.000Z"));
        assertThat(prepared.roundingSize(time("2015-01-01T00:00:00.000Z"), Rounding.DateTimeUnit.SECOND_OF_MINUTE),
            closeTo(3600.0, 0.000001));
        assertThat(prepared.roundingSize(Rounding.DateTimeUnit.SECOND_OF_MINUTE),
            closeTo(3600.0, 0.000001));
        assertThat(prepared.roundingSize(time("2015-01-01T00:00:00.000Z"), Rounding.DateTimeUnit.MINUTES_OF_HOUR),
            closeTo(60.0, 0.000001));
        assertThat(prepared.roundingSize(Rounding.DateTimeUnit.MINUTES_OF_HOUR),
            closeTo(60.0, 0.000001));
        assertThat(prepared.roundingSize(time("2015-01-01T00:00:00.000Z"), Rounding.DateTimeUnit.HOUR_OF_DAY),
            closeTo(1.0, 0.000001));
        assertThat(prepared.roundingSize(Rounding.DateTimeUnit.HOUR_OF_DAY),
            closeTo(1.0, 0.000001));
        assertThat(prepared.roundingSize(time("2015-01-01T00:00:00.000Z"), Rounding.DateTimeUnit.DAY_OF_MONTH),
            closeTo(1 / 24.0, 0.000001));
        assertThat(prepared.roundingSize(Rounding.DateTimeUnit.DAY_OF_MONTH),
            closeTo(1 / 24.0, 0.000001));
        assertThat(prepared.roundingSize(time("2015-01-01T00:00:00.000Z"), Rounding.DateTimeUnit.WEEK_OF_WEEKYEAR),
            closeTo(1 / 168.0, 0.000001));
        assertThat(prepared.roundingSize(Rounding.DateTimeUnit.WEEK_OF_WEEKYEAR),
            closeTo(1 / 168.0, 0.000001));
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
            () -> prepared.roundingSize(time("2015-01-01T00:00:00.000Z"), Rounding.DateTimeUnit.MONTH_OF_YEAR));
        assertThat(ex.getMessage(), equalTo("Cannot use month-based rate unit [month] with non-month based calendar interval " +
            "histogram [hour] only week, day, hour, minute and second are supported for this histogram"));
        ex = expectThrows(IllegalArgumentException.class,
            () -> prepared.roundingSize(Rounding.DateTimeUnit.MONTH_OF_YEAR));
        assertThat(ex.getMessage(), equalTo("Cannot use month-based rate unit [month] with non-month based calendar interval " +
            "histogram [hour] only week, day, hour, minute and second are supported for this histogram"));
        ex = expectThrows(IllegalArgumentException.class,
            () -> prepared.roundingSize(time("2015-01-01T00:00:00.000Z"), Rounding.DateTimeUnit.QUARTER_OF_YEAR));
        assertThat(ex.getMessage(), equalTo("Cannot use month-based rate unit [quarter] with non-month based calendar interval " +
            "histogram [hour] only week, day, hour, minute and second are supported for this histogram"));
        ex = expectThrows(IllegalArgumentException.class,
            () -> prepared.roundingSize(Rounding.DateTimeUnit.QUARTER_OF_YEAR));
        assertThat(ex.getMessage(), equalTo("Cannot use month-based rate unit [quarter] with non-month based calendar interval " +
            "histogram [hour] only week, day, hour, minute and second are supported for this histogram"));
        ex = expectThrows(IllegalArgumentException.class,
            () -> prepared.roundingSize(time("2015-01-01T00:00:00.000Z"), Rounding.DateTimeUnit.YEAR_OF_CENTURY));
        assertThat(ex.getMessage(), equalTo("Cannot use month-based rate unit [year] with non-month based calendar interval " +
            "histogram [hour] only week, day, hour, minute and second are supported for this histogram"));
        ex = expectThrows(IllegalArgumentException.class,
            () -> prepared.roundingSize(Rounding.DateTimeUnit.YEAR_OF_CENTURY));
        assertThat(ex.getMessage(), equalTo("Cannot use month-based rate unit [year] with non-month based calendar interval " +
            "histogram [hour] only week, day, hour, minute and second are supported for this histogram"));
    }

    public void testNonMillisecondsBasedUnitCalendarRoundingSize() {
        Rounding unitRounding = Rounding.builder(Rounding.DateTimeUnit.QUARTER_OF_YEAR).build();
        Rounding.Prepared prepared = unitRounding.prepare(time("2010-01-01T00:00:00.000Z"), time("2020-01-01T00:00:00.000Z"));
        long firstQuarter = prepared.round(time("2015-01-01T00:00:00.000Z"));
        // Ratio based
        assertThat(prepared.roundingSize(firstQuarter, Rounding.DateTimeUnit.MONTH_OF_YEAR), closeTo(3.0, 0.000001));
        assertThat(prepared.roundingSize(firstQuarter, Rounding.DateTimeUnit.QUARTER_OF_YEAR), closeTo(1.0, 0.000001));
        assertThat(prepared.roundingSize(firstQuarter, Rounding.DateTimeUnit.YEAR_OF_CENTURY), closeTo(0.25, 0.000001));
        assertThat(prepared.roundingSize(Rounding.DateTimeUnit.MONTH_OF_YEAR), closeTo(3.0, 0.000001));
        assertThat(prepared.roundingSize(Rounding.DateTimeUnit.QUARTER_OF_YEAR), closeTo(1.0, 0.000001));
        assertThat(prepared.roundingSize(Rounding.DateTimeUnit.YEAR_OF_CENTURY), closeTo(0.25, 0.000001));
        // Real interval based
        assertThat(prepared.roundingSize(firstQuarter, Rounding.DateTimeUnit.SECOND_OF_MINUTE), closeTo(7776000.0, 0.000001));
        assertThat(prepared.roundingSize(firstQuarter, Rounding.DateTimeUnit.MINUTES_OF_HOUR), closeTo(129600.0, 0.000001));
        assertThat(prepared.roundingSize(firstQuarter, Rounding.DateTimeUnit.HOUR_OF_DAY), closeTo(2160.0, 0.000001));
        assertThat(prepared.roundingSize(firstQuarter, Rounding.DateTimeUnit.DAY_OF_MONTH), closeTo(90.0, 0.000001));
        long thirdQuarter = prepared.round(time("2015-07-01T00:00:00.000Z"));
        assertThat(prepared.roundingSize(thirdQuarter, Rounding.DateTimeUnit.DAY_OF_MONTH), closeTo(92.0, 0.000001));
        assertThat(prepared.roundingSize(thirdQuarter, Rounding.DateTimeUnit.HOUR_OF_DAY), closeTo(2208.0, 0.000001));
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
            () -> prepared.roundingSize(Rounding.DateTimeUnit.SECOND_OF_MINUTE));
        assertThat(ex.getMessage(), equalTo("Cannot use non month-based rate unit [second] with calendar interval histogram " +
            "[quarter] only month, quarter and year are supported for this histogram"));
    }

    public void testFixedRoundingPoints() {
        Rounding rounding = Rounding.builder(Rounding.DateTimeUnit.QUARTER_OF_YEAR).build();
        assertFixedRoundingPoints(
            rounding.prepare(time("2020-01-01T00:00:00"), time("2021-01-01T00:00:00")),
            "2020-01-01T00:00:00",
            "2020-04-01T00:00:00",
            "2020-07-01T00:00:00",
            "2020-10-01T00:00:00",
            "2021-01-01T00:00:00"
        );
        rounding = Rounding.builder(Rounding.DateTimeUnit.DAY_OF_MONTH).build();
        assertFixedRoundingPoints(
            rounding.prepare(time("2020-01-01T00:00:00"), time("2020-01-06T00:00:00")),
            "2020-01-01T00:00:00",
            "2020-01-02T00:00:00",
            "2020-01-03T00:00:00",
            "2020-01-04T00:00:00",
            "2020-01-05T00:00:00",
            "2020-01-06T00:00:00"
        );
    }

    private void assertFixedRoundingPoints(Rounding.Prepared prepared, String... expected) {
        assertThat(
            Arrays.stream(prepared.fixedRoundingPoints()).mapToObj(Instant::ofEpochMilli).collect(toList()),
            equalTo(Arrays.stream(expected).map(RoundingTests::time).map(Instant::ofEpochMilli).collect(toList()))
        );
    }

    private void assertInterval(long rounded, long nextRoundingValue, Rounding rounding, int minutes,
                                ZoneId tz) {
        assertInterval(rounded, dateBetween(rounded, nextRoundingValue), nextRoundingValue, rounding, tz);
        long millisPerMinute = 60_000;
        assertEquals(millisPerMinute * minutes, nextRoundingValue - rounded);
    }

    /**
     * perform a number on assertions and checks on {@link org.elasticsearch.common.Rounding.TimeUnitRounding} intervals
     * @param rounded the expected low end of the rounding interval
     * @param unrounded a date in the interval to be checked for rounding
     * @param nextRoundingValue the expected upper end of the rounding interval
     * @param rounding the rounding instance
     */
    private void assertInterval(long rounded, long unrounded, long nextRoundingValue, Rounding rounding, ZoneId tz) {
        assertThat("rounding should be idempotent", rounding.round(rounded), isDate(rounded, tz));
        assertThat("rounded value smaller or equal than unrounded", rounded, lessThanOrEqualTo(unrounded));
        assertThat("values less than rounded should round further down", rounding.round(rounded - 1), lessThan(rounded));
        assertThat("nextRounding value should be a rounded date", rounding.round(nextRoundingValue), isDate(nextRoundingValue, tz));
        assertThat("values above nextRounding should round down there", rounding.round(nextRoundingValue + 1),
            isDate(nextRoundingValue, tz));

        if (isTimeWithWellDefinedRounding(tz, unrounded)) {
            assertThat("nextRounding value should be greater than date" + rounding, nextRoundingValue, greaterThan(unrounded));

            long dateBetween = dateBetween(rounded, nextRoundingValue);
            long roundingDateBetween = rounding.round(dateBetween);
            ZonedDateTime zonedDateBetween = ZonedDateTime.ofInstant(Instant.ofEpochMilli(dateBetween), tz);
            assertThat("dateBetween [" + zonedDateBetween + "/" + dateBetween + "] should round down to roundedDate [" +
                    Instant.ofEpochMilli(roundingDateBetween) + "]", roundingDateBetween, isDate(rounded, tz));
            assertThat("dateBetween [" + zonedDateBetween + "] should round up to nextRoundingValue",
                rounding.nextRoundingValue(dateBetween), isDate(nextRoundingValue, tz));
        }
    }

    private static boolean isTimeWithWellDefinedRounding(ZoneId tz, long t) {
        if (tz.getId().equals("America/St_Johns")
            || tz.getId().equals("America/Goose_Bay")
            || tz.getId().equals("America/Moncton")
            || tz.getId().equals("Canada/Newfoundland")) {

            // Clocks went back at 00:01 between 1987 and 2010, causing overlapping days.
            // These timezones are otherwise uninteresting, so just skip this period.

            return t <= time("1987-10-01T00:00:00Z")
                || t >= time("2010-12-01T00:00:00Z");
        }

        if (tz.getId().equals("Antarctica/Casey")) {

            // Clocks went back 3 hours at 02:00 on 2010-03-05, causing overlapping days.

            return t <= time("2010-03-03T00:00:00Z")
                || t >= time("2010-03-07T00:00:00Z");
        }
        if (tz.getId().equals("Pacific/Guam") || tz.getId().equals("Pacific/Saipan")) {
            // Clocks went back at 00:01 in 1969, causing overlapping days.
            return t <= time("1969-01-25T00:00:00Z")
                || t >= time("1969-01-26T00:00:00Z");
        }

        return true;
    }

    private static long randomDate() {
        return Math.abs(randomLong() % (2 * (long) 10e11)); // 1970-01-01T00:00:00Z - 2033-05-18T05:33:20.000+02:00
    }

    private static long[] randomDateBounds(Rounding.DateTimeUnit unit) {
        long b1 = randomDate();
        if (randomBoolean()) {
            // Sometimes use a fairly close date
            return new long[] {b1, b1 + unit.extraLocalOffsetLookup() * between(1, 40)};
        }
        // Otherwise use a totally random date
        long b2 = randomValueOtherThan(b1, RoundingTests::randomDate);
        if (b1 < b2) {
            return new long[] {b1, b2};
        }
        return new long[] {b2, b1};
    }
    private static long dateBetween(long lower, long upper) {
        long dateBetween = randomLongBetween(lower, upper - 1);
        assert lower <= dateBetween && dateBetween < upper;
        return dateBetween;
    }

    private static long time(String time) {
        return time(time, ZoneOffset.UTC);
    }

    private static long time(String time, ZoneId zone) {
        TemporalAccessor accessor = DateFormatter.forPattern("date_optional_time").withZone(zone).parse(time);
        return DateFormatters.from(accessor).toInstant().toEpochMilli();
    }

    private static Matcher<Long> isDate(final long expected, ZoneId tz) {
        return new TypeSafeMatcher<Long>() {
            @Override
            public boolean matchesSafely(final Long item) {
                return expected == item;
            }

            @Override
            public void describeTo(Description description) {
                ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(expected), tz);
                description.appendText(DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(zonedDateTime) + " [" + expected + "] ");
            }

            @Override
            protected void describeMismatchSafely(final Long actual, final Description mismatchDescription) {
                ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(actual), tz);
                mismatchDescription.appendText(" was ")
                    .appendValue(DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(zonedDateTime) + " [" + actual + "]");
            }
        };
    }

}
