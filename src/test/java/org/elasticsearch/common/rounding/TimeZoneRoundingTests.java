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
import org.elasticsearch.test.ElasticsearchTestCase;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 */
public class TimeZoneRoundingTests extends ElasticsearchTestCase {

    @Test
    public void testUTCMonthRounding() {
        Rounding tzRounding = TimeZoneRounding.builder(DateTimeUnit.MONTH_OF_YEAR).build();
        assertThat(tzRounding.round(utc("2009-02-03T01:01:01")), equalTo(utc("2009-02-01T00:00:00.000Z")));
        assertThat(tzRounding.nextRoundingValue(utc("2009-02-01T00:00:00.000Z")), equalTo(utc("2009-03-01T00:00:00.000Z")));

        tzRounding = TimeZoneRounding.builder(DateTimeUnit.WEEK_OF_WEEKYEAR).build();
        assertThat(tzRounding.round(utc("2012-01-10T01:01:01")), equalTo(utc("2012-01-09T00:00:00.000Z")));
        assertThat(tzRounding.nextRoundingValue(utc("2012-01-09T00:00:00.000Z")), equalTo(utc("2012-01-16T00:00:00.000Z")));

        tzRounding = TimeZoneRounding.builder(DateTimeUnit.WEEK_OF_WEEKYEAR).postOffset(-TimeValue.timeValueHours(24).millis()).build();
        assertThat(tzRounding.round(utc("2012-01-10T01:01:01")), equalTo(utc("2012-01-08T00:00:00.000Z")));
        assertThat(tzRounding.nextRoundingValue(utc("2012-01-08T00:00:00.000Z")), equalTo(utc("2012-01-15T00:00:00.000Z")));
    }

    @Test
    public void testDayTimeZoneRounding() {
        Rounding tzRounding = TimeZoneRounding.builder(DateTimeUnit.DAY_OF_MONTH).preZone(DateTimeZone.forOffsetHours(-2)).build();
        assertThat(tzRounding.round(0), equalTo(0l - TimeValue.timeValueHours(24).millis()));
        assertThat(tzRounding.nextRoundingValue(0l - TimeValue.timeValueHours(24).millis()), equalTo(0l));

        tzRounding = TimeZoneRounding.builder(DateTimeUnit.DAY_OF_MONTH).preZone(DateTimeZone.forOffsetHours(-2)).postZone(DateTimeZone.forOffsetHours(-2)).build();
        assertThat(tzRounding.round(0), equalTo(0l - TimeValue.timeValueHours(26).millis()));
        assertThat(tzRounding.nextRoundingValue(0l - TimeValue.timeValueHours(26).millis()), equalTo(-TimeValue.timeValueHours(2).millis()));

        tzRounding = TimeZoneRounding.builder(DateTimeUnit.DAY_OF_MONTH).preZone(DateTimeZone.forOffsetHours(-2)).build();
        assertThat(tzRounding.round(utc("2009-02-03T01:01:01")), equalTo(utc("2009-02-02T00:00:00")));
        assertThat(tzRounding.nextRoundingValue(utc("2009-02-02T00:00:00")), equalTo(utc("2009-02-03T00:00:00")));

        tzRounding = TimeZoneRounding.builder(DateTimeUnit.DAY_OF_MONTH).preZone(DateTimeZone.forOffsetHours(-2)).postZone(DateTimeZone.forOffsetHours(-2)).build();
        assertThat(tzRounding.round(utc("2009-02-03T01:01:01")), equalTo(time("2009-02-02T00:00:00", DateTimeZone.forOffsetHours(+2))));
        assertThat(tzRounding.nextRoundingValue(time("2009-02-02T00:00:00", DateTimeZone.forOffsetHours(+2))), equalTo(time("2009-02-03T00:00:00", DateTimeZone.forOffsetHours(+2))));
    }

    @Test
    public void testTimeTimeZoneRounding() {
        Rounding tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).preZone(DateTimeZone.forOffsetHours(-2)).build();
        assertThat(tzRounding.round(0), equalTo(0l));
        assertThat(tzRounding.nextRoundingValue(0l), equalTo(TimeValue.timeValueHours(1l).getMillis()));

        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).preZone(DateTimeZone.forOffsetHours(-2)).postZone(DateTimeZone.forOffsetHours(-2)).build();
        assertThat(tzRounding.round(0), equalTo(0l - TimeValue.timeValueHours(2).millis()));
        assertThat(tzRounding.nextRoundingValue(0l - TimeValue.timeValueHours(2).millis()), equalTo(0l - TimeValue.timeValueHours(1).millis()));

        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).preZone(DateTimeZone.forOffsetHours(-2)).build();
        assertThat(tzRounding.round(utc("2009-02-03T01:01:01")), equalTo(utc("2009-02-03T01:00:00")));
        assertThat(tzRounding.nextRoundingValue(utc("2009-02-03T01:00:00")), equalTo(utc("2009-02-03T02:00:00")));

        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).preZone(DateTimeZone.forOffsetHours(-2)).postZone(DateTimeZone.forOffsetHours(-2)).build();
        assertThat(tzRounding.round(utc("2009-02-03T01:01:01")), equalTo(time("2009-02-03T01:00:00", DateTimeZone.forOffsetHours(+2))));
        assertThat(tzRounding.nextRoundingValue(time("2009-02-03T01:00:00", DateTimeZone.forOffsetHours(+2))), equalTo(time("2009-02-03T02:00:00", DateTimeZone.forOffsetHours(+2))));
    }
    
    @Test
    public void testTimeTimeZoneRoundingDST() {
        Rounding tzRounding;
        // testing savings to non savings switch 
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).preZone(DateTimeZone.forID("UTC")).build();
        assertThat(tzRounding.round(time("2014-10-26T01:01:01", DateTimeZone.forID("CET"))), equalTo(time("2014-10-26T01:00:00", DateTimeZone.forID("CET"))));
        
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).preZone(DateTimeZone.forID("CET")).build();
        assertThat(tzRounding.round(time("2014-10-26T01:01:01", DateTimeZone.forID("CET"))), equalTo(time("2014-10-26T01:00:00", DateTimeZone.forID("CET"))));
        
        // testing non savings to savings switch
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).preZone(DateTimeZone.forID("UTC")).build();
        assertThat(tzRounding.round(time("2014-03-30T01:01:01", DateTimeZone.forID("CET"))), equalTo(time("2014-03-30T01:00:00", DateTimeZone.forID("CET"))));
        
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).preZone(DateTimeZone.forID("CET")).build();
        assertThat(tzRounding.round(time("2014-03-30T01:01:01", DateTimeZone.forID("CET"))), equalTo(time("2014-03-30T01:00:00", DateTimeZone.forID("CET"))));
        
        // testing non savings to savings switch (America/Chicago)
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).preZone(DateTimeZone.forID("UTC")).build();
        assertThat(tzRounding.round(time("2014-03-09T03:01:01", DateTimeZone.forID("America/Chicago"))), equalTo(time("2014-03-09T03:00:00", DateTimeZone.forID("America/Chicago"))));
        
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).preZone(DateTimeZone.forID("America/Chicago")).build();
        assertThat(tzRounding.round(time("2014-03-09T03:01:01", DateTimeZone.forID("America/Chicago"))), equalTo(time("2014-03-09T03:00:00", DateTimeZone.forID("America/Chicago"))));
        
        // testing savings to non savings switch 2013 (America/Chicago)
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).preZone(DateTimeZone.forID("UTC")).build();
        assertThat(tzRounding.round(time("2013-11-03T06:01:01", DateTimeZone.forID("America/Chicago"))), equalTo(time("2013-11-03T06:00:00", DateTimeZone.forID("America/Chicago"))));
        
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).preZone(DateTimeZone.forID("America/Chicago")).build();
        assertThat(tzRounding.round(time("2013-11-03T06:01:01", DateTimeZone.forID("America/Chicago"))), equalTo(time("2013-11-03T06:00:00", DateTimeZone.forID("America/Chicago"))));
        
        // testing savings to non savings switch 2014 (America/Chicago)
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).preZone(DateTimeZone.forID("UTC")).build();
        assertThat(tzRounding.round(time("2014-11-02T06:01:01", DateTimeZone.forID("America/Chicago"))), equalTo(time("2014-11-02T06:00:00", DateTimeZone.forID("America/Chicago"))));
        
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).preZone(DateTimeZone.forID("America/Chicago")).build();
        assertThat(tzRounding.round(time("2014-11-02T06:01:01", DateTimeZone.forID("America/Chicago"))), equalTo(time("2014-11-02T06:00:00", DateTimeZone.forID("America/Chicago"))));
    }

    @Test
    public void testPreZoneAdjustLargeInterval() {
        Rounding tzRounding;

        // Day interval
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.DAY_OF_MONTH).preZone(DateTimeZone.forID("Asia/Jerusalem")).preZoneAdjustLargeInterval(true).build();
        assertThat(tzRounding.round(time("2014-11-11T17:00:00", DateTimeZone.forID("Asia/Jerusalem"))), equalTo(time("2014-11-11T00:00:00", DateTimeZone.forID("Asia/Jerusalem"))));
        // DST on
        assertThat(tzRounding.round(time("2014-08-11T17:00:00", DateTimeZone.forID("Asia/Jerusalem"))), equalTo(time("2014-08-11T00:00:00", DateTimeZone.forID("Asia/Jerusalem"))));
        // Day of switching DST on -> off
        assertThat(tzRounding.round(time("2014-10-26T17:00:00", DateTimeZone.forID("Asia/Jerusalem"))), equalTo(time("2014-10-26T00:00:00", DateTimeZone.forID("Asia/Jerusalem"))));
        // Day of switching DST off -> on
        assertThat(tzRounding.round(time("2015-03-27T17:00:00", DateTimeZone.forID("Asia/Jerusalem"))), equalTo(time("2015-03-27T00:00:00", DateTimeZone.forID("Asia/Jerusalem"))));

        // Month interval
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.MONTH_OF_YEAR).preZone(DateTimeZone.forID("Asia/Jerusalem")).preZoneAdjustLargeInterval(true).build();
        assertThat(tzRounding.round(time("2014-11-11T17:00:00", DateTimeZone.forID("Asia/Jerusalem"))), equalTo(time("2014-11-01T00:00:00", DateTimeZone.forID("Asia/Jerusalem"))));
        // DST on
        assertThat(tzRounding.round(time("2014-10-10T17:00:00", DateTimeZone.forID("Asia/Jerusalem"))), equalTo(time("2014-10-01T00:00:00", DateTimeZone.forID("Asia/Jerusalem"))));

        // Year interval
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.YEAR_OF_CENTURY).preZone(DateTimeZone.forID("Asia/Jerusalem")).preZoneAdjustLargeInterval(true).build();
        assertThat(tzRounding.round(time("2014-11-11T17:00:00", DateTimeZone.forID("Asia/Jerusalem"))), equalTo(time("2014-01-01T00:00:00", DateTimeZone.forID("Asia/Jerusalem"))));

        // Two timestamps in same year and different timezone offset ("Double buckets" issue - #9491)
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.YEAR_OF_CENTURY).preZone(DateTimeZone.forID("Asia/Jerusalem")).preZoneAdjustLargeInterval(true).build();
        assertThat(tzRounding.round(time("2014-11-11T17:00:00", DateTimeZone.forID("Asia/Jerusalem"))),
                equalTo(tzRounding.round(time("2014-08-11T17:00:00", DateTimeZone.forID("Asia/Jerusalem")))));
    }

    @Test
    public void testAmbiguousHoursAfterDSTSwitch() {
        Rounding tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).preZone(DateTimeZone.forID("Asia/Jerusalem")).build();
        // Both timestamps "2014-10-25T22:30:00Z" and "2014-10-25T23:30:00Z" are "2014-10-26T01:30:00" in local time because
        // of DST switch between them. This test checks that they are both returned to their correct UTC time after rounding.
        assertThat(tzRounding.round(time("2014-10-25T22:30:00", DateTimeZone.UTC)), equalTo(time("2014-10-25T22:00:00", DateTimeZone.UTC)));
        assertThat(tzRounding.round(time("2014-10-25T23:30:00", DateTimeZone.UTC)), equalTo(time("2014-10-25T23:00:00", DateTimeZone.UTC)));
    }

    @Test
    public void testNextRoundingValueCornerCase8209() {
        Rounding tzRounding = TimeZoneRounding.builder(DateTimeUnit.MONTH_OF_YEAR).preZone(DateTimeZone.forID("+01:00")).
                preZoneAdjustLargeInterval(true).build();
        long roundedValue = tzRounding.round(time("2014-01-01T00:00:00Z", DateTimeZone.UTC));
        assertThat(roundedValue, equalTo(time("2013-12-31T23:00:00.000Z", DateTimeZone.UTC)));
        roundedValue = tzRounding.nextRoundingValue(roundedValue);
        assertThat(roundedValue, equalTo(time("2014-01-31T23:00:00.000Z", DateTimeZone.UTC)));
        roundedValue = tzRounding.nextRoundingValue(roundedValue);
        assertThat(roundedValue, equalTo(time("2014-02-28T23:00:00.000Z", DateTimeZone.UTC)));
        roundedValue = tzRounding.nextRoundingValue(roundedValue);
        assertThat(roundedValue, equalTo(time("2014-03-31T23:00:00.000Z", DateTimeZone.UTC)));
        roundedValue = tzRounding.nextRoundingValue(roundedValue);
        assertThat(roundedValue, equalTo(time("2014-04-30T23:00:00.000Z", DateTimeZone.UTC)));
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
        Rounding tzRounding = new TimeZoneRounding.TimeTimeZoneRoundingFloor(DateTimeUnit.MINUTES_OF_HOUR, tz, 
                DateTimeZone.UTC);
        Rounding dayTzRounding = new TimeZoneRounding.DayTimeZoneRoundingFloor(DateTimeUnit.MINUTES_OF_HOUR, tz, 
                DateTimeZone.UTC);
        for (long time = start; time < end; time = time + 60000) {
            assertThat(tzRounding.nextRoundingValue(time), greaterThan(time));
            assertThat(dayTzRounding.nextRoundingValue(time), greaterThan(time));
        }
    }

    private long utc(String time) {
        return time(time, DateTimeZone.UTC);
    }

    private long time(String time, DateTimeZone zone) {
        return ISODateTimeFormat.dateOptionalTimeParser().withZone(zone).parseMillis(time);
    }
}
