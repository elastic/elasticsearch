/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.deps.joda;

import org.elasticsearch.common.joda.TimeZoneRounding;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.joda.time.Chronology;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class TimeZoneRoundingTests extends ElasticsearchTestCase {

    @Test
    public void testUTCMonthRounding() {
        TimeZoneRounding tzRounding = TimeZoneRounding.builder(chronology().monthOfYear()).build();
        assertThat(tzRounding.calc(utc("2009-02-03T01:01:01")), equalTo(utc("2009-02-01T00:00:00.000Z")));

        tzRounding = TimeZoneRounding.builder(chronology().weekOfWeekyear()).build();
        assertThat(tzRounding.calc(utc("2012-01-10T01:01:01")), equalTo(utc("2012-01-09T00:00:00.000Z")));

        tzRounding = TimeZoneRounding.builder(chronology().weekOfWeekyear()).postOffset(-TimeValue.timeValueHours(24).millis()).build();
        assertThat(tzRounding.calc(utc("2012-01-10T01:01:01")), equalTo(utc("2012-01-08T00:00:00.000Z")));
    }

    @Test
    public void testDayTimeZoneRounding() {
        TimeZoneRounding tzRounding = TimeZoneRounding.builder(chronology().dayOfMonth()).preZone(DateTimeZone.forOffsetHours(-2)).build();
        assertThat(tzRounding.calc(0), equalTo(0l - TimeValue.timeValueHours(24).millis()));

        tzRounding = TimeZoneRounding.builder(chronology().dayOfMonth()).preZone(DateTimeZone.forOffsetHours(-2)).postZone(DateTimeZone.forOffsetHours(-2)).build();
        assertThat(tzRounding.calc(0), equalTo(0l - TimeValue.timeValueHours(26).millis()));

        tzRounding = TimeZoneRounding.builder(chronology().dayOfMonth()).preZone(DateTimeZone.forOffsetHours(-2)).build();
        assertThat(tzRounding.calc(utc("2009-02-03T01:01:01")), equalTo(utc("2009-02-02T00:00:00")));

        tzRounding = TimeZoneRounding.builder(chronology().dayOfMonth()).preZone(DateTimeZone.forOffsetHours(-2)).postZone(DateTimeZone.forOffsetHours(-2)).build();
        assertThat(tzRounding.calc(utc("2009-02-03T01:01:01")), equalTo(time("2009-02-02T00:00:00", DateTimeZone.forOffsetHours(+2))));
    }

    @Test
    public void testTimeTimeZoneRounding() {
        TimeZoneRounding tzRounding = TimeZoneRounding.builder(chronology().hourOfDay()).preZone(DateTimeZone.forOffsetHours(-2)).build();
        assertThat(tzRounding.calc(0), equalTo(0l));

        tzRounding = TimeZoneRounding.builder(chronology().hourOfDay()).preZone(DateTimeZone.forOffsetHours(-2)).postZone(DateTimeZone.forOffsetHours(-2)).build();
        assertThat(tzRounding.calc(0), equalTo(0l - TimeValue.timeValueHours(2).millis()));

        tzRounding = TimeZoneRounding.builder(chronology().hourOfDay()).preZone(DateTimeZone.forOffsetHours(-2)).build();
        assertThat(tzRounding.calc(utc("2009-02-03T01:01:01")), equalTo(utc("2009-02-03T01:00:00")));

        tzRounding = TimeZoneRounding.builder(chronology().hourOfDay()).preZone(DateTimeZone.forOffsetHours(-2)).postZone(DateTimeZone.forOffsetHours(-2)).build();
        assertThat(tzRounding.calc(utc("2009-02-03T01:01:01")), equalTo(time("2009-02-03T01:00:00", DateTimeZone.forOffsetHours(+2))));
    }

    private static Chronology chronology() {
        return ISOChronology.getInstanceUTC();
    }

    private long utc(String time) {
        return time(time, DateTimeZone.UTC);
    }

    private long time(String time, DateTimeZone zone) {
        return ISODateTimeFormat.dateOptionalTimeParser().withZone(zone).parseMillis(time);
    }
}
