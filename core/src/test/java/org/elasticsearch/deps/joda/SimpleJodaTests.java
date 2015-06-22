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

package org.elasticsearch.deps.joda;

import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.joda.time.MutableDateTime;
import org.joda.time.format.*;
import org.junit.Test;

import java.util.Date;
import java.util.Locale;

import static org.hamcrest.Matchers.*;

/**
 *
 */
public class SimpleJodaTests extends ElasticsearchTestCase {

    @Test
    public void testMultiParsers() {
        DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
        DateTimeParser[] parsers = new DateTimeParser[3];
        parsers[0] = DateTimeFormat.forPattern("MM/dd/yyyy").withZone(DateTimeZone.UTC).getParser();
        parsers[1] = DateTimeFormat.forPattern("MM-dd-yyyy").withZone(DateTimeZone.UTC).getParser();
        parsers[2] = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(DateTimeZone.UTC).getParser();
        builder.append(DateTimeFormat.forPattern("MM/dd/yyyy").withZone(DateTimeZone.UTC).getPrinter(), parsers);

        DateTimeFormatter formatter = builder.toFormatter();

        formatter.parseMillis("2009-11-15 14:12:12");
    }

    @Test
    public void testIsoDateFormatDateTimeNoMillisUTC() {
        DateTimeFormatter formatter = ISODateTimeFormat.dateTimeNoMillis().withZone(DateTimeZone.UTC);
        long millis = formatter.parseMillis("1970-01-01T00:00:00Z");

        assertThat(millis, equalTo(0l));
    }

    @Test
    public void testUpperBound() {
        MutableDateTime dateTime = new MutableDateTime(3000, 12, 31, 23, 59, 59, 999, DateTimeZone.UTC);
        DateTimeFormatter formatter = ISODateTimeFormat.dateOptionalTimeParser().withZone(DateTimeZone.UTC);

        String value = "2000-01-01";
        int i = formatter.parseInto(dateTime, value, 0);
        assertThat(i, equalTo(value.length()));
        assertThat(dateTime.toString(), equalTo("2000-01-01T23:59:59.999Z"));
    }

    @Test
    public void testIsoDateFormatDateOptionalTimeUTC() {
        DateTimeFormatter formatter = ISODateTimeFormat.dateOptionalTimeParser().withZone(DateTimeZone.UTC);
        long millis = formatter.parseMillis("1970-01-01T00:00:00Z");
        assertThat(millis, equalTo(0l));
        millis = formatter.parseMillis("1970-01-01T00:00:00.001Z");
        assertThat(millis, equalTo(1l));
        millis = formatter.parseMillis("1970-01-01T00:00:00.1Z");
        assertThat(millis, equalTo(100l));
        millis = formatter.parseMillis("1970-01-01T00:00:00.1");
        assertThat(millis, equalTo(100l));
        millis = formatter.parseMillis("1970-01-01T00:00:00");
        assertThat(millis, equalTo(0l));
        millis = formatter.parseMillis("1970-01-01");
        assertThat(millis, equalTo(0l));

        millis = formatter.parseMillis("1970");
        assertThat(millis, equalTo(0l));

        try {
            formatter.parseMillis("1970 kuku");
            fail("formatting should fail");
        } catch (IllegalArgumentException e) {
            // all is well
        }

        // test offset in format
        millis = formatter.parseMillis("1970-01-01T00:00:00-02:00");
        assertThat(millis, equalTo(TimeValue.timeValueHours(2).millis()));
    }

    @Test
    public void testIsoVsCustom() {
        DateTimeFormatter formatter = ISODateTimeFormat.dateOptionalTimeParser().withZone(DateTimeZone.UTC);
        long millis = formatter.parseMillis("1970-01-01T00:00:00");
        assertThat(millis, equalTo(0l));

        formatter = DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss").withZone(DateTimeZone.UTC);
        millis = formatter.parseMillis("1970/01/01 00:00:00");
        assertThat(millis, equalTo(0l));

        FormatDateTimeFormatter formatter2 = Joda.forPattern("yyyy/MM/dd HH:mm:ss");
        millis = formatter2.parser().parseMillis("1970/01/01 00:00:00");
        assertThat(millis, equalTo(0l));
    }

    @Test
    public void testWriteAndParse() {
        DateTimeFormatter dateTimeWriter = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC);
        DateTimeFormatter formatter = ISODateTimeFormat.dateOptionalTimeParser().withZone(DateTimeZone.UTC);
        Date date = new Date();
        assertThat(formatter.parseMillis(dateTimeWriter.print(date.getTime())), equalTo(date.getTime()));
    }

    @Test
    public void testSlashInFormat() {
        FormatDateTimeFormatter formatter = Joda.forPattern("MM/yyyy");
        formatter.parser().parseMillis("01/2001");

        formatter = Joda.forPattern("yyyy/MM/dd HH:mm:ss");
        long millis = formatter.parser().parseMillis("1970/01/01 00:00:00");
        formatter.printer().print(millis);

        try {
            millis = formatter.parser().parseMillis("1970/01/01");
            fail();
        } catch (IllegalArgumentException e) {
            // it really can't parse this one
        }
    }

    @Test
    public void testMultipleFormats() {
        FormatDateTimeFormatter formatter = Joda.forPattern("yyyy/MM/dd HH:mm:ss||yyyy/MM/dd");
        long millis = formatter.parser().parseMillis("1970/01/01 00:00:00");
        assertThat("1970/01/01 00:00:00", is(formatter.printer().print(millis)));
    }

    @Test
    public void testMultipleDifferentFormats() {
        FormatDateTimeFormatter formatter = Joda.forPattern("yyyy/MM/dd HH:mm:ss||yyyy/MM/dd");
        String input = "1970/01/01 00:00:00";
        long millis = formatter.parser().parseMillis(input);
        assertThat(input, is(formatter.printer().print(millis)));

        Joda.forPattern("yyyy/MM/dd HH:mm:ss||yyyy/MM/dd||dateOptionalTime");
        Joda.forPattern("dateOptionalTime||yyyy/MM/dd HH:mm:ss||yyyy/MM/dd");
        Joda.forPattern("yyyy/MM/dd HH:mm:ss||dateOptionalTime||yyyy/MM/dd");
        Joda.forPattern("date_time||date_time_no_millis");
        Joda.forPattern(" date_time || date_time_no_millis");
    }

    @Test
    public void testInvalidPatterns() {
        expectInvalidPattern("does_not_exist_pattern", "Invalid format: [does_not_exist_pattern]: Illegal pattern component: o");
        expectInvalidPattern("OOOOO", "Invalid format: [OOOOO]: Illegal pattern component: OOOOO");
        expectInvalidPattern(null, "No date pattern provided");
        expectInvalidPattern("", "No date pattern provided");
        expectInvalidPattern(" ", "No date pattern provided");
        expectInvalidPattern("||date_time_no_millis", "No date pattern provided");
        expectInvalidPattern("date_time_no_millis||", "No date pattern provided");
    }

    private void expectInvalidPattern(String pattern, String errorMessage) {
        try {
            Joda.forPattern(pattern);
            fail("Pattern " + pattern + " should have thrown an exception but did not");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString(errorMessage));
        }
    }

    @Test
    public void testRounding() {
        long TIME = utcTimeInMillis("2009-02-03T01:01:01");
        MutableDateTime time = new MutableDateTime(DateTimeZone.UTC);
        time.setMillis(TIME);
        assertThat(time.monthOfYear().roundFloor().toString(), equalTo("2009-02-01T00:00:00.000Z"));
        time.setMillis(TIME);
        assertThat(time.hourOfDay().roundFloor().toString(), equalTo("2009-02-03T01:00:00.000Z"));
        time.setMillis(TIME);
        assertThat(time.dayOfMonth().roundFloor().toString(), equalTo("2009-02-03T00:00:00.000Z"));
    }

    @Test
    public void testRoundingSetOnTime() {
        MutableDateTime time = new MutableDateTime(DateTimeZone.UTC);
        time.setRounding(time.getChronology().monthOfYear(), MutableDateTime.ROUND_FLOOR);
        time.setMillis(utcTimeInMillis("2009-02-03T01:01:01"));
        assertThat(time.toString(), equalTo("2009-02-01T00:00:00.000Z"));
        assertThat(time.getMillis(), equalTo(utcTimeInMillis("2009-02-01T00:00:00.000Z")));

        time.setMillis(utcTimeInMillis("2009-05-03T01:01:01"));
        assertThat(time.toString(), equalTo("2009-05-01T00:00:00.000Z"));
        assertThat(time.getMillis(), equalTo(utcTimeInMillis("2009-05-01T00:00:00.000Z")));

        time = new MutableDateTime(DateTimeZone.UTC);
        time.setRounding(time.getChronology().dayOfMonth(), MutableDateTime.ROUND_FLOOR);
        time.setMillis(utcTimeInMillis("2009-02-03T01:01:01"));
        assertThat(time.toString(), equalTo("2009-02-03T00:00:00.000Z"));
        assertThat(time.getMillis(), equalTo(utcTimeInMillis("2009-02-03T00:00:00.000Z")));

        time.setMillis(utcTimeInMillis("2009-02-02T23:01:01"));
        assertThat(time.toString(), equalTo("2009-02-02T00:00:00.000Z"));
        assertThat(time.getMillis(), equalTo(utcTimeInMillis("2009-02-02T00:00:00.000Z")));

        time = new MutableDateTime(DateTimeZone.UTC);
        time.setRounding(time.getChronology().weekOfWeekyear(), MutableDateTime.ROUND_FLOOR);
        time.setMillis(utcTimeInMillis("2011-05-05T01:01:01"));
        assertThat(time.toString(), equalTo("2011-05-02T00:00:00.000Z"));
        assertThat(time.getMillis(), equalTo(utcTimeInMillis("2011-05-02T00:00:00.000Z")));
    }

    @Test
    public void testRoundingWithTimeZone() {
        MutableDateTime time = new MutableDateTime(DateTimeZone.UTC);
        time.setZone(DateTimeZone.forOffsetHours(-2));
        time.setRounding(time.getChronology().dayOfMonth(), MutableDateTime.ROUND_FLOOR);

        MutableDateTime utcTime = new MutableDateTime(DateTimeZone.UTC);
        utcTime.setRounding(utcTime.getChronology().dayOfMonth(), MutableDateTime.ROUND_FLOOR);

        time.setMillis(utcTimeInMillis("2009-02-03T01:01:01"));
        utcTime.setMillis(utcTimeInMillis("2009-02-03T01:01:01"));

        assertThat(time.toString(), equalTo("2009-02-02T00:00:00.000-02:00"));
        assertThat(utcTime.toString(), equalTo("2009-02-03T00:00:00.000Z"));
        // the time is on the 2nd, and utcTime is on the 3rd, but, because time already encapsulates
        // time zone, the millis diff is not 24, but 22 hours
        assertThat(time.getMillis(), equalTo(utcTime.getMillis() - TimeValue.timeValueHours(22).millis()));

        time.setMillis(utcTimeInMillis("2009-02-04T01:01:01"));
        utcTime.setMillis(utcTimeInMillis("2009-02-04T01:01:01"));
        assertThat(time.toString(), equalTo("2009-02-03T00:00:00.000-02:00"));
        assertThat(utcTime.toString(), equalTo("2009-02-04T00:00:00.000Z"));
        assertThat(time.getMillis(), equalTo(utcTime.getMillis() - TimeValue.timeValueHours(22).millis()));
    }

    @Test
    public void testThatEpochsCanBeParsed() {
        boolean parseMilliSeconds = randomBoolean();

        // epoch: 1433144433655 => date: Mon Jun  1 09:40:33.655 CEST 2015
        FormatDateTimeFormatter formatter = Joda.forPattern(parseMilliSeconds ? "epoch_millis" : "epoch_second");
        DateTime dateTime = formatter.parser().parseDateTime(parseMilliSeconds ? "1433144433655" : "1433144433");

        assertThat(dateTime.getYear(), is(2015));
        assertThat(dateTime.getDayOfMonth(), is(1));
        assertThat(dateTime.getMonthOfYear(), is(6));
        assertThat(dateTime.getHourOfDay(), is(7)); // utc timezone, +2 offset due to CEST
        assertThat(dateTime.getMinuteOfHour(), is(40));
        assertThat(dateTime.getSecondOfMinute(), is(33));

        if (parseMilliSeconds) {
            assertThat(dateTime.getMillisOfSecond(), is(655));
        } else {
            assertThat(dateTime.getMillisOfSecond(), is(0));
        }
    }

    @Test
    public void testThatNegativeEpochsCanBeParsed() {
        // problem: negative epochs can be arbitrary in size...
        boolean parseMilliSeconds = randomBoolean();
        FormatDateTimeFormatter formatter = Joda.forPattern(parseMilliSeconds ? "epoch_millis" : "epoch_second");
        DateTime dateTime = formatter.parser().parseDateTime("-10000");

        assertThat(dateTime.getYear(), is(1969));
        assertThat(dateTime.getMonthOfYear(), is(12));
        assertThat(dateTime.getDayOfMonth(), is(31));
        if (parseMilliSeconds) {
            assertThat(dateTime.getHourOfDay(), is(23)); // utc timezone, +2 offset due to CEST
            assertThat(dateTime.getMinuteOfHour(), is(59));
            assertThat(dateTime.getSecondOfMinute(), is(50));
        } else {
            assertThat(dateTime.getHourOfDay(), is(21)); // utc timezone, +2 offset due to CEST
            assertThat(dateTime.getMinuteOfHour(), is(13));
            assertThat(dateTime.getSecondOfMinute(), is(20));
        }

        // every negative epoch must be parsed, no matter if exact the size or bigger
        if (parseMilliSeconds) {
            formatter.parser().parseDateTime("-100000000");
            formatter.parser().parseDateTime("-999999999999");
            formatter.parser().parseDateTime("-1234567890123");
        } else {
            formatter.parser().parseDateTime("-100000000");
            formatter.parser().parseDateTime("-1234567890");
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testForInvalidDatesInEpochSecond() {
        FormatDateTimeFormatter formatter = Joda.forPattern("epoch_second");
        formatter.parser().parseDateTime(randomFrom("invalid date", "12345678901", "12345678901234"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testForInvalidDatesInEpochMillis() {
        FormatDateTimeFormatter formatter = Joda.forPattern("epoch_millis");
        formatter.parser().parseDateTime(randomFrom("invalid date", "12345678901234"));
    }

    public void testThatEpochParserIsPrinter() {
        FormatDateTimeFormatter formatter = Joda.forPattern("epoch_millis");
        assertThat(formatter.parser().isPrinter(), is(true));
        assertThat(formatter.printer().isPrinter(), is(true));

        FormatDateTimeFormatter epochSecondFormatter = Joda.forPattern("epoch_second");
        assertThat(epochSecondFormatter.parser().isPrinter(), is(true));
        assertThat(epochSecondFormatter.printer().isPrinter(), is(true));
    }

    public void testThatEpochTimePrinterWorks() {
        StringBuffer buffer = new StringBuffer();
        LocalDateTime now = LocalDateTime.now();

        Joda.EpochTimePrinter epochTimePrinter = new Joda.EpochTimePrinter(false);
        epochTimePrinter.printTo(buffer, now, Locale.ROOT);
        assertThat(buffer.length(), is(10));
        // only check the last digit, as seconds go from 0-99 in the unix timestamp and dont stop at 60
        assertThat(buffer.toString(), endsWith(String.valueOf(now.getSecondOfMinute() % 10)));

        buffer = new StringBuffer();
        Joda.EpochTimePrinter epochMilliSecondTimePrinter = new Joda.EpochTimePrinter(true);
        epochMilliSecondTimePrinter.printTo(buffer, now, Locale.ROOT);
        assertThat(buffer.length(), is(13));
        assertThat(buffer.toString(), endsWith(String.valueOf(now.getMillisOfSecond())));
    }

    public void testThatEpochParserIsIdempotent() {
        FormatDateTimeFormatter formatter = Joda.forPattern("epoch_millis");
        DateTime dateTime = formatter.parser().parseDateTime("1234567890123");
        assertThat(dateTime.getMillis(), is(1234567890123l));
        dateTime = formatter.printer().parseDateTime("1234567890456");
        assertThat(dateTime.getMillis(), is(1234567890456l));
        dateTime = formatter.parser().parseDateTime("1234567890789");
        assertThat(dateTime.getMillis(), is(1234567890789l));

        FormatDateTimeFormatter secondsFormatter = Joda.forPattern("epoch_second");
        DateTime secondsDateTime = secondsFormatter.parser().parseDateTime("1234567890");
        assertThat(secondsDateTime.getMillis(), is(1234567890000l));
        secondsDateTime = secondsFormatter.printer().parseDateTime("1234567890");
        assertThat(secondsDateTime.getMillis(), is(1234567890000l));
        secondsDateTime = secondsFormatter.parser().parseDateTime("1234567890");
        assertThat(secondsDateTime.getMillis(), is(1234567890000l));
    }

    private long utcTimeInMillis(String time) {
        return ISODateTimeFormat.dateOptionalTimeParser().withZone(DateTimeZone.UTC).parseMillis(time);
    }

}
