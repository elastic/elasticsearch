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
import org.elasticsearch.index.mapper.object.RootObjectMapper;
import org.elasticsearch.test.ESTestCase;
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
public class SimpleJodaTests extends ESTestCase {

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

    public void testThatDefaultFormatterChecksForCorrectYearLength() throws Exception {
        // if no strict version is tested, this means the date format is already strict by itself
        // yyyyMMdd
        assertValidDateFormatParsing("basicDate", "20140303");
        assertDateFormatParsingThrowingException("basicDate", "2010303");

        // yyyyMMdd’T'HHmmss.SSSZ
        assertValidDateFormatParsing("basicDateTime", "20140303T124343.123Z");
        assertValidDateFormatParsing("basicDateTime", "00050303T124343.123Z");
        assertDateFormatParsingThrowingException("basicDateTime", "50303T124343.123Z");

        // yyyyMMdd’T'HHmmssZ
        assertValidDateFormatParsing("basicDateTimeNoMillis", "20140303T124343Z");
        assertValidDateFormatParsing("basicDateTimeNoMillis", "00050303T124343Z");
        assertDateFormatParsingThrowingException("basicDateTimeNoMillis", "50303T124343Z");

        // yyyyDDD
        assertValidDateFormatParsing("basicOrdinalDate", "0005165");
        assertDateFormatParsingThrowingException("basicOrdinalDate", "5165");

        // yyyyDDD’T'HHmmss.SSSZ
        assertValidDateFormatParsing("basicOrdinalDateTime", "0005165T124343.123Z");
        assertValidDateFormatParsing("basicOrdinalDateTime", "0005165T124343.123Z");
        assertDateFormatParsingThrowingException("basicOrdinalDateTime", "5165T124343.123Z");

        // yyyyDDD’T'HHmmssZ
        assertValidDateFormatParsing("basicOrdinalDateTimeNoMillis", "0005165T124343Z");
        assertValidDateFormatParsing("basicOrdinalDateTimeNoMillis", "0005165T124343Z");
        assertDateFormatParsingThrowingException("basicOrdinalDateTimeNoMillis", "5165T124343Z");

        // HHmmss.SSSZ
        assertValidDateFormatParsing("basicTime", "090909.123Z");
        assertDateFormatParsingThrowingException("basicTime", "90909.123Z");

        // HHmmssZ
        assertValidDateFormatParsing("basicTimeNoMillis", "090909Z");
        assertDateFormatParsingThrowingException("basicTimeNoMillis", "90909Z");

        // 'T’HHmmss.SSSZ
        assertValidDateFormatParsing("basicTTime", "T090909.123Z");
        assertDateFormatParsingThrowingException("basicTTime", "T90909.123Z");

        // T’HHmmssZ
        assertValidDateFormatParsing("basicTTimeNoMillis", "T090909Z");
        assertDateFormatParsingThrowingException("basicTTimeNoMillis", "T90909Z");

        // xxxx’W'wwe
        assertValidDateFormatParsing("basicWeekDate", "0005W414");
        assertValidDateFormatParsing("basicWeekDate", "5W414", "0005W414");
        assertDateFormatParsingThrowingException("basicWeekDate", "5W14");

        assertValidDateFormatParsing("strictBasicWeekDate", "0005W414");
        assertDateFormatParsingThrowingException("strictBasicWeekDate", "0005W47");
        assertDateFormatParsingThrowingException("strictBasicWeekDate", "5W414");
        assertDateFormatParsingThrowingException("strictBasicWeekDate", "5W14");

        // xxxx’W'wwe’T'HHmmss.SSSZ
        assertValidDateFormatParsing("basicWeekDateTime", "0005W414T124343.123Z");
        assertValidDateFormatParsing("basicWeekDateTime", "5W414T124343.123Z", "0005W414T124343.123Z");
        assertDateFormatParsingThrowingException("basicWeekDateTime", "5W14T124343.123Z");

        assertValidDateFormatParsing("strictBasicWeekDateTime", "0005W414T124343.123Z");
        assertDateFormatParsingThrowingException("strictBasicWeekDateTime", "0005W47T124343.123Z");
        assertDateFormatParsingThrowingException("strictBasicWeekDateTime", "5W414T124343.123Z");
        assertDateFormatParsingThrowingException("strictBasicWeekDateTime", "5W14T124343.123Z");

        // xxxx’W'wwe’T'HHmmssZ
        assertValidDateFormatParsing("basicWeekDateTimeNoMillis", "0005W414T124343Z");
        assertValidDateFormatParsing("basicWeekDateTimeNoMillis", "5W414T124343Z", "0005W414T124343Z");
        assertDateFormatParsingThrowingException("basicWeekDateTimeNoMillis", "5W14T124343Z");

        assertValidDateFormatParsing("strictBasicWeekDateTimeNoMillis", "0005W414T124343Z");
        assertDateFormatParsingThrowingException("strictBasicWeekDateTimeNoMillis", "0005W47T124343Z");
        assertDateFormatParsingThrowingException("strictBasicWeekDateTimeNoMillis", "5W414T124343Z");
        assertDateFormatParsingThrowingException("strictBasicWeekDateTimeNoMillis", "5W14T124343Z");

        // yyyy-MM-dd
        assertValidDateFormatParsing("date", "0005-06-03");
        assertValidDateFormatParsing("date", "5-6-3", "0005-06-03");

        assertValidDateFormatParsing("strictDate", "0005-06-03");
        assertDateFormatParsingThrowingException("strictDate", "5-6-3");
        assertDateFormatParsingThrowingException("strictDate", "0005-06-3");
        assertDateFormatParsingThrowingException("strictDate", "0005-6-03");
        assertDateFormatParsingThrowingException("strictDate", "5-06-03");

        // yyyy-MM-dd'T'HH
        assertValidDateFormatParsing("dateHour", "0005-06-03T12");
        assertValidDateFormatParsing("dateHour", "5-6-3T1", "0005-06-03T01");

        assertValidDateFormatParsing("strictDateHour", "0005-06-03T12");
        assertDateFormatParsingThrowingException("strictDateHour", "5-6-3T1");

        // yyyy-MM-dd'T'HH:mm
        assertValidDateFormatParsing("dateHourMinute", "0005-06-03T12:12");
        assertValidDateFormatParsing("dateHourMinute", "5-6-3T12:1", "0005-06-03T12:01");

        assertValidDateFormatParsing("strictDateHourMinute", "0005-06-03T12:12");
        assertDateFormatParsingThrowingException("strictDateHourMinute", "5-6-3T12:1");

        // yyyy-MM-dd'T'HH:mm:ss
        assertValidDateFormatParsing("dateHourMinuteSecond", "0005-06-03T12:12:12");
        assertValidDateFormatParsing("dateHourMinuteSecond", "5-6-3T12:12:1", "0005-06-03T12:12:01");

        assertValidDateFormatParsing("strictDateHourMinuteSecond", "0005-06-03T12:12:12");
        assertDateFormatParsingThrowingException("strictDateHourMinuteSecond", "5-6-3T12:12:1");

        // yyyy-MM-dd’T'HH:mm:ss.SSS
        assertValidDateFormatParsing("dateHourMinuteSecondFraction", "0005-06-03T12:12:12.123");
        assertValidDateFormatParsing("dateHourMinuteSecondFraction", "5-6-3T12:12:1.123", "0005-06-03T12:12:01.123");
        assertValidDateFormatParsing("dateHourMinuteSecondFraction", "5-6-3T12:12:1.1", "0005-06-03T12:12:01.100");

        assertValidDateFormatParsing("strictDateHourMinuteSecondFraction", "0005-06-03T12:12:12.123");
        assertDateFormatParsingThrowingException("strictDateHourMinuteSecondFraction", "5-6-3T12:12:12.1");
        assertDateFormatParsingThrowingException("strictDateHourMinuteSecondFraction", "5-6-3T12:12:12.12");

        assertValidDateFormatParsing("dateHourMinuteSecondMillis", "0005-06-03T12:12:12.123");
        assertValidDateFormatParsing("dateHourMinuteSecondMillis", "5-6-3T12:12:1.123", "0005-06-03T12:12:01.123");
        assertValidDateFormatParsing("dateHourMinuteSecondMillis", "5-6-3T12:12:1.1", "0005-06-03T12:12:01.100");

        assertValidDateFormatParsing("strictDateHourMinuteSecondMillis", "0005-06-03T12:12:12.123");
        assertDateFormatParsingThrowingException("strictDateHourMinuteSecondMillis", "5-6-3T12:12:12.1");
        assertDateFormatParsingThrowingException("strictDateHourMinuteSecondMillis", "5-6-3T12:12:12.12");

        // yyyy-MM-dd'T'HH:mm:ss.SSSZ
        assertValidDateFormatParsing("dateOptionalTime", "2014-03-03", "2014-03-03T00:00:00.000Z");
        assertValidDateFormatParsing("dateOptionalTime", "1257-3-03", "1257-03-03T00:00:00.000Z");
        assertValidDateFormatParsing("dateOptionalTime", "0005-03-3", "0005-03-03T00:00:00.000Z");
        assertValidDateFormatParsing("dateOptionalTime", "5-03-03", "0005-03-03T00:00:00.000Z");
        assertValidDateFormatParsing("dateOptionalTime", "5-03-03T1:1:1.1", "0005-03-03T01:01:01.100Z");
        assertValidDateFormatParsing("strictDateOptionalTime", "2014-03-03", "2014-03-03T00:00:00.000Z");
        assertDateFormatParsingThrowingException("strictDateOptionalTime", "5-03-03");
        assertDateFormatParsingThrowingException("strictDateOptionalTime", "0005-3-03");
        assertDateFormatParsingThrowingException("strictDateOptionalTime", "0005-03-3");
        assertDateFormatParsingThrowingException("strictDateOptionalTime", "5-03-03T1:1:1.1");
        assertDateFormatParsingThrowingException("strictDateOptionalTime", "5-03-03T01:01:01.1");
        assertDateFormatParsingThrowingException("strictDateOptionalTime", "5-03-03T01:01:1.100");
        assertDateFormatParsingThrowingException("strictDateOptionalTime", "5-03-03T01:1:01.100");
        assertDateFormatParsingThrowingException("strictDateOptionalTime", "5-03-03T1:01:01.100");

        // yyyy-MM-dd’T'HH:mm:ss.SSSZZ
        assertValidDateFormatParsing("dateTime", "5-03-03T1:1:1.1Z", "0005-03-03T01:01:01.100Z");
        assertValidDateFormatParsing("strictDateTime", "2014-03-03T11:11:11.100Z", "2014-03-03T11:11:11.100Z");
        assertDateFormatParsingThrowingException("strictDateTime", "0005-03-03T1:1:1.1Z");
        assertDateFormatParsingThrowingException("strictDateTime", "0005-03-03T01:01:1.100Z");
        assertDateFormatParsingThrowingException("strictDateTime", "0005-03-03T01:1:01.100Z");
        assertDateFormatParsingThrowingException("strictDateTime", "0005-03-03T1:01:01.100Z");

        // yyyy-MM-dd’T'HH:mm:ssZZ
        assertValidDateFormatParsing("dateTimeNoMillis", "5-03-03T1:1:1Z", "0005-03-03T01:01:01Z");
        assertValidDateFormatParsing("strictDateTimeNoMillis", "2014-03-03T11:11:11Z", "2014-03-03T11:11:11Z");
        assertDateFormatParsingThrowingException("strictDateTimeNoMillis", "0005-03-03T1:1:1Z");
        assertDateFormatParsingThrowingException("strictDateTimeNoMillis", "0005-03-03T01:01:1Z");
        assertDateFormatParsingThrowingException("strictDateTimeNoMillis", "0005-03-03T01:1:01Z");
        assertDateFormatParsingThrowingException("strictDateTimeNoMillis", "0005-03-03T1:01:01Z");

        // HH
        assertValidDateFormatParsing("hour", "12");
        assertValidDateFormatParsing("hour", "1", "01");
        assertValidDateFormatParsing("strictHour", "12");
        assertValidDateFormatParsing("strictHour", "01");
        assertDateFormatParsingThrowingException("strictHour", "1");

        // HH:mm
        assertValidDateFormatParsing("hourMinute", "12:12");
        assertValidDateFormatParsing("hourMinute", "12:1", "12:01");
        assertValidDateFormatParsing("strictHourMinute", "12:12");
        assertValidDateFormatParsing("strictHourMinute", "12:01");
        assertDateFormatParsingThrowingException("strictHourMinute", "12:1");

        // HH:mm:ss
        assertValidDateFormatParsing("hourMinuteSecond", "12:12:12");
        assertValidDateFormatParsing("hourMinuteSecond", "12:12:1", "12:12:01");
        assertValidDateFormatParsing("strictHourMinuteSecond", "12:12:12");
        assertValidDateFormatParsing("strictHourMinuteSecond", "12:12:01");
        assertDateFormatParsingThrowingException("strictHourMinuteSecond", "12:12:1");

        // HH:mm:ss.SSS
        assertValidDateFormatParsing("hourMinuteSecondFraction", "12:12:12.123");
        assertValidDateFormatParsing("hourMinuteSecondFraction", "12:12:12.1", "12:12:12.100");
        assertValidDateFormatParsing("strictHourMinuteSecondFraction", "12:12:12.123");
        assertValidDateFormatParsing("strictHourMinuteSecondFraction", "12:12:12.1", "12:12:12.100");

        assertValidDateFormatParsing("hourMinuteSecondMillis", "12:12:12.123");
        assertValidDateFormatParsing("hourMinuteSecondMillis", "12:12:12.1", "12:12:12.100");
        assertValidDateFormatParsing("strictHourMinuteSecondMillis", "12:12:12.123");
        assertValidDateFormatParsing("strictHourMinuteSecondMillis", "12:12:12.1", "12:12:12.100");

        // yyyy-DDD
        assertValidDateFormatParsing("ordinalDate", "5-3", "0005-003");
        assertValidDateFormatParsing("strictOrdinalDate", "0005-003");
        assertDateFormatParsingThrowingException("strictOrdinalDate", "5-3");
        assertDateFormatParsingThrowingException("strictOrdinalDate", "0005-3");
        assertDateFormatParsingThrowingException("strictOrdinalDate", "5-003");

        // yyyy-DDD’T'HH:mm:ss.SSSZZ
        assertValidDateFormatParsing("ordinalDateTime", "5-3T12:12:12.100Z", "0005-003T12:12:12.100Z");
        assertValidDateFormatParsing("strictOrdinalDateTime", "0005-003T12:12:12.100Z");
        assertDateFormatParsingThrowingException("strictOrdinalDateTime", "5-3T1:12:12.123Z");
        assertDateFormatParsingThrowingException("strictOrdinalDateTime", "5-3T12:1:12.123Z");
        assertDateFormatParsingThrowingException("strictOrdinalDateTime", "5-3T12:12:1.123Z");

        // yyyy-DDD’T'HH:mm:ssZZ
        assertValidDateFormatParsing("ordinalDateTimeNoMillis", "5-3T12:12:12Z", "0005-003T12:12:12Z");
        assertValidDateFormatParsing("strictOrdinalDateTimeNoMillis", "0005-003T12:12:12Z");
        assertDateFormatParsingThrowingException("strictOrdinalDateTimeNoMillis", "5-3T1:12:12Z");
        assertDateFormatParsingThrowingException("strictOrdinalDateTimeNoMillis", "5-3T12:1:12Z");
        assertDateFormatParsingThrowingException("strictOrdinalDateTimeNoMillis", "5-3T12:12:1Z");


        // HH:mm:ss.SSSZZ
        assertValidDateFormatParsing("time", "12:12:12.100Z");
        assertValidDateFormatParsing("time", "01:01:01.1Z", "01:01:01.100Z");
        assertValidDateFormatParsing("time", "1:1:1.1Z", "01:01:01.100Z");
        assertValidDateFormatParsing("strictTime", "12:12:12.100Z");
        assertDateFormatParsingThrowingException("strictTime", "12:12:1.100Z");
        assertDateFormatParsingThrowingException("strictTime", "12:1:12.100Z");
        assertDateFormatParsingThrowingException("strictTime", "1:12:12.100Z");

        // HH:mm:ssZZ
        assertValidDateFormatParsing("timeNoMillis", "12:12:12Z");
        assertValidDateFormatParsing("timeNoMillis", "01:01:01Z", "01:01:01Z");
        assertValidDateFormatParsing("timeNoMillis", "1:1:1Z", "01:01:01Z");
        assertValidDateFormatParsing("strictTimeNoMillis", "12:12:12Z");
        assertDateFormatParsingThrowingException("strictTimeNoMillis", "12:12:1Z");
        assertDateFormatParsingThrowingException("strictTimeNoMillis", "12:1:12Z");
        assertDateFormatParsingThrowingException("strictTimeNoMillis", "1:12:12Z");

        // 'T’HH:mm:ss.SSSZZ
        assertValidDateFormatParsing("tTime", "T12:12:12.100Z");
        assertValidDateFormatParsing("tTime", "T01:01:01.1Z", "T01:01:01.100Z");
        assertValidDateFormatParsing("tTime", "T1:1:1.1Z", "T01:01:01.100Z");
        assertValidDateFormatParsing("strictTTime", "T12:12:12.100Z");
        assertDateFormatParsingThrowingException("strictTTime", "T12:12:1.100Z");
        assertDateFormatParsingThrowingException("strictTTime", "T12:1:12.100Z");
        assertDateFormatParsingThrowingException("strictTTime", "T1:12:12.100Z");

        // 'T’HH:mm:ssZZ
        assertValidDateFormatParsing("tTimeNoMillis", "T12:12:12Z");
        assertValidDateFormatParsing("tTimeNoMillis", "T01:01:01Z", "T01:01:01Z");
        assertValidDateFormatParsing("tTimeNoMillis", "T1:1:1Z", "T01:01:01Z");
        assertValidDateFormatParsing("strictTTimeNoMillis", "T12:12:12Z");
        assertDateFormatParsingThrowingException("strictTTimeNoMillis", "T12:12:1Z");
        assertDateFormatParsingThrowingException("strictTTimeNoMillis", "T12:1:12Z");
        assertDateFormatParsingThrowingException("strictTTimeNoMillis", "T1:12:12Z");

        // xxxx-'W’ww-e
        assertValidDateFormatParsing("weekDate", "0005-W4-1", "0005-W04-1");
        assertValidDateFormatParsing("strictWeekDate", "0005-W04-1");
        assertDateFormatParsingThrowingException("strictWeekDate", "0005-W4-1");

        // xxxx-'W’ww-e’T'HH:mm:ss.SSSZZ
        assertValidDateFormatParsing("weekDateTime", "0005-W41-4T12:43:43.123Z");
        assertValidDateFormatParsing("weekDateTime", "5-W41-4T12:43:43.123Z", "0005-W41-4T12:43:43.123Z");
        assertValidDateFormatParsing("strictWeekDateTime", "0005-W41-4T12:43:43.123Z");
        assertValidDateFormatParsing("strictWeekDateTime", "0005-W06-4T12:43:43.123Z");
        assertDateFormatParsingThrowingException("strictWeekDateTime", "0005-W4-7T12:43:43.123Z");
        assertDateFormatParsingThrowingException("strictWeekDateTime", "5-W41-4T12:43:43.123Z");
        assertDateFormatParsingThrowingException("strictWeekDateTime", "5-W1-4T12:43:43.123Z");

        // xxxx-'W’ww-e’T'HH:mm:ssZZ
        assertValidDateFormatParsing("weekDateTimeNoMillis", "0005-W41-4T12:43:43Z");
        assertValidDateFormatParsing("weekDateTimeNoMillis", "5-W41-4T12:43:43Z", "0005-W41-4T12:43:43Z");
        assertValidDateFormatParsing("strictWeekDateTimeNoMillis", "0005-W41-4T12:43:43Z");
        assertValidDateFormatParsing("strictWeekDateTimeNoMillis", "0005-W06-4T12:43:43Z");
        assertDateFormatParsingThrowingException("strictWeekDateTimeNoMillis", "0005-W4-7T12:43:43Z");
        assertDateFormatParsingThrowingException("strictWeekDateTimeNoMillis", "5-W41-4T12:43:43Z");
        assertDateFormatParsingThrowingException("strictWeekDateTimeNoMillis", "5-W1-4T12:43:43Z");

        // yyyy
        assertValidDateFormatParsing("weekyear", "2014");
        assertValidDateFormatParsing("weekyear", "5", "0005");
        assertValidDateFormatParsing("weekyear", "0005");
        assertValidDateFormatParsing("strictWeekyear", "2014");
        assertValidDateFormatParsing("strictWeekyear", "0005");
        assertDateFormatParsingThrowingException("strictWeekyear", "5");

        // yyyy-'W'ee
        assertValidDateFormatParsing("weekyearWeek", "2014-W41");
        assertValidDateFormatParsing("weekyearWeek", "2014-W1", "2014-W01");
        assertValidDateFormatParsing("strictWeekyearWeek", "2014-W41");
        assertDateFormatParsingThrowingException("strictWeekyearWeek", "2014-W1");

        // weekyearWeekDay
        assertValidDateFormatParsing("weekyearWeekDay", "2014-W41-1");
        assertValidDateFormatParsing("weekyearWeekDay", "2014-W1-1", "2014-W01-1");
        assertValidDateFormatParsing("strictWeekyearWeekDay", "2014-W41-1");
        assertDateFormatParsingThrowingException("strictWeekyearWeekDay", "2014-W1-1");

        // yyyy
        assertValidDateFormatParsing("year", "2014");
        assertValidDateFormatParsing("year", "5", "0005");
        assertValidDateFormatParsing("strictYear", "2014");
        assertDateFormatParsingThrowingException("strictYear", "5");

        // yyyy-mm
        assertValidDateFormatParsing("yearMonth", "2014-12");
        assertValidDateFormatParsing("yearMonth", "2014-5", "2014-05");
        assertValidDateFormatParsing("strictYearMonth", "2014-12");
        assertDateFormatParsingThrowingException("strictYearMonth", "2014-5");

        // yyyy-mm-dd
        assertValidDateFormatParsing("yearMonthDay", "2014-12-12");
        assertValidDateFormatParsing("yearMonthDay", "2014-05-5", "2014-05-05");
        assertValidDateFormatParsing("strictYearMonthDay", "2014-12-12");
        assertDateFormatParsingThrowingException("strictYearMonthDay", "2014-05-5");
    }

    @Test
    public void testThatRootObjectParsingIsStrict() throws Exception {
        String[] datesThatWork = new String[] { "2014/10/10", "2014/10/10 12:12:12", "2014-05-05",  "2014-05-05T12:12:12.123Z" };
        String[] datesThatShouldNotWork = new String[]{ "5-05-05", "2014-5-05", "2014-05-5",
                "2014-05-05T1:12:12.123Z", "2014-05-05T12:1:12.123Z", "2014-05-05T12:12:1.123Z",
                "4/10/10", "2014/1/10", "2014/10/1",
                "2014/10/10 1:12:12", "2014/10/10 12:1:12", "2014/10/10 12:12:1"
        };

        // good case
        for (String date : datesThatWork) {
            boolean dateParsingSuccessful = false;
            for (FormatDateTimeFormatter dateTimeFormatter : RootObjectMapper.Defaults.DYNAMIC_DATE_TIME_FORMATTERS) {
                try {
                    dateTimeFormatter.parser().parseMillis(date);
                    dateParsingSuccessful = true;
                    break;
                } catch (Exception e) {}
            }
            if (!dateParsingSuccessful) {
                fail("Parsing for date " + date + " in root object mapper failed, but shouldnt");
            }
        }

        // bad case
        for (String date : datesThatShouldNotWork) {
            for (FormatDateTimeFormatter dateTimeFormatter : RootObjectMapper.Defaults.DYNAMIC_DATE_TIME_FORMATTERS) {
                try {
                    dateTimeFormatter.parser().parseMillis(date);
                    fail(String.format(Locale.ROOT, "Expected exception when parsing date %s in root mapper", date));
                } catch (Exception e) {}
            }
        }
    }

    private void assertValidDateFormatParsing(String pattern, String dateToParse) {
        assertValidDateFormatParsing(pattern, dateToParse, dateToParse);
    }

    private void assertValidDateFormatParsing(String pattern, String dateToParse, String expectedDate) {
        FormatDateTimeFormatter formatter = Joda.forPattern(pattern);
        assertThat(formatter.printer().print(formatter.parser().parseMillis(dateToParse)), is(expectedDate));
    }

    private void assertDateFormatParsingThrowingException(String pattern, String invalidDate) {
        try {
            FormatDateTimeFormatter formatter = Joda.forPattern(pattern);
            DateTimeFormatter parser = formatter.parser();
            parser.parseMillis(invalidDate);
            fail(String.format(Locale.ROOT, "Expected parsing exception for pattern [%s] with date [%s], but did not happen", pattern, invalidDate));
        } catch (IllegalArgumentException e) {
        }
    }

    private long utcTimeInMillis(String time) {
        return ISODateTimeFormat.dateOptionalTimeParser().withZone(DateTimeZone.UTC).parseMillis(time);
    }

}
