/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.utils.time;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.utils.time.DateTimeFormatterTimestampConverter;
import org.elasticsearch.xpack.core.ml.utils.time.TimestampConverter;

import java.text.ParseException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;


public class DateTimeFormatterTimestampConverterTests extends ESTestCase {
    public void testOfPattern_GivenPatternIsOnlyYear() {

        ESTestCase.expectThrows(IllegalArgumentException.class, () -> DateTimeFormatterTimestampConverter.ofPattern("y", ZoneOffset.UTC));
    }

    public void testOfPattern_GivenPatternIsOnlyDate() {

        ESTestCase.expectThrows(IllegalArgumentException.class,
                () -> DateTimeFormatterTimestampConverter.ofPattern("y-M-d", ZoneOffset.UTC));
    }

    public void testOfPattern_GivenPatternIsOnlyTime() {

        ESTestCase.expectThrows(IllegalArgumentException.class,
                () -> DateTimeFormatterTimestampConverter.ofPattern("HH:mm:ss", ZoneOffset.UTC));
    }

    public void testOfPattern_GivenPatternIsUsingYearInsteadOfYearOfEra() {
        ESTestCase.expectThrows(IllegalArgumentException.class,
                () -> DateTimeFormatterTimestampConverter.ofPattern("uuuu-MM-dd HH:mm:ss", ZoneOffset.UTC));
    }

    public void testToEpochSeconds_GivenValidTimestampDoesNotFollowPattern() {
        TimestampConverter formatter = DateTimeFormatterTimestampConverter.ofPattern("yyyy-MM-dd HH:mm:ss", ZoneOffset.UTC);
        ESTestCase.expectThrows(DateTimeParseException.class, () -> formatter.toEpochSeconds("14:00:22"));
    }

    public void testToEpochMillis_GivenValidTimestampDoesNotFollowPattern() {
        TimestampConverter formatter = DateTimeFormatterTimestampConverter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS", ZoneOffset.UTC);
        ESTestCase.expectThrows(DateTimeParseException.class, () -> formatter.toEpochMillis("2015-01-01 14:00:22"));
    }

    public void testToEpochSeconds_GivenPatternHasFullDateAndOnlyHours() {
        long expected = ZonedDateTime.of(2014, 3, 22, 1, 0, 0, 0, ZoneOffset.UTC).toEpochSecond();
        assertEquals(expected, toEpochSeconds("2014-03-22 01", "y-M-d HH"));
    }

    public void testToEpochSeconds_GivenPatternHasFullDateAndTimeWithoutTimeZone() {
        long expected = ZonedDateTime.of(1985, 8, 18, 20, 15, 40, 0, ZoneOffset.UTC).toEpochSecond();
        assertEquals(expected, toEpochSeconds("1985-08-18 20:15:40", "yyyy-MM-dd HH:mm:ss"));

        expected = ZonedDateTime.of(1985, 8, 18, 20, 15, 40, 0, ZoneOffset.MIN).toEpochSecond();
        assertEquals(expected, toEpochSeconds("1985-08-18 20:15:40", "yyyy-MM-dd HH:mm:ss", ZoneOffset.MIN));

        expected = ZonedDateTime.of(1985, 8, 18, 20, 15, 40, 0, ZoneOffset.MAX).toEpochSecond();
        assertEquals(expected, toEpochSeconds("1985-08-18 20:15:40", "yyyy-MM-dd HH:mm:ss", ZoneOffset.MAX));
    }

    public void testToEpochSeconds_GivenPatternHasFullDateAndTimeWithTimeZone() {
        assertEquals(1395703820, toEpochSeconds("2014-03-25 01:30:20 +02:00", "yyyy-MM-dd HH:mm:ss XXX"));
    }

    public void testToEpochSeconds_GivenTimestampRequiresLenientParsing() {
        assertEquals(1395703820, toEpochSeconds("2014-03-25 1:30:20 +02:00", "yyyy-MM-dd HH:mm:ss XXX"));
    }

    public void testToEpochSeconds_GivenPatternHasDateWithoutYearAndTimeWithoutTimeZone() throws ParseException {
        // Summertime
        long expected = ZonedDateTime.of(LocalDate.now(ZoneOffset.UTC).getYear(), 8, 14, 1, 30, 20, 0, ZoneOffset.UTC).toEpochSecond();
        assertEquals(expected, toEpochSeconds("08 14 01:30:20", "MM dd HH:mm:ss"));

        // Non-summertime
        expected = ZonedDateTime.of(LocalDate.now(ZoneOffset.UTC).getYear(), 12, 14, 1, 30, 20, 0, ZoneOffset.UTC).toEpochSecond();
        assertEquals(expected, toEpochSeconds("12 14 01:30:20", "MM dd HH:mm:ss"));
    }

    public void testToEpochMillis_GivenPatternHasFullDateAndTimeWithTimeZone() {
        assertEquals(1395703820542L,
                toEpochMillis("2014-03-25 01:30:20.542 +02:00", "yyyy-MM-dd HH:mm:ss.SSS XXX"));
    }

    private static long toEpochSeconds(String timestamp, String pattern) {
        TimestampConverter formatter = DateTimeFormatterTimestampConverter.ofPattern(pattern, ZoneOffset.UTC);
        return formatter.toEpochSeconds(timestamp);
    }

    private static long toEpochSeconds(String timestamp, String pattern, ZoneId defaultTimezone) {
        TimestampConverter formatter = DateTimeFormatterTimestampConverter.ofPattern(pattern, defaultTimezone);
        return formatter.toEpochSeconds(timestamp);
    }

    private static long toEpochMillis(String timestamp, String pattern) {
        TimestampConverter formatter = DateTimeFormatterTimestampConverter.ofPattern(pattern, ZoneOffset.UTC);
        return formatter.toEpochMillis(timestamp);
    }
}
