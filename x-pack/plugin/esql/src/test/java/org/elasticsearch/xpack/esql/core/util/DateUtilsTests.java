/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.util;

import org.elasticsearch.test.ESTestCase;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class DateUtilsTests extends ESTestCase {

    public void testRealCalendarDaysRead() {
        Map<String, String> expected = Map.ofEntries(
            Map.entry("2024-02-29", "2024-02-29T00:00:00Z"),
            Map.entry("2024-01-31", "2024-01-31T00:00:00Z"),
            Map.entry("2024-03-31", "2024-03-31T00:00:00Z"),
            Map.entry("2024-04-30", "2024-04-30T00:00:00Z"),
            Map.entry("2024-05-31", "2024-05-31T00:00:00Z"),
            Map.entry("2024-12-31", "2024-12-31T00:00:00Z"),
            Map.entry("2024-02-29T10:00:00Z", "2024-02-29T10:00:00Z"),
            Map.entry("2024-02-29 10:00:00", "2024-02-29T10:00:00Z"),
            Map.entry("2024-01-31T23:59:59.999Z", "2024-01-31T23:59:59.999Z")
        );
        for (Map.Entry<String, String> e : expected.entrySet()) {
            assertEquals(e.getKey(), Instant.parse(e.getValue()), DateUtils.asDateTime(e.getKey()).toInstant());
        }
    }

    public void testImpossibleCalendarDaysAreRefused() {
        for (String value : new String[] {
            "2024-02-30",
            "2023-02-29",
            "2024-04-31",
            "2024-06-31",
            "2024-09-31",
            "2024-11-31",
            "2024-01-32",
            "2024-02-30T10:00:00Z",
            "2024-02-30T10:00:00",
            "2024-02-30 10:00:00",
            "2023-02-29 10:00:00" }) {
            expectThrows(DateTimeParseException.class, value, () -> DateUtils.asDateTime(value));
        }
    }

    public void testEndOfDayHourReadsAsNextDayMidnight() {
        Map<String, String> expected = Map.ofEntries(
            Map.entry("2024-06-01T24:00:00Z", "2024-06-02T00:00:00Z"),
            Map.entry("2024-06-01T24:00:00", "2024-06-02T00:00:00Z"),
            Map.entry("2024-06-01T24:00", "2024-06-02T00:00:00Z"),
            Map.entry("2024-06-01T24:00:00.000Z", "2024-06-02T00:00:00Z"),
            Map.entry("2024-06-01T24:00:00.000000000Z", "2024-06-02T00:00:00Z"),
            Map.entry("2024-06-01 24:00:00", "2024-06-02T00:00:00Z"),
            Map.entry("2024-06-01 24:00", "2024-06-02T00:00:00Z"),
            Map.entry("2024-12-31T24:00:00Z", "2025-01-01T00:00:00Z"),
            Map.entry("2023-02-28T24:00:00Z", "2023-03-01T00:00:00Z"),
            Map.entry("2024-02-28T24:00:00Z", "2024-02-29T00:00:00Z"),
            Map.entry("2024-02-29T24:00:00Z", "2024-03-01T00:00:00Z"),
            Map.entry("2024-06-01T24:00:00+02:00", "2024-06-01T22:00:00Z"),
            Map.entry("2024-06-01T24:00:00-05:00", "2024-06-02T05:00:00Z"),
            Map.entry("2024-06-01T24:00:00.0Z", "2024-06-02T00:00:00Z"),
            Map.entry("2024-06-01T24:00Z", "2024-06-02T00:00:00Z")
        );
        for (Map.Entry<String, String> e : expected.entrySet()) {
            assertEquals(e.getKey(), Instant.parse(e.getValue()), DateUtils.asDateTime(e.getKey()).toInstant());
        }
    }

    public void testEndOfDayHourOnlyReadsAsMidnight() {
        for (String value : new String[] {
            "2024-06-01T24:30:00Z",
            "2024-06-01T24:00:01Z",
            "2024-06-01T24:00:00.001Z",
            "2024-06-01 24:30:00",
            "2024-06-01T24",
            "2024-06-01T24:00:00,000Z",
            "2024-06-01T25:00:00Z",
            "2024-06-01T23:59:60Z" }) {
            expectThrows(DateTimeParseException.class, value, () -> DateUtils.asDateTime(value));
        }
    }

    public void testEndOfDayHourOnLastSupportedDayIsRefusedNotAnOverflow() {
        String value = "+999999999-12-31T24:00:00Z";
        DateTimeParseException e = expectThrows(DateTimeParseException.class, value, () -> DateUtils.asDateTime(value));
        assertThat(e.getMessage(), containsString(value));
    }

    public void testImpossibleDayInLargeYearIsRefused() {
        String value = "+12345-02-30";
        expectThrows(DateTimeParseException.class, value, () -> DateUtils.asDateTime(value));
    }

    /** The day is validated before the end-of-day hour adds one: Feb 30 must not become March 1. */
    public void testEndOfDayHourOnImpossibleDayIsRefused() {
        for (String value : new String[] {
            "2024-02-30T24:00:00Z",
            "2023-02-29T24:00:00Z",
            "2024-04-31T24:00:00Z",
            "2024-02-30 24:00:00" }) {
            expectThrows(DateTimeParseException.class, value, () -> DateUtils.asDateTime(value));
        }
    }
}
