/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.date;

import org.elasticsearch.test.ESTestCase;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class DateUtilsTests extends ESTestCase {

    public void testConvertMillisToDateTime() {
        long millis = randomLong();
        String expected = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.of("UTC")).format(Instant.ofEpochMilli(millis));
        String actual = DateUtils.convertMillisToDateTime(millis);
        assertEquals(expected, actual);
    }

    public void testConvertMillisToDateTimeWithEpochZero() {
        long millis = 0L;
        String expected = "1970-01-01T00:00:00";
        String actual = DateUtils.convertMillisToDateTime(millis);
        assertEquals(expected, actual);
    }

    public void testConvertMillisToDateTimeWithEpochNegativeOne() {
        long millis = -1L; // Just before epoch
        String expected = "1969-12-31T23:59:59.999";
        String actual = DateUtils.convertMillisToDateTime(millis);
        assertEquals(expected, actual);
    }

    public void testConvertMillisToDateTimeWithCurrentTime() {
        long millis = System.currentTimeMillis();
        String expected = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.of("UTC")).format(Instant.ofEpochMilli(millis));
        String actual = DateUtils.convertMillisToDateTime(millis);
        assertEquals(expected, actual);
    }

    public void testConvertMillisToDateTimeConsistency() {
        // The method should be consistent for the same input
        long millis = randomLong();
        String first = DateUtils.convertMillisToDateTime(millis);
        String second = DateUtils.convertMillisToDateTime(millis);
        assertEquals(first, second);
    }
}
