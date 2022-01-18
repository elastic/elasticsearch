/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.time;

import org.elasticsearch.test.ESTestCase;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.hamcrest.Matchers.equalTo;

public class DateUtilsRoundingTests extends ESTestCase {

    public void testDateUtilsRounding() {
        for (int year = -1000; year < 3000; year++) {
            final long startOfYear = DateUtilsRounding.utcMillisAtStartOfYear(year);
            assertThat(startOfYear, equalTo(ZonedDateTime.of(year, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant().toEpochMilli()));
            assertThat(DateUtilsRounding.getYear(startOfYear), equalTo(year));
            assertThat(DateUtilsRounding.getYear(startOfYear - 1), equalTo(year - 1));
            assertThat(DateUtilsRounding.getMonthOfYear(startOfYear, year), equalTo(1));
            assertThat(DateUtilsRounding.getMonthOfYear(startOfYear - 1, year - 1), equalTo(12));
            for (int month = 1; month <= 12; month++) {
                final long startOfMonth = ZonedDateTime.of(year, month, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
                assertThat(DateUtilsRounding.getMonthOfYear(startOfMonth, year), equalTo(month));
                if (month > 1) {
                    assertThat(DateUtilsRounding.getYear(startOfMonth - 1), equalTo(year));
                    assertThat(DateUtilsRounding.getMonthOfYear(startOfMonth - 1, year), equalTo(month - 1));
                }
            }
        }
    }

    public void testIsLeapYear() {
        assertTrue(DateUtilsRounding.isLeapYear(2004));
        assertTrue(DateUtilsRounding.isLeapYear(2000));
        assertTrue(DateUtilsRounding.isLeapYear(1996));
        assertFalse(DateUtilsRounding.isLeapYear(2001));
        assertFalse(DateUtilsRounding.isLeapYear(1900));
        assertFalse(DateUtilsRounding.isLeapYear(-1000));
        assertTrue(DateUtilsRounding.isLeapYear(-996));
    }
}
