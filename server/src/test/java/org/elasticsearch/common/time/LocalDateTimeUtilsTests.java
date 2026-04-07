/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.time;

import org.elasticsearch.test.ESTestCase;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.Year;

import static org.hamcrest.Matchers.equalTo;

public class LocalDateTimeUtilsTests extends ESTestCase {
    public void testTruncateToYears() {
        int randomYear = randomIntBetween(1, 3000);
        assertTruncateToYears(1, randomYear, randomYear);

        assertTruncateToYears(10, 1, 1);
        assertTruncateToYears(10, 11, 11);
        assertTruncateToYears(10, 10, 1);
        assertTruncateToYears(10, 11, 11);
        assertTruncateToYears(10, 2015, 2011);

        assertTruncateToYears(4, 2000, 1997);
        assertTruncateToYears(4, 2003, 2001);
        assertTruncateToYears(4, 2004, 2001);
        assertTruncateToYears(4, 2005, 2005);

        assertTruncateToYears(4, 1, 1);
        assertTruncateToYears(4, -1, -3);
        assertTruncateToYears(4, -3, -3);
        assertTruncateToYears(4, -4, -7);
    }

    private void assertTruncateToYears(int interval, int year, int expectedYear) {
        int inputMonth = randomIntBetween(1, 12);
        LocalDateTime inputDate = LocalDateTime.of(
            year,
            inputMonth,
            Month.of(inputMonth).length(Year.isLeap(year)),
            randomIntBetween(0, 23),
            randomIntBetween(0, 59),
            randomIntBetween(0, 59)
        );

        LocalDateTime expectedResult = LocalDateTime.of(LocalDate.of(expectedYear, 1, 1), LocalTime.MIDNIGHT);

        LocalDateTime resultYears = LocalDateTimeUtils.truncateToYears(inputDate, interval);
        assertThat(resultYears, equalTo(expectedResult));

        // Also tests the same with months
        LocalDateTime resultMonths = LocalDateTimeUtils.truncateToMonths(inputDate, interval * 12);
        assertThat(resultMonths, equalTo(expectedResult));
    }

}
