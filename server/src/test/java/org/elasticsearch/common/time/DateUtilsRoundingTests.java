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
