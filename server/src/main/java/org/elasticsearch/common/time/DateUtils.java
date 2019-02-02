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

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.joda.time.DateTimeZone;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DateUtils {
    public static DateTimeZone zoneIdToDateTimeZone(ZoneId zoneId) {
        if (zoneId == null) {
            return null;
        }
        if (zoneId instanceof ZoneOffset) {
            // the id for zoneoffset is not ISO compatible, so cannot be read by ZoneId.of
            return DateTimeZone.forOffsetMillis(((ZoneOffset)zoneId).getTotalSeconds() * 1000);
        }
        return DateTimeZone.forID(zoneId.getId());
    }

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(DateFormatters.class));
    // pkg private for tests
    static final Map<String, String> DEPRECATED_SHORT_TIMEZONES;
    public static final Set<String> DEPRECATED_SHORT_TZ_IDS;
    static {
        Map<String, String> tzs = new HashMap<>();
        tzs.put("EST", "-05:00"); // eastern time without daylight savings
        tzs.put("HST", "-10:00");
        tzs.put("MST", "-07:00");
        tzs.put("ROC", "Asia/Taipei");
        tzs.put("Eire", "Europe/London");
        DEPRECATED_SHORT_TIMEZONES = Collections.unmodifiableMap(tzs);
        DEPRECATED_SHORT_TZ_IDS = tzs.keySet();
    }

    public static ZoneId dateTimeZoneToZoneId(DateTimeZone timeZone) {
        if (timeZone == null) {
            return null;
        }
        if (DateTimeZone.UTC.equals(timeZone)) {
            return ZoneOffset.UTC;
        }

        return of(timeZone.getID());
    }

    public static ZoneId of(String zoneId) {
        String deprecatedId = DEPRECATED_SHORT_TIMEZONES.get(zoneId);
        if (deprecatedId != null) {
            deprecationLogger.deprecatedAndMaybeLog("timezone",
                "Use of short timezone id " + zoneId + " is deprecated. Use " + deprecatedId + " instead");
            return ZoneId.of(deprecatedId);
        }
        return ZoneId.of(zoneId).normalized();
    }

    private static final int DAYS_0000_TO_1970 = 719527;
    private static final int MILLIS_PER_DAY = 86_400_000;
    private static final long MILLIS_PER_YEAR = 31556952000L;

    private static boolean isLeapYear(int year) {
        return ((year & 3) == 0) && ((year % 100) != 0 || (year % 400) == 0);
    }

    /**
     * calculates the first day of a year in milliseconds since the epoch (assuming UTC)
     *
     * @param year the year
     * @return the milliseconds since the epoch of the first of january at midnight of the specified year
     */
    private static long calculateFirstDayOfYearMillis(int year) {
        // Initial value is just temporary.
        int leapYears = year / 100;
        if (year < 0) {
            // Add 3 before shifting right since /4 and >>2 behave differently
            // on negative numbers. When the expression is written as
            // (year / 4) - (year / 100) + (year / 400),
            // it works for both positive and negative values, except this optimization
            // eliminates two divisions.
            leapYears = ((year + 3) >> 2) - leapYears + ((leapYears + 3) >> 2) - 1;
        } else {
            leapYears = (year >> 2) - leapYears + (leapYears >> 2);
            if (isLeapYear(year)) {
                leapYears--;
            }
        }

        return (year * 365L + (leapYears - DAYS_0000_TO_1970)) * MILLIS_PER_DAY; // millis per day
    }

    private static int getYear(long instant) {
        // Get an initial estimate of the year, and the millis value that
        // represents the start of that year. Then verify estimate and fix if
        // necessary.

        // Initial estimate uses values divided by two to avoid overflow.
        long unitMillis = getAverageMillisPerYearDividedByTwo();
        long i2 = (instant >> 1) + getApproxMillisAtEpochDividedByTwo();
        if (i2 < 0) {
            i2 = i2 - unitMillis + 1;
        }
        int year = (int) (i2 / unitMillis);

        long yearStart = calculateFirstDayOfYearMillis(year);
        long diff = instant - yearStart;

        if (diff < 0) {
            year--;
        } else if (diff >= MILLIS_PER_DAY * 365L) {
            // One year may need to be added to fix estimate.
            long oneYear;
            if (isLeapYear(year)) {
                oneYear = MILLIS_PER_DAY * 366L;
            } else {
                oneYear = MILLIS_PER_DAY * 365L;
            }

            yearStart += oneYear;

            if (yearStart <= instant) {
                // Didn't go too far, so actually add one year.
                year++;
            }
        }

        return year;
    }

    private static long getApproxMillisAtEpochDividedByTwo() {
        return (1970L * MILLIS_PER_YEAR) / 2;
    }

    private static long getAverageMillisPerYearDividedByTwo() {
        return MILLIS_PER_YEAR / 2;
    }


    public static long getFirstDayOfYearMillis(long utcMillis) {
        int year = getYear(utcMillis);
        return calculateFirstDayOfYearMillis(year);
    }
}
