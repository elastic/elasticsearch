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

import java.time.Instant;
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

    private static final Instant MAX_NANOSECOND_INSTANT = Instant.parse("2262-04-11T23:47:16.854775807Z");

    /**
     * convert a java time instant to a long value which is stored in lucene
     * the long value resembles the nanoseconds since the epoch
     *
     * @param instant the instant to convert
     * @return        the nano seconds and seconds as a single long
     */
    public static long toLong(Instant instant) {
        if (instant.isBefore(Instant.EPOCH)) {
            throw new IllegalArgumentException("date[" + instant + "] is before the epoch in 1970 and cannot be " +
                "stored in nanosecond resolution");
        }
        if (instant.isAfter(MAX_NANOSECOND_INSTANT)) {
            throw new IllegalArgumentException("date[" + instant + "] is after 2262-04-11T23:47:16.854775807 and cannot be " +
                "stored in nanosecond resolution");
        }
        return instant.getEpochSecond() * 1_000_000_000 + instant.getNano();
    }

    /**
     * convert a long value to a java time instant
     * the long value resembles the nanoseconds since the epoch
     *
     * @param nanoSecondsSinceEpoch the nanoseconds since the epoch
     * @return                      the instant resembling the specified date
     */
    public static Instant toInstant(long nanoSecondsSinceEpoch) {
        if (nanoSecondsSinceEpoch < 0) {
            throw new IllegalArgumentException("nanoseconds are [" + nanoSecondsSinceEpoch + "] are before the epoch in 1970 and cannot " +
                "be processed in nanosecond resolution");
        }
        if (nanoSecondsSinceEpoch == 0) {
            return Instant.EPOCH;
        }

        long seconds = nanoSecondsSinceEpoch / 1_000_000_000;
        long nanos = nanoSecondsSinceEpoch % 1_000_000_000;
        return Instant.ofEpochSecond(seconds, nanos);
    }

    /**
     * Convert a nanosecond timestamp in milliseconds
     *
     * @param nanoSecondsSinceEpoch the nanoseconds since the epoch
     * @return                      the milliseconds since the epoch
     */
    public static long toMilliSeconds(long nanoSecondsSinceEpoch) {
        if (nanoSecondsSinceEpoch < 0) {
            throw new IllegalArgumentException("nanoseconds are [" + nanoSecondsSinceEpoch + "] are before the epoch in 1970 and will " +
                "be converted to milliseconds");
        }

        if (nanoSecondsSinceEpoch == 0) {
            return 0;
        }

        return nanoSecondsSinceEpoch / 1_000_000;
    }

    /**
     * Rounds the given utc milliseconds sicne the epoch down to the next unit millis
     *
     * Note: This does not check for correctness of the result, as this only works with units smaller or equal than a day
     *       In order to ensure the performane of this methods, there are no guards or checks in it
     *
     * @param utcMillis   the milliseconds since the epoch
     * @param unitMillis  the unit to round to
     * @return            the rounded milliseconds since the epoch
     */
    public static long roundFloor(long utcMillis, long unitMillis) {
        if (utcMillis >= 0) {
            return utcMillis - utcMillis % unitMillis;
        } else {
            utcMillis += 1;
            return utcMillis - utcMillis % unitMillis - unitMillis;
        }
    }

    /**
     * Round down to the beginning of the quarter of the year of the specified time
     * @param utcMillis the milliseconds since the epoch
     * @return The milliseconds since the epoch rounded down to the quarter of the year
     */
    public static long roundQuarterOfYear(long utcMillis) {
        int year = DateUtils.getYear(utcMillis);
        int month = DateUtils.getMonthOfYear(utcMillis, year);
        int firstMonthOfQuarter = ((month + 1) / 3) * 3;
        return DateUtils.of(year, firstMonthOfQuarter);
    }

    /**
     * Round down to the beginning of the month of the year of the specified time
     * @param utcMillis the milliseconds since the epoch
     * @return The milliseconds since the epoch rounded down to the month of the year
     */
    public static long roundMonthOfYear(long utcMillis) {
        int year = DateUtils.getYear(utcMillis);
        int month = DateUtils.getMonthOfYear(utcMillis, year);
        return DateUtils.of(year, month);
    }

    /**
     * Round down to the beginning of the beginning of the year of the specified time
     * @param utcMillis the milliseconds since the epoch
     * @return The milliseconds since the epoch rounded down to the beginning of the year
     */
    public static long getFirstDayOfYearMillis(long utcMillis) {
        int year = getYear(utcMillis);
        return calculateFirstDayOfYearMillis(year);
    }

    /**
     * Return the first day of the month
     * @param year  the year to return
     * @param month the month to return, ranging from 1-12
     * @return the milliseconds since the epoch of the first day of the month in the year
     */
    private static long of(int year, int month) {
        long millis = getYearMillis(year);
        millis += getTotalMillisByYearMonth(year, month);
        return millis;
    }

    /*
     * begin of code that is partially copied from the joda time implementation in order to make calculations about utc rounding much
     * faster than using java-time and assigning all those objects
     *
     */
    private static final int DAYS_0000_TO_1970 = 719527;
    private static final int MILLIS_PER_DAY = 86_400_000;
    private static final long MILLIS_PER_YEAR = 31556952000L;

    /**
     * calculates the first day of a year in milliseconds since the epoch (assuming UTC)
     *
     * @param year the year
     * @return the milliseconds since the epoch of the first of january at midnight of the specified year
     */
    // see org.joda.time.chrono.GregorianChronology
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

    private static boolean isLeapYear(int year) {
        return ((year & 3) == 0) && ((year % 100) != 0 || (year % 400) == 0);
    }

    private static final long AVERAGE_MILLIS_PER_YEAR_DIVIDED_BY_TWO = MILLIS_PER_YEAR / 2;
    private static final long APPROX_MILLIS_AT_EPOCH_DIVIDED_BY_TWO = (1970L * MILLIS_PER_YEAR) / 2;

    // see org.joda.time.chrono.BasicChronology
    private static int getYear(long instant) {
        // Get an initial estimate of the year, and the millis value that
        // represents the start of that year. Then verify estimate and fix if
        // necessary.

        // Initial estimate uses values divided by two to avoid overflow.
        long unitMillis = AVERAGE_MILLIS_PER_YEAR_DIVIDED_BY_TWO;
        long i2 = (instant >> 1) + APPROX_MILLIS_AT_EPOCH_DIVIDED_BY_TWO;
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

    // see org.joda.time.chrono.BasicGJChronology
    private static int getMonthOfYear(long millis, int year) {
        // Perform a binary search to get the month. To make it go even faster,
        // compare using ints instead of longs. The number of milliseconds per
        // year exceeds the limit of a 32-bit int's capacity, so divide by
        // 1024. No precision is lost (except time of day) since the number of
        // milliseconds per day contains 1024 as a factor. After the division,
        // the instant isn't measured in milliseconds, but in units of
        // (128/125)seconds.

        int i = (int)((millis - getYearMillis(year)) >> 10);

        // There are 86400000 milliseconds per day, but divided by 1024 is
        // 84375. There are 84375 (128/125)seconds per day.

        return
            (isLeapYear(year))
                ? ((i < 182 * 84375)
                ? ((i < 91 * 84375)
                ? ((i < 31 * 84375) ? 1 : (i < 60 * 84375) ? 2 : 3)
                : ((i < 121 * 84375) ? 4 : (i < 152 * 84375) ? 5 : 6))
                : ((i < 274 * 84375)
                ? ((i < 213 * 84375) ? 7 : (i < 244 * 84375) ? 8 : 9)
                : ((i < 305 * 84375) ? 10 : (i < 335 * 84375) ? 11 : 12)))
                : ((i < 181 * 84375)
                ? ((i < 90 * 84375)
                ? ((i < 31 * 84375) ? 1 : (i < 59 * 84375) ? 2 : 3)
                : ((i < 120 * 84375) ? 4 : (i < 151 * 84375) ? 5 : 6))
                : ((i < 273 * 84375)
                ? ((i < 212 * 84375) ? 7 : (i < 243 * 84375) ? 8 : 9)
                : ((i < 304 * 84375) ? 10 : (i < 334 * 84375) ? 11 : 12)));
    }

    private static long getYearMillis(int year) {
        return calculateFirstDayOfYearMillis(year);
    }

    // see org.joda.time.chrono.BasicGJChronology
    private static long getTotalMillisByYearMonth(int year, int month) {
        if (isLeapYear(year)) {
            return MAX_TOTAL_MILLIS_BY_MONTH_ARRAY[month - 1];
        } else {
            return MIN_TOTAL_MILLIS_BY_MONTH_ARRAY[month - 1];
        }
    }

    private static final long[] MIN_TOTAL_MILLIS_BY_MONTH_ARRAY;
    private static final long[] MAX_TOTAL_MILLIS_BY_MONTH_ARRAY;
    private static final int[] MIN_DAYS_PER_MONTH_ARRAY = {
        31,28,31,30,31,30,31,31,30,31,30,31
    };
    private static final int[] MAX_DAYS_PER_MONTH_ARRAY = {
        31,29,31,30,31,30,31,31,30,31,30,31
    };

    static {
        MIN_TOTAL_MILLIS_BY_MONTH_ARRAY = new long[12];
        MAX_TOTAL_MILLIS_BY_MONTH_ARRAY = new long[12];

        long minSum = 0;
        long maxSum = 0;
        for (int i = 0; i < 11; i++) {
            long millis = MIN_DAYS_PER_MONTH_ARRAY[i]
                * (long) MILLIS_PER_DAY;
            minSum += millis;
            MIN_TOTAL_MILLIS_BY_MONTH_ARRAY[i + 1] = minSum;

            millis = MAX_DAYS_PER_MONTH_ARRAY[i]
                * (long) MILLIS_PER_DAY;
            maxSum += millis;
            MAX_TOTAL_MILLIS_BY_MONTH_ARRAY[i + 1] = maxSum;
        }
    }
}
