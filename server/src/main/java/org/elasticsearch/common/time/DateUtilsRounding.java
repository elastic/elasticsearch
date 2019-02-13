/*
 *  Copyright 2001-2014 Stephen Colebourne
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.elasticsearch.common.time;

/**
 * This class has been copied from different locations within the joda time package, as
 * these methods fast when used for rounding, as they do not require conversion to java
 * time objects
 *
 * This code has been copied from jodatime 2.10.1
 * The source can be found at https://github.com/JodaOrg/joda-time/tree/v2.10.1
 *
 * See following methods have been copied (along with required helper variables)
 *
 * - org.joda.time.chrono.GregorianChronology.calculateFirstDayOfYearMillis(int year)
 * - org.joda.time.chrono.BasicChronology.getYear(int year)
 * - org.joda.time.chrono.BasicGJChronology.getMonthOfYear(long utcMillis, int year)
 * - org.joda.time.chrono.BasicGJChronology.getTotalMillisByYearMonth(int year, int month)
 */
class DateUtilsRounding {

    private static final int DAYS_0000_TO_1970 = 719527;
    private static final int MILLIS_PER_DAY = 86_400_000;
    private static final long MILLIS_PER_YEAR = 31556952000L;

    // see org.joda.time.chrono.BasicGJChronology
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

    /**
     * calculates the first day of a year in milliseconds since the epoch (assuming UTC)
     *
     * @param year the year
     * @return the milliseconds since the epoch of the first of january at midnight of the specified year
     */
    // see org.joda.time.chrono.GregorianChronology.calculateFirstDayOfYearMillis
    static long utcMillisAtStartOfYear(final int year) {
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

    private static boolean isLeapYear(final int year) {
        return ((year & 3) == 0) && ((year % 100) != 0 || (year % 400) == 0);
    }

    private static final long AVERAGE_MILLIS_PER_YEAR_DIVIDED_BY_TWO = MILLIS_PER_YEAR / 2;
    private static final long APPROX_MILLIS_AT_EPOCH_DIVIDED_BY_TWO = (1970L * MILLIS_PER_YEAR) / 2;

    // see org.joda.time.chrono.BasicChronology
    static int getYear(final long utcMillis) {
        // Get an initial estimate of the year, and the millis value that
        // represents the start of that year. Then verify estimate and fix if
        // necessary.

        // Initial estimate uses values divided by two to avoid overflow.
        long unitMillis = AVERAGE_MILLIS_PER_YEAR_DIVIDED_BY_TWO;
        long i2 = (utcMillis >> 1) + APPROX_MILLIS_AT_EPOCH_DIVIDED_BY_TWO;
        if (i2 < 0) {
            i2 = i2 - unitMillis + 1;
        }
        int year = (int) (i2 / unitMillis);

        long yearStart = utcMillisAtStartOfYear(year);
        long diff = utcMillis - yearStart;

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

            if (yearStart <= utcMillis) {
                // Didn't go too far, so actually add one year.
                year++;
            }
        }

        return year;
    }

    // see org.joda.time.chrono.BasicGJChronology
    static int getMonthOfYear(final long utcMillis, final int year) {
        // Perform a binary search to get the month. To make it go even faster,
        // compare using ints instead of longs. The number of milliseconds per
        // year exceeds the limit of a 32-bit int's capacity, so divide by
        // 1024. No precision is lost (except time of day) since the number of
        // milliseconds per day contains 1024 as a factor. After the division,
        // the instant isn't measured in milliseconds, but in units of
        // (128/125)seconds.

        int i = (int)((utcMillis - utcMillisAtStartOfYear(year)) >> 10);

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

    // see org.joda.time.chrono.BasicGJChronology
    static long getTotalMillisByYearMonth(final int year, final int month) {
        if (isLeapYear(year)) {
            return MAX_TOTAL_MILLIS_BY_MONTH_ARRAY[month - 1];
        } else {
            return MIN_TOTAL_MILLIS_BY_MONTH_ARRAY[month - 1];
        }
    }
}
