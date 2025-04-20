/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.time;

import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Map.entry;
import static org.elasticsearch.common.time.DateUtilsRounding.getMonthOfYear;
import static org.elasticsearch.common.time.DateUtilsRounding.getTotalMillisByYearMonth;
import static org.elasticsearch.common.time.DateUtilsRounding.getYear;
import static org.elasticsearch.common.time.DateUtilsRounding.utcMillisAtStartOfYear;

public class DateUtils {
    public static final long MAX_MILLIS_BEFORE_9999 = 253402300799999L; // end of year 9999
    public static final long MAX_MILLIS_BEFORE_MINUS_9999 = -377705116800000L; // beginning of year -9999

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(DateUtils.class);
    // pkg private for tests
    static final Map<String, String> DEPRECATED_SHORT_TIMEZONES;
    static {
        Map<String, String> tzs = new HashMap<>();
        tzs.put("EST", "-05:00"); // eastern time without daylight savings
        tzs.put("HST", "-10:00");
        tzs.put("MST", "-07:00");
        tzs.put("ROC", "Asia/Taipei");
        tzs.put("Eire", "Europe/London");
        DEPRECATED_SHORT_TIMEZONES = Collections.unmodifiableMap(tzs);
    }

    // Map of deprecated timezones and their recommended new counterpart
    public static final Map<String, String> DEPRECATED_LONG_TIMEZONES = Map.ofEntries(
        entry("Africa/Asmera", "Africa/Nairobi"),
        entry("Africa/Timbuktu", "Africa/Abidjan"),
        entry("America/Argentina/ComodRivadavia", "America/Argentina/Catamarca"),
        entry("America/Atka", "America/Adak"),
        entry("America/Buenos_Aires", "America/Argentina/Buenos_Aires"),
        entry("America/Catamarca", "America/Argentina/Catamarca"),
        entry("America/Coral_Harbour", "America/Atikokan"),
        entry("America/Cordoba", "America/Argentina/Cordoba"),
        entry("America/Ensenada", "America/Tijuana"),
        entry("America/Fort_Wayne", "America/Indiana/Indianapolis"),
        entry("America/Indianapolis", "America/Indiana/Indianapolis"),
        entry("America/Jujuy", "America/Argentina/Jujuy"),
        entry("America/Knox_IN", "America/Indiana/Knox"),
        entry("America/Louisville", "America/Kentucky/Louisville"),
        entry("America/Mendoza", "America/Argentina/Mendoza"),
        entry("America/Montreal", "America/Toronto"),
        entry("America/Porto_Acre", "America/Rio_Branco"),
        entry("America/Rosario", "America/Argentina/Cordoba"),
        entry("America/Santa_Isabel", "America/Tijuana"),
        entry("America/Shiprock", "America/Denver"),
        entry("America/Virgin", "America/Port_of_Spain"),
        entry("Antarctica/South_Pole", "Pacific/Auckland"),
        entry("Asia/Ashkhabad", "Asia/Ashgabat"),
        entry("Asia/Calcutta", "Asia/Kolkata"),
        entry("Asia/Chongqing", "Asia/Shanghai"),
        entry("Asia/Chungking", "Asia/Shanghai"),
        entry("Asia/Dacca", "Asia/Dhaka"),
        entry("Asia/Harbin", "Asia/Shanghai"),
        entry("Asia/Kashgar", "Asia/Urumqi"),
        entry("Asia/Katmandu", "Asia/Kathmandu"),
        entry("Asia/Macao", "Asia/Macau"),
        entry("Asia/Rangoon", "Asia/Yangon"),
        entry("Asia/Saigon", "Asia/Ho_Chi_Minh"),
        entry("Asia/Tel_Aviv", "Asia/Jerusalem"),
        entry("Asia/Thimbu", "Asia/Thimphu"),
        entry("Asia/Ujung_Pandang", "Asia/Makassar"),
        entry("Asia/Ulan_Bator", "Asia/Ulaanbaatar"),
        entry("Atlantic/Faeroe", "Atlantic/Faroe"),
        entry("Atlantic/Jan_Mayen", "Europe/Oslo"),
        entry("Australia/ACT", "Australia/Sydney"),
        entry("Australia/Canberra", "Australia/Sydney"),
        entry("Australia/LHI", "Australia/Lord_Howe"),
        entry("Australia/NSW", "Australia/Sydney"),
        entry("Australia/North", "Australia/Darwin"),
        entry("Australia/Queensland", "Australia/Brisbane"),
        entry("Australia/South", "Australia/Adelaide"),
        entry("Australia/Tasmania", "Australia/Hobart"),
        entry("Australia/Victoria", "Australia/Melbourne"),
        entry("Australia/West", "Australia/Perth"),
        entry("Australia/Yancowinna", "Australia/Broken_Hill"),
        entry("Brazil/Acre", "America/Rio_Branco"),
        entry("Brazil/DeNoronha", "America/Noronha"),
        entry("Brazil/East", "America/Sao_Paulo"),
        entry("Brazil/West", "America/Manaus"),
        entry("Canada/Atlantic", "America/Halifax"),
        entry("Canada/Central", "America/Winnipeg"),
        entry("Canada/East-Saskatchewan", "America/Regina"),
        entry("Canada/Eastern", "America/Toronto"),
        entry("Canada/Mountain", "America/Edmonton"),
        entry("Canada/Newfoundland", "America/St_Johns"),
        entry("Canada/Pacific", "America/Vancouver"),
        entry("Canada/Yukon", "America/Whitehorse"),
        entry("Chile/Continental", "America/Santiago"),
        entry("Chile/EasterIsland", "Pacific/Easter"),
        entry("Cuba", "America/Havana"),
        entry("Egypt", "Africa/Cairo"),
        entry("Eire", "Europe/Dublin"),
        entry("Europe/Belfast", "Europe/London"),
        entry("Europe/Tiraspol", "Europe/Chisinau"),
        entry("GB", "Europe/London"),
        entry("GB-Eire", "Europe/London"),
        entry("Greenwich", "Etc/GMT"),
        entry("Hongkong", "Asia/Hong_Kong"),
        entry("Iceland", "Atlantic/Reykjavik"),
        entry("Iran", "Asia/Tehran"),
        entry("Israel", "Asia/Jerusalem"),
        entry("Jamaica", "America/Jamaica"),
        entry("Japan", "Asia/Tokyo"),
        entry("Kwajalein", "Pacific/Kwajalein"),
        entry("Libya", "Africa/Tripoli"),
        entry("Mexico/BajaNorte", "America/Tijuana"),
        entry("Mexico/BajaSur", "America/Mazatlan"),
        entry("Mexico/General", "America/Mexico_City"),
        entry("NZ", "Pacific/Auckland"),
        entry("NZ-CHAT", "Pacific/Chatham"),
        entry("Navajo", "America/Denver"),
        entry("PRC", "Asia/Shanghai"),
        entry("Pacific/Johnston", "Pacific/Honolulu"),
        entry("Pacific/Ponape", "Pacific/Pohnpei"),
        entry("Pacific/Samoa", "Pacific/Pago_Pago"),
        entry("Pacific/Truk", "Pacific/Chuuk"),
        entry("Pacific/Yap", "Pacific/Chuuk"),
        entry("Poland", "Europe/Warsaw"),
        entry("Portugal", "Europe/Lisbon"),
        entry("ROC", "Asia/Taipei"),
        entry("ROK", "Asia/Seoul"),
        entry("Singapore", "Asia/Singapore"),
        entry("Turkey", "Europe/Istanbul"),
        entry("UCT", "Etc/UCT"),
        entry("US/Alaska", "America/Anchorage"),
        entry("US/Aleutian", "America/Adak"),
        entry("US/Arizona", "America/Phoenix"),
        entry("US/Central", "America/Chicago"),
        entry("US/East-Indiana", "America/Indiana/Indianapolis"),
        entry("US/Eastern", "America/New_York"),
        entry("US/Hawaii", "Pacific/Honolulu"),
        entry("US/Indiana-Starke", "America/Indiana/Knox"),
        entry("US/Michigan", "America/Detroit"),
        entry("US/Mountain", "America/Denver"),
        entry("US/Pacific", "America/Los_Angeles"),
        entry("US/Samoa", "Pacific/Pago_Pago"),
        entry("Universal", "Etc/UTC"),
        entry("W-SU", "Europe/Moscow"),
        entry("Zulu", "Etc/UTC")
    );

    public static ZoneId of(String zoneId) {
        String deprecatedId = DEPRECATED_SHORT_TIMEZONES.get(zoneId);
        if (deprecatedId != null) {
            deprecationLogger.warn(
                DeprecationCategory.PARSING,
                "timezone",
                "Use of short timezone id " + zoneId + " is deprecated. Use " + deprecatedId + " instead"
            );
            return ZoneId.of(deprecatedId);
        }
        return ZoneId.of(zoneId).normalized();
    }

    /**
     * The maximum nanosecond resolution date we can properly handle.
     */
    public static final Instant MAX_NANOSECOND_INSTANT = Instant.parse("2262-04-11T23:47:16.854775807Z");

    static final long MAX_NANOSECOND_IN_MILLIS = MAX_NANOSECOND_INSTANT.toEpochMilli();

    public static final long MAX_NANOSECOND = toLong(MAX_NANOSECOND_INSTANT);

    /**
     * convert a java time instant to a long value which is stored in lucene
     * the long value represents the nanoseconds since the epoch
     *
     * @param instant the instant to convert
     * @return        the nano seconds and seconds as a single long
     */
    public static long toLong(Instant instant) {
        if (instant.isBefore(Instant.EPOCH)) {
            throw new IllegalArgumentException(
                "date[" + instant + "] is before the epoch in 1970 and cannot be " + "stored in nanosecond resolution"
            );
        }
        if (instant.isAfter(MAX_NANOSECOND_INSTANT)) {
            throw new IllegalArgumentException(
                "date[" + instant + "] is after 2262-04-11T23:47:16.854775807 and cannot be " + "stored in nanosecond resolution"
            );
        }
        return instant.getEpochSecond() * 1_000_000_000 + instant.getNano();
    }

    /**
     * Convert a java time instant to a long value which is stored in lucene,
     * the long value represents the milliseconds since epoch
     *
     * @param instant the instant to convert
     * @return        the total milliseconds as a single long
     */
    public static long toLongMillis(Instant instant) {
        try {
            return instant.toEpochMilli();
        } catch (ArithmeticException e) {
            if (instant.isAfter(Instant.now())) {
                throw new IllegalArgumentException(
                    "date[" + instant + "] is too far in the future to be represented in a long milliseconds variable",
                    e
                );
            } else {
                throw new IllegalArgumentException(
                    "date[" + instant + "] is too far in the past to be represented in a long milliseconds variable",
                    e
                );
            }
        }
    }

    /**
     * Returns an instant that is with valid nanosecond resolution. If
     * the parameter is before the valid nanosecond range then this returns
     * the minimum {@linkplain Instant} valid for nanosecond resolution. If
     * the parameter is after the valid nanosecond range then this returns
     * the maximum {@linkplain Instant} valid for nanosecond resolution.
     * <p>
     * Useful for checking if all values for the field are within some range,
     * even if the range's endpoints are not valid nanosecond resolution.
     */
    public static Instant clampToNanosRange(Instant instant) {
        if (instant.isBefore(Instant.EPOCH)) {
            return Instant.EPOCH;
        }
        if (instant.isAfter(MAX_NANOSECOND_INSTANT)) {
            return MAX_NANOSECOND_INSTANT;
        }
        return instant;
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
            throw new IllegalArgumentException(
                "nanoseconds ["
                    + nanoSecondsSinceEpoch
                    + "] are before the epoch in 1970 and cannot "
                    + "be processed in nanosecond resolution"
            );
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
     * @param milliSecondsSinceEpoch the millisecond since the epoch
     * @return                      the nanoseconds since the epoch
     */
    public static long toNanoSeconds(long milliSecondsSinceEpoch) {
        if (milliSecondsSinceEpoch < 0) {
            throw new IllegalArgumentException(
                "milliSeconds [" + milliSecondsSinceEpoch + "] are before the epoch in 1970 and cannot " + "be converted to nanoseconds"
            );
        } else if (milliSecondsSinceEpoch > MAX_NANOSECOND_IN_MILLIS) {
            throw new IllegalArgumentException(
                "milliSeconds ["
                    + milliSecondsSinceEpoch
                    + "] are after 2262-04-11T23:47:16.854775807 "
                    + "and cannot be converted to nanoseconds"
            );
        }

        return milliSecondsSinceEpoch * 1_000_000;
    }

    /**
     * Convert a nanosecond timestamp in milliseconds
     *
     * @param nanoSecondsSinceEpoch the nanoseconds since the epoch
     * @return                      the milliseconds since the epoch
     */
    public static long toMilliSeconds(long nanoSecondsSinceEpoch) {
        if (nanoSecondsSinceEpoch < 0) {
            throw new IllegalArgumentException(
                "nanoseconds are [" + nanoSecondsSinceEpoch + "] are before the epoch in 1970 and cannot " + "be converted to milliseconds"
            );
        }

        if (nanoSecondsSinceEpoch == 0) {
            return 0;
        }

        return nanoSecondsSinceEpoch / 1_000_000;
    }

    /**
     * Compare an epoch nanosecond date (such as returned by {@link DateUtils#toLong}
     * to an epoch millisecond date (such as returned by {@link Instant#toEpochMilli()}}.
     * <p>
     * NB: This function does not implement {@link java.util.Comparator} in
     * order to avoid performance costs of autoboxing the input longs.
     *
     * @param nanos Epoch date represented as a long number of nanoseconds.
     *              Note that Elasticsearch does not support nanosecond dates
     *              before Epoch, so this number should never be negative.
     * @param millis Epoch date represented as a long number of milliseconds.
     *               This parameter does not have to be constrained to the
     *               range of long nanosecond dates.
     * @return -1 if the nanosecond date is before the millisecond date,
     *         0  if the two dates represent the same instant,
     *         1  if the nanosecond date is after the millisecond date
     */
    public static int compareNanosToMillis(long nanos, long millis) {
        assert nanos >= 0;
        if (millis < 0) {
            return 1;
        }
        if (millis > MAX_NANOSECOND_IN_MILLIS) {
            return -1;
        }
        // This can't overflow, because we know millis is between 0 and MAX_NANOSECOND_IN_MILLIS,
        // and MAX_NANOSECOND_IN_MILLIS * 1_000_000 doesn't overflow.
        long diff = nanos - (millis * 1_000_000);
        return diff == 0 ? 0 : diff < 0 ? -1 : 1;
    }

    /**
     * Rounds the given utc milliseconds since the epoch down to the next unit millis
     * <p>
     * Note: This does not check for correctness of the result, as this only works with units smaller or equal than a day
     *       In order to ensure the performance of this methods, there are no guards or checks in it
     *
     * @param utcMillis   the milliseconds since the epoch
     * @param unitMillis  the unit to round to
     * @return            the rounded milliseconds since the epoch
     */
    public static long roundFloor(long utcMillis, final long unitMillis) {
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
    public static long roundQuarterOfYear(final long utcMillis) {
        int year = getYear(utcMillis);
        int month = getMonthOfYear(utcMillis, year);
        int firstMonthOfQuarter = (((month - 1) / 3) * 3) + 1;
        return DateUtils.of(year, firstMonthOfQuarter);
    }

    /**
     * Round down to the beginning of the month of the year of the specified time
     * @param utcMillis the milliseconds since the epoch
     * @return The milliseconds since the epoch rounded down to the month of the year
     */
    public static long roundMonthOfYear(final long utcMillis) {
        int year = getYear(utcMillis);
        int month = getMonthOfYear(utcMillis, year);
        return DateUtils.of(year, month);
    }

    /**
     * Round down to the beginning of the nearest multiple of the specified month interval based on the year
     * @param utcMillis the milliseconds since the epoch
     * @param monthInterval the interval in months to round down to
     *
     * @return The milliseconds since the epoch rounded down to the beginning of the nearest multiple of the
     * specified month interval based on the year
     */
    public static long roundIntervalMonthOfYear(final long utcMillis, final int monthInterval) {
        if (monthInterval <= 0) {
            throw new IllegalArgumentException("month interval must be strictly positive, got [" + monthInterval + "]");
        }
        int year = getYear(utcMillis);
        int month = getMonthOfYear(utcMillis, year);

        // Convert date to total months since epoch reference point (year 1 BCE boundary which is year 0)
        // 1. (year-1): Adjusts for 1-based year counting
        // 2. * 12: Converts years to months
        // 3. (month-1): Converts 1-based month to 0-based index
        int totalMonths = (year - 1) * 12 + (month - 1);

        // Calculate interval index using floor division to handle negative values correctly
        // This ensures proper alignment for BCE dates (negative totalMonths)
        int quotient = Math.floorDiv(totalMonths, monthInterval);

        // Calculate the starting month of the interval period
        int firstMonthOfInterval = quotient * monthInterval;

        // Convert back to month-of-year (1-12):
        // 1. Calculate modulo 12 to get 0-11 month index
        // 2. Add 12 before final modulo to handle negative values
        // 3. Convert to 1-based month numbering
        int monthInYear = (firstMonthOfInterval % 12 + 12) % 12 + 1;

        // Calculate corresponding year:
        // 1. Subtract month offset (monthInYear - 1) to get total months at year boundary
        // 2. Convert months to years
        // 3. Add 1 to adjust back to 1-based year counting
        int yearResult = (firstMonthOfInterval - (monthInYear - 1)) / 12 + 1;

        return DateUtils.of(yearResult, monthInYear);
    }

    /**
     * Round down to the beginning of the year of the specified time
     * @param utcMillis the milliseconds since the epoch
     * @return The milliseconds since the epoch rounded down to the beginning of the year
     */
    public static long roundYear(final long utcMillis) {
        int year = getYear(utcMillis);
        return utcMillisAtStartOfYear(year);
    }

    /**
     * Round down to the beginning of the nearest multiple of the specified year interval
     * @param utcMillis the milliseconds since the epoch
     * @param yearInterval the interval in years to round down to
     *
     * @return The milliseconds since the epoch rounded down to the beginning of the nearest multiple of the specified year interval
     */
    public static long roundYearInterval(final long utcMillis, final int yearInterval) {
        if (yearInterval <= 0) {
            throw new IllegalArgumentException("year interval must be strictly positive, got [" + yearInterval + "]");
        }
        int year = getYear(utcMillis);

        // Convert date to total years since epoch reference point (year 1 BCE boundary which is year 0)
        int totalYears = year - 1;

        // Calculate interval index using floor division to handle negative values correctly
        // This ensures proper alignment for BCE dates (negative totalYears)
        int quotient = Math.floorDiv(totalYears, yearInterval);

        // Calculate the starting total years of the current interval
        int startTotalYears = quotient * yearInterval;

        // Convert back to actual calendar year by adding 1 (reverse the base year adjustment)
        int startYear = startTotalYears + 1;

        return utcMillisAtStartOfYear(startYear);
    }

    /**
     * Round down to the beginning of the week based on week year of the specified time
     * @param utcMillis the milliseconds since the epoch
     * @return The milliseconds since the epoch rounded down to the beginning of the week based on week year
     */
    public static long roundWeekOfWeekYear(final long utcMillis) {
        return roundWeekIntervalOfWeekYear(utcMillis, 1);
    }

    /**
     * Round down to the beginning of the nearest multiple of the specified week interval based on week year
     * <p>
     * Consider Sun Dec 29 1969 00:00:00.000 as the start of the first week.
     * @param utcMillis the milliseconds since the epoch
     * @param weekInterval the interval in weeks to round down to
     *
     * @return The milliseconds since the epoch rounded down to the beginning of the nearest multiple of the
     * specified week interval based on week year
     */
    public static long roundWeekIntervalOfWeekYear(final long utcMillis, final int weekInterval) {
        if (weekInterval <= 0) {
            throw new IllegalArgumentException("week interval must be strictly positive, got [" + weekInterval + "]");
        }
        return roundFloor(utcMillis + 3 * 86400 * 1000L, 604800000L * weekInterval) - 3 * 86400 * 1000L;
    }

    /**
     * Return the first day of the month
     * @param year  the year to return
     * @param month the month to return, ranging from 1-12
     * @return the milliseconds since the epoch of the first day of the month in the year
     */
    private static long of(final int year, final int month) {
        long millis = utcMillisAtStartOfYear(year);
        millis += getTotalMillisByYearMonth(year, month);
        return millis;
    }

    /**
     * Returns the current UTC date-time with milliseconds precision.
     * In Java 9+ (as opposed to Java 8) the {@code Clock} implementation uses system's best clock implementation (which could mean
     * that the precision of the clock can be milliseconds, microseconds or nanoseconds), whereas in Java 8
     * {@code System.currentTimeMillis()} is always used. To account for these differences, this method defines a new {@code Clock}
     * which will offer a value for {@code ZonedDateTime.now()} set to always have milliseconds precision.
     *
     * @return {@link ZonedDateTime} instance for the current date-time with milliseconds precision in UTC
     */
    public static ZonedDateTime nowWithMillisResolution() {
        return nowWithMillisResolution(Clock.systemUTC());
    }

    public static ZonedDateTime nowWithMillisResolution(Clock clock) {
        Clock millisResolutionClock = Clock.tick(clock, Duration.ofMillis(1));
        return ZonedDateTime.now(millisResolutionClock);
    }
}
