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

import org.elasticsearch.common.logging.DeprecationLogger;
import org.joda.time.DateTimeZone;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static java.util.Map.entry;
import static org.elasticsearch.common.time.DateUtilsRounding.getMonthOfYear;
import static org.elasticsearch.common.time.DateUtilsRounding.getTotalMillisByYearMonth;
import static org.elasticsearch.common.time.DateUtilsRounding.getYear;
import static org.elasticsearch.common.time.DateUtilsRounding.utcMillisAtStartOfYear;

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

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(DateUtils.class);
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
            entry("Zulu", "Etc/UTC"));

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
            deprecationLogger.deprecate("timezone",
                "Use of short timezone id " + zoneId + " is deprecated. Use " + deprecatedId + " instead");
            return ZoneId.of(deprecatedId);
        }
        return ZoneId.of(zoneId).normalized();
    }

    static final Instant MAX_NANOSECOND_INSTANT = Instant.parse("2262-04-11T23:47:16.854775807Z");

    static final long MAX_NANOSECOND_IN_MILLIS = MAX_NANOSECOND_INSTANT.toEpochMilli();

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
     * Returns an instant that is with valid nanosecond resolution. If
     * the parameter is before the valid nanosecond range then this returns
     * the minimum {@linkplain Instant} valid for nanosecond resultion. If
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
            throw new IllegalArgumentException("nanoseconds [" + nanoSecondsSinceEpoch + "] are before the epoch in 1970 and cannot " +
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
     * @param milliSecondsSinceEpoch the millisecond since the epoch
     * @return                      the nanoseconds since the epoch
     */
    public static long toNanoSeconds(long milliSecondsSinceEpoch) {
        if (milliSecondsSinceEpoch < 0) {
            throw new IllegalArgumentException("milliSeconds [" + milliSecondsSinceEpoch + "] are before the epoch in 1970 and cannot " +
                "be converted to nanoseconds");
        } else if (milliSecondsSinceEpoch > MAX_NANOSECOND_IN_MILLIS) {
            throw new IllegalArgumentException("milliSeconds [" + milliSecondsSinceEpoch + "] are after 2262-04-11T23:47:16.854775807 " +
                "and cannot be converted to nanoseconds");
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
            throw new IllegalArgumentException("nanoseconds are [" + nanoSecondsSinceEpoch + "] are before the epoch in 1970 and cannot " +
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
        int firstMonthOfQuarter = (((month-1) / 3) * 3) + 1;
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
     * Round down to the beginning of the year of the specified time
     * @param utcMillis the milliseconds since the epoch
     * @return The milliseconds since the epoch rounded down to the beginning of the year
     */
    public static long roundYear(final long utcMillis) {
        int year = getYear(utcMillis);
        return utcMillisAtStartOfYear(year);
    }

    /**
     * Round down to the beginning of the week based on week year of the specified time
     * @param utcMillis the milliseconds since the epoch
     * @return The milliseconds since the epoch rounded down to the beginning of the week based on week year
     */
    public static long roundWeekOfWeekYear(final long utcMillis) {
        return roundFloor(utcMillis + 3 * 86400 * 1000L, 604800000) - 3 * 86400 * 1000L;
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
