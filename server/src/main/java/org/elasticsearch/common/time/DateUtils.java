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

    // Map of deprecated timezones and their recommended new counterpart
    public static final Map<String, String> DEPRECATED_LONG_TIMEZONES;
    static {
        Map<String, String> tzs = new HashMap<>();
        tzs.put("Africa/Asmera","Africa/Nairobi");
        tzs.put("Africa/Timbuktu","Africa/Abidjan");
        tzs.put("America/Argentina/ComodRivadavia","America/Argentina/Catamarca");
        tzs.put("America/Atka","America/Adak");
        tzs.put("America/Buenos_Aires","America/Argentina/Buenos_Aires");
        tzs.put("America/Catamarca","America/Argentina/Catamarca");
        tzs.put("America/Coral_Harbour","America/Atikokan");
        tzs.put("America/Cordoba","America/Argentina/Cordoba");
        tzs.put("America/Ensenada","America/Tijuana");
        tzs.put("America/Fort_Wayne","America/Indiana/Indianapolis");
        tzs.put("America/Indianapolis","America/Indiana/Indianapolis");
        tzs.put("America/Jujuy","America/Argentina/Jujuy");
        tzs.put("America/Knox_IN","America/Indiana/Knox");
        tzs.put("America/Louisville","America/Kentucky/Louisville");
        tzs.put("America/Mendoza","America/Argentina/Mendoza");
        tzs.put("America/Montreal","America/Toronto");
        tzs.put("America/Porto_Acre","America/Rio_Branco");
        tzs.put("America/Rosario","America/Argentina/Cordoba");
        tzs.put("America/Santa_Isabel","America/Tijuana");
        tzs.put("America/Shiprock","America/Denver");
        tzs.put("America/Virgin","America/Port_of_Spain");
        tzs.put("Antarctica/South_Pole","Pacific/Auckland");
        tzs.put("Asia/Ashkhabad","Asia/Ashgabat");
        tzs.put("Asia/Calcutta","Asia/Kolkata");
        tzs.put("Asia/Chongqing","Asia/Shanghai");
        tzs.put("Asia/Chungking","Asia/Shanghai");
        tzs.put("Asia/Dacca","Asia/Dhaka");
        tzs.put("Asia/Harbin","Asia/Shanghai");
        tzs.put("Asia/Kashgar","Asia/Urumqi");
        tzs.put("Asia/Katmandu","Asia/Kathmandu");
        tzs.put("Asia/Macao","Asia/Macau");
        tzs.put("Asia/Rangoon","Asia/Yangon");
        tzs.put("Asia/Saigon","Asia/Ho_Chi_Minh");
        tzs.put("Asia/Tel_Aviv","Asia/Jerusalem");
        tzs.put("Asia/Thimbu","Asia/Thimphu");
        tzs.put("Asia/Ujung_Pandang","Asia/Makassar");
        tzs.put("Asia/Ulan_Bator","Asia/Ulaanbaatar");
        tzs.put("Atlantic/Faeroe","Atlantic/Faroe");
        tzs.put("Atlantic/Jan_Mayen","Europe/Oslo");
        tzs.put("Australia/ACT","Australia/Sydney");
        tzs.put("Australia/Canberra","Australia/Sydney");
        tzs.put("Australia/LHI","Australia/Lord_Howe");
        tzs.put("Australia/NSW","Australia/Sydney");
        tzs.put("Australia/North","Australia/Darwin");
        tzs.put("Australia/Queensland","Australia/Brisbane");
        tzs.put("Australia/South","Australia/Adelaide");
        tzs.put("Australia/Tasmania","Australia/Hobart");
        tzs.put("Australia/Victoria","Australia/Melbourne");
        tzs.put("Australia/West","Australia/Perth");
        tzs.put("Australia/Yancowinna","Australia/Broken_Hill");
        tzs.put("Brazil/Acre","America/Rio_Branco");
        tzs.put("Brazil/DeNoronha","America/Noronha");
        tzs.put("Brazil/East","America/Sao_Paulo");
        tzs.put("Brazil/West","America/Manaus");
        tzs.put("Canada/Atlantic","America/Halifax");
        tzs.put("Canada/Central","America/Winnipeg");
        tzs.put("Canada/East-Saskatchewan","America/Regina");
        tzs.put("Canada/Eastern","America/Toronto");
        tzs.put("Canada/Mountain","America/Edmonton");
        tzs.put("Canada/Newfoundland","America/St_Johns");
        tzs.put("Canada/Pacific","America/Vancouver");
        tzs.put("Canada/Yukon","America/Whitehorse");
        tzs.put("Chile/Continental","America/Santiago");
        tzs.put("Chile/EasterIsland","Pacific/Easter");
        tzs.put("Cuba","America/Havana");
        tzs.put("Egypt","Africa/Cairo");
        tzs.put("Eire","Europe/Dublin");
        tzs.put("Europe/Belfast","Europe/London");
        tzs.put("Europe/Tiraspol","Europe/Chisinau");
        tzs.put("GB","Europe/London");
        tzs.put("GB-Eire","Europe/London");
        tzs.put("Greenwich","Etc/GMT");
        tzs.put("Hongkong","Asia/Hong_Kong");
        tzs.put("Iceland","Atlantic/Reykjavik");
        tzs.put("Iran","Asia/Tehran");
        tzs.put("Israel","Asia/Jerusalem");
        tzs.put("Jamaica","America/Jamaica");
        tzs.put("Japan","Asia/Tokyo");
        tzs.put("Kwajalein","Pacific/Kwajalein");
        tzs.put("Libya","Africa/Tripoli");
        tzs.put("Mexico/BajaNorte","America/Tijuana");
        tzs.put("Mexico/BajaSur","America/Mazatlan");
        tzs.put("Mexico/General","America/Mexico_City");
        tzs.put("NZ","Pacific/Auckland");
        tzs.put("NZ-CHAT","Pacific/Chatham");
        tzs.put("Navajo","America/Denver");
        tzs.put("PRC","Asia/Shanghai");
        tzs.put("Pacific/Johnston","Pacific/Honolulu");
        tzs.put("Pacific/Ponape","Pacific/Pohnpei");
        tzs.put("Pacific/Samoa","Pacific/Pago_Pago");
        tzs.put("Pacific/Truk","Pacific/Chuuk");
        tzs.put("Pacific/Yap","Pacific/Chuuk");
        tzs.put("Poland","Europe/Warsaw");
        tzs.put("Portugal","Europe/Lisbon");
        tzs.put("ROC","Asia/Taipei");
        tzs.put("ROK","Asia/Seoul");
        tzs.put("Singapore","Asia/Singapore");
        tzs.put("Turkey","Europe/Istanbul");
        tzs.put("UCT","Etc/UCT");
        tzs.put("US/Alaska","America/Anchorage");
        tzs.put("US/Aleutian","America/Adak");
        tzs.put("US/Arizona","America/Phoenix");
        tzs.put("US/Central","America/Chicago");
        tzs.put("US/East-Indiana","America/Indiana/Indianapolis");
        tzs.put("US/Eastern","America/New_York");
        tzs.put("US/Hawaii","Pacific/Honolulu");
        tzs.put("US/Indiana-Starke","America/Indiana/Knox");
        tzs.put("US/Michigan","America/Detroit");
        tzs.put("US/Mountain","America/Denver");
        tzs.put("US/Pacific","America/Los_Angeles");
        tzs.put("US/Samoa","Pacific/Pago_Pago");
        tzs.put("Universal","Etc/UTC");
        tzs.put("W-SU","Europe/Moscow");
        tzs.put("Zulu","Etc/UTC");
        DEPRECATED_LONG_TIMEZONES = Collections.unmodifiableMap(tzs);
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
     *       In order to ensure the performane of this methods, there are no guards or checks in it
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
