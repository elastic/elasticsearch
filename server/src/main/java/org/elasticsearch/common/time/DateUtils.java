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
}
