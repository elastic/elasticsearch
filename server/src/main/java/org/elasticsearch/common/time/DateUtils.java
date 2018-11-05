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

        String deprecatedId = DEPRECATED_SHORT_TIMEZONES.get(timeZone.getID());
        if (deprecatedId != null) {
            deprecationLogger.deprecatedAndMaybeLog("timezone",
                "Use of short timezone id " + timeZone.getID() + " is deprecated. Use " + deprecatedId + " instead");
            return ZoneId.of(deprecatedId);
        }
        return ZoneId.of(timeZone.getID());
    }
}
