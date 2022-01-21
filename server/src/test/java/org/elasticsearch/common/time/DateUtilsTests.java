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
import org.joda.time.DateTimeZone;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class DateUtilsTests extends ESTestCase {
    // list of ignored timezones.
    // These should be cleaned up when all tested jdks (oracle, adoptopenjdk, openjdk etc) have the timezone db included
    // see when a timezone was included in jdk version here https://www.oracle.com/java/technologies/tzdata-versions.html
    private static final Set<String> IGNORE = new HashSet<>(Arrays.asList(
        "Eire", "Europe/Dublin", // dublin timezone in joda does not account for DST
        "Asia/Qostanay", // part of tzdata2018h
        "America/Godthab", // part of tzdata2020a (maps to America/Nuuk)
        "America/Nuuk"// part of tzdata2020a
    ));

    // A temporary list of zones and JDKs, where Joda and the JDK timezone data are out of sync, until either Joda or the JDK
    // are updated, see https://github.com/elastic/elasticsearch/issues/82356
    private static final Set<String> IGNORE_IDS = new HashSet<>(Arrays.asList("Pacific/Niue"));

    private static boolean maybeIgnore(String jodaId) {
        if (IGNORE_IDS.contains(jodaId)) {
            return true;
        }
        return false;
    }

    public void testTimezoneIds() {
        assertNull(DateUtils.dateTimeZoneToZoneId(null));
        assertNull(DateUtils.zoneIdToDateTimeZone(null));
        for (String jodaId : DateTimeZone.getAvailableIDs()) {
            if (IGNORE.contains(jodaId) || maybeIgnore(jodaId)) continue;
            DateTimeZone jodaTz = DateTimeZone.forID(jodaId);
            ZoneId zoneId = DateUtils.dateTimeZoneToZoneId(jodaTz); // does not throw
            long now = 0;
            assertThat(jodaId, zoneId.getRules().getOffset(Instant.ofEpochMilli(now)).getTotalSeconds() * 1000,
                equalTo(jodaTz.getOffset(now)));
            if (DateUtils.DEPRECATED_SHORT_TIMEZONES.containsKey(jodaTz.getID())) {
                assertWarnings("Use of short timezone id " + jodaId + " is deprecated. Use " + zoneId.getId() + " instead");
            }
            // roundtrip does not throw either
            assertNotNull(DateUtils.zoneIdToDateTimeZone(zoneId));
        }
    }
}
