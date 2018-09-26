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
    private static final Set<String> IGNORE = new HashSet<>(Arrays.asList(
        "Eire", "Europe/Dublin" // dublin timezone in joda does not account for DST
    ));
    public void testTimezoneIds() {
        assertNull(DateUtils.dateTimeZoneToZoneId(null));
        assertNull(DateUtils.zoneIdToDateTimeZone(null));
        for (String jodaId : DateTimeZone.getAvailableIDs()) {
            if (IGNORE.contains(jodaId)) continue;
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
