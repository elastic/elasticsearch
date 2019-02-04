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
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.common.time.DateUtils.toInstant;
import static org.elasticsearch.common.time.DateUtils.toLong;
import static org.elasticsearch.common.time.DateUtils.toMilliSeconds;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

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

    public void testInstantToLong() {
        assertThat(toLong(Instant.EPOCH), is(0L));

        Instant instant = createRandomInstant();
        long timeSinceEpochInNanos = instant.getEpochSecond() * 1_000_000_000 + instant.getNano();
        assertThat(toLong(instant), is(timeSinceEpochInNanos));
    }

    public void testInstantToLongMin() {
        Instant tooEarlyInstant = ZonedDateTime.parse("1677-09-21T00:12:43.145224191Z").toInstant();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> toLong(tooEarlyInstant));
        assertThat(e.getMessage(), containsString("is before"));
        e = expectThrows(IllegalArgumentException.class, () -> toLong(Instant.EPOCH.minusMillis(1)));
        assertThat(e.getMessage(), containsString("is before"));
    }

    public void testInstantToLongMax() {
        Instant tooEarlyInstant = ZonedDateTime.parse("2262-04-11T23:47:16.854775808Z").toInstant();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> toLong(tooEarlyInstant));
        assertThat(e.getMessage(), containsString("is after"));
    }

    public void testLongToInstant() {
        assertThat(toInstant(0), is(Instant.EPOCH));
        assertThat(toInstant(1), is(Instant.EPOCH.plusNanos(1)));

        Instant instant = createRandomInstant();
        long nowInNs = toLong(instant);
        assertThat(toInstant(nowInNs), is(instant));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> toInstant(-1));
        assertThat(e.getMessage(),
            is("nanoseconds are [-1] are before the epoch in 1970 and cannot be processed in nanosecond resolution"));

        e = expectThrows(IllegalArgumentException.class, () -> toInstant(Long.MIN_VALUE));
        assertThat(e.getMessage(),
            is("nanoseconds are [" + Long.MIN_VALUE + "] are before the epoch in 1970 and cannot be processed in nanosecond resolution"));

        assertThat(toInstant(Long.MAX_VALUE),
            is(ZonedDateTime.parse("2262-04-11T23:47:16.854775807Z").toInstant()));
    }

    public void testNanosToMillis() {
        assertThat(toMilliSeconds(0), is(Instant.EPOCH.toEpochMilli()));

        Instant instant = createRandomInstant();
        long nowInNs = toLong(instant);
        assertThat(toMilliSeconds(nowInNs), is(instant.toEpochMilli()));
    }

    private Instant createRandomInstant() {
        long seconds = randomLongBetween(0, Long.MAX_VALUE) / 1_000_000_000L;
        long nanos = randomLongBetween(0, 999_999_999L);
        return Instant.ofEpochSecond(seconds, nanos);
    }
}
