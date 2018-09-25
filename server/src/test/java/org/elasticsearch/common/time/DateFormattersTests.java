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
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DateFormattersTests extends ESTestCase {

    public void testEpochMilliParser() {
        DateFormatter formatter = DateFormatters.forPattern("epoch_millis");

        DateTimeParseException e = expectThrows(DateTimeParseException.class, () -> formatter.parse("invalid"));
        assertThat(e.getMessage(), containsString("invalid number"));

        // different zone, should still yield the same output, as epoch is time zone independent
        ZoneId zoneId = randomZone();
        DateFormatter zonedFormatter = formatter.withZone(zoneId);

        // test with negative and non negative values
        assertThatSameDateTime(formatter, zonedFormatter, randomNonNegativeLong() * -1);
        assertThatSameDateTime(formatter, zonedFormatter, randomNonNegativeLong());
        assertThatSameDateTime(formatter, zonedFormatter, 0);
        assertThatSameDateTime(formatter, zonedFormatter, -1);
        assertThatSameDateTime(formatter, zonedFormatter, 1);

        // format() output should be equal as well
        assertSameFormat(formatter, randomNonNegativeLong() * -1);
        assertSameFormat(formatter, randomNonNegativeLong());
        assertSameFormat(formatter, 0);
        assertSameFormat(formatter, -1);
        assertSameFormat(formatter, 1);
    }

    public void testEpochMilliParsersWithDifferentFormatters() {
        DateFormatter formatter = DateFormatters.forPattern("strict_date_optional_time||epoch_millis");
        TemporalAccessor accessor = formatter.parse("123");
        assertThat(DateFormatters.toZonedDateTime(accessor).toInstant().toEpochMilli(), is(123L));
        assertThat(formatter.pattern(), is("strict_date_optional_time||epoch_millis"));
    }

    private void assertThatSameDateTime(DateFormatter formatter, DateFormatter zonedFormatter, long millis) {
        String millisAsString = String.valueOf(millis);
        ZonedDateTime formatterZonedDateTime = DateFormatters.toZonedDateTime(formatter.parse(millisAsString));
        ZonedDateTime zonedFormatterZonedDateTime = DateFormatters.toZonedDateTime(zonedFormatter.parse(millisAsString));
        assertThat(formatterZonedDateTime.toInstant().toEpochMilli(), is(zonedFormatterZonedDateTime.toInstant().toEpochMilli()));
    }

    private void assertSameFormat(DateFormatter formatter, long millis) {
        String millisAsString = String.valueOf(millis);
        TemporalAccessor accessor = formatter.parse(millisAsString);
        assertThat(millisAsString, is(formatter.format(accessor)));
    }

    private static final Set<String> IGNORE = new HashSet<>(Arrays.asList(
        "Eire", "Europe/Dublin" // dublin timezone in joda does not account for DST
    ));
    public void testTimezoneIds() {
        assertNull(DateFormatters.dateTimeZoneToZoneId(null));
        assertNull(DateFormatters.zoneIdToDateTimeZone(null));
        for (String jodaId : DateTimeZone.getAvailableIDs()) {
            if (IGNORE.contains(jodaId)) continue;
            DateTimeZone jodaTz = DateTimeZone.forID(jodaId);
            ZoneId zoneId = DateFormatters.dateTimeZoneToZoneId(jodaTz); // does not throw
            long now = 0;
            assertThat(jodaId, zoneId.getRules().getOffset(Instant.ofEpochMilli(now)).getTotalSeconds() * 1000,
                       equalTo(jodaTz.getOffset(now)));
            if (DateFormatters.DEPRECATED_SHORT_TIMEZONES.containsKey(jodaTz.getID())) {
                assertWarnings("Use of short timezone id " + jodaId + " is deprecated. Use " + zoneId.getId() + " instead");
            }
            // roundtrip does not throw either
            assertNotNull(DateFormatters.zoneIdToDateTimeZone(zoneId));
        }
    }
}
