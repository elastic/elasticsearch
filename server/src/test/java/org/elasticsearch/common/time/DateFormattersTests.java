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

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;

import static org.hamcrest.Matchers.containsString;
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

    // this is not in the duelling tests, because the epoch second parser in joda time drops the milliseconds after the comma
    // but is able to parse the rest
    // as this feature is supported it also makes sense to make it exact
    public void testEpochSecondParser() {
        DateFormatter formatter = DateFormatters.forPattern("epoch_second");

        assertThat(Instant.from(formatter.parse("1234.567")).toEpochMilli(), is(1234567L));
        assertThat(Instant.from(formatter.parse("1234.")).getNano(), is(0));
        assertThat(Instant.from(formatter.parse("1234.")).getEpochSecond(), is(1234L));
        assertThat(Instant.from(formatter.parse("1234.1")).getNano(), is(100_000_000));
        assertThat(Instant.from(formatter.parse("1234.12")).getNano(), is(120_000_000));
        assertThat(Instant.from(formatter.parse("1234.123")).getNano(), is(123_000_000));
        assertThat(Instant.from(formatter.parse("1234.1234")).getNano(), is(123_400_000));
        assertThat(Instant.from(formatter.parse("1234.12345")).getNano(), is(123_450_000));
        assertThat(Instant.from(formatter.parse("1234.123456")).getNano(), is(123_456_000));
        assertThat(Instant.from(formatter.parse("1234.1234567")).getNano(), is(123_456_700));
        assertThat(Instant.from(formatter.parse("1234.12345678")).getNano(), is(123_456_780));
        assertThat(Instant.from(formatter.parse("1234.123456789")).getNano(), is(123_456_789));
        DateTimeParseException e = expectThrows(DateTimeParseException.class, () -> formatter.parse("1234.1234567890"));
        assertThat(e.getMessage(), is("too much granularity after dot [1234.1234567890]"));
        e = expectThrows(DateTimeParseException.class, () -> formatter.parse("1234.123456789013221"));
        assertThat(e.getMessage(), is("too much granularity after dot [1234.123456789013221]"));
        e = expectThrows(DateTimeParseException.class, () -> formatter.parse("abc"));
        assertThat(e.getMessage(), is("invalid number [abc]"));
        e = expectThrows(DateTimeParseException.class, () -> formatter.parse("1234.abc"));
        assertThat(e.getMessage(), is("invalid number [1234.abc]"));

        // different zone, should still yield the same output, as epoch is time zone independent
        ZoneId zoneId = randomZone();
        DateFormatter zonedFormatter = formatter.withZone(zoneId);

        assertThatSameDateTime(formatter, zonedFormatter, randomLongBetween(-100_000_000, 100_000_000));
        assertSameFormat(formatter, randomLongBetween(-100_000_000, 100_000_000));
        assertThat(formatter.format(Instant.ofEpochSecond(1234, 567_000_000)), is("1234.567"));
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
}
