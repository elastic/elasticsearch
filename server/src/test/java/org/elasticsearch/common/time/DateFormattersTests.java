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
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class DateFormattersTests extends ESTestCase {

    // this is not in the duelling tests, because the epoch millis parser in joda time drops the milliseconds after the comma
    // but is able to parse the rest
    // as this feature is supported it also makes sense to make it exact
    public void testEpochMillisParser() {
        DateFormatter formatter = DateFormatters.forPattern("epoch_millis");
        {
            Instant instant = Instant.from(formatter.parse("12345.6789"));
            assertThat(instant.getEpochSecond(), is(12L));
            assertThat(instant.getNano(), is(345_678_900));
        }
        {
            Instant instant = Instant.from(formatter.parse("12345"));
            assertThat(instant.getEpochSecond(), is(12L));
            assertThat(instant.getNano(), is(345_000_000));
        }
        {
            Instant instant = Instant.from(formatter.parse("12345."));
            assertThat(instant.getEpochSecond(), is(12L));
            assertThat(instant.getNano(), is(345_000_000));
        }
        {
            Instant instant = Instant.from(formatter.parse("0"));
            assertThat(instant.getEpochSecond(), is(0L));
            assertThat(instant.getNano(), is(0));
        }
    }

    public void testEpochMilliParser() {
        DateFormatter formatter = DateFormatters.forPattern("epoch_millis");
        DateTimeParseException e = expectThrows(DateTimeParseException.class, () -> formatter.parse("invalid"));
        assertThat(e.getMessage(), containsString("could not be parsed"));

        e = expectThrows(DateTimeParseException.class, () -> formatter.parse("123.1234567"));
        assertThat(e.getMessage(), containsString("unparsed text found at index 3"));
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
        assertThat(e.getMessage(), is("Text '1234.1234567890' could not be parsed, unparsed text found at index 4"));
        e = expectThrows(DateTimeParseException.class, () -> formatter.parse("1234.123456789013221"));
        assertThat(e.getMessage(), is("Text '1234.123456789013221' could not be parsed, unparsed text found at index 4"));
        e = expectThrows(DateTimeParseException.class, () -> formatter.parse("abc"));
        assertThat(e.getMessage(), is("Text 'abc' could not be parsed at index 0"));
        e = expectThrows(DateTimeParseException.class, () -> formatter.parse("1234.abc"));
        assertThat(e.getMessage(), is("Text '1234.abc' could not be parsed, unparsed text found at index 4"));
    }

    public void testEpochMilliParsersWithDifferentFormatters() {
        DateFormatter formatter = DateFormatter.forPattern("strict_date_optional_time||epoch_millis");
        TemporalAccessor accessor = formatter.parse("123");
        assertThat(DateFormatters.toZonedDateTime(accessor).toInstant().toEpochMilli(), is(123L));
        assertThat(formatter.pattern(), is("strict_date_optional_time||epoch_millis"));
    }

    public void testLocales() {
        assertThat(DateFormatters.forPattern("strict_date_optional_time").locale(), is(Locale.ROOT));
        Locale locale = randomLocale(random());
        assertThat(DateFormatters.forPattern("strict_date_optional_time").withLocale(locale).locale(), is(locale));
    }

    public void testTimeZones() {
        // zone is null by default due to different behaviours between java8 and above
        assertThat(DateFormatters.forPattern("strict_date_optional_time").zone(), is(nullValue()));
        ZoneId zoneId = randomZone();
        assertThat(DateFormatters.forPattern("strict_date_optional_time").withZone(zoneId).zone(), is(zoneId));
    }

    public void testEqualsAndHashcode() {
        assertThat(DateFormatters.forPattern("strict_date_optional_time"),
            sameInstance(DateFormatters.forPattern("strict_date_optional_time")));
        assertThat(DateFormatters.forPattern("YYYY"), equalTo(DateFormatters.forPattern("YYYY")));
        assertThat(DateFormatters.forPattern("YYYY").hashCode(),
            is(DateFormatters.forPattern("YYYY").hashCode()));

        // different timezone, thus not equals
        assertThat(DateFormatters.forPattern("YYYY").withZone(ZoneId.of("CET")), not(equalTo(DateFormatters.forPattern("YYYY"))));

        // different locale, thus not equals
        DateFormatter f1 = DateFormatters.forPattern("YYYY").withLocale(Locale.CANADA);
        DateFormatter f2 = f1.withLocale(Locale.FRENCH);
        assertThat(f1, not(equalTo(f2)));

        // different pattern, thus not equals
        assertThat(DateFormatters.forPattern("YYYY"), not(equalTo(DateFormatters.forPattern("YY"))));

        DateFormatter epochSecondFormatter = DateFormatters.forPattern("epoch_second");
        assertThat(epochSecondFormatter, sameInstance(DateFormatters.forPattern("epoch_second")));
        assertThat(epochSecondFormatter, equalTo(DateFormatters.forPattern("epoch_second")));
        assertThat(epochSecondFormatter.hashCode(), is(DateFormatters.forPattern("epoch_second").hashCode()));

        DateFormatter epochMillisFormatter = DateFormatters.forPattern("epoch_millis");
        assertThat(epochMillisFormatter.hashCode(), is(DateFormatters.forPattern("epoch_millis").hashCode()));
        assertThat(epochMillisFormatter, sameInstance(DateFormatters.forPattern("epoch_millis")));
        assertThat(epochMillisFormatter, equalTo(DateFormatters.forPattern("epoch_millis")));
    }

    public void testForceJava8() {
        assertThat(DateFormatter.forPattern("8yyyy-MM-dd"), instanceOf(JavaDateFormatter.class));
        // named formats too
        assertThat(DateFormatter.forPattern("8date_optional_time"), instanceOf(JavaDateFormatter.class));
        // named formats too
        DateFormatter formatter = DateFormatter.forPattern("8date_optional_time||ww-MM-dd");
        assertThat(formatter, instanceOf(DateFormatters.MergedDateFormatter.class));
        DateFormatters.MergedDateFormatter mergedFormatter = (DateFormatters.MergedDateFormatter) formatter;
        assertThat(mergedFormatter.formatters.get(0), instanceOf(JavaDateFormatter.class));
        assertThat(mergedFormatter.formatters.get(1), instanceOf(JavaDateFormatter.class));
    }

    public void testParsingStrictNanoDates() {
        DateFormatter formatter = DateFormatters.forPattern("strict_date_optional_time_nanos");
        formatter.format(formatter.parse("2016-01-01T00:00:00.000"));
        formatter.format(formatter.parse("2018-05-15T17:14:56"));
        formatter.format(formatter.parse("2018-05-15T17:14:56Z"));
        formatter.format(formatter.parse("2018-05-15T17:14:56+0100"));
        formatter.format(formatter.parse("2018-05-15T17:14:56+01:00"));
        formatter.format(formatter.parse("2018-05-15T17:14:56.123456789Z"));
        formatter.format(formatter.parse("2018-05-15T17:14:56.123456789+0100"));
        formatter.format(formatter.parse("2018-05-15T17:14:56.123456789+01:00"));
    }
}
