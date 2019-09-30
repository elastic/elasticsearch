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
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;

public class DateFormattersTests extends ESTestCase {

    // this is not in the duelling tests, because the epoch millis parser in joda time drops the milliseconds after the comma
    // but is able to parse the rest
    // as this feature is supported it also makes sense to make it exact
    public void testEpochMillisParser() {
        DateFormatter formatter = DateFormatters.forPattern("epoch_millis");
        {
            Instant instant = Instant.from(formatter.parse("12345"));
            assertThat(instant.getEpochSecond(), is(12L));
            assertThat(instant.getNano(), is(345_000_000));
        }
        {
            Instant instant = Instant.from(formatter.parse("0"));
            assertThat(instant.getEpochSecond(), is(0L));
            assertThat(instant.getNano(), is(0));
        }
        {
            Instant instant = Instant.from(formatter.parse("123.123456"));
            assertThat(instant.getEpochSecond(), is(0L));
            assertThat(instant.getNano(), is(123123456));
        }
    }

    public void testEpochMilliParser() {
        DateFormatter formatter = DateFormatter.forPattern("8epoch_millis");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> formatter.parse("invalid"));
        assertThat(e.getMessage(), containsString("invalid"));
        assertThat(e.getMessage(), containsString("8epoch_millis"));

        e = expectThrows(IllegalArgumentException.class, () -> formatter.parse("123.1234567"));
        assertThat(e.getMessage(), containsString("123.1234567"));
        assertThat(e.getMessage(), containsString("8epoch_millis"));
    }

    // this is not in the duelling tests, because the epoch second parser in joda time drops the milliseconds after the comma
    // but is able to parse the rest
    // as this feature is supported it also makes sense to make it exact
    public void testEpochSecondParserWithFraction() {
        DateFormatter formatter = DateFormatters.forPattern("epoch_second");

        TemporalAccessor accessor = formatter.parse("1234.1");
        Instant instant = DateFormatters.from(accessor).toInstant();
        assertThat(instant.getEpochSecond(), is(1234L));
        assertThat(DateFormatters.from(accessor).toInstant().getNano(), is(100_000_000));

        accessor = formatter.parse("1234");
        instant = DateFormatters.from(accessor).toInstant();
        assertThat(instant.getEpochSecond(), is(1234L));
        assertThat(instant.getNano(), is(0));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> formatter.parse("abc"));
        assertThat(e.getMessage(), containsString("abc"));
        assertThat(e.getMessage(), containsString("epoch_second"));

        e = expectThrows(IllegalArgumentException.class, () -> formatter.parse("1234.abc"));
        assertThat(e.getMessage(), containsString("1234.abc"));
        assertThat(e.getMessage(), containsString("epoch_second"));

        e = expectThrows(IllegalArgumentException.class, () -> formatter.parse("1234.1234567890"));
        assertThat(e.getMessage(), containsString("1234.1234567890"));
        assertThat(e.getMessage(), containsString("epoch_second"));    }

    public void testEpochMilliParsersWithDifferentFormatters() {
        DateFormatter formatter = DateFormatter.forPattern("8strict_date_optional_time||epoch_millis");
        TemporalAccessor accessor = formatter.parse("123");
        assertThat(DateFormatters.from(accessor).toInstant().toEpochMilli(), is(123L));
        assertThat(formatter.pattern(), is("8strict_date_optional_time||epoch_millis"));
    }

    public void testParsersWithMultipleInternalFormats() throws Exception {
        ZonedDateTime first = DateFormatters.from(
            DateFormatters.forPattern("strict_date_optional_time_nanos").parse("2018-05-15T17:14:56+0100"));
        ZonedDateTime second = DateFormatters.from(
            DateFormatters.forPattern("strict_date_optional_time_nanos").parse("2018-05-15T17:14:56+01:00"));
        assertThat(first, is(second));
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
        assertThat(formatter, instanceOf(JavaDateFormatter.class));
    }

    public void testEpochFormatting() {
        long seconds = randomLongBetween(0, 130L * 365 * 86400); // from 1970 epoch till around 2100
        long nanos = randomLongBetween(0, 999_999_999L);
        Instant instant = Instant.ofEpochSecond(seconds, nanos);

        DateFormatter millisFormatter = DateFormatter.forPattern("8epoch_millis");
        String millis = millisFormatter.format(instant);
        Instant millisInstant = Instant.from(millisFormatter.parse(millis));
        assertThat(millisInstant.toEpochMilli(), is(instant.toEpochMilli()));
        assertThat(millisFormatter.format(Instant.ofEpochSecond(42, 0)), is("42000"));
        assertThat(millisFormatter.format(Instant.ofEpochSecond(42, 123456789L)), is("42123.456789"));

        DateFormatter secondsFormatter = DateFormatter.forPattern("8epoch_second");
        String formattedSeconds = secondsFormatter.format(instant);
        Instant secondsInstant = Instant.from(secondsFormatter.parse(formattedSeconds));
        assertThat(secondsInstant.getEpochSecond(), is(instant.getEpochSecond()));

        assertThat(secondsFormatter.format(Instant.ofEpochSecond(42, 0)), is("42"));
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

    public void testRoundupFormatterWithEpochDates() {
        assertRoundupFormatter("8epoch_millis", "1234567890", 1234567890L);
        // also check nanos of the epoch_millis formatter if it is rounded up to the nano second
        JavaDateFormatter roundUpFormatter = ((JavaDateFormatter) DateFormatter.forPattern("8epoch_millis")).getRoundupParser();
        Instant epochMilliInstant = DateFormatters.from(roundUpFormatter.parse("1234567890")).toInstant();
        assertThat(epochMilliInstant.getLong(ChronoField.NANO_OF_SECOND), is(890_999_999L));

        assertRoundupFormatter("8strict_date_optional_time||epoch_millis", "2018-10-10T12:13:14.123Z", 1539173594123L);
        assertRoundupFormatter("8strict_date_optional_time||epoch_millis", "1234567890", 1234567890L);
        assertRoundupFormatter("8uuuu-MM-dd'T'HH:mm:ss.SSS||epoch_millis", "2018-10-10T12:13:14.123", 1539173594123L);
        assertRoundupFormatter("8uuuu-MM-dd'T'HH:mm:ss.SSS||epoch_millis", "1234567890", 1234567890L);

        assertRoundupFormatter("8epoch_second", "1234567890", 1234567890999L);
        // also check nanos of the epoch_millis formatter if it is rounded up to the nano second
        JavaDateFormatter epochSecondRoundupParser = ((JavaDateFormatter) DateFormatter.forPattern("8epoch_second")).getRoundupParser();
        Instant epochSecondInstant = DateFormatters.from(epochSecondRoundupParser.parse("1234567890")).toInstant();
        assertThat(epochSecondInstant.getLong(ChronoField.NANO_OF_SECOND), is(999_999_999L));

        assertRoundupFormatter("8strict_date_optional_time||epoch_second", "2018-10-10T12:13:14.123Z", 1539173594123L);
        assertRoundupFormatter("8strict_date_optional_time||epoch_second", "1234567890", 1234567890999L);
        assertRoundupFormatter("8uuuu-MM-dd'T'HH:mm:ss.SSS||epoch_second", "2018-10-10T12:13:14.123", 1539173594123L);
        assertRoundupFormatter("8uuuu-MM-dd'T'HH:mm:ss.SSS||epoch_second", "1234567890", 1234567890999L);
    }

    private void assertRoundupFormatter(String format, String input, long expectedMilliSeconds) {
        assertThat(format, startsWith("8"));
        JavaDateFormatter dateFormatter = (JavaDateFormatter) DateFormatter.forPattern(format);
        dateFormatter.parse(input);
        JavaDateFormatter roundUpFormatter = dateFormatter.getRoundupParser();
        long millis = DateFormatters.from(roundUpFormatter.parse(input)).toInstant().toEpochMilli();
        assertThat(millis, is(expectedMilliSeconds));
    }

    public void testRoundupFormatterZone() {
        ZoneId zoneId = randomZone();
        String format = randomFrom("8epoch_second", "8epoch_millis", "8strict_date_optional_time", "8uuuu-MM-dd'T'HH:mm:ss.SSS",
            "8strict_date_optional_time||date_optional_time");
        JavaDateFormatter formatter = (JavaDateFormatter) DateFormatter.forPattern(format).withZone(zoneId);
        JavaDateFormatter roundUpFormatter = formatter.getRoundupParser();
        assertThat(roundUpFormatter.zone(), is(zoneId));
        assertThat(formatter.zone(), is(zoneId));
    }

    public void testRoundupFormatterLocale() {
        Locale locale = randomLocale(random());
        String format = randomFrom("8epoch_second", "8epoch_millis", "8strict_date_optional_time", "8uuuu-MM-dd'T'HH:mm:ss.SSS",
            "8strict_date_optional_time||date_optional_time");
        JavaDateFormatter formatter = (JavaDateFormatter) DateFormatter.forPattern(format).withLocale(locale);
        JavaDateFormatter roundupParser = formatter.getRoundupParser();
        assertThat(roundupParser.locale(), is(locale));
        assertThat(formatter.locale(), is(locale));
    }
}
