/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.time;

import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
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

public class DateFormattersTests extends ESTestCase {

    public void testWeekBasedDates() {
        // as per WeekFields.ISO first week starts on Monday and has minimum 4 days
        DateFormatter dateFormatter = DateFormatters.forPattern("YYYY-ww");

        // first week of 2016 starts on Monday 2016-01-04 as previous week in 2016 has only 3 days
        assertThat(DateFormatters.from(dateFormatter.parse("2016-01")) ,
            equalTo(ZonedDateTime.of(2016,01,04, 0,0,0,0,ZoneOffset.UTC)));

        // first week of 2015 starts on Monday 2014-12-29 because 4days belong to 2019
        assertThat(DateFormatters.from(dateFormatter.parse("2015-01")) ,
            equalTo(ZonedDateTime.of(2014,12,29, 0,0,0,0,ZoneOffset.UTC)));


        // as per WeekFields.ISO first week starts on Monday and has minimum 4 days
         dateFormatter = DateFormatters.forPattern("YYYY");

        // first week of 2016 starts on Monday 2016-01-04 as previous week in 2016 has only 3 days
        assertThat(DateFormatters.from(dateFormatter.parse("2016")) ,
            equalTo(ZonedDateTime.of(2016,01,04, 0,0,0,0,ZoneOffset.UTC)));

        // first week of 2015 starts on Monday 2014-12-29 because 4days belong to 2019
        assertThat(DateFormatters.from(dateFormatter.parse("2015")) ,
            equalTo(ZonedDateTime.of(2014,12,29, 0,0,0,0,ZoneOffset.UTC)));
    }

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

    /**
     * test that formatting a date with Long.MAX_VALUE or Long.MIN_VALUE doesn throw errors since we use these
     * e.g. for sorting documents with `null` values first or last
     */
    public void testPrintersLongMinMaxValue() {
        for (FormatNames format : FormatNames.values()) {
            DateFormatter formatter = DateFormatters.forPattern(format.getName());
            formatter.format(DateFieldMapper.Resolution.MILLISECONDS.toInstant(DateUtils.MAX_MILLIS_BEFORE_9999));
            formatter.format(DateFieldMapper.Resolution.MILLISECONDS.toInstant(DateUtils.MAX_MILLIS_BEFORE_MINUS_9999));
        }
    }

    public void testInvalidEpochMilliParser() {
        DateFormatter formatter = DateFormatters.forPattern("epoch_millis");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> formatter.parse("invalid"));
        assertThat(e.getMessage(), containsString("failed to parse date field [invalid] with format [epoch_millis]"));

        e = expectThrows(IllegalArgumentException.class, () -> formatter.parse("123.1234567"));
        assertThat(e.getMessage(), containsString("failed to parse date field [123.1234567] with format [epoch_millis]"));
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
        assertThat(e.getMessage(), is("failed to parse date field [abc] with format [epoch_second]"));

        e = expectThrows(IllegalArgumentException.class, () -> formatter.parse("1234.abc"));
        assertThat(e.getMessage(), is("failed to parse date field [1234.abc] with format [epoch_second]"));

        e = expectThrows(IllegalArgumentException.class, () -> formatter.parse("1234.1234567890"));
        assertThat(e.getMessage(), is("failed to parse date field [1234.1234567890] with format [epoch_second]"));
    }



    public void testEpochMilliParsersWithDifferentFormatters() {
        DateFormatter formatter = DateFormatter.forPattern("strict_date_optional_time||epoch_millis");
        TemporalAccessor accessor = formatter.parse("123");
        assertThat(DateFormatters.from(accessor).toInstant().toEpochMilli(), is(123L));
        assertThat(formatter.pattern(), is("strict_date_optional_time||epoch_millis"));
    }

    public void testParsersWithMultipleInternalFormats() throws Exception {
        ZonedDateTime first = DateFormatters.from(
            DateFormatters.forPattern("strict_date_optional_time_nanos").parse("2018-05-15T17:14:56+0100"));
        ZonedDateTime second = DateFormatters.from(
            DateFormatters.forPattern("strict_date_optional_time_nanos").parse("2018-05-15T17:14:56+01:00"));
        assertThat(first, is(second));
    }

    public void testNanoOfSecondWidth() throws Exception {
        ZonedDateTime first = DateFormatters.from(
            DateFormatters.forPattern("strict_date_optional_time_nanos").parse("1970-01-01T00:00:00.1"));
        assertThat(first.getNano(), is(100000000));
        ZonedDateTime second = DateFormatters.from(
            DateFormatters.forPattern("strict_date_optional_time_nanos").parse("1970-01-01T00:00:00.000000001"));
        assertThat(second.getNano(), is(1));
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

    public void testSupportBackwardsJava8Format() {
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

        DateFormatter millisFormatter = DateFormatter.forPattern("epoch_millis");
        String millis = millisFormatter.format(instant);
        Instant millisInstant = Instant.from(millisFormatter.parse(millis));
        assertThat(millisInstant.toEpochMilli(), is(instant.toEpochMilli()));
        assertThat(millisFormatter.format(Instant.ofEpochSecond(42, 0)), is("42000"));
        assertThat(millisFormatter.format(Instant.ofEpochSecond(42, 123456789L)), is("42123.456789"));

        DateFormatter secondsFormatter = DateFormatter.forPattern("epoch_second");
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

    public void testIso8601Parsing() {
        DateFormatter formatter = DateFormatters.forPattern("iso8601");

        // timezone not allowed with just date
        formatter.format(formatter.parse("2018-05-15"));

        formatter.format(formatter.parse("2018-05-15T17"));
        formatter.format(formatter.parse("2018-05-15T17Z"));
        formatter.format(formatter.parse("2018-05-15T17+0100"));
        formatter.format(formatter.parse("2018-05-15T17+01:00"));

        formatter.format(formatter.parse("2018-05-15T17:14"));
        formatter.format(formatter.parse("2018-05-15T17:14Z"));
        formatter.format(formatter.parse("2018-05-15T17:14-0100"));
        formatter.format(formatter.parse("2018-05-15T17:14-01:00"));

        formatter.format(formatter.parse("2018-05-15T17:14:56"));
        formatter.format(formatter.parse("2018-05-15T17:14:56Z"));
        formatter.format(formatter.parse("2018-05-15T17:14:56+0100"));
        formatter.format(formatter.parse("2018-05-15T17:14:56+01:00"));

        // milliseconds can be separated using comma or decimal point
        formatter.format(formatter.parse("2018-05-15T17:14:56.123"));
        formatter.format(formatter.parse("2018-05-15T17:14:56.123Z"));
        formatter.format(formatter.parse("2018-05-15T17:14:56.123-0100"));
        formatter.format(formatter.parse("2018-05-15T17:14:56.123-01:00"));
        formatter.format(formatter.parse("2018-05-15T17:14:56,123"));
        formatter.format(formatter.parse("2018-05-15T17:14:56,123Z"));
        formatter.format(formatter.parse("2018-05-15T17:14:56,123+0100"));
        formatter.format(formatter.parse("2018-05-15T17:14:56,123+01:00"));

        // microseconds can be separated using comma or decimal point
        formatter.format(formatter.parse("2018-05-15T17:14:56.123456"));
        formatter.format(formatter.parse("2018-05-15T17:14:56.123456Z"));
        formatter.format(formatter.parse("2018-05-15T17:14:56.123456+0100"));
        formatter.format(formatter.parse("2018-05-15T17:14:56.123456+01:00"));
        formatter.format(formatter.parse("2018-05-15T17:14:56,123456"));
        formatter.format(formatter.parse("2018-05-15T17:14:56,123456Z"));
        formatter.format(formatter.parse("2018-05-15T17:14:56,123456-0100"));
        formatter.format(formatter.parse("2018-05-15T17:14:56,123456-01:00"));

        // nanoseconds can be separated using comma or decimal point
        formatter.format(formatter.parse("2018-05-15T17:14:56.123456789"));
        formatter.format(formatter.parse("2018-05-15T17:14:56.123456789Z"));
        formatter.format(formatter.parse("2018-05-15T17:14:56.123456789-0100"));
        formatter.format(formatter.parse("2018-05-15T17:14:56.123456789-01:00"));
        formatter.format(formatter.parse("2018-05-15T17:14:56,123456789"));
        formatter.format(formatter.parse("2018-05-15T17:14:56,123456789Z"));
        formatter.format(formatter.parse("2018-05-15T17:14:56,123456789+0100"));
        formatter.format(formatter.parse("2018-05-15T17:14:56,123456789+01:00"));
    }

    public void testRoundupFormatterWithEpochDates() {
        assertRoundupFormatter("epoch_millis", "1234567890", 1234567890L);
        // also check nanos of the epoch_millis formatter if it is rounded up to the nano second
        JavaDateFormatter roundUpFormatter = ((JavaDateFormatter) DateFormatter.forPattern("8epoch_millis")).getRoundupParser();
        Instant epochMilliInstant = DateFormatters.from(roundUpFormatter.parse("1234567890")).toInstant();
        assertThat(epochMilliInstant.getLong(ChronoField.NANO_OF_SECOND), is(890_999_999L));

        assertRoundupFormatter("strict_date_optional_time||epoch_millis", "2018-10-10T12:13:14.123Z", 1539173594123L);
        assertRoundupFormatter("strict_date_optional_time||epoch_millis", "1234567890", 1234567890L);
        assertRoundupFormatter("strict_date_optional_time||epoch_millis", "2018-10-10", 1539215999999L);
        assertRoundupFormatter("strict_date_optional_time||epoch_millis", "2019-01-25T15:37:17.346928Z", 1548430637346L);
        assertRoundupFormatter("uuuu-MM-dd'T'HH:mm:ss.SSS||epoch_millis", "2018-10-10T12:13:14.123", 1539173594123L);
        assertRoundupFormatter("uuuu-MM-dd'T'HH:mm:ss.SSS||epoch_millis", "1234567890", 1234567890L);

        assertRoundupFormatter("epoch_second", "1234567890", 1234567890999L);
        // also check nanos of the epoch_millis formatter if it is rounded up to the nano second
        JavaDateFormatter epochSecondRoundupParser = ((JavaDateFormatter) DateFormatter.forPattern("8epoch_second")).getRoundupParser();
        Instant epochSecondInstant = DateFormatters.from(epochSecondRoundupParser.parse("1234567890")).toInstant();
        assertThat(epochSecondInstant.getLong(ChronoField.NANO_OF_SECOND), is(999_999_999L));

        assertRoundupFormatter("strict_date_optional_time||epoch_second", "2018-10-10T12:13:14.123Z", 1539173594123L);
        assertRoundupFormatter("strict_date_optional_time||epoch_second", "1234567890", 1234567890999L);
        assertRoundupFormatter("strict_date_optional_time||epoch_second", "2018-10-10", 1539215999999L);
        assertRoundupFormatter("uuuu-MM-dd'T'HH:mm:ss.SSS||epoch_second", "2018-10-10T12:13:14.123", 1539173594123L);
        assertRoundupFormatter("uuuu-MM-dd'T'HH:mm:ss.SSS||epoch_second", "1234567890", 1234567890999L);
    }

    private void assertRoundupFormatter(String format, String input, long expectedMilliSeconds) {
        JavaDateFormatter dateFormatter = (JavaDateFormatter) DateFormatter.forPattern(format);
        dateFormatter.parse(input);
        JavaDateFormatter roundUpFormatter = dateFormatter.getRoundupParser();
        long millis = DateFormatters.from(roundUpFormatter.parse(input)).toInstant().toEpochMilli();
        assertThat(millis, is(expectedMilliSeconds));
    }

    public void testRoundupFormatterZone() {
        ZoneId zoneId = randomZone();
        String format = randomFrom("epoch_second", "epoch_millis", "strict_date_optional_time", "uuuu-MM-dd'T'HH:mm:ss.SSS",
            "strict_date_optional_time||date_optional_time");
        JavaDateFormatter formatter = (JavaDateFormatter) DateFormatter.forPattern(format).withZone(zoneId);
        JavaDateFormatter roundUpFormatter = formatter.getRoundupParser();
        assertThat(roundUpFormatter.zone(), is(zoneId));
        assertThat(formatter.zone(), is(zoneId));
    }

    public void testRoundupFormatterLocale() {
        Locale locale = randomLocale(random());
        String format = randomFrom("epoch_second", "epoch_millis", "strict_date_optional_time", "uuuu-MM-dd'T'HH:mm:ss.SSS",
            "strict_date_optional_time||date_optional_time");
        JavaDateFormatter formatter = (JavaDateFormatter) DateFormatter.forPattern(format).withLocale(locale);
        JavaDateFormatter roundupParser = formatter.getRoundupParser();
        assertThat(roundupParser.locale(), is(locale));
        assertThat(formatter.locale(), is(locale));
    }

    public void test0MillisAreFormatted() {
        DateFormatter formatter = DateFormatter.forPattern("strict_date_time");
        Clock clock = Clock.fixed(ZonedDateTime.of(2019, 02, 8, 11, 43, 00, 0,
            ZoneOffset.UTC).toInstant(), ZoneOffset.UTC);
        String formatted = formatter.formatMillis(clock.millis());
        assertThat(formatted, is("2019-02-08T11:43:00.000Z"));
    }

    public void testFractionalSeconds() {
        DateFormatter formatter = DateFormatters.forPattern("strict_date_optional_time");
        {
            Instant instant = Instant.from(formatter.parse("2019-05-06T14:52:37.1Z"));
            assertThat(instant.getNano(), is(100_000_000));
        }
        {
            Instant instant = Instant.from(formatter.parse("2019-05-06T14:52:37.12Z"));
            assertThat(instant.getNano(), is(120_000_000));
        }
        {
            Instant instant = Instant.from(formatter.parse("2019-05-06T14:52:37.123Z"));
            assertThat(instant.getNano(), is(123_000_000));
        }
        {
            Instant instant = Instant.from(formatter.parse("2019-05-06T14:52:37.1234Z"));
            assertThat(instant.getNano(), is(123_400_000));
        }
        {
            Instant instant = Instant.from(formatter.parse("2019-05-06T14:52:37.12345Z"));
            assertThat(instant.getNano(), is(123_450_000));
        }
        {
            Instant instant = Instant.from(formatter.parse("2019-05-06T14:52:37.123456Z"));
            assertThat(instant.getNano(), is(123_456_000));
        }
        {
            Instant instant = Instant.from(formatter.parse("2019-05-06T14:52:37.1234567Z"));
            assertThat(instant.getNano(), is(123_456_700));
        }
        {
            Instant instant = Instant.from(formatter.parse("2019-05-06T14:52:37.12345678Z"));
            assertThat(instant.getNano(), is(123_456_780));
        }
        {
            Instant instant = Instant.from(formatter.parse("2019-05-06T14:52:37.123456789Z"));
            assertThat(instant.getNano(), is(123_456_789));
        }
    }
}
