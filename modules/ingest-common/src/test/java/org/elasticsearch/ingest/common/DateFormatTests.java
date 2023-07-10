/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.test.ESTestCase;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DateFormatTests extends ESTestCase {

    public void testParseJava() {
        var zone = randomZone();
        assertEquals(
            DateFormat.Java.getFunction("MMM dd HH:mm:ss Z", zone, Locale.ENGLISH)
                .apply("Nov 24 01:29:01 -0800")
                .toInstant()
                .atZone(zone)
                .format(DateTimeFormatter.ofPattern("MM dd HH:mm:ss", Locale.ENGLISH)),
            "11 24 01:29:01"
        );
    }

    public void testParseYearOfEraJavaWithTimeZone() {
        Function<String, ZonedDateTime> javaFunction = DateFormat.Java.getFunction(
            "yyyy-MM-dd'T'HH:mm:ss.SSSZZ",
            ZoneOffset.UTC,
            Locale.ROOT
        );
        ZonedDateTime datetime = javaFunction.apply("2018-02-05T13:44:56.657+0100");
        String expectedDateTime = DateFormatter.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").withZone(ZoneOffset.UTC).format(datetime);
        assertThat(expectedDateTime, is("2018-02-05T12:44:56.657Z"));
    }

    public void testParseYearJavaWithTimeZone() {
        Function<String, ZonedDateTime> javaFunction = DateFormat.Java.getFunction(
            "uuuu-MM-dd'T'HH:mm:ss.SSSZZ",
            ZoneOffset.UTC,
            Locale.ROOT
        );
        ZonedDateTime datetime = javaFunction.apply("2018-02-05T13:44:56.657+0100");
        String expectedDateTime = DateFormatter.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").withZone(ZoneOffset.UTC).format(datetime);
        assertThat(expectedDateTime, is("2018-02-05T12:44:56.657Z"));
    }

    public void testParseJavaDefaultYear() {
        String format = randomFrom("8dd/MM", "dd/MM");
        ZoneId timezone = DateUtils.of("Europe/Amsterdam");
        Function<String, ZonedDateTime> javaFunction = DateFormat.Java.getFunction(format, timezone, Locale.ENGLISH);
        int year = ZonedDateTime.now(ZoneOffset.UTC).getYear();
        ZonedDateTime dateTime = javaFunction.apply("12/06");
        assertThat(dateTime.getYear(), is(year));
    }

    public void testParseWeekBasedYearAndWeek() {
        var timezone = randomZone();
        assertEquals(
            DateFormat.Java.getFunction("YYYY-ww", timezone, Locale.ROOT).apply("2020-33"),
            ZonedDateTime.of(2020, 8, 10, 0, 0, 0, 0, timezone)
        );
    }

    public void testParseWeekBasedYear() {
        var timezone = randomZone();
        assertEquals(
            DateFormat.Java.getFunction("YYYY", timezone, Locale.ROOT).apply("2019"),
            ZonedDateTime.of(2018, 12, 31, 0, 0, 0, 0, timezone)
        );
    }

    public void testParseWeekBasedWithLocale() {
        var timezone = randomZone();
        // 33rd week of 2020 starts on 9th August 2020 as per US locale
        assertEquals(
            DateFormat.Java.getFunction("YYYY-ww", timezone, Locale.US).apply("2020-33"),
            ZonedDateTime.of(2020, 8, 9, 0, 0, 0, 0, timezone)
        );
    }

    public void testNoTimezoneOnPatternAndOverride() {
        {
            var timezone = randomZone();
            // this means that hour will be 01:00 at UTC as timezone was not on a pattern, but provided as an ingest param
            assertEquals(
                DateFormat.Java.getFunction("yyyy-MM-dd'T'HH:mm", timezone, Locale.ROOT).apply("2020-01-01T01:00"),
                ZonedDateTime.of(2020, 01, 01, 01, 0, 0, 0, timezone)
            );
        }
        {
            var timezone = randomZone();
            // this means that hour will be 01:00 at -01:00 as timezone was not on a pattern, but provided as an ingest param
            assertEquals(
                DateFormat.Java.getFunction("yyyy-MM-dd'T'HH:mm", timezone, Locale.ROOT).apply("2020-01-01T01:00"),
                ZonedDateTime.of(2020, 01, 01, 01, 0, 0, 0, timezone)
            );
        }
    }

    public void testTimezoneOnAPatternAndNonUTCOverride() {
        var timezone = ZoneId.of("-01:00");
        // this means that hour will be 01:00 at -02:00 as timezone on a pattern. Converted to -01:00 as requested on ingest param
        assertEquals(
            DateFormat.Java.getFunction("yyyy-MM-dd'T'HH:mm XXX", timezone, Locale.ROOT).apply("2020-01-01T01:00 -02:00"),
            ZonedDateTime.of(2020, 01, 01, 02, 0, 0, 0, timezone)
        );
    }

    public void testDefaultHourDefaultedToTimezoneOverride() {
        var timezone = randomZone();
        // this means that hour will be 00:00 (default) at -01:00 as timezone was not on a pattern, but -01:00 was an ingest param
        assertEquals(
            DateFormat.Java.getFunction("yyyy-MM-dd", timezone, Locale.ROOT).apply("2020-01-01"),
            ZonedDateTime.of(2020, 01, 01, 0, 0, 0, 0, timezone)
        );
    }

    public void testParseAllFormatNames() {
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        for (FormatNames formatName : FormatNames.values()) {
            String name = formatName.getName();
            DateFormatter formatter = DateFormatter.forPattern(name);
            String formattedInput = formatter.format(now);
            DateFormat dateFormat = DateFormat.fromString(name);
            ZonedDateTime parsed = dateFormat.getFunction(name, ZoneOffset.UTC, Locale.ROOT).apply(formattedInput);
            String formattedOutput = formatter.format(parsed);
            assertEquals(name, formattedOutput, formattedInput);
        }
    }

    public void testParseUnixMs() {
        var toUnixMsFn = DateFormat.UnixMs.getFunction(null, ZoneOffset.UTC, null);
        assertEquals(toUnixMsFn.apply("1000500").toInstant().toEpochMilli(), 1000500L);

        var invalidMs = randomAlphaOfLength(10);
        var e = expectThrows(IllegalArgumentException.class, () -> toUnixMsFn.apply(invalidMs));
        assertThat(e.getMessage(), containsString("For input string: \"" + invalidMs + "\""));
    }

    public void testParseUnix() {
        var toUnixFn = DateFormat.Unix.getFunction(null, ZoneOffset.UTC, null);
        assertEquals(toUnixFn.apply("1000.5").toInstant().toEpochMilli(), 1000500L);

        var invalidMs = randomAlphaOfLength(10);
        var e = expectThrows(IllegalArgumentException.class, () -> toUnixFn.apply(invalidMs));
        assertThat(e.getMessage(), containsString("For input string: \"" + invalidMs + "\""));
    }

    public void testParseUnixWithMsPrecision() {
        assertEquals(
            DateFormat.Unix.getFunction(null, ZoneOffset.UTC, null).apply("1495718015").toInstant().toEpochMilli(),
            1495718015000L
        );
    }

    public void testParseISO8601() {
        var formattedDate = DateFormat.Iso8601.getFunction(null, ZoneOffset.UTC, null).apply("2001-01-01T00:00:00-0800");
        assertEquals(formattedDate.toInstant().toEpochMilli(), 978336000000L);
        assertEquals(formattedDate.toString(), "2001-01-01T08:00Z");
    }

    public void testParseWhenZoneNotPresentInText() {
        var formattedDate = DateFormat.Iso8601.getFunction(null, ZoneOffset.of("+0100"), null).apply("2001-01-01T00:00:00");
        assertEquals(formattedDate.toInstant().toEpochMilli(), 978303600000L);
        assertEquals(formattedDate.toString(), "2001-01-01T00:00+01:00");
    }

    public void testParseISO8601Failure() {
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> DateFormat.Iso8601.getFunction(null, ZoneOffset.UTC, null).apply("2001-01-0:00-0800")
        );
        assertEquals(e.getMessage(), "failed to parse date field [2001-01-0:00-0800] with format [iso8601]");
    }

    public void testTAI64NParse() {
        String input = (randomBoolean() ? "@" : "") + "4000000050d506482dbdf024";
        String expected = "2012-12-22T03:00:46.767+02:00";
        assertEquals(DateFormat.Tai64n.getFunction(null, ZoneOffset.ofHours(2), null).apply(input).toString(), expected);
    }

    public void testUnixNanoseconds() {
        assertEquals(
            DateFormat.UnixNs.getFunction(null, ZoneOffset.ofHours(2), null).apply("1688548995").toString(),
            "2023-07-05T11:23:15+02:00"
        );

        assertEquals(
            DateFormat.UnixNs.getFunction(null, ZoneOffset.ofHours(2), null).apply("1688548995.987654321").toString(),
            "2023-07-05T11:23:15.987654321+02:00"
        );
        {
            var invalidNs = randomAlphaOfLength(10) + "." + randomLongBetween(10000, 99999999);
            assertEquals(
                expectThrows(
                    IllegalArgumentException.class,
                    () -> DateFormat.UnixNs.getFunction(null, ZoneOffset.ofHours(2), null).apply(invalidNs)
                ).getMessage(),
                "failed to parse date field [" + invalidNs + "] with format [UNIX_NS]"
            );
        }
        {
            var invalidNs = randomLongBetween(10000, 99999999) + "." + randomAlphaOfLength(10);
            assertEquals(
                expectThrows(
                    IllegalArgumentException.class,
                    () -> DateFormat.UnixNs.getFunction(null, ZoneOffset.ofHours(2), null).apply(invalidNs)
                ).getMessage(),
                "failed to parse date field [" + invalidNs + "] with format [UNIX_NS]"
            );
        }
        {
            var invalidNs = randomIntBetween(1000, 50000) + "." + randomIntBetween(1000, 50000) + "." + randomIntBetween(1000, 50000);
            assertEquals(
                expectThrows(
                    IllegalArgumentException.class,
                    () -> DateFormat.UnixNs.getFunction(null, ZoneOffset.ofHours(2), null).apply(invalidNs)
                ).getMessage(),
                "failed to parse date field [" + invalidNs + "] with format [UNIX_NS]"
            );
        }
    }

    public void testFromString() {
        assertThat(DateFormat.fromString("UNIX_MS"), equalTo(DateFormat.UnixMs));
        assertThat(DateFormat.fromString("unix_ms"), equalTo(DateFormat.Java));
        assertThat(DateFormat.fromString("UNIX_NS"), equalTo(DateFormat.UnixNs));
        assertThat(DateFormat.fromString("unix_ns"), equalTo(DateFormat.Java));
        assertThat(DateFormat.fromString("UNIX"), equalTo(DateFormat.Unix));
        assertThat(DateFormat.fromString("unix"), equalTo(DateFormat.Java));
        assertThat(DateFormat.fromString("ISO8601"), equalTo(DateFormat.Iso8601));
        assertThat(DateFormat.fromString("iso8601"), equalTo(DateFormat.Java));
        assertThat(DateFormat.fromString("TAI64N"), equalTo(DateFormat.Tai64n));
        assertThat(DateFormat.fromString("tai64n"), equalTo(DateFormat.Java));
        assertThat(DateFormat.fromString("prefix-" + randomAlphaOfLengthBetween(1, 10)), equalTo(DateFormat.Java));
    }
}
