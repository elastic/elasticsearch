/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.time;

import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.not;

/** Checks that the millis and nanos fast paths agree with the String parser whenever they succeed. */
public class DateFormatterTryParseEpochTests extends ESTestCase {

    private static final List<String> FORMATS = List.of(
        "strict_date_optional_time",
        "strict_date_optional_time_nanos",
        "strict_date_optional_time||epoch_millis",
        "epoch_millis||strict_date_optional_time",
        "iso8601",
        "strict_date_time",
        "strict_date_time_no_millis",
        "strict_date_hour_minute_second",
        "strict_date_hour_minute_second_fraction",
        "strict_date_hour_minute_second_millis",
        "strict_year_month_day",
        "strict_year_month",
        "strict_year",
        "yyyy-MM-dd HH:mm:ss",
        "yyyy-MM-dd",
        "date_optional_time",
        "basic_date_time"
    );

    private static final List<String> ZONES = List.of(
        "",
        "",
        "Z",
        "z",
        "+01:00",
        "-05:30",
        "+00:00",
        "-00:00",
        "+18:00",
        "-18:00",
        "+18:30",
        "+23:59",
        "+24:00",
        "+0100",
        "+01",
        "+01:00:30",
        "+01000",
        "+a1:00",
        "+01:b0",
        "+01:60",
        "~",
        "UTC",
        "GMT",
        "Europe/Paris",
        "Zjunk"
    );

    public void testMatchesStringParse() {
        int accepted = 0;
        for (int i = 0; i < 20_000; i++) {
            DateFormatter formatter = DateFormatter.forPattern(randomFrom(FORMATS));
            if (randomInt(9) == 0) {
                formatter = formatter.withZone(randomFrom(ZoneOffset.ofHours(2), ZoneId.of("Europe/Paris"), ZoneOffset.UTC));
            }
            String input = randomInput();
            if (assertMatches(formatter, input)) {
                accepted++;
            }
        }
        // fails if the generator stops producing input the fast path accepts
        assertThat(accepted, not(0));
    }

    public void testEdgeCases() {
        DateFormatter iso = DateFormatter.forPattern("strict_date_optional_time");
        for (String input : List.of(
            "1970-01-01T00:00:00Z",
            "1969-12-31T23:59:59.999999999Z",
            "1970-01-01T00:00:00.000000001+00:01",
            "2262-04-11T23:47:16.854775807Z",
            "2262-04-11T23:47:16.854775808Z",
            "2262-04-12",
            "0000-01-01",
            "0000-02-29",
            "1900-02-29",
            "2000-02-29",
            "2023-02-29",
            "2024-02-29T12:00:00",
            "2024-04-31",
            "2024-00-10",
            "2024-13-10",
            "2024-01-00",
            "9999-12-31T23:59:59.999999999-18:00",
            "2024-01-01T24:00:00Z",
            "2024-01-01T23:60:00Z",
            "2024-01-01T23:59:60Z",
            "2024-01-01T00:00:00.Z",
            "2024-01-01T00:00:00.1234567890Z",
            "2024-01-01T00:00:00,5Z",
            "-2024-01-01",
            "2024-01-01T",
            "2024-01-01T10",
            "2024-01-01T10:00",
            "2024-01-01T10:00Z",
            "2024-01-01T10:00:00Z ",
            "２０２４-01-01",
            ""
        )) {
            assertMatches(iso, input);
            assertMatches(DateFormatter.forPattern("strict_date_optional_time_nanos"), input);
            assertMatches(DateFormatter.forPattern("yyyy-MM-dd HH:mm:ss"), input.replace('T', ' '));
        }
    }

    /** Exercises parser settings beyond those used by named formats. */
    public void testRandomParserSettings() {
        List<ChronoField> fieldOrder = List.of(
            ChronoField.MONTH_OF_YEAR,
            ChronoField.DAY_OF_MONTH,
            ChronoField.HOUR_OF_DAY,
            ChronoField.MINUTE_OF_HOUR,
            ChronoField.SECOND_OF_MINUTE,
            ChronoField.NANO_OF_SECOND
        );
        int accepted = 0;
        for (int i = 0; i < 2_000; i++) {
            Set<ChronoField> mandatory = Set.copyOf(fieldOrder.subList(0, randomInt(fieldOrder.size())));
            ChronoField maxAllowed = randomBoolean() ? null : randomFrom(ChronoField.YEAR, randomFrom(fieldOrder));
            FastDateTimeParser parser = new FastDateTimeParser(
                mandatory,
                randomBoolean(),
                maxAllowed,
                randomFrom(DecimalSeparator.values()),
                randomFrom(TimezonePresence.values()),
                randomFrom('T', ' ')
            );
            if (randomInt(9) == 0) {
                parser = parser.withDefaults(Map.of(ChronoField.MONTH_OF_YEAR, 1, ChronoField.DAY_OF_MONTH, 1));
            }
            FastDateTimeParser withZone = randomInt(9) == 0 ? (FastDateTimeParser) parser.withZone(ZoneOffset.ofHours(3)) : parser;
            for (int j = 0; j < 20; j++) {
                String input = randomInput();
                byte[] bytes = bytes(input);
                long millis = withZone.tryParseEpoch(bytes, 0, bytes.length, false);
                long nanos = withZone.tryParseEpoch(bytes, 0, bytes.length, true);
                if (millis == DateFormatter.DECLINED && nanos == DateFormatter.DECLINED) {
                    continue;
                }
                accepted++;
                Instant instant;
                try {
                    instant = DateFormatters.from(withZone.parse(input), Locale.ROOT).toInstant();
                } catch (RuntimeException e) {
                    throw new AssertionError("fast path accepted [" + input + "] but parse failed", e);
                }
                if (millis != DateFormatter.DECLINED) {
                    assertEquals(input, DateUtils.toLongMillis(instant), millis);
                }
                if (nanos != DateFormatter.DECLINED) {
                    assertEquals(input, DateUtils.toLong(instant), nanos);
                }
            }
        }
        assertThat(accepted, not(0));
    }

    /** Checks calendar arithmetic against {@link LocalDate#toEpochDay} across all four-digit years. */
    public void testEveryDate() {
        DateFormatter millis = DateFormatter.forPattern("strict_date_optional_time");
        DateFormatter nanos = DateFormatter.forPattern("strict_date_optional_time_nanos");
        long maxNanosDay = DateUtils.MAX_NANOSECOND_INSTANT.getEpochSecond() / 86_400;
        byte[] b = new byte[10];
        b[4] = '-';
        b[7] = '-';
        for (int year = 0; year <= 9999; year++) {
            putDigits(b, 0, 4, year);
            for (int month = 1; month <= 12; month++) {
                putDigits(b, 5, 2, month);
                int length = YearMonth.of(year, month).lengthOfMonth();
                for (int day = 1; day <= 31; day++) {
                    putDigits(b, 8, 2, day);
                    long parsedMillis = millis.tryParseMillis(b, 0, 10);
                    if (day > length) {
                        assertEquals(DateFormatter.DECLINED, parsedMillis);
                        continue;
                    }
                    long epochDay = LocalDate.of(year, month, day).toEpochDay();
                    assertEquals(year + "-" + month + "-" + day, epochDay * 86_400_000L, parsedMillis);
                    long parsedNanos = nanos.tryParseNanos(b, 0, 10);
                    long expectedNanos = epochDay >= 0 && epochDay <= maxNanosDay ? epochDay * 86_400_000_000_000L : DateFormatter.DECLINED;
                    assertEquals(year + "-" + month + "-" + day, expectedNanos, parsedNanos);
                }
            }
        }
    }

    /** Every {@code +hh:mm} and {@code -hh:mm} offset up to 18:00 matches the String parse, and every larger one declines. */
    public void testEveryOffset() {
        DateFormatter formatter = DateFormatter.forPattern("strict_date_optional_time");
        for (char sign : new char[] { '+', '-' }) {
            for (int hours = 0; hours <= 23; hours++) {
                for (int minutes = 0; minutes <= 59; minutes++) {
                    String input = String.format(Locale.ROOT, "2024-01-15T10:11:12.123%c%02d:%02d", sign, hours, minutes);
                    boolean fits = hours * 60 + minutes <= 18 * 60;
                    assertEquals(input, fits, assertMatches(formatter, input));
                }
            }
        }
    }

    private static void putDigits(byte[] b, int start, int count, int value) {
        for (int i = start + count - 1; i >= start; i--) {
            b[i] = (byte) ('0' + value % 10);
            value /= 10;
        }
    }

    public void testCommonLayoutsTakeTheFastPath() {
        assertAccepted("strict_date_optional_time", "2024-01-15T10:11:12.123Z", 1705313472123L);
        assertAccepted("strict_date_optional_time", "2024-01-15T10:11:12Z", 1705313472000L);
        assertAccepted("strict_date_optional_time", "2024-01-15T12:11:12.123+02:00", 1705313472123L);
        assertAccepted("strict_date_optional_time", "2024-01-15", 1705276800000L);
        assertAccepted("strict_date_optional_time||epoch_millis", "2024-01-15T10:11:12.123Z", 1705313472123L);
        assertAccepted("strict_date_time", "2024-01-15T10:11:12.123Z", 1705313472123L);
        assertAccepted("yyyy-MM-dd HH:mm:ss", "2024-01-15 10:11:12", 1705313472000L);
        assertAccepted("yyyy-MM-dd", "2024-01-15", 1705276800000L);
        assertEquals(
            1705313472123456789L,
            DateFormatter.forPattern("strict_date_optional_time_nanos").tryParseNanos(bytes("2024-01-15T10:11:12.123456789Z"), 0, 30)
        );
    }

    private static void assertAccepted(String format, String input, long expectedMillis) {
        byte[] bytes = bytes(input);
        assertEquals(input, expectedMillis, DateFormatter.forPattern(format).tryParseMillis(bytes, 0, bytes.length));
        assertTrue(assertMatches(DateFormatter.forPattern(format), input));
    }

    /** Checks accepted values against the String parser and reports whether either fast path succeeded. */
    private static boolean assertMatches(DateFormatter formatter, String input) {
        byte[] utf8 = bytes(input);
        int offset = randomIntBetween(0, 4);
        byte[] padded = new byte[offset + utf8.length + randomIntBetween(0, 4)];
        for (int i = 0; i < padded.length; i++) {
            padded[i] = (byte) "09Z-:+".charAt(randomInt(5));
        }
        System.arraycopy(utf8, 0, padded, offset, utf8.length);

        long millis = formatter.tryParseMillis(padded, offset, utf8.length);
        long nanos = formatter.tryParseNanos(padded, offset, utf8.length);
        if (millis == DateFormatter.DECLINED && nanos == DateFormatter.DECLINED) {
            return false;
        }
        Instant instant;
        try {
            instant = DateFormatters.from(formatter.parse(input), formatter.locale()).toInstant();
        } catch (RuntimeException e) {
            throw new AssertionError("fast path accepted [" + input + "] with [" + formatter.pattern() + "] but parse failed", e);
        }
        if (millis != DateFormatter.DECLINED) {
            assertEquals(formatter.pattern() + " " + input, DateUtils.toLongMillis(instant), millis);
        }
        if (nanos != DateFormatter.DECLINED) {
            assertEquals(formatter.pattern() + " " + input, DateUtils.toLong(instant), nanos);
        }
        return true;
    }

    private static byte[] bytes(String input) {
        return input.getBytes(StandardCharsets.UTF_8);
    }

    private static String randomInput() {
        StringBuilder b = new StringBuilder();
        int year = randomBoolean() ? randomIntBetween(1960, 2300) : randomIntBetween(0, 9999);
        b.append(String.format(Locale.ROOT, "%04d", year));
        if (randomInt(19) > 0) {
            b.append('-').append(twoDigits(randomIntBetween(0, 13)));
            if (randomInt(19) > 0) {
                b.append('-').append(twoDigits(randomInt(9) == 0 ? randomIntBetween(0, 32) : randomIntBetween(1, 28)));
                if (randomInt(4) > 0) {
                    b.append(randomFrom('T', 'T', ' ', 't'));
                    b.append(twoDigits(randomInt(9) == 0 ? 24 : randomInt(23)));
                    if (randomInt(9) > 0) {
                        b.append(':').append(twoDigits(randomInt(19) == 0 ? 60 : randomInt(59)));
                        if (randomInt(9) > 0) {
                            b.append(':').append(twoDigits(randomInt(19) == 0 ? 60 : randomInt(59)));
                            if (randomBoolean()) {
                                b.append(randomFrom('.', '.', ','));
                                int digits = randomInt(10);
                                for (int d = 0; d < digits; d++) {
                                    b.append((char) ('0' + randomInt(9)));
                                }
                            }
                        }
                    }
                    b.append(randomFrom(ZONES));
                }
            }
        }
        if (randomInt(9) == 0) {
            mutate(b);
        }
        return b.toString();
    }

    private static void mutate(StringBuilder b) {
        int pos = randomInt(b.length());
        switch (randomInt(3)) {
            case 0 -> b.insert(pos, randomFrom('x', '-', ':', '9', ' ', 'é'));
            case 1 -> {
                if (pos < b.length()) {
                    b.deleteCharAt(pos);
                }
            }
            case 2 -> {
                if (pos < b.length()) {
                    b.setCharAt(pos, randomFrom('x', '-', ':', '9', '/', 'é'));
                }
            }
            case 3 -> b.insert(0, '-');
            default -> throw new AssertionError();
        }
    }

    private static String twoDigits(int value) {
        return String.format(Locale.ROOT, "%02d", value);
    }
}
