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
import org.hamcrest.Matcher;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.time.temporal.ValueRange;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;
import static org.elasticsearch.common.time.DecimalSeparator.BOTH;
import static org.elasticsearch.common.time.DecimalSeparator.COMMA;
import static org.elasticsearch.common.time.DecimalSeparator.DOT;
import static org.elasticsearch.common.time.TimezonePresence.FORBIDDEN;
import static org.elasticsearch.common.time.TimezonePresence.MANDATORY;
import static org.elasticsearch.common.time.TimezonePresence.OPTIONAL;
import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class Iso8601ParserTests extends ESTestCase {

    private static Iso8601Parser defaultParser() {
        return new Iso8601Parser(Set.of(), true, null, BOTH, OPTIONAL, Map.of());
    }

    private static Matcher<ParseResult> hasResult(DateTime dateTime) {
        return transformedMatch(ParseResult::result, equalTo(dateTime));
    }

    private static Matcher<ParseResult> hasError(int parseError) {
        return transformedMatch(ParseResult::errorIndex, equalTo(parseError));
    }

    public void testStrangeParses() {
        assertThat(defaultParser().tryParse("-9999-01-01", null), hasResult(new DateTime(-9999, 1, 1, null, null, null, null, null, null)));
        assertThat(defaultParser().tryParse("1000", null), hasResult(new DateTime(1000, null, null, null, null, null, null, null, null)));
        assertThat(defaultParser().tryParse("2023-02-02T", null), hasResult(new DateTime(2023, 2, 2, null, null, null, null, null, null)));

        // these are accepted by the previous formatters, but are not valid ISO8601
        assertThat(defaultParser().tryParse("2023-01-01T12:00:00.01,02", null), hasError(22));
        assertThat(defaultParser().tryParse("2023-01-01T12:00:00Europe/Paris+0400", null), hasError(19));
    }

    public void testOutOfRange() {
        assertThat(defaultParser().tryParse("2023-13-12", null), hasError(5));
        assertThat(defaultParser().tryParse("2023-12-32", null), hasError(8));
        assertThat(defaultParser().tryParse("2023-12-31T24", null), hasError(11));
        assertThat(defaultParser().tryParse("2023-12-31T23:60", null), hasError(14));
        assertThat(defaultParser().tryParse("2023-12-31T23:59:60", null), hasError(17));
        assertThat(defaultParser().tryParse("2023-12-31T23:59:59+18:30", null), hasError(19));
        assertThat(defaultParser().tryParse("2023-12-31T23:59:59+24", null), hasError(19));
        assertThat(defaultParser().tryParse("2023-12-31T23:59:59+1060", null), hasError(19));
        assertThat(defaultParser().tryParse("2023-12-31T23:59:59+105960", null), hasError(19));
    }

    public void testMandatoryFields() {
        assertThat(
            new Iso8601Parser(Set.of(YEAR), true, null, BOTH, OPTIONAL, Map.of()).tryParse("2023", null),
            hasResult(new DateTime(2023, null, null, null, null, null, null, null, null))
        );
        assertThat(
            new Iso8601Parser(Set.of(YEAR, MONTH_OF_YEAR), true, null, BOTH, OPTIONAL, Map.of()).tryParse("2023", null),
            hasError(4)
        );

        assertThat(
            new Iso8601Parser(Set.of(YEAR, MONTH_OF_YEAR), true, null, BOTH, OPTIONAL, Map.of()).tryParse("2023-06", null),
            hasResult(new DateTime(2023, 6, null, null, null, null, null, null, null))
        );
        assertThat(
            new Iso8601Parser(Set.of(YEAR, MONTH_OF_YEAR, DAY_OF_MONTH), true, null, BOTH, OPTIONAL, Map.of()).tryParse("2023-06", null),
            hasError(7)
        );

        assertThat(
            new Iso8601Parser(Set.of(YEAR, MONTH_OF_YEAR, DAY_OF_MONTH), true, null, BOTH, OPTIONAL, Map.of()).tryParse("2023-06-20", null),
            hasResult(new DateTime(2023, 6, 20, null, null, null, null, null, null))
        );
        assertThat(
            new Iso8601Parser(Set.of(YEAR, MONTH_OF_YEAR, DAY_OF_MONTH, HOUR_OF_DAY), false, null, BOTH, OPTIONAL, Map.of()).tryParse(
                "2023-06-20",
                null
            ),
            hasError(10)
        );

        assertThat(
            new Iso8601Parser(Set.of(YEAR, MONTH_OF_YEAR, DAY_OF_MONTH, HOUR_OF_DAY), false, null, BOTH, OPTIONAL, Map.of()).tryParse(
                "2023-06-20T15",
                null
            ),
            hasResult(new DateTime(2023, 6, 20, 15, 0, 0, 0, null, null))
        );
        assertThat(
            new Iso8601Parser(Set.of(YEAR, MONTH_OF_YEAR, DAY_OF_MONTH, HOUR_OF_DAY, MINUTE_OF_HOUR), false, null, BOTH, OPTIONAL, Map.of())
                .tryParse("2023-06-20T15", null),
            hasError(13)
        );
        assertThat(
            new Iso8601Parser(Set.of(YEAR, MONTH_OF_YEAR, DAY_OF_MONTH, HOUR_OF_DAY, MINUTE_OF_HOUR), false, null, BOTH, OPTIONAL, Map.of())
                .tryParse("2023-06-20T15Z", null),
            hasError(13)
        );

        assertThat(
            new Iso8601Parser(Set.of(YEAR, MONTH_OF_YEAR, DAY_OF_MONTH, HOUR_OF_DAY, MINUTE_OF_HOUR), false, null, BOTH, OPTIONAL, Map.of())
                .tryParse("2023-06-20T15:48", null),
            hasResult(new DateTime(2023, 6, 20, 15, 48, 0, 0, null, null))
        );
        assertThat(
            new Iso8601Parser(
                Set.of(YEAR, MONTH_OF_YEAR, DAY_OF_MONTH, HOUR_OF_DAY, MINUTE_OF_HOUR, SECOND_OF_MINUTE),
                false,
                null,
                BOTH,
                OPTIONAL,
                Map.of()
            ).tryParse("2023-06-20T15:48", null),
            hasError(16)
        );
        assertThat(
            new Iso8601Parser(
                Set.of(YEAR, MONTH_OF_YEAR, DAY_OF_MONTH, HOUR_OF_DAY, MINUTE_OF_HOUR, SECOND_OF_MINUTE),
                false,
                null,
                BOTH,
                OPTIONAL,
                Map.of()
            ).tryParse("2023-06-20T15:48Z", null),
            hasError(16)
        );

        assertThat(
            new Iso8601Parser(
                Set.of(YEAR, MONTH_OF_YEAR, DAY_OF_MONTH, HOUR_OF_DAY, MINUTE_OF_HOUR, SECOND_OF_MINUTE),
                false,
                null,
                BOTH,
                OPTIONAL,
                Map.of()
            ).tryParse("2023-06-20T15:48:09", null),
            hasResult(new DateTime(2023, 6, 20, 15, 48, 9, 0, null, null))
        );

        assertThat(
            new Iso8601Parser(
                Set.of(YEAR, MONTH_OF_YEAR, DAY_OF_MONTH, HOUR_OF_DAY, MINUTE_OF_HOUR, SECOND_OF_MINUTE, NANO_OF_SECOND),
                false,
                null,
                BOTH,
                OPTIONAL,
                Map.of()
            ).tryParse("2023-06-20T15:48:09", null),
            hasError(19)
        );
        assertThat(
            new Iso8601Parser(
                Set.of(YEAR, MONTH_OF_YEAR, DAY_OF_MONTH, HOUR_OF_DAY, MINUTE_OF_HOUR, SECOND_OF_MINUTE, NANO_OF_SECOND),
                false,
                null,
                BOTH,
                OPTIONAL,
                Map.of()
            ).tryParse("2023-06-20T15:48:09.5", null),
            hasResult(new DateTime(2023, 6, 20, 15, 48, 9, 500_000_000, null, null))
        );
    }

    public void testMaxAllowedField() {
        assertThat(
            new Iso8601Parser(Set.of(), false, YEAR, BOTH, FORBIDDEN, Map.of()).tryParse("2023", null),
            hasResult(new DateTime(2023, null, null, null, null, null, null, null, null))
        );
        assertThat(new Iso8601Parser(Set.of(), false, YEAR, BOTH, FORBIDDEN, Map.of()).tryParse("2023-01", null), hasError(4));

        assertThat(
            new Iso8601Parser(Set.of(), false, MONTH_OF_YEAR, BOTH, FORBIDDEN, Map.of()).tryParse("2023-01", null),
            hasResult(new DateTime(2023, 1, null, null, null, null, null, null, null))
        );
        assertThat(new Iso8601Parser(Set.of(), false, MONTH_OF_YEAR, BOTH, FORBIDDEN, Map.of()).tryParse("2023-01-01", null), hasError(7));

        assertThat(
            new Iso8601Parser(Set.of(), false, DAY_OF_MONTH, BOTH, FORBIDDEN, Map.of()).tryParse("2023-01-01", null),
            hasResult(new DateTime(2023, 1, 1, null, null, null, null, null, null))
        );
        assertThat(new Iso8601Parser(Set.of(), false, DAY_OF_MONTH, BOTH, FORBIDDEN, Map.of()).tryParse("2023-01-01T", null), hasError(10));
        assertThat(
            new Iso8601Parser(Set.of(), false, DAY_OF_MONTH, BOTH, FORBIDDEN, Map.of()).tryParse("2023-01-01T12", null),
            hasError(10)
        );

        assertThat(
            new Iso8601Parser(Set.of(), false, HOUR_OF_DAY, BOTH, FORBIDDEN, Map.of()).tryParse("2023-01-01T12", null),
            hasResult(new DateTime(2023, 1, 1, 12, 0, 0, 0, null, null))
        );
        assertThat(
            new Iso8601Parser(Set.of(), false, HOUR_OF_DAY, BOTH, FORBIDDEN, Map.of()).tryParse("2023-01-01T12:00", null),
            hasError(13)
        );

        assertThat(
            new Iso8601Parser(Set.of(), false, MINUTE_OF_HOUR, BOTH, FORBIDDEN, Map.of()).tryParse("2023-01-01T12:00", null),
            hasResult(new DateTime(2023, 1, 1, 12, 0, 0, 0, null, null))
        );
        assertThat(
            new Iso8601Parser(Set.of(), false, MINUTE_OF_HOUR, BOTH, FORBIDDEN, Map.of()).tryParse("2023-01-01T12:00:00", null),
            hasError(16)
        );

        assertThat(
            new Iso8601Parser(Set.of(), false, SECOND_OF_MINUTE, BOTH, FORBIDDEN, Map.of()).tryParse("2023-01-01T12:00:00", null),
            hasResult(new DateTime(2023, 1, 1, 12, 0, 0, 0, null, null))
        );
        assertThat(
            new Iso8601Parser(Set.of(), false, SECOND_OF_MINUTE, BOTH, FORBIDDEN, Map.of()).tryParse("2023-01-01T12:00:00.5", null),
            hasError(19)
        );
    }

    public void testTimezoneForbidden() {
        assertThat(new Iso8601Parser(Set.of(), false, null, BOTH, FORBIDDEN, Map.of()).tryParse("2023-01-01T12Z", null), hasError(13));
        assertThat(new Iso8601Parser(Set.of(), false, null, BOTH, FORBIDDEN, Map.of()).tryParse("2023-01-01T12:00Z", null), hasError(16));
        assertThat(
            new Iso8601Parser(Set.of(), false, null, BOTH, FORBIDDEN, Map.of()).tryParse("2023-01-01T12:00:00Z", null),
            hasError(19)
        );

        // a default timezone should still make it through
        ZoneOffset zoneId = ZoneOffset.ofHours(2);
        assertThat(
            new Iso8601Parser(Set.of(), false, null, BOTH, FORBIDDEN, Map.of()).tryParse("2023-01-01T12:00:00", zoneId),
            hasResult(new DateTime(2023, 1, 1, 12, 0, 0, 0, zoneId, zoneId))
        );
    }

    public void testTimezoneMandatory() {
        assertThat(new Iso8601Parser(Set.of(), false, null, BOTH, MANDATORY, Map.of()).tryParse("2023-01-01T12", null), hasError(13));
        assertThat(new Iso8601Parser(Set.of(), false, null, BOTH, MANDATORY, Map.of()).tryParse("2023-01-01T12:00", null), hasError(16));
        assertThat(new Iso8601Parser(Set.of(), false, null, BOTH, MANDATORY, Map.of()).tryParse("2023-01-01T12:00:00", null), hasError(19));

        assertThat(
            new Iso8601Parser(Set.of(), false, null, BOTH, MANDATORY, Map.of()).tryParse("2023-01-01T12:00:00Z", null),
            hasResult(new DateTime(2023, 1, 1, 12, 0, 0, 0, ZoneOffset.UTC, ZoneOffset.UTC))
        );
    }

    public void testParseNanos() {
        assertThat(
            defaultParser().tryParse("2023-01-01T12:00:00.5", null),
            hasResult(new DateTime(2023, 1, 1, 12, 0, 0, 500_000_000, null, null))
        );
        assertThat(
            defaultParser().tryParse("2023-01-01T12:00:00,5", null),
            hasResult(new DateTime(2023, 1, 1, 12, 0, 0, 500_000_000, null, null))
        );

        assertThat(
            defaultParser().tryParse("2023-01-01T12:00:00.05", null),
            hasResult(new DateTime(2023, 1, 1, 12, 0, 0, 50_000_000, null, null))
        );
        assertThat(
            defaultParser().tryParse("2023-01-01T12:00:00,005", null),
            hasResult(new DateTime(2023, 1, 1, 12, 0, 0, 5_000_000, null, null))
        );
        assertThat(
            defaultParser().tryParse("2023-01-01T12:00:00.0005", null),
            hasResult(new DateTime(2023, 1, 1, 12, 0, 0, 500_000, null, null))
        );
        assertThat(
            defaultParser().tryParse("2023-01-01T12:00:00,00005", null),
            hasResult(new DateTime(2023, 1, 1, 12, 0, 0, 50_000, null, null))
        );
        assertThat(
            defaultParser().tryParse("2023-01-01T12:00:00.000005", null),
            hasResult(new DateTime(2023, 1, 1, 12, 0, 0, 5_000, null, null))
        );
        assertThat(
            defaultParser().tryParse("2023-01-01T12:00:00,0000005", null),
            hasResult(new DateTime(2023, 1, 1, 12, 0, 0, 500, null, null))
        );
        assertThat(
            defaultParser().tryParse("2023-01-01T12:00:00.00000005", null),
            hasResult(new DateTime(2023, 1, 1, 12, 0, 0, 50, null, null))
        );
        assertThat(
            defaultParser().tryParse("2023-01-01T12:00:00,000000005", null),
            hasResult(new DateTime(2023, 1, 1, 12, 0, 0, 5, null, null))
        );

        // too many nanos
        assertThat(defaultParser().tryParse("2023-01-01T12:00:00.0000000005", null), hasError(29));
    }

    public void testParseDecimalSeparator() {
        assertThat(
            new Iso8601Parser(Set.of(), false, null, BOTH, OPTIONAL, Map.of()).tryParse("2023-01-01T12:00:00.0", null),
            hasResult(new DateTime(2023, 1, 1, 12, 0, 0, 0, null, null))
        );
        assertThat(
            new Iso8601Parser(Set.of(), false, null, BOTH, OPTIONAL, Map.of()).tryParse("2023-01-01T12:00:00,0", null),
            hasResult(new DateTime(2023, 1, 1, 12, 0, 0, 0, null, null))
        );

        assertThat(
            new Iso8601Parser(Set.of(), false, null, DOT, OPTIONAL, Map.of()).tryParse("2023-01-01T12:00:00.0", null),
            hasResult(new DateTime(2023, 1, 1, 12, 0, 0, 0, null, null))
        );
        assertThat(new Iso8601Parser(Set.of(), false, null, DOT, OPTIONAL, Map.of()).tryParse("2023-01-01T12:00:00,0", null), hasError(19));

        assertThat(
            new Iso8601Parser(Set.of(), false, null, COMMA, OPTIONAL, Map.of()).tryParse("2023-01-01T12:00:00.0", null),
            hasError(19)
        );
        assertThat(
            new Iso8601Parser(Set.of(), false, null, COMMA, OPTIONAL, Map.of()).tryParse("2023-01-01T12:00:00,0", null),
            hasResult(new DateTime(2023, 1, 1, 12, 0, 0, 0, null, null))
        );

        assertThat(
            new Iso8601Parser(Set.of(), false, null, BOTH, OPTIONAL, Map.of()).tryParse("2023-01-01T12:00:00+0", null),
            hasError(19)
        );
        assertThat(
            new Iso8601Parser(Set.of(), false, null, BOTH, OPTIONAL, Map.of()).tryParse("2023-01-01T12:00:00+0", null),
            hasError(19)
        );
    }

    private static Matcher<ParseResult> hasTimezone(ZoneId offset) {
        return transformedMatch(r -> r.result().query(TemporalQueries.zone()), equalTo(offset));
    }

    public void testParseTimezones() {
        // using defaults
        assertThat(defaultParser().tryParse("2023-01-01T12:00:00", null), hasTimezone(null));
        assertThat(defaultParser().tryParse("2023-01-01T12:00:00", ZoneOffset.UTC), hasTimezone(ZoneOffset.UTC));
        assertThat(defaultParser().tryParse("2023-01-01T12:00:00", ZoneOffset.ofHours(-3)), hasTimezone(ZoneOffset.ofHours(-3)));

        // timezone specified
        assertThat(defaultParser().tryParse("2023-01-01T12:00:00Z", null), hasTimezone(ZoneOffset.UTC));

        assertThat(defaultParser().tryParse("2023-01-01T12:00:00-05", null), hasTimezone(ZoneOffset.ofHours(-5)));
        assertThat(defaultParser().tryParse("2023-01-01T12:00:00+11", null), hasTimezone(ZoneOffset.ofHours(11)));
        assertThat(defaultParser().tryParse("2023-01-01T12:00:00+0830", null), hasTimezone(ZoneOffset.ofHoursMinutes(8, 30)));
        assertThat(defaultParser().tryParse("2023-01-01T12:00:00-0415", null), hasTimezone(ZoneOffset.ofHoursMinutes(-4, -15)));
        assertThat(defaultParser().tryParse("2023-01-01T12:00:00+08:30", null), hasTimezone(ZoneOffset.ofHoursMinutes(8, 30)));
        assertThat(defaultParser().tryParse("2023-01-01T12:00:00-04:15", null), hasTimezone(ZoneOffset.ofHoursMinutes(-4, -15)));
        assertThat(defaultParser().tryParse("2023-01-01T12:00:00+011030", null), hasTimezone(ZoneOffset.ofHoursMinutesSeconds(1, 10, 30)));
        assertThat(
            defaultParser().tryParse("2023-01-01T12:00:00-074520", null),
            hasTimezone(ZoneOffset.ofHoursMinutesSeconds(-7, -45, -20))
        );
        assertThat(
            defaultParser().tryParse("2023-01-01T12:00:00+01:10:30", null),
            hasTimezone(ZoneOffset.ofHoursMinutesSeconds(1, 10, 30))
        );
        assertThat(
            defaultParser().tryParse("2023-01-01T12:00:00-07:45:20", null),
            hasTimezone(ZoneOffset.ofHoursMinutesSeconds(-7, -45, -20))
        );

        assertThat(defaultParser().tryParse("2023-01-01T12:00:00GMT", null), hasTimezone(ZoneId.of("GMT")));
        assertThat(defaultParser().tryParse("2023-01-01T12:00:00UTC", null), hasTimezone(ZoneId.of("UTC")));
        assertThat(defaultParser().tryParse("2023-01-01T12:00:00UT", null), hasTimezone(ZoneId.of("UT")));
        assertThat(defaultParser().tryParse("2023-01-01T12:00:00GMT+3", null), hasTimezone(ZoneId.of("GMT+3")));
        assertThat(defaultParser().tryParse("2023-01-01T12:00:00UTC-4", null), hasTimezone(ZoneId.of("UTC-4")));
        assertThat(defaultParser().tryParse("2023-01-01T12:00:00UT+6", null), hasTimezone(ZoneId.of("UT+6")));
        assertThat(defaultParser().tryParse("2023-01-01T12:00:00Europe/Paris", null), hasTimezone(ZoneId.of("Europe/Paris")));

        // we could be more specific in the error index for invalid timezones,
        // but that would require keeping track & propagating Result objects within date-time parsing just for the ZoneId
        assertThat(defaultParser().tryParse("2023-01-01T12:00:00+04:0030", null), hasError(19));
        assertThat(defaultParser().tryParse("2023-01-01T12:00:00+0400:30", null), hasError(19));
        assertThat(defaultParser().tryParse("2023-01-01T12:00:00Invalid", null), hasError(19));
    }

    private static void assertEquivalent(String text, DateTimeFormatter formatter) {
        TemporalAccessor expected = formatter.parse(text);
        TemporalAccessor actual = defaultParser().tryParse(text, null).result();
        assertThat(actual, is(notNullValue()));

        assertThat(actual.query(TemporalQueries.localDate()), equalTo(expected.query(TemporalQueries.localDate())));
        assertThat(actual.query(TemporalQueries.localTime()), equalTo(expected.query(TemporalQueries.localTime())));
        assertThat(actual.query(TemporalQueries.zone()), equalTo(expected.query(TemporalQueries.zone())));
    }

    private static void assertEquivalentFailure(String text, DateTimeFormatter formatter) {
        DateTimeParseException expected = expectThrows(DateTimeParseException.class, () -> formatter.parse(text));
        int error = defaultParser().tryParse(text, null).errorIndex();
        assertThat(error, greaterThanOrEqualTo(0));

        assertThat(error, equalTo(expected.getErrorIndex()));
    }

    public void testEquivalence() {
        // test that Iso8601Parser produces the same output as DateTimeFormatter
        DateTimeFormatter mandatoryFormatter = new DateTimeFormatterBuilder().append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
            .optionalStart()
            .appendZoneOrOffsetId()
            .optionalEnd()
            .optionalStart()
            .appendOffset("+HHmm", "Z")
            .optionalEnd()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT);

        // just checking timezones/ids here
        assertEquivalent("2023-01-01T12:00:00", mandatoryFormatter);
        assertEquivalent("2023-01-01T12:00:00Z", mandatoryFormatter);
        assertEquivalent("2023-01-01T12:00:00UT", mandatoryFormatter);
        assertEquivalent("2023-01-01T12:00:00UTC", mandatoryFormatter);
        assertEquivalent("2023-01-01T12:00:00GMT", mandatoryFormatter);
        assertEquivalent("2023-01-01T12:00:00+00", mandatoryFormatter);
        assertEquivalent("2023-01-01T12:00:00-00", mandatoryFormatter);
        assertEquivalent("2023-01-01T12:00:00+05", mandatoryFormatter);
        assertEquivalent("2023-01-01T12:00:00+0500", mandatoryFormatter);
        assertEquivalent("2023-01-01T12:00:00+05:00", mandatoryFormatter);
        assertEquivalent("2023-01-01T12:00:00+05:00:30", mandatoryFormatter);
        assertEquivalent("2023-01-01T12:00:00-07", mandatoryFormatter);
        assertEquivalent("2023-01-01T12:00:00-0715", mandatoryFormatter);
        assertEquivalent("2023-01-01T12:00:00-07:15", mandatoryFormatter);
        assertEquivalent("2023-01-01T12:00:00UTC+05:00", mandatoryFormatter);
        assertEquivalent("2023-01-01T12:00:00GMT-09:45:30", mandatoryFormatter);
        assertEquivalent("2023-01-01T12:00:00Zulu", mandatoryFormatter);
        assertEquivalent("2023-01-01T12:00:00Europe/Paris", mandatoryFormatter);

        assertEquivalentFailure("2023-01-01T12:00:00+5", mandatoryFormatter);
        assertEquivalentFailure("2023-01-01T12:00:00-7", mandatoryFormatter);
        assertEquivalentFailure("2023-01-01T12:00:00InvalidTimeZone", mandatoryFormatter);

        DateTimeFormatter allFieldsOptional = new DateTimeFormatterBuilder().appendValue(YEAR, 4, 4, SignStyle.EXCEEDS_PAD)
            .optionalStart()
            .appendLiteral('-')
            .appendValue(MONTH_OF_YEAR, 2)
            .optionalStart()
            .appendLiteral('-')
            .appendValue(DAY_OF_MONTH, 2)
            .optionalStart()
            .appendLiteral('T')
            .appendValue(HOUR_OF_DAY, 2)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2)
            .optionalEnd()
            .optionalEnd()
            .optionalEnd()
            .optionalEnd()
            .optionalEnd()
            .optionalStart()
            .appendZoneOrOffsetId()
            .optionalEnd()
            .optionalStart()
            .appendOffset("+HHmm", "Z")
            .optionalEnd()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT);

        assertEquivalent("2023", allFieldsOptional);
        assertEquivalent("2023-04", allFieldsOptional);
        assertEquivalent("2023-04-08", allFieldsOptional);
        assertEquivalent("2023-04-08T13", allFieldsOptional);
        assertEquivalent("2023-04-08T13:45", allFieldsOptional);
        assertEquivalent("2023-04-08T13:45:50", allFieldsOptional);
        assertEquivalent("-2023-04-08T13:45:50", allFieldsOptional);
    }

    private static int randomValue(ValueRange range) {
        assert range.isIntValue();
        return randomIntBetween((int) range.getMinimum(), (int) range.getMaximum());
    }

    public void testDefaults() {
        Map<ChronoField, Integer> defaults = Map.of(
            MONTH_OF_YEAR,
            randomValue(MONTH_OF_YEAR.range()),
            DAY_OF_MONTH,
            randomValue(DAY_OF_MONTH.range()),
            HOUR_OF_DAY,
            randomValue(HOUR_OF_DAY.range()),
            MINUTE_OF_HOUR,
            randomValue(MINUTE_OF_HOUR.range()),
            SECOND_OF_MINUTE,
            randomValue(SECOND_OF_MINUTE.range()),
            NANO_OF_SECOND,
            randomValue(NANO_OF_SECOND.range())
        );

        assertThat(
            new Iso8601Parser(Set.of(), true, null, BOTH, OPTIONAL, defaults).tryParse("2023", null),
            hasResult(
                new DateTime(
                    2023,
                    defaults.get(MONTH_OF_YEAR),
                    defaults.get(DAY_OF_MONTH),
                    defaults.get(HOUR_OF_DAY),
                    defaults.get(MINUTE_OF_HOUR),
                    defaults.get(SECOND_OF_MINUTE),
                    defaults.get(NANO_OF_SECOND),
                    null,
                    null
                )
            )
        );
        assertThat(
            new Iso8601Parser(Set.of(), true, null, BOTH, OPTIONAL, defaults).tryParse("2023-01", null),
            hasResult(
                new DateTime(
                    2023,
                    1,
                    defaults.get(DAY_OF_MONTH),
                    defaults.get(HOUR_OF_DAY),
                    defaults.get(MINUTE_OF_HOUR),
                    defaults.get(SECOND_OF_MINUTE),
                    defaults.get(NANO_OF_SECOND),
                    null,
                    null
                )
            )
        );
        assertThat(
            new Iso8601Parser(Set.of(), true, null, BOTH, OPTIONAL, defaults).tryParse("2023-01-01", null),
            hasResult(
                new DateTime(
                    2023,
                    1,
                    1,
                    defaults.get(HOUR_OF_DAY),
                    defaults.get(MINUTE_OF_HOUR),
                    defaults.get(SECOND_OF_MINUTE),
                    defaults.get(NANO_OF_SECOND),
                    null,
                    null
                )
            )
        );
        assertThat(
            new Iso8601Parser(Set.of(), true, null, BOTH, OPTIONAL, defaults).tryParse("2023-01-01T00", null),
            hasResult(
                new DateTime(
                    2023,
                    1,
                    1,
                    0,
                    defaults.get(MINUTE_OF_HOUR),
                    defaults.get(SECOND_OF_MINUTE),
                    defaults.get(NANO_OF_SECOND),
                    null,
                    null
                )
            )
        );
        assertThat(
            new Iso8601Parser(Set.of(), true, null, BOTH, OPTIONAL, defaults).tryParse("2023-01-01T00:00", null),
            hasResult(new DateTime(2023, 1, 1, 0, 0, defaults.get(SECOND_OF_MINUTE), defaults.get(NANO_OF_SECOND), null, null))
        );
        assertThat(
            new Iso8601Parser(Set.of(), true, null, BOTH, OPTIONAL, defaults).tryParse("2023-01-01T00:00:00", null),
            hasResult(new DateTime(2023, 1, 1, 0, 0, 0, defaults.get(NANO_OF_SECOND), null, null))
        );
        assertThat(
            new Iso8601Parser(Set.of(), true, null, BOTH, OPTIONAL, defaults).tryParse("2023-01-01T00:00:00.0", null),
            hasResult(new DateTime(2023, 1, 1, 0, 0, 0, 0, null, null))
        );
    }
}
