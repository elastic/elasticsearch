/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.literal.interval;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.core.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.ParsingException;
import org.elasticsearch.xpack.sql.expression.literal.interval.Intervals.TimeUnit;

import java.time.Duration;
import java.time.Period;
import java.time.temporal.TemporalAmount;
import java.util.Locale;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;
import static org.elasticsearch.xpack.sql.expression.literal.interval.Intervals.intervalType;
import static org.elasticsearch.xpack.sql.expression.literal.interval.Intervals.of;
import static org.elasticsearch.xpack.sql.expression.literal.interval.Intervals.parseInterval;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_DAY;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_DAY_TO_HOUR;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_DAY_TO_MINUTE;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_DAY_TO_SECOND;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_HOUR;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_HOUR_TO_MINUTE;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_HOUR_TO_SECOND;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_MINUTE;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_MINUTE_TO_SECOND;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_MONTH;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_SECOND;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_YEAR;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_YEAR_TO_MONTH;

public class IntervalsTests extends ESTestCase {

    @ParametersFactory
    public static Iterable<Object[]> params() {
        return Stream.of("+", "-", "").map(s -> new Object[] { s }).collect(toList());
    }

    private String sign;

    public IntervalsTests(String sign) {
        this.sign = sign;
    }

    public void testYearInterval() throws Exception {
        int random = randomNonNegativeInt();
        TemporalAmount amount = parseInterval(EMPTY, sign + random, INTERVAL_YEAR);
        assertEquals(maybeNegate(sign, Period.ofYears(random)), amount);
    }

    public void testMonthInterval() throws Exception {
        int random = randomNonNegativeInt();
        TemporalAmount amount = parseInterval(EMPTY, sign + random, INTERVAL_MONTH);
        assertEquals(maybeNegate(sign, Period.ofMonths(random)), amount);
    }

    public void testDayInterval() throws Exception {
        int random = randomNonNegativeInt();
        TemporalAmount amount = parseInterval(EMPTY, sign + random, INTERVAL_DAY);
        assertEquals(maybeNegate(sign, Duration.ofDays(random)), amount);
    }

    public void testHourInterval() throws Exception {
        int random = randomNonNegativeInt();
        TemporalAmount amount = parseInterval(EMPTY, sign + random, INTERVAL_HOUR);
        assertEquals(maybeNegate(sign, Duration.ofHours(random)), amount);
    }

    public void testMinuteInterval() throws Exception {
        int random = randomNonNegativeInt();
        TemporalAmount amount = parseInterval(EMPTY, sign + random, INTERVAL_MINUTE);
        assertEquals(maybeNegate(sign, Duration.ofMinutes(random)), amount);
    }

    public void testSecondInterval() throws Exception {
        int randomSeconds = randomNonNegativeInt();
        int randomMillis = randomBoolean() ? (randomBoolean() ? 0 : 999) : randomInt(999);
        String value = format(Locale.ROOT, "%s%d", sign, randomSeconds);
        value += randomMillis > 0 ? Strings.format(".%03d", randomMillis) : "";
        TemporalAmount amount = parseInterval(EMPTY, value, INTERVAL_SECOND);
        assertEquals(maybeNegate(sign, Duration.ofSeconds(randomSeconds).plusMillis(randomMillis)), amount);
    }

    public void testSecondNoMillisInterval() throws Exception {
        int randomSeconds = randomNonNegativeInt();
        String value = format(Locale.ROOT, "%s%d", sign, randomSeconds);
        TemporalAmount amount = parseInterval(EMPTY, value, INTERVAL_SECOND);
        assertEquals(maybeNegate(sign, Duration.ofSeconds(randomSeconds)), amount);
    }

    public void testYearToMonth() throws Exception {
        int randomYear = randomNonNegativeInt();
        int randomMonth = randomInt(11);
        String value = format(Locale.ROOT, "%s%d-%d", sign, randomYear, randomMonth);
        TemporalAmount amount = parseInterval(EMPTY, value, INTERVAL_YEAR_TO_MONTH);
        assertEquals(maybeNegate(sign, Period.ofYears(randomYear).plusMonths(randomMonth)), amount);
    }

    public void testDayToHour() throws Exception {
        int randomDay = randomNonNegativeInt();
        int randomHour = randomInt(23);
        String value = format(Locale.ROOT, "%s%d %d", sign, randomDay, randomHour);
        TemporalAmount amount = parseInterval(EMPTY, value, INTERVAL_DAY_TO_HOUR);
        assertEquals(maybeNegate(sign, Duration.ofDays(randomDay).plusHours(randomHour)), amount);
    }

    public void testDayToMinute() throws Exception {
        int randomDay = randomNonNegativeInt();
        int randomHour = randomInt(23);
        int randomMinute = randomInt(59);
        String value = format(Locale.ROOT, "%s%d %d:%d", sign, randomDay, randomHour, randomMinute);
        TemporalAmount amount = parseInterval(EMPTY, value, INTERVAL_DAY_TO_MINUTE);
        assertEquals(maybeNegate(sign, Duration.ofDays(randomDay).plusHours(randomHour).plusMinutes(randomMinute)), amount);
    }

    public void testDayToSecond() throws Exception {
        int randomDay = randomNonNegativeInt();
        int randomHour = randomInt(23);
        int randomMinute = randomInt(59);
        int randomSecond = randomInt(59);

        boolean withMillis = randomBoolean();
        int randomMilli = withMillis ? randomInt(999) : 0;
        String millisString = withMillis && randomMilli > 0 ? Strings.format(".%03d", randomMilli) : "";

        String value = format(Locale.ROOT, "%s%d %d:%d:%d%s", sign, randomDay, randomHour, randomMinute, randomSecond, millisString);
        TemporalAmount amount = parseInterval(EMPTY, value, INTERVAL_DAY_TO_SECOND);
        assertEquals(
            maybeNegate(
                sign,
                Duration.ofDays(randomDay).plusHours(randomHour).plusMinutes(randomMinute).plusSeconds(randomSecond).plusMillis(randomMilli)
            ),
            amount
        );
    }

    public void testHourToMinute() throws Exception {
        int randomHour = randomNonNegativeInt();
        int randomMinute = randomInt(59);
        String value = format(Locale.ROOT, "%s%d:%d", sign, randomHour, randomMinute);
        TemporalAmount amount = parseInterval(EMPTY, value, INTERVAL_HOUR_TO_MINUTE);
        assertEquals(maybeNegate(sign, Duration.ofHours(randomHour).plusMinutes(randomMinute)), amount);
    }

    public void testHourToSecond() throws Exception {
        int randomHour = randomNonNegativeInt();
        int randomMinute = randomInt(59);
        int randomSecond = randomInt(59);

        boolean withMillis = randomBoolean();
        int randomMilli = withMillis ? randomInt(999) : 0;
        String millisString = withMillis && randomMilli > 0 ? Strings.format(".%03d", randomMilli) : "";

        String value = format(Locale.ROOT, "%s%d:%d:%d%s", sign, randomHour, randomMinute, randomSecond, millisString);
        TemporalAmount amount = parseInterval(EMPTY, value, INTERVAL_HOUR_TO_SECOND);
        assertEquals(
            maybeNegate(sign, Duration.ofHours(randomHour).plusMinutes(randomMinute).plusSeconds(randomSecond).plusMillis(randomMilli)),
            amount
        );
    }

    public void testMinuteToSecond() throws Exception {
        int randomMinute = randomNonNegativeInt();
        int randomSecond = randomInt(59);

        boolean withMillis = randomBoolean();
        int randomMilli = withMillis ? randomInt(999) : 0;
        String millisString = withMillis && randomMilli > 0 ? Strings.format(".%03d", randomMilli) : "";

        String value = format(Locale.ROOT, "%s%d:%d%s", sign, randomMinute, randomSecond, millisString);
        TemporalAmount amount = parseInterval(EMPTY, value, INTERVAL_MINUTE_TO_SECOND);
        assertEquals(maybeNegate(sign, Duration.ofMinutes(randomMinute).plusSeconds(randomSecond).plusMillis(randomMilli)), amount);
    }

    // validation
    public void testYearToMonthTooBig() throws Exception {
        int randomYear = randomNonNegativeInt();
        int randomTooBig = randomIntBetween(12, 9999);
        String value = format(Locale.ROOT, "%s%d-%d", sign, randomYear, randomTooBig);
        ParsingException pe = expectThrows(ParsingException.class, () -> parseInterval(EMPTY, value, INTERVAL_YEAR_TO_MONTH));
        assertEquals(
            "line -1:0: Invalid [INTERVAL YEAR TO MONTH] value ["
                + value
                + "]: [MONTH] unit has illegal value ["
                + randomTooBig
                + "], expected a positive number up to [11]",
            pe.getMessage()
        );
    }

    public void testMillisTooBig() throws Exception {
        int randomSeconds = randomNonNegativeInt();
        int millisTooLarge = 1234;
        String value = format(Locale.ROOT, "%s%d.%d", sign, randomSeconds, millisTooLarge);
        ParsingException pe = expectThrows(ParsingException.class, () -> parseInterval(EMPTY, value, INTERVAL_SECOND));
        assertEquals(
            "line -1:0: Invalid [INTERVAL SECOND] value ["
                + value
                + "]: [MILLISECOND] unit has illegal value ["
                + millisTooLarge
                + "], expected a positive number up to [999]",
            pe.getMessage()
        );
    }

    public void testDayToMinuteTooBig() throws Exception {
        int randomDay = randomNonNegativeInt();
        int randomHour = randomIntBetween(24, 9999);
        int randomMinute = randomInt(59);
        String value = format(Locale.ROOT, "%s%d %d:%d", sign, randomDay, randomHour, randomMinute);
        ParsingException pe = expectThrows(ParsingException.class, () -> parseInterval(EMPTY, value, INTERVAL_DAY_TO_MINUTE));
        assertEquals(
            "line -1:0: Invalid [INTERVAL DAY TO MINUTE] value ["
                + value
                + "]: [HOUR] unit has illegal value ["
                + randomHour
                + "], expected a positive number up to [23]",
            pe.getMessage()
        );
    }

    public void testIncompleteYearToMonthInterval() throws Exception {
        String value = "123-";
        ParsingException pe = expectThrows(ParsingException.class, () -> parseInterval(EMPTY, value, INTERVAL_YEAR_TO_MONTH));
        assertEquals(
            "line -1:0: Invalid [INTERVAL YEAR TO MONTH] value [123-]: incorrect format, expecting [numeric]-[numeric]",
            pe.getMessage()
        );
    }

    public void testIncompleteDayToHourInterval() throws Exception {
        String value = "123 23:";
        ParsingException pe = expectThrows(ParsingException.class, () -> parseInterval(EMPTY, value, INTERVAL_DAY_TO_HOUR));
        assertEquals(
            "line -1:0: Invalid [INTERVAL DAY TO HOUR] value [123 23:]: unexpected trailing characters found [:]",
            pe.getMessage()
        );
    }

    public void testExtraCharLeading() throws Exception {
        String value = "a123";
        ParsingException pe = expectThrows(ParsingException.class, () -> parseInterval(EMPTY, value, INTERVAL_YEAR));
        assertEquals("line -1:0: Invalid [INTERVAL YEAR] value [a123]: expected digit (at [0]) but found [a]", pe.getMessage());
    }

    public void testExtraCharTrailing() throws Exception {
        String value = "123x";
        ParsingException pe = expectThrows(ParsingException.class, () -> parseInterval(EMPTY, value, INTERVAL_YEAR));
        assertEquals("line -1:0: Invalid [INTERVAL YEAR] value [123x]: unexpected trailing characters found [x]", pe.getMessage());
    }

    public void testIncorrectSeparator() throws Exception {
        String value = "123^456";
        ParsingException pe = expectThrows(ParsingException.class, () -> parseInterval(EMPTY, value, INTERVAL_SECOND));
        assertEquals("line -1:0: Invalid [INTERVAL SECOND] value [123^456]: expected [.] (at [3]) but found [^]", pe.getMessage());
    }

    public void testOfValueTooLarge() throws Exception {
        ParsingException pe = expectThrows(ParsingException.class, () -> of(EMPTY, Long.MAX_VALUE, TimeUnit.YEAR));
        assertEquals("line -1:0: Value [9223372036854775807] cannot be used as it is too large to convert into [YEAR]s", pe.getMessage());
    }

    public void testIntervalType() throws Exception {
        ParsingException pe = expectThrows(ParsingException.class, () -> intervalType(EMPTY, TimeUnit.DAY, TimeUnit.YEAR));
        assertEquals("line -1:0: Cannot determine datatype for combination [DAY] [YEAR]", pe.getMessage());
    }

    private Object maybeNegate(String sign, TemporalAmount interval) {
        return "-".equals(sign) ? Intervals.negate(interval) : interval;
    }
}
