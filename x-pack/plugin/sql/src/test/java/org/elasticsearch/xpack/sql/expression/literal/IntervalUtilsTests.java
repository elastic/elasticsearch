/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.literal;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.expression.literal.IntervalUtils.TimeUnit;
import org.elasticsearch.xpack.sql.parser.ParsingException;

import java.time.Duration;
import java.time.Period;
import java.time.temporal.TemporalAmount;
import java.util.Locale;

import static java.lang.String.format;
import static org.elasticsearch.xpack.sql.expression.literal.IntervalUtils.intervalType;
import static org.elasticsearch.xpack.sql.expression.literal.IntervalUtils.of;
import static org.elasticsearch.xpack.sql.expression.literal.IntervalUtils.parseInterval;
import static org.elasticsearch.xpack.sql.tree.Location.EMPTY;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_DAY;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_DAY_TO_HOUR;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_DAY_TO_MINUTE;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_DAY_TO_SECOND;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_HOUR;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_HOUR_TO_MINUTE;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_HOUR_TO_SECOND;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_MINUTE;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_MINUTE_TO_SECOND;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_MONTH;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_SECOND;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_YEAR;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_YEAR_TO_MONTH;

public class IntervalUtilsTests extends ESTestCase {

    public void testYearInterval() throws Exception {
        String randomSign = randomSign();
        int random = randomNonNegativeInt();
        TemporalAmount amount = parseInterval(EMPTY, randomSign + random, INTERVAL_YEAR);
        assertEquals(maybeNegate(randomSign, Period.ofYears(random)), amount);
    }

    public void testMonthInterval() throws Exception {
        String randomSign = randomSign();
        int random = randomNonNegativeInt();
        TemporalAmount amount = parseInterval(EMPTY, randomSign + random, INTERVAL_MONTH);
        assertEquals(maybeNegate(randomSign, Period.ofMonths(random)), amount);
    }

    public void testDayInterval() throws Exception {
        String randomSign = randomSign();
        int random = randomNonNegativeInt();
        TemporalAmount amount = parseInterval(EMPTY, randomSign + random, INTERVAL_DAY);
        assertEquals(maybeNegate(randomSign, Duration.ofDays(random)), amount);
    }

    public void testHourInterval() throws Exception {
        String randomSign = randomSign();
        int random = randomNonNegativeInt();
        TemporalAmount amount = parseInterval(EMPTY, randomSign + random, INTERVAL_HOUR);
        assertEquals(maybeNegate(randomSign, Duration.ofHours(random)), amount);
    }

    public void testMinuteInterval() throws Exception {
        String randomSign = randomSign();
        int random = randomNonNegativeInt();
        TemporalAmount amount = parseInterval(EMPTY, randomSign + random, INTERVAL_MINUTE);
        assertEquals(maybeNegate(randomSign, Duration.ofMinutes(random)), amount);
    }

    public void testSecondInterval() throws Exception {
        String randomSign = randomSign();
        int randomSeconds = randomNonNegativeInt();
        int randomMillis = randomInt(999999999);
        String value = format(Locale.ROOT, "%s%d.%d", randomSign, randomSeconds, randomMillis);
        TemporalAmount amount = parseInterval(EMPTY, value, INTERVAL_SECOND);
        assertEquals(maybeNegate(randomSign, Duration.ofSeconds(randomSeconds).plusMillis(randomMillis)), amount);
    }

    public void testYearToMonth() throws Exception {
        String randomSign = randomSign();
        int randomYear = randomNonNegativeInt();
        int randomMonth = randomInt(11);
        String value = format(Locale.ROOT, "%s%d-%d", randomSign, randomYear, randomMonth);
        TemporalAmount amount = parseInterval(EMPTY, value, INTERVAL_YEAR_TO_MONTH);
        assertEquals(maybeNegate(randomSign, Period.ofYears(randomYear).plusMonths(randomMonth)), amount);
    }

    public void testDayToHour() throws Exception {
        String randomSign = randomSign();
        int randomDay = randomNonNegativeInt();
        int randomHour = randomInt(23);
        String value = format(Locale.ROOT, "%s%d %d", randomSign, randomDay, randomHour);
        TemporalAmount amount = parseInterval(EMPTY, value, INTERVAL_DAY_TO_HOUR);
        assertEquals(maybeNegate(randomSign, Duration.ofDays(randomDay).plusHours(randomHour)), amount);
    }

    public void testDayToMinute() throws Exception {
        String randomSign = randomSign();
        int randomDay = randomNonNegativeInt();
        int randomHour = randomInt(23);
        int randomMinute = randomInt(59);
        String value = format(Locale.ROOT, "%s%d %d:%d", randomSign, randomDay, randomHour, randomMinute);
        TemporalAmount amount = parseInterval(EMPTY, value, INTERVAL_DAY_TO_MINUTE);
        assertEquals(maybeNegate(randomSign, Duration.ofDays(randomDay).plusHours(randomHour).plusMinutes(randomMinute)), amount);
    }

    public void testDayToSecond() throws Exception {
        String randomSign = randomSign();
        int randomDay = randomNonNegativeInt();
        int randomHour = randomInt(23);
        int randomMinute = randomInt(59);
        int randomSecond = randomInt(59);
        int randomMilli = randomInt(999999999);

        String value = format(Locale.ROOT, "%s%d %d:%d:%d.%d", randomSign, randomDay, randomHour, randomMinute, randomSecond,
                randomMilli);
        TemporalAmount amount = parseInterval(EMPTY, value, INTERVAL_DAY_TO_SECOND);
        assertEquals(maybeNegate(randomSign, Duration.ofDays(randomDay).plusHours(randomHour).plusMinutes(randomMinute)
                .plusSeconds(randomSecond).plusMillis(randomMilli)), amount);
    }

    public void testHourToMinute() throws Exception {
        String randomSign = randomSign();
        int randomHour = randomNonNegativeInt();
        int randomMinute = randomInt(59);
        String value = format(Locale.ROOT, "%s%d:%d", randomSign, randomHour, randomMinute);
        TemporalAmount amount = parseInterval(EMPTY, value, INTERVAL_HOUR_TO_MINUTE);
        assertEquals(maybeNegate(randomSign, Duration.ofHours(randomHour).plusMinutes(randomMinute)), amount);
    }

    public void testHourToSecond() throws Exception {
        String randomSign = randomSign();
        int randomHour = randomNonNegativeInt();
        int randomMinute = randomInt(59);
        int randomSecond = randomInt(59);
        int randomMilli = randomInt(999999999);

        String value = format(Locale.ROOT, "%s%d:%d:%d.%d", randomSign, randomHour, randomMinute, randomSecond, randomMilli);
        TemporalAmount amount = parseInterval(EMPTY, value, INTERVAL_HOUR_TO_SECOND);
        assertEquals(maybeNegate(randomSign,
                Duration.ofHours(randomHour).plusMinutes(randomMinute).plusSeconds(randomSecond).plusMillis(randomMilli)), amount);
    }

    public void testMinuteToSecond() throws Exception {
        String randomSign = randomSign();
        int randomMinute = randomNonNegativeInt();
        int randomSecond = randomInt(59);
        int randomMilli = randomInt(999999999);

        String value = format(Locale.ROOT, "%s%d:%d.%d", randomSign, randomMinute, randomSecond, randomMilli);
        TemporalAmount amount = parseInterval(EMPTY, value, INTERVAL_MINUTE_TO_SECOND);
        assertEquals(maybeNegate(randomSign, Duration.ofMinutes(randomMinute).plusSeconds(randomSecond).plusMillis(randomMilli)), amount);
    }


    // validation
    public void testYearToMonthTooBig() throws Exception {
        String randomSign = randomSign();
        int randomYear = randomNonNegativeInt();
        int randomTooBig = randomIntBetween(12, 9999);
        String value = format(Locale.ROOT, "%s%d-%d", randomSign, randomYear, randomTooBig);
        ParsingException pe = expectThrows(ParsingException.class,
                () -> parseInterval(EMPTY, value, INTERVAL_YEAR_TO_MONTH));
        assertEquals("line -1:0: Invalid [INTERVAL YEAR TO MONTH] value [" + value + "]: [MONTH] unit has illegal value [" + randomTooBig
                + "], expected a positive number up to [11]", pe.getMessage());
    }

    public void testMillisTooBig() throws Exception {
        String randomSign = randomSign();
        int randomSeconds = randomNonNegativeInt();
        int millisTooLarge = 1234567890;
        String value = format(Locale.ROOT, "%s%d.%d", randomSign, randomSeconds, millisTooLarge);
        ParsingException pe = expectThrows(ParsingException.class,
                () -> parseInterval(EMPTY, value, INTERVAL_SECOND));
        assertEquals("line -1:0: Invalid [INTERVAL SECOND] value [" + value + "]: [MILLISECOND] unit has illegal value [" + millisTooLarge
                + "], expected a positive number up to [999999999]", pe.getMessage());
    }

    public void testDayToMinuteTooBig() throws Exception {
        String randomSign = randomSign();
        int randomDay = randomNonNegativeInt();
        int randomHour = randomIntBetween(24, 9999);
        int randomMinute = randomInt(59);
        String value = format(Locale.ROOT, "%s%d %d:%d", randomSign, randomDay, randomHour, randomMinute);
        ParsingException pe = expectThrows(ParsingException.class, () -> parseInterval(EMPTY, value, INTERVAL_DAY_TO_MINUTE));
        assertEquals("line -1:0: Invalid [INTERVAL DAY TO MINUTE] value [" + value + "]: [HOUR] unit has illegal value [" + randomHour
                + "], expected a positive number up to [23]", pe.getMessage());
    }

    public void testExtraCharLeading() throws Exception {
        String value = "a123";
        ParsingException pe = expectThrows(ParsingException.class, () -> parseInterval(EMPTY, value, INTERVAL_YEAR));
        assertEquals("line -1:0: Invalid [INTERVAL YEAR] value [a123]: expected digit (at [0]) but found [a]", pe.getMessage());
    }

    public void testExtraCharTrailing() throws Exception {
        String value = "123x";
        ParsingException pe = expectThrows(ParsingException.class,
                () -> parseInterval(EMPTY, value, INTERVAL_YEAR));
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

    private static int randomNonNegativeInt() {
        int random = randomInt();
        return random == Integer.MIN_VALUE ? 0 : Math.abs(random);
    }

    //Maybe returns a sign, which might be + or -.
    private static String randomSign() {
        return randomBoolean() ? (randomBoolean() ? "+" : "-") : "";
    }

    private Object maybeNegate(String randomSign, TemporalAmount interval) {
        return "-".equals(randomSign) ? IntervalUtils.negate(interval) : interval;
    }
}