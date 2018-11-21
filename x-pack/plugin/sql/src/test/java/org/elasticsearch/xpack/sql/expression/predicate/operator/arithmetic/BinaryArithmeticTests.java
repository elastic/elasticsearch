/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.expression.literal.IntervalDayTime;
import org.elasticsearch.xpack.sql.expression.literal.IntervalYearMonth;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.util.DateUtils;

import java.time.Duration;
import java.time.Period;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAmount;

import static org.elasticsearch.xpack.sql.tree.Location.EMPTY;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_DAY;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_DAY_TO_HOUR;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_HOUR;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_MONTH;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_YEAR;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_YEAR_TO_MONTH;

public class BinaryArithmeticTests extends ESTestCase {

    public void testAddNumbers() throws Exception {
        assertEquals(Long.valueOf(3), add(1L, 2L));
    }

    public void testAddYearMonthIntervals() throws Exception {
        Literal l = interval(Period.ofYears(1), INTERVAL_YEAR);
        Literal r = interval(Period.ofMonths(2), INTERVAL_MONTH);
        IntervalYearMonth x = add(l, r);
        assertEquals(interval(Period.ofYears(1).plusMonths(2), INTERVAL_YEAR_TO_MONTH), L(x));
    }

    public void testAddYearMonthMixedIntervals() throws Exception {
        Literal l = interval(Period.ofYears(1).plusMonths(5), INTERVAL_YEAR_TO_MONTH);
        Literal r = interval(Period.ofMonths(2), INTERVAL_MONTH);
        IntervalYearMonth x = add(l, r);
        assertEquals(interval(Period.ofYears(1).plusMonths(7), INTERVAL_YEAR_TO_MONTH), L(x));
    }

    public void testAddDayTimeIntervals() throws Exception {
        Literal l = interval(Duration.ofDays(1), INTERVAL_DAY);
        Literal r = interval(Duration.ofHours(2), INTERVAL_HOUR);
        IntervalDayTime x = add(l, r);
        assertEquals(interval(Duration.ofDays(1).plusHours(2), INTERVAL_DAY_TO_HOUR), L(x));
    }

    public void testAddYearMonthIntervalToDate() throws Exception {
        ZonedDateTime now = ZonedDateTime.now(DateUtils.UTC);
        Literal l = L(now);
        TemporalAmount t = Period.ofYears(100).plusMonths(50);
        Literal r = interval(t, INTERVAL_HOUR);
        ZonedDateTime x = add(l, r);
        assertEquals(L(now.plus(t)), L(x));
    }

    public void testAddDayTimeIntervalToDate() throws Exception {
        ZonedDateTime now = ZonedDateTime.now(DateUtils.UTC);
        Literal l = L(now);
        TemporalAmount t = Duration.ofHours(2);
        Literal r = interval(Duration.ofHours(2), INTERVAL_HOUR);
        ZonedDateTime x = add(l, r);
        assertEquals(L(now.plus(t)), L(x));
    }

    public void testAddDayTimeIntervalToDateReverse() throws Exception {
        ZonedDateTime now = ZonedDateTime.now(DateUtils.UTC);
        Literal l = L(now);
        TemporalAmount t = Duration.ofHours(2);
        Literal r = interval(Duration.ofHours(2), INTERVAL_HOUR);
        ZonedDateTime x = add(r, l);
        assertEquals(L(now.plus(t)), L(x));
    }

    public void testAddNumberToIntervalIllegal() throws Exception {
        Literal r = interval(Duration.ofHours(2), INTERVAL_HOUR);
        SqlIllegalArgumentException expect = expectThrows(SqlIllegalArgumentException.class, () -> add(r, L(1)));
        assertEquals("Cannot compute [+] between [IntervalDayTime] [Integer]", expect.getMessage());
    }

    public void testSubYearMonthIntervals() throws Exception {
        Literal l = interval(Period.ofYears(1), INTERVAL_YEAR);
        Literal r = interval(Period.ofMonths(2), INTERVAL_MONTH);
        IntervalYearMonth x = sub(l, r);
        assertEquals(interval(Period.ofMonths(10), INTERVAL_YEAR_TO_MONTH), L(x));
    }

    public void testSubDayTimeIntervals() throws Exception {
        Literal l = interval(Duration.ofDays(1).plusHours(10), INTERVAL_DAY_TO_HOUR);
        Literal r = interval(Duration.ofHours(2), INTERVAL_HOUR);
        IntervalDayTime x = sub(l, r);
        assertEquals(interval(Duration.ofDays(1).plusHours(8), INTERVAL_DAY_TO_HOUR), L(x));
    }

    public void testSubYearMonthIntervalToDate() throws Exception {
        ZonedDateTime now = ZonedDateTime.now(DateUtils.UTC);
        Literal l = L(now);
        TemporalAmount t = Period.ofYears(100).plusMonths(50);
        Literal r = interval(t, INTERVAL_HOUR);
        ZonedDateTime x = sub(l, r);
        assertEquals(L(now.minus(t)), L(x));
    }

    public void testSubYearMonthIntervalToDateIllegal() throws Exception {
        ZonedDateTime now = ZonedDateTime.now(DateUtils.UTC);
        Literal l = L(now);
        TemporalAmount t = Period.ofYears(100).plusMonths(50);
        Literal r = interval(t, INTERVAL_HOUR);
        SqlIllegalArgumentException ex = expectThrows(SqlIllegalArgumentException.class, () -> sub(r, l));
        assertEquals("Cannot substract a date from an interval; do you mean the reverse?", ex.getMessage());
    }

    public void testSubNumberFromIntervalIllegal() throws Exception {
        Literal r = interval(Duration.ofHours(2), INTERVAL_HOUR);
        SqlIllegalArgumentException expect = expectThrows(SqlIllegalArgumentException.class, () -> sub(r, L(1)));
        assertEquals("Cannot compute [-] between [IntervalDayTime] [Integer]", expect.getMessage());
    }

    public void testSubDayTimeIntervalToDate() throws Exception {
        ZonedDateTime now = ZonedDateTime.now(DateUtils.UTC);
        Literal l = L(now);
        TemporalAmount t = Duration.ofHours(2);
        Literal r = interval(Duration.ofHours(2), INTERVAL_HOUR);
        ZonedDateTime x = sub(l, r);
        assertEquals(L(now.minus(t)), L(x));
    }

    @SuppressWarnings("unchecked")
    private static <T> T add(Object l, Object r) {
        Add add = new Add(EMPTY, L(l), L(r));
        assertTrue(add.foldable());
        return (T) add.fold();
    }

    @SuppressWarnings("unchecked")
    private static <T> T sub(Object l, Object r) {
        Sub sub = new Sub(EMPTY, L(l), L(r));
        assertTrue(sub.foldable());
        return (T) sub.fold();
    }


    private static Literal L(Object value) {
        return Literal.of(EMPTY, value);
    }

    private static Literal interval(TemporalAmount value, DataType intervalType) {
        Object i = value instanceof Period ? new IntervalYearMonth((Period) value, intervalType)
                 : new IntervalDayTime((Duration) value, intervalType);
        return Literal.of(EMPTY, i);
    }
}