/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.literal.IntervalDayTime;
import org.elasticsearch.xpack.ql.expression.literal.IntervalYearMonth;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.util.DateUtils;

import java.time.Duration;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAmount;

import static org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Arithmetics.mod;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;
import static org.elasticsearch.xpack.ql.type.DataType.INTERVAL_DAY;
import static org.elasticsearch.xpack.ql.type.DataType.INTERVAL_DAY_TO_HOUR;
import static org.elasticsearch.xpack.ql.type.DataType.INTERVAL_HOUR;
import static org.elasticsearch.xpack.ql.type.DataType.INTERVAL_MONTH;
import static org.elasticsearch.xpack.ql.type.DataType.INTERVAL_YEAR;
import static org.elasticsearch.xpack.ql.type.DataType.INTERVAL_YEAR_TO_MONTH;

public class BinaryArithmeticTests extends ESTestCase {

    public void testAddNumbers() {
        assertEquals(Long.valueOf(3), add(1L, 2L));
    }

    public void testMod() {
        assertEquals(2, mod(10, 8));
        assertEquals(2, mod(10, -8));
        assertEquals(-2, mod(-10, 8));
        assertEquals(-2, mod(-10, -8));

        assertEquals(2L, mod(10L, 8));
        assertEquals(2L, mod(10, -8L));
        assertEquals(-2L, mod(-10L, 8L));
        assertEquals(-2L, mod(-10L, -8L));

        assertEquals(2.3000002f, mod(10.3f, 8L));
        assertEquals(1.5f, mod(10, -8.5f));
        assertEquals(-1.8000002f, mod(-10.3f, 8.5f));
        assertEquals(-1.8000002f, mod(-10.3f, -8.5f));

        assertEquals(2.3000000000000007d, mod(10.3d, 8L));
        assertEquals(1.5d, mod(10, -8.5d));
        assertEquals(-1.8000001907348633d, mod(-10.3f, 8.5d));
        assertEquals(-1.8000000000000007, mod(-10.3d, -8.5d));
    }

    public void testAddYearMonthIntervals() {
        Literal l = interval(Period.ofYears(1), INTERVAL_YEAR);
        Literal r = interval(Period.ofMonths(2), INTERVAL_MONTH);
        IntervalYearMonth x = add(l, r);
        assertEquals(interval(Period.ofYears(1).plusMonths(2), INTERVAL_YEAR_TO_MONTH), L(x));
    }

    public void testAddYearMonthMixedIntervals() {
        Literal l = interval(Period.ofYears(1).plusMonths(5), INTERVAL_YEAR_TO_MONTH);
        Literal r = interval(Period.ofMonths(2), INTERVAL_MONTH);
        IntervalYearMonth x = add(l, r);
        assertEquals(interval(Period.ofYears(1).plusMonths(7), INTERVAL_YEAR_TO_MONTH), L(x));
    }

    public void testAddDayTimeIntervals() {
        Literal l = interval(Duration.ofDays(1), INTERVAL_DAY);
        Literal r = interval(Duration.ofHours(2), INTERVAL_HOUR);
        IntervalDayTime x = add(l, r);
        assertEquals(interval(Duration.ofDays(1).plusHours(2), INTERVAL_DAY_TO_HOUR), L(x));
    }

    public void testAddYearMonthIntervalToDateTime() {
        ZonedDateTime now = ZonedDateTime.now(DateUtils.UTC);
        Literal l = L(now);
        TemporalAmount t = Period.ofYears(100).plusMonths(50);
        Literal r = interval(t, INTERVAL_HOUR);
        ZonedDateTime x = add(l, r);
        assertEquals(L(now.plus(t)), L(x));
    }

    public void testAddDayTimeIntervalToDateTime() {
        ZonedDateTime now = ZonedDateTime.now(DateUtils.UTC);
        Literal l = L(now);
        TemporalAmount t = Duration.ofHours(2);
        Literal r = interval(Duration.ofHours(2), INTERVAL_HOUR);
        ZonedDateTime x = add(l, r);
        assertEquals(L(now.plus(t)), L(x));
    }

    public void testAddDayTimeIntervalToDateTimeReverse() {
        ZonedDateTime now = ZonedDateTime.now(DateUtils.UTC);
        Literal l = L(now);
        TemporalAmount t = Duration.ofHours(2);
        Literal r = interval(Duration.ofHours(2), INTERVAL_HOUR);
        ZonedDateTime x = add(r, l);
        assertEquals(L(now.plus(t)), L(x));
    }

    public void testAddYearMonthIntervalToTime() {
        OffsetTime now = OffsetTime.now(DateUtils.UTC);
        Literal l = L(now);
        TemporalAmount t = Period.ofYears(100).plusMonths(50);
        Literal r = interval(t, INTERVAL_HOUR);
        OffsetTime x = add(l, r);
        assertEquals(L(now), L(x));
    }

    public void testAddDayTimeIntervalToTime() {
        OffsetTime now = OffsetTime.now(DateUtils.UTC);
        Literal l = L(now);
        TemporalAmount t = Duration.ofHours(32);
        Literal r = interval(Duration.ofHours(32), INTERVAL_HOUR);
        OffsetTime x = add(l, r);
        assertEquals(L(now.plus(t)), L(x));
    }

    public void testAddDayTimeIntervalToTimeReverse() {
        OffsetTime now = OffsetTime.now(DateUtils.UTC);
        Literal l = L(now);
        TemporalAmount t = Duration.ofHours(45);
        Literal r = interval(Duration.ofHours(45), INTERVAL_HOUR);
        OffsetTime x = add(r, l);
        assertEquals(L(now.plus(t)), L(x));
    }

    public void testAddNumberToIntervalIllegal() {
        Literal r = interval(Duration.ofHours(2), INTERVAL_HOUR);
        QlIllegalArgumentException expect = expectThrows(QlIllegalArgumentException.class, () -> add(r, L(1)));
        assertEquals("Cannot compute [+] between [IntervalDayTime] [Integer]", expect.getMessage());
    }

    public void testSubYearMonthIntervals() {
        Literal l = interval(Period.ofYears(1), INTERVAL_YEAR);
        Literal r = interval(Period.ofMonths(2), INTERVAL_MONTH);
        IntervalYearMonth x = sub(l, r);
        assertEquals(interval(Period.ofMonths(10), INTERVAL_YEAR_TO_MONTH), L(x));
    }

    public void testSubDayTimeIntervals() {
        Literal l = interval(Duration.ofDays(1).plusHours(10), INTERVAL_DAY_TO_HOUR);
        Literal r = interval(Duration.ofHours(2), INTERVAL_HOUR);
        IntervalDayTime x = sub(l, r);
        assertEquals(interval(Duration.ofDays(1).plusHours(8), INTERVAL_DAY_TO_HOUR), L(x));
    }

    public void testSubYearMonthIntervalToDateTime() {
        ZonedDateTime now = ZonedDateTime.now(DateUtils.UTC);
        Literal l = L(now);
        TemporalAmount t = Period.ofYears(100).plusMonths(50);
        Literal r = interval(t, INTERVAL_HOUR);
        ZonedDateTime x = sub(l, r);
        assertEquals(L(now.minus(t)), L(x));
    }

    public void testSubYearMonthIntervalToDateTimeIllegal() {
        ZonedDateTime now = ZonedDateTime.now(DateUtils.UTC);
        Literal l = L(now);
        TemporalAmount t = Period.ofYears(100).plusMonths(50);
        Literal r = interval(t, INTERVAL_HOUR);
        QlIllegalArgumentException ex = expectThrows(QlIllegalArgumentException.class, () -> sub(r, l));
        assertEquals("Cannot subtract a date from an interval; do you mean the reverse?", ex.getMessage());
    }

    public void testSubDayTimeIntervalToDateTime() {
        ZonedDateTime now = ZonedDateTime.now(DateUtils.UTC);
        Literal l = L(now);
        TemporalAmount t = Duration.ofHours(2);
        Literal r = interval(Duration.ofHours(2), INTERVAL_HOUR);
        ZonedDateTime x = sub(l, r);
        assertEquals(L(now.minus(t)), L(x));
    }

    public void testSubYearMonthIntervalToTime() {
        OffsetTime now = OffsetTime.now(DateUtils.UTC);
        Literal l = L(now);
        TemporalAmount t = Period.ofYears(100).plusMonths(50);
        Literal r = interval(t, INTERVAL_HOUR);
        OffsetTime x = sub(l, r);
        assertEquals(L(now), L(x));
    }

    public void testSubYearMonthIntervalToTimeIllegal() {
        OffsetTime now = OffsetTime.now(DateUtils.UTC);
        Literal l = L(now);
        TemporalAmount t = Period.ofYears(100).plusMonths(50);
        Literal r = interval(t, INTERVAL_HOUR);
        QlIllegalArgumentException ex = expectThrows(QlIllegalArgumentException.class, () -> sub(r, l));
        assertEquals("Cannot subtract a date from an interval; do you mean the reverse?", ex.getMessage());
    }

    public void testSubDayTimeIntervalToTime() {
        OffsetTime now = OffsetTime.now(DateUtils.UTC);
        Literal l = L(now);
        TemporalAmount t = Duration.ofHours(36);
        Literal r = interval(Duration.ofHours(36), INTERVAL_HOUR);
        OffsetTime x = sub(l, r);
        assertEquals(L(now.minus(t)), L(x));
    }

    public void testSubNumberFromIntervalIllegal() {
        Literal r = interval(Duration.ofHours(2), INTERVAL_HOUR);
        QlIllegalArgumentException expect = expectThrows(QlIllegalArgumentException.class, () -> sub(r, L(1)));
        assertEquals("Cannot compute [-] between [IntervalDayTime] [Integer]", expect.getMessage());
    }

    public void testMulIntervalNumber() {
        Literal l = interval(Duration.ofHours(2), INTERVAL_HOUR);
        IntervalDayTime interval = mul(l, -1);
        assertEquals(INTERVAL_HOUR, interval.dataType());
        Duration p = interval.interval();
        assertEquals(Duration.ofHours(2).negated(), p);
    }

    public void testMulNumberInterval() {
        Literal r = interval(Period.ofYears(1), INTERVAL_YEAR);
        IntervalYearMonth interval = mul(-2, r);
        assertEquals(INTERVAL_YEAR, interval.dataType());
        Period p = interval.interval();
        assertEquals(Period.ofYears(2).negated(), p);
    }
    
    public void testMulNullInterval() {
        Literal literal = interval(Period.ofMonths(1), INTERVAL_MONTH);
        Mul result = new Mul(EMPTY, L(null), literal);
        assertTrue(result.foldable());
        assertNull(result.fold());
        assertEquals(INTERVAL_MONTH, result.dataType());
        
        result = new Mul(EMPTY, literal, L(null));
        assertTrue(result.foldable());
        assertNull(result.fold());
        assertEquals(INTERVAL_MONTH, result.dataType());
    }

    public void testAddNullInterval() {
        Literal literal = interval(Period.ofMonths(1), INTERVAL_MONTH);
        Add result = new Add(EMPTY, L(null), literal);
        assertTrue(result.foldable());
        assertNull(result.fold());
        assertEquals(INTERVAL_MONTH, result.dataType());
        
        result = new Add(EMPTY, literal, L(null));
        assertTrue(result.foldable());
        assertNull(result.fold());
        assertEquals(INTERVAL_MONTH, result.dataType());
    }

    public void testSubNullInterval() {
        Literal literal = interval(Period.ofMonths(1), INTERVAL_MONTH);
        Sub result = new Sub(EMPTY, L(null), literal);
        assertTrue(result.foldable());
        assertNull(result.fold());
        assertEquals(INTERVAL_MONTH, result.dataType());
        
        result = new Sub(EMPTY, literal, L(null));
        assertTrue(result.foldable());
        assertNull(result.fold());
        assertEquals(INTERVAL_MONTH, result.dataType());
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

    @SuppressWarnings("unchecked")
    private static <T> T mul(Object l, Object r) {
        Mul mul = new Mul(EMPTY, L(l), L(r));
        assertTrue(mul.foldable());
        return (T) mul.fold();
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
