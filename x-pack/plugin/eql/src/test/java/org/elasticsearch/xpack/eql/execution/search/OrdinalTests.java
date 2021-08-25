/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.elasticsearch.test.ESTestCase;

import java.math.BigDecimal;

@SuppressWarnings({"unchecked", "rawtypes"})
public class OrdinalTests extends ESTestCase {

    public void testCompareToDifferentTs() {
        Number ts1 = randomOrdinalNumber();
        Number ts2 = randomValueOtherThan(ts1, () -> randomOrdinalNumber(ts1));
        Ordinal one = new Ordinal(ts1, (Comparable) randomLong(), randomLong());
        Ordinal two = new Ordinal(ts2, (Comparable) randomLong(), randomLong());

        assertEquals(compareOrdinalNumbers(one.timestamp(), two.timestamp()), one.compareTo(two));
    }

    public void testCompareToSameTsDifferentTie() {
        Number ts = randomOrdinalNumber();
        Number tie1 = randomOrdinalNumber();
        Number tie2 = randomValueOtherThan(tie1, () -> randomOrdinalNumber(tie1));
        Ordinal one = new Ordinal(ts, (Comparable) tie1, randomLong());
        Ordinal two = new Ordinal(ts, (Comparable) tie2, randomLong());

        assertEquals(one.tiebreaker().compareTo(two.tiebreaker()), one.compareTo(two));
    }

    public void testCompareToSameTsOneTieNull() {
        Number ts = randomOrdinalNumber();
        Ordinal one = new Ordinal(ts, (Comparable) randomOrdinalNumber(), randomOrdinalNumber());
        Ordinal two = new Ordinal(ts, null, randomOrdinalNumber());

        assertEquals(-1, one.compareTo(two));
    }

    public void testCompareToSameTsSameTieSameImplicitTb() {
        Number ts = randomOrdinalNumber();
        Comparable c = (Comparable) randomOrdinalNumber();
        Number implicitTb = randomOrdinalNumber();
        Ordinal one = new Ordinal(ts, c, implicitTb);
        Ordinal two = new Ordinal(ts, c, implicitTb);

        assertEquals(0, one.compareTo(two));
        assertEquals(0, one.compareTo(one));
        assertEquals(0, two.compareTo(two));
    }

    public void testCompareToSameTsSameTieDifferentImplicitTb() {
        Number ts = randomOrdinalNumber();
        Comparable c = (Comparable) randomOrdinalNumber();
        Number implicitTb = randomOrdinalNumber();
        Ordinal one = new Ordinal(ts, c, implicitTb);
        Ordinal two = new Ordinal(ts, c, randomValueOtherThan(implicitTb, () -> randomOrdinalNumber(implicitTb)));

        assertEquals(compareOrdinalNumbers(one.implicitTiebreaker(), two.implicitTiebreaker()), one.compareTo(two));
    }

    public void testCompareToSameTsSameTieNullSameImplicitTb() {
        Number ts = randomOrdinalNumber();
        Number implicitTb = randomOrdinalNumber();
        Ordinal one = new Ordinal(ts, null, implicitTb);
        Ordinal two = new Ordinal(ts, null, implicitTb);

        assertEquals(0, one.compareTo(two));
        assertEquals(0, one.compareTo(one));
        assertEquals(0, two.compareTo(two));
    }

    public void testCompareToSameTsSameTieNullDifferentImplicitTb() {
        Number ts = randomOrdinalNumber();
        Number implicitTb1 = randomOrdinalNumber();
        Number implicitTb2 = randomValueOtherThan(implicitTb1, () -> randomOrdinalNumber(implicitTb1));
        Ordinal one = new Ordinal(ts, null, implicitTb1);
        Ordinal two = new Ordinal(ts, null, implicitTb2);

        assertEquals(compareOrdinalNumbers(one.implicitTiebreaker(), two.implicitTiebreaker()), one.compareTo(two));
    }

    public void testTestBetween() {
        Ordinal before = new Ordinal(randomOrdinalNumberBetween(1000, 2000), (Comparable) randomOrdinalNumber(), randomOrdinalNumber());
        Ordinal between = new Ordinal(randomOrdinalNumberBetween(3000, 4000, before.timestamp()),
            (Comparable) randomOrdinalNumber(before.tiebreaker()), randomOrdinalNumber(before.implicitTiebreaker()));
        Ordinal after = new Ordinal(randomOrdinalNumberBetween(5000, 6000, before.timestamp()),
            (Comparable) randomOrdinalNumber(before.tiebreaker()), randomOrdinalNumber(before.implicitTiebreaker()));

        assertTrue(before.between(before, after));
        assertTrue(after.between(before, after));
        assertTrue(between.between(before, after));

        Ordinal beforeBefore = new Ordinal(randomOrdinalNumberBetween(0, 999, before.timestamp()), null,
            randomOrdinalNumber(before.implicitTiebreaker()));
        Ordinal afterAfter = new Ordinal(randomOrdinalNumberBetween(7000, 8000, before.timestamp()), null,
            randomOrdinalNumber(before.implicitTiebreaker()));

        assertFalse(beforeBefore.between(before, after));
        assertFalse(afterAfter.between(before, after));
    }

    public void testTestBefore() {
        Ordinal before = new Ordinal(randomOrdinalNumberBetween(1000, 2000), (Comparable) randomOrdinalNumber(), randomOrdinalNumber());
        Ordinal after = new Ordinal(randomOrdinalNumberBetween(5000, 6000, before.timestamp()),
            (Comparable) randomOrdinalNumber(before.tiebreaker()), randomOrdinalNumber(before.implicitTiebreaker()));

        assertTrue(before.before(after));
        assertFalse(before.before(before));
        assertFalse(after.before(before));
    }

    public void testBeforeOrAt() {
        Ordinal before = new Ordinal(randomOrdinalNumberBetween(1000, 2000), (Comparable) randomOrdinalNumber(), randomOrdinalNumber());
        Ordinal after = new Ordinal(randomOrdinalNumberBetween(5000, 6000, before.timestamp()),
            (Comparable) randomOrdinalNumber(before.tiebreaker()), randomOrdinalNumber(before.implicitTiebreaker()));

        assertTrue(before.beforeOrAt(after));
        assertTrue(before.beforeOrAt(before));
        assertFalse(after.beforeOrAt(before));
    }

    public void testTestAfter() {
        Ordinal before = new Ordinal(randomOrdinalNumberBetween(1000, 2000), (Comparable) randomOrdinalNumber(), randomOrdinalNumber());
        Ordinal after = new Ordinal(randomOrdinalNumberBetween(5000, 6000, before.timestamp()),
            (Comparable) randomOrdinalNumber(before.tiebreaker()), randomOrdinalNumber(before.implicitTiebreaker()));

        assertTrue(after.after(before));
        assertFalse(after.after(after));
        assertFalse(before.after(after));
    }

    public void testAfterOrAt() {
        Ordinal before = new Ordinal(randomOrdinalNumberBetween(1000, 2000), (Comparable) randomOrdinalNumber(), randomOrdinalNumber());
        Ordinal after = new Ordinal(randomOrdinalNumberBetween(5000, 6000, before.timestamp()),
            (Comparable) randomOrdinalNumber(before.tiebreaker()), randomOrdinalNumber(before.implicitTiebreaker()));

        assertTrue(after.afterOrAt(before));
        assertTrue(after.afterOrAt(after));
        assertFalse(before.afterOrAt(after));
    }

    static Number randomOrdinalNumber() {
        return randomOrdinalNumber(null);
    }

    private static Number randomOrdinalNumberBetween(long min, long max) {
        return randomOrdinalNumberBetween(min, max, null);
    }

    private static Number randomOrdinalNumber(Object asType) {
        return generateRandomLong(asType) ? randomLong() : new BigDecimal(randomBigInteger(), randomNonNegativeByte());
    }

    private static Number randomOrdinalNumberBetween(long min, long max, Object asType) {
        long l = randomLongBetween(min, max);
        return generateRandomLong(asType) ? l : BigDecimal.valueOf(l);
    }

    private static boolean generateRandomLong(Object asType) {
        return (asType == null && randomBoolean()) || (asType != null && asType.getClass() == Long.class);
    }

    private static int compareOrdinalNumbers(Number n1, Number n2) {
        assert n1.getClass() == n2.getClass();
        return n1 instanceof Long ? Long.compare((long) n1, (long) n2) : ((BigDecimal) n1).compareTo((BigDecimal) n2);
    }
}
