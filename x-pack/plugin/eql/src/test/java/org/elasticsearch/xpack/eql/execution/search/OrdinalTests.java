/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.elasticsearch.test.ESTestCase;

@SuppressWarnings({"unchecked", "rawtypes"})
public class OrdinalTests extends ESTestCase {

    public void testCompareToDifferentTs() {
        long ts1 = randomLong();
        long ts2 = randomValueOtherThan(ts1, () -> randomLong());
        Ordinal one = new Ordinal(ts1, (Comparable) randomLong(), randomLong());
        Ordinal two = new Ordinal(ts2, (Comparable) randomLong(), randomLong());

        assertEquals(Long.valueOf(one.timestamp()).compareTo(two.timestamp()), one.compareTo(two));
    }

    public void testCompareToSameTsDifferentTie() {
        long ts = randomLong();
        Comparable tie1 = (Comparable) randomLong();
        Comparable tie2 = randomValueOtherThan(tie1, () -> (Comparable) randomLong());
        Ordinal one = new Ordinal(ts, tie1, randomLong());
        Ordinal two = new Ordinal(ts, tie2, randomLong());

        assertEquals(one.tiebreaker().compareTo(two.tiebreaker()), one.compareTo(two));
    }

    public void testCompareToSameTsOneTieNull() {
        long ts = randomLong();
        Ordinal one = new Ordinal(ts, (Comparable) randomLong(), randomLong());
        Ordinal two = new Ordinal(ts, null, randomLong());

        assertEquals(-1, one.compareTo(two));
    }

    public void testCompareToSameTsSameTieSameImplicitTb() {
        long ts = randomLong();
        Comparable c = randomLong();
        long implicitTb = randomLong();
        Ordinal one = new Ordinal(ts, c, implicitTb);
        Ordinal two = new Ordinal(ts, c, implicitTb);

        assertEquals(0, one.compareTo(two));
        assertEquals(0, one.compareTo(one));
        assertEquals(0, two.compareTo(two));
    }

    public void testCompareToSameTsSameTieDifferentImplicitTb() {
        long ts = randomLong();
        Comparable c = randomLong();
        long implicitTb = randomLong();
        Ordinal one = new Ordinal(ts, c, implicitTb);
        Ordinal two = new Ordinal(ts, c, randomValueOtherThan(implicitTb, () -> randomLong()));

        assertEquals(Long.valueOf(one.implicitTiebreaker()).compareTo(two.implicitTiebreaker()), one.compareTo(two));
    }

    public void testCompareToSameTsSameTieNullSameImplicitTb() {
        long ts = randomLong();
        long implicitTb = randomLong();
        Ordinal one = new Ordinal(ts, null, implicitTb);
        Ordinal two = new Ordinal(ts, null, implicitTb);

        assertEquals(0, one.compareTo(two));
        assertEquals(0, one.compareTo(one));
        assertEquals(0, two.compareTo(two));
    }

    public void testCompareToSameTsSameTieNullDifferentImplicitTb() {
        long ts = randomLong();
        long implicitTb1 = randomLong();
        long implicitTb2 = randomValueOtherThan(implicitTb1, () -> randomLong());
        Ordinal one = new Ordinal(ts, null, implicitTb1);
        Ordinal two = new Ordinal(ts, null, implicitTb2);

        assertEquals(Long.valueOf(one.implicitTiebreaker()).compareTo(two.implicitTiebreaker()), one.compareTo(two));
    }

    public void testTestBetween() {
        Ordinal before = new Ordinal(randomLongBetween(1000, 2000), (Comparable) randomLong(), randomLong());
        Ordinal between = new Ordinal(randomLongBetween(3000, 4000), (Comparable) randomLong(), randomLong());
        Ordinal after = new Ordinal(randomLongBetween(5000, 6000), (Comparable) randomLong(), randomLong());

        assertTrue(before.between(before, after));
        assertTrue(after.between(before, after));
        assertTrue(between.between(before, after));

        assertFalse(new Ordinal(randomLongBetween(0, 999), null, randomLong()).between(before, after));
        assertFalse(new Ordinal(randomLongBetween(7000, 8000), null, randomLong()).between(before, after));
    }

    public void testTestBefore() {
        Ordinal before = new Ordinal(randomLongBetween(1000, 2000), (Comparable) randomLong(), randomLong());
        Ordinal after = new Ordinal(randomLongBetween(5000, 6000), (Comparable) randomLong(), randomLong());

        assertTrue(before.before(after));
        assertFalse(before.before(before));
        assertFalse(after.before(before));
    }

    public void testBeforeOrAt() {
        Ordinal before = new Ordinal(randomLongBetween(1000, 2000), (Comparable) randomLong(), randomLong());
        Ordinal after = new Ordinal(randomLongBetween(5000, 6000), (Comparable) randomLong(), randomLong());

        assertTrue(before.beforeOrAt(after));
        assertTrue(before.beforeOrAt(before));
        assertFalse(after.beforeOrAt(before));
    }

    public void testTestAfter() {
        Ordinal before = new Ordinal(randomLongBetween(1000, 2000), (Comparable) randomLong(), randomLong());
        Ordinal after = new Ordinal(randomLongBetween(5000, 6000), (Comparable) randomLong(), randomLong());

        assertTrue(after.after(before));
        assertFalse(after.after(after));
        assertFalse(before.after(after));
    }

    public void testAfterOrAt() {
        Ordinal before = new Ordinal(randomLongBetween(1000, 2000), (Comparable) randomLong(), randomLong());
        Ordinal after = new Ordinal(randomLongBetween(5000, 6000), (Comparable) randomLong(), randomLong());

        assertTrue(after.afterOrAt(before));
        assertTrue(after.afterOrAt(after));
        assertFalse(before.afterOrAt(after));
    }
}
