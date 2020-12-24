/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.elasticsearch.test.ESTestCase;

@SuppressWarnings("unchecked")
public class OrdinalTests extends ESTestCase {

    public void testCompareToDifferentTs() {
        Ordinal one = new Ordinal(randomLong(), (Comparable) randomLong());
        Ordinal two = new Ordinal(randomLong(), (Comparable) randomLong());

        assertEquals(Long.valueOf(one.timestamp()).compareTo(two.timestamp()), one.compareTo(two));
    }

    public void testCompareToSameTsDifferentTie() {
        Long ts = randomLong();
        Ordinal one = new Ordinal(ts, (Comparable) randomLong());
        Ordinal two = new Ordinal(ts, (Comparable) randomLong());

        assertEquals(one.tiebreaker().compareTo(two.tiebreaker()), one.compareTo(two));
    }

    public void testCompareToSameTsOneTieNull() {
        Long ts = randomLong();
        Ordinal one = new Ordinal(ts, (Comparable) randomLong());
        Ordinal two = new Ordinal(ts, null);

        assertEquals(-1, one.compareTo(two));
    }

    @SuppressWarnings("rawtypes")
    public void testCompareToSameTsSameTie() {
        Long ts = randomLong();
        Comparable c = randomLong();
        Ordinal one = new Ordinal(ts, c);
        Ordinal two = new Ordinal(ts, c);

        assertEquals(0, one.compareTo(two));
        assertEquals(0, one.compareTo(one));
        assertEquals(0, two.compareTo(two));
    }

    public void testCompareToSameTsSameTieNull() {
        Long ts = randomLong();
        Ordinal one = new Ordinal(ts, null);
        Ordinal two = new Ordinal(ts, null);

        assertEquals(0, one.compareTo(two));
        assertEquals(0, one.compareTo(one));
        assertEquals(0, two.compareTo(two));
    }

    public void testTestBetween() {
        Ordinal before = new Ordinal(randomLongBetween(1000, 2000), (Comparable) randomLong());
        Ordinal between = new Ordinal(randomLongBetween(3000, 4000), (Comparable) randomLong());
        Ordinal after = new Ordinal(randomLongBetween(5000, 6000), (Comparable) randomLong());

        assertTrue(before.between(before, after));
        assertTrue(after.between(before, after));
        assertTrue(between.between(before, after));

        assertFalse(new Ordinal(randomLongBetween(0, 999), null).between(before, after));
        assertFalse(new Ordinal(randomLongBetween(7000, 8000), null).between(before, after));
    }

    public void testTestBefore() {
        Ordinal before = new Ordinal(randomLongBetween(1000, 2000), (Comparable) randomLong());
        Ordinal after = new Ordinal(randomLongBetween(5000, 6000), (Comparable) randomLong());

        assertTrue(before.before(after));
        assertFalse(before.before(before));
        assertFalse(after.before(before));
    }

    public void testBeforeOrAt() {
        Ordinal before = new Ordinal(randomLongBetween(1000, 2000), (Comparable) randomLong());
        Ordinal after = new Ordinal(randomLongBetween(5000, 6000), (Comparable) randomLong());

        assertTrue(before.beforeOrAt(after));
        assertTrue(before.beforeOrAt(before));
        assertFalse(after.beforeOrAt(before));
    }

    public void testTestAfter() {
        Ordinal before = new Ordinal(randomLongBetween(1000, 2000), (Comparable) randomLong());
        Ordinal after = new Ordinal(randomLongBetween(5000, 6000), (Comparable) randomLong());

        assertTrue(after.after(before));
        assertFalse(after.after(after));
        assertFalse(before.after(after));
    }

    public void testAfterOrAt() {
        Ordinal before = new Ordinal(randomLongBetween(1000, 2000), (Comparable) randomLong());
        Ordinal after = new Ordinal(randomLongBetween(5000, 6000), (Comparable) randomLong());

        assertTrue(after.afterOrAt(before));
        assertTrue(after.afterOrAt(after));
        assertFalse(before.afterOrAt(after));
    }
}
