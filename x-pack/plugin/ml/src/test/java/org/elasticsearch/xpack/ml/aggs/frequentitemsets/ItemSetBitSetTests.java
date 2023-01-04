/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets;

import org.elasticsearch.test.ESTestCase;

public class ItemSetBitSetTests extends ESTestCase {

    public void testBasics() {
        ItemSetBitSet set = new ItemSetBitSet();
        set.set(0);
        set.set(3);
        set.set(200);
        set.set(5);
        set.set(65);

        assertEquals(5, set.cardinality());
        assertTrue(set.get(0));
        assertFalse(set.get(1));
        assertFalse(set.get(2));
        assertTrue(set.get(3));
        assertTrue(set.get(5));
        assertFalse(set.get(64));
        assertTrue(set.get(65));
        assertTrue(set.get(200));

        set.clear(0);
        set.clear(65);
        set.clear(5);
        assertEquals(2, set.cardinality());

        assertFalse(set.get(0));
        assertFalse(set.get(1));
        assertFalse(set.get(2));
        assertTrue(set.get(3));
        assertFalse(set.get(5));
        assertFalse(set.get(64));
        assertFalse(set.get(65));
        assertTrue(set.get(200));
    }

    public void testIsSubSetAndSetRelation() {
        ItemSetBitSet set1 = new ItemSetBitSet();
        set1.set(0);
        set1.set(3);
        set1.set(200);
        set1.set(5);
        set1.set(65);

        assertEquals(5, set1.cardinality());
        ItemSetBitSet set2 = new ItemSetBitSet();
        set2.set(3);
        set2.set(200);
        set2.set(65);

        assertEquals(3, set2.cardinality());
        assertTrue(set2.isSubset(set1));
        assertEquals(ItemSetBitSet.SetRelation.SUB_SET, set2.setRelation(set1));
        assertFalse(set1.isSubset(set2));
        assertEquals(ItemSetBitSet.SetRelation.SUPER_SET, set1.setRelation(set2));
        assertTrue(set1.isSubset(set1));
        assertEquals(ItemSetBitSet.SetRelation.EQUAL, set1.setRelation(set1));

        set2.set(0);
        set2.set(5);
        assertTrue(set2.isSubset(set1));
        assertTrue(set1.isSubset(set2));
        assertEquals(ItemSetBitSet.SetRelation.EQUAL, set1.setRelation(set2));
        assertEquals(ItemSetBitSet.SetRelation.EQUAL, set2.setRelation(set1));

        set2.set(99);
        assertFalse(set2.isSubset(set1));
        assertTrue(set1.isSubset(set2));
        assertEquals(ItemSetBitSet.SetRelation.SUPER_SET, set2.setRelation(set1));
        assertEquals(ItemSetBitSet.SetRelation.SUB_SET, set1.setRelation(set2));

        set1.set(999);
        assertFalse(set1.isSubset(set2));
        assertEquals(ItemSetBitSet.SetRelation.DISJOINT_OR_INTERSECT, set2.setRelation(set1));
        assertEquals(ItemSetBitSet.SetRelation.DISJOINT_OR_INTERSECT, set1.setRelation(set2));

        set2.set(999);
        assertTrue(set1.isSubset(set2));
        assertEquals(ItemSetBitSet.SetRelation.SUPER_SET, set2.setRelation(set1));
        assertEquals(ItemSetBitSet.SetRelation.SUB_SET, set1.setRelation(set2));

        set2.set(2222);
        assertTrue(set1.isSubset(set2));
        assertEquals(ItemSetBitSet.SetRelation.SUPER_SET, set2.setRelation(set1));
        assertEquals(ItemSetBitSet.SetRelation.SUB_SET, set1.setRelation(set2));
    }

    public void testClone() {
        ItemSetBitSet set1 = new ItemSetBitSet();
        set1.set(0);
        set1.set(3);
        set1.set(200);
        set1.set(5);
        set1.set(65);

        ItemSetBitSet set2 = (ItemSetBitSet) set1.clone();
        assertEquals(5, set2.cardinality());

        assertTrue(set2.get(0));
        assertFalse(set2.get(1));
        assertFalse(set2.get(2));
        assertTrue(set2.get(3));
        assertTrue(set2.get(5));
        assertFalse(set2.get(64));
        assertTrue(set2.get(65));
        assertTrue(set2.get(200));

        set1.clear(200);
        assertTrue(set2.get(200));

        set1.set(42);
        assertTrue(set1.get(42));
        assertFalse(set2.get(42));
    }

    public void testReset() {
        ItemSetBitSet set1 = new ItemSetBitSet();
        set1.set(0);
        set1.set(3);
        set1.set(200);
        set1.set(5);
        set1.set(65);

        ItemSetBitSet set2 = new ItemSetBitSet();
        set2.reset(set1);
        assertEquals(set1, set2);
        assertEquals(set1.cardinality(), set2.cardinality());

        assertTrue(set2.get(0));
        assertFalse(set2.get(1));
        assertFalse(set2.get(2));
        assertTrue(set2.get(3));
        assertTrue(set2.get(5));
        assertFalse(set2.get(64));
        assertTrue(set2.get(65));
        assertTrue(set2.get(200));

        set1.clear(200);
        assertTrue(set2.get(200));

        set1.set(42);
        assertTrue(set1.get(42));
        assertFalse(set2.get(42));

        set2.set(99999999);
        assertTrue(set2.get(99999999));

        ItemSetBitSet set3 = new ItemSetBitSet();
        set3.set(2);
        set3.set(9);
        set2.reset(set3);

        assertEquals(set3, set2);
        assertFalse(set2.get(99999999));
    }

    public void testCardinality() {
        ItemSetBitSet set = new ItemSetBitSet();
        set.set(0);
        set.set(3);
        set.set(200);
        set.set(5);
        set.set(65);

        assertEquals(5, set.cardinality());
        set.clear(1);
        assertEquals(5, set.cardinality());
        set.clear(200);
        assertEquals(4, set.cardinality());
        set.set(204);
        set.set(204);
        set.set(204);
        set.set(204);
        assertEquals(5, set.cardinality());
        ItemSetBitSet set2 = new ItemSetBitSet();
        set.reset(set2);
        assertEquals(0, set.cardinality());
        set.clear(999);

        set.set(54);
        set.set(20);
        assertEquals(2, set.cardinality());

        set.clear();
        assertEquals(0, set.cardinality());
    }

    public void testHashCode() {
        ItemSetBitSet set1 = new ItemSetBitSet();
        set1.set(0);
        set1.set(3);
        set1.set(200);
        set1.set(5);
        set1.set(65);

        ItemSetBitSet set2 = new ItemSetBitSet();
        set2.reset(set1);

        assertEquals(set1.hashCode(), set2.hashCode());
        set2.set(99999999);
        assertNotEquals(set1.hashCode(), set2.hashCode());
        set2.clear(99999999);
        assertEquals(set1.hashCode(), set2.hashCode());
    }

    public void testCompare() {
        ItemSetBitSet set1 = new ItemSetBitSet();
        set1.set(0);
        set1.set(3);
        ItemSetBitSet set2 = new ItemSetBitSet();
        set2.set(0);

        assertEquals(1, ItemSetBitSet.compare(set1, set2));
        assertEquals(-1, ItemSetBitSet.compare(set2, set1));

        set2.set(3);
        assertEquals(0, ItemSetBitSet.compare(set2, set1));
        set1.set(4);
        set2.set(5);

        assertEquals(1, ItemSetBitSet.compare(set1, set2));
        set1.set(6);
        set2.set(6);
        assertEquals(1, ItemSetBitSet.compare(set1, set2));
        set1.set(7);
        set2.set(8);
        assertEquals(1, ItemSetBitSet.compare(set1, set2));

        ItemSetBitSet set3 = new ItemSetBitSet();
        set3.set(2);
        set3.set(3);
        set3.set(4);
        ItemSetBitSet set4 = new ItemSetBitSet();
        set4.set(2);
        set4.set(3);
        set4.set(4);

        set3.set(71);
        set4.set(101);
        assertEquals(1, ItemSetBitSet.compare(set3, set4));
        assertEquals(-1, ItemSetBitSet.compare(set4, set3));

        set3.set(61);
        assertEquals(1, ItemSetBitSet.compare(set3, set4));
        assertEquals(-1, ItemSetBitSet.compare(set4, set3));

        set3.clear(71);
        set4.set(101);

        assertEquals(1, ItemSetBitSet.compare(set3, set4));
        assertEquals(-1, ItemSetBitSet.compare(set4, set3));
    }

}
