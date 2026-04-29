/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class WordMaskTests extends ESTestCase {

    public void testClearBit() {
        WordMask mask = new WordMask();
        mask.reset(128);
        mask.set(10);
        mask.set(42);
        mask.set(100);
        assertTrue(mask.get(42));

        mask.clear(42);
        assertFalse(mask.get(42));
        // other bits remain unchanged
        assertTrue(mask.get(10));
        assertTrue(mask.get(100));
    }

    public void testSetAll() {
        WordMask mask = new WordMask();
        mask.setAll(128);
        for (int i = 0; i < 128; i++) {
            assertTrue("bit " + i + " should be set", mask.get(i));
        }
        assertThat(mask.popCount(), equalTo(128));
    }

    public void testPopCount() {
        WordMask mask = new WordMask();
        mask.reset(256);
        assertThat(mask.popCount(), equalTo(0));

        mask.set(0);
        mask.set(63);
        mask.set(64);
        mask.set(127);
        mask.set(200);
        assertThat(mask.popCount(), equalTo(5));

        // full mask
        WordMask full = new WordMask();
        full.setAll(256);
        assertThat(full.popCount(), equalTo(256));
    }

    public void testIsAll() {
        WordMask mask = new WordMask();
        mask.setAll(128);
        assertTrue(mask.isAll());

        mask.clear(64);
        assertFalse(mask.isAll());

        // empty mask (all zeros)
        WordMask empty = new WordMask();
        empty.reset(128);
        assertFalse(empty.isAll());
    }

    public void testAnd() {
        WordMask a = new WordMask();
        a.reset(128);
        a.set(1);
        a.set(2);
        a.set(3);
        a.set(100);

        WordMask b = new WordMask();
        b.reset(128);
        b.set(2);
        b.set(3);
        b.set(4);
        b.set(100);

        a.and(b);

        assertFalse(a.get(1));
        assertTrue(a.get(2));
        assertTrue(a.get(3));
        assertFalse(a.get(4));
        assertTrue(a.get(100));
        assertThat(a.popCount(), equalTo(3));
    }

    public void testOr() {
        WordMask a = new WordMask();
        a.reset(128);
        a.set(1);
        a.set(2);

        WordMask b = new WordMask();
        b.reset(128);
        b.set(2);
        b.set(3);
        b.set(100);

        a.or(b);

        assertTrue(a.get(1));
        assertTrue(a.get(2));
        assertTrue(a.get(3));
        assertTrue(a.get(100));
        assertThat(a.popCount(), equalTo(4));
    }

    public void testNegate() {
        WordMask mask = new WordMask();
        mask.reset(128);
        mask.set(0);
        mask.set(10);
        mask.set(127);

        mask.negate();

        assertFalse(mask.get(0));
        assertFalse(mask.get(10));
        assertFalse(mask.get(127));
        assertTrue(mask.get(1));
        assertTrue(mask.get(50));
        assertTrue(mask.get(126));
        assertThat(mask.popCount(), equalTo(128 - 3));
    }

    public void testSurvivingPositions() {
        WordMask mask = new WordMask();
        mask.reset(256);
        mask.set(0);
        mask.set(5);
        mask.set(63);
        mask.set(64);
        mask.set(200);

        int[] positions = mask.survivingPositions();
        assertThat(positions, equalTo(new int[] { 0, 5, 63, 64, 200 }));
    }

    public void testSurvivingPositionsEmpty() {
        WordMask mask = new WordMask();
        mask.reset(128);

        int[] positions = mask.survivingPositions();
        assertThat(positions.length, equalTo(0));
    }

    public void testSurvivingPositionsFull() {
        WordMask mask = new WordMask();
        int n = 130;
        mask.setAll(n);

        int[] positions = mask.survivingPositions();
        assertThat(positions.length, equalTo(n));
        for (int i = 0; i < n; i++) {
            assertThat("position at index " + i, positions[i], equalTo(i));
        }
    }

    public void testTrailingBits() {
        // numBits=65 is not word-aligned: 1 full word (64 bits) + 1 bit in second word
        int numBits = 65;

        // setAll should set exactly 65 bits
        WordMask mask = new WordMask();
        mask.setAll(numBits);
        assertThat(mask.popCount(), equalTo(65));
        assertTrue(mask.isAll());

        // verify bit 64 (the lone bit in the second word) is set
        assertTrue(mask.get(64));

        // clear the trailing bit and verify isAll becomes false
        mask.clear(64);
        assertFalse(mask.isAll());
        assertThat(mask.popCount(), equalTo(64));

        // negate on a non-word-aligned mask
        WordMask negMask = new WordMask();
        negMask.reset(numBits);
        negMask.set(0);
        negMask.set(64);
        negMask.negate();
        // after negate, bits 0 and 64 should be clear, all others set
        assertFalse(negMask.get(0));
        assertFalse(negMask.get(64));
        assertTrue(negMask.get(1));
        assertTrue(negMask.get(63));
        assertThat(negMask.popCount(), equalTo(63));

        // survivingPositions on non-word-aligned full mask
        WordMask fullMask = new WordMask();
        fullMask.setAll(numBits);
        int[] positions = fullMask.survivingPositions();
        assertThat(positions.length, equalTo(65));
        assertThat(positions[64], equalTo(64));
    }
}
