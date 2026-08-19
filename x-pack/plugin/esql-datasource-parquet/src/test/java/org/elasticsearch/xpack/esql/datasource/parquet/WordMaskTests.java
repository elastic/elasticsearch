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

    public void testOrLongAtWordAligned() {
        WordMask mask = new WordMask();
        mask.reset(192);
        long bits = 0xCAFEBABEDEADBEEFL;
        mask.orLongAt(64, bits);
        for (int i = 0; i < 192; i++) {
            boolean expected = i >= 64 && i < 128 && ((bits >>> (i - 64)) & 1L) != 0;
            assertEquals("bit " + i, expected, mask.get(i));
        }
    }

    public void testOrLongAtMisaligned() {
        WordMask mask = new WordMask();
        mask.reset(256);
        long bits = 0xF0F0F0F0F0F0F0F0L;
        int offset = 17;
        mask.orLongAt(offset, bits);
        for (int i = 0; i < 256; i++) {
            boolean expected = i >= offset && i < offset + 64 && ((bits >>> (i - offset)) & 1L) != 0;
            assertEquals("bit " + i, expected, mask.get(i));
        }
    }

    public void testOrLongAtPreservesExistingBits() {
        WordMask mask = new WordMask();
        mask.reset(192);
        mask.set(5);
        mask.set(70);
        mask.set(150);
        long bits = 0x00000000FFFFFFFFL; // low 32 bits set
        int offset = 60; // straddles word boundary at bit 64
        mask.orLongAt(offset, bits);
        // expected: bits 60..91 set (from the OR), plus pre-existing 5, 70, 150
        for (int i = 0; i < 192; i++) {
            boolean fromOr = i >= offset && i < offset + 64 && ((bits >>> (i - offset)) & 1L) != 0;
            boolean preexisting = i == 5 || i == 70 || i == 150;
            assertEquals("bit " + i, fromOr || preexisting, mask.get(i));
        }
    }

    public void testOrLongAtAllOnesStraddling() {
        WordMask mask = new WordMask();
        mask.reset(256);
        // 64 bits set at offset 30 → covers bits 30..93
        mask.orLongAt(30, ~0L);
        for (int i = 0; i < 256; i++) {
            assertEquals("bit " + i, i >= 30 && i < 94, mask.get(i));
        }
    }

    public void testOrLongAtRandomized() {
        for (int trial = 0; trial < 50; trial++) {
            int numBits = randomIntBetween(64, 512);
            int offset = randomIntBetween(0, numBits - 64);
            long bits = randomLong();
            WordMask mask = new WordMask();
            mask.reset(numBits);
            // pre-set a few random bits to verify OR semantics
            int[] preset = new int[randomIntBetween(0, 5)];
            for (int i = 0; i < preset.length; i++) {
                preset[i] = randomIntBetween(0, numBits - 1);
                mask.set(preset[i]);
            }
            mask.orLongAt(offset, bits);
            for (int i = 0; i < numBits; i++) {
                boolean fromOr = i >= offset && i < offset + 64 && ((bits >>> (i - offset)) & 1L) != 0;
                boolean wasPreset = false;
                for (int p : preset) {
                    if (p == i) {
                        wasPreset = true;
                        break;
                    }
                }
                assertEquals("trial=" + trial + " bit=" + i, fromOr || wasPreset, mask.get(i));
            }
        }
    }

    public void testSetRangeWithinSingleWord() {
        WordMask mask = new WordMask();
        mask.reset(128);
        mask.setRange(3, 17);
        for (int i = 0; i < 128; i++) {
            assertEquals("bit " + i, i >= 3 && i < 17, mask.get(i));
        }
        assertThat(mask.popCount(), equalTo(14));
    }

    public void testSetRangeAcrossMultipleWords() {
        WordMask mask = new WordMask();
        mask.reset(256);
        // [40, 200) spans words 0, 1, 2 with misaligned ends
        mask.setRange(40, 200);
        for (int i = 0; i < 256; i++) {
            assertEquals("bit " + i, i >= 40 && i < 200, mask.get(i));
        }
        assertThat(mask.popCount(), equalTo(160));
    }

    public void testSetRangeAlignedToWordBoundaries() {
        WordMask mask = new WordMask();
        mask.reset(256);
        mask.setRange(64, 192);
        for (int i = 0; i < 256; i++) {
            assertEquals("bit " + i, i >= 64 && i < 192, mask.get(i));
        }
        assertThat(mask.popCount(), equalTo(128));
    }

    public void testSetRangeFromZero() {
        WordMask mask = new WordMask();
        mask.reset(128);
        mask.setRange(0, 64);
        for (int i = 0; i < 128; i++) {
            assertEquals("bit " + i, i < 64, mask.get(i));
        }
    }

    public void testSetRangeToEnd() {
        WordMask mask = new WordMask();
        mask.reset(128);
        mask.setRange(70, 128);
        for (int i = 0; i < 128; i++) {
            assertEquals("bit " + i, i >= 70, mask.get(i));
        }
        assertThat(mask.popCount(), equalTo(58));
    }

    public void testSetRangeFullMask() {
        WordMask mask = new WordMask();
        mask.reset(200);
        mask.setRange(0, 200);
        for (int i = 0; i < 200; i++) {
            assertTrue("bit " + i, mask.get(i));
        }
        assertThat(mask.popCount(), equalTo(200));
    }

    public void testSetRangeEmpty() {
        WordMask mask = new WordMask();
        mask.reset(128);
        mask.set(50);
        mask.setRange(10, 10);
        // empty range is a no-op; only the pre-set bit should be set
        assertThat(mask.popCount(), equalTo(1));
        assertTrue(mask.get(50));
    }

    public void testSetRangeIsIdempotentWithExistingBits() {
        WordMask mask = new WordMask();
        mask.reset(128);
        mask.set(20);
        mask.set(80);
        mask.setRange(10, 100);
        for (int i = 0; i < 128; i++) {
            assertEquals("bit " + i, i >= 10 && i < 100, mask.get(i));
        }
        assertThat(mask.popCount(), equalTo(90));
    }

    public void testSetRangeRandomized() {
        for (int trial = 0; trial < 50; trial++) {
            int numBits = randomIntBetween(1, 1024);
            int from = randomIntBetween(0, numBits);
            int to = randomIntBetween(from, numBits);
            WordMask mask = new WordMask();
            mask.reset(numBits);
            mask.setRange(from, to);
            for (int i = 0; i < numBits; i++) {
                assertEquals("trial=" + trial + " bit=" + i + " range=[" + from + "," + to + ")", i >= from && i < to, mask.get(i));
            }
            assertThat(mask.popCount(), equalTo(to - from));
        }
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

    /**
     * AND of two full masks with non-aligned numBits must not leak trailing bits.
     * If maskTrailingBits() were missing from and(), the trailing bits in the last word
     * could survive the operation and corrupt popCount/isAll/survivingPositions.
     */
    public void testAndMasksTrailingBits() {
        int numBits = 70; // 1 full word (64) + 6 bits in the second word

        // Both masks fully set: AND should produce a full mask of exactly 70 bits
        WordMask a = new WordMask();
        a.setAll(numBits);
        WordMask b = new WordMask();
        b.setAll(numBits);

        a.and(b);

        assertThat("popCount must equal numBits after AND of two full masks", a.popCount(), equalTo(70));
        assertTrue("isAll must be true after AND of two full masks", a.isAll());
        assertThat("survivingPositions length must equal numBits", a.survivingPositions().length, equalTo(70));

        // Negate then negate: produces a full mask through two negations.
        // If AND did not mask trailing bits, the double-negate path could expose the issue.
        WordMask c = new WordMask();
        c.setAll(numBits);
        c.negate(); // all zeros within numBits
        c.negate(); // back to all ones within numBits

        WordMask d = new WordMask();
        d.setAll(numBits);

        c.and(d);
        assertThat("popCount after double-negate then AND", c.popCount(), equalTo(70));
        assertTrue("isAll after double-negate then AND", c.isAll());
    }

    /**
     * OR of two masks with non-aligned numBits must not propagate trailing bits.
     */
    public void testOrMasksTrailingBits() {
        int numBits = 70;

        // OR of a full mask with an empty mask should produce exactly numBits set bits
        WordMask a = new WordMask();
        a.setAll(numBits);
        WordMask b = new WordMask();
        b.reset(numBits);

        a.or(b);
        assertThat("popCount must equal numBits after OR with empty mask", a.popCount(), equalTo(70));
        assertTrue("isAll must be true after OR full with empty", a.isAll());

        // OR of two partial masks: set different bits, verify no trailing bit leakage
        WordMask c = new WordMask();
        c.reset(numBits);
        c.set(0);
        c.set(69); // last valid bit

        WordMask d = new WordMask();
        d.reset(numBits);
        d.set(63);
        d.set(64);

        c.or(d);
        assertThat("popCount after OR of two sparse masks", c.popCount(), equalTo(4));
        assertThat(c.survivingPositions(), equalTo(new int[] { 0, 63, 64, 69 }));

        // OR of two negated masks: negate introduces risk of trailing bits
        WordMask e = new WordMask();
        e.reset(numBits);
        e.set(0);
        e.negate(); // bits 1..69 set
        WordMask f = new WordMask();
        f.reset(numBits);
        f.set(1);
        f.negate(); // bits 0, 2..69 set

        e.or(f); // should be all 70 bits set
        assertThat("popCount after OR of two negated masks", e.popCount(), equalTo(70));
        assertTrue("isAll after OR of two negated masks", e.isAll());
    }

    /**
     * Negate with non-aligned numBits must keep trailing bits clean.
     */
    public void testNegateMasksTrailingBits() {
        int numBits = 70;

        // Negate an empty mask: should produce exactly numBits set bits
        WordMask mask = new WordMask();
        mask.reset(numBits);
        mask.negate();
        assertThat("popCount after negating empty mask", mask.popCount(), equalTo(70));
        assertTrue("isAll after negating empty mask", mask.isAll());
        assertThat("survivingPositions length after negating empty mask", mask.survivingPositions().length, equalTo(70));

        // Negate a full mask: should produce zero set bits
        WordMask full = new WordMask();
        full.setAll(numBits);
        full.negate();
        assertThat("popCount after negating full mask", full.popCount(), equalTo(0));
        assertTrue("isEmpty after negating full mask", full.isEmpty());
        assertThat("survivingPositions after negating full mask", full.survivingPositions().length, equalTo(0));

        // Double negate should restore the original mask
        WordMask orig = new WordMask();
        orig.reset(numBits);
        orig.set(5);
        orig.set(65);
        orig.negate();
        orig.negate();
        assertTrue(orig.get(5));
        assertTrue(orig.get(65));
        assertThat(orig.popCount(), equalTo(2));
    }

    /**
     * Verify popCount accuracy across word boundaries with known bit patterns.
     */
    public void testPopCountAccuracy() {
        // 200 bits = 3 full words + 8 bits in the 4th word
        int numBits = 200;
        WordMask mask = new WordMask();
        mask.reset(numBits);

        // Set one bit per word boundary region
        mask.set(0);    // first bit of word 0
        mask.set(63);   // last bit of word 0
        mask.set(64);   // first bit of word 1
        mask.set(127);  // last bit of word 1
        mask.set(128);  // first bit of word 2
        mask.set(191);  // last bit of word 2
        mask.set(192);  // first bit of word 3
        mask.set(199);  // last valid bit
        assertThat(mask.popCount(), equalTo(8));

        // Set all bits and verify
        mask.setAll(numBits);
        assertThat(mask.popCount(), equalTo(200));

        // Non-aligned: 100 bits = 1 full word + 36 bits
        WordMask mask2 = new WordMask();
        mask2.reset(100);
        for (int i = 0; i < 100; i += 2) {
            mask2.set(i); // set even bits
        }
        assertThat("50 even bits in range [0,100)", mask2.popCount(), equalTo(50));
    }

    /**
     * Verify survivingPositions returns correct indices in ascending order for specific bit patterns.
     */
    public void testSurvivingPositionsSpecificBits() {
        int numBits = 130; // 2 full words + 2 bits
        WordMask mask = new WordMask();
        mask.reset(numBits);

        // Set bits at word boundaries and in the trailing partial word
        mask.set(0);
        mask.set(1);
        mask.set(62);
        mask.set(63);
        mask.set(64);
        mask.set(65);
        mask.set(126);
        mask.set(127);
        mask.set(128);
        mask.set(129);

        int[] positions = mask.survivingPositions();
        assertThat(positions, equalTo(new int[] { 0, 1, 62, 63, 64, 65, 126, 127, 128, 129 }));

        // Negate and verify surviving positions are the complement
        mask.negate();
        int[] negPositions = mask.survivingPositions();
        assertThat("negated mask should have numBits - original count bits", negPositions.length, equalTo(130 - 10));
        // First few positions after negate: 2, 3, 4, ...
        assertThat(negPositions[0], equalTo(2));
        assertThat(negPositions[1], equalTo(3));
    }

    /**
     * isAll must return true only when exactly all numBits bits are set,
     * and must not be fooled by trailing bits beyond numBits.
     */
    public void testIsAllWithNonAlignedBits() {
        int numBits = 70;

        // A mask with all 70 bits set
        WordMask full = new WordMask();
        full.setAll(numBits);
        assertTrue("setAll(70) must produce isAll=true", full.isAll());

        // Clear one bit: isAll must be false
        full.clear(69);
        assertFalse("clearing last valid bit must make isAll false", full.isAll());

        // Reset and set only bits 0..68 (missing bit 69): isAll must be false
        WordMask partial = new WordMask();
        partial.reset(numBits);
        for (int i = 0; i < 69; i++) {
            partial.set(i);
        }
        assertFalse("missing bit 69 means isAll must be false", partial.isAll());

        // Zero-bit mask: isAll is vacuously true
        WordMask zero = new WordMask();
        zero.reset(0);
        assertTrue("zero-bit mask is vacuously all-set", zero.isAll());
    }

    /**
     * setAll sets exactly n bits and reset clears all bits.
     */
    public void testSetAllAndReset() {
        WordMask mask = new WordMask();

        // setAll with non-aligned size
        mask.setAll(70);
        assertThat(mask.popCount(), equalTo(70));
        assertTrue(mask.isAll());
        for (int i = 0; i < 70; i++) {
            assertTrue("bit " + i + " should be set", mask.get(i));
        }

        // reset should clear everything
        mask.reset(70);
        assertThat(mask.popCount(), equalTo(0));
        assertTrue(mask.isEmpty());
        for (int i = 0; i < 70; i++) {
            assertFalse("bit " + i + " should be clear after reset", mask.get(i));
        }

        // setAll with word-aligned size
        mask.setAll(128);
        assertThat(mask.popCount(), equalTo(128));
        assertTrue(mask.isAll());

        // setAll with small non-aligned size
        mask.setAll(1);
        assertThat(mask.popCount(), equalTo(1));
        assertTrue(mask.isAll());
        assertTrue(mask.get(0));

        // setAll with zero
        mask.setAll(0);
        assertThat(mask.popCount(), equalTo(0));
        assertTrue(mask.isAll());
        assertTrue(mask.isEmpty());
    }

    /**
     * and() and or() must throw IllegalArgumentException when mask sizes differ.
     */
    public void testAndOrWithMismatchedSizes() {
        WordMask a = new WordMask();
        a.reset(64);
        WordMask b = new WordMask();
        b.reset(128);

        IllegalArgumentException andEx = expectThrows(IllegalArgumentException.class, () -> a.and(b));
        assertTrue(andEx.getMessage().contains("numBits mismatch"));

        IllegalArgumentException orEx = expectThrows(IllegalArgumentException.class, () -> a.or(b));
        assertTrue(orEx.getMessage().contains("numBits mismatch"));
    }
}
