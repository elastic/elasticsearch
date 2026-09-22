/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson.internal.parsers;

import org.elasticsearch.simdjson.JsonParsingException;
import org.elasticsearch.test.ESTestCase;

// Fast lookups for JSON structural chars, whitespace, escapes, and \\u hex parsing.
public class CharacterUtilsTests extends ESTestCase {

    // ---- isStructuralOrWhitespace ----

    public void testStructuralCharsReturnTrue() {
        assertTrue(CharacterUtils.isStructuralOrWhitespace((byte) '{'));
        assertTrue(CharacterUtils.isStructuralOrWhitespace((byte) '}'));
        assertTrue(CharacterUtils.isStructuralOrWhitespace((byte) '['));
        assertTrue(CharacterUtils.isStructuralOrWhitespace((byte) ']'));
        assertTrue(CharacterUtils.isStructuralOrWhitespace((byte) ':'));
        assertTrue(CharacterUtils.isStructuralOrWhitespace((byte) ','));
    }

    public void testWhitespaceCharsReturnTrue() {
        assertTrue(CharacterUtils.isStructuralOrWhitespace((byte) 0x20));
        assertTrue(CharacterUtils.isStructuralOrWhitespace((byte) 0x09));
        assertTrue(CharacterUtils.isStructuralOrWhitespace((byte) 0x0A));
        assertTrue(CharacterUtils.isStructuralOrWhitespace((byte) 0x0D));
    }

    public void testAlphanumericReturnFalse() {
        assertFalse(CharacterUtils.isStructuralOrWhitespace((byte) 'a'));
        assertFalse(CharacterUtils.isStructuralOrWhitespace((byte) 'z'));
        assertFalse(CharacterUtils.isStructuralOrWhitespace((byte) 'A'));
        assertFalse(CharacterUtils.isStructuralOrWhitespace((byte) 'Z'));
        assertFalse(CharacterUtils.isStructuralOrWhitespace((byte) '0'));
        assertFalse(CharacterUtils.isStructuralOrWhitespace((byte) '9'));
    }

    public void testHighBitBytesReturnFalse() {
        assertFalse(CharacterUtils.isStructuralOrWhitespace((byte) 0x80));
        assertFalse(CharacterUtils.isStructuralOrWhitespace((byte) 0xFF));
    }

    public void testQuoteAndBackslashReturnFalse() {
        assertFalse(CharacterUtils.isStructuralOrWhitespace((byte) '"'));
        assertFalse(CharacterUtils.isStructuralOrWhitespace((byte) '\\'));
    }

    // ---- JSON string escape sequences ----

    public void testEscapeQuote() {
        assertEquals((byte) '"', CharacterUtils.escape((byte) '"'));
    }

    public void testEscapeBackslash() {
        assertEquals((byte) '\\', CharacterUtils.escape((byte) '\\'));
    }

    public void testEscapeSlash() {
        assertEquals((byte) '/', CharacterUtils.escape((byte) '/'));
    }

    public void testEscapeB() {
        assertEquals((byte) 0x08, CharacterUtils.escape((byte) 'b'));
    }

    public void testEscapeF() {
        assertEquals((byte) 0x0C, CharacterUtils.escape((byte) 'f'));
    }

    public void testEscapeN() {
        assertEquals((byte) 0x0A, CharacterUtils.escape((byte) 'n'));
    }

    public void testEscapeR() {
        assertEquals((byte) 0x0D, CharacterUtils.escape((byte) 'r'));
    }

    public void testEscapeT() {
        assertEquals((byte) 0x09, CharacterUtils.escape((byte) 't'));
    }

    public void testEscapeInvalidThrows() {
        expectThrows(JsonParsingException.class, () -> CharacterUtils.escape((byte) 'a'));
        expectThrows(JsonParsingException.class, () -> CharacterUtils.escape((byte) 'x'));
        expectThrows(JsonParsingException.class, () -> CharacterUtils.escape((byte) '0'));
    }

    public void testEscapeNullAndNegativeByteThrows() {
        expectThrows(JsonParsingException.class, () -> CharacterUtils.escape((byte) 0));
        expectThrows(JsonParsingException.class, () -> CharacterUtils.escape((byte) 0x80));
    }

    // ---- \\uXXXX hex conversion ----

    public void testHexToIntValidLowerCase() {
        byte[] buff = new byte[] { '0', '0', '4', '1' };
        assertEquals(0x0041, CharacterUtils.hexToInt(buff, 0));
    }

    public void testHexToIntValidUpperCase() {
        byte[] buff = new byte[] { 'F', 'F', 'F', 'F' };
        assertEquals(0xFFFF, CharacterUtils.hexToInt(buff, 0));
    }

    public void testHexToIntMixedCase() {
        byte[] buff = new byte[] { 'A', 'b', 'C', 'd' };
        assertEquals(0xABCD, CharacterUtils.hexToInt(buff, 0));
    }

    public void testHexToIntZero() {
        byte[] buff = new byte[] { '0', '0', '0', '0' };
        assertEquals(0, CharacterUtils.hexToInt(buff, 0));
    }

    public void testHexToIntBoundaryDigits() {
        byte[] nines = new byte[] { '9', '9', '9', '9' };
        assertEquals(0x9999, CharacterUtils.hexToInt(nines, 0));

        byte[] lowerAs = new byte[] { 'a', 'a', 'a', 'a' };
        assertEquals(0xAAAA, CharacterUtils.hexToInt(lowerAs, 0));

        byte[] lowerFs = new byte[] { 'f', 'f', 'f', 'f' };
        assertEquals(0xFFFF, CharacterUtils.hexToInt(lowerFs, 0));
    }

    // Invalid hex digits return -1; StringParser rejects malformed \\u escapes.
    public void testHexToIntInvalidDigitsReturnsNegativeOne() {
        assertEquals(-1, CharacterUtils.hexToInt(new byte[] { '0', '0', 'G', '0' }, 0));
        assertEquals(-1, CharacterUtils.hexToInt(new byte[] { '0', '0', '0', ' ' }, 0));
        assertEquals(-1, CharacterUtils.hexToInt(new byte[] { '0', '0', 'g', '0' }, 0));
    }

    // Hex digits may start mid-buffer after '\\u' in a JSON string.
    public void testHexToIntWithNonZeroOffset() {
        byte[] buff = new byte[] { '\\', 'u', '0', '0', '4', '1' };
        assertEquals(0x0041, CharacterUtils.hexToInt(buff, 2));
    }
}
