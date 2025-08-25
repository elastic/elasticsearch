/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.api;

import org.elasticsearch.test.ESTestCase;

public class HexadecimalArgumentTests extends ESTestCase {

    public void testEvenLengthHexString() {
        HexadecimalArgument arg = new HexadecimalArgument("A1B2C3D4", 0, 8);
        byte[] expected = { (byte) 0xA1, (byte) 0xB2, (byte) 0xC3, (byte) 0xD4 };
        assertArrayEquals(expected, arg.value());
    }

    public void testOddLengthHexString() {
        HexadecimalArgument arg = new HexadecimalArgument("A1B2C3D4", 1, 7);
        byte[] expected = { (byte) 0x1B, (byte) 0x2C, (byte) 0x3D, (byte) 0x04 };
        assertArrayEquals(expected, arg.value());
    }

    public void testSingleDigitHexString() {
        HexadecimalArgument arg = new HexadecimalArgument("A1B2C3D4", 4, 1);
        byte[] expected = { (byte) 0x0C };
        assertArrayEquals(expected, arg.value());
    }

    public void testEmptyHexString() {
        HexadecimalArgument arg = new HexadecimalArgument("", 0, 0);
        byte[] expected = {};
        assertArrayEquals(expected, arg.value());
    }

    public void testEncodeMethod() {
        HexadecimalArgument arg = new HexadecimalArgument("A1B2C3D4", 0, 8);
        String expected = "obLD1A"; // Base64 encoding of {0xA1, 0xB2, 0xC3, 0xD4}
        assertEquals(expected, arg.encode());
    }
}
