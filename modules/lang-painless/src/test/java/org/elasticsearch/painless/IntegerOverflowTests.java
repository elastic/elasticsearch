/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

/** Tests integer overflow cases */
public class IntegerOverflowTests extends ScriptTestCase {

    public void testAssignmentAdditionOverflow() {
        // byte
        assertEquals((byte)(0 + 128), exec("byte x = 0; x += 128; return x;"));
        assertEquals((byte)(0 + -129), exec("byte x = 0; x += -129; return x;"));

        // short
        assertEquals((short)(0 + 32768), exec("short x = 0; x += 32768; return x;"));
        assertEquals((short)(0 + -32769), exec("short x = 0; x += -32769; return x;"));

        // char
        assertEquals((char)(0 + 65536), exec("char x = 0; x += 65536; return x;"));
        assertEquals((char)(0 + -65536), exec("char x = 0; x += -65536; return x;"));

        // int
        assertEquals(1 + 2147483647, exec("int x = 1; x += 2147483647; return x;"));
        assertEquals(-2 + -2147483647, exec("int x = -2; x += -2147483647; return x;"));

        // long
        assertEquals(1L + 9223372036854775807L, exec("long x = 1; x += 9223372036854775807L; return x;"));
        assertEquals(-2L + -9223372036854775807L, exec("long x = -2; x += -9223372036854775807L; return x;"));
    }

    public void testAssignmentSubtractionOverflow() {
        // byte
        assertEquals((byte)(0 - -128), exec("byte x = 0; x -= -128; return x;"));
        assertEquals((byte)(0 - 129), exec("byte x = 0; x -= 129; return x;"));

        // short
        assertEquals((short)(0 - -32768), exec("short x = 0; x -= -32768; return x;"));
        assertEquals((short)(0 - 32769), exec("short x = 0; x -= 32769; return x;"));

        // char
        assertEquals((char)(0 - -65536), exec("char x = 0; x -= -65536; return x;"));
        assertEquals((char)(0 - 65536), exec("char x = 0; x -= 65536; return x;"));

        // int
        assertEquals(1 - -2147483647, exec("int x = 1; x -= -2147483647; return x;"));
        assertEquals(-2 - 2147483647, exec("int x = -2; x -= 2147483647; return x;"));

        // long
        assertEquals(1L - -9223372036854775807L, exec("long x = 1; x -= -9223372036854775807L; return x;"));
        assertEquals(-2L - 9223372036854775807L, exec("long x = -2; x -= 9223372036854775807L; return x;"));
    }

    public void testAssignmentMultiplicationOverflow() {
        // byte
        assertEquals((byte) (2 * 128), exec("byte x = 2; x *= 128; return x;"));
        assertEquals((byte) (2 * -128), exec("byte x = 2; x *= -128; return x;"));

        // char
        assertEquals((char) (2 * 65536), exec("char x = 2; x *= 65536; return x;"));
        assertEquals((char) (2 * -65536), exec("char x = 2; x *= -65536; return x;"));

        // int
        assertEquals(2 * 2147483647, exec("int x = 2; x *= 2147483647; return x;"));
        assertEquals(2 * -2147483647, exec("int x = 2; x *= -2147483647; return x;"));

        // long
        assertEquals(2L * 9223372036854775807L, exec("long x = 2; x *= 9223372036854775807L; return x;"));
        assertEquals(2L * -9223372036854775807L, exec("long x = 2; x *= -9223372036854775807L; return x;"));
    }

    public void testAssignmentDivisionOverflow() {
        // byte
        assertEquals((byte) (-128 / -1), exec("byte x = (byte) -128; x /= -1; return x;"));

        // short
        assertEquals((short) (-32768 / -1), exec("short x = (short) -32768; x /= -1; return x;"));

        // cannot happen for char: unsigned

        // int
        assertEquals((-2147483647 - 1) / -1, exec("int x = -2147483647 - 1; x /= -1; return x;"));

        // long
        assertEquals((-9223372036854775807L - 1L) / -1L, exec("long x = -9223372036854775807L - 1L; x /=-1L; return x;"));
    }

    public void testIncrementOverFlow() throws Exception {
        // byte
        assertEquals((byte) 128, exec("byte x = 127; ++x; return x;"));
        assertEquals((byte) 128, exec("byte x = 127; x++; return x;"));
        assertEquals((byte) -129, exec("byte x = (byte) -128; --x; return x;"));
        assertEquals((byte) -129, exec("byte x = (byte) -128; x--; return x;"));

        // short
        assertEquals((short) 32768, exec("short x = 32767; ++x; return x;"));
        assertEquals((short) 32768, exec("short x = 32767; x++; return x;"));
        assertEquals((short) -32769, exec("short x = (short) -32768; --x; return x;"));
        assertEquals((short) -32769, exec("short x = (short) -32768; x--; return x;"));

        // char
        assertEquals((char) 65536, exec("char x = 65535; ++x; return x;"));
        assertEquals((char) 65536, exec("char x = 65535; x++; return x;"));
        assertEquals((char) -1, exec("char x = (char) 0; --x; return x;"));
        assertEquals((char) -1, exec("char x = (char) 0; x--; return x;"));

        // int
        assertEquals(2147483647 + 1, exec("int x = 2147483647; ++x; return x;"));
        assertEquals(2147483647 + 1, exec("int x = 2147483647; x++; return x;"));
        assertEquals(-2147483648 - 1, exec("int x = (int) -2147483648L; --x; return x;"));
        assertEquals(-2147483648 - 1, exec("int x = (int) -2147483648L; x--; return x;"));

        // long
        assertEquals(9223372036854775807L + 1L, exec("long x = 9223372036854775807L; ++x; return x;"));
        assertEquals(9223372036854775807L + 1L, exec("long x = 9223372036854775807L; x++; return x;"));
        assertEquals(-9223372036854775807L - 1L - 1L, exec("long x = -9223372036854775807L - 1L; --x; return x;"));
        assertEquals(-9223372036854775807L - 1L - 1L, exec("long x = -9223372036854775807L - 1L; x--; return x;"));
    }

    public void testAddition() throws Exception {
        assertEquals(2147483647 + 2147483647, exec("int x = 2147483647; int y = 2147483647; return x + y;"));
        assertEquals(9223372036854775807L + 9223372036854775807L,
                exec("long x = 9223372036854775807L; long y = 9223372036854775807L; return x + y;"));
    }

    public void testAdditionConst() throws Exception {
        assertEquals(2147483647 + 2147483647, exec("return 2147483647 + 2147483647;"));
        assertEquals(9223372036854775807L + 9223372036854775807L, exec("return 9223372036854775807L + 9223372036854775807L;"));
    }

    public void testSubtraction() throws Exception {
        assertEquals(-10 - 2147483647, exec("int x = -10; int y = 2147483647; return x - y;"));
        assertEquals(-10L - 9223372036854775807L, exec("long x = -10L; long y = 9223372036854775807L; return x - y;"));
    }

    public void testSubtractionConst() throws Exception {
        assertEquals(-10 - 2147483647, exec("return -10 - 2147483647;"));
        assertEquals(-10L - 9223372036854775807L, exec("return -10L - 9223372036854775807L;"));
    }

    public void testMultiplication() throws Exception {
        assertEquals(2147483647 * 2147483647, exec("int x = 2147483647; int y = 2147483647; return x * y;"));
        assertEquals(9223372036854775807L * 9223372036854775807L,
                exec("long x = 9223372036854775807L; long y = 9223372036854775807L; return x * y;"));
    }

    public void testMultiplicationConst() throws Exception {
        assertEquals(2147483647 * 2147483647, exec("return 2147483647 * 2147483647;"));
        assertEquals(9223372036854775807L * 9223372036854775807L, exec("return 9223372036854775807L * 9223372036854775807L;"));
    }

    public void testDivision() throws Exception {
        assertEquals((-2147483647 - 1) / -1, exec("int x = -2147483648; int y = -1; return x / y;"));
        assertEquals((-9223372036854775807L - 1L) / -1L, exec("long x = -9223372036854775808L; long y = -1L; return x / y;"));
    }

    public void testDivisionConst() throws Exception {
        assertEquals((-2147483647 - 1) / -1, exec("return (-2147483648) / -1;"));
        assertEquals((-9223372036854775807L - 1L) / -1L, exec("return (-9223372036854775808L) / -1L;"));
    }

    public void testNegationOverflow() throws Exception {
        assertEquals(-(-2147483647 - 1), exec("int x = -2147483648; x = -x; return x;"));
        assertEquals(-(-9223372036854775807L - 1L), exec("long x = -9223372036854775808L; x = -x; return x;"));
    }

    public void testNegationOverflowConst() throws Exception {
        assertEquals(-(-2147483647 - 1), exec("int x = -(-2147483648); return x;"));
        assertEquals(-(-9223372036854775807L - 1L), exec("long x = -(-9223372036854775808L); return x;"));
    }
}
