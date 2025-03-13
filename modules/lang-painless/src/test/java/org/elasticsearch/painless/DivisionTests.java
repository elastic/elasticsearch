/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

/** Tests for division operator across all types */
// TODO: NaN/Inf/overflow/...
public class DivisionTests extends ScriptTestCase {

    // TODO: byte,short,char

    public void testBasics() throws Exception {
        assertEquals(2.25F / 1.5F, exec("return 2.25F / 1.5F;"));
        assertEquals(0.5, exec("double x = 1; float y = 2; return x / y;"));
    }

    public void testInt() throws Exception {
        assertEquals(1 / 1, exec("int x = 1; int y = 1; return x/y;"));
        assertEquals(2 / 3, exec("int x = 2; int y = 3; return x/y;"));
        assertEquals(5 / 10, exec("int x = 5; int y = 10; return x/y;"));
        assertEquals(10 / 1 / 2, exec("int x = 10; int y = 1; int z = 2; return x/y/z;"));
        assertEquals((10 / 1) / 2, exec("int x = 10; int y = 1; int z = 2; return (x/y)/z;"));
        assertEquals(10 / (4 / 2), exec("int x = 10; int y = 4; int z = 2; return x/(y/z);"));
        assertEquals(10 / 1, exec("int x = 10; int y = 1; return x/y;"));
        assertEquals(0 / 1, exec("int x = 0; int y = 1; return x/y;"));
    }

    public void testIntConst() throws Exception {
        assertEquals(1 / 1, exec("return 1/1;"));
        assertEquals(2 / 3, exec("return 2/3;"));
        assertEquals(5 / 10, exec("return 5/10;"));
        assertEquals(10 / 1 / 2, exec("return 10/1/2;"));
        assertEquals((10 / 1) / 2, exec("return (10/1)/2;"));
        assertEquals(10 / (4 / 2), exec("return 10/(4/2);"));
        assertEquals(10 / 1, exec("return 10/1;"));
        assertEquals(0 / 1, exec("return 0/1;"));
    }

    public void testLong() throws Exception {
        assertEquals(1L / 1L, exec("long x = 1; long y = 1; return x/y;"));
        assertEquals(2L / 3L, exec("long x = 2; long y = 3; return x/y;"));
        assertEquals(5L / 10L, exec("long x = 5; long y = 10; return x/y;"));
        assertEquals(10L / 1L / 2L, exec("long x = 10; long y = 1; long z = 2; return x/y/z;"));
        assertEquals((10L / 1L) / 2L, exec("long x = 10; long y = 1; long z = 2; return (x/y)/z;"));
        assertEquals(10L / (4L / 2L), exec("long x = 10; long y = 4; long z = 2; return x/(y/z);"));
        assertEquals(10L / 1L, exec("long x = 10; long y = 1; return x/y;"));
        assertEquals(0L / 1L, exec("long x = 0; long y = 1; return x/y;"));
    }

    public void testLongConst() throws Exception {
        assertEquals(1L / 1L, exec("return 1L/1L;"));
        assertEquals(2L / 3L, exec("return 2L/3L;"));
        assertEquals(5L / 10L, exec("return 5L/10L;"));
        assertEquals(10L / 1L / 2L, exec("return 10L/1L/2L;"));
        assertEquals((10L / 1L) / 2L, exec("return (10L/1L)/2L;"));
        assertEquals(10L / (4L / 2L), exec("return 10L/(4L/2L);"));
        assertEquals(10L / 1L, exec("return 10L/1L;"));
        assertEquals(0L / 1L, exec("return 0L/1L;"));
    }

    public void testFloat() throws Exception {
        assertEquals(1F / 1F, exec("float x = 1; float y = 1; return x/y;"));
        assertEquals(2F / 3F, exec("float x = 2; float y = 3; return x/y;"));
        assertEquals(5F / 10F, exec("float x = 5; float y = 10; return x/y;"));
        assertEquals(10F / 1F / 2F, exec("float x = 10; float y = 1; float z = 2; return x/y/z;"));
        assertEquals((10F / 1F) / 2F, exec("float x = 10; float y = 1; float z = 2; return (x/y)/z;"));
        assertEquals(10F / (4F / 2F), exec("float x = 10; float y = 4; float z = 2; return x/(y/z);"));
        assertEquals(10F / 1F, exec("float x = 10; float y = 1; return x/y;"));
        assertEquals(0F / 1F, exec("float x = 0; float y = 1; return x/y;"));
    }

    public void testFloatConst() throws Exception {
        assertEquals(1F / 1F, exec("return 1F/1F;"));
        assertEquals(2F / 3F, exec("return 2F/3F;"));
        assertEquals(5F / 10F, exec("return 5F/10F;"));
        assertEquals(10F / 1F / 2F, exec("return 10F/1F/2F;"));
        assertEquals((10F / 1F) / 2F, exec("return (10F/1F)/2F;"));
        assertEquals(10F / (4F / 2F), exec("return 10F/(4F/2F);"));
        assertEquals(10F / 1F, exec("return 10F/1F;"));
        assertEquals(0F / 1F, exec("return 0F/1F;"));
    }

    public void testDouble() throws Exception {
        assertEquals(1.0 / 1.0, exec("double x = 1; double y = 1; return x/y;"));
        assertEquals(2.0 / 3.0, exec("double x = 2; double y = 3; return x/y;"));
        assertEquals(5.0 / 10.0, exec("double x = 5; double y = 10; return x/y;"));
        assertEquals(10.0 / 1.0 / 2.0, exec("double x = 10; double y = 1; double z = 2; return x/y/z;"));
        assertEquals((10.0 / 1.0) / 2.0, exec("double x = 10; double y = 1; double z = 2; return (x/y)/z;"));
        assertEquals(10.0 / (4.0 / 2.0), exec("double x = 10; double y = 4; double z = 2; return x/(y/z);"));
        assertEquals(10.0 / 1.0, exec("double x = 10; double y = 1; return x/y;"));
        assertEquals(0.0 / 1.0, exec("double x = 0; double y = 1; return x/y;"));
    }

    public void testDoubleConst() throws Exception {
        assertEquals(1.0 / 1.0, exec("return 1.0/1.0;"));
        assertEquals(2.0 / 3.0, exec("return 2.0/3.0;"));
        assertEquals(5.0 / 10.0, exec("return 5.0/10.0;"));
        assertEquals(10.0 / 1.0 / 2.0, exec("return 10.0/1.0/2.0;"));
        assertEquals((10.0 / 1.0) / 2.0, exec("return (10.0/1.0)/2.0;"));
        assertEquals(10.0 / (4.0 / 2.0), exec("return 10.0/(4.0/2.0);"));
        assertEquals(10.0 / 1.0, exec("return 10.0/1.0;"));
        assertEquals(0.0 / 1.0, exec("return 0.0/1.0;"));
    }

    public void testDivideByZero() throws Exception {
        expectScriptThrows(ArithmeticException.class, () -> { exec("int x = 1; int y = 0; return x / y;"); });

        expectScriptThrows(ArithmeticException.class, () -> { exec("long x = 1L; long y = 0L; return x / y;"); });
    }

    public void testDivideByZeroConst() throws Exception {
        expectScriptThrows(ArithmeticException.class, () -> { exec("return 1/0;"); });

        expectScriptThrows(ArithmeticException.class, () -> { exec("return 1L/0L;"); });
    }

    public void testDef() {
        assertEquals(1, exec("def x = (byte)2; def y = (byte)2; return x / y"));
        assertEquals(1, exec("def x = (short)2; def y = (byte)2; return x / y"));
        assertEquals(1, exec("def x = (char)2; def y = (byte)2; return x / y"));
        assertEquals(1, exec("def x = (int)2; def y = (byte)2; return x / y"));
        assertEquals(1L, exec("def x = (long)2; def y = (byte)2; return x / y"));
        assertEquals(1F, exec("def x = (float)2; def y = (byte)2; return x / y"));
        assertEquals(1D, exec("def x = (double)2; def y = (byte)2; return x / y"));

        assertEquals(1, exec("def x = (byte)2; def y = (short)2; return x / y"));
        assertEquals(1, exec("def x = (short)2; def y = (short)2; return x / y"));
        assertEquals(1, exec("def x = (char)2; def y = (short)2; return x / y"));
        assertEquals(1, exec("def x = (int)2; def y = (short)2; return x / y"));
        assertEquals(1L, exec("def x = (long)2; def y = (short)2; return x / y"));
        assertEquals(1F, exec("def x = (float)2; def y = (short)2; return x / y"));
        assertEquals(1D, exec("def x = (double)2; def y = (short)2; return x / y"));

        assertEquals(1, exec("def x = (byte)2; def y = (char)2; return x / y"));
        assertEquals(1, exec("def x = (short)2; def y = (char)2; return x / y"));
        assertEquals(1, exec("def x = (char)2; def y = (char)2; return x / y"));
        assertEquals(1, exec("def x = (int)2; def y = (char)2; return x / y"));
        assertEquals(1L, exec("def x = (long)2; def y = (char)2; return x / y"));
        assertEquals(1F, exec("def x = (float)2; def y = (char)2; return x / y"));
        assertEquals(1D, exec("def x = (double)2; def y = (char)2; return x / y"));

        assertEquals(1, exec("def x = (byte)2; def y = (int)2; return x / y"));
        assertEquals(1, exec("def x = (short)2; def y = (int)2; return x / y"));
        assertEquals(1, exec("def x = (char)2; def y = (int)2; return x / y"));
        assertEquals(1, exec("def x = (int)2; def y = (int)2; return x / y"));
        assertEquals(1L, exec("def x = (long)2; def y = (int)2; return x / y"));
        assertEquals(1F, exec("def x = (float)2; def y = (int)2; return x / y"));
        assertEquals(1D, exec("def x = (double)2; def y = (int)2; return x / y"));

        assertEquals(1L, exec("def x = (byte)2; def y = (long)2; return x / y"));
        assertEquals(1L, exec("def x = (short)2; def y = (long)2; return x / y"));
        assertEquals(1L, exec("def x = (char)2; def y = (long)2; return x / y"));
        assertEquals(1L, exec("def x = (int)2; def y = (long)2; return x / y"));
        assertEquals(1L, exec("def x = (long)2; def y = (long)2; return x / y"));
        assertEquals(1F, exec("def x = (float)2; def y = (long)2; return x / y"));
        assertEquals(1D, exec("def x = (double)2; def y = (long)2; return x / y"));

        assertEquals(1F, exec("def x = (byte)2; def y = (float)2; return x / y"));
        assertEquals(1F, exec("def x = (short)2; def y = (float)2; return x / y"));
        assertEquals(1F, exec("def x = (char)2; def y = (float)2; return x / y"));
        assertEquals(1F, exec("def x = (int)2; def y = (float)2; return x / y"));
        assertEquals(1F, exec("def x = (long)2; def y = (float)2; return x / y"));
        assertEquals(1F, exec("def x = (float)2; def y = (float)2; return x / y"));
        assertEquals(1D, exec("def x = (double)2; def y = (float)2; return x / y"));

        assertEquals(1D, exec("def x = (byte)2; def y = (double)2; return x / y"));
        assertEquals(1D, exec("def x = (short)2; def y = (double)2; return x / y"));
        assertEquals(1D, exec("def x = (char)2; def y = (double)2; return x / y"));
        assertEquals(1D, exec("def x = (int)2; def y = (double)2; return x / y"));
        assertEquals(1D, exec("def x = (long)2; def y = (double)2; return x / y"));
        assertEquals(1D, exec("def x = (float)2; def y = (double)2; return x / y"));
        assertEquals(1D, exec("def x = (double)2; def y = (double)2; return x / y"));

        assertEquals(1, exec("def x = (byte)2; def y = (byte)2; return x / y"));
        assertEquals(1, exec("def x = (short)2; def y = (short)2; return x / y"));
        assertEquals(1, exec("def x = (char)2; def y = (char)2; return x / y"));
        assertEquals(1, exec("def x = (int)2; def y = (int)2; return x / y"));
        assertEquals(1L, exec("def x = (long)2; def y = (long)2; return x / y"));
        assertEquals(1F, exec("def x = (float)2; def y = (float)2; return x / y"));
        assertEquals(1D, exec("def x = (double)2; def y = (double)2; return x / y"));
    }

    public void testDefTypedLHS() {
        assertEquals(1, exec("byte x = (byte)2; def y = (byte)2; return x / y"));
        assertEquals(1, exec("short x = (short)2; def y = (byte)2; return x / y"));
        assertEquals(1, exec("char x = (char)2; def y = (byte)2; return x / y"));
        assertEquals(1, exec("int x = (int)2; def y = (byte)2; return x / y"));
        assertEquals(1L, exec("long x = (long)2; def y = (byte)2; return x / y"));
        assertEquals(1F, exec("float x = (float)2; def y = (byte)2; return x / y"));
        assertEquals(1D, exec("double x = (double)2; def y = (byte)2; return x / y"));

        assertEquals(1, exec("byte x = (byte)2; def y = (short)2; return x / y"));
        assertEquals(1, exec("short x = (short)2; def y = (short)2; return x / y"));
        assertEquals(1, exec("char x = (char)2; def y = (short)2; return x / y"));
        assertEquals(1, exec("int x = (int)2; def y = (short)2; return x / y"));
        assertEquals(1L, exec("long x = (long)2; def y = (short)2; return x / y"));
        assertEquals(1F, exec("float x = (float)2; def y = (short)2; return x / y"));
        assertEquals(1D, exec("double x = (double)2; def y = (short)2; return x / y"));

        assertEquals(1, exec("byte x = (byte)2; def y = (char)2; return x / y"));
        assertEquals(1, exec("short x = (short)2; def y = (char)2; return x / y"));
        assertEquals(1, exec("char x = (char)2; def y = (char)2; return x / y"));
        assertEquals(1, exec("int x = (int)2; def y = (char)2; return x / y"));
        assertEquals(1L, exec("long x = (long)2; def y = (char)2; return x / y"));
        assertEquals(1F, exec("float x = (float)2; def y = (char)2; return x / y"));
        assertEquals(1D, exec("double x = (double)2; def y = (char)2; return x / y"));

        assertEquals(1, exec("byte x = (byte)2; def y = (int)2; return x / y"));
        assertEquals(1, exec("short x = (short)2; def y = (int)2; return x / y"));
        assertEquals(1, exec("char x = (char)2; def y = (int)2; return x / y"));
        assertEquals(1, exec("int x = (int)2; def y = (int)2; return x / y"));
        assertEquals(1L, exec("long x = (long)2; def y = (int)2; return x / y"));
        assertEquals(1F, exec("float x = (float)2; def y = (int)2; return x / y"));
        assertEquals(1D, exec("double x = (double)2; def y = (int)2; return x / y"));

        assertEquals(1L, exec("byte x = (byte)2; def y = (long)2; return x / y"));
        assertEquals(1L, exec("short x = (short)2; def y = (long)2; return x / y"));
        assertEquals(1L, exec("char x = (char)2; def y = (long)2; return x / y"));
        assertEquals(1L, exec("int x = (int)2; def y = (long)2; return x / y"));
        assertEquals(1L, exec("long x = (long)2; def y = (long)2; return x / y"));
        assertEquals(1F, exec("float x = (float)2; def y = (long)2; return x / y"));
        assertEquals(1D, exec("double x = (double)2; def y = (long)2; return x / y"));

        assertEquals(1F, exec("byte x = (byte)2; def y = (float)2; return x / y"));
        assertEquals(1F, exec("short x = (short)2; def y = (float)2; return x / y"));
        assertEquals(1F, exec("char x = (char)2; def y = (float)2; return x / y"));
        assertEquals(1F, exec("int x = (int)2; def y = (float)2; return x / y"));
        assertEquals(1F, exec("long x = (long)2; def y = (float)2; return x / y"));
        assertEquals(1F, exec("float x = (float)2; def y = (float)2; return x / y"));
        assertEquals(1D, exec("double x = (double)2; def y = (float)2; return x / y"));

        assertEquals(1D, exec("byte x = (byte)2; def y = (double)2; return x / y"));
        assertEquals(1D, exec("short x = (short)2; def y = (double)2; return x / y"));
        assertEquals(1D, exec("char x = (char)2; def y = (double)2; return x / y"));
        assertEquals(1D, exec("int x = (int)2; def y = (double)2; return x / y"));
        assertEquals(1D, exec("long x = (long)2; def y = (double)2; return x / y"));
        assertEquals(1D, exec("float x = (float)2; def y = (double)2; return x / y"));
        assertEquals(1D, exec("double x = (double)2; def y = (double)2; return x / y"));

        assertEquals(1, exec("byte x = (byte)2; def y = (byte)2; return x / y"));
        assertEquals(1, exec("short x = (short)2; def y = (short)2; return x / y"));
        assertEquals(1, exec("char x = (char)2; def y = (char)2; return x / y"));
        assertEquals(1, exec("int x = (int)2; def y = (int)2; return x / y"));
        assertEquals(1L, exec("long x = (long)2; def y = (long)2; return x / y"));
        assertEquals(1F, exec("float x = (float)2; def y = (float)2; return x / y"));
        assertEquals(1D, exec("double x = (double)2; def y = (double)2; return x / y"));
    }

    public void testDefTypedRHS() {
        assertEquals(1, exec("def x = (byte)2; byte y = (byte)2; return x / y"));
        assertEquals(1, exec("def x = (short)2; byte y = (byte)2; return x / y"));
        assertEquals(1, exec("def x = (char)2; byte y = (byte)2; return x / y"));
        assertEquals(1, exec("def x = (int)2; byte y = (byte)2; return x / y"));
        assertEquals(1L, exec("def x = (long)2; byte y = (byte)2; return x / y"));
        assertEquals(1F, exec("def x = (float)2; byte y = (byte)2; return x / y"));
        assertEquals(1D, exec("def x = (double)2; byte y = (byte)2; return x / y"));

        assertEquals(1, exec("def x = (byte)2; short y = (short)2; return x / y"));
        assertEquals(1, exec("def x = (short)2; short y = (short)2; return x / y"));
        assertEquals(1, exec("def x = (char)2; short y = (short)2; return x / y"));
        assertEquals(1, exec("def x = (int)2; short y = (short)2; return x / y"));
        assertEquals(1L, exec("def x = (long)2; short y = (short)2; return x / y"));
        assertEquals(1F, exec("def x = (float)2; short y = (short)2; return x / y"));
        assertEquals(1D, exec("def x = (double)2; short y = (short)2; return x / y"));

        assertEquals(1, exec("def x = (byte)2; char y = (char)2; return x / y"));
        assertEquals(1, exec("def x = (short)2; char y = (char)2; return x / y"));
        assertEquals(1, exec("def x = (char)2; char y = (char)2; return x / y"));
        assertEquals(1, exec("def x = (int)2; char y = (char)2; return x / y"));
        assertEquals(1L, exec("def x = (long)2; char y = (char)2; return x / y"));
        assertEquals(1F, exec("def x = (float)2; char y = (char)2; return x / y"));
        assertEquals(1D, exec("def x = (double)2; char y = (char)2; return x / y"));

        assertEquals(1, exec("def x = (byte)2; int y = (int)2; return x / y"));
        assertEquals(1, exec("def x = (short)2; int y = (int)2; return x / y"));
        assertEquals(1, exec("def x = (char)2; int y = (int)2; return x / y"));
        assertEquals(1, exec("def x = (int)2; int y = (int)2; return x / y"));
        assertEquals(1L, exec("def x = (long)2; int y = (int)2; return x / y"));
        assertEquals(1F, exec("def x = (float)2; int y = (int)2; return x / y"));
        assertEquals(1D, exec("def x = (double)2; int y = (int)2; return x / y"));

        assertEquals(1L, exec("def x = (byte)2; long y = (long)2; return x / y"));
        assertEquals(1L, exec("def x = (short)2; long y = (long)2; return x / y"));
        assertEquals(1L, exec("def x = (char)2; long y = (long)2; return x / y"));
        assertEquals(1L, exec("def x = (int)2; long y = (long)2; return x / y"));
        assertEquals(1L, exec("def x = (long)2; long y = (long)2; return x / y"));
        assertEquals(1F, exec("def x = (float)2; long y = (long)2; return x / y"));
        assertEquals(1D, exec("def x = (double)2; long y = (long)2; return x / y"));

        assertEquals(1F, exec("def x = (byte)2; float y = (float)2; return x / y"));
        assertEquals(1F, exec("def x = (short)2; float y = (float)2; return x / y"));
        assertEquals(1F, exec("def x = (char)2; float y = (float)2; return x / y"));
        assertEquals(1F, exec("def x = (int)2; float y = (float)2; return x / y"));
        assertEquals(1F, exec("def x = (long)2; float y = (float)2; return x / y"));
        assertEquals(1F, exec("def x = (float)2; float y = (float)2; return x / y"));
        assertEquals(1D, exec("def x = (double)2; float y = (float)2; return x / y"));

        assertEquals(1D, exec("def x = (byte)2; double y = (double)2; return x / y"));
        assertEquals(1D, exec("def x = (short)2; double y = (double)2; return x / y"));
        assertEquals(1D, exec("def x = (char)2; double y = (double)2; return x / y"));
        assertEquals(1D, exec("def x = (int)2; double y = (double)2; return x / y"));
        assertEquals(1D, exec("def x = (long)2; double y = (double)2; return x / y"));
        assertEquals(1D, exec("def x = (float)2; double y = (double)2; return x / y"));
        assertEquals(1D, exec("def x = (double)2; double y = (double)2; return x / y"));

        assertEquals(1, exec("def x = (byte)2; byte y = (byte)2; return x / y"));
        assertEquals(1, exec("def x = (short)2; short y = (short)2; return x / y"));
        assertEquals(1, exec("def x = (char)2; char y = (char)2; return x / y"));
        assertEquals(1, exec("def x = (int)2; int y = (int)2; return x / y"));
        assertEquals(1L, exec("def x = (long)2; long y = (long)2; return x / y"));
        assertEquals(1F, exec("def x = (float)2; float y = (float)2; return x / y"));
        assertEquals(1D, exec("def x = (double)2; double y = (double)2; return x / y"));
    }

    public void testCompoundAssignment() {
        // byte
        assertEquals((byte) 15, exec("byte x = 45; x /= 3; return x;"));
        assertEquals((byte) -5, exec("byte x = 5; x /= -1; return x;"));
        // short
        assertEquals((short) 15, exec("short x = 45; x /= 3; return x;"));
        assertEquals((short) -5, exec("short x = 5; x /= -1; return x;"));
        // char
        assertEquals((char) 15, exec("char x = 45; x /= 3; return x;"));
        // int
        assertEquals(15, exec("int x = 45; x /= 3; return x;"));
        assertEquals(-5, exec("int x = 5; x /= -1; return x;"));
        // long
        assertEquals(15L, exec("long x = 45; x /= 3; return x;"));
        assertEquals(-5L, exec("long x = 5; x /= -1; return x;"));
        // float
        assertEquals(15F, exec("float x = 45f; x /= 3; return x;"));
        assertEquals(-5F, exec("float x = 5f; x /= -1; return x;"));
        // double
        assertEquals(15D, exec("double x = 45.0; x /= 3; return x;"));
        assertEquals(-5D, exec("double x = 5.0; x /= -1; return x;"));
    }

    public void testDefCompoundAssignment() {
        // byte
        assertEquals((byte) 15, exec("def x = (byte)45; x /= 3; return x;"));
        assertEquals((byte) -5, exec("def x = (byte)5; x /= -1; return x;"));
        // short
        assertEquals((short) 15, exec("def x = (short)45; x /= 3; return x;"));
        assertEquals((short) -5, exec("def x = (short)5; x /= -1; return x;"));
        // char
        assertEquals((char) 15, exec("def x = (char)45; x /= 3; return x;"));
        // int
        assertEquals(15, exec("def x = 45; x /= 3; return x;"));
        assertEquals(-5, exec("def x = 5; x /= -1; return x;"));
        // long
        assertEquals(15L, exec("def x = 45L; x /= 3; return x;"));
        assertEquals(-5L, exec("def x = 5L; x /= -1; return x;"));
        // float
        assertEquals(15F, exec("def x = 45f; x /= 3; return x;"));
        assertEquals(-5F, exec("def x = 5f; x /= -1; return x;"));
        // double
        assertEquals(15D, exec("def x = 45.0; x /= 3; return x;"));
        assertEquals(-5D, exec("def x = 5.0; x /= -1; return x;"));
    }

    public void testCompoundAssignmentByZero() {
        // byte
        expectScriptThrows(ArithmeticException.class, () -> { exec("byte x = 1; x /= 0; return x;"); });

        // short
        expectScriptThrows(ArithmeticException.class, () -> { exec("short x = 1; x /= 0; return x;"); });

        // char
        expectScriptThrows(ArithmeticException.class, () -> { exec("char x = 1; x /= 0; return x;"); });

        // int
        expectScriptThrows(ArithmeticException.class, () -> { exec("int x = 1; x /= 0; return x;"); });

        // long
        expectScriptThrows(ArithmeticException.class, () -> { exec("long x = 1; x /= 0; return x;"); });

        // def
        expectScriptThrows(ArithmeticException.class, () -> { exec("def x = 1; x /= 0; return x;"); });
    }
}
