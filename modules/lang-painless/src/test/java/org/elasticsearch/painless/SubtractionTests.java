/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

/** Tests for subtraction operator across all types */
// TODO: NaN/Inf/overflow/...
public class SubtractionTests extends ScriptTestCase {

    public void testBasics() throws Exception {
        assertEquals(2 - 1, exec("return 2 - 1;"));
        assertEquals(-1, exec("int x = 1; char y = 2; return x - y;"));
    }

    public void testInt() throws Exception {
        assertEquals(1 - 1, exec("int x = 1; int y = 1; return x-y;"));
        assertEquals(2 - 3, exec("int x = 2; int y = 3; return x-y;"));
        assertEquals(5 - 10, exec("int x = 5; int y = 10; return x-y;"));
        assertEquals(1 - 1 - 2, exec("int x = 1; int y = 1; int z = 2; return x-y-z;"));
        assertEquals((1 - 1) - 2, exec("int x = 1; int y = 1; int z = 2; return (x-y)-z;"));
        assertEquals(1 - (1 - 2), exec("int x = 1; int y = 1; int z = 2; return x-(y-z);"));
        assertEquals(10 - 0, exec("int x = 10; int y = 0; return x-y;"));
        assertEquals(0 - 0, exec("int x = 0; int y = 0; return x-x;"));
    }

    public void testIntConst() throws Exception {
        assertEquals(1 - 1, exec("return 1-1;"));
        assertEquals(2 - 3, exec("return 2-3;"));
        assertEquals(5 - 10, exec("return 5-10;"));
        assertEquals(1 - 1 - 2, exec("return 1-1-2;"));
        assertEquals((1 - 1) - 2, exec("return (1-1)-2;"));
        assertEquals(1 - (1 - 2), exec("return 1-(1-2);"));
        assertEquals(10 - 0, exec("return 10-0;"));
        assertEquals(0 - 0, exec("return 0-0;"));
    }

    public void testByte() throws Exception {
        assertEquals((byte) 1 - (byte) 1, exec("byte x = 1; byte y = 1; return x-y;"));
        assertEquals((byte) 2 - (byte) 3, exec("byte x = 2; byte y = 3; return x-y;"));
        assertEquals((byte) 5 - (byte) 10, exec("byte x = 5; byte y = 10; return x-y;"));
        assertEquals((byte) 1 - (byte) 1 - (byte) 2, exec("byte x = 1; byte y = 1; byte z = 2; return x-y-z;"));
        assertEquals(((byte) 1 - (byte) 1) - (byte) 2, exec("byte x = 1; byte y = 1; byte z = 2; return (x-y)-z;"));
        assertEquals((byte) 1 - ((byte) 1 - (byte) 2), exec("byte x = 1; byte y = 1; byte z = 2; return x-(y-z);"));
        assertEquals((byte) 10 - (byte) 1, exec("byte x = 10; byte y = 1; return x-y;"));
        assertEquals((byte) 0 - (byte) 0, exec("byte x = 0; byte y = 0; return x-y;"));
    }

    public void testByteConst() throws Exception {
        assertEquals((byte) 1 - (byte) 1, exec("return (byte)1-(byte)1;"));
        assertEquals((byte) 2 - (byte) 3, exec("return (byte)2-(byte)3;"));
        assertEquals((byte) 5 - (byte) 10, exec("return (byte)5-(byte)10;"));
        assertEquals((byte) 1 - (byte) 1 - (byte) 2, exec("return (byte)1-(byte)1-(byte)2;"));
        assertEquals(((byte) 1 - (byte) 1) - (byte) 2, exec("return ((byte)1-(byte)1)-(byte)2;"));
        assertEquals((byte) 1 - ((byte) 1 - (byte) 2), exec("return (byte)1-((byte)1-(byte)2);"));
        assertEquals((byte) 10 - (byte) 1, exec("return (byte)10-(byte)1;"));
        assertEquals((byte) 0 - (byte) 0, exec("return (byte)0-(byte)0;"));
    }

    public void testChar() throws Exception {
        assertEquals((char) 1 - (char) 1, exec("char x = 1; char y = 1; return x-y;"));
        assertEquals((char) 2 - (char) 3, exec("char x = 2; char y = 3; return x-y;"));
        assertEquals((char) 5 - (char) 10, exec("char x = 5; char y = 10; return x-y;"));
        assertEquals((char) 1 - (char) 1 - (char) 2, exec("char x = 1; char y = 1; char z = 2; return x-y-z;"));
        assertEquals(((char) 1 - (char) 1) - (char) 2, exec("char x = 1; char y = 1; char z = 2; return (x-y)-z;"));
        assertEquals((char) 1 - ((char) 1 - (char) 2), exec("char x = 1; char y = 1; char z = 2; return x-(y-z);"));
        assertEquals((char) 10 - (char) 1, exec("char x = 10; char y = 1; return x-y;"));
        assertEquals((char) 0 - (char) 0, exec("char x = 0; char y = 0; return x-y;"));
    }

    public void testCharConst() throws Exception {
        assertEquals((char) 1 - (char) 1, exec("return (char)1-(char)1;"));
        assertEquals((char) 2 - (char) 3, exec("return (char)2-(char)3;"));
        assertEquals((char) 5 - (char) 10, exec("return (char)5-(char)10;"));
        assertEquals((char) 1 - (char) 1 - (char) 2, exec("return (char)1-(char)1-(char)2;"));
        assertEquals(((char) 1 - (char) 1) - (char) 2, exec("return ((char)1-(char)1)-(char)2;"));
        assertEquals((char) 1 - ((char) 1 - (char) 2), exec("return (char)1-((char)1-(char)2);"));
        assertEquals((char) 10 - (char) 1, exec("return (char)10-(char)1;"));
        assertEquals((char) 0 - (char) 0, exec("return (char)0-(char)0;"));
    }

    public void testShort() throws Exception {
        assertEquals((short) 1 - (short) 1, exec("short x = 1; short y = 1; return x-y;"));
        assertEquals((short) 2 - (short) 3, exec("short x = 2; short y = 3; return x-y;"));
        assertEquals((short) 5 - (short) 10, exec("short x = 5; short y = 10; return x-y;"));
        assertEquals((short) 1 - (short) 1 - (short) 2, exec("short x = 1; short y = 1; short z = 2; return x-y-z;"));
        assertEquals(((short) 1 - (short) 1) - (short) 2, exec("short x = 1; short y = 1; short z = 2; return (x-y)-z;"));
        assertEquals((short) 1 - ((short) 1 - (short) 2), exec("short x = 1; short y = 1; short z = 2; return x-(y-z);"));
        assertEquals((short) 10 - (short) 1, exec("short x = 10; short y = 1; return x-y;"));
        assertEquals((short) 0 - (short) 0, exec("short x = 0; short y = 0; return x-y;"));
    }

    public void testShortConst() throws Exception {
        assertEquals((short) 1 - (short) 1, exec("return (short)1-(short)1;"));
        assertEquals((short) 2 - (short) 3, exec("return (short)2-(short)3;"));
        assertEquals((short) 5 - (short) 10, exec("return (short)5-(short)10;"));
        assertEquals((short) 1 - (short) 1 - (short) 2, exec("return (short)1-(short)1-(short)2;"));
        assertEquals(((short) 1 - (short) 1) - (short) 2, exec("return ((short)1-(short)1)-(short)2;"));
        assertEquals((short) 1 - ((short) 1 - (short) 2), exec("return (short)1-((short)1-(short)2);"));
        assertEquals((short) 10 - (short) 1, exec("return (short)10-(short)1;"));
        assertEquals((short) 0 - (short) 0, exec("return (short)0-(short)0;"));
    }

    public void testLong() throws Exception {
        assertEquals(1L - 1L, exec("long x = 1; long y = 1; return x-y;"));
        assertEquals(2L - 3L, exec("long x = 2; long y = 3; return x-y;"));
        assertEquals(5L - 10L, exec("long x = 5; long y = 10; return x-y;"));
        assertEquals(1L - 1L - 2L, exec("long x = 1; long y = 1; int z = 2; return x-y-z;"));
        assertEquals((1L - 1L) - 2L, exec("long x = 1; long y = 1; int z = 2; return (x-y)-z;"));
        assertEquals(1L - (1L - 2L), exec("long x = 1; long y = 1; int z = 2; return x-(y-z);"));
        assertEquals(10L - 0L, exec("long x = 10; long y = 0; return x-y;"));
        assertEquals(0L - 0L, exec("long x = 0; long y = 0; return x-x;"));
    }

    public void testLongConst() throws Exception {
        assertEquals(1L - 1L, exec("return 1L-1L;"));
        assertEquals(2L - 3L, exec("return 2L-3L;"));
        assertEquals(5L - 10L, exec("return 5L-10L;"));
        assertEquals(1L - 1L - 2L, exec("return 1L-1L-2L;"));
        assertEquals((1L - 1L) - 2L, exec("return (1L-1L)-2L;"));
        assertEquals(1L - (1L - 2L), exec("return 1L-(1L-2L);"));
        assertEquals(10L - 0L, exec("return 10L-0L;"));
        assertEquals(0L - 0L, exec("return 0L-0L;"));
    }

    public void testFloat() throws Exception {
        assertEquals(1F - 1F, exec("float x = 1; float y = 1; return x-y;"));
        assertEquals(2F - 3F, exec("float x = 2; float y = 3; return x-y;"));
        assertEquals(5F - 10F, exec("float x = 5; float y = 10; return x-y;"));
        assertEquals(1F - 1F - 2F, exec("float x = 1; float y = 1; float z = 2; return x-y-z;"));
        assertEquals((1F - 1F) - 2F, exec("float x = 1; float y = 1; float z = 2; return (x-y)-z;"));
        assertEquals(1F - (1F - 2F), exec("float x = 1; float y = 1; float z = 2; return x-(y-z);"));
        assertEquals(10F - 0F, exec("float x = 10; float y = 0; return x-y;"));
        assertEquals(0F - 0F, exec("float x = 0; float y = 0; return x-x;"));
    }

    public void testFloatConst() throws Exception {
        assertEquals(1F - 1F, exec("return 1F-1F;"));
        assertEquals(2F - 3F, exec("return 2F-3F;"));
        assertEquals(5F - 10F, exec("return 5F-10F;"));
        assertEquals(1F - 1F - 2F, exec("return 1F-1F-2F;"));
        assertEquals((1F - 1F) - 2F, exec("return (1F-1F)-2F;"));
        assertEquals(1F - (1F - 2F), exec("return 1F-(1F-2F);"));
        assertEquals(10F - 0F, exec("return 10F-0F;"));
        assertEquals(0F - 0F, exec("return 0F-0F;"));
    }

    public void testDouble() throws Exception {
        assertEquals(1D - 1D, exec("double x = 1; double y = 1; return x-y;"));
        assertEquals(2D - 3D, exec("double x = 2; double y = 3; return x-y;"));
        assertEquals(5D - 10D, exec("double x = 5; double y = 10; return x-y;"));
        assertEquals(1D - 1D - 2D, exec("double x = 1; double y = 1; double z = 2; return x-y-z;"));
        assertEquals((1D - 1D) - 2D, exec("double x = 1; double y = 1; double z = 2; return (x-y)-z;"));
        assertEquals(1D - (1D - 2D), exec("double x = 1; double y = 1; double z = 2; return x-(y-z);"));
        assertEquals(10D - 0D, exec("double x = 10; float y = 0; return x-y;"));
        assertEquals(0D - 0D, exec("double x = 0; float y = 0; return x-x;"));
    }

    public void testyDoubleConst() throws Exception {
        assertEquals(1.0 - 1.0, exec("return 1.0-1.0;"));
        assertEquals(2.0 - 3.0, exec("return 2.0-3.0;"));
        assertEquals(5.0 - 10.0, exec("return 5.0-10.0;"));
        assertEquals(1.0 - 1.0 - 2.0, exec("return 1.0-1.0-2.0;"));
        assertEquals((1.0 - 1.0) - 2.0, exec("return (1.0-1.0)-2.0;"));
        assertEquals(1.0 - (1.0 - 2.0), exec("return 1.0-(1.0-2.0);"));
        assertEquals(10.0 - 0.0, exec("return 10.0-0.0;"));
        assertEquals(0.0 - 0.0, exec("return 0.0-0.0;"));
    }

    public void testDef() {
        assertEquals(0, exec("def x = (byte)1; def y = (byte)1; return x - y"));
        assertEquals(0, exec("def x = (short)1; def y = (byte)1; return x - y"));
        assertEquals(0, exec("def x = (char)1; def y = (byte)1; return x - y"));
        assertEquals(0, exec("def x = (int)1; def y = (byte)1; return x - y"));
        assertEquals(0L, exec("def x = (long)1; def y = (byte)1; return x - y"));
        assertEquals(0F, exec("def x = (float)1; def y = (byte)1; return x - y"));
        assertEquals(0D, exec("def x = (double)1; def y = (byte)1; return x - y"));

        assertEquals(0, exec("def x = (byte)1; def y = (short)1; return x - y"));
        assertEquals(0, exec("def x = (short)1; def y = (short)1; return x - y"));
        assertEquals(0, exec("def x = (char)1; def y = (short)1; return x - y"));
        assertEquals(0, exec("def x = (int)1; def y = (short)1; return x - y"));
        assertEquals(0L, exec("def x = (long)1; def y = (short)1; return x - y"));
        assertEquals(0F, exec("def x = (float)1; def y = (short)1; return x - y"));
        assertEquals(0D, exec("def x = (double)1; def y = (short)1; return x - y"));

        assertEquals(0, exec("def x = (byte)1; def y = (char)1; return x - y"));
        assertEquals(0, exec("def x = (short)1; def y = (char)1; return x - y"));
        assertEquals(0, exec("def x = (char)1; def y = (char)1; return x - y"));
        assertEquals(0, exec("def x = (int)1; def y = (char)1; return x - y"));
        assertEquals(0L, exec("def x = (long)1; def y = (char)1; return x - y"));
        assertEquals(0F, exec("def x = (float)1; def y = (char)1; return x - y"));
        assertEquals(0D, exec("def x = (double)1; def y = (char)1; return x - y"));

        assertEquals(0, exec("def x = (byte)1; def y = (int)1; return x - y"));
        assertEquals(0, exec("def x = (short)1; def y = (int)1; return x - y"));
        assertEquals(0, exec("def x = (char)1; def y = (int)1; return x - y"));
        assertEquals(0, exec("def x = (int)1; def y = (int)1; return x - y"));
        assertEquals(0L, exec("def x = (long)1; def y = (int)1; return x - y"));
        assertEquals(0F, exec("def x = (float)1; def y = (int)1; return x - y"));
        assertEquals(0D, exec("def x = (double)1; def y = (int)1; return x - y"));

        assertEquals(0L, exec("def x = (byte)1; def y = (long)1; return x - y"));
        assertEquals(0L, exec("def x = (short)1; def y = (long)1; return x - y"));
        assertEquals(0L, exec("def x = (char)1; def y = (long)1; return x - y"));
        assertEquals(0L, exec("def x = (int)1; def y = (long)1; return x - y"));
        assertEquals(0L, exec("def x = (long)1; def y = (long)1; return x - y"));
        assertEquals(0F, exec("def x = (float)1; def y = (long)1; return x - y"));
        assertEquals(0D, exec("def x = (double)1; def y = (long)1; return x - y"));

        assertEquals(0F, exec("def x = (byte)1; def y = (float)1; return x - y"));
        assertEquals(0F, exec("def x = (short)1; def y = (float)1; return x - y"));
        assertEquals(0F, exec("def x = (char)1; def y = (float)1; return x - y"));
        assertEquals(0F, exec("def x = (int)1; def y = (float)1; return x - y"));
        assertEquals(0F, exec("def x = (long)1; def y = (float)1; return x - y"));
        assertEquals(0F, exec("def x = (float)1; def y = (float)1; return x - y"));
        assertEquals(0D, exec("def x = (double)1; def y = (float)1; return x - y"));

        assertEquals(0D, exec("def x = (byte)1; def y = (double)1; return x - y"));
        assertEquals(0D, exec("def x = (short)1; def y = (double)1; return x - y"));
        assertEquals(0D, exec("def x = (char)1; def y = (double)1; return x - y"));
        assertEquals(0D, exec("def x = (int)1; def y = (double)1; return x - y"));
        assertEquals(0D, exec("def x = (long)1; def y = (double)1; return x - y"));
        assertEquals(0D, exec("def x = (float)1; def y = (double)1; return x - y"));
        assertEquals(0D, exec("def x = (double)1; def y = (double)1; return x - y"));
    }

    public void testDefTypedLHS() {
        assertEquals(0, exec("byte x = (byte)1; def y = (byte)1; return x - y"));
        assertEquals(0, exec("short x = (short)1; def y = (byte)1; return x - y"));
        assertEquals(0, exec("char x = (char)1; def y = (byte)1; return x - y"));
        assertEquals(0, exec("int x = (int)1; def y = (byte)1; return x - y"));
        assertEquals(0L, exec("long x = (long)1; def y = (byte)1; return x - y"));
        assertEquals(0F, exec("float x = (float)1; def y = (byte)1; return x - y"));
        assertEquals(0D, exec("double x = (double)1; def y = (byte)1; return x - y"));

        assertEquals(0, exec("byte x = (byte)1; def y = (short)1; return x - y"));
        assertEquals(0, exec("short x = (short)1; def y = (short)1; return x - y"));
        assertEquals(0, exec("char x = (char)1; def y = (short)1; return x - y"));
        assertEquals(0, exec("int x = (int)1; def y = (short)1; return x - y"));
        assertEquals(0L, exec("long x = (long)1; def y = (short)1; return x - y"));
        assertEquals(0F, exec("float x = (float)1; def y = (short)1; return x - y"));
        assertEquals(0D, exec("double x = (double)1; def y = (short)1; return x - y"));

        assertEquals(0, exec("byte x = (byte)1; def y = (char)1; return x - y"));
        assertEquals(0, exec("short x = (short)1; def y = (char)1; return x - y"));
        assertEquals(0, exec("char x = (char)1; def y = (char)1; return x - y"));
        assertEquals(0, exec("int x = (int)1; def y = (char)1; return x - y"));
        assertEquals(0L, exec("long x = (long)1; def y = (char)1; return x - y"));
        assertEquals(0F, exec("float x = (float)1; def y = (char)1; return x - y"));
        assertEquals(0D, exec("double x = (double)1; def y = (char)1; return x - y"));

        assertEquals(0, exec("byte x = (byte)1; def y = (int)1; return x - y"));
        assertEquals(0, exec("short x = (short)1; def y = (int)1; return x - y"));
        assertEquals(0, exec("char x = (char)1; def y = (int)1; return x - y"));
        assertEquals(0, exec("int x = (int)1; def y = (int)1; return x - y"));
        assertEquals(0L, exec("long x = (long)1; def y = (int)1; return x - y"));
        assertEquals(0F, exec("float x = (float)1; def y = (int)1; return x - y"));
        assertEquals(0D, exec("double x = (double)1; def y = (int)1; return x - y"));

        assertEquals(0L, exec("byte x = (byte)1; def y = (long)1; return x - y"));
        assertEquals(0L, exec("short x = (short)1; def y = (long)1; return x - y"));
        assertEquals(0L, exec("char x = (char)1; def y = (long)1; return x - y"));
        assertEquals(0L, exec("int x = (int)1; def y = (long)1; return x - y"));
        assertEquals(0L, exec("long x = (long)1; def y = (long)1; return x - y"));
        assertEquals(0F, exec("float x = (float)1; def y = (long)1; return x - y"));
        assertEquals(0D, exec("double x = (double)1; def y = (long)1; return x - y"));

        assertEquals(0F, exec("byte x = (byte)1; def y = (float)1; return x - y"));
        assertEquals(0F, exec("short x = (short)1; def y = (float)1; return x - y"));
        assertEquals(0F, exec("char x = (char)1; def y = (float)1; return x - y"));
        assertEquals(0F, exec("int x = (int)1; def y = (float)1; return x - y"));
        assertEquals(0F, exec("long x = (long)1; def y = (float)1; return x - y"));
        assertEquals(0F, exec("float x = (float)1; def y = (float)1; return x - y"));
        assertEquals(0D, exec("double x = (double)1; def y = (float)1; return x - y"));

        assertEquals(0D, exec("byte x = (byte)1; def y = (double)1; return x - y"));
        assertEquals(0D, exec("short x = (short)1; def y = (double)1; return x - y"));
        assertEquals(0D, exec("char x = (char)1; def y = (double)1; return x - y"));
        assertEquals(0D, exec("int x = (int)1; def y = (double)1; return x - y"));
        assertEquals(0D, exec("long x = (long)1; def y = (double)1; return x - y"));
        assertEquals(0D, exec("float x = (float)1; def y = (double)1; return x - y"));
        assertEquals(0D, exec("double x = (double)1; def y = (double)1; return x - y"));
    }

    public void testDefTypedRHS() {
        assertEquals(0, exec("def x = (byte)1; byte y = (byte)1; return x - y"));
        assertEquals(0, exec("def x = (short)1; byte y = (byte)1; return x - y"));
        assertEquals(0, exec("def x = (char)1; byte y = (byte)1; return x - y"));
        assertEquals(0, exec("def x = (int)1; byte y = (byte)1; return x - y"));
        assertEquals(0L, exec("def x = (long)1; byte y = (byte)1; return x - y"));
        assertEquals(0F, exec("def x = (float)1; byte y = (byte)1; return x - y"));
        assertEquals(0D, exec("def x = (double)1; byte y = (byte)1; return x - y"));

        assertEquals(0, exec("def x = (byte)1; short y = (short)1; return x - y"));
        assertEquals(0, exec("def x = (short)1; short y = (short)1; return x - y"));
        assertEquals(0, exec("def x = (char)1; short y = (short)1; return x - y"));
        assertEquals(0, exec("def x = (int)1; short y = (short)1; return x - y"));
        assertEquals(0L, exec("def x = (long)1; short y = (short)1; return x - y"));
        assertEquals(0F, exec("def x = (float)1; short y = (short)1; return x - y"));
        assertEquals(0D, exec("def x = (double)1; short y = (short)1; return x - y"));

        assertEquals(0, exec("def x = (byte)1; char y = (char)1; return x - y"));
        assertEquals(0, exec("def x = (short)1; char y = (char)1; return x - y"));
        assertEquals(0, exec("def x = (char)1; char y = (char)1; return x - y"));
        assertEquals(0, exec("def x = (int)1; char y = (char)1; return x - y"));
        assertEquals(0L, exec("def x = (long)1; char y = (char)1; return x - y"));
        assertEquals(0F, exec("def x = (float)1; char y = (char)1; return x - y"));
        assertEquals(0D, exec("def x = (double)1; char y = (char)1; return x - y"));

        assertEquals(0, exec("def x = (byte)1; int y = (int)1; return x - y"));
        assertEquals(0, exec("def x = (short)1; int y = (int)1; return x - y"));
        assertEquals(0, exec("def x = (char)1; int y = (int)1; return x - y"));
        assertEquals(0, exec("def x = (int)1; int y = (int)1; return x - y"));
        assertEquals(0L, exec("def x = (long)1; int y = (int)1; return x - y"));
        assertEquals(0F, exec("def x = (float)1; int y = (int)1; return x - y"));
        assertEquals(0D, exec("def x = (double)1; int y = (int)1; return x - y"));

        assertEquals(0L, exec("def x = (byte)1; long y = (long)1; return x - y"));
        assertEquals(0L, exec("def x = (short)1; long y = (long)1; return x - y"));
        assertEquals(0L, exec("def x = (char)1; long y = (long)1; return x - y"));
        assertEquals(0L, exec("def x = (int)1; long y = (long)1; return x - y"));
        assertEquals(0L, exec("def x = (long)1; long y = (long)1; return x - y"));
        assertEquals(0F, exec("def x = (float)1; long y = (long)1; return x - y"));
        assertEquals(0D, exec("def x = (double)1; long y = (long)1; return x - y"));

        assertEquals(0F, exec("def x = (byte)1; float y = (float)1; return x - y"));
        assertEquals(0F, exec("def x = (short)1; float y = (float)1; return x - y"));
        assertEquals(0F, exec("def x = (char)1; float y = (float)1; return x - y"));
        assertEquals(0F, exec("def x = (int)1; float y = (float)1; return x - y"));
        assertEquals(0F, exec("def x = (long)1; float y = (float)1; return x - y"));
        assertEquals(0F, exec("def x = (float)1; float y = (float)1; return x - y"));
        assertEquals(0D, exec("def x = (double)1; float y = (float)1; return x - y"));

        assertEquals(0D, exec("def x = (byte)1; double y = (double)1; return x - y"));
        assertEquals(0D, exec("def x = (short)1; double y = (double)1; return x - y"));
        assertEquals(0D, exec("def x = (char)1; double y = (double)1; return x - y"));
        assertEquals(0D, exec("def x = (int)1; double y = (double)1; return x - y"));
        assertEquals(0D, exec("def x = (long)1; double y = (double)1; return x - y"));
        assertEquals(0D, exec("def x = (float)1; double y = (double)1; return x - y"));
        assertEquals(0D, exec("def x = (double)1; double y = (double)1; return x - y"));
    }

    public void testCompoundAssignment() {
        // byte
        assertEquals((byte) 15, exec("byte x = 5; x -= -10; return x;"));
        assertEquals((byte) -5, exec("byte x = 5; x -= 10; return x;"));
        // short
        assertEquals((short) 15, exec("short x = 5; x -= -10; return x;"));
        assertEquals((short) -5, exec("short x = 5; x -= 10; return x;"));
        // char
        assertEquals((char) 15, exec("char x = 5; x -= -10; return x;"));
        assertEquals((char) 5, exec("char x = 10; x -= 5; return x;"));
        // int
        assertEquals(15, exec("int x = 5; x -= -10; return x;"));
        assertEquals(-5, exec("int x = 5; x -= 10; return x;"));
        // long
        assertEquals(15L, exec("long x = 5; x -= -10; return x;"));
        assertEquals(-5L, exec("long x = 5; x -= 10; return x;"));
        // float
        assertEquals(15F, exec("float x = 5f; x -= -10; return x;"));
        assertEquals(-5F, exec("float x = 5f; x -= 10; return x;"));
        // double
        assertEquals(15D, exec("double x = 5.0; x -= -10; return x;"));
        assertEquals(-5D, exec("double x = 5.0; x -= 10; return x;"));
    }

    public void testDefCompoundAssignment() {
        // byte
        assertEquals((byte) 15, exec("def x = (byte)5; x -= -10; return x;"));
        assertEquals((byte) -5, exec("def x = (byte)5; x -= 10; return x;"));
        // short
        assertEquals((short) 15, exec("def x = (short)5; x -= -10; return x;"));
        assertEquals((short) -5, exec("def x = (short)5; x -= 10; return x;"));
        // char
        assertEquals((char) 15, exec("def x = (char)5; x -= -10; return x;"));
        assertEquals((char) 5, exec("def x = (char)10; x -= 5; return x;"));
        // int
        assertEquals(15, exec("def x = 5; x -= -10; return x;"));
        assertEquals(-5, exec("def x = 5; x -= 10; return x;"));
        // long
        assertEquals(15L, exec("def x = 5L; x -= -10; return x;"));
        assertEquals(-5L, exec("def x = 5L; x -= 10; return x;"));
        // float
        assertEquals(15F, exec("def x = 5f; x -= -10; return x;"));
        assertEquals(-5F, exec("def x = 5f; x -= 10; return x;"));
        // double
        assertEquals(15D, exec("def x = 5.0; x -= -10; return x;"));
        assertEquals(-5D, exec("def x = 5.0; x -= 10; return x;"));
    }
}
