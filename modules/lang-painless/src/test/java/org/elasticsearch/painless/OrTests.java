/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

/** Tests for or operator across all types */
public class OrTests extends ScriptTestCase {

    public void testBasics() throws Exception {
        assertEquals(5 | 3, exec("return 5 | 3;"));
        assertEquals(5L | 3, exec("return 5L | 3;"));
        assertEquals(5 | 3L, exec("return 5 | 3L;"));
        assertEquals(7, exec("short x = 5; byte y = 3; return x | y;"));
    }

    public void testInt() throws Exception {
        assertEquals(5 | 12, exec("int x = 5; int y = 12; return x | y;"));
        assertEquals(5 | -12, exec("int x = 5; int y = -12; return x | y;"));
        assertEquals(7 | 15 | 3, exec("int x = 7; int y = 15; int z = 3; return x | y | z;"));
    }

    public void testIntConst() throws Exception {
        assertEquals(5 | 12, exec("return 5 | 12;"));
        assertEquals(5 | -12, exec("return 5 | -12;"));
        assertEquals(7 | 15 | 3, exec("return 7 | 15 | 3;"));
    }

    public void testLong() throws Exception {
        assertEquals(5L | 12L, exec("long x = 5; long y = 12; return x | y;"));
        assertEquals(5L | -12L, exec("long x = 5; long y = -12; return x | y;"));
        assertEquals(7L | 15L | 3L, exec("long x = 7; long y = 15; long z = 3; return x | y | z;"));
    }

    public void testLongConst() throws Exception {
        assertEquals(5L | 12L, exec("return 5L | 12L;"));
        assertEquals(5L | -12L, exec("return 5L | -12L;"));
        assertEquals(7L | 15L | 3L, exec("return 7L | 15L | 3L;"));
    }

    public void testIllegal() throws Exception {
        expectScriptThrows(ClassCastException.class, () -> { exec("float x = (float)4; int y = 1; return x | y"); });
        expectScriptThrows(ClassCastException.class, () -> { exec("double x = (double)4; int y = 1; return x | y"); });
    }

    public void testDef() {
        expectScriptThrows(ClassCastException.class, () -> { exec("def x = (float)4; def y = (byte)1; return x | y"); });
        expectScriptThrows(ClassCastException.class, () -> { exec("def x = (double)4; def y = (byte)1; return x | y"); });
        assertEquals(5, exec("def x = (byte)4; def y = (byte)1; return x | y"));
        assertEquals(5, exec("def x = (short)4; def y = (byte)1; return x | y"));
        assertEquals(5, exec("def x = (char)4; def y = (byte)1; return x | y"));
        assertEquals(5, exec("def x = (int)4; def y = (byte)1; return x | y"));
        assertEquals(5L, exec("def x = (long)4; def y = (byte)1; return x | y"));

        assertEquals(5, exec("def x = (byte)4; def y = (short)1; return x | y"));
        assertEquals(5, exec("def x = (short)4; def y = (short)1; return x | y"));
        assertEquals(5, exec("def x = (char)4; def y = (short)1; return x | y"));
        assertEquals(5, exec("def x = (int)4; def y = (short)1; return x | y"));
        assertEquals(5L, exec("def x = (long)4; def y = (short)1; return x | y"));

        assertEquals(5, exec("def x = (byte)4; def y = (char)1; return x | y"));
        assertEquals(5, exec("def x = (short)4; def y = (char)1; return x | y"));
        assertEquals(5, exec("def x = (char)4; def y = (char)1; return x | y"));
        assertEquals(5, exec("def x = (int)4; def y = (char)1; return x | y"));
        assertEquals(5L, exec("def x = (long)4; def y = (char)1; return x | y"));

        assertEquals(5, exec("def x = (byte)4; def y = (int)1; return x | y"));
        assertEquals(5, exec("def x = (short)4; def y = (int)1; return x | y"));
        assertEquals(5, exec("def x = (char)4; def y = (int)1; return x | y"));
        assertEquals(5, exec("def x = (int)4; def y = (int)1; return x | y"));
        assertEquals(5L, exec("def x = (long)4; def y = (int)1; return x | y"));

        assertEquals(5L, exec("def x = (byte)4; def y = (long)1; return x | y"));
        assertEquals(5L, exec("def x = (short)4; def y = (long)1; return x | y"));
        assertEquals(5L, exec("def x = (char)4; def y = (long)1; return x | y"));
        assertEquals(5L, exec("def x = (int)4; def y = (long)1; return x | y"));
        assertEquals(5L, exec("def x = (long)4; def y = (long)1; return x | y"));

        assertEquals(5, exec("def x = (byte)4; def y = (byte)1; return x | y"));
        assertEquals(5, exec("def x = (short)4; def y = (short)1; return x | y"));
        assertEquals(5, exec("def x = (char)4; def y = (char)1; return x | y"));
        assertEquals(5, exec("def x = (int)4; def y = (int)1; return x | y"));
        assertEquals(5L, exec("def x = (long)4; def y = (long)1; return x | y"));

        assertEquals(true, exec("def x = true;  def y = true; return x | y"));
        assertEquals(true, exec("def x = true;  def y = false; return x | y"));
        assertEquals(true, exec("def x = false; def y = true; return x | y"));
        assertEquals(false, exec("def x = false; def y = false; return x | y"));
    }

    public void testDefTypedLHS() {
        expectScriptThrows(ClassCastException.class, () -> { exec("float x = (float)4; def y = (byte)1; return x | y"); });
        expectScriptThrows(ClassCastException.class, () -> { exec("double x = (double)4; def y = (byte)1; return x | y"); });
        assertEquals(5, exec("byte x = (byte)4; def y = (byte)1; return x | y"));
        assertEquals(5, exec("short x = (short)4; def y = (byte)1; return x | y"));
        assertEquals(5, exec("char x = (char)4; def y = (byte)1; return x | y"));
        assertEquals(5, exec("int x = (int)4; def y = (byte)1; return x | y"));
        assertEquals(5L, exec("long x = (long)4; def y = (byte)1; return x | y"));

        assertEquals(5, exec("byte x = (byte)4; def y = (short)1; return x | y"));
        assertEquals(5, exec("short x = (short)4; def y = (short)1; return x | y"));
        assertEquals(5, exec("char x = (char)4; def y = (short)1; return x | y"));
        assertEquals(5, exec("int x = (int)4; def y = (short)1; return x | y"));
        assertEquals(5L, exec("long x = (long)4; def y = (short)1; return x | y"));

        assertEquals(5, exec("byte x = (byte)4; def y = (char)1; return x | y"));
        assertEquals(5, exec("short x = (short)4; def y = (char)1; return x | y"));
        assertEquals(5, exec("char x = (char)4; def y = (char)1; return x | y"));
        assertEquals(5, exec("int x = (int)4; def y = (char)1; return x | y"));
        assertEquals(5L, exec("long x = (long)4; def y = (char)1; return x | y"));

        assertEquals(5, exec("byte x = (byte)4; def y = (int)1; return x | y"));
        assertEquals(5, exec("short x = (short)4; def y = (int)1; return x | y"));
        assertEquals(5, exec("char x = (char)4; def y = (int)1; return x | y"));
        assertEquals(5, exec("int x = (int)4; def y = (int)1; return x | y"));
        assertEquals(5L, exec("long x = (long)4; def y = (int)1; return x | y"));

        assertEquals(5L, exec("byte x = (byte)4; def y = (long)1; return x | y"));
        assertEquals(5L, exec("short x = (short)4; def y = (long)1; return x | y"));
        assertEquals(5L, exec("char x = (char)4; def y = (long)1; return x | y"));
        assertEquals(5L, exec("int x = (int)4; def y = (long)1; return x | y"));
        assertEquals(5L, exec("long x = (long)4; def y = (long)1; return x | y"));

        assertEquals(5, exec("byte x = (byte)4; def y = (byte)1; return x | y"));
        assertEquals(5, exec("short x = (short)4; def y = (short)1; return x | y"));
        assertEquals(5, exec("char x = (char)4; def y = (char)1; return x | y"));
        assertEquals(5, exec("int x = (int)4; def y = (int)1; return x | y"));
        assertEquals(5L, exec("long x = (long)4; def y = (long)1; return x | y"));

        assertEquals(true, exec("boolean x = true;  def y = true; return x | y"));
        assertEquals(true, exec("boolean x = true;  def y = false; return x | y"));
        assertEquals(true, exec("boolean x = false; def y = true; return x | y"));
        assertEquals(false, exec("boolean x = false; def y = false; return x | y"));
    }

    public void testDefTypedRHS() {
        expectScriptThrows(ClassCastException.class, () -> { exec("def x = (float)4; byte y = (byte)1; return x | y"); });
        expectScriptThrows(ClassCastException.class, () -> { exec("def x = (double)4; byte y = (byte)1; return x | y"); });
        assertEquals(5, exec("def x = (byte)4; byte y = (byte)1; return x | y"));
        assertEquals(5, exec("def x = (short)4; byte y = (byte)1; return x | y"));
        assertEquals(5, exec("def x = (char)4; byte y = (byte)1; return x | y"));
        assertEquals(5, exec("def x = (int)4; byte y = (byte)1; return x | y"));
        assertEquals(5L, exec("def x = (long)4; byte y = (byte)1; return x | y"));

        assertEquals(5, exec("def x = (byte)4; short y = (short)1; return x | y"));
        assertEquals(5, exec("def x = (short)4; short y = (short)1; return x | y"));
        assertEquals(5, exec("def x = (char)4; short y = (short)1; return x | y"));
        assertEquals(5, exec("def x = (int)4; short y = (short)1; return x | y"));
        assertEquals(5L, exec("def x = (long)4; short y = (short)1; return x | y"));

        assertEquals(5, exec("def x = (byte)4; char y = (char)1; return x | y"));
        assertEquals(5, exec("def x = (short)4; char y = (char)1; return x | y"));
        assertEquals(5, exec("def x = (char)4; char y = (char)1; return x | y"));
        assertEquals(5, exec("def x = (int)4; char y = (char)1; return x | y"));
        assertEquals(5L, exec("def x = (long)4; char y = (char)1; return x | y"));

        assertEquals(5, exec("def x = (byte)4; int y = (int)1; return x | y"));
        assertEquals(5, exec("def x = (short)4; int y = (int)1; return x | y"));
        assertEquals(5, exec("def x = (char)4; int y = (int)1; return x | y"));
        assertEquals(5, exec("def x = (int)4; int y = (int)1; return x | y"));
        assertEquals(5L, exec("def x = (long)4; int y = (int)1; return x | y"));

        assertEquals(5L, exec("def x = (byte)4; long y = (long)1; return x | y"));
        assertEquals(5L, exec("def x = (short)4; long y = (long)1; return x | y"));
        assertEquals(5L, exec("def x = (char)4; long y = (long)1; return x | y"));
        assertEquals(5L, exec("def x = (int)4; long y = (long)1; return x | y"));
        assertEquals(5L, exec("def x = (long)4; long y = (long)1; return x | y"));

        assertEquals(5, exec("def x = (byte)4; byte y = (byte)1; return x | y"));
        assertEquals(5, exec("def x = (short)4; short y = (short)1; return x | y"));
        assertEquals(5, exec("def x = (char)4; char y = (char)1; return x | y"));
        assertEquals(5, exec("def x = (int)4; int y = (int)1; return x | y"));
        assertEquals(5L, exec("def x = (long)4; long y = (long)1; return x | y"));

        assertEquals(true, exec("def x = true;  boolean y = true; return x | y"));
        assertEquals(true, exec("def x = true;  boolean y = false; return x | y"));
        assertEquals(true, exec("def x = false; boolean y = true; return x | y"));
        assertEquals(false, exec("def x = false; boolean y = false; return x | y"));
    }

    public void testCompoundAssignment() {
        // boolean
        assertEquals(true, exec("boolean x = true; x |= true; return x;"));
        assertEquals(true, exec("boolean x = true; x |= false; return x;"));
        assertEquals(true, exec("boolean x = false; x |= true; return x;"));
        assertEquals(false, exec("boolean x = false; x |= false; return x;"));
        assertEquals(true, exec("boolean[] x = new boolean[1]; x[0] = true; x[0] |= true; return x[0];"));
        assertEquals(true, exec("boolean[] x = new boolean[1]; x[0] = true; x[0] |= false; return x[0];"));
        assertEquals(true, exec("boolean[] x = new boolean[1]; x[0] = false; x[0] |= true; return x[0];"));
        assertEquals(false, exec("boolean[] x = new boolean[1]; x[0] = false; x[0] |= false; return x[0];"));

        // byte
        assertEquals((byte) (13 | 14), exec("byte x = 13; x |= 14; return x;"));
        // short
        assertEquals((short) (13 | 14), exec("short x = 13; x |= 14; return x;"));
        // char
        assertEquals((char) (13 | 14), exec("char x = 13; x |= 14; return x;"));
        // int
        assertEquals(13 | 14, exec("int x = 13; x |= 14; return x;"));
        // long
        assertEquals((long) (13 | 14), exec("long x = 13L; x |= 14; return x;"));
    }

    public void testBogusCompoundAssignment() {
        expectScriptThrows(ClassCastException.class, () -> { exec("float x = 4; int y = 1; x |= y"); });
        expectScriptThrows(ClassCastException.class, () -> { exec("double x = 4; int y = 1; x |= y"); });
        expectScriptThrows(ClassCastException.class, () -> { exec("int x = 4; float y = 1; x |= y"); });
        expectScriptThrows(ClassCastException.class, () -> { exec("int x = 4; double y = 1; x |= y"); });
    }

    public void testDefCompoundAssignment() {
        // boolean
        assertEquals(true, exec("def x = true; x |= true; return x;"));
        assertEquals(true, exec("def x = true; x |= false; return x;"));
        assertEquals(true, exec("def x = false; x |= true; return x;"));
        assertEquals(false, exec("def x = false; x |= false; return x;"));
        assertEquals(true, exec("def[] x = new def[1]; x[0] = true; x[0] |= true; return x[0];"));
        assertEquals(true, exec("def[] x = new def[1]; x[0] = true; x[0] |= false; return x[0];"));
        assertEquals(true, exec("def[] x = new def[1]; x[0] = false; x[0] |= true; return x[0];"));
        assertEquals(false, exec("def[] x = new def[1]; x[0] = false; x[0] |= false; return x[0];"));

        // byte
        assertEquals((byte) (13 | 14), exec("def x = (byte)13; x |= 14; return x;"));
        // short
        assertEquals((short) (13 | 14), exec("def x = (short)13; x |= 14; return x;"));
        // char
        assertEquals((char) (13 | 14), exec("def x = (char)13; x |= 14; return x;"));
        // int
        assertEquals(13 | 14, exec("def x = 13; x |= 14; return x;"));
        // long
        assertEquals((long) (13 | 14), exec("def x = 13L; x |= 14; return x;"));
    }

    public void testDefBogusCompoundAssignment() {
        expectScriptThrows(ClassCastException.class, () -> { exec("def x = 4F; int y = 1; x |= y"); });
        expectScriptThrows(ClassCastException.class, () -> { exec("def x = 4D; int y = 1; x |= y"); });
        expectScriptThrows(ClassCastException.class, () -> { exec("def x = 4; float y = 1; x |= y"); });
        expectScriptThrows(ClassCastException.class, () -> { exec("def x = 4; double y = 1; x |= y"); });
    }
}
