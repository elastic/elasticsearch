/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

/** Tests for increment/decrement operators across all data types */
public class IncrementTests extends ScriptTestCase {

    /** incrementing byte values */
    public void testIncrementByte() {
        assertEquals((byte)0, exec("byte x = (byte)0; return x++;"));
        assertEquals((byte)0, exec("byte x = (byte)0; return x--;"));
        assertEquals((byte)1, exec("byte x = (byte)0; return ++x;"));
        assertEquals((byte)-1, exec("byte x = (byte)0; return --x;"));
    }

    /** incrementing char values */
    public void testIncrementChar() {
        assertEquals((char)0, exec("char x = (char)0; return x++;"));
        assertEquals((char)1, exec("char x = (char)1; return x--;"));
        assertEquals((char)1, exec("char x = (char)0; return ++x;"));
    }

    /** incrementing short values */
    public void testIncrementShort() {
        assertEquals((short)0, exec("short x = (short)0; return x++;"));
        assertEquals((short)0, exec("short x = (short)0; return x--;"));
        assertEquals((short)1, exec("short x = (short)0; return ++x;"));
        assertEquals((short)-1, exec("short x = (short)0; return --x;"));
    }

    /** incrementing integer values */
    public void testIncrementInt() {
        assertEquals(0, exec("int x = 0; return x++;"));
        assertEquals(0, exec("int x = 0; return x--;"));
        assertEquals(1, exec("int x = 0; return ++x;"));
        assertEquals(-1, exec("int x = 0; return --x;"));
    }

    /** incrementing long values */
    public void testIncrementLong() {
        assertEquals(0L, exec("long x = 0; return x++;"));
        assertEquals(0L, exec("long x = 0; return x--;"));
        assertEquals(1L, exec("long x = 0; return ++x;"));
        assertEquals(-1L, exec("long x = 0; return --x;"));
    }

    /** incrementing float values */
    public void testIncrementFloat() {
        assertEquals(0F, exec("float x = 0F; return x++;"));
        assertEquals(0F, exec("float x = 0F; return x--;"));
        assertEquals(1F, exec("float x = 0F; return ++x;"));
        assertEquals(-1F, exec("float x = 0F; return --x;"));
    }

    /** incrementing double values */
    public void testIncrementDouble() {
        assertEquals(0D, exec("double x = 0.0; return x++;"));
        assertEquals(0D, exec("double x = 0.0; return x--;"));
        assertEquals(1D, exec("double x = 0.0; return ++x;"));
        assertEquals(-1D, exec("double x = 0.0; return --x;"));
    }

    /** incrementing def values */
    public void testIncrementDef() {
        assertEquals((byte)0, exec("def x = (byte)0; return x++;"));
        assertEquals((byte)0, exec("def x = (byte)0; return x--;"));
        assertEquals((byte)1, exec("def x = (byte)0; return ++x;"));
        assertEquals((byte)-1, exec("def x = (byte)0; return --x;"));
        assertEquals((char)0, exec("def x = (char)0; return x++;"));
        assertEquals((char)1, exec("def x = (char)1; return x--;"));
        assertEquals((char)1, exec("def x = (char)0; return ++x;"));
        assertEquals((short)0, exec("def x = (short)0; return x++;"));
        assertEquals((short)0, exec("def x = (short)0; return x--;"));
        assertEquals((short)1, exec("def x = (short)0; return ++x;"));
        assertEquals((short)-1, exec("def x = (short)0; return --x;"));
        assertEquals(0, exec("def x = 0; return x++;"));
        assertEquals(0, exec("def x = 0; return x--;"));
        assertEquals(1, exec("def x = 0; return ++x;"));
        assertEquals(-1, exec("def x = 0; return --x;"));
        assertEquals(0L, exec("def x = 0L; return x++;"));
        assertEquals(0L, exec("def x = 0L; return x--;"));
        assertEquals(1L, exec("def x = 0L; return ++x;"));
        assertEquals(-1L, exec("def x = 0L; return --x;"));
        assertEquals(0F, exec("def x = 0F; return x++;"));
        assertEquals(0F, exec("def x = 0F; return x--;"));
        assertEquals(1F, exec("def x = 0F; return ++x;"));
        assertEquals(-1F, exec("def x = 0F; return --x;"));
        assertEquals(0D, exec("def x = 0.0; return x++;"));
        assertEquals(0D, exec("def x = 0.0; return x--;"));
        assertEquals(1D, exec("def x = 0.0; return ++x;"));
        assertEquals(-1D, exec("def x = 0.0; return --x;"));
    }
}
