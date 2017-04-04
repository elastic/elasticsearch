/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.painless;

/** Tests for shift operator across all types */
public class ShiftTests extends ScriptTestCase {

    public void testBasics() {
        assertEquals(1 << 2, exec("return 1 << 2;"));
        assertEquals(4 >> 2, exec("return 4 >> 2;"));
        assertEquals(-1 >>> 29, exec("return -1 >>> 29;"));
        assertEquals(4, exec("int x = 1; char y = 2; return x << y;"));
        assertEquals(-1, exec("int x = -1; char y = 29; return x >> y;"));
        assertEquals(3, exec("int x = -1; char y = 30; return x >>> y;"));
    }

    public void testLongShifts() {
        assertEquals(1L << 2, exec("long x = 1L; int y = 2; return x << y;"));
        assertEquals(1 << 2L, exec("int x = 1; long y = 2L; return x << y;"));
        assertEquals(4 >> 2L, exec("int x = 4; long y = 2L; return x >> y;"));
        assertEquals(4L >> 2, exec("long x = 4L; int y = 2; return x >> y;"));
        assertEquals(-1L >>> 29, exec("long x = -1L; int y = 29; return x >>> y;"));
        assertEquals(-1 >>> 29L, exec("int x = -1; long y = 29L; return x >>> y;"));
    }
    
    public void testLongShiftsConst() {
        assertEquals(1L << 2, exec("return 1L << 2;"));
        assertEquals(1 << 2L, exec("return 1 << 2L;"));
        assertEquals(4 >> 2L, exec("return 4 >> 2L;"));
        assertEquals(4L >> 2, exec("return 4L >> 2;"));
        assertEquals(-1L >>> 29, exec("return -1L >>> 29;"));
        assertEquals(-1 >>> 29L, exec("return -1 >>> 29L;"));
    }
    
    public void testBogusShifts() {
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("long x = 1L; float y = 2; return x << y;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("int x = 1; double y = 2L; return x << y;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("float x = 1F; int y = 2; return x << y;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("double x = 1D; int y = 2L; return x << y;");
        });
    }

    public void testBogusShiftsConst() {
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("return 1L << 2F;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("return 1L << 2.0;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("return 1F << 2;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("return 1D << 2L");
        });
    }
    
    public void testLshDef() {
        assertEquals(2, exec("def x = (byte)1; def y = (byte)1; return x << y"));
        assertEquals(2, exec("def x = (short)1; def y = (byte)1; return x << y"));
        assertEquals(2, exec("def x = (char)1; def y = (byte)1; return x << y"));
        assertEquals(2, exec("def x = (int)1; def y = (byte)1; return x << y"));
        assertEquals(2L, exec("def x = (long)1; def y = (byte)1; return x << y"));

        assertEquals(2, exec("def x = (byte)1; def y = (short)1; return x << y"));
        assertEquals(2, exec("def x = (short)1; def y = (short)1; return x << y"));
        assertEquals(2, exec("def x = (char)1; def y = (short)1; return x << y"));
        assertEquals(2, exec("def x = (int)1; def y = (short)1; return x << y"));
        assertEquals(2L, exec("def x = (long)1; def y = (short)1; return x << y"));

        assertEquals(2, exec("def x = (byte)1; def y = (char)1; return x << y"));
        assertEquals(2, exec("def x = (short)1; def y = (char)1; return x << y"));
        assertEquals(2, exec("def x = (char)1; def y = (char)1; return x << y"));
        assertEquals(2, exec("def x = (int)1; def y = (char)1; return x << y"));
        assertEquals(2L, exec("def x = (long)1; def y = (char)1; return x << y"));

        assertEquals(2, exec("def x = (byte)1; def y = (int)1; return x << y"));
        assertEquals(2, exec("def x = (short)1; def y = (int)1; return x << y"));
        assertEquals(2, exec("def x = (char)1; def y = (int)1; return x << y"));
        assertEquals(2, exec("def x = (int)1; def y = (int)1; return x << y"));
        assertEquals(2L, exec("def x = (long)1; def y = (int)1; return x << y"));

        assertEquals(2, exec("def x = (byte)1; def y = (long)1; return x << y"));
        assertEquals(2, exec("def x = (short)1; def y = (long)1; return x << y"));
        assertEquals(2, exec("def x = (char)1; def y = (long)1; return x << y"));
        assertEquals(2, exec("def x = (int)1; def y = (long)1; return x << y"));
        assertEquals(2L, exec("def x = (long)1; def y = (long)1; return x << y"));

        assertEquals(2, exec("def x = (byte)1; def y = (byte)1; return x << y"));
        assertEquals(2, exec("def x = (short)1; def y = (short)1; return x << y"));
        assertEquals(2, exec("def x = (char)1; def y = (char)1; return x << y"));
        assertEquals(2, exec("def x = (int)1; def y = (int)1; return x << y"));
        assertEquals(2L, exec("def x = (long)1; def y = (long)1; return x << y"));
    }
    
    public void testLshDefTypedLHS() {
        assertEquals(2, exec("byte x = (byte)1; def y = (byte)1; return x << y"));
        assertEquals(2, exec("short x = (short)1; def y = (byte)1; return x << y"));
        assertEquals(2, exec("char x = (char)1; def y = (byte)1; return x << y"));
        assertEquals(2, exec("int x = (int)1; def y = (byte)1; return x << y"));
        assertEquals(2L, exec("long x = (long)1; def y = (byte)1; return x << y"));

        assertEquals(2, exec("byte x = (byte)1; def y = (short)1; return x << y"));
        assertEquals(2, exec("short x = (short)1; def y = (short)1; return x << y"));
        assertEquals(2, exec("char x = (char)1; def y = (short)1; return x << y"));
        assertEquals(2, exec("int x = (int)1; def y = (short)1; return x << y"));
        assertEquals(2L, exec("long x = (long)1; def y = (short)1; return x << y"));

        assertEquals(2, exec("byte x = (byte)1; def y = (char)1; return x << y"));
        assertEquals(2, exec("short x = (short)1; def y = (char)1; return x << y"));
        assertEquals(2, exec("char x = (char)1; def y = (char)1; return x << y"));
        assertEquals(2, exec("int x = (int)1; def y = (char)1; return x << y"));
        assertEquals(2L, exec("long x = (long)1; def y = (char)1; return x << y"));

        assertEquals(2, exec("byte x = (byte)1; def y = (int)1; return x << y"));
        assertEquals(2, exec("short x = (short)1; def y = (int)1; return x << y"));
        assertEquals(2, exec("char x = (char)1; def y = (int)1; return x << y"));
        assertEquals(2, exec("int x = (int)1; def y = (int)1; return x << y"));
        assertEquals(2L, exec("long x = (long)1; def y = (int)1; return x << y"));

        assertEquals(2, exec("byte x = (byte)1; def y = (long)1; return x << y"));
        assertEquals(2, exec("short x = (short)1; def y = (long)1; return x << y"));
        assertEquals(2, exec("char x = (char)1; def y = (long)1; return x << y"));
        assertEquals(2, exec("int x = (int)1; def y = (long)1; return x << y"));
        assertEquals(2L, exec("long x = (long)1; def y = (long)1; return x << y"));

        assertEquals(2, exec("byte x = (byte)1; def y = (byte)1; return x << y"));
        assertEquals(2, exec("short x = (short)1; def y = (short)1; return x << y"));
        assertEquals(2, exec("char x = (char)1; def y = (char)1; return x << y"));
        assertEquals(2, exec("int x = (int)1; def y = (int)1; return x << y"));
        assertEquals(2L, exec("long x = (long)1; def y = (long)1; return x << y"));
    }
    
    public void testLshDefTypedRHS() {
        assertEquals(2, exec("def x = (byte)1; byte y = (byte)1; return x << y"));
        assertEquals(2, exec("def x = (short)1; byte y = (byte)1; return x << y"));
        assertEquals(2, exec("def x = (char)1; byte y = (byte)1; return x << y"));
        assertEquals(2, exec("def x = (int)1; byte y = (byte)1; return x << y"));
        assertEquals(2L, exec("def x = (long)1; byte y = (byte)1; return x << y"));

        assertEquals(2, exec("def x = (byte)1; short y = (short)1; return x << y"));
        assertEquals(2, exec("def x = (short)1; short y = (short)1; return x << y"));
        assertEquals(2, exec("def x = (char)1; short y = (short)1; return x << y"));
        assertEquals(2, exec("def x = (int)1; short y = (short)1; return x << y"));
        assertEquals(2L, exec("def x = (long)1; short y = (short)1; return x << y"));

        assertEquals(2, exec("def x = (byte)1; char y = (char)1; return x << y"));
        assertEquals(2, exec("def x = (short)1; char y = (char)1; return x << y"));
        assertEquals(2, exec("def x = (char)1; char y = (char)1; return x << y"));
        assertEquals(2, exec("def x = (int)1; char y = (char)1; return x << y"));
        assertEquals(2L, exec("def x = (long)1; char y = (char)1; return x << y"));

        assertEquals(2, exec("def x = (byte)1; int y = (int)1; return x << y"));
        assertEquals(2, exec("def x = (short)1; int y = (int)1; return x << y"));
        assertEquals(2, exec("def x = (char)1; int y = (int)1; return x << y"));
        assertEquals(2, exec("def x = (int)1; int y = (int)1; return x << y"));
        assertEquals(2L, exec("def x = (long)1; int y = (int)1; return x << y"));

        assertEquals(2, exec("def x = (byte)1; long y = (long)1; return x << y"));
        assertEquals(2, exec("def x = (short)1; long y = (long)1; return x << y"));
        assertEquals(2, exec("def x = (char)1; long y = (long)1; return x << y"));
        assertEquals(2, exec("def x = (int)1; long y = (long)1; return x << y"));
        assertEquals(2L, exec("def x = (long)1; long y = (long)1; return x << y"));

        assertEquals(2, exec("def x = (byte)1; byte y = (byte)1; return x << y"));
        assertEquals(2, exec("def x = (short)1; short y = (short)1; return x << y"));
        assertEquals(2, exec("def x = (char)1; char y = (char)1; return x << y"));
        assertEquals(2, exec("def x = (int)1; int y = (int)1; return x << y"));
        assertEquals(2L, exec("def x = (long)1; long y = (long)1; return x << y"));
    }

    public void testRshDef() {
        assertEquals(2, exec("def x = (byte)4; def y = (byte)1; return x >> y"));
        assertEquals(2, exec("def x = (short)4; def y = (byte)1; return x >> y"));
        assertEquals(2, exec("def x = (char)4; def y = (byte)1; return x >> y"));
        assertEquals(2, exec("def x = (int)4; def y = (byte)1; return x >> y"));
        assertEquals(2L, exec("def x = (long)4; def y = (byte)1; return x >> y"));

        assertEquals(2, exec("def x = (byte)4; def y = (short)1; return x >> y"));
        assertEquals(2, exec("def x = (short)4; def y = (short)1; return x >> y"));
        assertEquals(2, exec("def x = (char)4; def y = (short)1; return x >> y"));
        assertEquals(2, exec("def x = (int)4; def y = (short)1; return x >> y"));
        assertEquals(2L, exec("def x = (long)4; def y = (short)1; return x >> y"));

        assertEquals(2, exec("def x = (byte)4; def y = (char)1; return x >> y"));
        assertEquals(2, exec("def x = (short)4; def y = (char)1; return x >> y"));
        assertEquals(2, exec("def x = (char)4; def y = (char)1; return x >> y"));
        assertEquals(2, exec("def x = (int)4; def y = (char)1; return x >> y"));
        assertEquals(2L, exec("def x = (long)4; def y = (char)1; return x >> y"));

        assertEquals(2, exec("def x = (byte)4; def y = (int)1; return x >> y"));
        assertEquals(2, exec("def x = (short)4; def y = (int)1; return x >> y"));
        assertEquals(2, exec("def x = (char)4; def y = (int)1; return x >> y"));
        assertEquals(2, exec("def x = (int)4; def y = (int)1; return x >> y"));
        assertEquals(2L, exec("def x = (long)4; def y = (int)1; return x >> y"));

        assertEquals(2, exec("def x = (byte)4; def y = (long)1; return x >> y"));
        assertEquals(2, exec("def x = (short)4; def y = (long)1; return x >> y"));
        assertEquals(2, exec("def x = (char)4; def y = (long)1; return x >> y"));
        assertEquals(2, exec("def x = (int)4; def y = (long)1; return x >> y"));
        assertEquals(2L, exec("def x = (long)4; def y = (long)1; return x >> y"));

        assertEquals(2, exec("def x = (byte)4; def y = (byte)1; return x >> y"));
        assertEquals(2, exec("def x = (short)4; def y = (short)1; return x >> y"));
        assertEquals(2, exec("def x = (char)4; def y = (char)1; return x >> y"));
        assertEquals(2, exec("def x = (int)4; def y = (int)1; return x >> y"));
        assertEquals(2L, exec("def x = (long)4; def y = (long)1; return x >> y"));
    }
    
    public void testRshDefTypeLHS() {
        assertEquals(2, exec("byte x = (byte)4; def y = (byte)1; return x >> y"));
        assertEquals(2, exec("short x = (short)4; def y = (byte)1; return x >> y"));
        assertEquals(2, exec("char x = (char)4; def y = (byte)1; return x >> y"));
        assertEquals(2, exec("int x = (int)4; def y = (byte)1; return x >> y"));
        assertEquals(2L, exec("long x = (long)4; def y = (byte)1; return x >> y"));

        assertEquals(2, exec("byte x = (byte)4; def y = (short)1; return x >> y"));
        assertEquals(2, exec("short x = (short)4; def y = (short)1; return x >> y"));
        assertEquals(2, exec("char x = (char)4; def y = (short)1; return x >> y"));
        assertEquals(2, exec("int x = (int)4; def y = (short)1; return x >> y"));
        assertEquals(2L, exec("long x = (long)4; def y = (short)1; return x >> y"));

        assertEquals(2, exec("byte x = (byte)4; def y = (char)1; return x >> y"));
        assertEquals(2, exec("short x = (short)4; def y = (char)1; return x >> y"));
        assertEquals(2, exec("char x = (char)4; def y = (char)1; return x >> y"));
        assertEquals(2, exec("int x = (int)4; def y = (char)1; return x >> y"));
        assertEquals(2L, exec("long x = (long)4; def y = (char)1; return x >> y"));

        assertEquals(2, exec("byte x = (byte)4; def y = (int)1; return x >> y"));
        assertEquals(2, exec("short x = (short)4; def y = (int)1; return x >> y"));
        assertEquals(2, exec("char x = (char)4; def y = (int)1; return x >> y"));
        assertEquals(2, exec("int x = (int)4; def y = (int)1; return x >> y"));
        assertEquals(2L, exec("long x = (long)4; def y = (int)1; return x >> y"));

        assertEquals(2, exec("byte x = (byte)4; def y = (long)1; return x >> y"));
        assertEquals(2, exec("short x = (short)4; def y = (long)1; return x >> y"));
        assertEquals(2, exec("char x = (char)4; def y = (long)1; return x >> y"));
        assertEquals(2, exec("int x = (int)4; def y = (long)1; return x >> y"));
        assertEquals(2L, exec("long x = (long)4; def y = (long)1; return x >> y"));

        assertEquals(2, exec("byte x = (byte)4; def y = (byte)1; return x >> y"));
        assertEquals(2, exec("short x = (short)4; def y = (short)1; return x >> y"));
        assertEquals(2, exec("char x = (char)4; def y = (char)1; return x >> y"));
        assertEquals(2, exec("int x = (int)4; def y = (int)1; return x >> y"));
        assertEquals(2L, exec("long x = (long)4; def y = (long)1; return x >> y"));
    }
    
    public void testRshDefTypedLHS() {
        assertEquals(2, exec("def x = (byte)4; byte y = (byte)1; return x >> y"));
        assertEquals(2, exec("def x = (short)4; byte y = (byte)1; return x >> y"));
        assertEquals(2, exec("def x = (char)4; byte y = (byte)1; return x >> y"));
        assertEquals(2, exec("def x = (int)4; byte y = (byte)1; return x >> y"));
        assertEquals(2L, exec("def x = (long)4; byte y = (byte)1; return x >> y"));

        assertEquals(2, exec("def x = (byte)4; short y = (short)1; return x >> y"));
        assertEquals(2, exec("def x = (short)4; short y = (short)1; return x >> y"));
        assertEquals(2, exec("def x = (char)4; short y = (short)1; return x >> y"));
        assertEquals(2, exec("def x = (int)4; short y = (short)1; return x >> y"));
        assertEquals(2L, exec("def x = (long)4; short y = (short)1; return x >> y"));

        assertEquals(2, exec("def x = (byte)4; char y = (char)1; return x >> y"));
        assertEquals(2, exec("def x = (short)4; char y = (char)1; return x >> y"));
        assertEquals(2, exec("def x = (char)4; char y = (char)1; return x >> y"));
        assertEquals(2, exec("def x = (int)4; char y = (char)1; return x >> y"));
        assertEquals(2L, exec("def x = (long)4; char y = (char)1; return x >> y"));

        assertEquals(2, exec("def x = (byte)4; int y = (int)1; return x >> y"));
        assertEquals(2, exec("def x = (short)4; int y = (int)1; return x >> y"));
        assertEquals(2, exec("def x = (char)4; int y = (int)1; return x >> y"));
        assertEquals(2, exec("def x = (int)4; int y = (int)1; return x >> y"));
        assertEquals(2L, exec("def x = (long)4; int y = (int)1; return x >> y"));

        assertEquals(2, exec("def x = (byte)4; long y = (long)1; return x >> y"));
        assertEquals(2, exec("def x = (short)4; long y = (long)1; return x >> y"));
        assertEquals(2, exec("def x = (char)4; long y = (long)1; return x >> y"));
        assertEquals(2, exec("def x = (int)4; long y = (long)1; return x >> y"));
        assertEquals(2L, exec("def x = (long)4; long y = (long)1; return x >> y"));

        assertEquals(2, exec("def x = (byte)4; byte y = (byte)1; return x >> y"));
        assertEquals(2, exec("def x = (short)4; short y = (short)1; return x >> y"));
        assertEquals(2, exec("def x = (char)4; char y = (char)1; return x >> y"));
        assertEquals(2, exec("def x = (int)4; int y = (int)1; return x >> y"));
        assertEquals(2L, exec("def x = (long)4; long y = (long)1; return x >> y"));
    }

    public void testUshDef() {
        assertEquals(2, exec("def x = (byte)4; def y = (byte)1; return x >>> y"));
        assertEquals(2, exec("def x = (short)4; def y = (byte)1; return x >>> y"));
        assertEquals(2, exec("def x = (char)4; def y = (byte)1; return x >>> y"));
        assertEquals(2, exec("def x = (int)4; def y = (byte)1; return x >>> y"));
        assertEquals(2L, exec("def x = (long)4; def y = (byte)1; return x >>> y"));

        assertEquals(2, exec("def x = (byte)4; def y = (short)1; return x >>> y"));
        assertEquals(2, exec("def x = (short)4; def y = (short)1; return x >>> y"));
        assertEquals(2, exec("def x = (char)4; def y = (short)1; return x >>> y"));
        assertEquals(2, exec("def x = (int)4; def y = (short)1; return x >>> y"));
        assertEquals(2L, exec("def x = (long)4; def y = (short)1; return x >>> y"));

        assertEquals(2, exec("def x = (byte)4; def y = (char)1; return x >>> y"));
        assertEquals(2, exec("def x = (short)4; def y = (char)1; return x >>> y"));
        assertEquals(2, exec("def x = (char)4; def y = (char)1; return x >>> y"));
        assertEquals(2, exec("def x = (int)4; def y = (char)1; return x >>> y"));
        assertEquals(2L, exec("def x = (long)4; def y = (char)1; return x >>> y"));

        assertEquals(2, exec("def x = (byte)4; def y = (int)1; return x >>> y"));
        assertEquals(2, exec("def x = (short)4; def y = (int)1; return x >>> y"));
        assertEquals(2, exec("def x = (char)4; def y = (int)1; return x >>> y"));
        assertEquals(2, exec("def x = (int)4; def y = (int)1; return x >>> y"));
        assertEquals(2L, exec("def x = (long)4; def y = (int)1; return x >>> y"));

        assertEquals(2, exec("def x = (byte)4; def y = (long)1; return x >>> y"));
        assertEquals(2, exec("def x = (short)4; def y = (long)1; return x >>> y"));
        assertEquals(2, exec("def x = (char)4; def y = (long)1; return x >>> y"));
        assertEquals(2, exec("def x = (int)4; def y = (long)1; return x >>> y"));
        assertEquals(2L, exec("def x = (long)4; def y = (long)1; return x >>> y"));

        assertEquals(2, exec("def x = (byte)4; def y = (byte)1; return x >>> y"));
        assertEquals(2, exec("def x = (short)4; def y = (short)1; return x >>> y"));
        assertEquals(2, exec("def x = (char)4; def y = (char)1; return x >>> y"));
        assertEquals(2, exec("def x = (int)4; def y = (int)1; return x >>> y"));
        assertEquals(2L, exec("def x = (long)4; def y = (long)1; return x >>> y"));
    }
    
    public void testUshDefTypedLHS() {
        assertEquals(2, exec("byte x = (byte)4; def y = (byte)1; return x >>> y"));
        assertEquals(2, exec("short x = (short)4; def y = (byte)1; return x >>> y"));
        assertEquals(2, exec("char x = (char)4; def y = (byte)1; return x >>> y"));
        assertEquals(2, exec("int x = (int)4; def y = (byte)1; return x >>> y"));
        assertEquals(2L, exec("long x = (long)4; def y = (byte)1; return x >>> y"));

        assertEquals(2, exec("byte x = (byte)4; def y = (short)1; return x >>> y"));
        assertEquals(2, exec("short x = (short)4; def y = (short)1; return x >>> y"));
        assertEquals(2, exec("char x = (char)4; def y = (short)1; return x >>> y"));
        assertEquals(2, exec("int x = (int)4; def y = (short)1; return x >>> y"));
        assertEquals(2L, exec("long x = (long)4; def y = (short)1; return x >>> y"));

        assertEquals(2, exec("byte x = (byte)4; def y = (char)1; return x >>> y"));
        assertEquals(2, exec("short x = (short)4; def y = (char)1; return x >>> y"));
        assertEquals(2, exec("char x = (char)4; def y = (char)1; return x >>> y"));
        assertEquals(2, exec("int x = (int)4; def y = (char)1; return x >>> y"));
        assertEquals(2L, exec("long x = (long)4; def y = (char)1; return x >>> y"));

        assertEquals(2, exec("byte x = (byte)4; def y = (int)1; return x >>> y"));
        assertEquals(2, exec("short x = (short)4; def y = (int)1; return x >>> y"));
        assertEquals(2, exec("char x = (char)4; def y = (int)1; return x >>> y"));
        assertEquals(2, exec("int x = (int)4; def y = (int)1; return x >>> y"));
        assertEquals(2L, exec("long x = (long)4; def y = (int)1; return x >>> y"));

        assertEquals(2, exec("byte x = (byte)4; def y = (long)1; return x >>> y"));
        assertEquals(2, exec("short x = (short)4; def y = (long)1; return x >>> y"));
        assertEquals(2, exec("char x = (char)4; def y = (long)1; return x >>> y"));
        assertEquals(2, exec("int x = (int)4; def y = (long)1; return x >>> y"));
        assertEquals(2L, exec("long x = (long)4; def y = (long)1; return x >>> y"));

        assertEquals(2, exec("byte x = (byte)4; def y = (byte)1; return x >>> y"));
        assertEquals(2, exec("short x = (short)4; def y = (short)1; return x >>> y"));
        assertEquals(2, exec("char x = (char)4; def y = (char)1; return x >>> y"));
        assertEquals(2, exec("int x = (int)4; def y = (int)1; return x >>> y"));
        assertEquals(2L, exec("long x = (long)4; def y = (long)1; return x >>> y"));
    }
    
    public void testUshDefTypedRHS() {
        assertEquals(2, exec("def x = (byte)4; byte y = (byte)1; return x >>> y"));
        assertEquals(2, exec("def x = (short)4; byte y = (byte)1; return x >>> y"));
        assertEquals(2, exec("def x = (char)4; byte y = (byte)1; return x >>> y"));
        assertEquals(2, exec("def x = (int)4; byte y = (byte)1; return x >>> y"));
        assertEquals(2L, exec("def x = (long)4; byte y = (byte)1; return x >>> y"));

        assertEquals(2, exec("def x = (byte)4; short y = (short)1; return x >>> y"));
        assertEquals(2, exec("def x = (short)4; short y = (short)1; return x >>> y"));
        assertEquals(2, exec("def x = (char)4; short y = (short)1; return x >>> y"));
        assertEquals(2, exec("def x = (int)4; short y = (short)1; return x >>> y"));
        assertEquals(2L, exec("def x = (long)4; short y = (short)1; return x >>> y"));

        assertEquals(2, exec("def x = (byte)4; char y = (char)1; return x >>> y"));
        assertEquals(2, exec("def x = (short)4; char y = (char)1; return x >>> y"));
        assertEquals(2, exec("def x = (char)4; char y = (char)1; return x >>> y"));
        assertEquals(2, exec("def x = (int)4; char y = (char)1; return x >>> y"));
        assertEquals(2L, exec("def x = (long)4; char y = (char)1; return x >>> y"));

        assertEquals(2, exec("def x = (byte)4; int y = (int)1; return x >>> y"));
        assertEquals(2, exec("def x = (short)4; int y = (int)1; return x >>> y"));
        assertEquals(2, exec("def x = (char)4; int y = (int)1; return x >>> y"));
        assertEquals(2, exec("def x = (int)4; int y = (int)1; return x >>> y"));
        assertEquals(2L, exec("def x = (long)4; int y = (int)1; return x >>> y"));

        assertEquals(2, exec("def x = (byte)4; long y = (long)1; return x >>> y"));
        assertEquals(2, exec("def x = (short)4; long y = (long)1; return x >>> y"));
        assertEquals(2, exec("def x = (char)4; long y = (long)1; return x >>> y"));
        assertEquals(2, exec("def x = (int)4; long y = (long)1; return x >>> y"));
        assertEquals(2L, exec("def x = (long)4; long y = (long)1; return x >>> y"));

        assertEquals(2, exec("def x = (byte)4; byte y = (byte)1; return x >>> y"));
        assertEquals(2, exec("def x = (short)4; short y = (short)1; return x >>> y"));
        assertEquals(2, exec("def x = (char)4; char y = (char)1; return x >>> y"));
        assertEquals(2, exec("def x = (int)4; int y = (int)1; return x >>> y"));
        assertEquals(2L, exec("def x = (long)4; long y = (long)1; return x >>> y"));
    }
    
    public void testBogusDefShifts() {
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("def x = 1L; def y = 2F; return x << y;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("def x = 1; def y = 2D; return x << y;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("def x = 1F; def y = 2; return x << y;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("def x = 1D; def y = 2L; return x << y;");
        });
        
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("def x = 1L; def y = 2F; return x >> y;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("def x = 1; def y = 2D; return x >> y;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("def x = 1F; def y = 2; return x >> y;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("def x = 1D; def y = 2L; return x >> y;");
        });
        
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("def x = 1L; def y = 2F; return x >>> y;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("def x = 1; def y = 2D; return x >>> y;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("def x = 1F; def y = 2; return x >>> y;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("def x = 1D; def y = 2L; return x >>> y;");
        });
    }
    
    public void testBogusDefShiftsTypedLHS() {
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("long x = 1L; def y = 2F; return x << y;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("int x = 1; def y = 2D; return x << y;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("float x = 1F; def y = 2; return x << y;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("double x = 1D; def y = 2L; return x << y;");
        });
        
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("long x = 1L; def y = 2F; return x >> y;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("int x = 1; def y = 2D; return x >> y;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("float x = 1F; def y = 2; return x >> y;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("double x = 1D; def y = 2L; return x >> y;");
        });
        
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("long x = 1L; def y = 2F; return x >>> y;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("int x = 1; def y = 2D; return x >>> y;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("float x = 1F; def y = 2; return x >>> y;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("double x = 1D; def y = 2L; return x >>> y;");
        });
    }
    
    public void testBogusDefShiftsTypedRHS() {
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("def x = 1L; float y = 2F; return x << y;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("def x = 1; double y = 2D; return x << y;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("def x = 1F; int y = 2; return x << y;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("def x = 1D; long y = 2L; return x << y;");
        });
        
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("def x = 1L; float y = 2F; return x >> y;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("def x = 1; double y = 2D; return x >> y;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("def x = 1F; int y = 2; return x >> y;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("def x = 1D; long y = 2L; return x >> y;");
        });
        
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("def x = 1L; float y = 2F; return x >>> y;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("def x = 1; double y = 2D; return x >>> y;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("def x = 1F; int y = 2; return x >>> y;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("def x = 1D; long y = 2L; return x >>> y;");
        });
    }

    public void testLshCompoundAssignment() {
        // byte
        assertEquals((byte) 60, exec("byte x = 15; x <<= 2; return x;"));
        assertEquals((byte) -60, exec("byte x = (byte) -15; x <<= 2; return x;"));
        // short
        assertEquals((short) 60, exec("short x = 15; x <<= 2; return x;"));
        assertEquals((short) -60, exec("short x = (short) -15; x <<= 2; return x;"));
        // char
        assertEquals((char) 60, exec("char x = (char) 15; x <<= 2; return x;"));
        // int
        assertEquals(60, exec("int x = 15; x <<= 2; return x;"));
        assertEquals(-60, exec("int x = -15; x <<= 2; return x;"));
        // long
        assertEquals(60L, exec("long x = 15L; x <<= 2; return x;"));
        assertEquals(-60L, exec("long x = -15L; x <<= 2; return x;"));
        // long shift distance
        assertEquals(60, exec("int x = 15; x <<= 2L; return x;"));
        assertEquals(-60, exec("int x = -15; x <<= 2L; return x;"));
    }

    public void testRshCompoundAssignment() {
        // byte
        assertEquals((byte) 15, exec("byte x = 60; x >>= 2; return x;"));
        assertEquals((byte) -15, exec("byte x = (byte) -60; x >>= 2; return x;"));
        // short
        assertEquals((short) 15, exec("short x = 60; x >>= 2; return x;"));
        assertEquals((short) -15, exec("short x = (short) -60; x >>= 2; return x;"));
        // char
        assertEquals((char) 15, exec("char x = (char) 60; x >>= 2; return x;"));
        // int
        assertEquals(15, exec("int x = 60; x >>= 2; return x;"));
        assertEquals(-15, exec("int x = -60; x >>= 2; return x;"));
        // long
        assertEquals(15L, exec("long x = 60L; x >>= 2; return x;"));
        assertEquals(-15L, exec("long x = -60L; x >>= 2; return x;"));
        // long shift distance
        assertEquals(15, exec("int x = 60; x >>= 2L; return x;"));
        assertEquals(-15, exec("int x = -60; x >>= 2L; return x;"));
    }

    public void testUshCompoundAssignment() {
        // byte
        assertEquals((byte) 15, exec("byte x = 60; x >>>= 2; return x;"));
        assertEquals((byte) -15, exec("byte x = (byte) -60; x >>>= 2; return x;"));
        // short
        assertEquals((short) 15, exec("short x = 60; x >>>= 2; return x;"));
        assertEquals((short) -15, exec("short x = (short) -60; x >>>= 2; return x;"));
        // char
        assertEquals((char) 15, exec("char x = (char) 60; x >>>= 2; return x;"));
        // int
        assertEquals(15, exec("int x = 60; x >>>= 2; return x;"));
        assertEquals(-60 >>> 2, exec("int x = -60; x >>>= 2; return x;"));
        // long
        assertEquals(15L, exec("long x = 60L; x >>>= 2; return x;"));
        assertEquals(-60L >>> 2, exec("long x = -60L; x >>>= 2; return x;"));
        // long shift distance
        assertEquals(15, exec("int x = 60; x >>>= 2L; return x;"));
        assertEquals(-60 >>> 2, exec("int x = -60; x >>>= 2L; return x;"));
    }
    
    public void testBogusCompoundAssignment() {
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("long x = 1L; float y = 2; x <<= y;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("int x = 1; double y = 2L; x <<= y;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("float x = 1F; int y = 2; x <<= y;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("double x = 1D; int y = 2L; x <<= y;");
        });
    }

    public void testBogusCompoundAssignmentConst() {
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("int x = 1L; x <<= 2F;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("int x = 1L; x <<= 2.0;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("float x = 1F; x <<= 2;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("double x = 1D; x <<= 2L;");
        });
    }
    
    public void testBogusCompoundAssignmentDef() {
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("def x = 1L; float y = 2; x <<= y;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("def x = 1; double y = 2L; x <<= y;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("float x = 1F; def y = 2; x <<= y;");
        });
        expectScriptThrows(ClassCastException.class, ()-> {
            exec("double x = 1D; def y = 2L; x <<= y;");
        });
    }
}
