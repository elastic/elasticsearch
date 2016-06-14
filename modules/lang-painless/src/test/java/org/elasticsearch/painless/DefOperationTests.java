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

public class DefOperationTests extends ScriptTestCase {
    public void testIllegalCast() {
        Exception exception = expectScriptThrows(ClassCastException.class, () -> { 
            exec("def x = 1.0; int y = x; return y;");
        });
        assertTrue(exception.getMessage().contains("cannot be cast"));

        exception = expectScriptThrows(ClassCastException.class, () -> { 
            exec("def x = (short)1; byte y = x; return y;");
        });
        assertTrue(exception.getMessage().contains("cannot be cast"));
    }

    public void testNot() {
        assertEquals(~1, exec("def x = (byte)1; return ~x"));
        assertEquals(~1, exec("def x = (short)1; return ~x"));
        assertEquals(~1, exec("def x = (char)1; return ~x"));
        assertEquals(~1, exec("def x = 1; return ~x"));
        assertEquals(~1L, exec("def x = 1L; return ~x"));
    }

    public void testNeg() {
        assertEquals(-1, exec("def x = (byte)1; return -x"));
        assertEquals(-1, exec("def x = (short)1; return -x"));
        assertEquals(-1, exec("def x = (char)1; return -x"));
        assertEquals(-1, exec("def x = 1; return -x"));
        assertEquals(-1L, exec("def x = 1L; return -x"));
        assertEquals(-1.0F, exec("def x = 1F; return -x"));
        assertEquals(-1.0, exec("def x = 1.0; return -x"));
    }
    
    public void testPlus() {
        assertEquals(-1, exec("def x = (byte)-1; return +x"));
        assertEquals(-1, exec("def x = (short)-1; return +x"));
        assertEquals(65535, exec("def x = (char)-1; return +x"));
        assertEquals(-1, exec("def x = -1; return +x"));
        assertEquals(-1L, exec("def x = -1L; return +x"));
        assertEquals(-1.0F, exec("def x = -1F; return +x"));
        assertEquals(-1.0D, exec("def x = -1.0; return +x"));
    }

    public void testMul() {
        assertEquals(4, exec("def x = (byte)2; def y = (byte)2; return x * y"));
        assertEquals(4, exec("def x = (short)2; def y = (byte)2; return x * y"));
        assertEquals(4, exec("def x = (char)2; def y = (byte)2; return x * y"));
        assertEquals(4, exec("def x = (int)2; def y = (byte)2; return x * y"));
        assertEquals(4L, exec("def x = (long)2; def y = (byte)2; return x * y"));
        assertEquals(4F, exec("def x = (float)2; def y = (byte)2; return x * y"));
        assertEquals(4D, exec("def x = (double)2; def y = (byte)2; return x * y"));

        assertEquals(4, exec("def x = (byte)2; def y = (short)2; return x * y"));
        assertEquals(4, exec("def x = (short)2; def y = (short)2; return x * y"));
        assertEquals(4, exec("def x = (char)2; def y = (short)2; return x * y"));
        assertEquals(4, exec("def x = (int)2; def y = (short)2; return x * y"));
        assertEquals(4L, exec("def x = (long)2; def y = (short)2; return x * y"));
        assertEquals(4F, exec("def x = (float)2; def y = (short)2; return x * y"));
        assertEquals(4D, exec("def x = (double)2; def y = (short)2; return x * y"));

        assertEquals(4, exec("def x = (byte)2; def y = (char)2; return x * y"));
        assertEquals(4, exec("def x = (short)2; def y = (char)2; return x * y"));
        assertEquals(4, exec("def x = (char)2; def y = (char)2; return x * y"));
        assertEquals(4, exec("def x = (int)2; def y = (char)2; return x * y"));
        assertEquals(4L, exec("def x = (long)2; def y = (char)2; return x * y"));
        assertEquals(4F, exec("def x = (float)2; def y = (char)2; return x * y"));
        assertEquals(4D, exec("def x = (double)2; def y = (char)2; return x * y"));

        assertEquals(4, exec("def x = (byte)2; def y = (int)2; return x * y"));
        assertEquals(4, exec("def x = (short)2; def y = (int)2; return x * y"));
        assertEquals(4, exec("def x = (char)2; def y = (int)2; return x * y"));
        assertEquals(4, exec("def x = (int)2; def y = (int)2; return x * y"));
        assertEquals(4L, exec("def x = (long)2; def y = (int)2; return x * y"));
        assertEquals(4F, exec("def x = (float)2; def y = (int)2; return x * y"));
        assertEquals(4D, exec("def x = (double)2; def y = (int)2; return x * y"));

        assertEquals(4L, exec("def x = (byte)2; def y = (long)2; return x * y"));
        assertEquals(4L, exec("def x = (short)2; def y = (long)2; return x * y"));
        assertEquals(4L, exec("def x = (char)2; def y = (long)2; return x * y"));
        assertEquals(4L, exec("def x = (int)2; def y = (long)2; return x * y"));
        assertEquals(4L, exec("def x = (long)2; def y = (long)2; return x * y"));
        assertEquals(4F, exec("def x = (float)2; def y = (long)2; return x * y"));
        assertEquals(4D, exec("def x = (double)2; def y = (long)2; return x * y"));

        assertEquals(4F, exec("def x = (byte)2; def y = (float)2; return x * y"));
        assertEquals(4F, exec("def x = (short)2; def y = (float)2; return x * y"));
        assertEquals(4F, exec("def x = (char)2; def y = (float)2; return x * y"));
        assertEquals(4F, exec("def x = (int)2; def y = (float)2; return x * y"));
        assertEquals(4F, exec("def x = (long)2; def y = (float)2; return x * y"));
        assertEquals(4F, exec("def x = (float)2; def y = (float)2; return x * y"));
        assertEquals(4D, exec("def x = (double)2; def y = (float)2; return x * y"));

        assertEquals(4D, exec("def x = (byte)2; def y = (double)2; return x * y"));
        assertEquals(4D, exec("def x = (short)2; def y = (double)2; return x * y"));
        assertEquals(4D, exec("def x = (char)2; def y = (double)2; return x * y"));
        assertEquals(4D, exec("def x = (int)2; def y = (double)2; return x * y"));
        assertEquals(4D, exec("def x = (long)2; def y = (double)2; return x * y"));
        assertEquals(4D, exec("def x = (float)2; def y = (double)2; return x * y"));
        assertEquals(4D, exec("def x = (double)2; def y = (double)2; return x * y"));

        assertEquals(4, exec("def x = (byte)2; def y = (byte)2; return x * y"));
        assertEquals(4, exec("def x = (short)2; def y = (short)2; return x * y"));
        assertEquals(4, exec("def x = (char)2; def y = (char)2; return x * y"));
        assertEquals(4, exec("def x = (int)2; def y = (int)2; return x * y"));
        assertEquals(4L, exec("def x = (long)2; def y = (long)2; return x * y"));
        assertEquals(4F, exec("def x = (float)2; def y = (float)2; return x * y"));
        assertEquals(4D, exec("def x = (double)2; def y = (double)2; return x * y"));
    }
    
    public void testMulTypedLHS() {
        assertEquals(4, exec("byte x = (byte)2; def y = (byte)2; return x * y"));
        assertEquals(4, exec("short x = (short)2; def y = (byte)2; return x * y"));
        assertEquals(4, exec("char x = (char)2; def y = (byte)2; return x * y"));
        assertEquals(4, exec("int x = (int)2; def y = (byte)2; return x * y"));
        assertEquals(4L, exec("long x = (long)2; def y = (byte)2; return x * y"));
        assertEquals(4F, exec("float x = (float)2; def y = (byte)2; return x * y"));
        assertEquals(4D, exec("double x = (double)2; def y = (byte)2; return x * y"));

        assertEquals(4, exec("byte x = (byte)2; def y = (short)2; return x * y"));
        assertEquals(4, exec("short x = (short)2; def y = (short)2; return x * y"));
        assertEquals(4, exec("char x = (char)2; def y = (short)2; return x * y"));
        assertEquals(4, exec("int x = (int)2; def y = (short)2; return x * y"));
        assertEquals(4L, exec("long x = (long)2; def y = (short)2; return x * y"));
        assertEquals(4F, exec("float x = (float)2; def y = (short)2; return x * y"));
        assertEquals(4D, exec("double x = (double)2; def y = (short)2; return x * y"));

        assertEquals(4, exec("byte x = (byte)2; def y = (char)2; return x * y"));
        assertEquals(4, exec("short x = (short)2; def y = (char)2; return x * y"));
        assertEquals(4, exec("char x = (char)2; def y = (char)2; return x * y"));
        assertEquals(4, exec("int x = (int)2; def y = (char)2; return x * y"));
        assertEquals(4L, exec("long x = (long)2; def y = (char)2; return x * y"));
        assertEquals(4F, exec("float x = (float)2; def y = (char)2; return x * y"));
        assertEquals(4D, exec("double x = (double)2; def y = (char)2; return x * y"));

        assertEquals(4, exec("byte x = (byte)2; def y = (int)2; return x * y"));
        assertEquals(4, exec("short x = (short)2; def y = (int)2; return x * y"));
        assertEquals(4, exec("char x = (char)2; def y = (int)2; return x * y"));
        assertEquals(4, exec("int x = (int)2; def y = (int)2; return x * y"));
        assertEquals(4L, exec("long x = (long)2; def y = (int)2; return x * y"));
        assertEquals(4F, exec("float x = (float)2; def y = (int)2; return x * y"));
        assertEquals(4D, exec("double x = (double)2; def y = (int)2; return x * y"));

        assertEquals(4L, exec("byte x = (byte)2; def y = (long)2; return x * y"));
        assertEquals(4L, exec("short x = (short)2; def y = (long)2; return x * y"));
        assertEquals(4L, exec("char x = (char)2; def y = (long)2; return x * y"));
        assertEquals(4L, exec("int x = (int)2; def y = (long)2; return x * y"));
        assertEquals(4L, exec("long x = (long)2; def y = (long)2; return x * y"));
        assertEquals(4F, exec("float x = (float)2; def y = (long)2; return x * y"));
        assertEquals(4D, exec("double x = (double)2; def y = (long)2; return x * y"));

        assertEquals(4F, exec("byte x = (byte)2; def y = (float)2; return x * y"));
        assertEquals(4F, exec("short x = (short)2; def y = (float)2; return x * y"));
        assertEquals(4F, exec("char x = (char)2; def y = (float)2; return x * y"));
        assertEquals(4F, exec("int x = (int)2; def y = (float)2; return x * y"));
        assertEquals(4F, exec("long x = (long)2; def y = (float)2; return x * y"));
        assertEquals(4F, exec("float x = (float)2; def y = (float)2; return x * y"));
        assertEquals(4D, exec("double x = (double)2; def y = (float)2; return x * y"));

        assertEquals(4D, exec("byte x = (byte)2; def y = (double)2; return x * y"));
        assertEquals(4D, exec("short x = (short)2; def y = (double)2; return x * y"));
        assertEquals(4D, exec("char x = (char)2; def y = (double)2; return x * y"));
        assertEquals(4D, exec("int x = (int)2; def y = (double)2; return x * y"));
        assertEquals(4D, exec("long x = (long)2; def y = (double)2; return x * y"));
        assertEquals(4D, exec("float x = (float)2; def y = (double)2; return x * y"));
        assertEquals(4D, exec("double x = (double)2; def y = (double)2; return x * y"));

        assertEquals(4, exec("byte x = (byte)2; def y = (byte)2; return x * y"));
        assertEquals(4, exec("short x = (short)2; def y = (short)2; return x * y"));
        assertEquals(4, exec("char x = (char)2; def y = (char)2; return x * y"));
        assertEquals(4, exec("int x = (int)2; def y = (int)2; return x * y"));
        assertEquals(4L, exec("long x = (long)2; def y = (long)2; return x * y"));
        assertEquals(4F, exec("float x = (float)2; def y = (float)2; return x * y"));
        assertEquals(4D, exec("double x = (double)2; def y = (double)2; return x * y"));
    }
    
    public void testMulTypedRHS() {
        assertEquals(4, exec("def x = (byte)2; byte y = (byte)2; return x * y"));
        assertEquals(4, exec("def x = (short)2; byte y = (byte)2; return x * y"));
        assertEquals(4, exec("def x = (char)2; byte y = (byte)2; return x * y"));
        assertEquals(4, exec("def x = (int)2; byte y = (byte)2; return x * y"));
        assertEquals(4L, exec("def x = (long)2; byte y = (byte)2; return x * y"));
        assertEquals(4F, exec("def x = (float)2; byte y = (byte)2; return x * y"));
        assertEquals(4D, exec("def x = (double)2; byte y = (byte)2; return x * y"));

        assertEquals(4, exec("def x = (byte)2; short y = (short)2; return x * y"));
        assertEquals(4, exec("def x = (short)2; short y = (short)2; return x * y"));
        assertEquals(4, exec("def x = (char)2; short y = (short)2; return x * y"));
        assertEquals(4, exec("def x = (int)2; short y = (short)2; return x * y"));
        assertEquals(4L, exec("def x = (long)2; short y = (short)2; return x * y"));
        assertEquals(4F, exec("def x = (float)2; short y = (short)2; return x * y"));
        assertEquals(4D, exec("def x = (double)2; short y = (short)2; return x * y"));

        assertEquals(4, exec("def x = (byte)2; char y = (char)2; return x * y"));
        assertEquals(4, exec("def x = (short)2; char y = (char)2; return x * y"));
        assertEquals(4, exec("def x = (char)2; char y = (char)2; return x * y"));
        assertEquals(4, exec("def x = (int)2; char y = (char)2; return x * y"));
        assertEquals(4L, exec("def x = (long)2; char y = (char)2; return x * y"));
        assertEquals(4F, exec("def x = (float)2; char y = (char)2; return x * y"));
        assertEquals(4D, exec("def x = (double)2; char y = (char)2; return x * y"));

        assertEquals(4, exec("def x = (byte)2; int y = (int)2; return x * y"));
        assertEquals(4, exec("def x = (short)2; int y = (int)2; return x * y"));
        assertEquals(4, exec("def x = (char)2; int y = (int)2; return x * y"));
        assertEquals(4, exec("def x = (int)2; int y = (int)2; return x * y"));
        assertEquals(4L, exec("def x = (long)2; int y = (int)2; return x * y"));
        assertEquals(4F, exec("def x = (float)2; int y = (int)2; return x * y"));
        assertEquals(4D, exec("def x = (double)2; int y = (int)2; return x * y"));

        assertEquals(4L, exec("def x = (byte)2; long y = (long)2; return x * y"));
        assertEquals(4L, exec("def x = (short)2; long y = (long)2; return x * y"));
        assertEquals(4L, exec("def x = (char)2; long y = (long)2; return x * y"));
        assertEquals(4L, exec("def x = (int)2; long y = (long)2; return x * y"));
        assertEquals(4L, exec("def x = (long)2; long y = (long)2; return x * y"));
        assertEquals(4F, exec("def x = (float)2; long y = (long)2; return x * y"));
        assertEquals(4D, exec("def x = (double)2; long y = (long)2; return x * y"));

        assertEquals(4F, exec("def x = (byte)2; float y = (float)2; return x * y"));
        assertEquals(4F, exec("def x = (short)2; float y = (float)2; return x * y"));
        assertEquals(4F, exec("def x = (char)2; float y = (float)2; return x * y"));
        assertEquals(4F, exec("def x = (int)2; float y = (float)2; return x * y"));
        assertEquals(4F, exec("def x = (long)2; float y = (float)2; return x * y"));
        assertEquals(4F, exec("def x = (float)2; float y = (float)2; return x * y"));
        assertEquals(4D, exec("def x = (double)2; float y = (float)2; return x * y"));

        assertEquals(4D, exec("def x = (byte)2; double y = (double)2; return x * y"));
        assertEquals(4D, exec("def x = (short)2; double y = (double)2; return x * y"));
        assertEquals(4D, exec("def x = (char)2; double y = (double)2; return x * y"));
        assertEquals(4D, exec("def x = (int)2; double y = (double)2; return x * y"));
        assertEquals(4D, exec("def x = (long)2; double y = (double)2; return x * y"));
        assertEquals(4D, exec("def x = (float)2; double y = (double)2; return x * y"));
        assertEquals(4D, exec("def x = (double)2; double y = (double)2; return x * y"));

        assertEquals(4, exec("def x = (byte)2; byte y = (byte)2; return x * y"));
        assertEquals(4, exec("def x = (short)2; short y = (short)2; return x * y"));
        assertEquals(4, exec("def x = (char)2; char y = (char)2; return x * y"));
        assertEquals(4, exec("def x = (int)2; int y = (int)2; return x * y"));
        assertEquals(4L, exec("def x = (long)2; long y = (long)2; return x * y"));
        assertEquals(4F, exec("def x = (float)2; float y = (float)2; return x * y"));
        assertEquals(4D, exec("def x = (double)2; double y = (double)2; return x * y"));
    }

    public void testDiv() {
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
    
    public void testDivTypedLHS() {
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
    
    public void testDivTypedRHS() {
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

    public void testRem() {
        assertEquals(0, exec("def x = (byte)2; def y = (byte)2; return x % y"));
        assertEquals(0, exec("def x = (short)2; def y = (byte)2; return x % y"));
        assertEquals(0, exec("def x = (char)2; def y = (byte)2; return x % y"));
        assertEquals(0, exec("def x = (int)2; def y = (byte)2; return x % y"));
        assertEquals(0L, exec("def x = (long)2; def y = (byte)2; return x % y"));
        assertEquals(0F, exec("def x = (float)2; def y = (byte)2; return x % y"));
        assertEquals(0D, exec("def x = (double)2; def y = (byte)2; return x % y"));

        assertEquals(0, exec("def x = (byte)2; def y = (short)2; return x % y"));
        assertEquals(0, exec("def x = (short)2; def y = (short)2; return x % y"));
        assertEquals(0, exec("def x = (char)2; def y = (short)2; return x % y"));
        assertEquals(0, exec("def x = (int)2; def y = (short)2; return x % y"));
        assertEquals(0L, exec("def x = (long)2; def y = (short)2; return x % y"));
        assertEquals(0F, exec("def x = (float)2; def y = (short)2; return x % y"));
        assertEquals(0D, exec("def x = (double)2; def y = (short)2; return x % y"));

        assertEquals(0, exec("def x = (byte)2; def y = (char)2; return x % y"));
        assertEquals(0, exec("def x = (short)2; def y = (char)2; return x % y"));
        assertEquals(0, exec("def x = (char)2; def y = (char)2; return x % y"));
        assertEquals(0, exec("def x = (int)2; def y = (char)2; return x % y"));
        assertEquals(0L, exec("def x = (long)2; def y = (char)2; return x % y"));
        assertEquals(0F, exec("def x = (float)2; def y = (char)2; return x % y"));
        assertEquals(0D, exec("def x = (double)2; def y = (char)2; return x % y"));

        assertEquals(0, exec("def x = (byte)2; def y = (int)2; return x % y"));
        assertEquals(0, exec("def x = (short)2; def y = (int)2; return x % y"));
        assertEquals(0, exec("def x = (char)2; def y = (int)2; return x % y"));
        assertEquals(0, exec("def x = (int)2; def y = (int)2; return x % y"));
        assertEquals(0L, exec("def x = (long)2; def y = (int)2; return x % y"));
        assertEquals(0F, exec("def x = (float)2; def y = (int)2; return x % y"));
        assertEquals(0D, exec("def x = (double)2; def y = (int)2; return x % y"));

        assertEquals(0L, exec("def x = (byte)2; def y = (long)2; return x % y"));
        assertEquals(0L, exec("def x = (short)2; def y = (long)2; return x % y"));
        assertEquals(0L, exec("def x = (char)2; def y = (long)2; return x % y"));
        assertEquals(0L, exec("def x = (int)2; def y = (long)2; return x % y"));
        assertEquals(0L, exec("def x = (long)2; def y = (long)2; return x % y"));
        assertEquals(0F, exec("def x = (float)2; def y = (long)2; return x % y"));
        assertEquals(0D, exec("def x = (double)2; def y = (long)2; return x % y"));

        assertEquals(0F, exec("def x = (byte)2; def y = (float)2; return x % y"));
        assertEquals(0F, exec("def x = (short)2; def y = (float)2; return x % y"));
        assertEquals(0F, exec("def x = (char)2; def y = (float)2; return x % y"));
        assertEquals(0F, exec("def x = (int)2; def y = (float)2; return x % y"));
        assertEquals(0F, exec("def x = (long)2; def y = (float)2; return x % y"));
        assertEquals(0F, exec("def x = (float)2; def y = (float)2; return x % y"));
        assertEquals(0D, exec("def x = (double)2; def y = (float)2; return x % y"));

        assertEquals(0D, exec("def x = (byte)2; def y = (double)2; return x % y"));
        assertEquals(0D, exec("def x = (short)2; def y = (double)2; return x % y"));
        assertEquals(0D, exec("def x = (char)2; def y = (double)2; return x % y"));
        assertEquals(0D, exec("def x = (int)2; def y = (double)2; return x % y"));
        assertEquals(0D, exec("def x = (long)2; def y = (double)2; return x % y"));
        assertEquals(0D, exec("def x = (float)2; def y = (double)2; return x % y"));
        assertEquals(0D, exec("def x = (double)2; def y = (double)2; return x % y"));

        assertEquals(0, exec("def x = (byte)2; def y = (byte)2; return x % y"));
        assertEquals(0, exec("def x = (short)2; def y = (short)2; return x % y"));
        assertEquals(0, exec("def x = (char)2; def y = (char)2; return x % y"));
        assertEquals(0, exec("def x = (int)2; def y = (int)2; return x % y"));
        assertEquals(0L, exec("def x = (long)2; def y = (long)2; return x % y"));
        assertEquals(0F, exec("def x = (float)2; def y = (float)2; return x % y"));
        assertEquals(0D, exec("def x = (double)2; def y = (double)2; return x % y"));
    }
    
    public void testRemTypedLHS() {
        assertEquals(0, exec("byte x = (byte)2; def y = (byte)2; return x % y"));
        assertEquals(0, exec("short x = (short)2; def y = (byte)2; return x % y"));
        assertEquals(0, exec("char x = (char)2; def y = (byte)2; return x % y"));
        assertEquals(0, exec("int x = (int)2; def y = (byte)2; return x % y"));
        assertEquals(0L, exec("long x = (long)2; def y = (byte)2; return x % y"));
        assertEquals(0F, exec("float x = (float)2; def y = (byte)2; return x % y"));
        assertEquals(0D, exec("double x = (double)2; def y = (byte)2; return x % y"));

        assertEquals(0, exec("byte x = (byte)2; def y = (short)2; return x % y"));
        assertEquals(0, exec("short x = (short)2; def y = (short)2; return x % y"));
        assertEquals(0, exec("char x = (char)2; def y = (short)2; return x % y"));
        assertEquals(0, exec("int x = (int)2; def y = (short)2; return x % y"));
        assertEquals(0L, exec("long x = (long)2; def y = (short)2; return x % y"));
        assertEquals(0F, exec("float x = (float)2; def y = (short)2; return x % y"));
        assertEquals(0D, exec("double x = (double)2; def y = (short)2; return x % y"));

        assertEquals(0, exec("byte x = (byte)2; def y = (char)2; return x % y"));
        assertEquals(0, exec("short x = (short)2; def y = (char)2; return x % y"));
        assertEquals(0, exec("char x = (char)2; def y = (char)2; return x % y"));
        assertEquals(0, exec("int x = (int)2; def y = (char)2; return x % y"));
        assertEquals(0L, exec("long x = (long)2; def y = (char)2; return x % y"));
        assertEquals(0F, exec("float x = (float)2; def y = (char)2; return x % y"));
        assertEquals(0D, exec("double x = (double)2; def y = (char)2; return x % y"));

        assertEquals(0, exec("byte x = (byte)2; def y = (int)2; return x % y"));
        assertEquals(0, exec("short x = (short)2; def y = (int)2; return x % y"));
        assertEquals(0, exec("char x = (char)2; def y = (int)2; return x % y"));
        assertEquals(0, exec("int x = (int)2; def y = (int)2; return x % y"));
        assertEquals(0L, exec("long x = (long)2; def y = (int)2; return x % y"));
        assertEquals(0F, exec("float x = (float)2; def y = (int)2; return x % y"));
        assertEquals(0D, exec("double x = (double)2; def y = (int)2; return x % y"));

        assertEquals(0L, exec("byte x = (byte)2; def y = (long)2; return x % y"));
        assertEquals(0L, exec("short x = (short)2; def y = (long)2; return x % y"));
        assertEquals(0L, exec("char x = (char)2; def y = (long)2; return x % y"));
        assertEquals(0L, exec("int x = (int)2; def y = (long)2; return x % y"));
        assertEquals(0L, exec("long x = (long)2; def y = (long)2; return x % y"));
        assertEquals(0F, exec("float x = (float)2; def y = (long)2; return x % y"));
        assertEquals(0D, exec("double x = (double)2; def y = (long)2; return x % y"));

        assertEquals(0F, exec("byte x = (byte)2; def y = (float)2; return x % y"));
        assertEquals(0F, exec("short x = (short)2; def y = (float)2; return x % y"));
        assertEquals(0F, exec("char x = (char)2; def y = (float)2; return x % y"));
        assertEquals(0F, exec("int x = (int)2; def y = (float)2; return x % y"));
        assertEquals(0F, exec("long x = (long)2; def y = (float)2; return x % y"));
        assertEquals(0F, exec("float x = (float)2; def y = (float)2; return x % y"));
        assertEquals(0D, exec("double x = (double)2; def y = (float)2; return x % y"));

        assertEquals(0D, exec("byte x = (byte)2; def y = (double)2; return x % y"));
        assertEquals(0D, exec("short x = (short)2; def y = (double)2; return x % y"));
        assertEquals(0D, exec("char x = (char)2; def y = (double)2; return x % y"));
        assertEquals(0D, exec("int x = (int)2; def y = (double)2; return x % y"));
        assertEquals(0D, exec("long x = (long)2; def y = (double)2; return x % y"));
        assertEquals(0D, exec("float x = (float)2; def y = (double)2; return x % y"));
        assertEquals(0D, exec("double x = (double)2; def y = (double)2; return x % y"));

        assertEquals(0, exec("byte x = (byte)2; def y = (byte)2; return x % y"));
        assertEquals(0, exec("short x = (short)2; def y = (short)2; return x % y"));
        assertEquals(0, exec("char x = (char)2; def y = (char)2; return x % y"));
        assertEquals(0, exec("int x = (int)2; def y = (int)2; return x % y"));
        assertEquals(0L, exec("long x = (long)2; def y = (long)2; return x % y"));
        assertEquals(0F, exec("float x = (float)2; def y = (float)2; return x % y"));
        assertEquals(0D, exec("double x = (double)2; def y = (double)2; return x % y"));
    }

    public void testRemTypedRHS() {
        assertEquals(0, exec("def x = (byte)2; byte y = (byte)2; return x % y"));
        assertEquals(0, exec("def x = (short)2; byte y = (byte)2; return x % y"));
        assertEquals(0, exec("def x = (char)2; byte y = (byte)2; return x % y"));
        assertEquals(0, exec("def x = (int)2; byte y = (byte)2; return x % y"));
        assertEquals(0L, exec("def x = (long)2; byte y = (byte)2; return x % y"));
        assertEquals(0F, exec("def x = (float)2; byte y = (byte)2; return x % y"));
        assertEquals(0D, exec("def x = (double)2; byte y = (byte)2; return x % y"));

        assertEquals(0, exec("def x = (byte)2; short y = (short)2; return x % y"));
        assertEquals(0, exec("def x = (short)2; short y = (short)2; return x % y"));
        assertEquals(0, exec("def x = (char)2; short y = (short)2; return x % y"));
        assertEquals(0, exec("def x = (int)2; short y = (short)2; return x % y"));
        assertEquals(0L, exec("def x = (long)2; short y = (short)2; return x % y"));
        assertEquals(0F, exec("def x = (float)2; short y = (short)2; return x % y"));
        assertEquals(0D, exec("def x = (double)2; short y = (short)2; return x % y"));

        assertEquals(0, exec("def x = (byte)2; char y = (char)2; return x % y"));
        assertEquals(0, exec("def x = (short)2; char y = (char)2; return x % y"));
        assertEquals(0, exec("def x = (char)2; char y = (char)2; return x % y"));
        assertEquals(0, exec("def x = (int)2; char y = (char)2; return x % y"));
        assertEquals(0L, exec("def x = (long)2; char y = (char)2; return x % y"));
        assertEquals(0F, exec("def x = (float)2; char y = (char)2; return x % y"));
        assertEquals(0D, exec("def x = (double)2; char y = (char)2; return x % y"));

        assertEquals(0, exec("def x = (byte)2; int y = (int)2; return x % y"));
        assertEquals(0, exec("def x = (short)2; int y = (int)2; return x % y"));
        assertEquals(0, exec("def x = (char)2; int y = (int)2; return x % y"));
        assertEquals(0, exec("def x = (int)2; int y = (int)2; return x % y"));
        assertEquals(0L, exec("def x = (long)2; int y = (int)2; return x % y"));
        assertEquals(0F, exec("def x = (float)2; int y = (int)2; return x % y"));
        assertEquals(0D, exec("def x = (double)2; int y = (int)2; return x % y"));

        assertEquals(0L, exec("def x = (byte)2; long y = (long)2; return x % y"));
        assertEquals(0L, exec("def x = (short)2; long y = (long)2; return x % y"));
        assertEquals(0L, exec("def x = (char)2; long y = (long)2; return x % y"));
        assertEquals(0L, exec("def x = (int)2; long y = (long)2; return x % y"));
        assertEquals(0L, exec("def x = (long)2; long y = (long)2; return x % y"));
        assertEquals(0F, exec("def x = (float)2; long y = (long)2; return x % y"));
        assertEquals(0D, exec("def x = (double)2; long y = (long)2; return x % y"));

        assertEquals(0F, exec("def x = (byte)2; float y = (float)2; return x % y"));
        assertEquals(0F, exec("def x = (short)2; float y = (float)2; return x % y"));
        assertEquals(0F, exec("def x = (char)2; float y = (float)2; return x % y"));
        assertEquals(0F, exec("def x = (int)2; float y = (float)2; return x % y"));
        assertEquals(0F, exec("def x = (long)2; float y = (float)2; return x % y"));
        assertEquals(0F, exec("def x = (float)2; float y = (float)2; return x % y"));
        assertEquals(0D, exec("def x = (double)2; float y = (float)2; return x % y"));

        assertEquals(0D, exec("def x = (byte)2; double y = (double)2; return x % y"));
        assertEquals(0D, exec("def x = (short)2; double y = (double)2; return x % y"));
        assertEquals(0D, exec("def x = (char)2; double y = (double)2; return x % y"));
        assertEquals(0D, exec("def x = (int)2; double y = (double)2; return x % y"));
        assertEquals(0D, exec("def x = (long)2; double y = (double)2; return x % y"));
        assertEquals(0D, exec("def x = (float)2; double y = (double)2; return x % y"));
        assertEquals(0D, exec("def x = (double)2; double y = (double)2; return x % y"));

        assertEquals(0, exec("def x = (byte)2; byte y = (byte)2; return x % y"));
        assertEquals(0, exec("def x = (short)2; short y = (short)2; return x % y"));
        assertEquals(0, exec("def x = (char)2; char y = (char)2; return x % y"));
        assertEquals(0, exec("def x = (int)2; int y = (int)2; return x % y"));
        assertEquals(0L, exec("def x = (long)2; long y = (long)2; return x % y"));
        assertEquals(0F, exec("def x = (float)2; float y = (float)2; return x % y"));
        assertEquals(0D, exec("def x = (double)2; double y = (double)2; return x % y"));
    }

    public void testAdd() {
        assertEquals(2, exec("def x = (byte)1; def y = (byte)1; return x + y"));
        assertEquals(2, exec("def x = (short)1; def y = (byte)1; return x + y"));
        assertEquals(2, exec("def x = (char)1; def y = (byte)1; return x + y"));
        assertEquals(2, exec("def x = (int)1; def y = (byte)1; return x + y"));
        assertEquals(2L, exec("def x = (long)1; def y = (byte)1; return x + y"));
        assertEquals(2F, exec("def x = (float)1; def y = (byte)1; return x + y"));
        assertEquals(2D, exec("def x = (double)1; def y = (byte)1; return x + y"));

        assertEquals(2, exec("def x = (byte)1; def y = (short)1; return x + y"));
        assertEquals(2, exec("def x = (short)1; def y = (short)1; return x + y"));
        assertEquals(2, exec("def x = (char)1; def y = (short)1; return x + y"));
        assertEquals(2, exec("def x = (int)1; def y = (short)1; return x + y"));
        assertEquals(2L, exec("def x = (long)1; def y = (short)1; return x + y"));
        assertEquals(2F, exec("def x = (float)1; def y = (short)1; return x + y"));
        assertEquals(2D, exec("def x = (double)1; def y = (short)1; return x + y"));

        assertEquals(2, exec("def x = (byte)1; def y = (char)1; return x + y"));
        assertEquals(2, exec("def x = (short)1; def y = (char)1; return x + y"));
        assertEquals(2, exec("def x = (char)1; def y = (char)1; return x + y"));
        assertEquals(2, exec("def x = (int)1; def y = (char)1; return x + y"));
        assertEquals(2L, exec("def x = (long)1; def y = (char)1; return x + y"));
        assertEquals(2F, exec("def x = (float)1; def y = (char)1; return x + y"));
        assertEquals(2D, exec("def x = (double)1; def y = (char)1; return x + y"));

        assertEquals(2, exec("def x = (byte)1; def y = (int)1; return x + y"));
        assertEquals(2, exec("def x = (short)1; def y = (int)1; return x + y"));
        assertEquals(2, exec("def x = (char)1; def y = (int)1; return x + y"));
        assertEquals(2, exec("def x = (int)1; def y = (int)1; return x + y"));
        assertEquals(2L, exec("def x = (long)1; def y = (int)1; return x + y"));
        assertEquals(2F, exec("def x = (float)1; def y = (int)1; return x + y"));
        assertEquals(2D, exec("def x = (double)1; def y = (int)1; return x + y"));

        assertEquals(2L, exec("def x = (byte)1; def y = (long)1; return x + y"));
        assertEquals(2L, exec("def x = (short)1; def y = (long)1; return x + y"));
        assertEquals(2L, exec("def x = (char)1; def y = (long)1; return x + y"));
        assertEquals(2L, exec("def x = (int)1; def y = (long)1; return x + y"));
        assertEquals(2L, exec("def x = (long)1; def y = (long)1; return x + y"));
        assertEquals(2F, exec("def x = (float)1; def y = (long)1; return x + y"));
        assertEquals(2D, exec("def x = (double)1; def y = (long)1; return x + y"));

        assertEquals(2F, exec("def x = (byte)1; def y = (float)1; return x + y"));
        assertEquals(2F, exec("def x = (short)1; def y = (float)1; return x + y"));
        assertEquals(2F, exec("def x = (char)1; def y = (float)1; return x + y"));
        assertEquals(2F, exec("def x = (int)1; def y = (float)1; return x + y"));
        assertEquals(2F, exec("def x = (long)1; def y = (float)1; return x + y"));
        assertEquals(2F, exec("def x = (float)1; def y = (float)1; return x + y"));
        assertEquals(2D, exec("def x = (double)1; def y = (float)1; return x + y"));

        assertEquals(2D, exec("def x = (byte)1; def y = (double)1; return x + y"));
        assertEquals(2D, exec("def x = (short)1; def y = (double)1; return x + y"));
        assertEquals(2D, exec("def x = (char)1; def y = (double)1; return x + y"));
        assertEquals(2D, exec("def x = (int)1; def y = (double)1; return x + y"));
        assertEquals(2D, exec("def x = (long)1; def y = (double)1; return x + y"));
        assertEquals(2D, exec("def x = (float)1; def y = (double)1; return x + y"));
        assertEquals(2D, exec("def x = (double)1; def y = (double)1; return x + y"));
    }
    
    public void testAddTypedLHS() {
        assertEquals(2, exec("byte x = (byte)1; def y = (byte)1; return x + y"));
        assertEquals(2, exec("short x = (short)1; def y = (byte)1; return x + y"));
        assertEquals(2, exec("char x = (char)1; def y = (byte)1; return x + y"));
        assertEquals(2, exec("int x = (int)1; def y = (byte)1; return x + y"));
        assertEquals(2L, exec("long x = (long)1; def y = (byte)1; return x + y"));
        assertEquals(2F, exec("float x = (float)1; def y = (byte)1; return x + y"));
        assertEquals(2D, exec("double x = (double)1; def y = (byte)1; return x + y"));

        assertEquals(2, exec("byte x = (byte)1; def y = (short)1; return x + y"));
        assertEquals(2, exec("short x = (short)1; def y = (short)1; return x + y"));
        assertEquals(2, exec("char x = (char)1; def y = (short)1; return x + y"));
        assertEquals(2, exec("int x = (int)1; def y = (short)1; return x + y"));
        assertEquals(2L, exec("long x = (long)1; def y = (short)1; return x + y"));
        assertEquals(2F, exec("float x = (float)1; def y = (short)1; return x + y"));
        assertEquals(2D, exec("double x = (double)1; def y = (short)1; return x + y"));

        assertEquals(2, exec("byte x = (byte)1; def y = (char)1; return x + y"));
        assertEquals(2, exec("short x = (short)1; def y = (char)1; return x + y"));
        assertEquals(2, exec("char x = (char)1; def y = (char)1; return x + y"));
        assertEquals(2, exec("int x = (int)1; def y = (char)1; return x + y"));
        assertEquals(2L, exec("long x = (long)1; def y = (char)1; return x + y"));
        assertEquals(2F, exec("float x = (float)1; def y = (char)1; return x + y"));
        assertEquals(2D, exec("double x = (double)1; def y = (char)1; return x + y"));

        assertEquals(2, exec("byte x = (byte)1; def y = (int)1; return x + y"));
        assertEquals(2, exec("short x = (short)1; def y = (int)1; return x + y"));
        assertEquals(2, exec("char x = (char)1; def y = (int)1; return x + y"));
        assertEquals(2, exec("int x = (int)1; def y = (int)1; return x + y"));
        assertEquals(2L, exec("long x = (long)1; def y = (int)1; return x + y"));
        assertEquals(2F, exec("float x = (float)1; def y = (int)1; return x + y"));
        assertEquals(2D, exec("double x = (double)1; def y = (int)1; return x + y"));

        assertEquals(2L, exec("byte x = (byte)1; def y = (long)1; return x + y"));
        assertEquals(2L, exec("short x = (short)1; def y = (long)1; return x + y"));
        assertEquals(2L, exec("char x = (char)1; def y = (long)1; return x + y"));
        assertEquals(2L, exec("int x = (int)1; def y = (long)1; return x + y"));
        assertEquals(2L, exec("long x = (long)1; def y = (long)1; return x + y"));
        assertEquals(2F, exec("float x = (float)1; def y = (long)1; return x + y"));
        assertEquals(2D, exec("double x = (double)1; def y = (long)1; return x + y"));

        assertEquals(2F, exec("byte x = (byte)1; def y = (float)1; return x + y"));
        assertEquals(2F, exec("short x = (short)1; def y = (float)1; return x + y"));
        assertEquals(2F, exec("char x = (char)1; def y = (float)1; return x + y"));
        assertEquals(2F, exec("int x = (int)1; def y = (float)1; return x + y"));
        assertEquals(2F, exec("long x = (long)1; def y = (float)1; return x + y"));
        assertEquals(2F, exec("float x = (float)1; def y = (float)1; return x + y"));
        assertEquals(2D, exec("double x = (double)1; def y = (float)1; return x + y"));

        assertEquals(2D, exec("byte x = (byte)1; def y = (double)1; return x + y"));
        assertEquals(2D, exec("short x = (short)1; def y = (double)1; return x + y"));
        assertEquals(2D, exec("char x = (char)1; def y = (double)1; return x + y"));
        assertEquals(2D, exec("int x = (int)1; def y = (double)1; return x + y"));
        assertEquals(2D, exec("long x = (long)1; def y = (double)1; return x + y"));
        assertEquals(2D, exec("float x = (float)1; def y = (double)1; return x + y"));
        assertEquals(2D, exec("double x = (double)1; def y = (double)1; return x + y"));
    }
    
    public void testAddTypedRHS() {
        assertEquals(2, exec("def x = (byte)1; byte y = (byte)1; return x + y"));
        assertEquals(2, exec("def x = (short)1; byte y = (byte)1; return x + y"));
        assertEquals(2, exec("def x = (char)1; byte y = (byte)1; return x + y"));
        assertEquals(2, exec("def x = (int)1; byte y = (byte)1; return x + y"));
        assertEquals(2L, exec("def x = (long)1; byte y = (byte)1; return x + y"));
        assertEquals(2F, exec("def x = (float)1; byte y = (byte)1; return x + y"));
        assertEquals(2D, exec("def x = (double)1; byte y = (byte)1; return x + y"));

        assertEquals(2, exec("def x = (byte)1; short y = (short)1; return x + y"));
        assertEquals(2, exec("def x = (short)1; short y = (short)1; return x + y"));
        assertEquals(2, exec("def x = (char)1; short y = (short)1; return x + y"));
        assertEquals(2, exec("def x = (int)1; short y = (short)1; return x + y"));
        assertEquals(2L, exec("def x = (long)1; short y = (short)1; return x + y"));
        assertEquals(2F, exec("def x = (float)1; short y = (short)1; return x + y"));
        assertEquals(2D, exec("def x = (double)1; short y = (short)1; return x + y"));

        assertEquals(2, exec("def x = (byte)1; char y = (char)1; return x + y"));
        assertEquals(2, exec("def x = (short)1; char y = (char)1; return x + y"));
        assertEquals(2, exec("def x = (char)1; char y = (char)1; return x + y"));
        assertEquals(2, exec("def x = (int)1; char y = (char)1; return x + y"));
        assertEquals(2L, exec("def x = (long)1; char y = (char)1; return x + y"));
        assertEquals(2F, exec("def x = (float)1; char y = (char)1; return x + y"));
        assertEquals(2D, exec("def x = (double)1; char y = (char)1; return x + y"));

        assertEquals(2, exec("def x = (byte)1; int y = (int)1; return x + y"));
        assertEquals(2, exec("def x = (short)1; int y = (int)1; return x + y"));
        assertEquals(2, exec("def x = (char)1; int y = (int)1; return x + y"));
        assertEquals(2, exec("def x = (int)1; int y = (int)1; return x + y"));
        assertEquals(2L, exec("def x = (long)1; int y = (int)1; return x + y"));
        assertEquals(2F, exec("def x = (float)1; int y = (int)1; return x + y"));
        assertEquals(2D, exec("def x = (double)1; int y = (int)1; return x + y"));

        assertEquals(2L, exec("def x = (byte)1; long y = (long)1; return x + y"));
        assertEquals(2L, exec("def x = (short)1; long y = (long)1; return x + y"));
        assertEquals(2L, exec("def x = (char)1; long y = (long)1; return x + y"));
        assertEquals(2L, exec("def x = (int)1; long y = (long)1; return x + y"));
        assertEquals(2L, exec("def x = (long)1; long y = (long)1; return x + y"));
        assertEquals(2F, exec("def x = (float)1; long y = (long)1; return x + y"));
        assertEquals(2D, exec("def x = (double)1; long y = (long)1; return x + y"));

        assertEquals(2F, exec("def x = (byte)1; float y = (float)1; return x + y"));
        assertEquals(2F, exec("def x = (short)1; float y = (float)1; return x + y"));
        assertEquals(2F, exec("def x = (char)1; float y = (float)1; return x + y"));
        assertEquals(2F, exec("def x = (int)1; float y = (float)1; return x + y"));
        assertEquals(2F, exec("def x = (long)1; float y = (float)1; return x + y"));
        assertEquals(2F, exec("def x = (float)1; float y = (float)1; return x + y"));
        assertEquals(2D, exec("def x = (double)1; float y = (float)1; return x + y"));

        assertEquals(2D, exec("def x = (byte)1; double y = (double)1; return x + y"));
        assertEquals(2D, exec("def x = (short)1; double y = (double)1; return x + y"));
        assertEquals(2D, exec("def x = (char)1; double y = (double)1; return x + y"));
        assertEquals(2D, exec("def x = (int)1; double y = (double)1; return x + y"));
        assertEquals(2D, exec("def x = (long)1; double y = (double)1; return x + y"));
        assertEquals(2D, exec("def x = (float)1; double y = (double)1; return x + y"));
        assertEquals(2D, exec("def x = (double)1; double y = (double)1; return x + y"));
    }
    
    public void testAddConcat() {
        assertEquals("a" + (byte)2, exec("def x = 'a'; def y = (byte)2; return x + y"));
        assertEquals("a" + (short)2, exec("def x = 'a'; def y = (short)2; return x + y"));
        assertEquals("a" + (char)2, exec("def x = 'a'; def y = (char)2; return x + y"));
        assertEquals("a" + 2, exec("def x = 'a'; def y = (int)2; return x + y"));
        assertEquals("a" + 2L, exec("def x = 'a'; def y = (long)2; return x + y"));
        assertEquals("a" + 2F, exec("def x = 'a'; def y = (float)2; return x + y"));
        assertEquals("a" + 2D, exec("def x = 'a'; def y = (double)2; return x + y"));
        assertEquals("ab", exec("def x = 'a'; def y = 'b'; return x + y"));
        assertEquals((byte)2 + "a", exec("def x = 'a'; def y = (byte)2; return y + x"));
        assertEquals((short)2 + "a", exec("def x = 'a'; def y = (short)2; return y + x"));
        assertEquals((char)2 + "a", exec("def x = 'a'; def y = (char)2; return y + x"));
        assertEquals(2 + "a", exec("def x = 'a'; def y = (int)2; return y + x"));
        assertEquals(2L + "a", exec("def x = 'a'; def y = (long)2; return y + x"));
        assertEquals(2F + "a", exec("def x = 'a'; def y = (float)2; return y + x"));
        assertEquals(2D + "a", exec("def x = 'a'; def y = (double)2; return y + x"));
        assertEquals("anull", exec("def x = 'a'; def y = null; return x + y"));
        assertEquals("nullb", exec("def x = null; def y = 'b'; return x + y"));
        expectScriptThrows(NullPointerException.class, () -> {
            exec("def x = null; def y = null; return x + y");
        });
    }

    public void testSub() {
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
    
    public void testSubTypedLHS() {
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
    
    public void testSubTypedRHS() {
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

    public void testLsh() {
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
    
    public void testLshTypedLHS() {
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
    
    public void testLshTypedRHS() {
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

    public void testRsh() {
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
    
    public void testRshTypeLHS() {
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
    
    public void testRshTypedLHS() {
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

    public void testUsh() {
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
    
    public void testUshTypedLHS() {
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
    
    public void testUshTypedRHS() {
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
    
    public void testBogusShifts() {
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
    
    public void testBogusShiftsTypedLHS() {
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
    
    public void testBogusShiftsTypedRHS() {
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

    public void testAnd() {
        expectScriptThrows(ClassCastException.class, () -> {
            exec("def x = (float)4; def y = (byte)1; return x & y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("def x = (double)4; def y = (byte)1; return x & y");
        });
        assertEquals(0, exec("def x = (byte)4; def y = (byte)1; return x & y"));
        assertEquals(0, exec("def x = (short)4; def y = (byte)1; return x & y"));
        assertEquals(0, exec("def x = (char)4; def y = (byte)1; return x & y"));
        assertEquals(0, exec("def x = (int)4; def y = (byte)1; return x & y"));
        assertEquals(0L, exec("def x = (long)4; def y = (byte)1; return x & y"));

        assertEquals(0, exec("def x = (byte)4; def y = (short)1; return x & y"));
        assertEquals(0, exec("def x = (short)4; def y = (short)1; return x & y"));
        assertEquals(0, exec("def x = (char)4; def y = (short)1; return x & y"));
        assertEquals(0, exec("def x = (int)4; def y = (short)1; return x & y"));
        assertEquals(0L, exec("def x = (long)4; def y = (short)1; return x & y"));

        assertEquals(0, exec("def x = (byte)4; def y = (char)1; return x & y"));
        assertEquals(0, exec("def x = (short)4; def y = (char)1; return x & y"));
        assertEquals(0, exec("def x = (char)4; def y = (char)1; return x & y"));
        assertEquals(0, exec("def x = (int)4; def y = (char)1; return x & y"));
        assertEquals(0L, exec("def x = (long)4; def y = (char)1; return x & y"));

        assertEquals(0, exec("def x = (byte)4; def y = (int)1; return x & y"));
        assertEquals(0, exec("def x = (short)4; def y = (int)1; return x & y"));
        assertEquals(0, exec("def x = (char)4; def y = (int)1; return x & y"));
        assertEquals(0, exec("def x = (int)4; def y = (int)1; return x & y"));
        assertEquals(0L, exec("def x = (long)4; def y = (int)1; return x & y"));

        assertEquals(0L, exec("def x = (byte)4; def y = (long)1; return x & y"));
        assertEquals(0L, exec("def x = (short)4; def y = (long)1; return x & y"));
        assertEquals(0L, exec("def x = (char)4; def y = (long)1; return x & y"));
        assertEquals(0L, exec("def x = (int)4; def y = (long)1; return x & y"));
        assertEquals(0L, exec("def x = (long)4; def y = (long)1; return x & y"));

        assertEquals(0, exec("def x = (byte)4; def y = (byte)1; return x & y"));
        assertEquals(0, exec("def x = (short)4; def y = (short)1; return x & y"));
        assertEquals(0, exec("def x = (char)4; def y = (char)1; return x & y"));
        assertEquals(0, exec("def x = (int)4; def y = (int)1; return x & y"));
        assertEquals(0L, exec("def x = (long)4; def y = (long)1; return x & y"));

        assertEquals(true,  exec("def x = true;  def y = true; return x & y"));
        assertEquals(false, exec("def x = true;  def y = false; return x & y"));
        assertEquals(false, exec("def x = false; def y = true; return x & y"));
        assertEquals(false, exec("def x = false; def y = false; return x & y"));
    }
    
    public void testAndTypedLHS() {
        expectScriptThrows(ClassCastException.class, () -> {
            exec("float x = (float)4; def y = (byte)1; return x & y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("double x = (double)4; def y = (byte)1; return x & y");
        });
        assertEquals(0, exec("byte x = (byte)4; def y = (byte)1; return x & y"));
        assertEquals(0, exec("short x = (short)4; def y = (byte)1; return x & y"));
        assertEquals(0, exec("char x = (char)4; def y = (byte)1; return x & y"));
        assertEquals(0, exec("int x = (int)4; def y = (byte)1; return x & y"));
        assertEquals(0L, exec("long x = (long)4; def y = (byte)1; return x & y"));

        assertEquals(0, exec("byte x = (byte)4; def y = (short)1; return x & y"));
        assertEquals(0, exec("short x = (short)4; def y = (short)1; return x & y"));
        assertEquals(0, exec("char x = (char)4; def y = (short)1; return x & y"));
        assertEquals(0, exec("int x = (int)4; def y = (short)1; return x & y"));
        assertEquals(0L, exec("long x = (long)4; def y = (short)1; return x & y"));

        assertEquals(0, exec("byte x = (byte)4; def y = (char)1; return x & y"));
        assertEquals(0, exec("short x = (short)4; def y = (char)1; return x & y"));
        assertEquals(0, exec("char x = (char)4; def y = (char)1; return x & y"));
        assertEquals(0, exec("int x = (int)4; def y = (char)1; return x & y"));
        assertEquals(0L, exec("long x = (long)4; def y = (char)1; return x & y"));

        assertEquals(0, exec("byte x = (byte)4; def y = (int)1; return x & y"));
        assertEquals(0, exec("short x = (short)4; def y = (int)1; return x & y"));
        assertEquals(0, exec("char x = (char)4; def y = (int)1; return x & y"));
        assertEquals(0, exec("int x = (int)4; def y = (int)1; return x & y"));
        assertEquals(0L, exec("long x = (long)4; def y = (int)1; return x & y"));

        assertEquals(0L, exec("byte x = (byte)4; def y = (long)1; return x & y"));
        assertEquals(0L, exec("short x = (short)4; def y = (long)1; return x & y"));
        assertEquals(0L, exec("char x = (char)4; def y = (long)1; return x & y"));
        assertEquals(0L, exec("int x = (int)4; def y = (long)1; return x & y"));
        assertEquals(0L, exec("long x = (long)4; def y = (long)1; return x & y"));

        assertEquals(0, exec("byte x = (byte)4; def y = (byte)1; return x & y"));
        assertEquals(0, exec("short x = (short)4; def y = (short)1; return x & y"));
        assertEquals(0, exec("char x = (char)4; def y = (char)1; return x & y"));
        assertEquals(0, exec("int x = (int)4; def y = (int)1; return x & y"));
        assertEquals(0L, exec("long x = (long)4; def y = (long)1; return x & y"));

        assertEquals(true,  exec("boolean x = true;  def y = true; return x & y"));
        assertEquals(false, exec("boolean x = true;  def y = false; return x & y"));
        assertEquals(false, exec("boolean x = false; def y = true; return x & y"));
        assertEquals(false, exec("boolean x = false; def y = false; return x & y"));
    }
    
    public void testAndTypedRHS() {
        expectScriptThrows(ClassCastException.class, () -> {
            exec("def x = (float)4; byte y = (byte)1; return x & y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("def x = (double)4; byte y = (byte)1; return x & y");
        });
        assertEquals(0, exec("def x = (byte)4; byte y = (byte)1; return x & y"));
        assertEquals(0, exec("def x = (short)4; byte y = (byte)1; return x & y"));
        assertEquals(0, exec("def x = (char)4; byte y = (byte)1; return x & y"));
        assertEquals(0, exec("def x = (int)4; byte y = (byte)1; return x & y"));
        assertEquals(0L, exec("def x = (long)4; byte y = (byte)1; return x & y"));

        assertEquals(0, exec("def x = (byte)4; short y = (short)1; return x & y"));
        assertEquals(0, exec("def x = (short)4; short y = (short)1; return x & y"));
        assertEquals(0, exec("def x = (char)4; short y = (short)1; return x & y"));
        assertEquals(0, exec("def x = (int)4; short y = (short)1; return x & y"));
        assertEquals(0L, exec("def x = (long)4; short y = (short)1; return x & y"));

        assertEquals(0, exec("def x = (byte)4; char y = (char)1; return x & y"));
        assertEquals(0, exec("def x = (short)4; char y = (char)1; return x & y"));
        assertEquals(0, exec("def x = (char)4; char y = (char)1; return x & y"));
        assertEquals(0, exec("def x = (int)4; char y = (char)1; return x & y"));
        assertEquals(0L, exec("def x = (long)4; char y = (char)1; return x & y"));

        assertEquals(0, exec("def x = (byte)4; int y = (int)1; return x & y"));
        assertEquals(0, exec("def x = (short)4; int y = (int)1; return x & y"));
        assertEquals(0, exec("def x = (char)4; int y = (int)1; return x & y"));
        assertEquals(0, exec("def x = (int)4; int y = (int)1; return x & y"));
        assertEquals(0L, exec("def x = (long)4; int y = (int)1; return x & y"));

        assertEquals(0L, exec("def x = (byte)4; long y = (long)1; return x & y"));
        assertEquals(0L, exec("def x = (short)4; long y = (long)1; return x & y"));
        assertEquals(0L, exec("def x = (char)4; long y = (long)1; return x & y"));
        assertEquals(0L, exec("def x = (int)4; long y = (long)1; return x & y"));
        assertEquals(0L, exec("def x = (long)4; long y = (long)1; return x & y"));

        assertEquals(0, exec("def x = (byte)4; byte y = (byte)1; return x & y"));
        assertEquals(0, exec("def x = (short)4; short y = (short)1; return x & y"));
        assertEquals(0, exec("def x = (char)4; char y = (char)1; return x & y"));
        assertEquals(0, exec("def x = (int)4; int y = (int)1; return x & y"));
        assertEquals(0L, exec("def x = (long)4; long y = (long)1; return x & y"));

        assertEquals(true,  exec("def x = true;  boolean y = true; return x & y"));
        assertEquals(false, exec("def x = true;  boolean y = false; return x & y"));
        assertEquals(false, exec("def x = false; boolean y = true; return x & y"));
        assertEquals(false, exec("def x = false; boolean y = false; return x & y"));
    }

    public void testXor() {
        expectScriptThrows(ClassCastException.class, () -> {
            exec("def x = (float)4; def y = (byte)1; return x ^ y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("def x = (double)4; def y = (byte)1; return x ^ y");
        });
        assertEquals(5, exec("def x = (byte)4; def y = (byte)1; return x ^ y"));
        assertEquals(5, exec("def x = (short)4; def y = (byte)1; return x ^ y"));
        assertEquals(5, exec("def x = (char)4; def y = (byte)1; return x ^ y"));
        assertEquals(5, exec("def x = (int)4; def y = (byte)1; return x ^ y"));
        assertEquals(5L, exec("def x = (long)4; def y = (byte)1; return x ^ y"));

        assertEquals(5, exec("def x = (byte)4; def y = (short)1; return x ^ y"));
        assertEquals(5, exec("def x = (short)4; def y = (short)1; return x ^ y"));
        assertEquals(5, exec("def x = (char)4; def y = (short)1; return x ^ y"));
        assertEquals(5, exec("def x = (int)4; def y = (short)1; return x ^ y"));
        assertEquals(5L, exec("def x = (long)4; def y = (short)1; return x ^ y"));

        assertEquals(5, exec("def x = (byte)4; def y = (char)1; return x ^ y"));
        assertEquals(5, exec("def x = (short)4; def y = (char)1; return x ^ y"));
        assertEquals(5, exec("def x = (char)4; def y = (char)1; return x ^ y"));
        assertEquals(5, exec("def x = (int)4; def y = (char)1; return x ^ y"));
        assertEquals(5L, exec("def x = (long)4; def y = (char)1; return x ^ y"));

        assertEquals(5, exec("def x = (byte)4; def y = (int)1; return x ^ y"));
        assertEquals(5, exec("def x = (short)4; def y = (int)1; return x ^ y"));
        assertEquals(5, exec("def x = (char)4; def y = (int)1; return x ^ y"));
        assertEquals(5, exec("def x = (int)4; def y = (int)1; return x ^ y"));
        assertEquals(5L, exec("def x = (long)4; def y = (int)1; return x ^ y"));

        assertEquals(5L, exec("def x = (byte)4; def y = (long)1; return x ^ y"));
        assertEquals(5L, exec("def x = (short)4; def y = (long)1; return x ^ y"));
        assertEquals(5L, exec("def x = (char)4; def y = (long)1; return x ^ y"));
        assertEquals(5L, exec("def x = (int)4; def y = (long)1; return x ^ y"));
        assertEquals(5L, exec("def x = (long)4; def y = (long)1; return x ^ y"));

        assertEquals(5, exec("def x = (byte)4; def y = (byte)1; return x ^ y"));
        assertEquals(5, exec("def x = (short)4; def y = (short)1; return x ^ y"));
        assertEquals(5, exec("def x = (char)4; def y = (char)1; return x ^ y"));
        assertEquals(5, exec("def x = (int)4; def y = (int)1; return x ^ y"));
        assertEquals(5L, exec("def x = (long)4; def y = (long)1; return x ^ y"));
        
        assertEquals(false, exec("def x = true;  def y = true; return x ^ y"));
        assertEquals(true,  exec("def x = true;  def y = false; return x ^ y"));
        assertEquals(true,  exec("def x = false; def y = true; return x ^ y"));
        assertEquals(false, exec("def x = false; def y = false; return x ^ y"));
    }
    
    public void testXorTypedLHS() {
        expectScriptThrows(ClassCastException.class, () -> {
            exec("float x = (float)4; def y = (byte)1; return x ^ y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("double x = (double)4; def y = (byte)1; return x ^ y");
        });
        assertEquals(5, exec("def x = (byte)4; def y = (byte)1; return x ^ y"));
        assertEquals(5, exec("def x = (short)4; def y = (byte)1; return x ^ y"));
        assertEquals(5, exec("def x = (char)4; def y = (byte)1; return x ^ y"));
        assertEquals(5, exec("def x = (int)4; def y = (byte)1; return x ^ y"));
        assertEquals(5L, exec("def x = (long)4; def y = (byte)1; return x ^ y"));

        assertEquals(5, exec("def x = (byte)4; def y = (short)1; return x ^ y"));
        assertEquals(5, exec("def x = (short)4; def y = (short)1; return x ^ y"));
        assertEquals(5, exec("def x = (char)4; def y = (short)1; return x ^ y"));
        assertEquals(5, exec("def x = (int)4; def y = (short)1; return x ^ y"));
        assertEquals(5L, exec("def x = (long)4; def y = (short)1; return x ^ y"));

        assertEquals(5, exec("def x = (byte)4; def y = (char)1; return x ^ y"));
        assertEquals(5, exec("def x = (short)4; def y = (char)1; return x ^ y"));
        assertEquals(5, exec("def x = (char)4; def y = (char)1; return x ^ y"));
        assertEquals(5, exec("def x = (int)4; def y = (char)1; return x ^ y"));
        assertEquals(5L, exec("def x = (long)4; def y = (char)1; return x ^ y"));

        assertEquals(5, exec("def x = (byte)4; def y = (int)1; return x ^ y"));
        assertEquals(5, exec("def x = (short)4; def y = (int)1; return x ^ y"));
        assertEquals(5, exec("def x = (char)4; def y = (int)1; return x ^ y"));
        assertEquals(5, exec("def x = (int)4; def y = (int)1; return x ^ y"));
        assertEquals(5L, exec("def x = (long)4; def y = (int)1; return x ^ y"));

        assertEquals(5L, exec("def x = (byte)4; def y = (long)1; return x ^ y"));
        assertEquals(5L, exec("def x = (short)4; def y = (long)1; return x ^ y"));
        assertEquals(5L, exec("def x = (char)4; def y = (long)1; return x ^ y"));
        assertEquals(5L, exec("def x = (int)4; def y = (long)1; return x ^ y"));
        assertEquals(5L, exec("def x = (long)4; def y = (long)1; return x ^ y"));

        assertEquals(5, exec("def x = (byte)4; def y = (byte)1; return x ^ y"));
        assertEquals(5, exec("def x = (short)4; def y = (short)1; return x ^ y"));
        assertEquals(5, exec("def x = (char)4; def y = (char)1; return x ^ y"));
        assertEquals(5, exec("def x = (int)4; def y = (int)1; return x ^ y"));
        assertEquals(5L, exec("def x = (long)4; def y = (long)1; return x ^ y"));
        
        assertEquals(false, exec("def x = true;  def y = true; return x ^ y"));
        assertEquals(true,  exec("def x = true;  def y = false; return x ^ y"));
        assertEquals(true,  exec("def x = false; def y = true; return x ^ y"));
        assertEquals(false, exec("def x = false; def y = false; return x ^ y"));
    }
    
    public void testXorTypedRHS() {
        expectScriptThrows(ClassCastException.class, () -> {
            exec("def x = (float)4; byte y = (byte)1; return x ^ y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("def x = (double)4; byte y = (byte)1; return x ^ y");
        });
        assertEquals(5, exec("def x = (byte)4; byte y = (byte)1; return x ^ y"));
        assertEquals(5, exec("def x = (short)4; byte y = (byte)1; return x ^ y"));
        assertEquals(5, exec("def x = (char)4; byte y = (byte)1; return x ^ y"));
        assertEquals(5, exec("def x = (int)4; byte y = (byte)1; return x ^ y"));
        assertEquals(5L, exec("def x = (long)4; byte y = (byte)1; return x ^ y"));

        assertEquals(5, exec("def x = (byte)4; short y = (short)1; return x ^ y"));
        assertEquals(5, exec("def x = (short)4; short y = (short)1; return x ^ y"));
        assertEquals(5, exec("def x = (char)4; short y = (short)1; return x ^ y"));
        assertEquals(5, exec("def x = (int)4; short y = (short)1; return x ^ y"));
        assertEquals(5L, exec("def x = (long)4; short y = (short)1; return x ^ y"));

        assertEquals(5, exec("def x = (byte)4; char y = (char)1; return x ^ y"));
        assertEquals(5, exec("def x = (short)4; char y = (char)1; return x ^ y"));
        assertEquals(5, exec("def x = (char)4; char y = (char)1; return x ^ y"));
        assertEquals(5, exec("def x = (int)4; char y = (char)1; return x ^ y"));
        assertEquals(5L, exec("def x = (long)4; char y = (char)1; return x ^ y"));

        assertEquals(5, exec("def x = (byte)4; int y = (int)1; return x ^ y"));
        assertEquals(5, exec("def x = (short)4; int y = (int)1; return x ^ y"));
        assertEquals(5, exec("def x = (char)4; int y = (int)1; return x ^ y"));
        assertEquals(5, exec("def x = (int)4; int y = (int)1; return x ^ y"));
        assertEquals(5L, exec("def x = (long)4; int y = (int)1; return x ^ y"));

        assertEquals(5L, exec("def x = (byte)4; long y = (long)1; return x ^ y"));
        assertEquals(5L, exec("def x = (short)4; long y = (long)1; return x ^ y"));
        assertEquals(5L, exec("def x = (char)4; long y = (long)1; return x ^ y"));
        assertEquals(5L, exec("def x = (int)4; long y = (long)1; return x ^ y"));
        assertEquals(5L, exec("def x = (long)4; long y = (long)1; return x ^ y"));

        assertEquals(5, exec("def x = (byte)4; byte y = (byte)1; return x ^ y"));
        assertEquals(5, exec("def x = (short)4; short y = (short)1; return x ^ y"));
        assertEquals(5, exec("def x = (char)4; char y = (char)1; return x ^ y"));
        assertEquals(5, exec("def x = (int)4; int y = (int)1; return x ^ y"));
        assertEquals(5L, exec("def x = (long)4; long y = (long)1; return x ^ y"));
        
        assertEquals(false, exec("def x = true;  boolean y = true; return x ^ y"));
        assertEquals(true,  exec("def x = true;  boolean y = false; return x ^ y"));
        assertEquals(true,  exec("def x = false; boolean y = true; return x ^ y"));
        assertEquals(false, exec("def x = false; boolean y = false; return x ^ y"));
    }

    public void testOr() {
        expectScriptThrows(ClassCastException.class, () -> {
            exec("def x = (float)4; def y = (byte)1; return x | y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("def x = (double)4; def y = (byte)1; return x | y");
        });
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
        
        assertEquals(true,  exec("def x = true;  def y = true; return x | y"));
        assertEquals(true,  exec("def x = true;  def y = false; return x | y"));
        assertEquals(true,  exec("def x = false; def y = true; return x | y"));
        assertEquals(false, exec("def x = false; def y = false; return x | y"));
    }
    
    public void testOrTypedLHS() {
        expectScriptThrows(ClassCastException.class, () -> {
            exec("float x = (float)4; def y = (byte)1; return x | y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("double x = (double)4; def y = (byte)1; return x | y");
        });
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
        
        assertEquals(true,  exec("boolean x = true;  def y = true; return x | y"));
        assertEquals(true,  exec("boolean x = true;  def y = false; return x | y"));
        assertEquals(true,  exec("boolean x = false; def y = true; return x | y"));
        assertEquals(false, exec("boolean x = false; def y = false; return x | y"));
    }
    
    public void testOrTypedRHS() {
        expectScriptThrows(ClassCastException.class, () -> {
            exec("def x = (float)4; byte y = (byte)1; return x | y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("def x = (double)4; byte y = (byte)1; return x | y");
        });
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
        
        assertEquals(true,  exec("def x = true;  boolean y = true; return x | y"));
        assertEquals(true,  exec("def x = true;  boolean y = false; return x | y"));
        assertEquals(true,  exec("def x = false; boolean y = true; return x | y"));
        assertEquals(false, exec("def x = false; boolean y = false; return x | y"));
    }

    public void testEq() {
        assertEquals(true, exec("def x = (byte)7; def y = (int)7; return x == y"));
        assertEquals(true, exec("def x = (short)6; def y = (int)6; return x == y"));
        assertEquals(true, exec("def x = (char)5; def y = (int)5; return x == y"));
        assertEquals(true, exec("def x = (int)4; def y = (int)4; return x == y"));
        assertEquals(false, exec("def x = (long)5; def y = (int)3; return x == y"));
        assertEquals(false, exec("def x = (float)6; def y = (int)2; return x == y"));
        assertEquals(false, exec("def x = (double)7; def y = (int)1; return x == y"));

        assertEquals(true, exec("def x = (byte)7; def y = (double)7; return x == y"));
        assertEquals(true, exec("def x = (short)6; def y = (double)6; return x == y"));
        assertEquals(true, exec("def x = (char)5; def y = (double)5; return x == y"));
        assertEquals(true, exec("def x = (int)4; def y = (double)4; return x == y"));
        assertEquals(false, exec("def x = (long)5; def y = (double)3; return x == y"));
        assertEquals(false, exec("def x = (float)6; def y = (double)2; return x == y"));
        assertEquals(false, exec("def x = (double)7; def y = (double)1; return x == y"));
        
        assertEquals(false, exec("def x = false; def y = true; return x == y"));
        assertEquals(false, exec("def x = true; def y = false; return x == y"));
        assertEquals(false, exec("def x = true; def y = null; return x == y"));
        assertEquals(false, exec("def x = null; def y = true; return x == y"));
        assertEquals(true, exec("def x = true; def y = true; return x == y"));
        assertEquals(true, exec("def x = false; def y = false; return x == y"));

        assertEquals(true, exec("def x = new HashMap(); def y = new HashMap(); return x == y"));
        assertEquals(false, exec("def x = new HashMap(); x.put(3, 3); def y = new HashMap(); return x == y"));
        assertEquals(true, exec("def x = new HashMap(); x.put(3, 3); def y = new HashMap(); y.put(3, 3); return x == y"));
        assertEquals(true, exec("def x = new HashMap(); def y = x; x.put(3, 3); y.put(3, 3); return x == y"));
    }
    
    public void testEqTypedLHS() {
        assertEquals(true, exec("byte x = (byte)7; def y = (int)7; return x == y"));
        assertEquals(true, exec("short x = (short)6; def y = (int)6; return x == y"));
        assertEquals(true, exec("char x = (char)5; def y = (int)5; return x == y"));
        assertEquals(true, exec("int x = (int)4; def y = (int)4; return x == y"));
        assertEquals(false, exec("long x = (long)5; def y = (int)3; return x == y"));
        assertEquals(false, exec("float x = (float)6; def y = (int)2; return x == y"));
        assertEquals(false, exec("double x = (double)7; def y = (int)1; return x == y"));

        assertEquals(true, exec("byte x = (byte)7; def y = (double)7; return x == y"));
        assertEquals(true, exec("short x = (short)6; def y = (double)6; return x == y"));
        assertEquals(true, exec("char x = (char)5; def y = (double)5; return x == y"));
        assertEquals(true, exec("int x = (int)4; def y = (double)4; return x == y"));
        assertEquals(false, exec("long x = (long)5; def y = (double)3; return x == y"));
        assertEquals(false, exec("float x = (float)6; def y = (double)2; return x == y"));
        assertEquals(false, exec("double x = (double)7; def y = (double)1; return x == y"));
        
        assertEquals(false, exec("boolean x = false; def y = true; return x == y"));
        assertEquals(false, exec("boolean x = true; def y = false; return x == y"));
        assertEquals(false, exec("boolean x = true; def y = null; return x == y"));
        assertEquals(true, exec("boolean x = true; def y = true; return x == y"));
        assertEquals(true, exec("boolean x = false; def y = false; return x == y"));

        assertEquals(true, exec("Map x = new HashMap(); def y = new HashMap(); return x == y"));
        assertEquals(false, exec("Map x = new HashMap(); x.put(3, 3); def y = new HashMap(); return x == y"));
        assertEquals(true, exec("Map x = new HashMap(); x.put(3, 3); def y = new HashMap(); y.put(3, 3); return x == y"));
        assertEquals(true, exec("Map x = new HashMap(); def y = x; x.put(3, 3); y.put(3, 3); return x == y"));
    }
    
    public void testEqTypedRHS() {
        assertEquals(true, exec("def x = (byte)7; int y = (int)7; return x == y"));
        assertEquals(true, exec("def x = (short)6; int y = (int)6; return x == y"));
        assertEquals(true, exec("def x = (char)5; int y = (int)5; return x == y"));
        assertEquals(true, exec("def x = (int)4; int y = (int)4; return x == y"));
        assertEquals(false, exec("def x = (long)5; int y = (int)3; return x == y"));
        assertEquals(false, exec("def x = (float)6; int y = (int)2; return x == y"));
        assertEquals(false, exec("def x = (double)7; int y = (int)1; return x == y"));

        assertEquals(true, exec("def x = (byte)7; double y = (double)7; return x == y"));
        assertEquals(true, exec("def x = (short)6; double y = (double)6; return x == y"));
        assertEquals(true, exec("def x = (char)5; double y = (double)5; return x == y"));
        assertEquals(true, exec("def x = (int)4; double y = (double)4; return x == y"));
        assertEquals(false, exec("def x = (long)5; double y = (double)3; return x == y"));
        assertEquals(false, exec("def x = (float)6; double y = (double)2; return x == y"));
        assertEquals(false, exec("def x = (double)7; double y = (double)1; return x == y"));
        
        assertEquals(false, exec("def x = false; boolean y = true; return x == y"));
        assertEquals(false, exec("def x = true; boolean y = false; return x == y"));
        assertEquals(false, exec("def x = null; boolean y = true; return x == y"));
        assertEquals(true, exec("def x = true; boolean y = true; return x == y"));
        assertEquals(true, exec("def x = false; boolean y = false; return x == y"));

        assertEquals(true, exec("def x = new HashMap(); Map y = new HashMap(); return x == y"));
        assertEquals(false, exec("def x = new HashMap(); x.put(3, 3); Map y = new HashMap(); return x == y"));
        assertEquals(true, exec("def x = new HashMap(); x.put(3, 3); Map y = new HashMap(); y.put(3, 3); return x == y"));
        assertEquals(true, exec("def x = new HashMap(); Map y = x; x.put(3, 3); y.put(3, 3); return x == y"));
    }

    public void testEqr() {
        assertEquals(false, exec("def x = (byte)7; def y = (int)7; return x === y"));
        assertEquals(false, exec("def x = (short)6; def y = (int)6; return x === y"));
        assertEquals(false, exec("def x = (char)5; def y = (int)5; return x === y"));
        assertEquals(true, exec("def x = (int)4; def y = (int)4; return x === y"));
        assertEquals(false, exec("def x = (long)5; def y = (int)3; return x === y"));
        assertEquals(false, exec("def x = (float)6; def y = (int)2; return x === y"));
        assertEquals(false, exec("def x = (double)7; def y = (int)1; return x === y"));
        assertEquals(false, exec("def x = false; def y = true; return x === y"));

        assertEquals(false, exec("def x = new HashMap(); def y = new HashMap(); return x === y"));
        assertEquals(false, exec("def x = new HashMap(); x.put(3, 3); def y = new HashMap(); return x === y"));
        assertEquals(false, exec("def x = new HashMap(); x.put(3, 3); def y = new HashMap(); y.put(3, 3); return x === y"));
        assertEquals(true, exec("def x = new HashMap(); def y = x; x.put(3, 3); y.put(3, 3); return x === y"));
    }

    public void testNe() {
        assertEquals(false, exec("def x = (byte)7; def y = (int)7; return x != y"));
        assertEquals(false, exec("def x = (short)6; def y = (int)6; return x != y"));
        assertEquals(false, exec("def x = (char)5; def y = (int)5; return x != y"));
        assertEquals(false, exec("def x = (int)4; def y = (int)4; return x != y"));
        assertEquals(true, exec("def x = (long)5; def y = (int)3; return x != y"));
        assertEquals(true, exec("def x = (float)6; def y = (int)2; return x != y"));
        assertEquals(true, exec("def x = (double)7; def y = (int)1; return x != y"));

        assertEquals(false, exec("def x = (byte)7; def y = (double)7; return x != y"));
        assertEquals(false, exec("def x = (short)6; def y = (double)6; return x != y"));
        assertEquals(false, exec("def x = (char)5; def y = (double)5; return x != y"));
        assertEquals(false, exec("def x = (int)4; def y = (double)4; return x != y"));
        assertEquals(true, exec("def x = (long)5; def y = (double)3; return x != y"));
        assertEquals(true, exec("def x = (float)6; def y = (double)2; return x != y"));
        assertEquals(true, exec("def x = (double)7; def y = (double)1; return x != y"));

        assertEquals(false, exec("def x = new HashMap(); def y = new HashMap(); return x != y"));
        assertEquals(true, exec("def x = new HashMap(); x.put(3, 3); def y = new HashMap(); return x != y"));
        assertEquals(false, exec("def x = new HashMap(); x.put(3, 3); def y = new HashMap(); y.put(3, 3); return x != y"));
        assertEquals(false, exec("def x = new HashMap(); def y = x; x.put(3, 3); y.put(3, 3); return x != y"));
        
        assertEquals(false,  exec("def x = true;  def y = true; return x != y"));
        assertEquals(true,   exec("def x = true;  def y = false; return x != y"));
        assertEquals(true,   exec("def x = false; def y = true; return x != y"));
        assertEquals(false,  exec("def x = false; def y = false; return x != y"));
    }
    
    public void testNeTypedLHS() {
        assertEquals(false, exec("byte x = (byte)7; def y = (int)7; return x != y"));
        assertEquals(false, exec("short x = (short)6; def y = (int)6; return x != y"));
        assertEquals(false, exec("char x = (char)5; def y = (int)5; return x != y"));
        assertEquals(false, exec("int x = (int)4; def y = (int)4; return x != y"));
        assertEquals(true, exec("long x = (long)5; def y = (int)3; return x != y"));
        assertEquals(true, exec("float x = (float)6; def y = (int)2; return x != y"));
        assertEquals(true, exec("double x = (double)7; def y = (int)1; return x != y"));

        assertEquals(false, exec("byte x = (byte)7; def y = (double)7; return x != y"));
        assertEquals(false, exec("short x = (short)6; def y = (double)6; return x != y"));
        assertEquals(false, exec("char x = (char)5; def y = (double)5; return x != y"));
        assertEquals(false, exec("int x = (int)4; def y = (double)4; return x != y"));
        assertEquals(true, exec("long x = (long)5; def y = (double)3; return x != y"));
        assertEquals(true, exec("float x = (float)6; def y = (double)2; return x != y"));
        assertEquals(true, exec("double x = (double)7; def y = (double)1; return x != y"));

        assertEquals(false, exec("Map x = new HashMap(); def y = new HashMap(); return x != y"));
        assertEquals(true, exec("Map x = new HashMap(); x.put(3, 3); def y = new HashMap(); return x != y"));
        assertEquals(false, exec("Map x = new HashMap(); x.put(3, 3); def y = new HashMap(); y.put(3, 3); return x != y"));
        assertEquals(false, exec("Map x = new HashMap(); def y = x; x.put(3, 3); y.put(3, 3); return x != y"));
        
        assertEquals(false,  exec("boolean x = true;  def y = true; return x != y"));
        assertEquals(true,   exec("boolean x = true;  def y = false; return x != y"));
        assertEquals(true,   exec("boolean x = false; def y = true; return x != y"));
        assertEquals(false,  exec("boolean x = false; def y = false; return x != y"));
    }
    
    public void testNeTypedRHS() {
        assertEquals(false, exec("def x = (byte)7; int y = (int)7; return x != y"));
        assertEquals(false, exec("def x = (short)6; int y = (int)6; return x != y"));
        assertEquals(false, exec("def x = (char)5; int y = (int)5; return x != y"));
        assertEquals(false, exec("def x = (int)4; int y = (int)4; return x != y"));
        assertEquals(true, exec("def x = (long)5; int y = (int)3; return x != y"));
        assertEquals(true, exec("def x = (float)6; int y = (int)2; return x != y"));
        assertEquals(true, exec("def x = (double)7; int y = (int)1; return x != y"));

        assertEquals(false, exec("def x = (byte)7; double y = (double)7; return x != y"));
        assertEquals(false, exec("def x = (short)6; double y = (double)6; return x != y"));
        assertEquals(false, exec("def x = (char)5; double y = (double)5; return x != y"));
        assertEquals(false, exec("def x = (int)4; double y = (double)4; return x != y"));
        assertEquals(true, exec("def x = (long)5; double y = (double)3; return x != y"));
        assertEquals(true, exec("def x = (float)6; double y = (double)2; return x != y"));
        assertEquals(true, exec("def x = (double)7; double y = (double)1; return x != y"));

        assertEquals(false, exec("def x = new HashMap(); Map y = new HashMap(); return x != y"));
        assertEquals(true, exec("def x = new HashMap(); x.put(3, 3); Map y = new HashMap(); return x != y"));
        assertEquals(false, exec("def x = new HashMap(); x.put(3, 3); Map y = new HashMap(); y.put(3, 3); return x != y"));
        assertEquals(false, exec("def x = new HashMap(); Map y = x; x.put(3, 3); y.put(3, 3); return x != y"));
        
        assertEquals(false,  exec("def x = true;  boolean y = true; return x != y"));
        assertEquals(true,   exec("def x = true;  boolean y = false; return x != y"));
        assertEquals(true,   exec("def x = false; boolean y = true; return x != y"));
        assertEquals(false,  exec("def x = false; boolean y = false; return x != y"));
    }

    public void testNer() {
        assertEquals(true, exec("def x = (byte)7; def y = (int)7; return x !== y"));
        assertEquals(true, exec("def x = (short)6; def y = (int)6; return x !== y"));
        assertEquals(true, exec("def x = (char)5; def y = (int)5; return x !== y"));
        assertEquals(false, exec("def x = (int)4; def y = (int)4; return x !== y"));
        assertEquals(true, exec("def x = (long)5; def y = (int)3; return x !== y"));
        assertEquals(true, exec("def x = (float)6; def y = (int)2; return x !== y"));
        assertEquals(true, exec("def x = (double)7; def y = (int)1; return x !== y"));

        assertEquals(true, exec("def x = new HashMap(); def y = new HashMap(); return x !== y"));
        assertEquals(true, exec("def x = new HashMap(); x.put(3, 3); def y = new HashMap(); return x !== y"));
        assertEquals(true, exec("def x = new HashMap(); x.put(3, 3); def y = new HashMap(); y.put(3, 3); return x !== y"));
        assertEquals(false, exec("def x = new HashMap(); def y = x; x.put(3, 3); y.put(3, 3); return x !== y"));
    }

    public void testLt() {
        assertEquals(true, exec("def x = (byte)1; def y = (int)7; return x < y"));
        assertEquals(true, exec("def x = (short)2; def y = (int)6; return x < y"));
        assertEquals(true, exec("def x = (char)3; def y = (int)5; return x < y"));
        assertEquals(false, exec("def x = (int)4; def y = (int)4; return x < y"));
        assertEquals(false, exec("def x = (long)5; def y = (int)3; return x < y"));
        assertEquals(false, exec("def x = (float)6; def y = (int)2; return x < y"));
        assertEquals(false, exec("def x = (double)7; def y = (int)1; return x < y"));

        assertEquals(true, exec("def x = (byte)1; def y = (double)7; return x < y"));
        assertEquals(true, exec("def x = (short)2; def y = (double)6; return x < y"));
        assertEquals(true, exec("def x = (char)3; def y = (double)5; return x < y"));
        assertEquals(false, exec("def x = (int)4; def y = (double)4; return x < y"));
        assertEquals(false, exec("def x = (long)5; def y = (double)3; return x < y"));
        assertEquals(false, exec("def x = (float)6; def y = (double)2; return x < y"));
        assertEquals(false, exec("def x = (double)7; def y = (double)1; return x < y"));
    }
    
    public void testLtTypedLHS() {
        assertEquals(true, exec("byte x = (byte)1; def y = (int)7; return x < y"));
        assertEquals(true, exec("short x = (short)2; def y = (int)6; return x < y"));
        assertEquals(true, exec("char x = (char)3; def y = (int)5; return x < y"));
        assertEquals(false, exec("int x = (int)4; def y = (int)4; return x < y"));
        assertEquals(false, exec("long x = (long)5; def y = (int)3; return x < y"));
        assertEquals(false, exec("float x = (float)6; def y = (int)2; return x < y"));
        assertEquals(false, exec("double x = (double)7; def y = (int)1; return x < y"));

        assertEquals(true, exec("byte x = (byte)1; def y = (double)7; return x < y"));
        assertEquals(true, exec("short x = (short)2; def y = (double)6; return x < y"));
        assertEquals(true, exec("char x = (char)3; def y = (double)5; return x < y"));
        assertEquals(false, exec("int x = (int)4; def y = (double)4; return x < y"));
        assertEquals(false, exec("long x = (long)5; def y = (double)3; return x < y"));
        assertEquals(false, exec("float x = (float)6; def y = (double)2; return x < y"));
        assertEquals(false, exec("double x = (double)7; def y = (double)1; return x < y"));
    }
    
    public void testLtTypedRHS() {
        assertEquals(true, exec("def x = (byte)1; int y = (int)7; return x < y"));
        assertEquals(true, exec("def x = (short)2; int y = (int)6; return x < y"));
        assertEquals(true, exec("def x = (char)3; int y = (int)5; return x < y"));
        assertEquals(false, exec("def x = (int)4; int y = (int)4; return x < y"));
        assertEquals(false, exec("def x = (long)5; int y = (int)3; return x < y"));
        assertEquals(false, exec("def x = (float)6; int y = (int)2; return x < y"));
        assertEquals(false, exec("def x = (double)7; int y = (int)1; return x < y"));

        assertEquals(true, exec("def x = (byte)1; double y = (double)7; return x < y"));
        assertEquals(true, exec("def x = (short)2; double y = (double)6; return x < y"));
        assertEquals(true, exec("def x = (char)3; double y = (double)5; return x < y"));
        assertEquals(false, exec("def x = (int)4; double y = (double)4; return x < y"));
        assertEquals(false, exec("def x = (long)5; double y = (double)3; return x < y"));
        assertEquals(false, exec("def x = (float)6; double y = (double)2; return x < y"));
        assertEquals(false, exec("def x = (double)7; double y = (double)1; return x < y"));
    }

    public void testLte() {
        assertEquals(true, exec("def x = (byte)1; def y = (int)7; return x <= y"));
        assertEquals(true, exec("def x = (short)2; def y = (int)6; return x <= y"));
        assertEquals(true, exec("def x = (char)3; def y = (int)5; return x <= y"));
        assertEquals(true, exec("def x = (int)4; def y = (int)4; return x <= y"));
        assertEquals(false, exec("def x = (long)5; def y = (int)3; return x <= y"));
        assertEquals(false, exec("def x = (float)6; def y = (int)2; return x <= y"));
        assertEquals(false, exec("def x = (double)7; def y = (int)1; return x <= y"));

        assertEquals(true, exec("def x = (byte)1; def y = (double)7; return x <= y"));
        assertEquals(true, exec("def x = (short)2; def y = (double)6; return x <= y"));
        assertEquals(true, exec("def x = (char)3; def y = (double)5; return x <= y"));
        assertEquals(true, exec("def x = (int)4; def y = (double)4; return x <= y"));
        assertEquals(false, exec("def x = (long)5; def y = (double)3; return x <= y"));
        assertEquals(false, exec("def x = (float)6; def y = (double)2; return x <= y"));
        assertEquals(false, exec("def x = (double)7; def y = (double)1; return x <= y"));
    }
    
    public void testLteTypedLHS() {
        assertEquals(true, exec("byte x = (byte)1; def y = (int)7; return x <= y"));
        assertEquals(true, exec("short x = (short)2; def y = (int)6; return x <= y"));
        assertEquals(true, exec("char x = (char)3; def y = (int)5; return x <= y"));
        assertEquals(true, exec("int x = (int)4; def y = (int)4; return x <= y"));
        assertEquals(false, exec("long x = (long)5; def y = (int)3; return x <= y"));
        assertEquals(false, exec("float x = (float)6; def y = (int)2; return x <= y"));
        assertEquals(false, exec("double x = (double)7; def y = (int)1; return x <= y"));

        assertEquals(true, exec("byte x = (byte)1; def y = (double)7; return x <= y"));
        assertEquals(true, exec("short x = (short)2; def y = (double)6; return x <= y"));
        assertEquals(true, exec("char x = (char)3; def y = (double)5; return x <= y"));
        assertEquals(true, exec("int x = (int)4; def y = (double)4; return x <= y"));
        assertEquals(false, exec("long x = (long)5; def y = (double)3; return x <= y"));
        assertEquals(false, exec("float x = (float)6; def y = (double)2; return x <= y"));
        assertEquals(false, exec("double x = (double)7; def y = (double)1; return x <= y"));
    }
    
    public void testLteTypedRHS() {
        assertEquals(true, exec("def x = (byte)1; int y = (int)7; return x <= y"));
        assertEquals(true, exec("def x = (short)2; int y = (int)6; return x <= y"));
        assertEquals(true, exec("def x = (char)3; int y = (int)5; return x <= y"));
        assertEquals(true, exec("def x = (int)4; int y = (int)4; return x <= y"));
        assertEquals(false, exec("def x = (long)5; int y = (int)3; return x <= y"));
        assertEquals(false, exec("def x = (float)6; int y = (int)2; return x <= y"));
        assertEquals(false, exec("def x = (double)7; int y = (int)1; return x <= y"));

        assertEquals(true, exec("def x = (byte)1; double y = (double)7; return x <= y"));
        assertEquals(true, exec("def x = (short)2; double y = (double)6; return x <= y"));
        assertEquals(true, exec("def x = (char)3; double y = (double)5; return x <= y"));
        assertEquals(true, exec("def x = (int)4; double y = (double)4; return x <= y"));
        assertEquals(false, exec("def x = (long)5; double y = (double)3; return x <= y"));
        assertEquals(false, exec("def x = (float)6; double y = (double)2; return x <= y"));
        assertEquals(false, exec("def x = (double)7; double y = (double)1; return x <= y"));
    }

    public void testGt() {
        assertEquals(false, exec("def x = (byte)1; def y = (int)7; return x > y"));
        assertEquals(false, exec("def x = (short)2; def y = (int)6; return x > y"));
        assertEquals(false, exec("def x = (char)3; def y = (int)5; return x > y"));
        assertEquals(false, exec("def x = (int)4; def y = (int)4; return x > y"));
        assertEquals(true, exec("def x = (long)5; def y = (int)3; return x > y"));
        assertEquals(true, exec("def x = (float)6; def y = (int)2; return x > y"));
        assertEquals(true, exec("def x = (double)7; def y = (int)1; return x > y"));

        assertEquals(false, exec("def x = (byte)1; def y = (double)7; return x > y"));
        assertEquals(false, exec("def x = (short)2; def y = (double)6; return x > y"));
        assertEquals(false, exec("def x = (char)3; def y = (double)5; return x > y"));
        assertEquals(false, exec("def x = (int)4; def y = (double)4; return x > y"));
        assertEquals(true, exec("def x = (long)5; def y = (double)3; return x > y"));
        assertEquals(true, exec("def x = (float)6; def y = (double)2; return x > y"));
        assertEquals(true, exec("def x = (double)7; def y = (double)1; return x > y"));
    }
    
    public void testGtTypedLHS() {
        assertEquals(false, exec("byte x = (byte)1; def y = (int)7; return x > y"));
        assertEquals(false, exec("short x = (short)2; def y = (int)6; return x > y"));
        assertEquals(false, exec("char x = (char)3; def y = (int)5; return x > y"));
        assertEquals(false, exec("int x = (int)4; def y = (int)4; return x > y"));
        assertEquals(true, exec("long x = (long)5; def y = (int)3; return x > y"));
        assertEquals(true, exec("float x = (float)6; def y = (int)2; return x > y"));
        assertEquals(true, exec("double x = (double)7; def y = (int)1; return x > y"));

        assertEquals(false, exec("byte x = (byte)1; def y = (double)7; return x > y"));
        assertEquals(false, exec("short x = (short)2; def y = (double)6; return x > y"));
        assertEquals(false, exec("char x = (char)3; def y = (double)5; return x > y"));
        assertEquals(false, exec("int x = (int)4; def y = (double)4; return x > y"));
        assertEquals(true, exec("long x = (long)5; def y = (double)3; return x > y"));
        assertEquals(true, exec("float x = (float)6; def y = (double)2; return x > y"));
        assertEquals(true, exec("double x = (double)7; def y = (double)1; return x > y"));
    }
    
    public void testGtTypedRHS() {
        assertEquals(false, exec("def x = (byte)1; int y = (int)7; return x > y"));
        assertEquals(false, exec("def x = (short)2; int y = (int)6; return x > y"));
        assertEquals(false, exec("def x = (char)3; int y = (int)5; return x > y"));
        assertEquals(false, exec("def x = (int)4; int y = (int)4; return x > y"));
        assertEquals(true, exec("def x = (long)5; int y = (int)3; return x > y"));
        assertEquals(true, exec("def x = (float)6; int y = (int)2; return x > y"));
        assertEquals(true, exec("def x = (double)7; int y = (int)1; return x > y"));

        assertEquals(false, exec("def x = (byte)1; double y = (double)7; return x > y"));
        assertEquals(false, exec("def x = (short)2; double y = (double)6; return x > y"));
        assertEquals(false, exec("def x = (char)3; double y = (double)5; return x > y"));
        assertEquals(false, exec("def x = (int)4; double y = (double)4; return x > y"));
        assertEquals(true, exec("def x = (long)5; double y = (double)3; return x > y"));
        assertEquals(true, exec("def x = (float)6; double y = (double)2; return x > y"));
        assertEquals(true, exec("def x = (double)7; double y = (double)1; return x > y"));
    }

    public void testGte() {
        assertEquals(false, exec("def x = (byte)1; def y = (int)7; return x >= y"));
        assertEquals(false, exec("def x = (short)2; def y = (int)6; return x >= y"));
        assertEquals(false, exec("def x = (char)3; def y = (int)5; return x >= y"));
        assertEquals(true, exec("def x = (int)4; def y = (int)4; return x >= y"));
        assertEquals(true, exec("def x = (long)5; def y = (int)3; return x >= y"));
        assertEquals(true, exec("def x = (float)6; def y = (int)2; return x >= y"));
        assertEquals(true, exec("def x = (double)7; def y = (int)1; return x >= y"));

        assertEquals(false, exec("def x = (byte)1; def y = (double)7; return x >= y"));
        assertEquals(false, exec("def x = (short)2; def y = (double)6; return x >= y"));
        assertEquals(false, exec("def x = (char)3; def y = (double)5; return x >= y"));
        assertEquals(true, exec("def x = (int)4; def y = (double)4; return x >= y"));
        assertEquals(true, exec("def x = (long)5; def y = (double)3; return x >= y"));
        assertEquals(true, exec("def x = (float)6; def y = (double)2; return x >= y"));
        assertEquals(true, exec("def x = (double)7; def y = (double)1; return x >= y"));
    }
    
    public void testGteTypedLHS() {
        assertEquals(false, exec("byte x = (byte)1; def y = (int)7; return x >= y"));
        assertEquals(false, exec("short x = (short)2; def y = (int)6; return x >= y"));
        assertEquals(false, exec("char x = (char)3; def y = (int)5; return x >= y"));
        assertEquals(true, exec("int x = (int)4; def y = (int)4; return x >= y"));
        assertEquals(true, exec("long x = (long)5; def y = (int)3; return x >= y"));
        assertEquals(true, exec("float x = (float)6; def y = (int)2; return x >= y"));
        assertEquals(true, exec("double x = (double)7; def y = (int)1; return x >= y"));

        assertEquals(false, exec("byte x = (byte)1; def y = (double)7; return x >= y"));
        assertEquals(false, exec("short x = (short)2; def y = (double)6; return x >= y"));
        assertEquals(false, exec("char x = (char)3; def y = (double)5; return x >= y"));
        assertEquals(true, exec("int x = (int)4; def y = (double)4; return x >= y"));
        assertEquals(true, exec("long x = (long)5; def y = (double)3; return x >= y"));
        assertEquals(true, exec("float x = (float)6; def y = (double)2; return x >= y"));
        assertEquals(true, exec("double x = (double)7; def y = (double)1; return x >= y"));
    }
    
    public void testGteTypedRHS() {
        assertEquals(false, exec("def x = (byte)1; int y = (int)7; return x >= y"));
        assertEquals(false, exec("def x = (short)2; int y = (int)6; return x >= y"));
        assertEquals(false, exec("def x = (char)3; int y = (int)5; return x >= y"));
        assertEquals(true, exec("def x = (int)4; int y = (int)4; return x >= y"));
        assertEquals(true, exec("def x = (long)5; int y = (int)3; return x >= y"));
        assertEquals(true, exec("def x = (float)6; int y = (int)2; return x >= y"));
        assertEquals(true, exec("def x = (double)7; int y = (int)1; return x >= y"));

        assertEquals(false, exec("def x = (byte)1; double y = (double)7; return x >= y"));
        assertEquals(false, exec("def x = (short)2; double y = (double)6; return x >= y"));
        assertEquals(false, exec("def x = (char)3; double y = (double)5; return x >= y"));
        assertEquals(true, exec("def x = (int)4; double y = (double)4; return x >= y"));
        assertEquals(true, exec("def x = (long)5; double y = (double)3; return x >= y"));
        assertEquals(true, exec("def x = (float)6; double y = (double)2; return x >= y"));
        assertEquals(true, exec("def x = (double)7; double y = (double)1; return x >= y"));
    }
}
