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

/** Tests for unary operators across different types */
public class UnaryTests extends ScriptTestCase {

    /** basic tests */
    public void testBasics() {
        assertEquals(false, exec("return !true;"));
        assertEquals(true, exec("boolean x = false; return !x;"));
        assertEquals(-2, exec("return ~1;"));
        assertEquals(-2, exec("byte x = 1; return ~x;"));
        assertEquals(1, exec("return +1;"));
        assertEquals(1.0, exec("double x = 1; return +x;"));
        assertEquals(-1, exec("return -1;"));
        assertEquals(-2, exec("short x = 2; return -x;"));
    }

    public void testNegationInt() throws Exception {
        assertEquals(-1, exec("return -1;"));
        assertEquals(1, exec("return -(-1);"));
        assertEquals(0, exec("return -0;"));
    }
    
    public void testPlus() {
        assertEquals(-1, exec("byte x = (byte)-1; return +x"));
        assertEquals(-1, exec("short x = (short)-1; return +x"));
        assertEquals(65535, exec("char x = (char)-1; return +x"));
        assertEquals(-1, exec("int x = -1; return +x"));
        assertEquals(-1L, exec("long x = -1L; return +x"));
        assertEquals(-1.0F, exec("float x = -1F; return +x"));
        assertEquals(-1.0, exec("double x = -1.0; return +x"));
    }
    
    public void testDefNot() {
        assertEquals(~1, exec("def x = (byte)1; return ~x"));
        assertEquals(~1, exec("def x = (short)1; return ~x"));
        assertEquals(~1, exec("def x = (char)1; return ~x"));
        assertEquals(~1, exec("def x = 1; return ~x"));
        assertEquals(~1L, exec("def x = 1L; return ~x"));
    }
    
    public void testDefNotTypedRet() {
        assertEquals((double)~1, exec("def x = (byte)1; double y = ~x; return y;"));
        assertEquals((float)~1, exec("def x = (short)1; float y = ~x; return y;"));
        assertEquals((long)~1, exec("def x = (char)1; long y = ~x; return y;"));
        assertEquals(~1, exec("def x = 1; int y = ~x; return y;"));
    }

    public void testDefNeg() {
        assertEquals(-1, exec("def x = (byte)1; return -x"));
        assertEquals(-1, exec("def x = (short)1; return -x"));
        assertEquals(-1, exec("def x = (char)1; return -x"));
        assertEquals(-1, exec("def x = 1; return -x"));
        assertEquals(-1L, exec("def x = 1L; return -x"));
        assertEquals(-1.0F, exec("def x = 1F; return -x"));
        assertEquals(-1.0, exec("def x = 1.0; return -x"));
    }
    
    public void testDefNegTypedRet() {
        assertEquals((double)-1, exec("def x = (byte)1; double y = -x; return y;"));
        assertEquals((float)-1, exec("def x = (short)1; float y = -x; return y;"));
        assertEquals((long)-1, exec("def x = (char)1; long y = -x; return y;"));
        assertEquals(-1, exec("def x = 1; int y = -x; return y;"));
    }
    
    public void testDefPlus() {
        assertEquals(-1, exec("def x = (byte)-1; return +x"));
        assertEquals(-1, exec("def x = (short)-1; return +x"));
        assertEquals(65535, exec("def x = (char)-1; return +x"));
        assertEquals(-1, exec("def x = -1; return +x"));
        assertEquals(-1L, exec("def x = -1L; return +x"));
        assertEquals(-1.0F, exec("def x = -1F; return +x"));
        assertEquals(-1.0D, exec("def x = -1.0; return +x"));
    }
    
    public void testDefPlusTypedRet() {
        assertEquals((double)-1, exec("def x = (byte)-1; double y = +x; return y;"));
        assertEquals((float)-1, exec("def x = (short)-1; float y = +x; return y;"));
        assertEquals((long)65535, exec("def x = (char)-1; long y = +x; return y;"));
        assertEquals(-1, exec("def x = -1; int y = +x; return y;"));
    }
}
