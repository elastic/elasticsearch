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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class NoSemiColonTests extends ScriptTestCase {

    public void testDeclarationStatement() {
        assertEquals((byte)2, exec("byte a = 2; return a"));
        assertEquals((short)2, exec("short a = 2; return a"));
        assertEquals((char)2, exec("char a = 2; return a"));
        assertEquals(2, exec("int a = 2; return a"));
        assertEquals(2L, exec("long a = 2; return a"));
        assertEquals(2F, exec("float a = 2; return a"));
        assertEquals(2.0, exec("double a = 2; return a"));
        assertEquals(false, exec("boolean a = false; return a"));
        assertEquals("string", exec("String a = \"string\"; return a"));
        assertEquals(HashMap.class, exec("Map a = new HashMap(); return a").getClass());

        assertEquals(byte[].class, exec("byte[] a = new byte[1]; return a").getClass());
        assertEquals(short[].class, exec("short[] a = new short[1]; return a").getClass());
        assertEquals(char[].class, exec("char[] a = new char[1]; return a").getClass());
        assertEquals(int[].class, exec("int[] a = new int[1]; return a").getClass());
        assertEquals(long[].class, exec("long[] a = new long[1]; return a").getClass());
        assertEquals(float[].class, exec("float[] a = new float[1]; return a").getClass());
        assertEquals(double[].class, exec("double[] a = new double[1]; return a").getClass());
        assertEquals(boolean[].class, exec("boolean[] a = new boolean[1]; return a").getClass());
        assertEquals(String[].class, exec("String[] a = new String[1]; return a").getClass());
        assertEquals(Map[].class, exec("Map[] a = new Map[1]; return a").getClass());

        assertEquals(byte[][].class, exec("byte[][] a = new byte[1][2]; return a").getClass());
        assertEquals(short[][][].class, exec("short[][][] a = new short[1][2][3]; return a").getClass());
        assertEquals(char[][][][].class, exec("char[][][][] a = new char[1][2][3][4]; return a").getClass());
        assertEquals(int[][][][][].class, exec("int[][][][][] a = new int[1][2][3][4][5]; return a").getClass());
        assertEquals(long[][].class, exec("long[][] a = new long[1][2]; return a").getClass());
        assertEquals(float[][][].class, exec("float[][][] a = new float[1][2][3]; return a").getClass());
        assertEquals(double[][][][].class, exec("double[][][][] a = new double[1][2][3][4]; return a").getClass());
        assertEquals(boolean[][][][][].class, exec("boolean[][][][][] a = new boolean[1][2][3][4][5]; return a").getClass());
        assertEquals(String[][].class, exec("String[][] a = new String[1][2]; return a").getClass());
        assertEquals(Map[][][].class, exec("Map[][][] a = new Map[1][2][3]; return a").getClass());
    }

    public void testExpression() {
        assertEquals(10, exec("10"));
        assertEquals(10, exec("5 + 5"));
        assertEquals(10, exec("5 + 5"));
        assertEquals(10, exec("params.param == 'yes' ? 10 : 5", Collections.singletonMap("param", "yes")));
    }

    @SuppressWarnings("rawtypes")
    public void testReturnStatement() {
        assertEquals(10, exec("return 10"));
        assertEquals(5, exec("int x = 5; return x"));
        assertEquals(4, exec("int[] x = new int[2]; x[1] = 4; return x[1]"));
        assertEquals(5, ((short[])exec("short[] s = new short[3]; s[1] = 5; return s"))[1]);
        assertEquals(10, ((Map)exec("Map s = new HashMap(); s.put(\"x\", 10); return s")).get("x"));
    }
}
