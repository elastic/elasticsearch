package org.elasticsearch.painless;

import java.util.Collections;

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

import java.util.HashMap;
import java.util.Map;

public class BasicStatementTests extends ScriptTestCase {

    public void testIfStatement() {
        assertEquals(1, exec("int x = 5; if (x == 5) return 1; return 0;"));
        assertEquals(0, exec("int x = 4; if (x == 5) return 1; else return 0;"));
        assertEquals(2, exec("int x = 4; if (x == 5) return 1; else if (x == 4) return 2; else return 0;"));
        assertEquals(1, exec("int x = 4; if (x == 5) return 1; else if (x == 4) return 1; else return 0;"));

        assertEquals(3, exec(
                "int x = 5;\n" +
                "if (x == 5) {\n" +
                "    int y = 2;\n" +
                "    \n" +
                "    if (y == 2) {\n" +
                "        x = 3;\n" +
                "    }\n" +
                "    \n" +
                "}\n" +
                "\n" +
                "return x;\n"));
    }

    public void testWhileStatement() {

        assertEquals("aaaaaa", exec("String c = \"a\"; int x; while (x < 5) { c += \"a\"; ++x; } return c;"));

        Object value = exec(
                " byte[][] b = new byte[5][5];       \n" +
                " byte x = 0, y;                     \n" +
                "                                    \n" +
                " while (x < 5) {                    \n" +
                "     y = 0;                         \n" +
                "                                    \n" +
                "     while (y < 5) {                \n" +
                "         b[x][y] = (byte)(x*y);     \n" +
                "         ++y;                       \n" +
                "     }                              \n" +
                "                                    \n" +
                "     ++x;                           \n" +
                " }                                  \n" +
                "                                    \n" +
                " return b;                          \n");

        byte[][] b = (byte[][])value;

        for (byte x = 0; x < 5; ++x) {
            for (byte y = 0; y < 5; ++y) {
                assertEquals(x*y, b[x][y]);
            }
        }
    }

    public void testDoWhileStatement() {
        assertEquals("aaaaaa", exec("String c = \"a\"; int x; do { c += \"a\"; ++x; } while (x < 5); return c;"));

        Object value = exec(
                " int[][] b = new int[5][5]; \n" +
                " int x = 0, y;                    \n" +
                "                                  \n" +
                " do {                             \n" +
                "     y = 0;                       \n" +
                "                                  \n" +
                "     do {                         \n" +
                "         b[x][y] = x*y;           \n" +
                "         ++y;                     \n" +
                "     } while (y < 5);             \n" +
                "                                  \n" +
                "     ++x;                         \n" +
                " } while (x < 5);                 \n" +
                "                                  \n" +
                " return b;                        \n");

        int[][] b = (int[][])value;

        for (byte x = 0; x < 5; ++x) {
            for (byte y = 0; y < 5; ++y) {
                assertEquals(x*y, b[x][y]);
            }
        }
    }

    public void testForStatement() {
        assertEquals("aaaaaa", exec("String c = \"a\"; for (int x = 0; x < 5; ++x) c += \"a\"; return c;"));

        Object value = exec(
                " int[][] b = new int[5][5];  \n" +
                " for (int x = 0; x < 5; ++x) {     \n" +
                "     for (int y = 0; y < 5; ++y) { \n" +
                "         b[x][y] = x*y;            \n" +
                "     }                             \n" +
                " }                                 \n" +
                "                                   \n" +
                " return b;                         \n");

        int[][] b = (int[][])value;

        for (byte x = 0; x < 5; ++x) {
            for (byte y = 0; y < 5; ++y) {
                assertEquals(x*y, b[x][y]);
            }
        }
    }

    public void testIterableForEachStatement() {
        assertEquals(6, exec("List l = new ArrayList(); l.add(1); l.add(2); l.add(3); int total = 0;" +
            " for (int x : l) total += x; return total"));
        assertEquals(6, exec("List l = new ArrayList(); l.add(1); l.add(2); l.add(3); int total = 0;" +
            " for (x in l) total += x; return total"));
        assertEquals("123", exec("List l = new ArrayList(); l.add('1'); l.add('2'); l.add('3'); String cat = '';" +
            " for (String x : l) cat += x; return cat"));
        assertEquals("123", exec("List l = new ArrayList(); l.add('1'); l.add('2'); l.add('3'); String cat = '';" +
            " for (x in l) cat += x; return cat"));
        assertEquals("1236", exec("Map m = new HashMap(); m.put('1', 1); m.put('2', 2); m.put('3', 3);" +
            " String cat = ''; int total = 0;" +
            " for (Map.Entry e : m.entrySet()) { cat += e.getKey(); total += e.getValue(); } return cat + total"));
        assertEquals("1236", exec("Map m = new HashMap(); m.put('1', 1); m.put('2', 2); m.put('3', 3);" +
                " String cat = ''; int total = 0;" +
                " for (e in m.entrySet()) { cat += e.getKey(); total += e.getValue(); } return cat + total"));
    }

    public void testIterableForEachStatementDef() {
        assertEquals(6, exec("def l = new ArrayList(); l.add(1); l.add(2); l.add(3); int total = 0;" +
            " for (int x : l) total += x; return total"));
        assertEquals(6, exec("def l = new ArrayList(); l.add(1); l.add(2); l.add(3); int total = 0;" +
            " for (x in l) total += x; return total"));
        assertEquals("123", exec("def l = new ArrayList(); l.add('1'); l.add('2'); l.add('3'); String cat = '';" +
            " for (String x : l) cat += x; return cat"));
        assertEquals("123", exec("def l = new ArrayList(); l.add('1'); l.add('2'); l.add('3'); String cat = '';" +
            " for (x in l) cat += x; return cat"));
        assertEquals("1236", exec("def m = new HashMap(); m.put('1', 1); m.put('2', 2); m.put('3', 3);" +
            " String cat = ''; int total = 0;" +
            " for (Map.Entry e : m.entrySet()) { cat += e.getKey(); total += e.getValue(); } return cat + total"));
        assertEquals("1236", exec("def m = new HashMap(); m.put('1', 1); m.put('2', 2); m.put('3', 3);" +
            " String cat = ''; int total = 0;" +
            " for (e in m.entrySet()) { cat += e.getKey(); total += e.getValue(); } return cat + total"));
    }

    public void testArrayForEachStatement() {
        assertEquals(6, exec("int[] a = new int[3]; a[0] = 1; a[1] = 2; a[2] = 3; int total = 0;" +
            " for (int x : a) total += x; return total"));
        assertEquals(6, exec("int[] a = new int[3]; a[0] = 1; a[1] = 2; a[2] = 3; int total = 0;" +
            " for (x in a) total += x; return total"));
        assertEquals("123", exec("String[] a = new String[3]; a[0] = '1'; a[1] = '2'; a[2] = '3'; def total = '';" +
            " for (String x : a) total += x; return total"));
        assertEquals("123", exec("String[] a = new String[3]; a[0] = '1'; a[1] = '2'; a[2] = '3'; def total = '';" +
            " for (x in a) total += x; return total"));
        assertEquals(6, exec("int[][] i = new int[3][1]; i[0][0] = 1; i[1][0] = 2; i[2][0] = 3; int total = 0;" +
            " for (int[] j : i) total += j[0]; return total"));
        assertEquals(6, exec("int[][] i = new int[3][1]; i[0][0] = 1; i[1][0] = 2; i[2][0] = 3; int total = 0;" +
            " for (j in i) total += j[0]; return total"));
    }

    public void testArrayForEachStatementDef() {
        assertEquals(6, exec("def a = new int[3]; a[0] = 1; a[1] = 2; a[2] = 3; int total = 0;" +
            " for (int x : a) total += x; return total"));
        assertEquals(6, exec("def a = new int[3]; a[0] = 1; a[1] = 2; a[2] = 3; int total = 0;" +
            " for (x in a) total += x; return total"));
        assertEquals("123", exec("def a = new String[3]; a[0] = '1'; a[1] = '2'; a[2] = '3'; def total = '';" +
            " for (String x : a) total += x; return total"));
        assertEquals("123", exec("def a = new String[3]; a[0] = '1'; a[1] = '2'; a[2] = '3'; def total = '';" +
            " for (x in a) total += x; return total"));
        assertEquals(6, exec("def i = new int[3][1]; i[0][0] = 1; i[1][0] = 2; i[2][0] = 3; int total = 0;" +
            " for (int[] j : i) total += j[0]; return total"));
        assertEquals(6, exec("def i = new int[3][1]; i[0][0] = 1; i[1][0] = 2; i[2][0] = 3; int total = 0;" +
            " for (j in i) total += j[0]; return total"));
    }

    public void testDeclarationStatement() {
        assertEquals((byte)2, exec("byte a = 2; return a;"));
        assertEquals((short)2, exec("short a = 2; return a;"));
        assertEquals((char)2, exec("char a = 2; return a;"));
        assertEquals(2, exec("int a = 2; return a;"));
        assertEquals(2L, exec("long a = 2; return a;"));
        assertEquals(2F, exec("float a = 2; return a;"));
        assertEquals(2.0, exec("double a = 2; return a;"));
        assertEquals(false, exec("boolean a = false; return a;"));
        assertEquals("string", exec("String a = \"string\"; return a;"));
        assertEquals(HashMap.class, exec("Map a = new HashMap(); return a;").getClass());

        assertEquals(byte[].class, exec("byte[] a = new byte[1]; return a;").getClass());
        assertEquals(short[].class, exec("short[] a = new short[1]; return a;").getClass());
        assertEquals(char[].class, exec("char[] a = new char[1]; return a;").getClass());
        assertEquals(int[].class, exec("int[] a = new int[1]; return a;").getClass());
        assertEquals(long[].class, exec("long[] a = new long[1]; return a;").getClass());
        assertEquals(float[].class, exec("float[] a = new float[1]; return a;").getClass());
        assertEquals(double[].class, exec("double[] a = new double[1]; return a;").getClass());
        assertEquals(boolean[].class, exec("boolean[] a = new boolean[1]; return a;").getClass());
        assertEquals(String[].class, exec("String[] a = new String[1]; return a;").getClass());
        assertEquals(Map[].class, exec("Map[] a = new Map[1]; return a;").getClass());

        assertEquals(byte[][].class, exec("byte[][] a = new byte[1][2]; return a;").getClass());
        assertEquals(short[][][].class, exec("short[][][] a = new short[1][2][3]; return a;").getClass());
        assertEquals(char[][][][].class, exec("char[][][][] a = new char[1][2][3][4]; return a;").getClass());
        assertEquals(int[][][][][].class, exec("int[][][][][] a = new int[1][2][3][4][5]; return a;").getClass());
        assertEquals(long[][].class, exec("long[][] a = new long[1][2]; return a;").getClass());
        assertEquals(float[][][].class, exec("float[][][] a = new float[1][2][3]; return a;").getClass());
        assertEquals(double[][][][].class, exec("double[][][][] a = new double[1][2][3][4]; return a;").getClass());
        assertEquals(boolean[][][][][].class, exec("boolean[][][][][] a = new boolean[1][2][3][4][5]; return a;").getClass());
        assertEquals(String[][].class, exec("String[][] a = new String[1][2]; return a;").getClass());
        assertEquals(Map[][][].class, exec("Map[][][] a = new Map[1][2][3]; return a;").getClass());
    }

    public void testContinueStatement() {
        assertEquals(9, exec("int x = 0, y = 0; while (x < 10) { ++x; if (x == 1) continue; ++y; } return y;"));
    }

    public void testBreakStatement() {
        assertEquals(4, exec("int x = 0, y = 0; while (x < 10) { ++x; if (x == 5) break; ++y; } return y;"));
    }

    @SuppressWarnings("rawtypes")
    public void testReturnStatement() {
        assertEquals(10, exec("return 10;"));
        assertEquals(5, exec("int x = 5; return x;"));
        assertEquals(4, exec("int[] x = new int[2]; x[1] = 4; return x[1];"));
        assertEquals(5, ((short[])exec("short[] s = new short[3]; s[1] = 5; return s;"))[1]);
        assertEquals(10, ((Map)exec("Map s = new HashMap(); s.put(\"x\", 10); return s;")).get("x"));
    }

    public void testLastInBlockDoesntNeedSemi() {
        // One statement in the block in case that is a special case
        assertEquals(10, exec("def i = 1; if (i == 1) {return 10}"));
        assertEquals(10, exec("def i = 1; if (i == 1) {return 10} else {return 12}"));
        // Two statements in the block, in case that is the general case
        assertEquals(10, exec("def i = 1; if (i == 1) {i = 2; return 10}"));
        assertEquals(10, exec("def i = 1; if (i == 1) {i = 2; return 10} else {return 12}"));
    }

    public void testArrayLoopWithoutCounter() {
       assertEquals(6L, exec("long sum = 0; long[] array = new long[] { 1, 2, 3 };" +
                              "for (int i = 0; i < array.length; i++) { sum += array[i] } return sum",
                              Collections.emptyMap(),
                              Collections.singletonMap(CompilerSettings.MAX_LOOP_COUNTER, "0"),
                              null, true
       ));
       assertEquals(6L, exec("long sum = 0; long[] array = new long[] { 1, 2, 3 };" +
                             "int i = 0; while (i < array.length) { sum += array[i++] } return sum",
                             Collections.emptyMap(),
                             Collections.singletonMap(CompilerSettings.MAX_LOOP_COUNTER, "0"),
                             null, true
       ));
       assertEquals(6L, exec("long sum = 0; long[] array = new long[] { 1, 2, 3 };" +
                             "int i = 0; do { sum += array[i++] } while (i < array.length); return sum",
                             Collections.emptyMap(),
                             Collections.singletonMap(CompilerSettings.MAX_LOOP_COUNTER, "0"),
                             null, true
       ));
    }
}
