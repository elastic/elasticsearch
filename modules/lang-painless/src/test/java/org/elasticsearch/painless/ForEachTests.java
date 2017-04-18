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

/**
 * Tests iterating in foreach loops. Much of it is testing the rather expansive casting rules we allow in this context.
 */
public class ForEachTests extends ScriptTestCase {
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

    public void testCastToByte() {
        castTestCase((byte) 1, "byte", true);
        castTestCase((byte) 1, "Byte", false);
    }

    public void testCastToShort() {
        castTestCase((short) 1, "short", true);
        castTestCase((short) 1, "Short", false);
    }

    public void testCastToChar() {
        castTestCase((char) 1, "char", true);
        castTestCase((char) 1, "Character", false);
    }

    public void testCastToInt() {
        castTestCase(1, "int", true);
        castTestCase(1, "Integer", false);
    }

    public void testCastToLong() {
        castTestCase((long) 1, "long", true);
        castTestCase((long) 1, "Long", false);
    }

    public void testCastToFloat() {
        castTestCase((float) 1, "float", true);
        castTestCase((float) 1, "Float", false);
    }

    public void testCastToDouble() {
        castTestCase((double) 1, "double", true);
        castTestCase((double) 1, "Double", false);
    }

    private void castTestCase(Object expected, String returnType, boolean canCastFromObject) {
        castTestCase(expected, returnType, "byte",   "(byte) 1");
        castTestCase(expected, returnType, "short",  "(short) 1");
        castTestCase(expected, returnType, "char",   "(char) 1");
        castTestCase(expected, returnType, "int",    "1");
        castTestCase(expected, returnType, "long",   "1L");
        castTestCase(expected, returnType, "float",  "1f");
        castTestCase(expected, returnType, "double", "1d");
        if (canCastFromObject) {
            castTestCase(expected, returnType, "Byte", "Byte.valueOf(1)");
            castTestCase(expected, returnType, "Short", "Short.valueOf(1)");
            castTestCase(expected, returnType, "Character", "Character.valueOf(1)");
            castTestCase(expected, returnType, "Integer", "Integer.valueOf(1)");
            castTestCase(expected, returnType, "Long", "Long.valueOf(1L)");
            castTestCase(expected, returnType, "Float", "Float.valueOf(1f)");
            castTestCase(expected, returnType, "Double", "Double.valueOf(1d)");
            if (false == returnType.equals("char")) {
                castTestCase(expected, returnType, "Number", "Double.valueOf(1d)");
                castTestCase(expected, returnType, "Object", "Double.valueOf(1d)");
            }
        }
    }

    private void castTestCase(Object expected, String returnType, String arrayType, String arrayValue) {
        String script =
                  arrayType + "[] a = new " + arrayType + "[1];\n"
                + "a[0] = " + arrayValue +";\n"
                + "def r;\n"
                + "for (" + returnType + " i : a) {r = i}\n"
                + "return r";
        assertEquals(expected, exec(script));
    }
}
