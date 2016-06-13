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

/** Tests for or operator across all types */
public class ArrayTests extends ScriptTestCase {

    public void testArrayLengthHelper() throws Throwable {
        assertArrayLength(2, new int[2]);
        assertArrayLength(3, new long[3]);
        assertArrayLength(4, new byte[4]);
        assertArrayLength(5, new float[5]);
        assertArrayLength(6, new double[6]);
        assertArrayLength(7, new char[7]);
        assertArrayLength(8, new short[8]);
        assertArrayLength(9, new Object[9]);
        assertArrayLength(10, new Integer[10]);
        assertArrayLength(11, new String[11][2]);
    }

    private void assertArrayLength(int length, Object array) throws Throwable {
        assertEquals(length, (int) Def.arrayLengthGetter(array.getClass()).invoke(array));
    }

    public void testArrayLoadStoreInt() {
        assertEquals(5, exec("def x = new int[5]; return x.length"));
        assertEquals(5, exec("def x = new int[4]; x[0] = 5; return x[0];"));
    }

    public void testArrayLoadStoreString() {
        assertEquals(5, exec("def x = new String[5]; return x.length"));
        assertEquals("foobar", exec("def x = new String[4]; x[0] = 'foobar'; return x[0];"));
    }

    public void testArrayLoadStoreDef() {
        assertEquals(5, exec("def x = new def[5]; return x.length"));
        assertEquals(5, exec("def x = new def[4]; x[0] = 5; return x[0];"));
    }

    public void testArrayCompoundInt() {
        assertEquals(6, exec("int[] x = new int[5]; x[0] = 5; x[0]++; return x[0];"));
    }

    public void testArrayCompoundDef() {
        assertEquals(6, exec("def x = new int[5]; x[0] = 5; x[0]++; return x[0];"));
    }

    public void testJacksCrazyExpression1() {
        assertEquals(1, exec("int x; def[] y = new def[1]; x = y[0] = 1; return x;"));
    }

    public void testJacksCrazyExpression2() {
        assertEquals(1, exec("int x; def y = new def[1]; x = y[0] = 1; return x;"));
    }

    public void testArrayVariable() {
        assertEquals(1, exec("int x = 1; int[] y = new int[x]; return y.length"));
    }

    public void testForLoop() {
        assertEquals(999*1000/2, exec("def a = new int[1000]; for (int x = 0; x < a.length; x++) { a[x] = x; } "+
            "int total = 0; for (int x = 0; x < a.length; x++) { total += a[x]; } return total;"));
    }

    /**
     * Make sure we don't try and convert the {@code /} after the {@code ]} into a regex....
     */
    public void testDivideArray() {
        assertEquals(1, exec("def[] x = new def[1]; x[0] = 2; return x[0] / 2"));
    }
}
