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

import org.apache.lucene.util.Constants;
import org.elasticsearch.bootstrap.JavaVersion;
import org.hamcrest.Matcher;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;

import static org.hamcrest.Matchers.equalTo;

/** Tests for working with arrays. */
public class ArrayTests extends ArrayLikeObjectTestCase {
    @Override
    protected String declType(String valueType) {
        return valueType + "[]";
    }

    @Override
    protected String valueCtorCall(String valueType, int size) {
        return "new " + valueType + "[" + size + "]";
    }

    @Override
    protected Matcher<String> outOfBoundsExceptionMessageMatcher(int index, int size) {
        if (JavaVersion.current().compareTo(JavaVersion.parse("11")) < 0) {
            return equalTo(Integer.toString(index));
        } else{
            return equalTo("Index " + Integer.toString(index) + " out of bounds for length " + Integer.toString(size));
        }
    }

    public void testArrayLengthHelper() throws Throwable {
        assertEquals(Constants.JRE_IS_MINIMUM_JAVA9, Def.JAVA9_ARRAY_LENGTH_MH_FACTORY != null);
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
        final MethodHandle mh = Def.arrayLengthGetter(array.getClass());
        assertSame(array.getClass(), mh.type().parameterType(0));
        assertEquals(length, (int) mh.asType(MethodType.methodType(int.class, Object.class))
                .invokeExact(array));
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
