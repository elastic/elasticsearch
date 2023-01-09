/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

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
        return equalTo("Index " + Integer.toString(index) + " out of bounds for length " + Integer.toString(size));
    }

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
        final MethodHandle mh = Def.arrayLengthGetter(array.getClass());
        assertSame(array.getClass(), mh.type().parameterType(0));
        assertEquals(length, (int) mh.asType(MethodType.methodType(int.class, Object.class)).invokeExact(array));
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
        assertEquals(
            999 * 1000 / 2,
            exec(
                "def a = new int[1000]; for (int x = 0; x < a.length; x++) { a[x] = x; } "
                    + "int total = 0; for (int x = 0; x < a.length; x++) { total += a[x]; } return total;"
            )
        );
    }

    /**
     * Make sure we don't try and convert the {@code /} after the {@code ]} into a regex....
     */
    public void testDivideArray() {
        assertEquals(1, exec("def[] x = new def[1]; x[0] = 2; return x[0] / 2"));
    }

    public void testPrimitiveIteration() {
        assertEquals(true, exec("def x = new boolean[] { true, false }; boolean s = false; for (boolean l : x) s |= l; return s"));
        expectScriptThrows(
            ClassCastException.class,
            () -> exec("def x = new boolean[] { true, false }; short s = 0; for (short l : x) s += l; return s")
        );
        expectScriptThrows(
            ClassCastException.class,
            () -> exec("def x = new boolean[] { true, false }; char s = 0; for (char l : x) s = l; return s")
        );
        expectScriptThrows(
            ClassCastException.class,
            () -> exec("def x = new boolean[] { true, false }; int s = 0; for (int l : x) s += l; return s")
        );
        expectScriptThrows(
            ClassCastException.class,
            () -> exec("def x = new boolean[] { true, false }; long s = 0; for (long l : x) s += l; return s")
        );
        expectScriptThrows(
            ClassCastException.class,
            () -> exec("def x = new boolean[] { true, false }; float s = 0; for (float l : x) s += l; return s")
        );
        expectScriptThrows(
            ClassCastException.class,
            () -> exec("def x = new boolean[] { true, false }; double s = 0; for (double l : x) s += l; return s")
        );
        assertEquals(true, exec("def x = new boolean[] { true, false }; boolean s = false; for (def l : x) s |= l; return s"));

        assertEquals((byte) 30, exec("def x = new byte[] { (byte)10, (byte)20 }; byte s = 0; for (byte l : x) s += l; return s"));
        assertEquals((short) 30, exec("def x = new byte[] { (byte)10, (byte)20 }; short s = 0; for (short l : x) s += l; return s"));
        assertEquals((char) 20, exec("def x = new byte[] { (byte)10, (byte)20 }; char s = 0; for (char l : x) s = l; return s"));
        assertEquals(30, exec("def x = new byte[] { (byte)10, (byte)20 }; int s = 0; for (int l : x) s += l; return s"));
        assertEquals(30L, exec("def x = new byte[] { (byte)10, (byte)20 }; long s = 0; for (long l : x) s += l; return s"));
        assertEquals(30f, exec("def x = new byte[] { (byte)10, (byte)20 }; float s = 0; for (float l : x) s += l; return s"));
        assertEquals(30d, exec("def x = new byte[] { (byte)10, (byte)20 }; double s = 0; for (double l : x) s += l; return s"));
        assertEquals((byte) 30, exec("def x = new byte[] { (byte)10, (byte)20 }; byte s = 0; for (def l : x) s += l; return s"));

        assertEquals((byte) 30, exec("def x = new short[] { (short)10, (short)20 }; byte s = 0; for (byte l : x) s += l; return s"));
        assertEquals((short) 300, exec("def x = new short[] { (short)100, (short)200 }; short s = 0; for (short l : x) s += l; return s"));
        assertEquals((char) 200, exec("def x = new short[] { (short)100, (short)200 }; char s = 0; for (char l : x) s = l; return s"));
        assertEquals(300, exec("def x = new short[] { (short)100, (short)200 }; int s = 0; for (int l : x) s += l; return s"));
        assertEquals(300L, exec("def x = new short[] { (short)100, (short)200 }; long s = 0; for (long l : x) s += l; return s"));
        assertEquals(300f, exec("def x = new short[] { (short)100, (short)200 }; float s = 0; for (float l : x) s += l; return s"));
        assertEquals(300d, exec("def x = new short[] { (short)100, (short)200 }; double s = 0; for (double l : x) s += l; return s"));
        assertEquals((short) 300, exec("def x = new short[] { (short)100, (short)200 }; short s = 0; for (def l : x) s += l; return s"));

        assertEquals((byte) 'b', exec("def x = new char[] { (char)'a', (char)'b' }; byte s = 0; for (byte l : x) s = l; return s"));
        assertEquals((short) 'b', exec("def x = new char[] { (char)'a', (char)'b' }; short s = 0; for (short l : x) s = l; return s"));
        assertEquals('b', exec("def x = new char[] { (char)'a', (char)'b' }; char s = 0; for (char l : x) s = l; return s"));
        assertEquals((int) 'b', exec("def x = new char[] { (char)'a', (char)'b' }; int s = 0; for (int l : x) s = l; return s"));
        assertEquals((long) 'b', exec("def x = new char[] { (char)'a', (char)'b' }; long s = 0; for (long l : x) s = l; return s"));
        assertEquals((float) 'b', exec("def x = new char[] { (char)'a', (char)'b' }; float s = 0; for (float l : x) s = l; return s"));
        assertEquals((double) 'b', exec("def x = new char[] { (char)'a', (char)'b' }; double s = 0; for (double l : x) s = l; return s"));
        assertEquals('b', exec("def x = new char[] { (char)'a', (char)'b' }; char s = 0; for (def l : x) s = l; return s"));

        assertEquals((byte) 30, exec("def x = new int[] { 10, 20 }; byte s = 0; for (byte l : x) s += l; return s"));
        assertEquals((short) 300, exec("def x = new int[] { 100, 200 }; short s = 0; for (short l : x) s += l; return s"));
        assertEquals((char) 200, exec("def x = new int[] { 100, 200 }; char s = 0; for (char l : x) s = l; return s"));
        assertEquals(300, exec("def x = new int[] { 100, 200 }; int s = 0; for (int l : x) s += l; return s"));
        assertEquals(300L, exec("def x = new int[] { 100, 200 }; long s = 0; for (long l : x) s += l; return s"));
        assertEquals(300f, exec("def x = new int[] { 100, 200 }; float s = 0; for (float l : x) s += l; return s"));
        assertEquals(300d, exec("def x = new int[] { 100, 200 }; double s = 0; for (double l : x) s += l; return s"));
        assertEquals(300, exec("def x = new int[] { 100, 200 }; int s = 0; for (def l : x) s += l; return s"));

        assertEquals((byte) 30, exec("def x = new long[] { 10, 20 }; byte s = 0; for (byte l : x) s += l; return s"));
        assertEquals((short) 300, exec("def x = new long[] { 100, 200 }; short s = 0; for (short l : x) s += l; return s"));
        assertEquals((char) 200, exec("def x = new long[] { 100, 200 }; char s = 0; for (char l : x) s = l; return s"));
        assertEquals(300, exec("def x = new long[] { 100, 200 }; int s = 0; for (int l : x) s += l; return s"));
        assertEquals(300L, exec("def x = new long[] { 100, 200 }; long s = 0; for (long l : x) s += l; return s"));
        assertEquals(300f, exec("def x = new long[] { 100, 200 }; float s = 0; for (float l : x) s += l; return s"));
        assertEquals(300d, exec("def x = new long[] { 100, 200 }; double s = 0; for (double l : x) s += l; return s"));
        assertEquals(300L, exec("def x = new long[] { 100, 200 }; long s = 0; for (def l : x) s += l; return s"));

        assertEquals((byte) 30, exec("def x = new float[] { 10, 20 }; byte s = 0; for (byte l : x) s += l; return s"));
        assertEquals((short) 300, exec("def x = new float[] { 100, 200 }; short s = 0; for (short l : x) s += l; return s"));
        assertEquals((char) 200, exec("def x = new float[] { 100, 200 }; char s = 0; for (char l : x) s = l; return s"));
        assertEquals(300, exec("def x = new float[] { 100, 200 }; int s = 0; for (int l : x) s += l; return s"));
        assertEquals(300L, exec("def x = new float[] { 100, 200 }; long s = 0; for (long l : x) s += l; return s"));
        assertEquals(300f, exec("def x = new float[] { 100, 200 }; float s = 0; for (float l : x) s += l; return s"));
        assertEquals(300d, exec("def x = new float[] { 100, 200 }; double s = 0; for (double l : x) s += l; return s"));
        assertEquals(300f, exec("def x = new float[] { 100, 200 }; float s = 0; for (def l : x) s += l; return s"));

        assertEquals((byte) 30, exec("def x = new double[] { 10, 20 }; byte s = 0; for (byte l : x) s += l; return s"));
        assertEquals((short) 300, exec("def x = new double[] { 100, 200 }; short s = 0; for (short l : x) s += l; return s"));
        assertEquals((char) 200, exec("def x = new double[] { 100, 200 }; char s = 0; for (char l : x) s = l; return s"));
        assertEquals(300, exec("def x = new double[] { 100, 200 }; int s = 0; for (int l : x) s += l; return s"));
        assertEquals(300L, exec("def x = new double[] { 100, 200 }; long s = 0; for (long l : x) s += l; return s"));
        assertEquals(300f, exec("def x = new double[] { 100, 200 }; float s = 0; for (float l : x) s += l; return s"));
        assertEquals(300d, exec("def x = new double[] { 100, 200 }; double s = 0; for (double l : x) s += l; return s"));
        assertEquals(300d, exec("def x = new double[] { 100, 200 }; double s = 0; for (def l : x) s += l; return s"));

        expectScriptThrows(
            ClassCastException.class,
            () -> exec("def x = new String[] { 'foo', 'bar' }; boolean s = false; for (boolean l : x) s |= l; return s")
        );
        expectScriptThrows(
            ClassCastException.class,
            () -> exec("def x = new String[] { 'foo', 'bar' }; byte s = 0; for (byte l : x) s += l; return s")
        );
        expectScriptThrows(
            ClassCastException.class,
            () -> exec("def x = new String[] { 'foo', 'bar' }; short s = 0; for (short l : x) s += l; return s")
        );
        expectScriptThrows(
            ClassCastException.class,
            () -> exec("def x = new String[] { 'foo', 'bar' }; char s = 0; for (char l : x) s = l; return s")
        );
        expectScriptThrows(
            ClassCastException.class,
            () -> exec("def x = new String[] { 'foo', 'bar' }; int s = 0; for (int l : x) s += l; return s")
        );
        expectScriptThrows(
            ClassCastException.class,
            () -> exec("def x = new String[] { 'foo', 'bar' }; long s = 0; for (long l : x) s += l; return s")
        );
        expectScriptThrows(
            ClassCastException.class,
            () -> exec("def x = new String[] { 'foo', 'bar' }; float s = 0; for (float l : x) s += l; return s")
        );
        expectScriptThrows(
            ClassCastException.class,
            () -> exec("def x = new String[] { 'foo', 'bar' }; double s = 0; for (double l : x) s += l; return s")
        );
    }
}
