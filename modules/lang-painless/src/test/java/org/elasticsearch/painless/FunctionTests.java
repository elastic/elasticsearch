/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import static org.hamcrest.Matchers.containsString;

public class FunctionTests extends ScriptTestCase {

    public void testBasic() {
        assertEquals(5, exec("int get() {5;} get()"));
    }

    public void testReference() {
        assertEquals(5, exec("void get(int[] x) {x[0] = 5;} int[] y = new int[1]; y[0] = 1; get(y); y[0]"));
    }

    public void testConcat() {
        assertEquals("xyxy", exec("String catcat(String single) {single + single;} catcat('xy')"));
    }

    public void testMultiArgs() {
        assertEquals(5, exec("int add(int x, int y) {return x + y;} int x = 1, y = 2; add(add(x, x), add(x, y))"));
    }

    public void testMultiFuncs() {
        assertEquals(1, exec("int add(int x, int y) {return x + y;} int sub(int x, int y) {return x - y;} add(2, sub(3, 4))"));
        assertEquals(3, exec("int sub2(int x, int y) {sub(x, y) - y;} int sub(int x, int y) {return x - y;} sub2(5, 1)"));
    }

    public void testRecursion() {
        assertEquals(55, exec("int fib(int n) {if (n <= 1) return n; else return fib(n-1) + fib(n-2);} fib(10)"));
    }

    public void testEmpty() {
        Exception expected = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("void test(int x) {} test()");
        });
        assertThat(expected.getMessage(), containsString(
                "invalid function definition: found no statements for function [test] with [1] parameters"));
    }

    public void testReturnsAreUnboxedIfNeeded() {
        assertEquals((byte) 5, exec("byte get() {Byte.valueOf(5)} get()"));
        assertEquals((short) 5, exec("short get() {Byte.valueOf(5)} get()"));
        assertEquals(5, exec("int get() {Byte.valueOf(5)} get()"));
        assertEquals((short) 5, exec("short get() {Short.valueOf(5)} get()"));
        assertEquals(5, exec("int get() {Integer.valueOf(5)} get()"));
        assertEquals(5.0f, exec("float get() {Float.valueOf(5)} get()"));
        assertEquals(5.0d, exec("double get() {Float.valueOf(5)} get()"));
        assertEquals(5.0d, exec("double get() {Double.valueOf(5)} get()"));
        assertEquals(true, exec("boolean get() {Boolean.TRUE} get()"));
    }

    public void testDuplicates() {
        Exception expected = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("void test(int x) {x = 2;} void test(def y) {y = 3;} test()");
        });
        assertThat(expected.getMessage(), containsString("found duplicate function"));
    }

    public void testBadCastFromMethod() {
        Exception e = expectScriptThrows(ClassCastException.class, () -> exec("int get() {5L} get()"));
        assertEquals("Cannot cast from [long] to [int].", e.getMessage());
        e = expectScriptThrows(ClassCastException.class, () -> exec("int get() {5.1f} get()"));
        assertEquals("Cannot cast from [float] to [int].", e.getMessage());
        e = expectScriptThrows(ClassCastException.class, () -> exec("int get() {5.1d} get()"));
        assertEquals("Cannot cast from [double] to [int].", e.getMessage());
    }

    public void testInfiniteLoop() {
        Error expected = expectScriptThrows(PainlessError.class, () -> {
            exec("void test() {boolean x = true; while (x) {}} test()");
        });
        assertThat(expected.getMessage(),
                containsString("The maximum number of statements that can be executed in a loop has been reached."));
    }

    public void testReturnVoid() {
        assertEquals(null, exec("void test(StringBuilder b, int i) {b.setLength(i)} test(new StringBuilder(), 1)"));
        Exception expected = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("int test(StringBuilder b, int i) {b.setLength(i)} test(new StringBuilder(), 1)");
        });
        assertEquals("invalid function definition: " +
                "not all paths provide a return value for function [test] with [2] parameters", expected.getMessage());
        expected = expectScriptThrows(ClassCastException.class, () -> {
            exec("int test(StringBuilder b, int i) {return b.setLength(i)} test(new StringBuilder(), 1)");
        });
        assertEquals("Cannot cast from [void] to [int].", expected.getMessage());
        expected = expectScriptThrows(ClassCastException.class, () -> {
            exec("def test(StringBuilder b, int i) {return b.setLength(i)} test(new StringBuilder(), 1)");
        });
        assertEquals("Cannot cast from [void] to [def].", expected.getMessage());
    }
}
