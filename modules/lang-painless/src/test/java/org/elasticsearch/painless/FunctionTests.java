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
        assertTrue(expected.getMessage().contains("Cannot generate an empty function"));
    }

    public void testDuplicates() {
        Exception expected = expectScriptThrows(IllegalArgumentException.class, () -> {
            exec("void test(int x) {x = 2;} void test(def y) {y = 3;} test()");
        });
        assertTrue(expected.getMessage().contains("Duplicate functions"));
    }

    public void testInfiniteLoop() {
        Error expected = expectScriptThrows(PainlessError.class, () -> {
            exec("void test() {boolean x = true; while (x) {}} test()");
        });
        assertTrue(expected.getMessage().contains(
            "The maximum number of statements that can be executed in a loop has been reached."));
    }
}
