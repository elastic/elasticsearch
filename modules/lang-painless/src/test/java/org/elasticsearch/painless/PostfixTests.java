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

public class PostfixTests extends ScriptTestCase {
    public void testConstantPostfixes() {
        assertEquals("2", exec("2.toString()"));
        assertEquals(4, exec("[1, 2, 3, 4, 5][3]"));
        assertEquals("4", exec("[1, 2, 3, 4, 5][3].toString()"));
        assertEquals(3, exec("new int[] {1, 2, 3, 4, 5}[2]"));
        assertEquals("4", exec("(2 + 2).toString()"));
    }

    public void testConditionalPostfixes() {
        assertEquals("5", exec("boolean b = false; (b ? 4 : 5).toString()"));
        assertEquals(3, exec(
            "Map x = new HashMap(); x['test'] = 3;" +
            "Map y = new HashMap(); y['test'] = 4;" +
            "boolean b = true;" +
            "return (int)(b ? x : y).get('test')")
        );
    }

    public void testAssignmentPostfixes() {
        assertEquals(true, exec("int x; '3' == (x = 3).toString()"));
        assertEquals(-1, exec("int x; (x = 3).compareTo(4)"));
        assertEquals(3L, exec("long[] x; (x = new long[1])[0] = 3; return x[0]"));
        assertEquals(2, exec("int x; ((x)) = 2; return x;"));
    }

    public void testDefConditionalPostfixes() {
        assertEquals("5", exec("def b = false; (b ? 4 : 5).toString()"));
        assertEquals(3, exec(
            "def x = new HashMap(); x['test'] = 3;" +
            "def y = new HashMap(); y['test'] = 4;" +
            "boolean b = true;" +
            "return (b ? x : y).get('test')")
        );
    }

    public void testDefAssignmentPostfixes() {
        assertEquals(true, exec("def x; '3' == (x = 3).toString()"));
        assertEquals(-1, exec("def x; (x = 3).compareTo(4)"));
        assertEquals(3L, exec("def x; (x = new long[1])[0] = 3; return x[0]"));
        assertEquals(2, exec("def x; ((x)) = 2; return x;"));
    }
}
