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

import static java.util.Collections.singletonMap;

/**
 * Tests for the Elvis operator ({@code ?:}).
 */
public class ElvisTests extends ScriptTestCase {
    public void testElvisOperator() {
        // Basics
        assertEquals("str", exec("return params.a ?: 'str'"));
        assertEquals("str", exec("return params.a ?: 'str2'", singletonMap("a", "str"), true));
        assertEquals("str", exec("return params.a ?: null", singletonMap("a", "str"), true));
        assertNull(         exec("return params.a ?: null"));

        // Basics with primitives
        assertEquals(1, exec("return params.a ?: 1"));
        assertEquals(1, exec("return params.a ?: 2", singletonMap("a", 1), true));

        // Now some chains
        assertEquals(1, exec("return params.a ?: params.a ?: 1"));
        assertEquals(1, exec("return params.a ?: params.b ?: null", singletonMap("b", 1), true));
        assertEquals(1, exec("return params.a ?: null ?: null", singletonMap("a", 1), true));

        // Precedence
        assertEquals(1, exec("return params.a ?: 2 + 2", singletonMap("a", 1), true));
        assertEquals(2, exec("Integer i = 1; return i + params.a ?: 2 + 2", singletonMap("a", 1), true));
        assertEquals(4, exec("return params.a ?: 2 + 2"));

        // Weird casts
        assertEquals(1,     exec("int i = params.i;     String s = params.s; return s ?: i", singletonMap("i", 1), true));
        assertEquals("str", exec("Integer i = params.i; String s = params.s; return s ?: i", singletonMap("s", "str"), true));
    }

    public void testElvisOperatorWithNullSafeDereference() {
        assertEquals(1, exec("return params.a?.b ?: 1"));
        assertEquals(1, exec("return params.a?.b ?: 2", singletonMap("a", singletonMap("b", 1)), true));
    }

    public void testExtraneousElvis() {
        Exception e = expectScriptThrows(IllegalArgumentException.class, () -> exec("int i = params.a; return i ?: 1"));
        assertEquals(e.getMessage(), "Extraneous elvis operator. LHS is a primitive.");
        e = expectScriptThrows(IllegalArgumentException.class, () -> exec("return 'cat' ?: 1"));
        assertEquals(e.getMessage(), "Extraneous elvis operator. LHS is a constant.");
    }
}
