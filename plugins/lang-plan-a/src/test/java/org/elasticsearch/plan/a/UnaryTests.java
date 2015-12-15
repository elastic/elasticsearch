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

package org.elasticsearch.plan.a;

/** Tests for unary operators across different types */
public class UnaryTests extends ScriptTestCase {

    /** basic tests */
    public void testBasics() {
        assertEquals(false, exec("return !true;"));
        assertEquals(true, exec("boolean x = false; return !x;"));
        assertEquals(-2, exec("return ~1;"));
        assertEquals(-2, exec("byte x = 1; return ~x;"));
        assertEquals(1, exec("return +1;"));
        assertEquals(1.0, exec("double x = 1; return +x;"));
        assertEquals(-1, exec("return -1;"));
        assertEquals(-2, exec("short x = 2; return -x;"));
    }

    public void testNegationInt() throws Exception {
        assertEquals(-1, exec("return -1;"));
        assertEquals(1, exec("return -(-1);"));
        assertEquals(0, exec("return -0;"));
    }
}
