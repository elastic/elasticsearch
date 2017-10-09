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

/** Tests for working with maps. */
public class MapTests extends ScriptTestCase {
    private void mapAccessesTestCase(String listType) {
        Object val = randomFrom("test", 1, 1.3, new Object());
        String decl = listType + " x = ['a': 1, 'b': 2, 0: 2, -5: 'slot', 123.1: 12]";
        assertEquals(5, exec(decl + "; return x.size()"));
        assertEquals(2, exec(decl + "; return x[0];", true));
        assertEquals(1, exec(decl + "; return x['a'];", true));
        assertEquals(12, exec(decl + "; return x[123.1];", true));
        assertEquals(val,    exec(decl + "; x[ 0] = params.val; return x[ 0];", singletonMap("val", val), true));
        assertEquals("slot", exec(decl + "; x[ 0] = params.val; return x[-5];", singletonMap("val", val), true));
        assertEquals(val,    exec(decl + "; x[-5] = params.val; return x[-5];", singletonMap("val", val), true));
    }

    public void testMapInDefAccesses() {
        mapAccessesTestCase("def");
    }

    public void testMapAccesses() {
        mapAccessesTestCase("Map");
    }
}
