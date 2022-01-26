/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
        assertEquals(val, exec(decl + "; x[ 0] = params.val; return x[ 0];", singletonMap("val", val), true));
        assertEquals("slot", exec(decl + "; x[ 0] = params.val; return x[-5];", singletonMap("val", val), true));
        assertEquals(val, exec(decl + "; x[-5] = params.val; return x[-5];", singletonMap("val", val), true));
    }

    public void testMapInDefAccesses() {
        mapAccessesTestCase("def");
    }

    public void testMapAccesses() {
        mapAccessesTestCase("Map");
    }
}
