/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

public class WriteFieldTest extends ESTestCase {

    public void testResolveDepthFlat() {
        Map<String, Object> map = new HashMap<>();
        map.put("abc.d.ef", "flat");

        Map<String, Object> abc = new HashMap<>();
        map.put("abc", abc);
        abc.put("d.ef", "mixed");

        Map<String, Object> d = new HashMap<>();
        abc.put("d", d);
        d.put("ef", "nested");

        // { "abc.d.ef", "flat", "abc": { "d.ef": "mixed", "d": { "ef": "nested" } } }
        WriteField wf = new WriteField("abc.d.ef", () -> map);
        assertTrue(wf.isExists());

        assertEquals("nested", wf.get("missing"));
        // { "abc.d.ef", "flat", "abc": { "d.ef": "mixed", "d": { } } }
        d.remove("ef");
        assertEquals("missing", wf.get("missing"));
        // { "abc.d.ef", "flat", "abc": { "d.ef": "mixed" }
        // TODO(stu): this should be inaccessible
        abc.remove("d");
        assertEquals("missing", wf.get("missing"));

        // resolution at construction time
        wf = new WriteField("abc.d.ef", () -> map);
        assertEquals("mixed", wf.get("missing"));
        abc.remove("d.ef");
        assertEquals("missing", wf.get("missing"));

        wf = new WriteField("abc.d.ef", () -> map);
        // abc is still there
        assertEquals("missing", wf.get("missing"));
        map.remove("abc");
        assertEquals("missing", wf.get("missing"));

        wf = new WriteField("abc.d.ef", () -> map);
        assertEquals("flat", wf.get("missing"));
    }

    public void testCreateDepth() {}
}
