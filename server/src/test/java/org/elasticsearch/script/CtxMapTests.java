/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class CtxMapTests extends ESTestCase {
    CtxMap<?> map;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        map = new CtxMap<>(new HashMap<>(), new Metadata(new HashMap<>(), new HashMap<>()));
    }

    public void testAddingJunkToCtx() {
        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> map.put("junk", "stuff"));
        assertEquals(err.getMessage(), "invalid field added to source [junk:stuff]");
    }

    public void testRemovingSource() {
        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> map.remove("_source"));
        assertEquals(err.getMessage(), "cannot remove [_source]");
    }

    @SuppressWarnings("unchecked")
    public void testReplacingSource() {
        map.put("_source", Map.of("abc", 123));
        assertEquals(123, ((Map<String, Object>) map.get("_source")).get("abc"));
    }

    public void testInvalidReplacementOfSource() {
        IllegalArgumentException err = expectThrows(
            IllegalArgumentException.class,
            () -> map.put("_source", List.of(1, 2, "buckle my shoe"))
        );
        assertThat(err.getMessage(), containsString("[_source] must be a map, not [[1, 2, buckle my shoe]] with type"));
    }
}
