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

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.hasEntry;

public class UpsertCtxMapTests extends ESTestCase {
    UpsertCtxMap map;
    Metadata meta;
    long TS = 922860000000L;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        map = new UpsertCtxMap("myIndex", "myId", "create", TS, Map.of("foo", "bar"));
        meta = map.getMetadata();
    }

    @SuppressWarnings("unchecked")
    public void testSourceWrapping() {
        assertThat((Map<String, Object>) map.get("_source"), hasEntry("foo", "bar"));
        assertThat(map.getSource(), hasEntry("foo", "bar"));
    }

    public void testGetters() {
        assertEquals("myIndex", meta.getIndex());
        assertEquals("myId", meta.getId());
        assertEquals("create", meta.getOp());
        UnsupportedOperationException err = expectThrows(UnsupportedOperationException.class, () -> meta.getVersion());
        assertEquals("version is unavailable for insert", err.getMessage());
        err = expectThrows(UnsupportedOperationException.class, () -> meta.getRouting());
        assertEquals("routing is unavailable for insert", err.getMessage());
    }

    public void testMetadataImmutable() {
        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> meta.put("_index", "myIndex2"));
        assertEquals("_index cannot be updated", err.getMessage());
        err = expectThrows(IllegalArgumentException.class, () -> meta.put("_id", "myId"));
        assertEquals("_id cannot be updated", err.getMessage());
        err = expectThrows(IllegalArgumentException.class, () -> meta.put("_now", 1234));
        assertEquals("_now cannot be updated", err.getMessage());
    }

    public void testValidOps() {
        List<String> ops = List.of("noop", "create");
        for (String op : ops) {
            meta.setOp(op);
            assertEquals(op, meta.getOp());
        }
        for (String op : ops) {
            map.put("op", op);
            assertEquals(op, map.get("op"));
        }
    }

    public void testNoneOp() {
        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> meta.setOp("none"));
        assertEquals("'none' is not allowed, use 'noop' instead", err.getMessage());
        meta.put("op", "none");
        assertEquals("noop", meta.getOp());
        meta.remove("op");
        assertEquals("noop", meta.getOp());
        meta.put("op", "create");
        assertEquals("create", meta.getOp());
    }
}
