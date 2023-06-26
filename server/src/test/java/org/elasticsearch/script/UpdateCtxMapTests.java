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

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.hasEntry;

public class UpdateCtxMapTests extends ESTestCase {
    UpdateCtxMap map;
    Metadata meta;

    private final long TS = 704289600000L;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        map = new UpdateCtxMap("myIndex", "myId", 5, "myRoute", "myType", "index", TS, Map.of("foo", "bar"));
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
        assertEquals(5, meta.getVersion());
        assertEquals("myRoute", meta.getRouting());
        assertEquals("index", meta.getOp());
    }

    public void testMetadataImmutable() {
        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> meta.put("_index", "myIndex2"));
        assertEquals("_index cannot be updated", err.getMessage());
        err = expectThrows(IllegalArgumentException.class, () -> meta.put("_id", "myId"));
        assertEquals("_id cannot be updated", err.getMessage());
        err = expectThrows(IllegalArgumentException.class, () -> meta.put("_routing", "myRouting"));
        assertEquals("_routing cannot be updated", err.getMessage());
        err = expectThrows(IllegalArgumentException.class, () -> meta.put("_type", "myType"));
        assertEquals("_type cannot be updated", err.getMessage());
        err = expectThrows(IllegalArgumentException.class, () -> meta.put("_version", 10));
        assertEquals("_version cannot be updated", err.getMessage());
    }

    public void testValidOps() {
        List<String> ops = List.of("noop", "index", "delete");
        for (String op : ops) {
            meta.setOp(op);
            assertEquals(op, meta.getOp());
        }
        for (String op : ops) {
            map.put("op", op);
            assertEquals(op, map.get("op"));
        }
    }

    public void testInvalidOp() {
        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> meta.setOp("none"));
        assertEquals("'none' is not allowed, use 'noop' instead", err.getMessage());
        err = expectThrows(IllegalArgumentException.class, () -> meta.setOp(null));
        assertEquals("op must be one of [delete, index, noop], not [null]", err.getMessage());
        err = expectThrows(IllegalArgumentException.class, () -> meta.setOp("foo"));
        assertEquals("op must be one of [delete, index, noop], not [foo]", err.getMessage());
        meta.put("op", "none");
        assertEquals("noop", meta.getOp());
        meta.put("op", "foo");
        assertEquals("noop", meta.getOp());
        meta.remove("op");
        assertEquals("noop", meta.getOp());
        meta.put("op", "index");
        assertEquals("index", meta.getOp());
    }

    public void testTimestamp() {
        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> meta.put("_now", 1234));
        assertEquals("_now cannot be updated", err.getMessage());
        assertEquals(TS, meta.get("_now"));
        ZonedDateTime zdt = meta.getNow();
        assertEquals(4, zdt.getMonthValue());
        assertEquals(26, zdt.getDayOfMonth());
        assertEquals(1992, zdt.getYear());
    }
}
