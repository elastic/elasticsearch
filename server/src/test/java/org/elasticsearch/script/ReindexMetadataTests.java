/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.test.ESTestCase;

public class ReindexMetadataTests extends ESTestCase {
    private static final String INDEX = "myIndex";
    private static final String ID = "myId";
    private static final long VERSION = 5;
    private static final String ROUTING = "myRouting";
    private static final String OP = "index";

    private static final long TIMESTAMP = 1_658_000_000_000L;
    private ReindexMetadata metadata;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        reset();
    }

    protected void reset() {
        metadata = new ReindexMetadata(INDEX, ID, VERSION, ROUTING, OP, TIMESTAMP);
    }

    public void testIndex() {
        assertFalse(metadata.indexChanged());

        metadata.put("_index", INDEX);
        assertFalse(metadata.indexChanged());

        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> metadata.remove("_index"));
        assertEquals("_index cannot be removed", err.getMessage());
        err = expectThrows(IllegalArgumentException.class, () -> metadata.put("_index", null));
        assertEquals("_index cannot be null", err.getMessage());
        assertFalse(metadata.indexChanged());

        metadata.put("_index", "myIndex2");
        assertTrue(metadata.indexChanged());

        metadata.put("_index", INDEX);
        assertFalse(metadata.indexChanged());

        metadata.setIndex("myIndex3");
        assertTrue(metadata.indexChanged());
    }

    public void testId() {
        assertFalse(metadata.idChanged());

        metadata.put("_id", ID);
        assertFalse(metadata.idChanged());

        metadata.remove("_id");
        assertTrue(metadata.idChanged());
        assertNull(metadata.getId());

        metadata.put("_id", "myId2");
        assertTrue(metadata.idChanged());

        metadata.setId(ID);
        assertFalse(metadata.idChanged());

        metadata.setId("myId3");
        assertTrue(metadata.idChanged());
    }

    public void testRouting() {
        assertFalse(metadata.routingChanged());

        metadata.put("_routing", ROUTING);
        assertFalse(metadata.routingChanged());

        metadata.remove("_routing");
        assertTrue(metadata.routingChanged());
        assertNull(metadata.getRouting());

        metadata.put("_routing", "myRouting2");
        assertTrue(metadata.routingChanged());

        metadata.setRouting(ROUTING);
        assertFalse(metadata.routingChanged());

        metadata.setRouting("myRouting3");
        assertTrue(metadata.routingChanged());
    }

    public void testVersion() {
        assertFalse(metadata.versionChanged());

        metadata.put("_version", VERSION);
        assertFalse(metadata.versionChanged());

        metadata.remove("_version");
        assertTrue(metadata.versionChanged());
        assertTrue(metadata.isVersionInternal());
        assertEquals(Long.MIN_VALUE, metadata.getVersion());
        assertNull(metadata.get("_version"));

        metadata.put("_version", VERSION + 5);
        assertTrue(metadata.versionChanged());

        metadata.setVersion(VERSION);
        assertFalse(metadata.versionChanged());

        metadata.setVersion(VERSION + 10);
        assertTrue(metadata.versionChanged());
        assertEquals(VERSION + 10, metadata.getVersion());

        ReindexMetadata.setVersionToInternal(metadata);
        assertTrue(metadata.isVersionInternal());
        assertEquals(Long.MIN_VALUE, metadata.getVersion());
        assertNull(metadata.get("_version"));
    }

    public void testOp() {
        assertEquals("index", metadata.getOp());
        assertEquals("index", metadata.get("op"));

        metadata.setOp("noop");
        assertEquals("noop", metadata.getOp());
        assertEquals("noop", metadata.get("op"));

        metadata.put("op", "delete");
        assertEquals("delete", metadata.getOp());

        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> metadata.setOp("bad"));
        assertEquals("[op] must be one of delete, index, noop, not [bad]", err.getMessage());

        err = expectThrows(IllegalArgumentException.class, () -> metadata.put("op", "malo"));
        assertEquals("[op] must be one of delete, index, noop, not [malo]", err.getMessage());
    }
}
