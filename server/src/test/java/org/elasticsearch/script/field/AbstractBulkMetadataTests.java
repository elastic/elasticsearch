/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

public class AbstractBulkMetadataTests extends ESTestCase {
    private static final String INDEX = "myIndex";
    private static final String ID = "myId";
    private static final long VERSION = 5;
    private static final String ROUTING = "myRouting";
    private static final Op OP = Op.INDEX;
    private static final Map<String, Object> SOURCE = Map.of("foo", List.of("bar", "baz"));
    private TestBulkMetadata metadata;
    private Map<String, Object> ctx;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        reset();
    }

    protected void reset() {
        metadata = new TestBulkMetadata(INDEX, ID, VERSION, ROUTING, OP, SOURCE);
        ctx = metadata.getCtx();
    }

    public void testIndexChanged() {
        assertFalse(metadata.indexChanged());

        ctx.put(MapBackedMetadata.INDEX, INDEX);
        assertFalse(metadata.indexChanged());

        ctx.remove(MapBackedMetadata.INDEX);
        assertTrue(metadata.indexChanged());
        assertNull(metadata.getIndex());

        ctx.put(MapBackedMetadata.INDEX, INDEX);
        assertFalse(metadata.indexChanged());

        ctx.put(MapBackedMetadata.INDEX, "myIndex2");
        assertTrue(metadata.indexChanged());
    }

    public void testIdChanged() {
        assertFalse(metadata.idChanged());

        ctx.put(MapBackedMetadata.ID, ID);
        assertFalse(metadata.idChanged());

        ctx.remove(MapBackedMetadata.ID);
        assertTrue(metadata.idChanged());
        assertNull(metadata.getId());

        ctx.put(MapBackedMetadata.ID, ID);
        assertFalse(metadata.idChanged());

        ctx.put(MapBackedMetadata.ID, "myId2");
        assertTrue(metadata.idChanged());
    }

    public void testRoutingChanged() {
        assertFalse(metadata.routingChanged());

        ctx.put(MapBackedMetadata.ROUTING, ROUTING);
        assertFalse(metadata.routingChanged());

        ctx.remove(MapBackedMetadata.ROUTING);
        assertTrue(metadata.routingChanged());
        assertNull(metadata.getRouting());

        ctx.put(MapBackedMetadata.ROUTING, ROUTING);
        assertFalse(metadata.routingChanged());

        ctx.put(MapBackedMetadata.ROUTING, "myRouting2");
        assertTrue(metadata.routingChanged());
    }

    public void testVersionChanged() {
        assertFalse(metadata.versionChanged());

        ctx.put(MapBackedMetadata.VERSION, VERSION);
        assertFalse(metadata.versionChanged());

        ctx.remove(MapBackedMetadata.VERSION);
        assertTrue(metadata.versionChanged());
        assertNull(metadata.getVersion());

        ctx.put(MapBackedMetadata.VERSION, VERSION);
        assertFalse(metadata.versionChanged());

        ctx.put(MapBackedMetadata.VERSION, VERSION + 1);
        assertTrue(metadata.versionChanged());

    }

    public void testOp() {
        assertFalse(metadata.opChanged());
        assertEquals(Op.INDEX, metadata.getOp());

        // Index has already been set
        ctx.put(MapBackedMetadata.OP, OP.name);
        assertFalse(metadata.opChanged());
        assertEquals(Op.INDEX, metadata.getOp());
        reset();
        metadata.setOp(Op.INDEX);
        assertFalse(metadata.opChanged());
        assertEquals(Op.INDEX, metadata.getOp());
        assertEquals("index", ctx.get(MapBackedMetadata.OP));

        reset();

        // NOOP works
        ctx.put(MapBackedMetadata.OP, Op.NOOP.name);
        assertTrue(metadata.opChanged());
        assertEquals(Op.NOOP, metadata.getOp());
        reset();
        metadata.setOp(Op.NOOP);
        assertTrue(metadata.opChanged());
        assertEquals(Op.NOOP, metadata.getOp());
        assertEquals("noop", ctx.get(MapBackedMetadata.OP));

        reset();

        // Delete works
        ctx.put(MapBackedMetadata.OP, Op.DELETE.name);
        assertTrue(metadata.opChanged());
        assertEquals(Op.DELETE, metadata.getOp());
        reset();
        metadata.setOp(Op.DELETE);
        assertTrue(metadata.opChanged());
        assertEquals(Op.DELETE, metadata.getOp());
        assertEquals("delete", ctx.get(MapBackedMetadata.OP));

        reset();

        // null not allowed via setOp
        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> metadata.setOp(null));
        assertEquals("operation must be non-null, valid operations are [noop, index, delete]", err.getMessage());
        assertFalse(metadata.opChanged());
        assertEquals(Op.INDEX, metadata.getOp());
        assertEquals("index", ctx.get(MapBackedMetadata.OP));

        // create not allowed via setOp
        err = expectThrows(IllegalArgumentException.class, () -> metadata.setOp(Op.CREATE));
        assertEquals("Operation type [create] not allowed, only [noop, index, delete] are allowed", err.getMessage());
        assertFalse(metadata.opChanged());
        assertEquals(Op.INDEX, metadata.getOp());
        assertEquals("index", ctx.get(MapBackedMetadata.OP));

        // unknown not allowed via setOp
        err = expectThrows(IllegalArgumentException.class, () -> metadata.setOp(Op.UNKOWN));
        assertEquals("Operation type [unknown] not allowed, only [noop, index, delete] are allowed", err.getMessage());
        assertFalse(metadata.opChanged());
        assertEquals(Op.INDEX, metadata.getOp());
        assertEquals("index", ctx.get(MapBackedMetadata.OP));

        // setting disallowed values in ctx causes getOp to fail
        ctx.put(MapBackedMetadata.OP, Op.CREATE.name);
        err = expectThrows(IllegalArgumentException.class, () -> metadata.getOp());
        assertEquals("Operation type [create] not allowed, only [noop, index, delete] are allowed", err.getMessage());

        ctx.put(MapBackedMetadata.OP, Op.UNKOWN.name);
        err = expectThrows(IllegalArgumentException.class, () -> metadata.getOp());
        assertEquals("Operation type [unknown] not allowed, only [noop, index, delete] are allowed", err.getMessage());

        ctx.put(MapBackedMetadata.OP, "other");
        err = expectThrows(IllegalArgumentException.class, () -> metadata.getOp());
        assertEquals("Operation type [other] not allowed, only [noop, index, delete] are allowed", err.getMessage());

        ctx.put(MapBackedMetadata.OP, null);
        err = expectThrows(IllegalArgumentException.class, () -> metadata.getOp());
        assertEquals("operation must be non-null, valid operations are [noop, index, delete]", err.getMessage());
    }

    private static class TestBulkMetadata extends AbstractBulkMetadata {

        TestBulkMetadata(String index, String id, Long version, String routing, Op op, Map<String, Object> source) {
            super(index, id, version, routing, op, source);
        }
    }
}
