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

public class BulkMetadataTests extends ESTestCase {
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

    public void testOp() {
        assertEquals(Op.INDEX, metadata.getOp());

        // Index has already been set
        ctx.put(MapBackedMetadata.OP, OP.name);
        assertEquals(Op.INDEX, metadata.getOp());
        reset();
        metadata.setOp(Op.INDEX);
        assertEquals(Op.INDEX, metadata.getOp());
        assertEquals("index", ctx.get(MapBackedMetadata.OP));

        reset();

        // NOOP works
        ctx.put(MapBackedMetadata.OP, Op.NOOP.name);
        assertEquals(Op.NOOP, metadata.getOp());
        reset();
        metadata.setOp(Op.NOOP);
        assertEquals(Op.NOOP, metadata.getOp());
        assertEquals("noop", ctx.get(MapBackedMetadata.OP));

        reset();

        // Delete works
        ctx.put(MapBackedMetadata.OP, Op.DELETE.name);
        assertEquals(Op.DELETE, metadata.getOp());
        reset();
        metadata.setOp(Op.DELETE);
        assertEquals(Op.DELETE, metadata.getOp());
        assertEquals("delete", ctx.get(MapBackedMetadata.OP));

        reset();

        // null not allowed via setOp
        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> metadata.setOp(null));
        assertEquals("operation must be non-null, valid operations are [noop, index, delete]", err.getMessage());
        assertEquals(Op.INDEX, metadata.getOp());
        assertEquals("index", ctx.get(MapBackedMetadata.OP));

        // create not allowed via setOp
        err = expectThrows(IllegalArgumentException.class, () -> metadata.setOp(Op.CREATE));
        assertEquals("Operation type [create] not allowed, only [noop, index, delete] are allowed", err.getMessage());
        assertEquals(Op.INDEX, metadata.getOp());
        assertEquals("index", ctx.get(MapBackedMetadata.OP));

        // unknown not allowed via setOp
        err = expectThrows(IllegalArgumentException.class, () -> metadata.setOp(Op.UNKOWN));
        assertEquals("Operation type [unknown] not allowed, only [noop, index, delete] are allowed", err.getMessage());
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

    private static class TestBulkMetadata extends BulkMetadata {

        TestBulkMetadata(String index, String id, Long version, String routing, Op op, Map<String, Object> source) {
            super(index, id, version, routing, op, source);
        }
    }
}
