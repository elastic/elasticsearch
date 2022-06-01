/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.script.field.Op;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class UpdateScriptMetadataTests extends ESTestCase {
    UpdateScript.Metadata meta = null;
    Map<String, Object> ctx = null;

    protected void insert() {
        meta = UpdateScript.insert("myIndex", "myId", Op.CREATE, 0L, Collections.emptyMap());
        ctx = meta.getCtx();
    }

    protected void update() {
        meta = UpdateScript.update("myIndex", "myId", 5L, "myRouting", Op.INDEX, 0L, "myType", Collections.emptyMap());
        ctx = meta.getCtx();
    }

    public void testSetOp() {
        insert();
        ctx.put("op", null);
        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, meta::getOp);
        assertEquals("op must be non-null", err.getMessage());

        ctx.put("op", "none");
        assertEquals(Op.NOOP, meta.getOp());

        ctx.put("op", Op.INDEX.name);
        err = expectThrows(IllegalArgumentException.class, meta::getOp);
        assertEquals("unknown op [index], valid ops are none, noop, create", err.getMessage());

        update();

        ctx.put("op", Op.INDEX.name);
        assertEquals(Op.INDEX, meta.getOp());
    }

    public void testInsertDisablesGetters() {
        insert();
        IllegalStateException err = expectThrows(IllegalStateException.class, meta::getRouting);
        assertEquals("routing unavailable for insert", err.getMessage());
        err = expectThrows(IllegalStateException.class, meta::getVersion);
        assertEquals("version unavailable for inserts", err.getMessage());
        err = expectThrows(IllegalStateException.class, meta::getType);
        assertEquals("type unavailable for inserts", err.getMessage());
    }

    public void testUpdateEnablesGetters() {
        update();
        assertEquals("myRouting", meta.getRouting());
        assertEquals(Long.valueOf(5L), meta.getVersion());
        assertEquals("myType", meta.getType());
    }

    public void testInsertOps() {
        insert();
        List<String> valid = meta.validOps();
        assertEquals(3, valid.size());
        assertTrue(valid.contains("none"));
        assertTrue(valid.contains("noop"));
        assertTrue(valid.contains("create"));
    }

    public void testInsertSetOp() {
        insert();
        meta.setOp(Op.NOOP);
        assertEquals(Op.NOOP, meta.getOp());
        meta.setOp(Op.CREATE);
        assertEquals(Op.CREATE, meta.getOp());
        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> meta.setOp(Op.INDEX));
        assertEquals("unknown op [index], valid ops are none, noop, create", err.getMessage());
    }

    public void testInsertValidOps() {
        insert();
        List<String> valid = meta.validOps();
        assertEquals(3, valid.size());
        assertTrue(valid.contains("none"));
        assertTrue(valid.contains("noop"));
        assertTrue(valid.contains("create"));
    }

    public void testUpdateValidOps() {
        update();
        List<String> valid = meta.validOps();
        assertEquals(4, valid.size());
        assertTrue(valid.contains("none"));
        assertTrue(valid.contains("noop"));
        assertTrue(valid.contains("index"));
        assertTrue(valid.contains("delete"));
    }

    public void testUpdateSetOp() {
        update();
        meta.setOp(Op.NOOP);
        assertEquals(Op.NOOP, meta.getOp());
        meta.setOp(Op.INDEX);
        assertEquals(Op.INDEX, meta.getOp());
        meta.setOp(Op.DELETE);
        assertEquals(Op.DELETE, meta.getOp());
        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> meta.setOp(Op.CREATE));
        assertEquals("unknown op [create], valid ops are none, noop, index, delete", err.getMessage());
    }
}
