/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

public class UpdateByQueryMetadataTests extends ESTestCase {
    UpdateByQueryMetadata meta;

    public void testROFields() {
        meta = new UpdateByQueryMetadata("myIndex", "myId", 5, "myRouting", "index", 12345000);
        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> meta.put("_index", "something"));
        assertEquals("_index cannot be updated", err.getMessage());
        err = expectThrows(IllegalArgumentException.class, () -> meta.put("_id", "something"));
        assertEquals("_id cannot be updated", err.getMessage());
        err = expectThrows(IllegalArgumentException.class, () -> meta.put("_version", 600));
        assertEquals("_version cannot be updated", err.getMessage());
        err = expectThrows(IllegalArgumentException.class, () -> meta.put("_routing", "something"));
        assertEquals("_routing cannot be updated", err.getMessage());
        err = expectThrows(IllegalArgumentException.class, () -> meta.put("_now", 9000));
        assertEquals("_now cannot be updated", err.getMessage());
    }

    public void testOpSet() {
        meta = new UpdateByQueryMetadata("myIndex", "myId", 5, "myRouting", "index", 12345000);
        for (String op : List.of("noop", "index", "delete")) {
            meta.setOp(op);
            meta.put("op", op);
        }
        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> meta.put("op", "bad"));
        assertEquals("[op] must be one of delete, index, noop, not [bad]", err.getMessage());
        err = expectThrows(IllegalArgumentException.class, () -> meta.setOp("bad"));
        assertEquals("[op] must be one of delete, index, noop, not [bad]", err.getMessage());
    }
}
