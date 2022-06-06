/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/** Common operations for metadata updates done on bulk search operations: reindex and update by query */
public abstract class BulkMetadata {
    public static final String SOURCE = "_source";
    protected MapBackedMetadata store;
    protected static final Set<String> CTX_KEYS = Set.of(
        MapBackedMetadata.INDEX,
        MapBackedMetadata.ID,
        MapBackedMetadata.VERSION,
        MapBackedMetadata.ROUTING,
        MapBackedMetadata.OP,
        MapBackedMetadata.SOURCE
    );
    protected static final EnumSet<Op> VALID_OPS = EnumSet.of(Op.NOOP, Op.INDEX, Op.DELETE);

    public BulkMetadata(String index, String id, Long version, String routing, Op op, Map<String, Object> source) {
        store = new MapBackedMetadata(6).setIndex(index).setId(id).setVersion(version).setRouting(routing).setOp(op).setSource(source);
    }

    public String getIndex() {
        return store.getIndex();
    }

    public String getId() {
        return store.getId();
    }

    public String getRouting() {
        return store.getRouting();
    }

    public boolean hasVersion() {
        return store.getRawVersion() != null;
    }

    public long getVersion() {
        Long version = store.getVersion();
        if (version == null) {
            return Long.MIN_VALUE;
        }
        return version;
    }

    public Op getOp() {
        MapBackedMetadata.RawOp raw = store.getOp();
        if (raw.str == null) {
            throw new IllegalArgumentException("operation must be non-null, valid operations are " + Arrays.toString(VALID_OPS.toArray()));
        }
        if (VALID_OPS.contains(raw.op) == false) {
            throw new IllegalArgumentException(
                "Operation type [" + raw.str + "] not allowed, only " + Arrays.toString(VALID_OPS.toArray()) + " are allowed"
            );
        }
        return raw.op;
    }

    public void setOp(Op op) {
        if (op == null) {
            throw new IllegalArgumentException("operation must be non-null, valid operations are " + Arrays.toString(VALID_OPS.toArray()));
        }
        if (VALID_OPS.contains(op) == false) {
            throw new IllegalArgumentException(
                "Operation type [" + op.name + "] not allowed, only " + Arrays.toString(VALID_OPS.toArray()) + " are allowed"
            );
        }
        store.setOp(op);
    }

    public Map<String, Object> getCtx() {
        return store.getMap();
    }

    public Map<String, Object> getSource() {
        return store.getSource();
    }
}
