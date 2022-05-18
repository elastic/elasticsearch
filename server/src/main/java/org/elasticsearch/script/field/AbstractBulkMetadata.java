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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Common operations for metadata updates done on bulk search operations: reindex and update by query */
public abstract class AbstractBulkMetadata implements BulkMetadata {
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

    protected final String index;
    protected final String id;
    protected final Long version;
    protected final String routing;
    protected final Op op;

    public AbstractBulkMetadata(String index, String id, Long version, String routing, Op op, Map<String, Object> source) {
        store = new MapBackedMetadata(6).setIndex(index).setId(id).setVersion(version).setRouting(routing).setOp(op).setSource(source);
        this.index = index;
        this.id = id;
        this.version = version;
        this.routing = routing;
        this.op = op;
    }

    public String getIndex() {
        return store.getIndex();
    }

    public boolean indexChanged() {
        return Objects.equals(index, getIndex()) == false;
    }

    public String getId() {
        return store.getId();
    }

    public boolean idChanged() {
        return Objects.equals(id, getId()) == false;
    }

    public String getRouting() {
        return store.getRouting();
    }

    public boolean routingChanged() {
        return Objects.equals(routing, getRouting()) == false;
    }

    public Long getVersion() {
        return store.getVersion();
    }

    public boolean versionChanged() {
        return Objects.equals(version, getVersion()) == false;
    }

    public Op getOp() {
        MapBackedMetadata.RawOp raw = store.getOp();
        if (raw.op == null) {
            throw new IllegalArgumentException("operation must be non-null, valid operations are " + Arrays.toString(VALID_OPS.toArray()));
        }
        if (VALID_OPS.contains(raw.op) == false) {
            throw new IllegalArgumentException(
                "Unsupported operation type [" + raw.str + "], must be one of " + Arrays.toString(VALID_OPS.toArray())
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
                "Unsupported operation type [" + op.name + "], must be one of " + Arrays.toString(VALID_OPS.toArray())
            );
        }
        store.setOp(op);
    }

    public boolean opChanged() {
        return Objects.equals(op, getOp()) == false;
    }

    public Map<String, Object> getCtx() {
        return store.getMap();
    }

    public List<String> extraKeys() {
        Set<String> keys = new HashSet<>(store.getMap().keySet());
        keys.removeAll(CTX_KEYS);
        return keys.stream().sorted().toList();
    }

    public Map<String, Object> getSource() {
        return store.getSource();
    }
}
