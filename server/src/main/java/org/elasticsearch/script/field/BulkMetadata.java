/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.elasticsearch.common.util.Maps;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Common operations for metadata updates done on bulk search operations: reindex and update by query */
public class BulkMetadata extends Metadata {
    public static final String SOURCE = "_source";
    protected static final Set<String> CTX_KEYS = Set.of(INDEX, ID, VERSION, ROUTING, OP, SOURCE);
    protected static final EnumSet<Op> VALID_OPS = EnumSet.of(Op.NOOP, Op.INDEX, Op.DELETE);

    public BulkMetadata(String index, String id, Long version, String routing, Op op, Map<String, Object> source) {
        super(Maps.newMapWithExpectedSize(6));
        setIndex(index);
        setId(id);
        setVersion(version);
        setRouting(routing);
        setOp(op);
        ctx.put(SOURCE, source);
    }

    public Op getOp() {
        String opStr = getString(OP);
        if (opStr == null) {
            throw new IllegalArgumentException("Operation must be non-null, valid ops are " + Arrays.toString(VALID_OPS.toArray()));
        }
        Op op = Op.fromString(opStr);
        if (VALID_OPS.contains(op) == false) {
            throw new IllegalArgumentException(
                "Unsupported operation type [" + opStr + "], must be one of " + Arrays.toString(VALID_OPS.toArray())
            );
        }
        return op;
    }

    public void setOp(Op op) {
        if (op == null) {
            throw new IllegalArgumentException("operation must be non-null");
        }
        // Only valid ops are whitelisted, but check anyway
        if (VALID_OPS.contains(op) == false) {
            throw new IllegalArgumentException(
                "Unsupported operation type [" + op.name + "], must be one of " + Arrays.toString(VALID_OPS.toArray())
            );
        }
        ctx.put(OP, op.name);
    }

    public List<String> extraKeys() {
        Set<String> keys = new HashSet<>(ctx.keySet());
        keys.removeAll(CTX_KEYS);
        return keys.stream().sorted().toList();
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getSource() {
        Object source = ctx.get(SOURCE);
        if (source instanceof Map<?, ?> map) {
            return (Map<String, Object>) map;
        } else {
            throw new IllegalArgumentException("source should be a map, not [" + source.getClass().getName() + "]");
        }
    }

    public Map<String, Object> getCtx() {
        return ctx;
    }
}
