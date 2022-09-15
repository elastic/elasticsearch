/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.common.util.Maps;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The update context has read-only metadata:
 * _index, _id, _version, _routing, _type (always '_doc'), _now (timestamp in millis)
 * and read-write op that may be one of 'noop' or 'none' (legacy), 'index', 'delete' or null
 */
public class UpdateMetadata extends Metadata {
    // AbstractAsyncBulkByScrollAction.OpType uses 'noop' rather than 'none', so unify on 'noop' but allow 'none' in
    // the ctx map
    protected static final String LEGACY_NOOP_STRING = "none";

    static final Map<String, FieldProperty<?>> PROPERTIES = Map.of(
        INDEX,
        StringField.withNullable(),
        ID,
        StringField.withNullable(),
        VERSION,
        LongField,
        ROUTING,
        StringField.withNullable(),
        TYPE,
        StringField.withNullable(),
        OP,
        StringField.withWritable().withNullable(),
        NOW,
        LongField
    );

    protected final Set<String> validOps;

    public UpdateMetadata(String index, String id, long version, String routing, String type, String op, long now) {
        this(metadataMap(index, id, version, routing, type, op, now), Set.of("noop", "index", "delete"), PROPERTIES);
    }

    protected UpdateMetadata(Map<String, Object> metadata, Set<String> validOps, Map<String, FieldProperty<?>> properties) {
        super(metadata, properties);
        this.validOps = validOps;
    }

    protected static Map<String, Object> metadataMap(
        String index,
        String id,
        long version,
        String routing,
        String type,
        String op,
        long now
    ) {
        Map<String, Object> metadata = Maps.newHashMapWithExpectedSize(PROPERTIES.size());
        metadata.put(INDEX, index);
        metadata.put(ID, id);
        metadata.put(VERSION, version);
        metadata.put(ROUTING, routing);
        metadata.put(TYPE, type);
        metadata.put(OP, op);
        metadata.put(NOW, now);
        return metadata;
    }

    @Override
    public String getOp() {
        String op = super.getOp();
        if (LEGACY_NOOP_STRING.equals(op) || op == null || validOps.contains(op) == false) {
            // UpdateHelper.UpdateOpType.lenientFromString treats all invalid ops as "noop"
            return "noop";
        }
        return op;
    }

    @Override
    public void setOp(String op) {
        // Due to existing leniency, we cannot rely on the map validator, so we must do validation here.
        if (LEGACY_NOOP_STRING.equals(op)) {
            throw new IllegalArgumentException("'" + LEGACY_NOOP_STRING + "' is not allowed, use 'noop' instead");
        } else if (op == null || validOps.contains(op) == false) {
            throw new IllegalArgumentException(
                "op must be one of [" + validOps.stream().sorted().collect(Collectors.joining(", ")) + "], not [" + op + "]"
            );
        }
        super.setOp(op);
    }
}
