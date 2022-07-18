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
import java.util.function.BiConsumer;
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

    protected static final FieldProperty<String> SET_ONCE_STRING = new FieldProperty<>(String.class, true, false, null);

    protected static final FieldProperty<Number> SET_ONCE_LONG = new FieldProperty<>(
        Number.class,
        false,
        false,
        FieldProperty.LONGABLE_NUMBER
    );

    protected static final Set<String> VALID_UPDATE_OPS = Set.of("noop", "index", "delete", LEGACY_NOOP_STRING);

    static final Map<String, FieldProperty<?>> PROPERTIES = Map.of(
        INDEX,
        SET_ONCE_STRING,
        ID,
        SET_ONCE_STRING,
        VERSION,
        SET_ONCE_LONG,
        ROUTING,
        SET_ONCE_STRING,
        TYPE,
        SET_ONCE_STRING,
        OP,
        new FieldProperty<>(String.class, true, true, stringSetValidator(VALID_UPDATE_OPS)),
        TIMESTAMP,
        SET_ONCE_LONG
    );

    protected static BiConsumer<String, String> stringSetValidator(Set<String> valid) {
        return (k, v) -> {
            if (valid.contains(v) == false) {
                throw new IllegalArgumentException(
                    "[" + k + "] must be one of " + valid.stream().sorted().collect(Collectors.joining(", ")) + ", not [" + v + "]"
                );
            }
        };
    }

    public UpdateMetadata(String index, String id, long version, String routing, String type, String op, long timestamp) {
        this(metadataMap(index, id, version, routing, type, op, timestamp), PROPERTIES);
    }

    protected UpdateMetadata(Map<String, Object> metadata, Map<String, FieldProperty<?>> properties) {
        super(metadata, properties);
    }

    protected static Map<String, Object> metadataMap(
        String index,
        String id,
        long version,
        String routing,
        String type,
        String op,
        long timestamp
    ) {
        Map<String, Object> metadata = Maps.newHashMapWithExpectedSize(PROPERTIES.size());
        metadata.put(INDEX, index);
        metadata.put(ID, id);
        metadata.put(VERSION, version);
        metadata.put(ROUTING, routing);
        metadata.put(TYPE, type);
        metadata.put(OP, op);
        metadata.put(TIMESTAMP, timestamp);
        return metadata;
    }

    @Override
    public String getOp() {
        String op = super.getOp();
        if (LEGACY_NOOP_STRING.equals(op) || op == null) {
            // UpdateHelper.UpdateOpType.lenientFromString allows anything into the map but UpdateMetadata limits
            // it to VALID_UPDATE_OPS
            return "noop";
        }
        return op;
    }

    @Override
    public void setOp(String op) {
        if (LEGACY_NOOP_STRING.equals(op)) {
            throw new IllegalArgumentException(LEGACY_NOOP_STRING + " is deprecated, use 'noop' instead");
        }
        super.setOp(op);
    }
}
