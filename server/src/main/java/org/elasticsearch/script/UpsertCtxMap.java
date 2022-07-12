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

/**
 * Metadata for insert via upsert in the Update context
 */
public class UpsertCtxMap extends UpdateCtxMap {
    public UpsertCtxMap(String index, String id, String op, long timestamp, Map<String, Object> source) {
        super(source, new UpsertMetadata(index, id, op, timestamp));
    }

    static class UpsertMetadata extends UpdateMetadata {
        protected static final Set<String> VALID_UPSERT_OPS = Set.of("noop", "create", LEGACY_NOOP_STRING);

        static final Map<String, FieldProperty<?>> PROPERTIES = Map.of(
            INDEX,
            SET_ONCE_STRING,
            ID,
            SET_ONCE_STRING,
            OP,
            new FieldProperty<>(String.class, true, true, setValidator(VALID_UPSERT_OPS)),
            TIMESTAMP,
            SET_ONCE_LONG
        );

        UpsertMetadata(String index, String id, String op, long timestamp) {
            super(metadataMap(index, id, op, timestamp), PROPERTIES);
        }

        protected static Map<String, Object> metadataMap(String index, String id, String op, long timestamp) {
            Map<String, Object> metadata = Maps.newHashMapWithExpectedSize(PROPERTIES.size());
            metadata.put(INDEX, index);
            metadata.put(ID, id);
            metadata.put(OP, op);
            metadata.put(TIMESTAMP, timestamp);
            return metadata;
        }

        @Override
        public String getRouting() {
            throw new IllegalArgumentException("routing is unavailable for insert");
        }

        @Override
        public long getVersion() {
            throw new IllegalArgumentException("version is unavailable for insert");
        }
    }
}
