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
public class UpsertSourceAndMetadata extends UpdateSourceAndMetadata {
    protected static final Set<String> VALID_OPS = Set.of("noop", "create", LEGACY_NOOP_STRING);

    public static Map<String, Validator> VALIDATORS = Map.of(
        INDEX,
        UpdateSourceAndMetadata::setOnceStringValidator,
        ID,
        UpdateSourceAndMetadata::setOnceStringValidator,
        OP,
        opValidatorFromValidOps(VALID_OPS),
        TIMESTAMP,
        UpdateSourceAndMetadata::setOnceLongValidator
    );

    public UpsertSourceAndMetadata(String index, String id, String op, long timestamp, Map<String, Object> source) {
        super(source, metadataMap(index, id, op, timestamp), VALIDATORS);
    }

    protected static Map<String, Object> metadataMap(String index, String id, String op, long timestamp) {
        Map<String, Object> metadata = Maps.newHashMapWithExpectedSize(VALIDATORS.size());
        metadata.put(INDEX, index);
        metadata.put(ID, id);
        metadata.put(OP, op);
        metadata.put(TIMESTAMP, timestamp);
        return metadata;
    }

    @Override
    public String getRouting() {
        throw new IllegalStateException("routing is unavailable for insert");
    }

    @Override
    public long getVersion() {
        throw new IllegalStateException("version is unavailable for insert");
    }

    @Override
    public boolean hasVersion() {
        throw new IllegalStateException("version is unavailable for insert");
    }
}
