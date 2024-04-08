/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.ingest.IngestDocument;

import java.util.Map;
import java.util.Set;

/**
 * Metadata for the {@link UpdateByQueryMetadata} context.
 * _index, _id, _version, _routing are all read-only.  _routing is also nullable.
 * _now is millis since epoch and read-only.
 * op is read-write and one of 'index', 'noop', 'delete'
 */
public class UpdateByQueryMetadata extends Metadata {
    static final Map<String, FieldProperty<?>> PROPERTIES = Map.of(
        INDEX,
        StringField,
        ID,
        StringField,
        VERSION,
        LongField,
        ROUTING,
        StringField.withNullable(),
        OP,
        StringField.withWritable().withValidation(stringSetValidator(Set.of("noop", "index", "delete"))),
        NOW,
        LongField
    );

    public UpdateByQueryMetadata(String index, String id, long version, String routing, String op, long timestamp) {
        super(metadataMap(index, id, version, routing, op, timestamp), PROPERTIES);
    }

    /**
     * Create the backing metadata map with the standard contents assuming default validators.
     */
    protected static Map<String, Object> metadataMap(String index, String id, Long version, String routing, String op, long timestamp) {
        Map<String, Object> metadata = Maps.newHashMapWithExpectedSize(IngestDocument.Metadata.values().length);
        metadata.put(INDEX, index);
        metadata.put(ID, id);
        metadata.put(VERSION, version);
        metadata.put(ROUTING, routing);
        metadata.put(OP, op);
        metadata.put(NOW, timestamp);
        return metadata;
    }
}
