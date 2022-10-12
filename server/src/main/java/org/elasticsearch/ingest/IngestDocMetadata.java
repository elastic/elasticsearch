/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.script.Metadata;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

class IngestDocMetadata extends Metadata {

    static final Map<String, FieldProperty<?>> PROPERTIES = Map.of(
        INDEX,
        StringField.withWritable().withNullable(),
        ID,
        StringField.withWritable().withNullable(),
        ROUTING,
        StringField.withWritable().withNullable(),
        VERSION_TYPE,
        StringField.withWritable().withNullable().withValidation(IngestDocMetadata::versionTypeValidator),
        VERSION,
        LongField.withWritable(),
        TYPE,
        StringField.withNullable(),
        IF_SEQ_NO,
        LongField.withWritable().withNullable(),
        IF_PRIMARY_TERM,
        LongField.withWritable().withNullable(),
        DYNAMIC_TEMPLATES,
        new FieldProperty<>(Map.class).withWritable().withNullable()
    );

    protected final ZonedDateTime timestamp;

    IngestDocMetadata(String index, String id, long version, String routing, VersionType versionType, ZonedDateTime timestamp) {
        this(metadataMap(index, id, version, routing, versionType), timestamp);
    }

    IngestDocMetadata(Map<String, Object> metadata, ZonedDateTime timestamp) {
        this(metadata, PROPERTIES, timestamp);
    }

    IngestDocMetadata(Map<String, Object> metadata, Map<String, FieldProperty<?>> properties, ZonedDateTime timestamp) {
        super(metadata, properties);
        this.timestamp = timestamp;
    }

    /**
     * Create the backing metadata map with the standard contents assuming default validators.
     */
    protected static Map<String, Object> metadataMap(String index, String id, long version, String routing, VersionType versionType) {
        Map<String, Object> metadata = Maps.newHashMapWithExpectedSize(IngestDocument.Metadata.values().length);
        metadata.put(IngestDocument.Metadata.INDEX.getFieldName(), index);
        metadata.put(IngestDocument.Metadata.ID.getFieldName(), id);
        metadata.put(IngestDocument.Metadata.VERSION.getFieldName(), version);
        if (routing != null) {
            metadata.put(IngestDocument.Metadata.ROUTING.getFieldName(), routing);
        }
        if (versionType != null) {
            metadata.put(IngestDocument.Metadata.VERSION_TYPE.getFieldName(), VersionType.toString(versionType));
        }
        return metadata;
    }

    @Override
    public ZonedDateTime getNow() {
        return timestamp;
    }

    @Override
    public IngestDocMetadata clone() {
        return new IngestDocMetadata(map, timestamp);
    }

    private static void versionTypeValidator(String key, String value) {
        try {
            VersionType.fromString(value);
            return;
        } catch (IllegalArgumentException ignored) {}
        throw new IllegalArgumentException(
            key
                + " must be a null or one of ["
                + Arrays.stream(VersionType.values()).map(vt -> VersionType.toString(vt)).collect(Collectors.joining(", "))
                + "] but was ["
                + value
                + "] with type ["
                + value.getClass().getName()
                + "]"
        );
    }
}
