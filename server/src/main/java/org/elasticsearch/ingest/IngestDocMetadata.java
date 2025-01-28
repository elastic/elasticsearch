/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.script.Metadata;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

final class IngestDocMetadata extends Metadata {

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

    private static final char UNDERSCORE = '_';
    static {
        // there's an optimization here in the overridden isAvailable below, but it only works if the first character of each of these
        // keys starts with an underscore, since we know all the keys up front, though, we can just make sure that's always true
        for (String key : PROPERTIES.keySet()) {
            if (key.charAt(0) != UNDERSCORE) {
                throw new IllegalArgumentException("IngestDocMetadata keys must begin with an underscore, but found [" + key + "]");
            }
        }
    }

    protected final ZonedDateTime timestamp;

    IngestDocMetadata(String index, String id, long version, String routing, VersionType versionType, ZonedDateTime timestamp) {
        this(metadataMap(index, id, version, routing, versionType), timestamp);
    }

    IngestDocMetadata(Map<String, Object> metadata, ZonedDateTime timestamp) {
        super(metadata, PROPERTIES);
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
        return new IngestDocMetadata(new HashMap<>(map), timestamp);
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

    @Override
    public boolean isAvailable(String key) {
        // the key cannot be null or empty because of the nature of the calling code, and this is already validated in IngestDocument
        assert key != null && key.isEmpty() == false;
        // we can avoid a map lookup on most keys since we know that the only keys that are 'metadata keys' for an ingest document
        // must be keys that start with an underscore
        if (key.charAt(0) != UNDERSCORE) {
            return false;
        }
        return super.isAvailable(key);
    }
}
