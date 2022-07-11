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
import org.elasticsearch.script.CtxMap;
import org.elasticsearch.script.Metadata;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Map containing ingest source and metadata.
 *
 * The Metadata values in {@link IngestDocument.Metadata} are validated when put in the map.
 * _index, _id and _routing must be a String or null
 * _version_type must be a lower case VersionType or null
 * _version must be representable as a long without loss of precision or null
 * _dyanmic_templates must be a map
 * _if_seq_no must be a long or null
 * _if_primary_term must be a long or null
 *
 * The map is expected to be used by processors, server code should the typed getter and setters where possible.
 */
class IngestCtxMap extends CtxMap {

    /**
     * Create an IngestCtxMap with the given metadata, source and default validators
     */
    IngestCtxMap(
        String index,
        String id,
        long version,
        String routing,
        VersionType versionType,
        ZonedDateTime timestamp,
        Map<String, Object> source
    ) {
        super(new HashMap<>(source), new IngestMetadata(index, id, version, routing, versionType, timestamp));
    }

    /**
     * Create IngestCtxMap from a source and metadata
     *
     * @param source the source document map
     * @param metadata the metadata map
     */
    IngestCtxMap(Map<String, Object> source, Metadata metadata) {
        super(source, metadata);
    }

    /**
     * Fetch the timestamp from the ingestMetadata, if it exists
     * @return the timestamp for the document or null
     */
    public static ZonedDateTime getTimestamp(Map<String, Object> ingestMetadata) {
        if (ingestMetadata == null) {
            return null;
        }
        Object ts = ingestMetadata.get(IngestDocument.TIMESTAMP);
        if (ts instanceof ZonedDateTime timestamp) {
            return timestamp;
        } else if (ts instanceof String str) {
            return ZonedDateTime.parse(str);
        }
        return null;
    }

    static class IngestMetadata extends Metadata {
        private static final FieldProperty<String> UPDATABLE_STRING = new FieldProperty<>(String.class, true, true, null);
        static final Map<String, FieldProperty<?>> PROPERTIES = Map.of(
            INDEX,
            UPDATABLE_STRING,
            ID,
            UPDATABLE_STRING,
            ROUTING,
            UPDATABLE_STRING,
            VERSION_TYPE,
            new FieldProperty<>(String.class, true, true, (k, v) -> {
                try {
                    VersionType.fromString(v);
                    return;
                } catch (IllegalArgumentException ignored) {}
                throw new IllegalArgumentException(
                    k
                        + " must be a null or one of ["
                        + Arrays.stream(VersionType.values()).map(vt -> VersionType.toString(vt)).collect(Collectors.joining(", "))
                        + "] but was ["
                        + v
                        + "] with type ["
                        + v.getClass().getName()
                        + "]"
                );
            }),
            VERSION,
            new FieldProperty<>(Number.class, false, true, FieldProperty.LONGABLE_NUMBER),
            TYPE,
            new FieldProperty<>(String.class, false, false, null),
            IF_SEQ_NO,
            new FieldProperty<>(Number.class, true, true, FieldProperty.LONGABLE_NUMBER),
            IF_PRIMARY_TERM,
            new FieldProperty<>(Number.class, true, true, FieldProperty.LONGABLE_NUMBER),
            DYNAMIC_TEMPLATES,
            new FieldProperty<>(Map.class, true, true, null)
        );

        protected final ZonedDateTime timestamp;

        IngestMetadata(String index, String id, long version, String routing, VersionType versionType, ZonedDateTime timestamp) {
            this(metadataMap(index, id, version, routing, versionType), timestamp);
        }

        IngestMetadata(Map<String, Object> metadata, ZonedDateTime timestamp) {
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
    }
}
