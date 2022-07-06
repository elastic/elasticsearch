/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.script.SourceAndMetadataMap;

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
class IngestSourceAndMetadata extends SourceAndMetadataMap {
    public static final String VERSION_TYPE = "_version_type";
    public static final String IF_SEQ_NO = "_if_seq_no";
    public static final String IF_PRIMARY_TERM = "_if_primary_term";
    public static final String DYNAMIC_TEMPLATES = "_dynamic_templates";

    protected final ZonedDateTime timestamp;

    /**
     * map of key to validating function. Should throw {@link IllegalArgumentException} on invalid value
     */
    protected static final Map<String, SourceAndMetadataMap.Validator> VALIDATORS = Map.of(
        INDEX,
        SourceAndMetadataMap::stringValidator,
        ID,
        SourceAndMetadataMap::stringValidator,
        ROUTING,
        SourceAndMetadataMap::stringValidator,
        VERSION,
        SourceAndMetadataMap::nonNullLongValidator,
        VERSION_TYPE,
        IngestSourceAndMetadata::versionTypeValidator,
        DYNAMIC_TEMPLATES,
        SourceAndMetadataMap::mapValidator,
        IF_SEQ_NO,
        SourceAndMetadataMap::longValidator,
        IF_PRIMARY_TERM,
        SourceAndMetadataMap::longValidator,
        TYPE,
        SourceAndMetadataMap::stringValidator
    );

    /**
     * Create an IngestSourceAndMetadata with the given metadata, source and default validators
     */
    IngestSourceAndMetadata(
        String index,
        String id,
        long version,
        String routing,
        VersionType versionType,
        ZonedDateTime timestamp,
        Map<String, Object> source
    ) {
        this(new HashMap<>(source), metadataMap(index, id, version, routing, versionType), timestamp, VALIDATORS);
    }

    /**
     * Create IngestSourceAndMetadata with custom validators.
     *
     * @param source the source document map
     * @param metadata the metadata map
     * @param timestamp the time of ingestion
     * @param validators validators to run on metadata map, if a key is in this map, the value is stored in metadata.
     *                   if null, use the default validators from {@link #VALIDATORS}
     */
    protected IngestSourceAndMetadata(
        Map<String, Object> source,
        Map<String, Object> metadata,
        ZonedDateTime timestamp,
        Map<String, Validator> validators
    ) {
        super(
            source != null ? source : new HashMap<>(),
            metadata != null ? metadata : new HashMap<>(),
            validators != null ? validators : VALIDATORS
        );
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

    /**
     * Returns a new metadata map and the existing source map with metadata removed.
     */
    public static Tuple<Map<String, Object>, Map<String, Object>> splitSourceAndMetadata(Map<String, Object> sourceAndMetadata) {
        if (sourceAndMetadata instanceof IngestSourceAndMetadata ingestSourceAndMetadata) {
            return new Tuple<>(new HashMap<>(ingestSourceAndMetadata.source), new HashMap<>(ingestSourceAndMetadata.metadata));
        }
        Map<String, Object> metadata = Maps.newHashMapWithExpectedSize(VALIDATORS.size());
        Map<String, Object> source = new HashMap<>(sourceAndMetadata);
        for (String metadataName : VALIDATORS.keySet()) {
            if (sourceAndMetadata.containsKey(metadataName)) {
                metadata.put(metadataName, source.remove(metadataName));
            }
        }
        return new Tuple<>(source, metadata);
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

    // timestamp isn't backed by the map
    public ZonedDateTime getTimestamp() {
        return timestamp;
    }

    public long getVersion() {
        Number version = getNumber(VERSION);
        assert version != null : VERSION + " validation allowed null version";
        return version.longValue();
    }

    @Override
    public String getVersionType() {
        return getString(VERSION_TYPE);
    }

    @Override
    public void setVersionType(String versionType) {
        put(VERSION_TYPE, versionType);
    }

    @Override
    public String getType() {
        return getString(TYPE);
    }

    public Number getIfSeqNo() {
        return getNumber(IF_SEQ_NO);
    }

    public Number getIfPrimaryTerm() {
        return getNumber(IF_PRIMARY_TERM);
    }

    @SuppressWarnings("unchecked")
    public Map<String, String> getDynamicTemplates() {
        return (Map<String, String>) metadata.get(DYNAMIC_TEMPLATES);
    }

    public Map<String, Validator> getValidators() {
        return validators;
    }

    /**
     * Allow lower case Strings that map to VersionType values, or null
     */
    public static void versionTypeValidator(MapOperation op, String key, Object value) {
        if (op == MapOperation.REMOVE || value == null) {
            return;
        }
        if (value instanceof String versionType) {
            try {
                VersionType.fromString(versionType);
                return;
            } catch (IllegalArgumentException ignored) {}
        }
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
