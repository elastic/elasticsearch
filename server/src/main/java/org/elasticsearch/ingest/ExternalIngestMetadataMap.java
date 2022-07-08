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
import org.elasticsearch.script.MetadataMap;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Ingest metadata available to user code.
 *
 * The Metadata values in {@link IngestDocument.Metadata} are validated when updated.
 * _index, _id and _routing must be a String or null
 * _version_type must be a lower case VersionType or null
 * _version must be representable as a long without loss of precision or null
 * _dyanmic_templates must be a map
 * _if_seq_no must be a long or null
 * _if_primary_term must be a long or null
 *
 * The map is expected to be used by processors, server code should the typed getter and setters where possible.
 */
class ExternalIngestMetadataMap extends MetadataMap {
    protected static final String INDEX = "_index";
    protected static final String ID = "_id";
    protected static final String ROUTING = "_routing";
    protected static final String VERSION_TYPE = "_version_type";
    protected static final String VERSION = "_version";
    protected static final String IF_SEQ_NO = "_if_seq_no";
    protected static final String IF_PRIMARY_TERM = "_if_primary_term";
    protected static final String DYNAMIC_TEMPLATES = "_dynamic_templates";

    static final Set<String> VERSION_TYPE_VALUES = Arrays.stream(VersionType.values())
        .map(vt -> VersionType.toString(vt))
        .collect(Collectors.toSet());

    protected static final Map<String, Validator> VALIDATORS = Map.of(
        INDEX,
        MetadataMap::stringValidator,
        ID,
        MetadataMap::stringValidator,
        ROUTING,
        MetadataMap::stringValidator,
        VERSION_TYPE,
        setOrNullValidator(VERSION_TYPE_VALUES),
        VERSION,
        ExternalIngestMetadataMap::nonNullLongValidator,
        IF_SEQ_NO,
        MetadataMap::longValidator,
        IF_PRIMARY_TERM,
        MetadataMap::longValidator,
        DYNAMIC_TEMPLATES,
        ExternalIngestMetadataMap::mapValidator
    );

    protected final ZonedDateTime timestamp;

    ExternalIngestMetadataMap(String index, String id, long version, String routing, VersionType versionType, ZonedDateTime timestamp) {
        this(metadataMap(index, id, version, routing, versionType), timestamp, VALIDATORS);
    }

    ExternalIngestMetadataMap(Map<String, Object> map, ZonedDateTime timestamp, Map<String, Validator> validators) {
        super(map, validators);
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

    // These are available to scripts
    public String getIndex() {
        return getString(INDEX);
    }

    public void setIndex(String index) {
        put(INDEX, index);
    }

    public String getId() {
        return getString(ID);
    }

    public void setId(String id) {
        put(ID, id);
    }

    public String getRouting() {
        return getString(ROUTING);
    }

    public void setRouting(String routing) {
        put(ROUTING, routing);
    }

    public String getVersionType() {
        return getString(VERSION_TYPE);
    }

    public void setVersionType(String versionType) {
        put(VERSION_TYPE, versionType);
    }

    public long getVersion() {
        return getNumber(VERSION).longValue();
    }

    public void setVersion(long version) {
        put(VERSION, version);
    }

    public ZonedDateTime getTimestamp() {
        return timestamp;
    }

    // These are not available to scripts
    public Number getIfSeqNo() {
        return getNumber(IF_SEQ_NO);
    }

    public Number getIfPrimaryTerm() {
        return getNumber(IF_PRIMARY_TERM);
    }

    @SuppressWarnings("unchecked")
    public Map<String, String> getDynamicTemplates() {
        return (Map<String, String>) get(DYNAMIC_TEMPLATES);
    }

    /**
     * Allow maps
     */
    public static void mapValidator(MetadataMap.MapOperation op, String key, Object value) {
        if (op == MetadataMap.MapOperation.REMOVE || value == null || value instanceof Map<?, ?>) {
            return;
        }
        throw new IllegalArgumentException(
            key + " must be a null or a Map but was [" + value + "] with type [" + value.getClass().getName() + "]"
        );
    }

    protected static void nonNullLongValidator(MapOperation op, String key, Object value) {
        if (op == MapOperation.REMOVE || value == null) {
            throw new IllegalArgumentException("cannot remove or set [" + key + "] to null");
        }
        if (value instanceof Number number) {
            long version = number.longValue();
            // did we round?
            if (number.doubleValue() == version) {
                return;
            }
        }
        throw new IllegalArgumentException(
            key + " may only be set to an int or a long but was [" + value + "] with type [" + value.getClass().getName() + "]"
        );
    }

    @SuppressWarnings("unchecked")
    protected static Map<String, String> toMapOrNull(String key, Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Map<?, ?>) {
            return (Map<String, String>) value;
        }
        throw new IllegalArgumentException(
            key + " must be a null or a Map but was [" + value + "] with type [" + value.getClass().getName() + "]"
        );
    }

}
