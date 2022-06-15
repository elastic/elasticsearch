/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.IndexFieldMapper;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.script.field.ValidatingMap;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Map containing ingest source and metadata.
 *
 * The Metadata values in {@link Metadata} are validated when put in the map.
 * _index, _id and _routing must be a String or null
 * _version_type must be a lower case VersionType or null
 * _version must be representable as a long without loss of precision and may not be null
 *
 * The map is expected to be used by processors where-as the typed getter and setters should
 * be used by server code where possible.
 */
public class IngestSourceAndMetadata extends ValidatingMap {

    private final ZonedDateTime timestamp;

    private static final Map<String, BiFunction<String, Object, Object>> VALIDATORS = Map.of(
        Metadata.INDEX.fieldName,
        IngestSourceAndMetadata::stringValidator,
        Metadata.ID.fieldName,
        IngestSourceAndMetadata::stringValidator,
        Metadata.ROUTING.fieldName,
        IngestSourceAndMetadata::stringValidator,
        Metadata.VERSION.fieldName,
        IngestSourceAndMetadata::longValidator,
        Metadata.VERSION_TYPE.fieldName,
        IngestSourceAndMetadata::versionTypeValidator
    );

    public IngestSourceAndMetadata(Map<String, Object> map, ZonedDateTime timestamp) {
        super(map, new HashMap<>(VALIDATORS));
        this.timestamp = timestamp;
    }

    // These are available to scripts

    public String getIndex() {
        return getString(Metadata.INDEX.fieldName);
    }

    public void setIndex(String index) {
        put(Metadata.INDEX.fieldName, index);
    }

    public String getId() {
        return getString(Metadata.ID.fieldName);
    }

    public void setId(String id) {
        put(Metadata.ID.fieldName, id);
    }

    public String getRouting() {
        return getString(Metadata.ROUTING.fieldName);
    }

    public void setRouting(String routing) {
        put(Metadata.ROUTING.fieldName, routing);
    }

    public String getVersionType() {
        return getString(Metadata.VERSION_TYPE.fieldName);
    }

    public void setVersionType(String versionType) {
        put(Metadata.VERSION_TYPE.fieldName, versionType);
    }

    public long getVersion() {
        Number version = getNumber(Metadata.VERSION.fieldName);
        assert version != null : Metadata.VERSION.fieldName + " validation allowed null version";
        return version.longValue();
    }

    public void setVersion(long version) {
        put(Metadata.VERSION.fieldName, version);
    }

    // timestamp isn't backed by the map
    public ZonedDateTime getTimestamp() {
        return timestamp;
    }

    // These are not available to scripts
    public Number getIfSeqNo() {
        return getNumber(Metadata.IF_SEQ_NO.fieldName);
    }

    public Number getIfPrimaryTerm() {
        return getNumber(Metadata.IF_PRIMARY_TERM.fieldName);
    }

    @SuppressWarnings("unchecked")
    public Map<String, String> getDynamicTemplates() {
        return (Map<String, String>) mapValidator(Metadata.DYNAMIC_TEMPLATES.fieldName, get(Metadata.DYNAMIC_TEMPLATES));
    }

    /**
     * Get Metadata value using the field name
     */
    public Object get(Metadata field) {
        return get(field.fieldName);
    }

    /**
     * One time operation that removes all metadata values and their validators and returns the underlying map, containing only source keys
     */
    public Map<String, Object> extractSource() {
        String fieldName;
        for (Metadata metadata : Metadata.values()) {
            fieldName = metadata.getFieldName();
            validators.remove(fieldName);
            remove(fieldName);
        }
        return map;
    }

    /**
     * Allow a String or null
     */
    protected static String stringValidator(String key, Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String str) {
            return str;
        }
        throw new IllegalArgumentException(
            key + " must be null or a String but was [" + value + "] with type [" + value.getClass().getName() + "]"
        );
    }

    /**
     * Allow Numbers that can be represented as longs without loss of precision
     */
    protected static Long longValidator(String key, Object value) {
        if (value == null) {
            throw new IllegalArgumentException(key + " must be non-null");
        }
        if (value instanceof Number number) {
            long version = number.longValue();
            if (number.doubleValue() != version) {
                // did we round?
                throw new IllegalArgumentException(
                    key + " may only be set to an int or a long but was [" + number + "] with type [" + value.getClass().getName() + "]"
                );
            }
            return version;
        }
        throw new IllegalArgumentException(
            "_version may only be set to an int or a long but was [" + value + "] with type [" + value.getClass().getName() + "]"
        );
    }

    /**
     * Allow lower case Strings that map to VersionType values, or null
     */
    protected static String versionTypeValidator(String key, Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String versionType) {
            VersionType.fromString(versionType);
            return versionType;
        }
        throw new IllegalArgumentException(
            key
                + " must be a null or one of ["
                + Arrays.stream(VersionType.values()).map(vt -> VersionType.toString(vt)).collect(Collectors.joining(","))
                + "]"
        );
    }

    /**
     * Allow maps
     */
    protected static Map<?, ?> mapValidator(String key, Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Map<?, ?> map) {
            return map;
        }
        throw new IllegalArgumentException(
            key + " must be a null or a Map but was [" + value + "] with type [" + value.getClass().getName() + "]"
        );
    }

    /**
     * Metadata keys that may be present in this map.
     */
    public enum Metadata {
        INDEX(IndexFieldMapper.NAME),
        TYPE("_type"),
        ID(IdFieldMapper.NAME),
        ROUTING(RoutingFieldMapper.NAME),
        VERSION(VersionFieldMapper.NAME),
        VERSION_TYPE("_version_type"),
        IF_SEQ_NO("_if_seq_no"),
        IF_PRIMARY_TERM("_if_primary_term"),
        DYNAMIC_TEMPLATES("_dynamic_templates");

        private static final Set<String> METADATA_NAMES = Arrays.stream(Metadata.values())
            .map(metadata -> metadata.fieldName)
            .collect(Collectors.toSet());

        private final String fieldName;

        Metadata(String fieldName) {
            this.fieldName = fieldName;
        }

        public static boolean isMetadata(String field) {
            return METADATA_NAMES.contains(field);
        }

        public String getFieldName() {
            return fieldName;
        }
    }
}
