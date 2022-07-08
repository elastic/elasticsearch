/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.ingest.IngestDocument;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Ingest and update metadata available to write scripts
 */
public class Metadata {
    protected static final String INDEX = "_index";
    protected static final String ID = "_id";
    protected static final String ROUTING = "_routing";
    protected static final String VERSION_TYPE = "_version_type";
    protected static final String VERSION = "_version";
    protected static final String IF_SEQ_NO = "_if_seq_no";
    protected static final String IF_PRIMARY_TERM = "_if_primary_term";
    protected static final String DYNAMIC_TEMPLATES = "_dynamic_templates";

    protected static final Map<String, Validator> VALIDATORS = Map.of(
        INDEX,
        Metadata::stringValidator,
        ID,
        Metadata::stringValidator,
        ROUTING,
        Metadata::stringValidator,
        VERSION_TYPE,
        Metadata::stringValidator,
        VERSION,
        Metadata::notNullLongValidator,
        IF_SEQ_NO,
        Metadata::longValidator,
        IF_PRIMARY_TERM,
        Metadata::longValidator,
        DYNAMIC_TEMPLATES,
        Metadata::mapValidator
    );

    protected final Map<String, Object> map;
    protected final ZonedDateTime timestamp;
    protected final Map<String, Validator> validators;

    public Metadata(String index, String id, long version, String routing, VersionType versionType, ZonedDateTime timestamp) {
        this(metadataMap(index, id, version, routing, versionType), timestamp, VALIDATORS);
    }

    public Metadata(Map<String, Object> map, ZonedDateTime timestamp) {
        this(map, timestamp, VALIDATORS);
    }

    Metadata(Map<String, Object> map, ZonedDateTime timestamp, Map<String, Validator> validators) {
        this.map = map;
        this.timestamp = timestamp;
        this.validators = validators;
        validateMetadata();
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
     * Check that all metadata map contains only valid metadata and no extraneous keys and source map contains no metadata
     */
    protected void validateMetadata() {
        int numMetadata = 0;
        for (Map.Entry<String, Validator> entry : validators.entrySet()) {
            String key = entry.getKey();
            if (map.containsKey(key)) {
                numMetadata++;
            }
            entry.getValue().accept(MapOperation.INIT, key, map.get(key));
        }
        if (numMetadata < map.size()) {
            Set<String> keys = new HashSet<>(map.keySet());
            keys.removeAll(validators.keySet());
            throw new IllegalArgumentException(
                "Unexpected metadata keys [" + keys.stream().sorted().map(k -> k + ":" + map.get(k)).collect(Collectors.joining(", ")) + "]"
            );
        }
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
     * Get the String version of the value associated with {@code key}, or null
     */
    public String getString(String key) {
        return Objects.toString(get(key), null);
    }

    /**
     * Get the {@link Number} associated with key, or null
     * @throws IllegalArgumentException if the value is not a {@link Number}
     */
    public Number getNumber(String key) {
        Object value = get(key);
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number;
        }
        throw new IllegalArgumentException(
            "unexpected type for [" + key + "] with value [" + value + "], expected Number, got [" + value.getClass().getName() + "]"
        );
    }

    public boolean ownsKey(String key) {
        return validators.containsKey(key);
    }

    public Object put(String key, Object value) {
        Validator v = validators.getOrDefault(key, this::badKey);
        v.accept(MapOperation.UPDATE, key, value);
        return map.put(key, value);
    }

    public boolean containsKey(String key) {
        return map.containsKey(key);
    }

    public boolean containsValue(Object value) {
        return map.containsValue(value);
    }

    public Object get(String key) {
        return map.get(key);
    }

    public Object remove(String key) {
        Validator v = validators.getOrDefault(key, this::badKey);
        v.accept(MapOperation.REMOVE, key, null);
        return map.remove(key);
    }

    public List<String> keys() {
        return new ArrayList<>(map.keySet());
    }

    public int size() {
        return map.size();
    }

    @Override
    public Metadata clone() {
        return new Metadata(new HashMap<>(map), timestamp, new HashMap<>(validators));
    }

    public Map<String, Object> mapCopy() {
        // This is used for tests, can be removed when Metadata implements HashMap
        return new HashMap<>(map);
    }

    /**
     * Allow a String or null
     */
    protected static void stringValidator(MapOperation op, String key, Object value) {
        if (op == MapOperation.REMOVE || value == null || value instanceof String) {
            return;
        }
        throw new IllegalArgumentException(
            key + " must be null or a String but was [" + value + "] with type [" + value.getClass().getName() + "]"
        );
    }

    /**
     * Allow Numbers that can be represented as longs without loss of precision
     */
    public static void longValidator(MapOperation op, String key, Object value) {
        if (op == MapOperation.REMOVE || value == null) {
            return;
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

    protected static void notNullLongValidator(MapOperation op, String key, Object value) {
        if (op == MapOperation.REMOVE || value == null) {
            throw new IllegalArgumentException(key + " cannot be removed or set to null");
        }
        longValidator(op, key, value);
    }

    /**
     * Allow maps
     */
    public static void mapValidator(MapOperation op, String key, Object value) {
        if (op == MapOperation.REMOVE || value == null || value instanceof Map<?, ?>) {
            return;
        }
        throw new IllegalArgumentException(
            key + " must be a null or a Map but was [" + value + "] with type [" + value.getClass().getName() + "]"
        );
    }

    private void badKey(MapOperation op, String key, Object value) {
        throw new IllegalArgumentException(
            "unexpected metadata key ["
                + key
                + "], expected one of ["
                + validators.keySet().stream().sorted().collect(Collectors.joining(", "))
                + "]"
        );
    }

    /**
     * The operation being performed on the value in the map.
     * INIT: Initial value - the metadata value as passed into this class
     * UPDATE: the metadata is being set to a different value
     * REMOVE: the metadata mapping is being removed
     */
    public enum MapOperation {
        INIT,
        UPDATE,
        REMOVE
    }

    /**
     * A "TriConsumer" that tests if the {@link MapOperation}, the metadata key and value are valid.
     *
     * throws IllegalArgumentException if the given triple is invalid
     */
    @FunctionalInterface
    public interface Validator {
        void accept(MapOperation op, String key, Object value);
    }
}
