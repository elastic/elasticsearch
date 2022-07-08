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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link MapWrappable} that validates updates to the map using {@link Validator}s.
 */
public class MetadataMap implements MapWrappable {
    protected final Map<String, Object> map;
    protected final Map<String, Validator> validators;

    protected MetadataMap(Map<String, Object> map, Map<String, Validator> validators) {
        this.map = new HashMap<>(map);
        this.validators = validators;
        validateMetadata();
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
                "Unexpected metadata keys ["
                    + keys.stream().sorted().map(k -> k + ":" + map.get(k)).collect(Collectors.joining(", "))
                    + "]"
            );
        }
    }

    @Override
    public boolean isAuthoritative(String key) {
        return validators.containsKey(key);
    }

    @Override
    public Object put(String key, Object value) {
        Validator v = validators.get(key);
        if (v == null) {
            throw new IllegalArgumentException("cannot set unknown key [" + key + "] to [" + value + "]");
        }
        v.accept(MapOperation.UPDATE, key, value);
        return map.put(key, value);
    }

    @Override
    public boolean containsKey(String key) {
        return map.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return map.containsValue(value);
    }

    @Override
    public Object get(String key) {
        return map.get(key);
    }

    @Override
    public Object remove(String key) {
        Validator v = validators.get(key);
        if (v == null) {
            throw new IllegalArgumentException("cannot remove unknown key [" + key + "]");
        }
        v.accept(MapOperation.REMOVE, key, null);
        return map.remove(key);
    }

    @Override
    public List<String> keys() {
        return new ArrayList<>(map.keySet());
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public MapWrappable clone() {
        return new MetadataMap(new HashMap<>(map), new HashMap<>(validators));
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
    protected static void longValidator(MapOperation op, String key, Object value) {
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


    /**
     * Build a validator that checks that mapping is being removed, value is null or value is one of the valid set
     */
    protected static Validator setOrNullValidator(Set<String> valid) {
        return (o, k, v) ->
        {
            if (o == MapOperation.REMOVE || v == null) {
                return;
            }
            if (v instanceof String str) {
                if (valid.contains(str) == false) {
                    return;
                }
            }
            throw new IllegalArgumentException("Cannot set [" + k + "] to [" + v + "], need null or one of [" + valid.stream().sorted().collect(Collectors.joining(", ")) + "]");
        };
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
