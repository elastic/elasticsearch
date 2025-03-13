/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Ingest and update metadata available to write scripts.
 *
 * Provides a map-like interface for backwards compatibility with the ctx map.
 *  - {@link #put(String, Object)}
 *  - {@link #get(String)}
 *  - {@link #remove(String)}
 *  - {@link #containsKey(String)}
 *  - {@link #containsValue(Object)}
 *  - {@link #keySet()} for iteration
 *  - {@link #size()}
 *  - {@link #isAvailable(String)} for determining if a key is a metadata key
 *
 * Provides getters and setters for script usage.
 *
 * Validates all updates whether originating in map-like interface or setters.
 */
public class Metadata {
    protected static final String INDEX = "_index";
    protected static final String ID = "_id";
    protected static final String ROUTING = "_routing";
    protected static final String VERSION_TYPE = "_version_type";
    protected static final String VERSION = "_version";
    protected static final String TYPE = "_type"; // type is deprecated, so it's supported in the map but not available as a getter
    protected static final String NOW = "_now";
    protected static final String OP = "op";
    protected static final String IF_SEQ_NO = "_if_seq_no";
    protected static final String IF_PRIMARY_TERM = "_if_primary_term";
    protected static final String DYNAMIC_TEMPLATES = "_dynamic_templates";

    public static final FieldProperty<Object> ObjectField = new FieldProperty<>(Object.class);
    public static FieldProperty<String> StringField = new FieldProperty<>(String.class);
    public static FieldProperty<Number> LongField = new FieldProperty<>(Number.class).withValidation(FieldProperty.LONGABLE_NUMBER);

    protected final Map<String, Object> map;
    private final Map<String, FieldProperty<?>> properties;
    protected static final FieldProperty<?> BAD_KEY = new FieldProperty<>(null, false, false, null);

    /**
     * Constructs a new Metadata object represented by the given map and properties.
     * <p>
     * The passed-in map is used directly -- subsequent modifications to it outside the methods of this class may result in
     * undefined behavior. Note also that mutation-like methods (e.g. setters, etc) on this class rely on the map being mutable,
     * which is the expected use for this class.
     * <p>
     * The properties map is used directly as well, but we verify at runtime that it <b>must</b> be an immutable map (i.e. constructed
     * via a call to {@link Map#of()} (or similar) in production, or via {@link Map#copyOf(Map)}} in tests). Since it must be an
     * immutable map, subsequent modifications are not possible.
     *
     * @param map the backing map for this metadata instance
     * @param properties the immutable map of defined properties for the type of metadata represented by this instance
     */
    @SuppressWarnings("this-escape")
    protected Metadata(Map<String, Object> map, Map<String, FieldProperty<?>> properties) {
        this.map = map;
        // we can't tell the compiler that properties must be a java.util.ImmutableCollections.AbstractImmutableMap, but
        // we can use this copyOf + assert to verify that at runtime.
        this.properties = Map.copyOf(properties);
        assert this.properties == properties : "properties map must be constructed via Map.of(...) or Map.copyOf(...)";
        validateMetadata();
    }

    // a 'not found' sentinel value for use in validateMetadata below
    private static final Object NOT_FOUND = new Object();

    /**
     * Check that all metadata map contains only valid metadata and no extraneous keys
     */
    protected void validateMetadata() {
        int numMetadata = 0;
        for (Map.Entry<String, FieldProperty<?>> entry : properties.entrySet()) {
            String key = entry.getKey();
            Object value = map.getOrDefault(key, NOT_FOUND); // getOrDefault is faster than containsKey + get
            if (value == NOT_FOUND) {
                // check whether it's permissible to *not* have a value for the property
                entry.getValue().check(MapOperation.INIT, key, null);
            } else {
                numMetadata++;
                entry.getValue().check(MapOperation.INIT, key, value);
            }
        }
        if (numMetadata < map.size()) {
            Set<String> keys = new HashSet<>(map.keySet());
            keys.removeAll(properties.keySet());
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

    public ZonedDateTime getNow() {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(getNumber(NOW).longValue()), ZoneOffset.UTC);
    }

    public String getOp() {
        return getString(OP);
    }

    public void setOp(String op) {
        put(OP, op);
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
    protected String getString(String key) {
        return Objects.toString(get(key), null);
    }

    /**
     * Get the {@link Number} associated with key, or null
     * @throws IllegalArgumentException if the value is not a {@link Number}
     */
    protected Number getNumber(String key) {
        Object value = get(key);
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number;
        }
        throw new IllegalStateException(
            "unexpected type for [" + key + "] with value [" + value + "], expected Number, got [" + value.getClass().getName() + "]"
        );
    }

    /**
     * Is this key a Metadata key?  A {@link #remove}d key would return false for {@link #containsKey(String)} but true for
     * this call.
     */
    public boolean isAvailable(String key) {
        return properties.containsKey(key);
    }

    /**
     * Create the mapping from key to value.
     * @throws IllegalArgumentException if {@link #isAvailable(String)} is false or the key cannot be updated to the value.
     */
    public Object put(String key, Object value) {
        properties.getOrDefault(key, Metadata.BAD_KEY).check(MapOperation.UPDATE, key, value);
        return map.put(key, value);
    }

    /**
     * Does the metadata contain the key?
     */
    public boolean containsKey(String key) {
        return map.containsKey(key);
    }

    /**
     * Does the metadata contain the value.
     */
    public boolean containsValue(Object value) {
        return map.containsValue(value);
    }

    /**
     * Get the value associated with {@param key}
     */
    public Object get(String key) {
        return map.get(key);
    }

    /**
     * Get the value associated with {@param key}, otherwise return {@param defaultValue}
     */
    public Object getOrDefault(String key, Object defaultValue) {
        return map.getOrDefault(key, defaultValue);
    }

    /**
     * Remove the mapping associated with {@param key}
     * @throws IllegalArgumentException if {@link #isAvailable(String)} is false or the key cannot be removed.
     */
    public Object remove(String key) {
        properties.getOrDefault(key, Metadata.BAD_KEY).check(MapOperation.REMOVE, key, null);
        return map.remove(key);
    }

    /**
     * Return the list of keys with mappings
     */
    public Set<String> keySet() {
        return Collections.unmodifiableSet(map.keySet());
    }

    /**
     * The number of metadata keys currently mapped.
     */
    public int size() {
        return map.size();
    }

    @Override
    public Metadata clone() {
        // properties is an unmodifiable map, no need to create a copy here
        return new Metadata(new HashMap<>(map), properties);
    }

    /**
     * Get the backing map, if modified then the guarantees of this class may not hold
     */
    public Map<String, Object> getMap() {
        return map;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o instanceof Metadata) == false) return false;
        Metadata metadata = (Metadata) o;
        return map.equals(metadata.map);
    }

    @Override
    public int hashCode() {
        return Objects.hash(map);
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
     * The properties of a metadata field.
     * @param type - the class of the field.  Updates must be assignable to this type.  If null, no type checking is performed.
     * @param nullable - can the field value be null and can it be removed
     * @param writable - can the field be updated after the initial set
     * @param extendedValidation - value validation after type checking, may be used for values that may be one of a set
     */
    public record FieldProperty<T>(Class<T> type, boolean nullable, boolean writable, BiConsumer<String, T> extendedValidation) {

        public FieldProperty(Class<T> type) {
            this(type, false, false, null);
        }

        public FieldProperty<T> withNullable() {
            if (nullable) {
                return this;
            }
            return new FieldProperty<>(type, true, writable, extendedValidation);
        }

        public FieldProperty<T> withWritable() {
            if (writable) {
                return this;
            }
            return new FieldProperty<>(type, nullable, true, extendedValidation);
        }

        public FieldProperty<T> withValidation(BiConsumer<String, T> extendedValidation) {
            if (this.extendedValidation == extendedValidation) {
                return this;
            }
            return new FieldProperty<>(type, nullable, writable, extendedValidation);
        }

        public static final BiConsumer<String, Number> LONGABLE_NUMBER = (k, v) -> {
            long version = v.longValue();
            // did we round?
            if (v.doubleValue() == version) {
                return;
            }
            throw new IllegalArgumentException(
                k + " may only be set to an int or a long but was [" + v + "] with type [" + v.getClass().getName() + "]"
            );
        };

        public static final FieldProperty<?> ALLOW_ALL = new FieldProperty<>(null, true, true, null);

        @SuppressWarnings("fallthrough")
        public void check(MapOperation op, String key, Object value) {
            switch (op) {
                case UPDATE:
                    if (writable == false) {
                        throw new IllegalArgumentException(key + " cannot be updated");
                    }
                    // fall through

                case INIT:
                    if (value == null) {
                        if (nullable == false) {
                            throw new IllegalArgumentException(key + " cannot be null");
                        }
                    } else {
                        checkType(key, value);
                    }
                    break;

                case REMOVE:
                    if (writable == false || nullable == false) {
                        throw new IllegalArgumentException(key + " cannot be removed");
                    }
                    break;

                default:
                    throw new IllegalArgumentException("unexpected op [" + op + "] for key [" + key + "] and value [" + value + "]");

            }
        }

        @SuppressWarnings("unchecked")
        private void checkType(String key, Object value) {
            if (type == null) {
                return;
            }
            if (type.isAssignableFrom(value.getClass()) == false) {
                throw new IllegalArgumentException(
                    key
                        + " ["
                        + value
                        + "] is wrong type, expected assignable to ["
                        + type.getName()
                        + "], not ["
                        + value.getClass().getName()
                        + "]"
                );
            }
            if (extendedValidation != null) {
                extendedValidation.accept(key, (T) value);
            }
        }
    }

    public static BiConsumer<String, String> stringSetValidator(Set<String> valid) {
        return (k, v) -> {
            if (valid.contains(v) == false) {
                throw new IllegalArgumentException(
                    "[" + k + "] must be one of " + valid.stream().sorted().collect(Collectors.joining(", ")) + ", not [" + v + "]"
                );
            }
        };
    }
}
