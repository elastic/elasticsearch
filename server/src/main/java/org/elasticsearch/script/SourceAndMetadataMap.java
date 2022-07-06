/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Container that holds the source and metadata for write scripts.  Acts like a map for backwards compatibilty with
 * ctx and implements {@link Metadata} so that scripts can use the newer API for reading and writing metadata values.
 *
 * Keeps two maps, a {@link #metadata} map for metadata mappings and a {@link #source} map for all other mappings.
 * A mapping belongs in metadata if and only if there is a {@link Validator} for the mapping.
 *
 * Validators ensure modifications to the metadata map are always valid so getters are exception free.
 */
public abstract class SourceAndMetadataMap extends AbstractMap<String, Object> implements Metadata {
    public static final String INDEX = "_index";
    public static final String ID = "_id";
    public static final String TYPE = "_type";
    public static final String ROUTING = "_routing";
    public static final String VERSION = "_version";

    protected final Map<String, Validator> validators;
    protected final Map<String, Object> source;
    protected final Map<String, Object> metadata;

    protected SourceAndMetadataMap(Map<String, Object> source, Map<String, Object> metadata, Map<String, Validator> validators) {
        this.source = source;
        this.metadata = metadata;
        this.validators = validators;
        validateMetadata();
    }

    protected AbstractSet<Map.Entry<String, Object>> entrySet; // cache to avoid recreation

    /**
     * get the source map, if externally modified then the guarantees of this class are not enforced
     */
    public Map<String, Object> getSource() {
        return source;
    }

    /**
     * get the metadata map, if externally modified then the guarantees of this class are not enforced
     */
    public Map<String, Object> getMetadata() {
        return metadata;
    }

    /**
     * Check that all metadata map contains only valid metadata and no extraneous keys and source map contains no metadata
     */
    protected void validateMetadata() {
        int numMetadata = 0;
        for (Map.Entry<String, Validator> entry : validators.entrySet()) {
            String key = entry.getKey();
            if (metadata.containsKey(key)) {
                numMetadata++;
            }
            entry.getValue().accept(MapOperation.INIT, key, metadata.get(key));
            if (source.containsKey(key)) {
                throw new IllegalArgumentException("Unexpected metadata key [" + key + "] in source with value [" + source.get(key) + "]");
            }
        }
        if (numMetadata < metadata.size()) {
            Set<String> keys = new HashSet<>(metadata.keySet());
            keys.removeAll(validators.keySet());
            throw new IllegalArgumentException(
                "Unexpected metadata keys ["
                    + keys.stream().sorted().map(k -> k + ":" + metadata.get(k)).collect(Collectors.joining(", "))
                    + "]"
            );
        }
    }

    /**
     * Returns an entrySet that respects the validators of the map.
     */
    @Override
    public Set<Map.Entry<String, Object>> entrySet() {
        if (entrySet == null) {
            entrySet = new EntrySet(source.entrySet(), metadata.entrySet());
        }
        return entrySet;
    }

    /**
     * Associate a key with a value.  If the key has a validator, it is applied before association.
     * @throws IllegalArgumentException if value does not pass validation for the given key
     */
    @Override
    public Object put(String key, Object value) {
        Validator validator = validators.get(key);
        if (validator != null) {
            validator.accept(MapOperation.UPDATE, key, value);
            return metadata.put(key, value);
        }
        return source.put(key, value);
    }

    /**
     * Remove the mapping of key.  If the key has a validator, it is checked before key removal.
     * @throws IllegalArgumentException if the validator does not allow the key to be removed
     */
    @Override
    public Object remove(Object key) {
        // uses map directly to avoid AbstractMaps linear time implementation using entrySet()
        if (key instanceof String strKey) {
            Validator validator = validators.get(key);
            if (validator != null) {
                validator.accept(MapOperation.REMOVE, strKey, null);
                return metadata.remove(key);
            }
        }
        return source.remove(key);
    }

    /**
     * Clear entire map.  For each key in the map with a validator, that validator is checked as in {@link #remove(Object)}.
     * @throws IllegalArgumentException if any validator does not allow the key to be removed, in this case the map is unmodified
     */
    @Override
    public void clear() {
        // AbstractMap uses entrySet().clear(), it should be quicker to run through the validators, then call the wrapped maps clear
        validators.forEach((k, v) -> {
            if (metadata.containsKey(k)) {
                v.accept(MapOperation.REMOVE, k, null);
            }
        });
        metadata.clear();
        source.clear();
    }

    @Override
    public int size() {
        // uses map directly to avoid creating an EntrySet via AbstractMaps implementation, which returns entrySet().size()
        return source.size() + metadata.size();
    }

    @Override
    public boolean containsValue(Object value) {
        // uses map directly to avoid AbstractMaps linear time implementation using entrySet()
        return metadata.containsValue(value) || source.containsValue(value);
    }

    @Override
    public boolean containsKey(Object key) {
        // uses map directly to avoid AbstractMaps linear time implementation using entrySet()
        return metadata.containsKey(key) || source.containsKey(key);
    }

    @Override
    public Object get(Object key) {
        // uses map directly to avoid AbstractMaps linear time implementation using entrySet()
        if (validators.get(key) != null) {
            return metadata.get(key);
        }
        return source.get(key);
    }

    /**
     * Get the String version of the value associated with {@code key}, or null
     */
    public String getString(Object key) {
        return Objects.toString(get(key), null);
    }

    /**
     * Get the {@link Number} associated with key, or null
     * @throws IllegalArgumentException if the value is not a {@link Number}
     */
    public Number getNumber(Object key) {
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
     * Get the {@link ZonedDateTime} associated with key, or null.  If the value stored is a {@link Number}, assumes that
     * value represents milliseconds from epoch.
     * @throws IllegalArgumentException if the value is neither a {@link Number} nor a {@link ZonedDateTime}.
     */
    public ZonedDateTime getZonedDateTime(Object key) {
        Object value = get(key);
        if (value == null) {
            return null;
        }
        if (value instanceof ZonedDateTime zdt) {
            return zdt;
        } else if (value instanceof Number number) {
            return ZonedDateTime.ofInstant(Instant.ofEpochMilli(number.longValue()), ZoneOffset.UTC);
        }
        throw new IllegalArgumentException(
            "unexpected type for ["
                + key
                + "] with value ["
                + value
                + "], expected Number or ZonedDateTime, got ["
                + value.getClass().getName()
                + "]"
        );
    }

    /**
     * Set of entries of the wrapped map that calls the appropriate validator before changing an entries value or removing an entry.
     *
     * Inherits {@link AbstractSet#removeAll(Collection)}, which calls the overridden {@link #remove(Object)} which performs validation.
     *
     * Inherits {@link AbstractCollection#retainAll(Collection)} and {@link AbstractCollection#clear()}, which both use
     * {@link EntrySetIterator#remove()} for removal.
     */
    class EntrySet extends AbstractSet<Map.Entry<String, Object>> {
        Set<Map.Entry<String, Object>> sourceSet;
        Set<Map.Entry<String, Object>> metadataSet;

        EntrySet(Set<Map.Entry<String, Object>> sourceSet, Set<Map.Entry<String, Object>> metadataSet) {
            this.sourceSet = sourceSet;
            this.metadataSet = metadataSet;
        }

        @Override
        public Iterator<Map.Entry<String, Object>> iterator() {
            return new EntrySetIterator(sourceSet.iterator(), metadataSet.iterator());
        }

        @Override
        public int size() {
            return sourceSet.size() + metadataSet.size();
        }

        @Override
        public boolean remove(Object o) {
            if (metadataSet.contains(o)) {
                if (o instanceof Map.Entry<?, ?> entry) {
                    if (entry.getKey()instanceof String key) {
                        Validator validator = validators.get(key);
                        if (validator != null) {
                            validator.accept(MapOperation.REMOVE, key, null);
                            return metadataSet.remove(o);
                        }
                    }
                }
            }
            return sourceSet.remove(o);
        }
    }

    /**
     * Iterator over the wrapped map that returns a validating {@link Entry} on {@link #next()} and validates on {@link #remove()}.
     *
     * {@link #remove()} is called by remove in {@link AbstractMap#values()}, {@link AbstractMap#keySet()}, {@link AbstractMap#clear()} via
     * {@link AbstractSet#clear()}
     */
    class EntrySetIterator implements Iterator<Map.Entry<String, Object>> {
        final Iterator<Map.Entry<String, Object>> sourceIter;
        final Iterator<Map.Entry<String, Object>> metadataIter;

        boolean sourceCur = true;
        Entry cur;

        EntrySetIterator(Iterator<Map.Entry<String, Object>> sourceIter, Iterator<Map.Entry<String, Object>> metadataIter) {
            this.sourceIter = sourceIter;
            this.metadataIter = metadataIter;
        }

        @Override
        public boolean hasNext() {
            return sourceIter.hasNext() || metadataIter.hasNext();
        }

        @Override
        public Map.Entry<String, Object> next() {
            sourceCur = sourceIter.hasNext();
            return cur = new Entry(sourceCur ? sourceIter.next() : metadataIter.next(), sourceCur);
        }

        /**
         * Remove current entry from the backing Map.  Checks the Entry's key's validator, if one exists, before removal.
         * @throws IllegalArgumentException if the validator does not allow the Entry to be removed
         * @throws IllegalStateException if remove is called before {@link #next()}
         */
        @Override
        public void remove() {
            if (cur == null) {
                throw new IllegalStateException();
            }
            if (sourceCur) {
                sourceIter.remove();
            } else {
                Validator validator = validators.get(cur.getKey());
                if (validator != null) {
                    validator.accept(MapOperation.REMOVE, cur.getKey(), null);
                }
                metadataIter.remove();
            }
        }
    }

    /**
     * Wrapped Map.Entry that calls the key's validator on {@link #setValue(Object)}
     */
    class Entry implements Map.Entry<String, Object> {
        final Map.Entry<String, Object> entry;
        final boolean isSource;

        Entry(Map.Entry<String, Object> entry, boolean isSource) {
            this.entry = entry;
            this.isSource = isSource;
        }

        @Override
        public String getKey() {
            return entry.getKey();
        }

        @Override
        public Object getValue() {
            return entry.getValue();
        }

        /**
         * Associate the value with the Entry's key in the linked Map.  If the Entry's key has a validator, it is applied before association
         * @throws IllegalArgumentException if value does not pass validation for the Entry's key
         */
        @Override
        public Object setValue(Object value) {
            if (isSource == false) {
                Validator validator = validators.get(entry.getKey());
                if (validator != null) {
                    validator.accept(MapOperation.UPDATE, entry.getKey(), value);
                }
            }
            return entry.setValue(value);
        }
    }

    @Override
    public String getIndex() {
        return getString(INDEX);
    }

    @Override
    public void setIndex(String index) {
        put(INDEX, index);
    }

    @Override
    public String getId() {
        return getString(ID);
    }

    @Override
    public void setId(String id) {
        put(ID, id);
    }

    @Override
    public String getRouting() {
        return getString(ROUTING);
    }

    @Override
    public void setRouting(String routing) {
        put(ROUTING, routing);
    }

    @Override
    public long getVersion() {
        return getNumber(VERSION).longValue();
    }

    @Override
    public void setVersion(long version) {
        put(VERSION, version);
    }

    @Override
    public boolean hasVersion() {
        if (containsKey(VERSION) == false) {
            return false;
        }
        return get(VERSION) == null;
    }

    /**
     * Allow a String or null
     */
    public static void stringValidator(MapOperation op, String key, Object value) {
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

    /**
     * Same as {@link #longValidator} but value cannot be null
     */
    public static void nonNullLongValidator(MapOperation op, String key, Object value) {
        if (op == MapOperation.REMOVE || value == null) {
            throw new IllegalArgumentException(key + " cannot be null");
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
