/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * ValidatingMap is an implementation of {@code Map<String, Object>} that has a validating functions for a subset of keys.
 *
 * The validators are BiFunctions that should either return the identity or throw an {@link IllegalArgumentException}.  The
 * arguments to the validator are the key and the proposed value or {@code null} for key removal.
 *
 * Validation occurs everywhere updates can happen, either directly via the {@link #put(String, Object)}, {@link #replace(Object, Object)},
 * {@link #merge(Object, Object, BiFunction)} family of methods, via {@link Map.Entry#setValue(Object)},
 * via the linked Set from {@link #entrySet()} or {@link Collection#remove(Object)} from the linked collection via {@link #values()}
 */
public class ValidatingMap extends AbstractMap<String, Object> {
    private static final BiFunction<String, Object, Object> IDENTITY = (k, v) -> v;

    protected final Map<String, BiFunction<String, Object, Object>> validators;
    protected final Map<String, Object> map;

    private EntrySet entrySet;

    /**
     * Create a ValidatingMap using the underlying map and set of validators.  The validators are applied to the map to ensure the incoming
     * map matches the invariants enforced by the validators.
     * @param map the wrapped map.  Should not be externally modified after creation.
     * @param validators map of key to validating function.  Should throw {@link IllegalArgumentException} on invalid value, otherwise
     *                   return identity
     * @throws IllegalArgumentException if a validator fails for a given key
     */
    public ValidatingMap(Map<String, Object> map, Map<String, BiFunction<String, Object, Object>> validators) {
        this.map = map;
        this.validators = validators;
        this.validators.forEach((k, v) -> v.apply(k, map.get(k)));
    }

    /**
     * Returns an entrySet that respects the validators of the map.
     */
    @Override
    public Set<Map.Entry<String, Object>> entrySet() {
        if (entrySet == null) {
            entrySet = new EntrySet(map.entrySet());
        }
        return entrySet;
    }

    /**
     * Associate a key with a value.  If the key has a validator, it is applied before association.
     * @throws IllegalArgumentException if value does not pass validation for the given key
     */
    @Override
    public Object put(String key, Object value) {
        return map.put(key, getValidator(key).apply(key, value));
    }

    /**
     * Remove the mapping of key.  If the key has a validator, it is checked before key removal.
     * @throws IllegalArgumentException if the validator does not allow the key to be removed
     */
    @Override
    public Object remove(Object key) {
        // uses map directly to avoid AbstractMaps linear time implementation using entrySet()
        if (key instanceof String keyStr) {
            getValidator(keyStr).apply(keyStr, null);
        }
        return map.remove(key);
    }

    /**
     * Clear entire map.  For each key in the map with a validator, that validator is checked as in {@link #remove(Object)}.
     * @throws IllegalArgumentException if any validator does not allow the key to be removed, in this case the map is unmodified
     */
    @Override
    public void clear() {
        // AbstractMap uses entrySet().clear(), it should be quicker to run through the validators, then call the wrapped map's clear
        validators.forEach((k, v) -> {
            if (map.containsKey(k)) {
                v.apply(k, null);
            }
        });
        map.clear();
    }

    @Override
    public int size() {
        // uses map directly to avoid creating an EntrySet via AbstractMaps implementation, which returns entrySet().size()
        return map.size();
    }

    @Override
    public boolean containsValue(Object value) {
        // uses map directly to avoid AbstractMaps linear time implementation using entrySet()
        return map.containsValue(value);
    }

    @Override
    public boolean containsKey(Object key) {
        // uses map directly to avoid AbstractMaps linear time implementation using entrySet()
        return map.containsKey(key);
    }

    @Override
    public Object get(Object key) {
        // uses map directly to avoid AbstractMaps linear time implementation using entrySet()
        return map.get(key);
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
     * Get the validator for the given key, or the identity function if the key has no validator.
     */
    protected BiFunction<String, Object, Object> getValidator(String key) {
        return validators.getOrDefault(key, IDENTITY);
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
        Set<Map.Entry<String, Object>> set;

        EntrySet(Set<Map.Entry<String, Object>> set) {
            this.set = set;
        }

        @Override
        public Iterator<Map.Entry<String, Object>> iterator() {
            return new EntrySetIterator(set.iterator());
        }

        @Override
        public int size() {
            return set.size();
        }

        @Override
        public boolean remove(Object o) {
            if (o instanceof Map.Entry<?, ?> entry) {
                if (entry.getKey()instanceof String key) {
                    getValidator(key).apply(key, null);
                }
                return set.remove(o);
            }
            return false;
        }
    }

    /**
     * Iterator over the wrapped map that returns a validating {@link Entry} on {@link #next()} and validates on {@link #remove()}.
     *
     * {@link #remove()} is called by remove in {@link AbstractMap#values()}, {@link AbstractMap#keySet()}, {@link AbstractMap#clear()} via
     * {@link AbstractSet#clear()}
     */
    class EntrySetIterator implements Iterator<Map.Entry<String, Object>> {
        Iterator<Map.Entry<String, Object>> iterator;
        Entry cur;

        EntrySetIterator(Iterator<Map.Entry<String, Object>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Map.Entry<String, Object> next() {
            return cur = new Entry(iterator.next());
        }

        /**
         * Remove current entry from the backing Map.  Checks the Entry's key's validator, if one exists, before removal.
         * @throws IllegalArgumentException if the validator does not allow the Entry to be removed
         */
        @Override
        public void remove() {
            if (cur != null) {
                getValidator(cur.getKey()).apply(cur.getKey(), null);
            }
            iterator.remove();
        }
    }

    /**
     * Wrapped Map.Entry that calls the key's validator on {@link #setValue(Object)}
     */
    class Entry implements Map.Entry<String, Object> {
        final Map.Entry<String, Object> entry;

        Entry(Map.Entry<String, Object> entry) {
            this.entry = entry;
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
            return entry.setValue(getValidator(entry.getKey()).apply(entry.getKey(), value));
        }
    }
}
