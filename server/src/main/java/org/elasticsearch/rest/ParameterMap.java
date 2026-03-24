/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link Map}{@code <String, String>} for HTTP request parameters that preserves multiple values
 * per key (e.g. repeated query parameters such as {@code match[]=foo&match[]=bar}).
 *
 * <p>Each key maps to a non-empty ordered list of values.  The standard {@link Map} interface
 * operates on the <em>last</em> value in that list:
 * <ul>
 *   <li>{@link #get(Object)} returns the last value for a key, or {@code null} if absent.</li>
 *   <li>{@link #put(String, String)} replaces all existing values with a single new one.</li>
 * </ul>
 * Use {@link #getAll(String)} to retrieve all values for a repeated key.
 */
public final class ParameterMap extends AbstractMap<String, String> {

    /** Single backing store: key → non-empty ordered list of all values. */
    private final LinkedHashMap<String, List<String>> map;

    // -------------------------------------------------------------------------
    // Factory methods
    // -------------------------------------------------------------------------

    /**
     * Returns a new, empty {@code ParameterMap}.
     */
    public static ParameterMap empty() {
        return new ParameterMap(Map.of());
    }

    /**
     * Creates a {@code ParameterMap} from a multi-value map.  Each list must be non-empty.
     * The last value in each list is what {@link #get(Object)} returns.
     *
     * @param multiValues a map from parameter name to all its values, in encounter order
     */
    public static ParameterMap of(Map<String, List<String>> multiValues) {
        return new ParameterMap(multiValues);
    }

    /**
     * Creates a {@code ParameterMap} from a plain single-value map.
     * Each value is wrapped in a singleton list so that {@link #getAll(String)} returns a
     * one-element list rather than an empty one.
     *
     * @param singleValues a map whose values are treated as the sole value for each key
     */
    public static ParameterMap fromSingleValues(Map<String, String> singleValues) {
        LinkedHashMap<String, List<String>> wrapped = new LinkedHashMap<>(singleValues.size() * 2);
        singleValues.forEach((k, v) -> wrapped.put(k, List.of(v)));
        return new ParameterMap(wrapped);
    }

    private ParameterMap(Map<String, List<String>> multiValues) {
        this.map = new LinkedHashMap<>(multiValues);
        assert map.values().stream().allMatch(list -> list != null && list.isEmpty() == false)
            : "ParameterMap requires every value list to be non-empty";
    }

    // -------------------------------------------------------------------------
    // Multi-value API
    // -------------------------------------------------------------------------

    /**
     * Returns all values for {@code key} in the order they were added,
     * or an empty list if the key is absent.
     *
     * @param key the parameter name
     * @return a non-empty list of all values, or an empty list if absent; never {@code null}
     */
    public List<String> getAll(String key) {
        var list = map.get(key);
        return list == null ? List.of() : Collections.unmodifiableList(list);
    }

    /**
     * Returns the single value for {@code key}, or {@code null} if absent.
     * Throws {@link IllegalArgumentException} if the key has more than one value.
     *
     * @param key the parameter name
     * @return the single value, or {@code null} if absent
     * @throws IllegalArgumentException if the key has multiple values
     */
    public String getSingle(String key) {
        var list = map.get(key);
        if (list == null) {
            return null;
        }
        if (list.size() > 1) {
            throw new IllegalArgumentException("parameter [" + key + "] must have a single value, but found: " + list);
        }
        return list.getFirst();
    }

    // -------------------------------------------------------------------------
    // Map<String, String> — single-value view over the backing list map
    // -------------------------------------------------------------------------

    /**
     * Returns the last value associated with {@code key}, or {@code null} if absent.
     */
    @Override
    public String get(Object key) {
        var list = map.get(key);
        return list == null ? null : list.getLast();
    }

    /**
     * Associates {@code key} with {@code value}, replacing all previous values for that key.
     * Returns the previous last value, or {@code null}.
     */
    @Override
    public String put(String key, String value) {
        var old = map.put(key, new ArrayList<>(List.of(value)));
        return old == null ? null : old.getLast();
    }

    @Override
    public String remove(Object key) {
        var old = map.remove(key);
        return old == null ? null : old.getLast();
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    /** Returns a live key set backed by the underlying map. */
    @Override
    public Set<String> keySet() {
        return map.keySet();
    }

    /**
     * Returns a live entry set where each entry's value is the <em>last</em> value for that key.
     * Removal through the iterator is supported and removes the key from the map entirely.
     */
    @Override
    public Set<Entry<String, String>> entrySet() {
        return new AbstractSet<>() {
            @Override
            public Iterator<Entry<String, String>> iterator() {
                var inner = map.entrySet().iterator();
                return new Iterator<>() {
                    @Override
                    public boolean hasNext() {
                        return inner.hasNext();
                    }

                    @Override
                    public Entry<String, String> next() {
                        var e = inner.next();
                        return Map.entry(e.getKey(), e.getValue().getLast());
                    }

                    @Override
                    public void remove() {
                        inner.remove();
                    }
                };
            }

            @Override
            public int size() {
                return map.size();
            }
        };
    }
}
