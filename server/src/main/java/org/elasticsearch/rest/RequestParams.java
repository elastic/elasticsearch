/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest;

import org.elasticsearch.common.util.Maps;

import java.net.URI;
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
 * operates on the <em>last</em> value in that list: {@link #get(Object)} returns the last value for a key, or {@code null} if absent,
 * and {@link #put(String, String)} sets a key to a single value (stored as a one-element list, so {@link #getAll(String)} returns
 * a singleton list after a {@code put}).
 * Use {@link #getAll(String)} to retrieve all values for a repeated key.
 *
 * <p>The value lists returned by {@link #getAll(String)} are always non-empty and immutable, so
 * {@link List#getFirst()} and {@link List#getLast()} are always safe to call on a non-{@code null} result.
 */
public final class RequestParams extends AbstractMap<String, String> {

    /** Single backing store: key → non-empty ordered list of all values. */
    private final Map<String, List<String>> map;

    // Factory methods

    /**
     * Returns an empty {@code RequestParams} instance.
     */
    public static RequestParams empty() {
        return new RequestParams(new LinkedHashMap<>());
    }

    /**
     * Creates a {@code RequestParams} from a multi-value map.  Each list must be non-empty.
     * The last value in each list is what {@link #get(Object)} returns.
     *
     * @param multiValues a map from parameter name to all its values, in encounter order
     * @throws IllegalArgumentException if any value list is empty
     */
    static RequestParams of(Map<String, List<String>> multiValues) {
        LinkedHashMap<String, List<String>> copy = Maps.newLinkedHashMapWithExpectedSize(multiValues.size());
        multiValues.forEach((k, v) -> {
            if (v.isEmpty()) {
                throw new IllegalArgumentException("value list for parameter [" + k + "] must not be empty");
            }
            copy.put(k, List.copyOf(v));
        });
        return new RequestParams(copy);
    }

    /**
     * Parses a URL-encoded query string into a {@code RequestParams}, preserving all values for
     * repeated parameters (e.g. {@code match[]=foo&match[]=bar} → {@code ["foo", "bar"]}).
     *
     * @param queryString the raw query string (the part after {@code ?}, without the {@code ?} itself)
     * @return a {@code RequestParams} from parameter name to all its values, in encounter order
     * @throws RestRequest.BadParameterException if the query string cannot be decoded
     */
    public static RequestParams fromQueryString(String queryString) {
        try {
            return RestUtils.decodeQueryString(queryString, 0);
        } catch (IllegalArgumentException e) {
            throw new RestRequest.BadParameterException(e);
        }
    }

    /**
     * Parses the query string from a URI string into a {@code RequestParams}, preserving all
     * values for repeated parameters. Returns an empty map if the URI contains no {@code ?}.
     *
     * @param uri a URI string, e.g. {@code /index/_search?pretty&size=10}
     * @return a {@code RequestParams} from parameter name to all its values, in encounter order
     * @throws RestRequest.BadParameterException if the query string cannot be decoded
     */
    public static RequestParams fromUri(String uri) {
        int index = uri.indexOf('?');
        return index >= 0 ? fromQueryString(uri.substring(index + 1)) : RequestParams.empty();
    }

    /**
     * Parses the query string from a {@link URI} into a {@code RequestParams}, preserving all
     * values for repeated parameters. Returns an empty map if the URI has no query.
     *
     * @param uri the URI whose raw query string is parsed
     * @return a {@code RequestParams} from parameter name to all its values, in encounter order
     * @throws RestRequest.BadParameterException if the query string cannot be decoded
     */
    public static RequestParams from(URI uri) {
        final var rawQuery = uri.getRawQuery();
        return rawQuery != null && rawQuery.isEmpty() == false ? fromQueryString(rawQuery) : RequestParams.empty();
    }

    /**
     * Creates a {@code RequestParams} from a plain single-value map.
     * Each value is wrapped in a singleton list so that {@link #getAll(String)} returns a
     * one-element list rather than an empty one.
     *
     * @param singleValues a map whose values are treated as the sole value for each key
     */
    public static RequestParams fromSingleValues(Map<String, String> singleValues) {
        LinkedHashMap<String, List<String>> wrapped = Maps.newLinkedHashMapWithExpectedSize(singleValues.size());
        singleValues.forEach((k, v) -> wrapped.put(k, Collections.singletonList(v)));
        return new RequestParams(wrapped);
    }

    private RequestParams(Map<String, List<String>> map) {
        this.map = map;
    }

    /**
     * Appends {@code value} to the list of values for {@code key}.
     * If the key is not yet present, a new entry is created.
     */
    void addValue(String key, String value) {
        map.computeIfAbsent(key, k -> new ArrayList<>(1)).add(value);
    }

    /**
     * Returns all values for {@code key} in the order they were added,
     * or an empty list if the key is absent.
     *
     * @param key the parameter name
     * @return an unmodifiable non-empty list of all values, or an empty list if absent; never {@code null}
     */
    public List<String> getAll(String key) {
        var list = map.get(key);
        return list == null ? List.of() : Collections.unmodifiableList(list);
    }

    /**
     * Returns the single value for {@code key}, or {@code null} if absent.
     *
     * @param key the parameter name
     * @return the single value, or {@code null} if absent
     * @throws RestRequest.BadParameterException if the key has multiple values
     */
    public String requireSingle(String key) {
        var list = map.get(key);
        if (list == null) {
            return null;
        }
        if (list.size() > 1) {
            throw new RestRequest.BadParameterException(
                new IllegalArgumentException("parameter [" + key + "] must have a single value, but found: " + list)
            );
        }
        return list.getFirst();
    }

    /**
     * Returns the last value associated with {@code key}, or {@code null} if absent.
     *
     * <p>When a query parameter appears multiple times (e.g. {@code a=1&a=2}), this method returns
     * the <em>last</em> value ({@code "2"}). Use {@link #getAll(String)} to retrieve all values, or
     * {@link #requireSingle(String)} to assert that only one value is present.
     */
    @Override
    public String get(Object key) {
        var list = map.get(key);
        return list == null ? null : list.getLast();
    }

    /**
     * Sets {@code key} to a single {@code value}, replacing any previous values for that key.
     * The value is stored as a one-element immutable list, so {@link #getAll(String)} will return
     * a singleton list and {@link List#getFirst()}/{@link List#getLast()} remain safe to call.
     *
     * @return the previous last value for {@code key}, or {@code null} if absent
     */
    @Override
    public String put(String key, String value) {
        var prev = map.put(key, Collections.singletonList(value));
        return prev == null ? null : prev.getLast();
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public String remove(Object key) {
        var prev = map.remove(key);
        return prev == null ? null : prev.getLast();
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    @Override
    public Set<String> keySet() {
        return Collections.unmodifiableSet(map.keySet());
    }

    /**
     * Returns an entry set where each entry's value is the <em>last</em> value for that key.
     * Supports removal via the iterator, which removes the entire key from the underlying map.
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

            @Override
            public void clear() {
                map.clear();
            }
        };
    }
}
