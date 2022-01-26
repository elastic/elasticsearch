/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;

public class Map {

    /**
     * Returns an unmodifiable map containing one mapping.
     */
    public static <K, V> java.util.Map<K, V> of() {
        return Collections.emptyMap();
    }

    /**
     * Returns an unmodifiable map containing one mapping.
     */
    public static <K, V> java.util.Map<K, V> of(K k1, V v1) {
        return Collections.singletonMap(k1, v1);
    }

    /**
     * Returns an unmodifiable map containing two mappings.
     */
    public static <K, V> java.util.Map<K, V> of(K k1, V v1, K k2, V v2) {
        return mapN(k1, v1, k2, v2);
    }

    /**
     * Returns an unmodifiable map containing three mappings.
     */
    public static <K, V> java.util.Map<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3) {
        return mapN(k1, v1, k2, v2, k3, v3);
    }

    /**
     * Returns an unmodifiable map containing four mappings.
     */
    public static <K, V> java.util.Map<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4) {
        return mapN(k1, v1, k2, v2, k3, v3, k4, v4);
    }

    /**
     * Returns an unmodifiable map containing five mappings.
     */
    public static <K, V> java.util.Map<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5) {
        return mapN(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5);
    }

    /**
     * Returns an unmodifiable map containing six mappings.
     */
    public static <K, V> java.util.Map<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5, K k6, V v6) {
        return mapN(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6);
    }

    /**
     * Returns an unmodifiable map containing seven mappings.
     */
    public static <K, V> java.util.Map<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5, K k6, V v6, K k7, V v7) {
        return mapN(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7);
    }

    /**
     * Returns an unmodifiable map containing eight mappings.
     */
    public static <K, V> java.util.Map<K, V> of(
        K k1,
        V v1,
        K k2,
        V v2,
        K k3,
        V v3,
        K k4,
        V v4,
        K k5,
        V v5,
        K k6,
        V v6,
        K k7,
        V v7,
        K k8,
        V v8
    ) {
        return mapN(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8);
    }

    /**
     * Returns an unmodifiable map containing nine mappings.
     */
    public static <K, V> java.util.Map<K, V> of(
        K k1,
        V v1,
        K k2,
        V v2,
        K k3,
        V v3,
        K k4,
        V v4,
        K k5,
        V v5,
        K k6,
        V v6,
        K k7,
        V v7,
        K k8,
        V v8,
        K k9,
        V v9
    ) {
        return mapN(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8, k9, v9);
    }

    /**
     * Returns an unmodifiable map containing ten mappings.
     */
    public static <K, V> java.util.Map<K, V> of(
        K k1,
        V v1,
        K k2,
        V v2,
        K k3,
        V v3,
        K k4,
        V v4,
        K k5,
        V v5,
        K k6,
        V v6,
        K k7,
        V v7,
        K k8,
        V v8,
        K k9,
        V v9,
        K k10,
        V v10
    ) {
        return mapN(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8, k9, v9, k10, v10);
    }

    @SuppressWarnings("unchecked")
    private static <K, V> java.util.Map<K, V> mapN(Object... objects) {
        if (objects.length % 2 != 0) {
            throw new IllegalStateException("Must provide an even number of arguments to Map::of method");
        }
        switch (objects.length) {
            case 0:
                return Map.of();
            case 2:
                return Map.of((K) objects[0], (V) objects[1]);
            default:
                HashMap<K, V> map = new HashMap<>();
                for (int k = 0; k < objects.length / 2; k++) {
                    map.put((K) objects[k * 2], (V) objects[k * 2 + 1]);
                }
                return Collections.unmodifiableMap(map);
        }
    }

    /**
     * Returns an unmodifiable map containing keys and values extracted from the given entries.
     *
     * @param <K> the {@code Map}'s key type
     * @param <V> the {@code Map}'s value type
     * @param entries {@code Map.Entry}s containing the keys and values from which the map is populated
     * @return a {@code Map} containing the specified mappings
     */
    @SafeVarargs
    public static <K, V> java.util.Map<K, V> ofEntries(java.util.Map.Entry<? extends K, ? extends V>... entries) {
        if (entries.length == 0) {
            return Collections.emptyMap();
        } else if (entries.length == 1) {
            return Collections.singletonMap(entries[0].getKey(), entries[0].getValue());
        } else {
            HashMap<K, V> map = new HashMap<>();
            for (java.util.Map.Entry<? extends K, ? extends V> entry : entries) {
                map.put(entry.getKey(), entry.getValue());
            }
            return Collections.unmodifiableMap(map);
        }
    }

    /**
     * Returns an unmodifiable Map.Entry for the provided key and value.
     */
    public static <K, V> java.util.Map.Entry<K, V> entry(K k, V v) {
        return new AbstractMap.SimpleImmutableEntry<>(k, v);
    }

    /**
     * Returns an unmodifiable {@code Map} containing the entries of the given {@code Map}.
     *
     * @param <K> the {@code Map}'s key type
     * @param <V> the {@code Map}'s value type
     * @param map a {@code Map} from which entries are drawn, must be non-null
     * @return a {@code Map} containing the entries of the given {@code Map}
     */
    public static <K, V> java.util.Map<K, V> copyOf(java.util.Map<? extends K, ? extends V> map) {
        return Collections.unmodifiableMap(new HashMap<>(map));
    }
}
