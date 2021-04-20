/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.elasticsearch.Assertions;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class Maps {

    /**
     * Returns {@code true} if the two specified maps are equal to one another. Two maps are considered equal if both represent identical
     * mappings where values are checked with Objects.deepEquals. The primary use case is to check if two maps with array values are equal.
     *
     * @param left  one map to be tested for equality
     * @param right the other map to be tested for equality
     * @return {@code true} if the two maps are equal
     */
    public static <K, V> boolean deepEquals(Map<K, V> left, Map<K, V> right) {
        if (left == right) {
            return true;
        }
        if (left == null || right == null || left.size() != right.size()) {
            return false;
        }
        return left.entrySet().stream()
                .allMatch(e -> right.containsKey(e.getKey()) && Objects.deepEquals(e.getValue(), right.get(e.getKey())));
    }

    /**
     * Remove the specified key from the provided immutable map by copying the underlying map and filtering out the specified
     * key if that key exists.
     *
     * @param map   the immutable map to remove the key from
     * @param key   the key to be removed
     * @param <K>   the type of the keys in the map
     * @param <V>   the type of the values in the map
     * @return an immutable map that contains the items from the specified map with the provided key removed
     */
    public static <K, V> Map<K, V> copyMapWithRemovedEntry(final Map<K, V> map, final K key) {
        Objects.requireNonNull(map);
        Objects.requireNonNull(key);
        assertImmutableMap(map, key, map.get(key));
        return map.entrySet().stream().filter(k -> key.equals(k.getKey()) == false)
            .collect(Collectors.collectingAndThen(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue),
                Collections::<K, V>unmodifiableMap));
    }

    private static <K, V> void assertImmutableMap(final Map<K, V> map, final K key, final V value) {
        if (Assertions.ENABLED) {
            boolean immutable;
            try {
                map.put(key, value);
                immutable = false;
            } catch (final UnsupportedOperationException e) {
                immutable = true;
            }
            assert immutable : "expected an immutable map but was [" + map.getClass() + "]";
        }
    }
}
