/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
        assert checkIsImmutableMap(map, key, map.get(key));
        return map.entrySet().stream().filter(k -> key.equals(k.getKey()) == false)
            .collect(Collectors.collectingAndThen(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue),
                Collections::<K, V>unmodifiableMap));
    }

    // map classes that are known to be immutable, used to speed up immutability check in #assertImmutableMap
    private static final Set<Class<?>> IMMUTABLE_MAP_CLASSES = org.elasticsearch.core.Set.of(
            Collections.emptyMap().getClass(),
            Collections.unmodifiableMap(new HashMap<>()).getClass(),
            org.elasticsearch.core.Map.of().getClass(),
            org.elasticsearch.core.Map.of("a", "b").getClass()
    );

    private static <K, V> boolean checkIsImmutableMap(final Map<K, V> map, final K key, final V value) {
        // check in the known immutable classes map first, most of the time we don't need to actually do the put and throw which is slow to
        // the point of visibly slowing down internal cluster tests without this short-cut
        if (IMMUTABLE_MAP_CLASSES.contains(map.getClass())) {
            return true;
        }
        try {
            map.put(key, value);
            return false;
        } catch (final UnsupportedOperationException ignored) {
        }
        return true;
    }
}
