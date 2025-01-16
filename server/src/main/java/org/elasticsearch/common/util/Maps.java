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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
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
        return left.entrySet()
            .stream()
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
        return map.entrySet()
            .stream()
            .filter(k -> key.equals(k.getKey()) == false)
            .collect(
                Collectors.collectingAndThen(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue), Collections::<K, V>unmodifiableMap)
            );
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
        } catch (final UnsupportedOperationException ignored) {}
        return true;
    }

    /**
     * Returns an array where all internal maps and optionally arrays are flattened into the root map.
     *
     * For example the map {"foo": {"bar": 1, "baz": [2, 3]}} will become {"foo.bar": 1, "foo.baz.0": 2, "foo.baz.1": 3}. Note that if
     * maps contains keys with "." or numbers it is possible that such keys will be silently overridden. For example the map
     * {"foo": {"bar": 1}, "foo.bar": 2} will become {"foo.bar": 1} or {"foo.bar": 2}.
     *
     * @param map - input to be flattened
     * @param flattenArrays - if false, arrays will be ignored
     * @param ordered - if true the resulted map will be sorted
     * @return
     */
    public static Map<String, Object> flatten(Map<String, Object> map, boolean flattenArrays, boolean ordered) {
        return flatten(map, flattenArrays, ordered, null);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> flatten(Map<String, Object> map, boolean flattenArrays, boolean ordered, String parentPath) {
        Map<String, Object> flatMap = ordered ? new TreeMap<>() : new HashMap<>();
        String prefix = parentPath != null ? parentPath + "." : "";
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getValue() instanceof Map) {
                flatMap.putAll(flatten((Map<String, Object>) entry.getValue(), flattenArrays, ordered, prefix + entry.getKey()));
            } else if (flattenArrays && entry.getValue() instanceof List) {
                flatMap.putAll(flatten((List<Object>) entry.getValue(), ordered, prefix + entry.getKey()));
            } else {
                flatMap.put(prefix + entry.getKey(), entry.getValue());
            }
        }
        return flatMap;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> flatten(List<Object> list, boolean ordered, String parentPath) {
        Map<String, Object> flatMap = ordered ? new TreeMap<>() : new HashMap<>();
        String prefix = parentPath != null ? parentPath + "." : "";
        for (int i = 0; i < list.size(); i++) {
            Object cur = list.get(i);
            if (cur instanceof Map) {
                flatMap.putAll(flatten((Map<String, Object>) cur, true, ordered, prefix + i));
            }
            if (cur instanceof List) {
                flatMap.putAll(flatten((List<Object>) cur, ordered, prefix + i));
            } else {
                flatMap.put(prefix + i, cur);
            }
        }
        return flatMap;
    }

    /**
     * Returns a {@link Collector} that accumulates the input elements into a sorted map and finishes the resulting set into an
     * unmodifiable sorted map. The resulting read-only view through the unmodifiable sorted map is a sorted map.
     *
     * @param <T> the type of the input elements
     * @return an unmodifiable {@link NavigableMap} where the underlying map is sorted
     */
    public static <T, K, V> Collector<T, ?, NavigableMap<K, V>> toUnmodifiableSortedMap(
        Function<T, ? extends K> keyMapper,
        Function<T, ? extends V> valueMapper
    ) {
        return Collectors.collectingAndThen(Collectors.toMap(keyMapper, valueMapper, (v1, v2) -> {
            throw new IllegalStateException("Duplicate key (attempted merging values " + v1 + "  and " + v2 + ")");
        }, () -> new TreeMap<K, V>()), Collections::unmodifiableNavigableMap);
    }

    /**
     * Returns a {@link Collector} that accumulates the input elements into a linked hash map and finishes the resulting set into an
     * unmodifiable map. The resulting read-only view through the unmodifiable map is a linked hash map.
     *
     * @param <T> the type of the input elements
     * @return an unmodifiable {@link Map} where the underlying map has a consistent order
     */
    public static <T, K, V> Collector<T, ?, Map<K, V>> toUnmodifiableOrderedMap(
        Function<T, ? extends K> keyMapper,
        Function<T, ? extends V> valueMapper
    ) {
        return Collectors.collectingAndThen(Collectors.toMap(keyMapper, valueMapper, (v1, v2) -> {
            throw new IllegalStateException("Duplicate key (attempted merging values " + v1 + "  and " + v2 + ")");
        }, (Supplier<LinkedHashMap<K, V>>) LinkedHashMap::new), Collections::unmodifiableMap);
    }

}
