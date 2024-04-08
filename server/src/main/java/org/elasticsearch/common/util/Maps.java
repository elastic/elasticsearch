/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class Maps {

    /**
     * Adds an entry to an immutable map by copying the underlying map and adding the new entry. This method expects there is not already a
     * mapping for the specified key in the map.
     *
     * @param map   the immutable map to concatenate the entry to
     * @param key   the key of the new entry
     * @param value the value of the entry
     * @param <K>   the type of the keys in the map
     * @param <V>   the type of the values in the map
     * @return an immutable map that contains the items from the specified map and the concatenated entry
     */
    @SuppressWarnings("unchecked")
    public static <K, V> Map<K, V> copyMapWithAddedEntry(final Map<K, V> map, final K key, final V value) {
        assert assertIsImmutableMapAndNonNullKey(map, key, value);
        assert value != null;
        assert map.containsKey(key) == false : "expected entry [" + key + "] to not already be present in map";
        @SuppressWarnings("rawtypes")
        final Map.Entry<K, V>[] entries = new Map.Entry[map.size() + 1];
        map.entrySet().toArray(entries);
        entries[entries.length - 1] = Map.entry(key, value);
        return Map.ofEntries(entries);
    }

    /**
     * Adds a new entry to or replaces an existing entry in an immutable map by copying the underlying map and adding the new or replacing
     * the existing entry.
     *
     * @param map   the immutable map to add to or replace in
     * @param key   the key of the new entry
     * @param value the value of the new entry
     * @param <K>   the type of the keys in the map
     * @param <V>   the type of the values in the map
     * @return an immutable map that contains the items from the specified map and a mapping from the specified key to the specified value
     */
    @SuppressWarnings("unchecked")
    public static <K, V> Map<K, V> copyMapWithAddedOrReplacedEntry(final Map<K, V> map, final K key, final V value) {
        final V existing = map.get(key);
        if (existing == null) {
            return copyMapWithAddedEntry(map, key, value);
        }
        assert assertIsImmutableMapAndNonNullKey(map, key, value);
        assert value != null;
        @SuppressWarnings("rawtypes")
        final Map.Entry<K, V>[] entries = new Map.Entry[map.size()];
        boolean replaced = false;
        int i = 0;
        for (Map.Entry<K, V> entry : map.entrySet()) {
            if (replaced == false && entry.getKey().equals(key)) {
                entry = Map.entry(entry.getKey(), value);
                replaced = true;
            }
            entries[i++] = entry;
        }
        return Map.ofEntries(entries);
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
        assert assertIsImmutableMapAndNonNullKey(map, key, map.get(key));
        return map.entrySet()
            .stream()
            .filter(k -> key.equals(k.getKey()) == false)
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    // map classes that are known to be immutable, used to speed up immutability check in #assertImmutableMap
    private static final Set<Class<?>> IMMUTABLE_MAP_CLASSES = Set.of(
        Collections.emptyMap().getClass(),
        Collections.unmodifiableMap(new HashMap<>()).getClass(),
        Map.of().getClass(),
        Map.of("a", "b").getClass()
    );

    private static <K, V> boolean assertIsImmutableMapAndNonNullKey(final Map<K, V> map, final K key, final V value) {
        assert key != null;
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
     * A convenience method to convert a collection of map entries to a map. The primary reason this method exists is to have a single
     * source file with an unchecked suppression rather than scattered at the various call sites.
     *
     * @param entries the entries to convert to a map
     * @param <K>     the type of the keys
     * @param <V>     the type of the values
     * @return an immutable map containing the specified entries
     */
    public static <K, V> Map<K, V> ofEntries(final Collection<Map.Entry<K, V>> entries) {
        @SuppressWarnings("unchecked")
        final Map<K, V> map = Map.ofEntries(entries.toArray(Map.Entry[]::new));
        return map;
    }

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

        for (Map.Entry<K, V> e : left.entrySet()) {
            if (right.containsKey(e.getKey()) == false) {
                return false;
            }

            V v1 = e.getValue();
            V v2 = right.get(e.getKey());
            if (v1 instanceof Map && v2 instanceof Map) {
                // if the values are both maps, then recursively compare them with Maps.deepEquals
                @SuppressWarnings("unchecked")
                Map<Object, Object> m1 = (Map<Object, Object>) v1;
                @SuppressWarnings("unchecked")
                Map<Object, Object> m2 = (Map<Object, Object>) v2;
                if (Maps.deepEquals(m1, m2) == false) {
                    return false;
                }
            } else if (Objects.deepEquals(v1, v2) == false) {
                return false;
            }
        }

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
            } else if (cur instanceof List) {
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

    /**
     * Returns a map with a capacity sufficient to keep expectedSize elements without being resized.
     *
     * @param expectedSize the expected amount of elements in the map
     * @param <K> the key type
     * @param <V> the value type
     * @return a new pre-sized {@link HashMap}
     */
    public static <K, V> Map<K, V> newMapWithExpectedSize(int expectedSize) {
        return newHashMapWithExpectedSize(expectedSize);
    }

    /**
     * Returns a hash map with a capacity sufficient to keep expectedSize elements without being resized.
     *
     * @param expectedSize the expected amount of elements in the map
     * @param <K> the key type
     * @param <V> the value type
     * @return a new pre-sized {@link HashMap}
     */
    public static <K, V> Map<K, V> newHashMapWithExpectedSize(int expectedSize) {
        return new HashMap<>(capacity(expectedSize));
    }

    /**
     * Returns a concurrent hash map with a capacity sufficient to keep expectedSize elements without being resized.
     *
     * @param expectedSize the expected amount of elements in the map
     * @param <K> the key type
     * @param <V> the value type
     * @return a new pre-sized {@link HashMap}
     */
    public static <K, V> Map<K, V> newConcurrentHashMapWithExpectedSize(int expectedSize) {
        return new ConcurrentHashMap<>(capacity(expectedSize));
    }

    /**
     * Returns a linked hash map with a capacity sufficient to keep expectedSize elements without being resized.
     *
     * @param expectedSize the expected amount of elements in the map
     * @param <K> the key type
     * @param <V> the value type
     * @return a new pre-sized {@link LinkedHashMap}
     */
    public static <K, V> LinkedHashMap<K, V> newLinkedHashMapWithExpectedSize(int expectedSize) {
        return new LinkedHashMap<>(capacity(expectedSize));
    }

    static int capacity(int expectedSize) {
        assert expectedSize >= 0;
        return expectedSize < 2 ? expectedSize + 1 : (int) (expectedSize / 0.75 + 1.0);
    }

    /**
     * This method creates a copy of the {@code source} map using {@code copyValueFunction} to create a defensive copy of each value.
     */
    public static <K, V> Map<K, V> copyOf(Map<K, V> source, Function<V, V> copyValueFunction) {
        return transformValues(source, copyValueFunction);
    }

    /**
     * Copy a map and transform it values using supplied function
     */
    public static <K, V1, V2> Map<K, V2> transformValues(Map<K, V1> source, Function<V1, V2> copyValueFunction) {
        var copy = Maps.<K, V2>newHashMapWithExpectedSize(source.size());
        for (var entry : source.entrySet()) {
            copy.put(entry.getKey(), copyValueFunction.apply(entry.getValue()));
        }
        return copy;
    }

    /**
     * An immutable implementation of {@link Map.Entry}.
     * Unlike {@code Map.entry(...)} this implementation permits null key and value.
     */
    public record ImmutableEntry<KType, VType>(KType key, VType value) implements Map.Entry<KType, VType> {

        @Override
        public KType getKey() {
            return key;
        }

        @Override
        public VType getValue() {
            return value;
        }

        @Override
        public VType setValue(VType value) {
            throw new UnsupportedOperationException();
        }

        @Override
        @SuppressWarnings("rawtypes")
        public boolean equals(Object o) {
            if (this == o) return true;
            if ((o instanceof Map.Entry) == false) return false;
            Map.Entry that = (Map.Entry) o;
            return Objects.equals(key, that.getKey()) && Objects.equals(value, that.getValue());
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(key) ^ Objects.hashCode(value);
        }
    }
}
