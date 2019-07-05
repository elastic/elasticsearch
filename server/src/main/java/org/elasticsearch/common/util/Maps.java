/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.util;

import org.elasticsearch.Assertions;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Map.entry;

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
    public static <K, V> Map<K, V> copyMapWithAddedEntry(final Map<K, V> map, final K key, final V value) {
        Objects.requireNonNull(map);
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        assertImmutableMap(map, key, value);
        assert map.containsKey(key) == false : "expected entry [" + key + "] to not already be present in map";
        return Stream.concat(map.entrySet().stream(), Stream.of(entry(key, value)))
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
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
    public static <K, V> Map<K, V> copyMayWithAddedOrReplacedEntry(final Map<K, V> map, final K key, final V value) {
        Objects.requireNonNull(map);
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        assertImmutableMap(map, key, value);
        return Stream.concat(map.entrySet().stream().filter(k -> key.equals(k.getKey()) == false), Stream.of(entry(key, value)))
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
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
        @SuppressWarnings("unchecked") final Map<K, V> map = Map.ofEntries(entries.toArray(Map.Entry[]::new));
        return map;
    }

}
