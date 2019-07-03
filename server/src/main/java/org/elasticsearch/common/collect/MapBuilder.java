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

package org.elasticsearch.common.collect;

import java.util.HashMap;
import java.util.Map;

// TODO: replace with usages of Map.of and Map.ofEntries
public class MapBuilder<K, V> {

    public static <K, V> MapBuilder<K, V> newMapBuilder() {
        return new MapBuilder<>();
    }

    public static <K, V> MapBuilder<K, V> newMapBuilder(Map<K, V> map) {
        return new MapBuilder<>(map);
    }

    private Map<K, V> map = new HashMap<>();

    public MapBuilder() {
        this.map = new HashMap<>();
    }

    public MapBuilder(Map<K, V> map) {
        this.map = new HashMap<>(map);
    }

    public MapBuilder<K, V> putAll(Map<K, V> map) {
        this.map.putAll(map);
        return this;
    }

    public MapBuilder<K, V> put(K key, V value) {
        this.map.put(key, value);
        return this;
    }

    public MapBuilder<K, V> remove(K key) {
        this.map.remove(key);
        return this;
    }

    public MapBuilder<K, V> clear() {
        this.map.clear();
        return this;
    }

    public V get(K key) {
        return map.get(key);
    }

    public boolean containsKey(K key) {
        return map.containsKey(key);
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public Map<K, V> map() {
        return this.map;
    }

    /**
     * Build an immutable copy of the map under construction. Always copies the map under construction. Prefer building
     * a HashMap by hand and wrapping it in an unmodifiableMap
     */
    public Map<K, V> immutableMap() {
        // TODO: follow the directions in the Javadoc for this method
        return Map.copyOf(map);
    }
}
