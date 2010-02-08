/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.util.concurrent;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * Provides the semantics of a thread safe copy on write map.
 *
 * @author kimchy (Shay Banon)
 */
@ThreadSafe
public class CopyOnWriteMap<K, V> implements ConcurrentMap<K, V> {

    private volatile Map<K, V> map = new HashMap<K, V>();

    public void clear() {
        map = new HashMap<K, V>();
    }

    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    public boolean containsValue(Object value) {
        return map.containsValue(value);
    }

    public Set<Entry<K, V>> entrySet() {
        return map.entrySet();
    }

    public V get(Object key) {
        return map.get(key);
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public Set<K> keySet() {
        return map.keySet();
    }

    public V put(K key, V value) {
        synchronized (this) {
            Map<K, V> copyMap = copyMap();
            V put = copyMap.put(key, value);
            map = copyMap;
            return put;
        }
    }

    public synchronized void putAll(Map<? extends K, ? extends V> t) {
        Map<K, V> copyMap = copyMap();
        copyMap.putAll(t);
        map = copyMap;
    }

    public synchronized V remove(Object key) {
        Map<K, V> copyMap = copyMap();
        V remove = copyMap.remove(key);
        map = copyMap;
        return remove;
    }

    public int size() {
        return map.size();
    }

    public Collection<V> values() {
        return map.values();
    }

    private Map<K, V> copyMap() {
        return new HashMap<K, V>(map);
    }

    public synchronized V putIfAbsent(K key, V value) {
        V v = map.get(key);
        if (v == null) {
            Map<K, V> copyMap = copyMap();
            copyMap.put(key, value);
            map = copyMap;
        }
        return v;
    }

    public synchronized boolean remove(Object key, Object value) {
        V v = map.get(key);
        if (v != null && v.equals(value)) {
            Map<K, V> copyMap = copyMap();
            copyMap.remove(key);
            map = copyMap;
            return true;
        }
        return false;
    }

    public synchronized V replace(K key, V value) {
        V v = map.get(key);
        if (v != null) {
            Map<K, V> copyMap = copyMap();
            copyMap.put(key, value);
            map = copyMap;
        }
        return v;
    }

    public synchronized boolean replace(K key, V oldValue, V newValue) {
        V v = map.get(key);
        if (v != null && v.equals(oldValue)) {
            Map<K, V> copyMap = copyMap();
            copyMap.put(key, newValue);
            map = copyMap;
            return true;
        }
        return false;
    }

}
