/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

public class ConcurrentHashMapLong<T> implements ConcurrentMapLong<T> {

    private final ConcurrentMap<Long, T> map;

    public ConcurrentHashMapLong(ConcurrentMap<Long, T> map) {
        this.map = map;
    }

    @Override
    public T get(long key) {
        return map.get(key);
    }

    @Override
    public T remove(long key) {
        return map.remove(key);
    }

    @Override
    public T put(long key, T value) {
        return map.put(key, value);
    }

    // MAP DELEGATION

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public T get(Object key) {
        return map.get(key);
    }

    @Override
    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return map.containsValue(value);
    }

    @Override
    public T put(Long key, T value) {
        return map.put(key, value);
    }

    @Override
    public T putIfAbsent(Long key, T value) {
        return map.putIfAbsent(key, value);
    }

    @Override
    public void putAll(Map<? extends Long, ? extends T> m) {
        map.putAll(m);
    }

    @Override
    public T remove(Object key) {
        return map.remove(key);
    }

    @Override
    public boolean remove(Object key, Object value) {
        return map.remove(key, value);
    }

    @Override
    public boolean replace(Long key, T oldValue, T newValue) {
        return map.replace(key, oldValue, newValue);
    }

    @Override
    public T replace(Long key, T value) {
        return map.replace(key, value);
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public Set<Long> keySet() {
        return map.keySet();
    }

    @Override
    public Collection<T> values() {
        return map.values();
    }

    @Override
    public Set<Entry<Long, T>> entrySet() {
        return map.entrySet();
    }

    @Override
    public boolean equals(Object o) {
        return map.equals(o);
    }

    @Override
    public int hashCode() {
        return map.hashCode();
    }

    @Override
    public String toString() {
        return map.toString();
    }
}
