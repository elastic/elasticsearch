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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public class LazyMap<K, V> implements Map<K, V> {

    private final Supplier<Map<K, V>> mapSupplier;
    private volatile Map<K, V> map;

    public LazyMap(Supplier<Map<K, V>> mapSupplier) {
        this.mapSupplier = mapSupplier;
    }

    private Map<K, V> get() {
        if (map == null) {
            synchronized (this) {
                if (map == null) {
                    map = Objects.requireNonNullElse(mapSupplier.get(), Collections.emptyMap());
                }
            }
        }
        return map;
    }

    @Override
    public int size() {
        return get().size();
    }

    @Override
    public boolean isEmpty() {
        return get().isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return get().containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return get().containsValue(value);
    }

    @Override
    public V get(Object key) {
        return get().get(key);
    }

    @Override
    public V put(K key, V value) {
        return get().put(key, value);
    }

    @Override
    public V remove(Object key) {
        return get().remove(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        get().putAll(m);
    }

    @Override
    public void clear() {
        get().clear();
    }

    @Override
    public Set<K> keySet() {
        return get().keySet();
    }

    @Override
    public Collection<V> values() {
        return get().values();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return get().entrySet();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return get().equals(o);
    }

    @Override
    public int hashCode() {
        return get().hashCode();
    }

    @Override
    public String toString() {
        return get().toString();
    }

    // Override default methods in Map
    @Override
    public V getOrDefault(Object k, V defaultValue) {
        return get().getOrDefault(k, defaultValue);
    }

    @Override
    public void forEach(BiConsumer<? super K, ? super V> action) {
        get().forEach(action);
    }

    @Override
    public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
        get().replaceAll(function);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        return get().putIfAbsent(key, value);
    }

    @Override
    public boolean remove(Object key, Object value) {
        return get().remove(key, value);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        return get().replace(key, oldValue, newValue);
    }

    @Override
    public V replace(K key, V value) {
        return get().replace(key, value);
    }

    @Override
    public V computeIfAbsent(K key,
                             Function<? super K, ? extends V> mappingFunction) {
        return get().computeIfAbsent(key, mappingFunction);
    }

    @Override
    public V computeIfPresent(K key,
                              BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        return get().computeIfPresent(key, remappingFunction);
    }

    @Override
    public V compute(K key,
                     BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        return get().compute(key, remappingFunction);
    }

    @Override
    public V merge(K key, V value,
                   BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        return get().merge(key, value, remappingFunction);
    }
}
