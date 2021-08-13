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

    public int size() {
        return get().size();
    }

    public boolean isEmpty() {
        return get().isEmpty();
    }

    public boolean containsKey(Object key) {
        return get().containsKey(key);
    }

    public boolean containsValue(Object value) {
        return get().containsValue(value);
    }

    public V get(Object key) {
        return get().get(key);
    }

    public V put(K key, V value) {
        return get().put(key, value);
    }

    public V remove(Object key) {
        return get().remove(key);
    }

    public void putAll(Map<? extends K, ? extends V> m) {
        get().putAll(m);
    }

    public void clear() {
        get().clear();
    }

    public Set<K> keySet() {
        return get().keySet();
    }

    public Collection<V> values() {
        return get().values();
    }

    public Set<Entry<K, V>> entrySet() {
        return get().entrySet();
    }

    public boolean equals(Object o) {
        return get().equals(o);
    }

    public int hashCode() {
        return get().hashCode();
    }
}
