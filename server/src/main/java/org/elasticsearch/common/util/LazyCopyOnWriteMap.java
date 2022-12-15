/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceComputer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * This map is designed to be constructed from an immutable map and only be copied if a (rare) mutation operation occurs.
 * It could be converted back to an immutable map using `org.elasticsearch.common.util.LazyCopyOnWriteMap#toImmutableMap()`.
 */
public class CopyOnFirstWriteMap<K, V> implements Map<K, V> {

    private static final Logger logger = LogManager.getLogger(DesiredBalanceComputer.class);

    private Map<K, V> source;
    private Map<K, V> copy;

    public LazyCopyOnWriteMap(Map<K, V> source) {
        this.source = source;
        this.copy = null;
    }

    private Map<K, V> getForUpdate() {
        if (copy == null) {
            this.copy = new HashMap<>(source);
            this.source = null;
        }
        return copy;
    }

    private Map<K, V> getForRead() {
        return Objects.requireNonNullElse(source, copy);
    }

    public Map<K, V> toImmutableMap() {
        return Map.copyOf(getForRead());
    }

    @Override
    public int size() {
        return getForRead().size();
    }

    @Override
    public boolean isEmpty() {
        return getForRead().isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return getForRead().containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return getForRead().containsValue(value);
    }

    @Override
    public V get(Object key) {
        return getForRead().get(key);
    }

    @Override
    public V put(K key, V value) {
        return getForUpdate().put(key, value);
    }

    @Override
    public V remove(Object key) {
        return getForUpdate().remove(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        getForUpdate().putAll(m);
    }

    @Override
    public void clear() {
        getForUpdate().clear();
    }

    @Override
    public Set<K> keySet() {
        return getForRead().keySet();
    }

    @Override
    public Collection<V> values() {
        return getForRead().values();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return getForRead().entrySet();
    }
}
