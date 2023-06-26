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
import java.util.Map;
import java.util.Set;

/**
 * This map is designed to be constructed from an immutable map and be copied only if a (rare) mutation operation occurs.
 * It should be converted back to an immutable map using `org.elasticsearch.common.util.LazyCopyOnWriteMap#toImmutableMap()`.
 */
public class CopyOnFirstWriteMap<K, V> implements Map<K, V> {

    private Map<K, V> source;
    private boolean wasCopied = false;

    public CopyOnFirstWriteMap(Map<K, V> source) {
        this.source = source;
    }

    private Map<K, V> getForUpdate() {
        if (wasCopied == false) {
            source = new HashMap<>(source);
            wasCopied = true;
        }
        return source;
    }

    private Map<K, V> getForRead() {
        return source;
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
        return Collections.unmodifiableSet(getForRead().keySet());
    }

    @Override
    public Collection<V> values() {
        return Collections.unmodifiableCollection(getForRead().values());
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return Collections.unmodifiableSet(getForRead().entrySet());
    }
}
