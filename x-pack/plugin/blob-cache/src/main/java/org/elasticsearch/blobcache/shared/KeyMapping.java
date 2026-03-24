/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.shared;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * A 2 layer key mapping for the shared cache.
 * @param <Key1> The outer layer key type
 * @param <Key2> The inner key type
 * @param <Value> The value type
 */
class KeyMapping<Key1, Key2, Value> {
    private final ConcurrentHashMap<Key1, ConcurrentHashMap<Key2, Value>> mapping = new ConcurrentHashMap<>();

    public Value get(Key1 key1, Key2 key2) {
        ConcurrentHashMap<Key2, Value> inner = mapping.get(key1);
        if (inner != null) {
            return inner.get(key2);
        } else {
            return null;
        }
    }

    /**
     * Compute a key if absent. Notice that unlike CHM#computeIfAbsent, locking will be done also when present
     * @param key1 The key1 part
     * @param key2 The key2 part
     * @param function the function to get from key2 to the value
     * @return the resulting value.
     */
    public Value computeIfAbsent(Key1 key1, Key2 key2, Function<Key2, Value> function) {
        AtomicReference<Value> result = new AtomicReference<>();
        mapping.compute(key1, (k, current) -> {
            ConcurrentHashMap<Key2, Value> map = current == null ? new ConcurrentHashMap<>() : current;
            result.setPlain(map.computeIfAbsent(key2, function));
            return map;
        });
        return result.getPlain();
    }

    public boolean remove(Key1 key1, Key2 key2, Value value) {
        ConcurrentHashMap<Key2, Value> inner = mapping.get(key1);
        if (inner != null) {
            boolean removed = inner.remove(key2, value);
            if (removed && inner.isEmpty()) {
                mapping.computeIfPresent(key1, (k, v) -> v.isEmpty() ? null : v);
            }
            return removed;
        }
        return false;
    }

    Iterable<Key1> key1s() {
        return mapping.keySet();
    }

    void forEach(Key1 key1, BiConsumer<Key2, Value> consumer) {
        ConcurrentHashMap<Key2, Value> map = mapping.get(key1);
        if (map != null) {
            map.forEach(consumer);
        }
    }

    void forEach(BiConsumer<Key2, Value> consumer) {
        for (ConcurrentHashMap<Key2, Value> map : mapping.values()) {
            map.forEach(consumer);
        }
    }
}
