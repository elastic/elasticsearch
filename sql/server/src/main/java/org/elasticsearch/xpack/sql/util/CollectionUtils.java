/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public abstract class CollectionUtils {

    private static class Entry<K, V> {
        private final K k;
        private final V v;

        private Entry(K k, V v) {
            this.k = k;
            this.v = v;
        }
    }

    private static class ArrayIterator<T> implements Iterator<T> {

        private final T[] array;
        private int index = 0;

        private ArrayIterator(T[] array) {
            this.array = array;
        }

        @Override
        public boolean hasNext() {
            return index < array.length;
        }

        @Override
        public T next() {
            return array[index++];
        }
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <K, V> Map<K, V> combine(Map<? extends K, ? extends V>... maps) {
        if (ObjectUtils.isEmpty(maps)) {
            return emptyMap();
        }

        Map<K, V> map = new LinkedHashMap<>();

        for (Map<? extends K, ? extends V> m : maps) {
            map.putAll(m);
        }
        return map;
    }

    public static <K, V> Map<K, V> combine(Map<? extends K, ? extends V> left, Map<? extends K, ? extends V> right) {
        Map<K, V> map = new LinkedHashMap<>(left.size() + right.size());
        if (!left.isEmpty()) {
            map.putAll(left);
        }
        if (!right.isEmpty()) {
            map.putAll(right);
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    public static <T> List<T> combine(List<? extends T> left, List<? extends T> right) {
        if (right.isEmpty()) {
            return (List<T>) left;
        }
        if (left.isEmpty()) {
            return (List<T>) right;
        }

        List<T> list = new ArrayList<>(left.size() + right.size());
        if (!left.isEmpty()) {
            list.addAll(left);
        }
        if (!right.isEmpty()) {
            list.addAll(right);
        }
        return list;
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <T> List<T> combine(Collection<? extends T>... collections) {
        if (ObjectUtils.isEmpty(collections)) {
            return emptyList();
        }

        List<T> list = new ArrayList<>();
        for (Collection<? extends T> col : collections) {
            list.addAll(col);
        }
        return list;
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <T> List<T> combine(Collection<? extends T> left, T... entries) {
        List<T> list = new ArrayList<>(left.size() + entries.length);
        if (!left.isEmpty()) {
            list.addAll(left);
        }
        if (entries.length > 0) {
            Collections.addAll(list, entries);
        }
        return list;
    }

    public static <K, V> Map<K, V> of(K k1, V v1) {
        return fromEntries(entryOf(k1, v1));
    }

    public static <K, V> Map<K, V> of(K k1, V v1, K k2, V v2) {
        return fromEntries(entryOf(k1, v1), entryOf(k2, v2));
    }

    public static <K, V> Map<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3) {
        return fromEntries(entryOf(k1, v1), entryOf(k2, v2), entryOf(k3, v3));
    }

    public static <K, V> Map<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4) {
        return fromEntries(entryOf(k1, v1), entryOf(k2, v2), entryOf(k3, v3), entryOf(k4, v4));
    }

    public static <K, V> Map<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5) {
        return fromEntries(entryOf(k1, v1), entryOf(k2, v2), entryOf(k3, v3), entryOf(k4, v4), entryOf(k5, v5));
    }

    public static <K, V> Map<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5, K k6, V v6) {
        return fromEntries(entryOf(k1, v1), entryOf(k2, v2), entryOf(k3, v3), entryOf(k4, v4), entryOf(k5, v5), entryOf(k6, v6));
    }

    public static <K, V> Map<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5, K k6, V v6, K k7, V v7) {
        return fromEntries(entryOf(k1, v1), entryOf(k2, v2), entryOf(k3, v3), entryOf(k4, v4), entryOf(k5, v5), entryOf(k6, v6), entryOf(k7, v7));
    }

    public static <K, V> Map<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5, K k6, V v6, K k7, V v7, K k8, V v8) {
        return fromEntries(entryOf(k1, v1), entryOf(k2, v2), entryOf(k3, v3), entryOf(k4, v4), entryOf(k5, v5), entryOf(k6, v6), entryOf(k7, v7), entryOf(k8, v8));
    }

    @SafeVarargs
    private static <K, V> Map<K, V> fromEntries(Entry<K, V>... entries) {
        Map<K, V> map = new LinkedHashMap<K, V>();
        for (Entry<K, V> entry : entries) {
            if (entry.k != null) {
                map.put(entry.k, entry.v);
            }
        }
        return map;
    }

    private static <K, V> Entry<K, V> entryOf(K k, V v) {
        return new Entry<>(k, v);
    }

    public static <T> Iterator<T> iterator(final T[] array) {
        return new ArrayIterator<>(array);
    }
}